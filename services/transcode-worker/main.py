import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time

import boto3
import psycopg
from botocore.config import Config
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("transcode-worker")

# ABR ladder — settled now (was JIT in SPEC.md), loosely sourced from the
# earlier research pass (dacast.com-style bitrate ranges). Revisit once we
# have real CPU-cost numbers or real viewers.
RENDITIONS = [
    {"name": "1080p", "width": 1920, "height": 1080, "v_bitrate": "5000k", "maxrate": "5350k", "bufsize": "7500k"},
    {"name": "720p", "width": 1280, "height": 720, "v_bitrate": "2800k", "maxrate": "2996k", "bufsize": "4200k"},
    {"name": "480p", "width": 854, "height": 480, "v_bitrate": "1400k", "maxrate": "1498k", "bufsize": "2100k"},
]
SEGMENT_SECONDS = 4
GOP_SECONDS = 2  # 2 keyframes/segment -> GOP is an integer divisor of segment length
OUTPUT_FPS = 30
LIVE_LIST_SIZE = 30  # segments kept in the live sliding window (30 * 4s = ~2min DVR range)
MEDIAMTX_RTMP_BASE = os.environ.get("MEDIAMTX_RTMP_URL", "rtmp://mediamtx:1935")
BUCKET = "media"

status_producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "enable.idempotence": True,
})

# path -> {"stop_event", "done_event"} for currently-live streams, so an
# "ended" event can find and stop the right job. A value of None means the
# path has been claimed (a "started" event was just dispatched) but
# start_live_transcode hasn't filled in the real entry yet -- see
# handle_stream_lifecycle's dedup guard.
active_streams: dict[str, dict | None] = {}
active_streams_lock = threading.Lock()


def s3_client():
    # path-style addressing is required for MinIO; S3 in prod also accepts it,
    # so this client code doesn't need to change between dev and prod.
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )


def pg_connect():
    return psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=3)


def emit_status(event: str, **fields) -> None:
    payload = json.dumps({"event": event, "ts": time.time(), **fields})
    key = fields.get("path") or fields.get("video_id")
    status_producer.produce("transcode.status", key=key, value=payload)
    status_producer.flush()


# ── ffmpeg command ───────────────────────────────────────────────────────
# One process, one decode, split into N scaled renditions via filter_complex
# — not N independent ffmpeg processes — so GOP/keyframe placement is
# identical across renditions (required for HLS ABR switching; see
# research_validated_architecture memory). -g/-keyint_min/-sc_threshold
# force that alignment; -r pins output framerate so GOP length in frames
# maps to a fixed, known duration regardless of source framerate.
def build_ffmpeg_command(input_url: str, output_dir: str, *, live: bool) -> list[str]:
    split_outputs = "".join(f"[v{i}]" for i in range(len(RENDITIONS)))
    filter_parts = [f"[0:v]split={len(RENDITIONS)}{split_outputs}"]
    for i, r in enumerate(RENDITIONS):
        filter_parts.append(f"[v{i}]scale=w={r['width']}:h={r['height']}[v{i}out]")
    filter_complex = ";".join(filter_parts)

    # Live must encode at least as fast as the source plays, or the encoder
    # falls behind and effectively stops being live; VOD has no such
    # constraint, so it gets a slower/higher-quality preset. (A short live
    # test once looked like it was hitting exactly this — output cut off
    # early — but that turned out to be a test-harness bug: the synthetic
    # publisher lacked `-re` to pace itself at real-time speed, so it dumped
    # the whole clip in a couple of wall-clock seconds. Keeping `veryfast`
    # regardless since it's still the right call for live headroom.)
    preset = "veryfast" if live else "medium"

    cmd = ["ffmpeg", "-y", "-i", input_url, "-filter_complex", filter_complex]
    for i, r in enumerate(RENDITIONS):
        cmd += [
            "-map", f"[v{i}out]",
            f"-c:v:{i}", "libx264",
            f"-preset:v:{i}", preset,
            f"-b:v:{i}", r["v_bitrate"],
            f"-maxrate:v:{i}", r["maxrate"],
            f"-bufsize:v:{i}", r["bufsize"],
        ]
    # One AAC encode per rendition (cheap relative to x264) rather than
    # sharing a single audio stream across variants — ffmpeg's HLS muxer
    # rejects mapping the same elementary stream into more than one variant
    # ("Same elementary stream found more than once in two different
    # variant definitions"), confirmed by hitting this error directly.
    for i in range(len(RENDITIONS)):
        cmd += ["-map", "a:0?", f"-c:a:{i}", "aac", f"-b:a:{i}", "128k"]

    hls_flags = "independent_segments" + ("+delete_segments" if live else "")
    cmd += [
        "-r", str(OUTPUT_FPS),
        "-g", str(OUTPUT_FPS * GOP_SECONDS),
        "-keyint_min", str(OUTPUT_FPS * GOP_SECONDS),
        "-sc_threshold", "0",
        "-f", "hls",
        "-hls_time", str(SEGMENT_SECONDS),
        "-hls_flags", hls_flags,
    ]
    if live:
        # ffmpeg's default -hls_list_size is 5 (20s at our 4s segment length)
        # -- enough for a sliding "live" window (see the playlist-type note
        # below) but too tight for two things viewers hit: (1) essentially no
        # seek-back range, and (2) a player that drifts even a few seconds
        # behind (a network blip, a tab backgrounded) can fall out the back
        # of the window before it catches up, forcing hls.js into a hard
        # live-edge resync -- which reads as "stuck". LIVE_LIST_SIZE trades
        # some disk/MinIO storage (cheap, and we have headroom) for a real
        # ~2 minute DVR window and much more slack before that resync case.
        cmd += ["-hls_list_size", str(LIVE_LIST_SIZE)]
    else:
        # "vod" finalizes the playlist with EXT-X-ENDLIST once ffmpeg exits.
        # Live intentionally omits -hls_playlist_type: ffmpeg's default here is
        # a sliding-window *live* playlist (bounded by -hls_list_size, default
        # 5 segments) where old entries actually drop out of the manifest as
        # new ones land — required for +delete_segments above to do anything,
        # and for players to join at the live edge instead of segment 0.
        # "event" (the previous setting) never removes segments — the
        # manifest and EXT-X-MEDIA-SEQUENCE grow forever — which is why a
        # fresh viewer / a page refresh started from the very beginning of
        # the stream instead of near live, and old .ts files piled up on
        # disk despite +delete_segments (confirmed empirically: pulled the
        # live manifest mid-stream and found all segments back to #000).
        cmd += ["-hls_playlist_type", "vod"]
    cmd += [
        "-master_pl_name", "master.m3u8",
        "-var_stream_map", " ".join(f"v:{i},a:{i},name:{r['name']}" for i, r in enumerate(RENDITIONS)),
        "-hls_segment_filename", os.path.join(output_dir, "%v", "segment_%03d.ts"),
        os.path.join(output_dir, "%v", "playlist.m3u8"),
    ]
    return cmd


def _make_rendition_dirs(output_dir: str) -> None:
    # ffmpeg's hls muxer doesn't create the %v subdirectories itself.
    for r in RENDITIONS:
        os.makedirs(os.path.join(output_dir, r["name"]), exist_ok=True)


# ── Upload helpers ───────────────────────────────────────────────────────
# boto3/MinIO don't set Content-Type or Cache-Control unless told to — MIME
# guessing doesn't know .m3u8/.ts, and with no Cache-Control at all the
# browser is free to apply its own heuristic caching to whatever it gets.
# For a *live* playlist that's actively wrong: it's rewritten every couple
# of seconds as segments land and rotate out of the sliding window, so a
# stale cached copy references segments that no longer exist — this was the
# "works in a fresh tab, breaks on refresh" bug (a fresh tab has no cached
# entry for that URL yet; a refresh of an already-visited watch page could
# get served MinIO's earlier response straight from the browser's HTTP
# cache). Segments are immutable once written (never rewritten under a
# given key) and safe to cache forever; a VOD playlist is also written once
# and never touched again, so it gets the same long-lived treatment.
def _s3_extra_args(fname: str, *, live: bool) -> dict:
    if fname.endswith(".m3u8"):
        cache_control = "no-cache, must-revalidate" if live else "public, max-age=31536000, immutable"
        return {"ContentType": "application/vnd.apple.mpegurl", "CacheControl": cache_control}
    return {"ContentType": "video/mp2t", "CacheControl": "public, max-age=31536000, immutable"}


def _upload_pass(client, output_dir: str, s3_prefix: str, uploaded_segments: set) -> None:
    for root, _dirs, files in os.walk(output_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, output_dir).replace(os.sep, "/")
            key = f"{s3_prefix}/{rel_path}"
            if fname.endswith(".ts"):
                # Segments are immutable once written — upload each once.
                if key in uploaded_segments:
                    continue
                uploaded_segments.add(key)
            # .m3u8 playlists get rewritten as new segments land, so they're
            # re-uploaded every pass (cheap — they're tiny text files).
            try:
                client.upload_file(local_path, BUCKET, key, ExtraArgs=_s3_extra_args(fname, live=True))
            except Exception:
                log.exception("failed to upload %s", key)


def upload_loop(output_dir: str, s3_prefix: str, stop_event: threading.Event) -> None:
    client = s3_client()
    uploaded_segments: set[str] = set()
    while not stop_event.is_set():
        _upload_pass(client, output_dir, s3_prefix, uploaded_segments)
        stop_event.wait(2)
    # Final pass to catch whatever ffmpeg wrote right before it exited
    # (including the EXT-X-ENDLIST tag on the now-finalized playlists).
    _upload_pass(client, output_dir, s3_prefix, uploaded_segments)


# ── Watchdog for a wedged ffmpeg ─────────────────────────────────────────
# Empirically observed on the dev box: when MediaMTX drops ffmpeg's RTMP
# read connection for falling behind ("reader is too slow" -> "i/o timeout"
# in mediamtx logs), ffmpeg does not exit or reconnect — it just goes idle
# (~0% CPU, still alive) and never writes another segment, silently
# freezing the stream for viewers forever. Root cause not fully pinned down
# (suspected backpressure deadlock across the single-process
# decode/split/3x-encode pipeline when one branch stalls), but since ffmpeg
# never self-recovers, this watches for segment-write staleness and signals
# a forced restart instead of leaving the stream dead.
STALL_TIMEOUT_SECONDS = 20  # generous vs. the 4s segment cadence + startup


def _watch_for_stall(output_dir: str, stop_event: threading.Event, stalled_event: threading.Event) -> None:
    started_at = time.time()
    while not stop_event.wait(3):
        newest = started_at
        for root, _dirs, files in os.walk(output_dir):
            for fname in files:
                if fname.endswith(".ts"):
                    newest = max(newest, os.path.getmtime(os.path.join(root, fname)))
        if time.time() - newest > STALL_TIMEOUT_SECONDS:
            stalled_event.set()
            return


# ── Live streams (stream.lifecycle) ─────────────────────────────────────
def start_live_transcode(path: str, stream_id: str | None) -> None:
    if stream_id is None:
        # Shouldn't happen — ingest-webhook always creates the streams row
        # (and includes stream_id in the event) before emitting "started".
        log.error("no stream_id for path=%s, refusing to start (can't pick a safe MinIO prefix)", path)
        # handle_stream_lifecycle already claimed this path with a None
        # placeholder before spawning us -- release it, or this path can
        # never start a transcode again (it'd look permanently "active").
        with active_streams_lock:
            if active_streams.get(path) is None:
                active_streams.pop(path, None)
        return

    input_url = f"{MEDIAMTX_RTMP_BASE}/{path}"
    # Keyed by stream_id (public-safe UUID), NOT path — path is the same
    # value as the stream's secret auth key (see research_validated_architecture
    # memory), so it must never end up in a URL a viewer's browser sees.
    s3_prefix = f"hls/live/{stream_id}"

    stop_event = threading.Event()  # set by stop_live_transcode: stop, don't restart
    done_event = threading.Event()  # set once this function has fully torn down
    with active_streams_lock:
        active_streams[path] = {"stop_event": stop_event, "done_event": done_event}

    emit_status("live_transcode_started", path=path, stream_id=stream_id)

    attempt = 0
    while not stop_event.is_set():
        attempt += 1
        output_dir = tempfile.mkdtemp(prefix="live-")
        _make_rendition_dirs(output_dir)

        cmd = build_ffmpeg_command(input_url, output_dir, live=True)
        log.info(
            "live transcode starting (attempt %d): path=%s stream_id=%s cmd=%s",
            attempt, path, stream_id, " ".join(cmd),
        )
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)

        uploader_stop = threading.Event()
        uploader = threading.Thread(target=upload_loop, args=(output_dir, s3_prefix, uploader_stop), daemon=True)
        uploader.start()

        stalled = threading.Event()
        watchdog = threading.Thread(target=_watch_for_stall, args=(output_dir, uploader_stop, stalled), daemon=True)
        watchdog.start()

        while proc.poll() is None and not stalled.is_set() and not stop_event.is_set():
            time.sleep(1)

        if proc.poll() is None:
            if stop_event.is_set():
                proc.terminate()  # SIGTERM -> ffmpeg finalizes the playlist (EXT-X-ENDLIST) and exits
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            else:
                # Confirmed wedged (no output for STALL_TIMEOUT_SECONDS) — it
                # never responds to a graceful signal on its own, so don't
                # wait around for one.
                proc.kill()
                proc.wait()
        returncode = proc.returncode

        uploader_stop.set()
        uploader.join(timeout=15)
        shutil.rmtree(output_dir, ignore_errors=True)

        if stop_event.is_set():
            emit_status("live_transcode_stopped", path=path, stream_id=stream_id)
            break
        if stalled.is_set():
            stderr_tail = proc.stderr.read()[-2000:] if proc.stderr else ""
            log.error(
                "ffmpeg wedged (no new segment for %ds) for path=%s, restarting: %s",
                STALL_TIMEOUT_SECONDS, path, stderr_tail,
            )
            continue
        # ffmpeg exited on its own (bad input, crash) with no stop requested
        # and no stall detected — don't retry forever into the same failure.
        if returncode != 0:
            stderr_tail = proc.stderr.read()[-2000:] if proc.stderr else ""
            log.error("ffmpeg exited %s for path=%s: %s", returncode, path, stderr_tail)
        break

    with active_streams_lock:
        if active_streams.get(path, {}).get("stop_event") is stop_event:
            active_streams.pop(path, None)
    done_event.set()


def stop_live_transcode(path: str) -> None:
    with active_streams_lock:
        entry = active_streams.get(path)
    if entry is None:
        log.warning("no active live transcode for path=%s (already stopped, or never started)", path)
        return

    entry["stop_event"].set()
    entry["done_event"].wait(timeout=30)


def handle_stream_lifecycle(data: dict) -> None:
    path = data.get("path")
    if not path:
        return
    if data.get("event") == "started":
        with active_streams_lock:
            # Redelivered/duplicate "started" (Kafka at-least-once delivery,
            # or ingest-webhook's own duplicate-hook guard missing a race) --
            # claim the path here, synchronously, rather than leaving it to
            # start_live_transcode to set active_streams[path] itself, since
            # that happens in the spawned thread and a second "started"
            # dispatched immediately after could otherwise land before the
            # first thread claims it. Note this only guards against
            # redelivery within one transcode-worker process's lifetime --
            # a container restart wipes active_streams entirely, but a
            # restart also tears down any in-flight ffmpeg child with it
            # (same PID namespace), so there's no orphaned process to race.
            if path in active_streams:
                log.warning("duplicate 'started' for path=%s, already transcoding, ignoring", path)
                return
            active_streams[path] = None
        threading.Thread(target=start_live_transcode, args=(path, data.get("stream_id")), daemon=True).start()
    elif data.get("event") == "ended":
        threading.Thread(target=stop_live_transcode, args=(path,), daemon=True).start()


# ── VOD uploads (upload.events) ─────────────────────────────────────────
def set_video_status(video_id: str, status: str) -> None:
    with pg_connect() as conn:
        conn.execute("UPDATE videos SET status = %s WHERE id = %s", (status, video_id))
        conn.commit()


def _claim_video(video_id: str) -> bool:
    # Atomic claim, backed by Postgres rather than the in-memory
    # active_streams-style tracking used for live streams -- a redelivered
    # "uploaded" event (Kafka at-least-once delivery) needs to be a no-op
    # even across a transcode-worker restart, since a fresh process has no
    # memory of what it was already working on. Only a video still in
    # "uploaded" gets claimed; one already "transcoding"/"ready"/"failed" is
    # left alone. Trade-off: if a worker crashes mid-transcode, the video is
    # stuck at status='transcoding' forever (a redelivered event won't match
    # this WHERE clause and retry it) -- safe against duplicates, but a
    # stuck-job sweep is a separate concern, not attempted here.
    with pg_connect() as conn:
        row = conn.execute(
            "UPDATE videos SET status = 'transcoding' WHERE id = %s AND status = 'uploaded' RETURNING id",
            (video_id,),
        ).fetchone()
        conn.commit()
    return row is not None


def transcode_video(video_id: str, object_key: str) -> None:
    client = s3_client()

    with tempfile.TemporaryDirectory() as tmp:
        local_input = os.path.join(tmp, "input" + os.path.splitext(object_key)[1])
        client.download_file(BUCKET, object_key, local_input)

        output_dir = os.path.join(tmp, "out")
        _make_rendition_dirs(output_dir)

        cmd = build_ffmpeg_command(local_input, output_dir, live=False)
        log.info("VOD transcode starting: video_id=%s cmd=%s", video_id, " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            log.error("ffmpeg failed for video_id=%s: %s", video_id, result.stderr[-2000:])
            set_video_status(video_id, "failed")
            emit_status("transcode_failed", video_id=video_id)
            return

        s3_prefix = f"hls/vod/{video_id}"
        for root, _dirs, files in os.walk(output_dir):
            for fname in files:
                local_path = os.path.join(root, fname)
                rel_path = os.path.relpath(local_path, output_dir).replace(os.sep, "/")
                client.upload_file(
                    local_path, BUCKET, f"{s3_prefix}/{rel_path}",
                    ExtraArgs=_s3_extra_args(fname, live=False),
                )

    set_video_status(video_id, "ready")
    emit_status("transcode_ready", video_id=video_id)


def handle_upload_event(data: dict) -> None:
    if data.get("event") != "uploaded":
        return
    video_id = data["video_id"]
    if not _claim_video(video_id):
        log.warning(
            "video_id=%s not in 'uploaded' status, ignoring duplicate/redelivered event", video_id
        )
        return
    threading.Thread(
        target=transcode_video, args=(video_id, data["object_key"]), daemon=True
    ).start()


# ── Kafka consume loop ───────────────────────────────────────────────────
def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "transcode-worker",
            "auto.offset.reset": "earliest",
        }
    )
    # transcode-worker consumes stream.lifecycle/upload.events directly —
    # no separate dispatcher stage onto `transcode.jobs` (settled JIT
    # question, see SPEC.md). That topic still exists in Kafka in case a
    # dispatcher turns out to be worth adding later.
    consumer.subscribe(["stream.lifecycle", "upload.events"])
    log.info("transcode-worker up, watching stream.lifecycle + upload.events")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                log.error("unparseable message on %s: %r", msg.topic(), msg.value())
                continue

            if msg.topic() == "stream.lifecycle":
                handle_stream_lifecycle(data)
            elif msg.topic() == "upload.events":
                handle_upload_event(data)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
