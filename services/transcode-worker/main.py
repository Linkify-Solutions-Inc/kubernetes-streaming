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
from config import require, seal
from confluent_kafka import OFFSET_END, Consumer, Producer, TopicPartition

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
# Defaults to "media" so docker compose (which never sets S3_BUCKET) keeps
# working unchanged -- on EKS this is set to the globally-unique bucket name
# (see docs/aws/00-preflight-code-changes.md).
BUCKET = os.environ.get("S3_BUCKET", "media")

KAFKA_BOOTSTRAP_SERVERS = require("KAFKA_BOOTSTRAP_SERVERS")
POSTGRES_DSN = require("POSTGRES_DSN")
# TRANSCODE_MODE picks the code path this pod runs: "live" | "vod". Same
# image, both modes (see docs/aws/14-keda-scaledjobs.md).
MODE = require("TRANSCODE_MODE")
# MUST be byte-identical to the ScaledJob trigger's consumerGroup. If they
# differ, KEDA measures lag on a group nobody commits to and spawns Jobs at
# maxReplicaCount forever, quietly.
GROUP = require("TRANSCODE_CONSUMER_GROUP")
TOPIC = require("TRANSCODE_TRIGGER_TOPIC")
POD_NAME = require("POD_NAME")
seal()

BOOTSTRAP = KAFKA_BOOTSTRAP_SERVERS
CLAIM_DEADLINE_S = int(os.environ.get("CLAIM_DEADLINE_SECONDS", "120"))
HEARTBEAT_S = 15

status_producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
})


def s3_client():
    # Endpoint and static credentials are set for MinIO under docker compose
    # and unset on AWS, where boto3 resolves the region from AWS_REGION and
    # picks up credentials from the EKS Pod Identity agent. Passing explicit
    # keys would make boto3 ignore that role entirely.
    kwargs = {
        "config": Config(
            s3={"addressing_style": os.environ.get("S3_ADDRESSING_STYLE", "path")},
            retries={"max_attempts": 5, "mode": "adaptive"},
        )
    }
    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("s3", **kwargs)


def pg_connect():
    return psycopg.connect(POSTGRES_DSN, connect_timeout=3)


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


# ── Live streams ─────────────────────────────────────────────────────────
# One pod owns exactly one stream now (see docs/aws/14-keda-scaledjobs.md),
# so the in-memory active_streams routing table this used to need is gone:
# "which ffmpeg does this event belong to" collapses to "is it my path".
def start_live_transcode(path: str, stream_id: str, stop_event: threading.Event) -> None:
    input_url = f"{MEDIAMTX_RTMP_BASE}/{path}"
    # Keyed by stream_id (public-safe UUID), NOT path — path is the same
    # value as the stream's secret auth key (see research_validated_architecture
    # memory), so it must never end up in a URL a viewer's browser sees.
    s3_prefix = f"hls/live/{stream_id}"

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


# ── KEDA claim protocol ──────────────────────────────────────────────────
# KEDA counts consumer-group lag and creates Jobs from a fixed template — it
# never hands the pod a message. So the pod has to consume its own trigger
# message to learn what it owns, and the Postgres claim (not the Kafka
# offset commit) is what makes ownership durable and exclusive. See
# docs/aws/14-keda-scaledjobs.md for the full reasoning.
def claim_one(topic: str, group: str, try_claim) -> dict | None:
    """Consume until one message is successfully claimed, or the deadline expires.

    Returns the claimed payload, or None meaning "there is nothing here for me"
    — which is a normal, successful outcome, not an error.
    """
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        # MUST match the ScaledJob trigger's offsetResetPolicy. If this says
        # "earliest" and the group has no committed offset, the first pod ever
        # to run replays the whole topic and starts a transcode for every
        # stream that has ever existed.
        "auto.offset.reset": "latest",
        # Auto-commit would fire on a 5s timer regardless of whether we claimed
        # anything, silently dropping work when the pod exits before claiming.
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 45000,
    })
    c.subscribe([topic])
    deadline = time.monotonic() + CLAIM_DEADLINE_S
    try:
        while time.monotonic() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                log.error("unparseable message on %s: %r", topic, msg.value())
                c.commit(message=msg, asynchronous=False)  # poison pill: skip it
                continue

            won = try_claim(data)
            # Commit EITHER WAY. A lost claim still means the message has an
            # owner. Not committing means infinite redelivery and infinite
            # Job creation.
            c.commit(message=msg, asynchronous=False)
            if won:
                return data
            log.info("lost claim for %s, looking for other work", data)
        return None
    finally:
        c.close()


def _claim_stream(stream_id: str) -> str | None:
    """Atomically claim a live stream. Returns its path, or None if lost."""
    with pg_connect() as conn:
        row = conn.execute(
            """
            UPDATE streams
               SET transcode_status = 'claimed', transcode_heartbeat = now()
             WHERE id = %s AND transcode_status = 'pending' AND status = 'live'
            RETURNING path
            """,
            (stream_id,),
        ).fetchone()
        conn.commit()
    return row[0] if row else None


def _claim_video(video_id: str) -> bool:
    # Atomic claim, backed by Postgres rather than in-memory tracking -- a
    # redelivered "uploaded" event (Kafka at-least-once delivery) needs to be
    # a no-op even across a pod restart, since a fresh pod has no memory of
    # what it was already working on. Only a video still in "uploaded" gets
    # claimed; one already "transcoding"/"ready"/"failed" is left alone.
    # Trade-off: if a pod crashes mid-transcode, the video is stuck at
    # status='transcoding' until the sweeper's heartbeat check re-queues it.
    with pg_connect() as conn:
        row = conn.execute(
            "UPDATE videos SET status = 'transcoding' WHERE id = %s AND status = 'uploaded' RETURNING id",
            (video_id,),
        ).fetchone()
        conn.commit()
    return row is not None


def heartbeat_thread(table: str, row_id: str, stop_event: threading.Event) -> None:
    """Renew the claim lease every 15s, and notice if the stream ended.

    The lease is what the sweeper reads. If this thread stops (pod killed,
    node reclaimed), the heartbeat goes stale and the sweeper re-queues the
    work. That is the entire recovery mechanism for a crash after commit.
    """
    while not stop_event.wait(HEARTBEAT_S):
        try:
            with pg_connect() as conn:
                conn.execute(
                    f"UPDATE {table} SET transcode_heartbeat = now() WHERE id = %s",
                    (row_id,),
                )
                if table == "streams":
                    # Tertiary teardown path: covers an 'ended' event we never
                    # saw on Kafka (broker restart mid-stream).
                    status = conn.execute(
                        "SELECT status FROM streams WHERE id = %s", (row_id,)
                    ).fetchone()
                    if status and status[0] != "live":
                        log.info("stream %s is no longer live in the DB, stopping", row_id)
                        stop_event.set()
                conn.commit()
        except Exception:
            # A transient RDS blip must not kill the transcode. Missing one
            # beat is fine — the sweeper's threshold is eight beats wide.
            log.exception("heartbeat failed for %s=%s", table, row_id)


def watch_for_end(path: str, stop_event: threading.Event, started_at: float) -> None:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        # Throwaway and unique per pod. confluent-kafka requires group.id to be
        # set, but we never commit and never join a group — see assign() below.
        "group.id": f"live-watch-{POD_NAME}",
        "enable.auto.commit": False,
    })
    md = c.list_topics("stream.lifecycle", timeout=10)
    parts = list(md.topics["stream.lifecycle"].partitions)

    # Start ~60s before this Job began, not at the tail. For a very short
    # stream the 'ended' event can land while this pod is still pulling its
    # image, and OFFSET_END would miss it forever.
    since_ms = int((started_at - 60) * 1000)
    tps = c.offsets_for_times(
        [TopicPartition("stream.lifecycle", p, since_ms) for p in parts], timeout=10
    )
    for tp in tps:
        if tp.offset < 0:            # no message at or after that timestamp
            tp.offset = OFFSET_END
    c.assign(tps)                    # NOT subscribe() — see below

    try:
        while not stop_event.is_set():
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                d = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                continue
            if d.get("path") == path and d.get("event") == "ended":
                log.info("received 'ended' for path=%s, stopping", path)
                stop_event.set()
                return
    finally:
        c.close()


# ── VOD uploads (upload.events) ─────────────────────────────────────────
def set_video_status(video_id: str, status: str) -> None:
    with pg_connect() as conn:
        conn.execute("UPDATE videos SET status = %s WHERE id = %s", (status, video_id))
        conn.commit()


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


# ── Entrypoint — a dispatcher, not a loop ────────────────────────────────
# KEDA creates one Job per unit of work; this process claims exactly one
# message, does the work (or discovers someone else already has it), and
# exits. See docs/aws/14-keda-scaledjobs.md.
def main() -> None:
    started_at = time.time()

    if MODE == "live":
        claimed = {}

        def try_claim(data: dict) -> bool:
            stream_id = data.get("stream_id")
            if not stream_id:
                log.error("no stream_id in %s, skipping", data)
                return False
            path = _claim_stream(stream_id)
            if path is None:
                return False
            claimed.update(stream_id=stream_id, path=path)
            return True

        if claim_one(TOPIC, GROUP, try_claim) is None:
            log.info("no live work available within the deadline, exiting cleanly")
            return  # exit 0, NOT an error

        stop_event = threading.Event()
        threading.Thread(
            target=watch_for_end, args=(claimed["path"], stop_event, started_at), daemon=True
        ).start()
        threading.Thread(
            target=heartbeat_thread, args=("streams", claimed["stream_id"], stop_event), daemon=True
        ).start()
        # start_live_transcode keeps its retry/stall/upload logic verbatim; it
        # takes stop_event from the caller and no longer touches active_streams.
        start_live_transcode(claimed["path"], claimed["stream_id"], stop_event)

    elif MODE == "vod":
        claimed = {}

        def try_claim(data: dict) -> bool:
            if data.get("event") != "uploaded":
                return False  # committed, then skipped
            if not _claim_video(data["video_id"]):
                return False
            claimed.update(video_id=data["video_id"], object_key=data["object_key"])
            return True

        if claim_one(TOPIC, GROUP, try_claim) is None:
            log.info("no VOD work available within the deadline, exiting cleanly")
            return

        stop_event = threading.Event()
        threading.Thread(
            target=heartbeat_thread, args=("videos", claimed["video_id"], stop_event), daemon=True
        ).start()
        try:
            transcode_video(claimed["video_id"], claimed["object_key"])
        finally:
            stop_event.set()

    else:
        raise SystemExit(f"unknown TRANSCODE_MODE {MODE!r}")


if __name__ == "__main__":
    main()
