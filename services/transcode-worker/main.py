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
MEDIAMTX_RTMP_BASE = os.environ.get("MEDIAMTX_RTMP_URL", "rtmp://mediamtx:1935")
BUCKET = "media"

status_producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})

# path -> {"proc", "stop_event", "uploader", "output_dir", "stream_id"} for
# currently-live streams, so an "ended" event can find and stop the right job.
active_streams: dict[str, dict] = {}
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
        "-hls_playlist_type", "event" if live else "vod",
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
                client.upload_file(local_path, BUCKET, key)
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


# ── Live streams (stream.lifecycle) ─────────────────────────────────────
def start_live_transcode(path: str, stream_id: str | None) -> None:
    if stream_id is None:
        # Shouldn't happen — ingest-webhook always creates the streams row
        # (and includes stream_id in the event) before emitting "started".
        log.error("no stream_id for path=%s, refusing to start (can't pick a safe MinIO prefix)", path)
        return

    output_dir = tempfile.mkdtemp(prefix="live-")
    _make_rendition_dirs(output_dir)

    input_url = f"{MEDIAMTX_RTMP_BASE}/{path}"
    cmd = build_ffmpeg_command(input_url, output_dir, live=True)
    log.info("live transcode starting: path=%s stream_id=%s cmd=%s", path, stream_id, " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)

    stop_event = threading.Event()
    # Keyed by stream_id (public-safe UUID), NOT path — path is the same
    # value as the stream's secret auth key (see research_validated_architecture
    # memory), so it must never end up in a URL a viewer's browser sees.
    s3_prefix = f"hls/live/{stream_id}"
    uploader = threading.Thread(target=upload_loop, args=(output_dir, s3_prefix, stop_event), daemon=True)
    uploader.start()

    with active_streams_lock:
        active_streams[path] = {
            "proc": proc,
            "stop_event": stop_event,
            "uploader": uploader,
            "output_dir": output_dir,
            "stream_id": stream_id,
        }

    emit_status("live_transcode_started", path=path, stream_id=stream_id)

    # If ffmpeg dies on its own (bad input, crash), don't leave a stale
    # entry in active_streams blocking a future "started" for the same path.
    returncode = proc.wait()
    if returncode != 0:
        stderr_tail = proc.stderr.read()[-2000:] if proc.stderr else ""
        log.error("ffmpeg exited %s for path=%s: %s", returncode, path, stderr_tail)
    with active_streams_lock:
        if active_streams.get(path, {}).get("proc") is proc:
            active_streams.pop(path, None)


def stop_live_transcode(path: str) -> None:
    with active_streams_lock:
        entry = active_streams.pop(path, None)
    if entry is None:
        log.warning("no active live transcode for path=%s (already stopped, or never started)", path)
        return

    entry["proc"].terminate()  # SIGTERM -> ffmpeg finalizes the playlist (EXT-X-ENDLIST) and exits
    try:
        entry["proc"].wait(timeout=15)
    except subprocess.TimeoutExpired:
        entry["proc"].kill()

    entry["stop_event"].set()
    entry["uploader"].join(timeout=15)
    shutil.rmtree(entry["output_dir"], ignore_errors=True)

    emit_status("live_transcode_stopped", path=path, stream_id=entry["stream_id"])


def handle_stream_lifecycle(data: dict) -> None:
    path = data.get("path")
    if not path:
        return
    if data.get("event") == "started":
        threading.Thread(target=start_live_transcode, args=(path, data.get("stream_id")), daemon=True).start()
    elif data.get("event") == "ended":
        threading.Thread(target=stop_live_transcode, args=(path,), daemon=True).start()


# ── VOD uploads (upload.events) ─────────────────────────────────────────
def set_video_status(video_id: str, status: str) -> None:
    with pg_connect() as conn:
        conn.execute("UPDATE videos SET status = %s WHERE id = %s", (status, video_id))
        conn.commit()


def transcode_video(video_id: str, object_key: str) -> None:
    set_video_status(video_id, "transcoding")
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
                client.upload_file(local_path, BUCKET, f"{s3_prefix}/{rel_path}")

    set_video_status(video_id, "ready")
    emit_status("transcode_ready", video_id=video_id)


def handle_upload_event(data: dict) -> None:
    if data.get("event") != "uploaded":
        return
    threading.Thread(
        target=transcode_video, args=(data["video_id"], data["object_key"]), daemon=True
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
