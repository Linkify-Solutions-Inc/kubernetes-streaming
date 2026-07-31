import json
import os
import secrets
import time
import uuid

import boto3
import psycopg
from botocore.config import Config
from config import require, seal
from confluent_kafka import Producer
from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile
from psycopg.rows import dict_row
from pydantic import BaseModel

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = require("KAFKA_BOOTSTRAP_SERVERS")
POSTGRES_DSN = require("POSTGRES_DSN")
seal()

# Defaults to "media" so docker compose (which never sets S3_BUCKET) keeps
# working unchanged -- on EKS this is set to the globally-unique bucket name
# (see docs/aws/00-preflight-code-changes.md).
BUCKET = os.environ.get("S3_BUCKET", "media")

# enable.idempotence: a produce-request retry (broker restart, network blip)
# would otherwise risk landing the same "uploaded"/"view" event twice.
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "enable.idempotence": True,
})

# Anyone holding a stream key could otherwise upload arbitrarily large or
# arbitrarily many files (see SPEC.md Phase 1 hardening) -- both configurable
# since "arbitrarily large" and "arbitrarily many" are judgment calls that'll
# want tuning once real usage exists.
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", 5 * 1024 * 1024 * 1024))  # 5 GiB
UPLOAD_RATE_LIMIT_COUNT = int(os.environ.get("UPLOAD_RATE_LIMIT_COUNT", 10))
UPLOAD_RATE_LIMIT_WINDOW_MINUTES = int(os.environ.get("UPLOAD_RATE_LIMIT_WINDOW_MINUTES", 60))


def pg_connect():
    return psycopg.connect(
        POSTGRES_DSN, connect_timeout=3, row_factory=dict_row
    )


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


@app.get("/health")
def health():
    checks = {}

    try:
        with pg_connect() as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001 - health check, want to report any failure
        checks["postgres"] = f"error: {exc}"

    try:
        s3_client().head_bucket(Bucket=BUCKET)
        checks["minio"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["minio"] = f"error: {exc}"

    return checks


# /health above answers 200 unconditionally and does real dependency checks
# -- fine for a human with curl, wrong for both Kubernetes probes: wired to
# liveness it turns a brief RDS blip into a restart storm; wired to
# readiness it can never actually fail. /livez and /readyz split the two
# questions a probe can ask ("is this process wedged" vs "should it get
# traffic right now") -- see docs/aws/11-workloads.md.
@app.get("/livez")
def livez():
    return {"status": "ok"}


@app.get("/readyz")
def readyz(response: Response):
    checks, ok = {}, True
    try:
        with pg_connect() as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["postgres"], ok = f"error: {exc}", False
    try:
        s3_client().head_bucket(Bucket=BUCKET)
        checks["s3"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["s3"], ok = f"error: {exc}", False
    if not ok:
        response.status_code = 503
    return checks


# ── Streamer / stream-key issuance ──────────────────────────────────────
class CreateStreamer(BaseModel):
    display_name: str


@app.post("/streamers")
def create_streamer(body: CreateStreamer):
    stream_key = secrets.token_hex(16)
    with pg_connect() as conn:
        row = conn.execute(
            """
            INSERT INTO streamers (display_name, stream_key)
            VALUES (%s, %s)
            RETURNING id, display_name, stream_key
            """,
            (body.display_name, stream_key),
        ).fetchone()
        conn.commit()
    return row


# ── Browse ───────────────────────────────────────────────────────────────
# view_events is a raw log (see postgres/init.sql) — counts are derived with
# GROUP BY here rather than kept in a running counter column.
@app.get("/streams")
def list_live_streams():
    with pg_connect() as conn:
        rows = conn.execute(
            """
            SELECT s.id, s.status, s.started_at, st.display_name,
                   COALESCE(v.view_count, 0) AS view_count
            FROM streams s
            JOIN streamers st ON st.id = s.streamer_id
            LEFT JOIN (
                SELECT content_id, COUNT(*) AS view_count
                FROM view_events WHERE content_type = 'stream'
                GROUP BY content_id
            ) v ON v.content_id = s.id
            WHERE s.status = 'live'
            ORDER BY s.started_at DESC
            """
        ).fetchall()
    return rows


@app.get("/videos")
def list_videos():
    with pg_connect() as conn:
        rows = conn.execute(
            """
            SELECT vi.id, vi.title, vi.status, vi.created_at, st.display_name,
                   COALESCE(ve.view_count, 0) AS view_count
            FROM videos vi
            JOIN streamers st ON st.id = vi.streamer_id
            LEFT JOIN (
                SELECT content_id, COUNT(*) AS view_count
                FROM view_events WHERE content_type = 'video'
                GROUP BY content_id
            ) ve ON ve.content_id = vi.id
            ORDER BY vi.created_at DESC
            """
        ).fetchall()
    return rows


# ── View tracking ─────────────────────────────────────────────────────────
# Records a "watch" click as a view event (see SPEC.md — there's no player
# yet to send finer-grained heartbeat/progress events). Kafka is the
# backbone (see SPEC.md), so this only produces the event; analytics-worker
# is what actually writes it to Postgres.
class RecordView(BaseModel):
    content_type: str
    content_id: str


@app.post("/analytics/view")
def record_view(body: RecordView):
    if body.content_type not in ("stream", "video"):
        raise HTTPException(status_code=400, detail="content_type must be 'stream' or 'video'")

    producer.produce(
        "viewer.analytics",
        key=body.content_id,
        value=json.dumps(
            {
                "event": "view",
                "content_type": body.content_type,
                "content_id": body.content_id,
                "ts": time.time(),
            }
        ),
    )
    producer.flush()
    return {"ok": True}


# ── Upload ───────────────────────────────────────────────────────────────
class _UploadTooLarge(Exception):
    pass


class _SizeLimitedReader:
    # Wraps UploadFile's underlying file object so boto3's upload_fileobj
    # (which reads directly from it, bypassing FastAPI's own body handling)
    # aborts once actual bytes read exceed the limit -- a backstop that
    # holds even if Content-Length is missing or understates the real size.
    def __init__(self, fileobj, limit: int):
        self._fileobj = fileobj
        self._limit = limit
        self._read = 0

    def read(self, size=-1):
        chunk = self._fileobj.read(size)
        self._read += len(chunk)
        if self._read > self._limit:
            raise _UploadTooLarge()
        return chunk


@app.post("/videos")
def upload_video(
    stream_key: str = Form(...),
    title: str = Form(...),
    file: UploadFile = File(...),
):
    with pg_connect() as conn:
        streamer = conn.execute(
            "SELECT id FROM streamers WHERE stream_key = %s", (stream_key,)
        ).fetchone()
        if streamer is None:
            raise HTTPException(status_code=401, detail="unknown stream key")

        recent_uploads = conn.execute(
            """
            SELECT COUNT(*) AS n FROM videos
            WHERE streamer_id = %s
              AND created_at > now() - (%s * interval '1 minute')
            """,
            (streamer["id"], UPLOAD_RATE_LIMIT_WINDOW_MINUTES),
        ).fetchone()["n"]
        if recent_uploads >= UPLOAD_RATE_LIMIT_COUNT:
            raise HTTPException(
                status_code=429,
                detail=(
                    f"upload rate limit exceeded: max {UPLOAD_RATE_LIMIT_COUNT} "
                    f"uploads per {UPLOAD_RATE_LIMIT_WINDOW_MINUTES} minutes"
                ),
            )

        video_id = uuid.uuid4()
        ext = os.path.splitext(file.filename or "")[1] or ".bin"
        object_key = f"raw/{video_id}{ext}"

        try:
            s3_client().upload_fileobj(
                _SizeLimitedReader(file.file, MAX_UPLOAD_BYTES), BUCKET, object_key
            )
        except _UploadTooLarge:
            # Confirmed empirically (both the single-put and multipart code
            # paths): boto3's upload_fileobj propagates an exception raised
            # from the Fileobj's own read() unwrapped, unlike upload_file's
            # convenience wrapper which re-raises as S3UploadFailedError.
            raise HTTPException(
                status_code=413, detail=f"file exceeds {MAX_UPLOAD_BYTES} byte limit"
            )

        video = conn.execute(
            """
            INSERT INTO videos (id, streamer_id, title, raw_object_key)
            VALUES (%s, %s, %s, %s)
            RETURNING id, title, status, raw_object_key
            """,
            (video_id, streamer["id"], title, object_key),
        ).fetchone()
        conn.commit()

    producer.produce(
        "upload.events",
        key=str(video_id),
        value=json.dumps(
            {
                "event": "uploaded",
                "video_id": str(video_id),
                "object_key": object_key,
                "ts": time.time(),
            }
        ),
    )
    producer.flush()

    return video


# TODO (JIT, see SPEC.md): playback endpoints (serve/redirect to HLS
# manifests once transcode-worker actually produces them).
