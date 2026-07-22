import json
import os
import secrets
import time
import uuid

import boto3
import psycopg
from botocore.config import Config
from confluent_kafka import Producer
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from psycopg.rows import dict_row
from pydantic import BaseModel

app = FastAPI()
producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})


def pg_connect():
    return psycopg.connect(
        os.environ["POSTGRES_DSN"], connect_timeout=3, row_factory=dict_row
    )


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
        s3_client().head_bucket(Bucket="media")
        checks["minio"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["minio"] = f"error: {exc}"

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

        video_id = uuid.uuid4()
        ext = os.path.splitext(file.filename or "")[1] or ".bin"
        object_key = f"raw/{video_id}{ext}"

        s3_client().upload_fileobj(file.file, "media", object_key)

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
