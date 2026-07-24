import json
import logging
import os
import time

import psycopg
from confluent_kafka import Producer
from fastapi import FastAPI, Form, Response
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest-webhook")

app = FastAPI()
# enable.idempotence: a produce-request retry (broker restart, network blip)
# would otherwise risk landing the same "started"/"ended" event twice.
producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "enable.idempotence": True,
})


def pg_connect():
    return psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=3)


def emit_lifecycle_event(event: str, path: str, **extra) -> None:
    payload = json.dumps({"event": event, "path": path, "ts": time.time(), **extra})
    # Keyed by stream path so per-stream ordering holds as partitions scale
    # (see SPEC.md / research_validated_architecture memory).
    producer.produce("stream.lifecycle", key=path, value=payload)
    producer.flush()


@app.get("/health")
def health():
    return {"status": "ok"}


# ── MediaMTX external HTTP auth (authHTTPAddress) ───────────────────────
# Called BEFORE a stream is accepted. Payload fields confirmed against
# MediaMTX's own source (internal/auth) — this is the real pre-publish
# gate; runOnReady below fires only *after* a stream has already been
# accepted, so it can't reject anything.
class MediaMTXAuthRequest(BaseModel):
    ip: str | None = None
    user: str | None = None
    password: str | None = None
    token: str | None = None
    action: str | None = None
    path: str | None = None
    protocol: str | None = None
    id: str | None = None
    query: str | None = None
    userAgent: str | None = None


@app.post("/hooks/auth")
def on_auth(req: MediaMTXAuthRequest, response: Response):
    if req.action != "publish":
        # Viewers stay anonymous (see SPEC.md) — allow all non-publish actions.
        return {"ok": True}

    with pg_connect() as conn:
        row = conn.execute(
            "SELECT id FROM streamers WHERE stream_key = %s", (req.path,)
        ).fetchone()

    if row is None:
        log.info("rejected publish: unknown stream key %r", req.path)
        response.status_code = 401
        return {"ok": False}

    return {"ok": True}


# ── MediaMTX runOnReady/runOnNotReady hooks ─────────────────────────────
# Fire once a stream is actually readable / stops being readable. By the
# time these run, /hooks/auth has already accepted the stream key, so no
# re-validation needed here — just record the session and emit the event.
@app.post("/hooks/publish")
def on_publish(path: str = Form(...)):
    with pg_connect() as conn:
        streamer = conn.execute(
            "SELECT id FROM streamers WHERE stream_key = %s", (path,)
        ).fetchone()
        if streamer is None:
            # Shouldn't happen (auth already gated this), but don't crash the hook.
            log.error("publish hook for unknown stream key %r", path)
            return {"ok": False}

        # MediaMTX can call runOnReady more than once for the same publish
        # session (e.g. it retries on a slow/lost webhook response). Without
        # this guard a duplicate call would insert a second "live" streams
        # row and emit a second "started" event, kicking off a duplicate
        # transcode downstream (see SPEC.md Phase 1 hardening).
        existing = conn.execute(
            "SELECT id FROM streams WHERE streamer_id = %s AND status = 'live'",
            (streamer[0],),
        ).fetchone()
        if existing is not None:
            log.warning(
                "duplicate publish hook for path=%s, stream_id=%s already live",
                path, existing[0],
            )
            return {"ok": True}

        stream_id = conn.execute(
            "INSERT INTO streams (streamer_id, path) VALUES (%s, %s) RETURNING id",
            (streamer[0], path),
        ).fetchone()[0]
        conn.commit()

    log.info("stream started: path=%s stream_id=%s", path, stream_id)
    emit_lifecycle_event("started", path, stream_id=str(stream_id))
    return {"ok": True}


@app.post("/hooks/unpublish")
def on_unpublish(path: str = Form(...)):
    with pg_connect() as conn:
        row = conn.execute(
            """
            UPDATE streams SET status = 'ended', ended_at = now()
            WHERE streamer_id = (SELECT id FROM streamers WHERE stream_key = %s)
              AND status = 'live'
            RETURNING id
            """,
            (path,),
        ).fetchone()
        conn.commit()

    stream_id = row[0] if row else None
    log.info("stream ended: path=%s stream_id=%s", path, stream_id)
    emit_lifecycle_event("ended", path, stream_id=str(stream_id) if stream_id else None)
    return {"ok": True}
