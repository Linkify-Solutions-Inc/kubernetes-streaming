import json
import logging
import time

import psycopg
from config import require, seal
from confluent_kafka import Producer
from fastapi import FastAPI, Form, Response
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingest-webhook")

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = require("KAFKA_BOOTSTRAP_SERVERS")
POSTGRES_DSN = require("POSTGRES_DSN")
# Read raw here and converted below, after seal() -- so a missing value is
# reported alongside every other missing var instead of raising ValueError
# from int("") before seal() gets a chance to report anything.
_MAX_CONCURRENT_LIVE_RAW = require("MAX_CONCURRENT_LIVE_STREAMS")
seal()

# The circuit breaker on Karpenter's blast radius (see
# docs/aws/14-keda-scaledjobs.md) -- lives in the ConfigMap, not a code
# constant, so an operator can raise it for an event without a rebuild.
MAX_CONCURRENT_LIVE = int(_MAX_CONCURRENT_LIVE_RAW)

# enable.idempotence: a produce-request retry (broker restart, network blip)
# would otherwise risk landing the same "started"/"ended" event twice.
# message.timeout.ms: without it, a Kafka outage makes producer.flush() block
# for the 300s default, holding a uvicorn worker and stalling MediaMTX's
# webhook call -- a stream failure caused by a monitoring dependency being down.
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "enable.idempotence": True,
    "message.timeout.ms": 10000,
})


def pg_connect():
    return psycopg.connect(POSTGRES_DSN, connect_timeout=3)


def emit_lifecycle_event(event: str, path: str, **extra) -> None:
    payload = json.dumps({"event": event, "path": path, "ts": time.time(), **extra})
    # Keyed by stream path so per-stream ordering holds as partitions scale
    # (see SPEC.md / research_validated_architecture memory).
    producer.produce("stream.lifecycle", key=path, value=payload)
    producer.flush()


@app.get("/health")
def health():
    return {"status": "ok"}


# Alias for the k8s probes (see docs/aws/11-workloads.md). Deliberately
# trivial and used for liveness, readiness AND startup here -- MediaMTX's
# authHTTPAddress is a hard gate, so a DB-dependent readiness check would
# turn a 30-second RDS blip into a total ingest outage (zero Ready pods,
# nobody on the platform can go live) instead of the honest per-request 500.
@app.get("/livez")
def livez():
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

        # streams_live_idx (postgres/migrations/002_transcode_claim.sql) makes
        # this a sub-millisecond index-only scan -- it runs on every publish.
        live = conn.execute(
            "SELECT COUNT(*) FROM streams WHERE status = 'live'"
        ).fetchone()[0]

    if live >= MAX_CONCURRENT_LIVE:
        # 503, not 401: MediaMTX rejects on any non-2xx so OBS can't tell the
        # difference, but "at capacity" needs to be distinguishable from "bad
        # key" in logs and alerting. This is not the only admission gate --
        # maxReplicaCount on the live ScaledJob is the backstop for the race
        # where two publishes land in the window between this check and the
        # streams-row INSERT in on_publish (see docs/aws/14-keda-scaledjobs.md).
        # Don't "fix" that race with a lock; it's a known, accepted gap.
        log.warning("rejected publish: at capacity (%d/%d live)", live, MAX_CONCURRENT_LIVE)
        response.status_code = 503
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
    # Separate, "started"-only KEDA trigger topic (see
    # docs/aws/14-keda-scaledjobs.md): KEDA's Kafka scaler can't inspect
    # message contents, so pointing it at stream.lifecycle would spawn a
    # transcode Job for "ended" events too. Same key (path) so per-stream
    # ordering holds. stream.lifecycle keeps carrying "started" as well --
    # that's the event log analytics-worker reads and the live pod's own
    # 'ended' watcher needs it populated.
    producer.produce(
        "stream.start.requests",
        key=path,
        value=json.dumps({"event": "started", "path": path, "stream_id": str(stream_id), "ts": time.time()}),
    )
    producer.flush()
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
