"""Re-queue transcode claims abandoned by a pod that died after committing
its Kafka offset but before finishing the work (spot reclaim, node
consolidation, OOMKill, ephemeral-storage eviction). See
docs/aws/14-keda-scaledjobs.md.

Runs as its own CronJob container (`command: ["python", "sweeper.py"]`), not
imported by main.py -- main.py's module-level require() calls expect
TRANSCODE_MODE/TRANSCODE_CONSUMER_GROUP/TRANSCODE_TRIGGER_TOPIC/POD_NAME,
none of which the sweeper's pod spec sets.
"""
import json
import logging
import os
import time

import psycopg
from config import require, seal
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("transcode-sweeper")

KAFKA_BOOTSTRAP_SERVERS = require("KAFKA_BOOTSTRAP_SERVERS")
POSTGRES_DSN = require("POSTGRES_DSN")
seal()

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "enable.idempotence": True,
    "message.timeout.ms": 10000,
})


def pg_connect():
    return psycopg.connect(POSTGRES_DSN, connect_timeout=3)


def sweep() -> None:
    with pg_connect() as conn:
        # Live: claimed, still live, but nothing has heartbeated in a while.
        stuck_live = conn.execute(
            """
            UPDATE streams SET transcode_status = 'pending'
             WHERE status = 'live'
               AND transcode_status = 'claimed'
               AND transcode_heartbeat < now() - %s::interval
            RETURNING id, path
            """,
            (os.environ["LIVE_STALE_AFTER"],),
        ).fetchall()

        # VOD: stuck at 'transcoding' with a stale heartbeat.
        stuck_vod = conn.execute(
            """
            UPDATE videos SET status = 'uploaded'
             WHERE status = 'transcoding'
               AND transcode_heartbeat < now() - %s::interval
            RETURNING id, raw_object_key
            """,
            (os.environ["VOD_STALE_AFTER"],),
        ).fetchall()
        conn.commit()

    # UPDATE ... RETURNING runs before the produce on purpose. If the produce
    # fails, the row is back to 'pending' and the next sweep re-emits it. The
    # reverse order (produce first, then un-claim) can emit the work twice.
    for stream_id, path in stuck_live:
        log.warning("re-queueing abandoned live stream %s (path=%s)", stream_id, path)
        producer.produce("stream.start.requests", key=path, value=json.dumps(
            {"event": "started", "path": path,
             "stream_id": str(stream_id), "ts": time.time()}))
    for video_id, object_key in stuck_vod:
        log.warning("re-queueing abandoned VOD transcode %s", video_id)
        producer.produce("upload.events", key=str(video_id), value=json.dumps(
            {"event": "uploaded", "video_id": str(video_id),
             "object_key": object_key, "ts": time.time()}))
    producer.flush()


if __name__ == "__main__":
    sweep()
