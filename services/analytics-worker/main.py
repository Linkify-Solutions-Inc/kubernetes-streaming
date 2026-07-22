import json
import logging
import os

import psycopg
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("analytics-worker")

TOPICS = ["viewer.analytics", "transcode.status"]


def pg_connect():
    return psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=3)


def handle_viewer_analytics(data: dict) -> None:
    if data.get("event") != "view":
        return
    content_type = data.get("content_type")
    content_id = data.get("content_id")
    if content_type not in ("stream", "video") or not content_id:
        log.error("malformed view event: %r", data)
        return

    with pg_connect() as conn:
        conn.execute(
            "INSERT INTO view_events (content_type, content_id) VALUES (%s, %s)",
            (content_type, content_id),
        )
        conn.commit()
    log.info("recorded view: %s %s", content_type, content_id)


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "analytics-worker",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)
    log.info("analytics-worker up, watching %s", TOPICS)

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
            except (ValueError, TypeError):
                log.error("unparseable message on %s: %r", msg.topic(), msg.value())
                continue

            if msg.topic() == "viewer.analytics":
                handle_viewer_analytics(data)
            else:
                # transcode.status: no aggregation needed yet — videos.status
                # is already updated directly by transcode-worker. Just log.
                log.info("[%s] %s", msg.topic(), data)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
