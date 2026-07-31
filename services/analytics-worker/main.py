import http.server
import json
import logging
import threading
import time

import psycopg
from config import require, seal
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("analytics-worker")

TOPICS = ["viewer.analytics", "transcode.status"]

KAFKA_BOOTSTRAP_SERVERS = require("KAFKA_BOOTSTRAP_SERVERS")
POSTGRES_DSN = require("POSTGRES_DSN")
seal()

# This process has no HTTP API of its own -- this thread exists solely to
# give Kubernetes something to probe. A Kafka consumer that quietly stops
# polling (rebalance storm, broker gone, an exception swallowed in a loop)
# is a real failure mode a probe-less Deployment would never notice. See
# docs/aws/11-workloads.md.
LAST_POLL = time.time()


class _HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if time.time() - LAST_POLL < 60 else 503)
        self.end_headers()

    def log_message(self, *args):
        pass


threading.Thread(
    target=lambda: http.server.HTTPServer(("", 8000), _HealthHandler).serve_forever(),
    daemon=True,
).start()


def pg_connect():
    return psycopg.connect(POSTGRES_DSN, connect_timeout=3)


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
    global LAST_POLL
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "analytics-worker",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)
    log.info("analytics-worker up, watching %s", TOPICS)

    try:
        while True:
            msg = consumer.poll(1.0)
            LAST_POLL = time.time()
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
