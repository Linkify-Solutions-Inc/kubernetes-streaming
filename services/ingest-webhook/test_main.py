import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "test:9092")
os.environ.setdefault("POSTGRES_DSN", "postgresql://test")
os.environ.setdefault("MAX_CONCURRENT_LIVE_STREAMS", "8")

import main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(main.app)


def _fake_conn(*fetchone_returns):
    # main.py uses `with pg_connect() as conn: conn.execute(...).fetchone()`,
    # possibly more than once per request (e.g. on_auth does a streamer
    # lookup THEN a live-count query) -- psycopg connections are their own
    # context manager, so __enter__ must return self, and each execute()
    # call needs its own fetchone() result, in call order.
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=r)) for r in fetchone_returns
    ]
    return conn


def test_non_publish_actions_are_always_allowed_without_a_db_lookup():
    with patch.object(main, "pg_connect") as pg_connect:
        resp = client.post("/hooks/auth", json={"action": "read", "path": "whatever"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    pg_connect.assert_not_called()


def test_publish_with_unknown_stream_key_is_rejected_before_the_capacity_check():
    conn = _fake_conn(None)
    with patch.object(main, "pg_connect", return_value=conn):
        resp = client.post("/hooks/auth", json={"action": "publish", "path": "not-a-real-key"})
    assert resp.status_code == 401
    assert resp.json() == {"ok": False}
    # Short-circuited: no COUNT(*) query once the stream key itself is unknown.
    assert conn.execute.call_count == 1


def test_publish_under_capacity_is_accepted():
    conn = _fake_conn(("some-streamer-id",), (main.MAX_CONCURRENT_LIVE - 1,))
    with patch.object(main, "pg_connect", return_value=conn):
        resp = client.post("/hooks/auth", json={"action": "publish", "path": "a-real-stream-key"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_publish_at_capacity_is_rejected_with_503_not_401():
    conn = _fake_conn(("some-streamer-id",), (main.MAX_CONCURRENT_LIVE,))
    with patch.object(main, "pg_connect", return_value=conn):
        resp = client.post("/hooks/auth", json={"action": "publish", "path": "a-real-stream-key"})
    # 503 ("at capacity"), not 401 ("bad key") -- MediaMTX can't tell the
    # difference, but logs/alerting need to.
    assert resp.status_code == 503
    assert resp.json() == {"ok": False}


def test_publish_hook_produces_to_both_lifecycle_and_trigger_topics():
    conn = _fake_conn(("streamer-id",), None, ("stream-id",))
    with patch.object(main, "pg_connect", return_value=conn), \
         patch.object(main, "producer") as producer:
        resp = client.post("/hooks/publish", data={"path": "some-path"})
    assert resp.status_code == 200
    topics = [call.args[0] for call in producer.produce.call_args_list]
    # stream.lifecycle: the event log analytics-worker reads, and what the
    # live pod's own 'ended' watcher tails. stream.start.requests: the
    # KEDA trigger topic, "started"-only so the scaler never sees "ended".
    assert topics == ["stream.lifecycle", "stream.start.requests"]
    assert producer.flush.call_count == 2
