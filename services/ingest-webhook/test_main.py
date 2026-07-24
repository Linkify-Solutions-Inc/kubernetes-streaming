import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "test:9092")
os.environ.setdefault("POSTGRES_DSN", "postgresql://test")

import main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(main.app)


def _fake_conn(fetchone_return):
    # main.py uses `with pg_connect() as conn: conn.execute(...).fetchone()`
    # -- psycopg connections are their own context manager, so the mock
    # needs __enter__ to return itself, same as the real thing.
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.execute.return_value.fetchone.return_value = fetchone_return
    return conn


def test_non_publish_actions_are_always_allowed_without_a_db_lookup():
    with patch.object(main, "pg_connect") as pg_connect:
        resp = client.post("/hooks/auth", json={"action": "read", "path": "whatever"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    pg_connect.assert_not_called()


def test_publish_with_known_stream_key_is_accepted():
    with patch.object(main, "pg_connect", return_value=_fake_conn(("some-streamer-id",))):
        resp = client.post("/hooks/auth", json={"action": "publish", "path": "a-real-stream-key"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_publish_with_unknown_stream_key_is_rejected():
    with patch.object(main, "pg_connect", return_value=_fake_conn(None)):
        resp = client.post("/hooks/auth", json={"action": "publish", "path": "not-a-real-key"})
    assert resp.status_code == 401
    assert resp.json() == {"ok": False}
