import json
import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "test:9092")
os.environ.setdefault("POSTGRES_DSN", "postgresql://test")
os.environ.setdefault("TRANSCODE_MODE", "live")
os.environ.setdefault("TRANSCODE_CONSUMER_GROUP", "transcode-live")
os.environ.setdefault("TRANSCODE_TRIGGER_TOPIC", "stream.start.requests")
os.environ.setdefault("POD_NAME", "test-pod")

import main  # noqa: E402 -- env vars above must be set before this import


# Regression coverage for two real bugs found via live debugging (see
# SPEC.md): "event" playlists never dropped segments (unbounded growth,
# fresh viewers always started at segment 0), and hls_list_size defaulted
# too small for a usable DVR window.
def test_live_omits_playlist_type_for_a_sliding_window():
    cmd = main.build_ffmpeg_command("rtmp://x/y", "/tmp/out", live=True)
    assert "-hls_playlist_type" not in cmd
    assert "-hls_list_size" in cmd
    idx = cmd.index("-hls_list_size")
    assert cmd[idx + 1] == str(main.LIVE_LIST_SIZE)


def test_vod_finalizes_playlist():
    cmd = main.build_ffmpeg_command("/tmp/in.mp4", "/tmp/out", live=False)
    idx = cmd.index("-hls_playlist_type")
    assert cmd[idx + 1] == "vod"
    assert "-hls_list_size" not in cmd


def test_live_hls_flags_delete_old_segments():
    cmd = main.build_ffmpeg_command("rtmp://x/y", "/tmp/out", live=True)
    idx = cmd.index("-hls_flags")
    assert "delete_segments" in cmd[idx + 1]


def test_vod_hls_flags_keep_all_segments():
    cmd = main.build_ffmpeg_command("/tmp/in.mp4", "/tmp/out", live=False)
    idx = cmd.index("-hls_flags")
    assert "delete_segments" not in cmd[idx + 1]


def test_live_uses_veryfast_preset_for_every_rendition():
    cmd = main.build_ffmpeg_command("rtmp://x/y", "/tmp/out", live=True)
    for i in range(len(main.RENDITIONS)):
        idx = cmd.index(f"-preset:v:{i}")
        assert cmd[idx + 1] == "veryfast"


def test_vod_uses_medium_preset():
    cmd = main.build_ffmpeg_command("/tmp/in.mp4", "/tmp/out", live=False)
    idx = cmd.index("-preset:v:0")
    assert cmd[idx + 1] == "medium"


# GOP/keyframe alignment across renditions is required for HLS ABR
# switching (see SPEC.md) -- -g and -keyint_min must match exactly.
def test_gop_alignment():
    cmd = main.build_ffmpeg_command("rtmp://x/y", "/tmp/out", live=True)
    g_idx = cmd.index("-g")
    keyint_idx = cmd.index("-keyint_min")
    expected = str(main.OUTPUT_FPS * main.GOP_SECONDS)
    assert cmd[g_idx + 1] == expected
    assert cmd[keyint_idx + 1] == expected


# ── KEDA claim protocol (see docs/aws/14-keda-scaledjobs.md) ────────────
def _fake_message(payload):
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps(payload).encode()
    return msg


def test_claim_one_commits_even_when_the_claim_is_lost():
    # A lost claim still means the message has an owner -- not committing it
    # would mean infinite redelivery and infinite Job creation.
    msg = _fake_message({"stream_id": "abc"})
    served = {"done": False}

    def poll(_timeout):
        if served["done"]:
            return None
        served["done"] = True
        return msg

    fake_consumer = MagicMock()
    fake_consumer.poll.side_effect = poll
    with patch.object(main, "Consumer", return_value=fake_consumer), \
         patch.object(main, "CLAIM_DEADLINE_S", 0.05):
        result = main.claim_one("topic", "group", lambda data: False)
    assert result is None
    fake_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)
    fake_consumer.close.assert_called_once()


def test_claim_one_returns_none_at_deadline_with_no_messages():
    fake_consumer = MagicMock()
    fake_consumer.poll.return_value = None
    with patch.object(main, "Consumer", return_value=fake_consumer), \
         patch.object(main, "CLAIM_DEADLINE_S", 0.05):
        result = main.claim_one("topic", "group", lambda data: True)
    assert result is None
    fake_consumer.commit.assert_not_called()


def test_claim_one_returns_the_payload_on_a_won_claim():
    msg = _fake_message({"stream_id": "abc"})
    fake_consumer = MagicMock()
    fake_consumer.poll.return_value = msg
    with patch.object(main, "Consumer", return_value=fake_consumer), \
         patch.object(main, "CLAIM_DEADLINE_S", 5):
        result = main.claim_one("topic", "group", lambda data: True)
    assert result == {"stream_id": "abc"}
    fake_consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


def test_claim_stream_is_a_no_op_on_a_second_call():
    # First caller wins the claim (row still 'pending'); a second, redelivered
    # or racing caller finds the row already 'claimed' and gets nothing back.
    won_conn = MagicMock()
    won_conn.__enter__.return_value = won_conn
    won_conn.__exit__.return_value = False
    won_conn.execute.return_value.fetchone.return_value = ("some-path",)

    lost_conn = MagicMock()
    lost_conn.__enter__.return_value = lost_conn
    lost_conn.__exit__.return_value = False
    lost_conn.execute.return_value.fetchone.return_value = None

    with patch.object(main, "pg_connect", side_effect=[won_conn, lost_conn]):
        first = main._claim_stream("stream-id")
        second = main._claim_stream("stream-id")
    assert first == "some-path"
    assert second is None


def test_watch_for_end_matches_only_its_own_path():
    fake_consumer = MagicMock()
    fake_consumer.list_topics.return_value.topics = {
        "stream.lifecycle": MagicMock(partitions={0: MagicMock()})
    }
    other_tp = MagicMock(offset=5)
    fake_consumer.offsets_for_times.return_value = [other_tp]

    other_streams_end = _fake_message({"path": "someone-else", "event": "ended"})
    my_stream_live = _fake_message({"path": "my-path", "event": "started"})
    my_stream_end = _fake_message({"path": "my-path", "event": "ended"})
    fake_consumer.poll.side_effect = [other_streams_end, my_stream_live, my_stream_end]

    stop_event = main.threading.Event()
    with patch.object(main, "Consumer", return_value=fake_consumer):
        main.watch_for_end("my-path", stop_event, main.time.time())

    assert stop_event.is_set()
    assert fake_consumer.poll.call_count == 3
    fake_consumer.close.assert_called_once()
