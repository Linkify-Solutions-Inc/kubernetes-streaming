import os

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "test:9092")

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
