from pathlib import Path


def test_video_viewer_functions_prefer_av1_then_hevc_then_h264_hls():
    repo_root = Path(__file__).resolve().parents[2]
    functions_js = (repo_root / "frontend/src/static/js/components/media-viewer/VideoViewer/functions.js").read_text()

    av1_pos = functions_js.index("addPreferredHlsData(av1HlsData)")
    hevc_pos = functions_js.index("addPreferredHlsData(hevcHlsData)")
    h264_pos = functions_js.index("addPreferredHlsData(hlsData)")

    assert av1_pos < hevc_pos < h264_pos
    assert "supportedFormats.support.av1" in functions_js
    assert "supportedFormats.support.h265" in functions_js
    assert "if (ret[k] && -1 < ret[k].format.indexOf('hls'))" in functions_js


def test_main_video_viewer_does_not_add_direct_mp4_when_hls_exists_for_selected_quality():
    repo_root = Path(__file__).resolve().parents[2]
    video_viewer_js = (repo_root / "frontend/src/static/js/components/media-viewer/VideoViewer/index.js").read_text()

    assert "let selectedResolutionHasHls = false" in video_viewer_js
    assert "selectedResolutionHasHls = true" in video_viewer_js
    assert "if (!selectedResolutionHasHls)" in video_viewer_js


def test_page_link_video_player_does_not_add_direct_mp4_when_hls_exists_for_selected_quality():
    repo_root = Path(__file__).resolve().parents[2]
    by_page_link_js = (repo_root / "frontend/src/static/js/components/video-player/VideoPlayerByPageLink.jsx").read_text()

    assert "let selectedResolutionHasHls = false" in by_page_link_js
    assert "selectedResolutionHasHls = true" in by_page_link_js
    assert "if (!selectedResolutionHasHls)" in by_page_link_js