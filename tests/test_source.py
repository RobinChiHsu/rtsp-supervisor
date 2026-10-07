import pytest

from rtsp_supervisor import redact_url


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("rtsp://admin:hunter2@10.0.0.5:554/live", "rtsp://admin:***@10.0.0.5:554/live"),
        ("rtsp://admin:p@ss@cam.local/stream?ch=1", "rtsp://admin:***@cam.local/stream?ch=1"),
        ("rtsp://user:secret@[fe80::1]:8554/a", "rtsp://user:***@[fe80::1]:8554/a"),
        ("rtsp://viewer@10.0.0.5/live", "rtsp://viewer@10.0.0.5/live"),
        ("rtsp://10.0.0.5/live", "rtsp://10.0.0.5/live"),
        ("/var/videos/clip.mp4", "/var/videos/clip.mp4"),
    ],
)
def test_redact_url_hides_only_the_password(url: str, expected: str) -> None:
    assert redact_url(url) == expected
