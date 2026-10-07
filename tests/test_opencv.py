from pathlib import Path

import cv2
import numpy as np
import pytest

from rtsp_supervisor import SourceError, StreamSupervisor, SupervisorConfig
from rtsp_supervisor.opencv import OpenCVSource
from tests.fakes import wait_until

WIDTH, HEIGHT = 32, 24


@pytest.fixture
def clip(tmp_path: Path) -> Path:
    path = tmp_path / "clip.avi"
    writer = cv2.VideoWriter(str(path), cv2.VideoWriter.fourcc(*"MJPG"), 10.0, (WIDTH, HEIGHT))
    for shade in (0, 120, 240):
        writer.write(np.full((HEIGHT, WIDTH, 3), shade, dtype=np.uint8))
    writer.release()
    return path


def test_reads_every_frame_then_reports_end_of_stream(clip: Path) -> None:
    source = OpenCVSource(str(clip))
    source.open()
    try:
        frames = [source.read() for _ in range(3)]
        with pytest.raises(SourceError, match="no frame"):
            source.read()
    finally:
        source.close()

    assert [frame.shape for frame in frames] == [(HEIGHT, WIDTH, 3)] * 3
    assert frames[0].mean() < frames[2].mean()


def test_open_failure_raises_source_error(tmp_path: Path) -> None:
    source = OpenCVSource(str(tmp_path / "missing.avi"))

    with pytest.raises(SourceError, match="cannot open"):
        source.open()


def test_read_before_open_raises() -> None:
    with pytest.raises(SourceError, match="not open"):
        OpenCVSource("rtsp://camera.local/live").read()


def test_close_is_idempotent(clip: Path) -> None:
    source = OpenCVSource(str(clip))
    source.open()

    source.close()
    source.close()

    with pytest.raises(SourceError, match="not open"):
        source.read()


def test_error_messages_never_contain_password(tmp_path: Path) -> None:
    source = OpenCVSource(f"file://admin:hunter2@localhost{tmp_path}/missing.avi")

    with pytest.raises(SourceError) as error:
        source.open()

    assert "hunter2" not in str(error.value)
    assert "admin:***@" in str(error.value)


async def test_supervisor_streams_a_real_video_file(clip: Path) -> None:
    supervisor = StreamSupervisor(
        lambda: OpenCVSource(str(clip)), config=SupervisorConfig(read_timeout=2.0)
    )

    async with supervisor:
        await wait_until(lambda: supervisor.stats.frames_read >= 3)
        frame = await supervisor.next_frame()

    assert frame.data.shape == (HEIGHT, WIDTH, 3)
