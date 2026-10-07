import asyncio

import pytest

from rtsp_supervisor import BufferClosed, Frame, LatestFrameBuffer


def make_frame(sequence: int) -> Frame[str]:
    return Frame(data=f"frame-{sequence}", sequence=sequence, captured_at=float(sequence))


async def test_get_returns_frame_that_was_put() -> None:
    buffer: LatestFrameBuffer[str] = LatestFrameBuffer()
    buffer.put(make_frame(1))

    assert await buffer.get() == make_frame(1)


async def test_newer_frame_replaces_unread_frame() -> None:
    buffer: LatestFrameBuffer[str] = LatestFrameBuffer()
    buffer.put(make_frame(1))
    buffer.put(make_frame(2))
    buffer.put(make_frame(3))

    assert await buffer.get() == make_frame(3)
    assert buffer.dropped == 2


async def test_frame_is_delivered_only_once() -> None:
    buffer: LatestFrameBuffer[str] = LatestFrameBuffer()
    buffer.put(make_frame(1))
    await buffer.get()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await buffer.get()


async def test_waiting_consumer_wakes_on_put() -> None:
    buffer: LatestFrameBuffer[str] = LatestFrameBuffer()
    waiter = asyncio.create_task(buffer.get())
    await asyncio.sleep(0)

    buffer.put(make_frame(7))

    assert await waiter == make_frame(7)
    assert buffer.dropped == 0


async def test_close_wakes_waiting_consumer() -> None:
    buffer: LatestFrameBuffer[str] = LatestFrameBuffer()
    waiter = asyncio.create_task(buffer.get())
    await asyncio.sleep(0)

    buffer.close()

    with pytest.raises(BufferClosed):
        await waiter


async def test_pending_frame_is_drained_after_close() -> None:
    buffer: LatestFrameBuffer[str] = LatestFrameBuffer()
    buffer.put(make_frame(1))
    buffer.close()

    assert await buffer.get() == make_frame(1)
    with pytest.raises(BufferClosed):
        await buffer.get()


def test_put_after_close_is_rejected() -> None:
    buffer: LatestFrameBuffer[str] = LatestFrameBuffer()
    buffer.close()

    assert buffer.closed
    with pytest.raises(BufferClosed):
        buffer.put(make_frame(1))
