from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

import pytest

from rtsp_supervisor import (
    ExponentialBackoff,
    FrameSource,
    SourceError,
    StreamClosed,
    StreamState,
    StreamSupervisor,
    SupervisorConfig,
)
from tests.fakes import BlockingSource, FakeClock, FakeSource, ScriptedFactory, wait_until


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make_supervisor(
    factory: Callable[[], FrameSource[int]],
    clock: FakeClock,
    *,
    on_state_change: Callable[[StreamState, StreamState], None] | None = None,
    **config: float,
) -> StreamSupervisor[int]:
    return StreamSupervisor(
        factory,
        clock=clock,
        backoff=ExponentialBackoff(initial=1.0, multiplier=2.0, maximum=8.0, jitter=0.0),
        config=SupervisorConfig(**config),
        on_state_change=on_state_change,
    )


async def test_consumer_receives_published_frame(clock: FakeClock) -> None:
    async with make_supervisor(ScriptedFactory(FakeSource(frames=1)), clock) as supervisor:
        frame = await supervisor.next_frame()

    assert frame.data == 1
    assert frame.sequence == 1


async def test_slow_consumer_gets_latest_frame(clock: FakeClock) -> None:
    async with make_supervisor(ScriptedFactory(FakeSource(frames=3)), clock) as supervisor:
        await wait_until(lambda: supervisor.stats.frames_read == 3)
        frame = await supervisor.next_frame()

        assert frame.data == 3
        assert supervisor.stats.frames_dropped == 2


async def test_reconnects_after_stream_ends(clock: FakeClock) -> None:
    factory = ScriptedFactory(FakeSource(frames=2), FakeSource(frames=2))

    async with make_supervisor(factory, clock) as supervisor:
        await wait_until(lambda: supervisor.stats.frames_read == 4)

        assert factory.created[0].closed
        assert supervisor.stats.reconnect_attempts >= 1
        assert clock.sleeps[0] == 1.0


async def test_backoff_grows_while_open_keeps_failing(clock: FakeClock) -> None:
    failing = [FakeSource(open_error=SourceError("refused")) for _ in range(3)]
    factory = ScriptedFactory(*failing)

    async with make_supervisor(factory, clock) as supervisor:
        await wait_until(
            lambda: len(factory.created) == 4 and supervisor.state is StreamState.STREAMING
        )

        assert clock.sleeps == [1.0, 2.0, 4.0]
        assert all(source.closed for source in failing)
        assert supervisor.stats.last_error == "open failed: refused"


async def test_backoff_resets_after_stable_streaming(clock: FakeClock) -> None:
    factory = ScriptedFactory(
        FakeSource(open_error=SourceError("refused")),
        FakeSource(frames=3, on_read=lambda: clock.advance(5.0)),
    )

    async with make_supervisor(factory, clock, stable_after=10.0) as supervisor:
        await wait_until(
            lambda: len(factory.created) == 3 and supervisor.state is StreamState.STREAMING
        )

        assert clock.sleeps == [1.0, 1.0]


async def test_short_lived_stream_does_not_reset_backoff(clock: FakeClock) -> None:
    factory = ScriptedFactory(
        FakeSource(open_error=SourceError("refused")),
        FakeSource(frames=3, on_read=lambda: clock.advance(1.0)),
    )

    async with make_supervisor(factory, clock, stable_after=10.0) as supervisor:
        await wait_until(
            lambda: len(factory.created) == 3 and supervisor.state is StreamState.STREAMING
        )

        assert clock.sleeps == [1.0, 2.0]


async def test_stalled_read_times_out_and_reconnects(
    clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    factory = ScriptedFactory(BlockingSource(), FakeSource(frames=1))

    with caplog.at_level(logging.WARNING):
        async with make_supervisor(factory, clock, read_timeout=0.05) as supervisor:
            await wait_until(lambda: supervisor.stats.frames_read == 1)

    assert factory.created[0].closed
    assert "read failed: TimeoutError" in caplog.text


async def test_factory_exception_is_retried(clock: FakeClock) -> None:
    calls = 0

    def factory() -> FrameSource[int]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("bad config")
        return BlockingSource(frames=1)

    async with make_supervisor(factory, clock) as supervisor:
        await wait_until(lambda: supervisor.stats.frames_read == 1)

        assert clock.sleeps == [1.0]


async def test_close_failure_does_not_stop_supervisor(
    clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    factory = ScriptedFactory(FakeSource(frames=1, close_error=OSError("device busy")))

    with caplog.at_level(logging.WARNING):
        async with make_supervisor(factory, clock):
            await wait_until(lambda: len(factory.created) == 2)

    assert "device busy" in caplog.text


async def test_reports_state_transitions(clock: FakeClock) -> None:
    transitions: list[tuple[StreamState, StreamState]] = []
    supervisor = make_supervisor(
        ScriptedFactory(FakeSource(frames=1)),
        clock,
        on_state_change=lambda old, new: transitions.append((old, new)),
    )

    async with supervisor:
        await wait_until(lambda: len(transitions) == 5)

    assert transitions == [
        (StreamState.IDLE, StreamState.CONNECTING),
        (StreamState.CONNECTING, StreamState.STREAMING),
        (StreamState.STREAMING, StreamState.BACKOFF),
        (StreamState.BACKOFF, StreamState.CONNECTING),
        (StreamState.CONNECTING, StreamState.STREAMING),
        (StreamState.STREAMING, StreamState.STOPPED),
    ]


async def test_stop_closes_source_and_ends_iteration(clock: FakeClock) -> None:
    factory = ScriptedFactory(BlockingSource(frames=1))
    supervisor = make_supervisor(factory, clock)
    received: list[int] = []

    async def consume() -> None:
        async for frame in supervisor.frames():
            received.append(frame.data)

    supervisor.start()
    consumer = asyncio.create_task(consume())
    await wait_until(lambda: received == [0])

    await supervisor.stop()
    await asyncio.wait_for(consumer, timeout=1.0)

    assert factory.created[0].closed
    assert supervisor.state is StreamState.STOPPED
    with pytest.raises(StreamClosed):
        await supervisor.next_frame()


async def test_health_tracks_frame_staleness(clock: FakeClock) -> None:
    supervisor = make_supervisor(ScriptedFactory(BlockingSource(frames=1)), clock)
    assert not supervisor.is_healthy(max_staleness=1.0)

    async with supervisor:
        await wait_until(lambda: supervisor.stats.frames_read == 1)
        assert supervisor.is_healthy(max_staleness=1.0)

        clock.advance(2.0)
        assert not supervisor.is_healthy(max_staleness=1.0)


async def test_cannot_start_twice(clock: FakeClock) -> None:
    async with make_supervisor(ScriptedFactory(), clock) as supervisor:
        with pytest.raises(RuntimeError):
            supervisor.start()


async def test_stop_before_start_is_noop(clock: FakeClock) -> None:
    supervisor = make_supervisor(ScriptedFactory(), clock)

    await supervisor.stop()

    assert supervisor.state is StreamState.IDLE


async def test_stop_preserves_cancellation_of_caller(clock: FakeClock) -> None:
    supervisor = make_supervisor(ScriptedFactory(), clock)
    supervisor.start()
    await wait_until(lambda: supervisor.state is StreamState.STREAMING)

    stopper = asyncio.create_task(supervisor.stop())
    await asyncio.sleep(0)
    stopper.cancel()

    with pytest.raises(asyncio.CancelledError):
        await stopper
    await supervisor.stop()


@pytest.mark.parametrize("field", ["open_timeout", "read_timeout", "close_timeout", "stable_after"])
def test_config_rejects_non_positive_values(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        SupervisorConfig(**{field: 0.0})
