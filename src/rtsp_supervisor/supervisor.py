from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, fields
from types import TracebackType
from typing import Generic, TypeVar

from .backoff import ExponentialBackoff
from .buffer import BufferClosed, LatestFrameBuffer
from .clock import Clock, SystemClock
from .frame import Frame
from .source import FrameSource
from .stats import StreamState, StreamStats

T = TypeVar("T")

StateListener = Callable[[StreamState, StreamState], None]

logger = logging.getLogger(__name__)


class StreamClosed(Exception):
    pass


@dataclass(frozen=True, slots=True)
class SupervisorConfig:
    open_timeout: float = 10.0
    read_timeout: float = 5.0
    close_timeout: float = 2.0
    stable_after: float = 10.0

    def __post_init__(self) -> None:
        for item in fields(self):
            if getattr(self, item.name) <= 0:
                raise ValueError(f"{item.name} must be positive")


class StreamSupervisor(Generic[T]):
    def __init__(
        self,
        source_factory: Callable[[], FrameSource[T]],
        *,
        config: SupervisorConfig | None = None,
        backoff: ExponentialBackoff | None = None,
        clock: Clock | None = None,
        on_state_change: StateListener | None = None,
    ) -> None:
        self._factory = source_factory
        self._config = config or SupervisorConfig()
        self._backoff = backoff or ExponentialBackoff()
        self._clock = clock or SystemClock()
        self._on_state_change = on_state_change
        self._buffer: LatestFrameBuffer[T] = LatestFrameBuffer()
        self._task: asyncio.Task[None] | None = None
        self._state = StreamState.IDLE
        self._sequence = 0
        self._reconnect_attempts = 0
        self._last_frame_at: float | None = None
        self._last_error: str | None = None

    @property
    def state(self) -> StreamState:
        return self._state

    @property
    def stats(self) -> StreamStats:
        return StreamStats(
            state=self._state,
            frames_read=self._sequence,
            frames_dropped=self._buffer.dropped,
            reconnect_attempts=self._reconnect_attempts,
            last_frame_at=self._last_frame_at,
            last_error=self._last_error,
        )

    def is_healthy(self, max_staleness: float) -> bool:
        if self._state is not StreamState.STREAMING or self._last_frame_at is None:
            return False
        return self._clock.monotonic() - self._last_frame_at <= max_staleness

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("supervisor has already been started")
        self._task = asyncio.create_task(self._run(), name="stream-supervisor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise

    async def next_frame(self) -> Frame[T]:
        try:
            return await self._buffer.get()
        except BufferClosed:
            raise StreamClosed from None

    async def frames(self) -> AsyncIterator[Frame[T]]:
        while True:
            try:
                frame = await self.next_frame()
            except StreamClosed:
                return
            yield frame

    async def __aenter__(self) -> StreamSupervisor[T]:
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.stop()

    async def _run(self) -> None:
        try:
            while True:
                source = await self._connect()
                if source is not None:
                    await self._stream(source)
                await self._wait_before_retry()
        finally:
            self._set_state(StreamState.STOPPED)
            self._buffer.close()

    async def _connect(self) -> FrameSource[T] | None:
        self._set_state(StreamState.CONNECTING)
        try:
            source = self._factory()
        except Exception as exc:
            self._record_failure("create", exc)
            return None
        opened = False
        try:
            await asyncio.wait_for(asyncio.to_thread(source.open), self._config.open_timeout)
            opened = True
        except Exception as exc:
            self._record_failure("open", exc)
        finally:
            if not opened:
                await self._close(source)
        return source if opened else None

    async def _stream(self, source: FrameSource[T]) -> None:
        self._set_state(StreamState.STREAMING)
        started_at = self._clock.monotonic()
        stable = False
        try:
            while True:
                data = await asyncio.wait_for(
                    asyncio.to_thread(source.read), self._config.read_timeout
                )
                self._publish(data)
                if not stable and self._clock.monotonic() - started_at >= self._config.stable_after:
                    self._backoff.reset()
                    stable = True
        except Exception as exc:
            self._record_failure("read", exc)
        finally:
            await self._close(source)

    async def _wait_before_retry(self) -> None:
        self._set_state(StreamState.BACKOFF)
        delay = self._backoff.next_delay()
        self._reconnect_attempts += 1
        logger.info("reconnecting in %.2fs (attempt %d)", delay, self._reconnect_attempts)
        await self._clock.sleep(delay)

    async def _close(self, source: FrameSource[T]) -> None:
        try:
            await asyncio.wait_for(asyncio.to_thread(source.close), self._config.close_timeout)
        except Exception as exc:
            logger.warning("failed to close source: %s", _describe(exc))

    def _publish(self, data: T) -> None:
        now = self._clock.monotonic()
        self._sequence += 1
        self._last_frame_at = now
        self._buffer.put(Frame(data=data, sequence=self._sequence, captured_at=now))

    def _record_failure(self, stage: str, exc: Exception) -> None:
        self._last_error = f"{stage} failed: {_describe(exc)}"
        logger.warning("stream %s", self._last_error)

    def _set_state(self, state: StreamState) -> None:
        previous, self._state = self._state, state
        if self._on_state_change is not None:
            self._on_state_change(previous, state)


def _describe(exc: BaseException) -> str:
    return str(exc) or type(exc).__name__
