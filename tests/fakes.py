from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable

from rtsp_supervisor import FrameSource, SourceError


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


class FakeSource:
    def __init__(
        self,
        frames: int = 0,
        *,
        open_error: Exception | None = None,
        close_error: Exception | None = None,
        on_read: Callable[[], None] | None = None,
    ) -> None:
        self._remaining = frames
        self._open_error = open_error
        self._close_error = close_error
        self._on_read = on_read
        self._produced = 0
        self.closed = False

    def open(self) -> None:
        if self._open_error is not None:
            raise self._open_error

    def read(self) -> int:
        if self._remaining == 0:
            raise SourceError("end of stream")
        self._remaining -= 1
        self._produced += 1
        if self._on_read is not None:
            self._on_read()
        return self._produced

    def close(self) -> None:
        self.closed = True
        if self._close_error is not None:
            raise self._close_error


class BlockingSource:
    def __init__(self, frames: int = 0) -> None:
        self._remaining = frames
        self._released = threading.Event()
        self.closed = False

    def open(self) -> None:
        pass

    def read(self) -> int:
        if self._remaining > 0:
            self._remaining -= 1
            return self._remaining
        self._released.wait()
        raise SourceError("closed while reading")

    def close(self) -> None:
        self.closed = True
        self._released.set()


class ScriptedFactory:
    def __init__(self, *sources: FakeSource | BlockingSource) -> None:
        self._pending = list(sources)
        self.created: list[FakeSource | BlockingSource] = []

    def __call__(self) -> FrameSource[int]:
        source = self._pending.pop(0) if self._pending else BlockingSource()
        self.created.append(source)
        return source


async def wait_until(predicate: Callable[[], bool], within: float = 2.0) -> None:
    async with asyncio.timeout(within):
        while not predicate():
            await asyncio.sleep(0.005)
