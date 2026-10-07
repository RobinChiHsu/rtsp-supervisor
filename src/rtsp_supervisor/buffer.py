from __future__ import annotations

import asyncio
from typing import Generic, TypeVar

from .frame import Frame

T = TypeVar("T")


class BufferClosed(Exception):
    pass


class LatestFrameBuffer(Generic[T]):
    def __init__(self) -> None:
        self._pending: Frame[T] | None = None
        self._ready = asyncio.Event()
        self._closed = False
        self._dropped = 0

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def closed(self) -> bool:
        return self._closed

    def put(self, frame: Frame[T]) -> None:
        if self._closed:
            raise BufferClosed
        if self._pending is not None:
            self._dropped += 1
        self._pending = frame
        self._ready.set()

    async def get(self) -> Frame[T]:
        while self._pending is None:
            if self._closed:
                raise BufferClosed
            self._ready.clear()
            await self._ready.wait()
        frame, self._pending = self._pending, None
        return frame

    def close(self) -> None:
        self._closed = True
        self._ready.set()
