from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class StreamState(StrEnum):
    IDLE = "idle"
    CONNECTING = "connecting"
    STREAMING = "streaming"
    BACKOFF = "backoff"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class StreamStats:
    state: StreamState
    frames_read: int
    frames_dropped: int
    reconnect_attempts: int
    last_frame_at: float | None
    last_error: str | None
