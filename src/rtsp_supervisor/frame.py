from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Frame(Generic[T]):
    data: T
    sequence: int
    captured_at: float
