from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class ExponentialBackoff:
    initial: float = 0.5
    maximum: float = 30.0
    multiplier: float = 2.0
    jitter: float = 0.1
    rng: Callable[[], float] = field(default=random.random, repr=False)
    _attempt: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.initial <= 0:
            raise ValueError("initial must be positive")
        if self.maximum < self.initial:
            raise ValueError("maximum must be greater than or equal to initial")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be within [0, 1]")

    @property
    def attempt(self) -> int:
        return self._attempt

    def next_delay(self) -> float:
        base = self.initial * self.multiplier**self._attempt
        if base >= self.maximum:
            base = self.maximum
        else:
            self._attempt += 1
        spread = base * self.jitter
        delay = base - spread + 2 * spread * self.rng()
        return min(max(delay, 0.0), self.maximum)

    def reset(self) -> None:
        self._attempt = 0
