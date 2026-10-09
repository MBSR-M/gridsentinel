"""Bounded retry with injected clock, randomness and explicit classification."""

from collections.abc import Callable
from dataclasses import dataclass
import random
import time
from typing import TypeVar

from gridsentinel.core.exceptions import is_transient

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Full-jitter exponential backoff, bounded in attempts and delay."""

    attempts: int = 3
    base_delay: float = 0.2
    max_delay: float = 5.0

    def __post_init__(self) -> None:
        if not 1 <= self.attempts <= 100 or not 0 <= self.base_delay <= self.max_delay:
            raise ValueError("Invalid retry policy")

    def run(self, operation: Callable[[], T],
            retryable: Callable[[Exception], bool] = is_transient,
            sleep: Callable[[float], None] = time.sleep,
            uniform: Callable[[float, float], float] = random.uniform) -> T:
        """Retry an entire idempotent operation, never an uncertain partial write."""
        for attempt in range(self.attempts):
            try:
                return operation()
            except Exception as error:
                if attempt + 1 == self.attempts or not retryable(error):
                    raise
                sleep(uniform(0.0, min(self.max_delay, self.base_delay * 2 ** attempt)))
        raise RuntimeError("Unreachable retry exhaustion")
