"""Bounded retries for complete SQLite operations, never partial transactions."""

from __future__ import annotations

import os
import sqlite3
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeVar


Result = TypeVar("Result")


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 5
    delay_seconds: float = 2.0
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if type(self.attempts) is not int or not 1 <= self.attempts <= 20:
            raise ValueError("sqlite_retry.attempts must be between 1 and 20")
        for value in (self.delay_seconds, self.timeout_seconds):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 60:
                raise ValueError("SQLite retry seconds must be finite and between 0 and 60")


def load_retry_policy(path: Path | None = None) -> RetryPolicy:
    path = path or Path(os.environ.get("CVAL_CONFIG", Path(__file__).resolve().parents[2] / "config/cval.toml"))
    settings = tomllib.loads(path.read_text(encoding="utf-8")).get("sqlite_retry", {})
    return RetryPolicy(**settings)


def retry_sqlite(
    operation: Callable[[], Result],
    policy: RetryPolicy,
    *,
    sleeper: Callable[[float], None] = time.sleep,
) -> Result:
    for attempt in range(policy.attempts):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", 0) & 0xFF
            if code not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or attempt + 1 == policy.attempts:
                raise
            sleeper(policy.delay_seconds)
    raise AssertionError("unreachable")