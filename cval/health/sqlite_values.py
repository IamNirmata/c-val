"""Strict SQLite scalar decoding for health adapter read-only rows."""

from __future__ import annotations

import math
from typing import Any


def sqlite_integer(
    value: Any,
    field_name: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be stored as SQLite INTEGER")
    if positive and value <= 0:
        raise ValueError(f"{field_name} must be positive")
    if non_negative and value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def sqlite_text(value: Any, field_name: str, *, nonempty: bool = True) -> str:
    if not isinstance(value, str) or (nonempty and not value):
        qualifier = "non-empty " if nonempty else ""
        raise ValueError(f"{field_name} must be stored as {qualifier}SQLite TEXT")
    return value


def sqlite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be stored as SQLite INTEGER/REAL")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result
