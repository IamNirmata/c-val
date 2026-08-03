"""Canonical, injection-safe SQLite filesystem URI handling.

SQLite treats ``?``, ``#``, and percent escapes specially in URI filenames.
Every filesystem-backed connection should therefore encode the absolute path
before appending controlled query parameters, then bind the open connection to
the intended canonical path and inode.
"""

from __future__ import annotations

import os
import sqlite3
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlencode


@dataclass(frozen=True)
class SQLiteFileIdentity:
    """Canonical filesystem identity captured for one regular SQLite file."""

    path: Path
    device: int
    inode: int

    @classmethod
    def capture(cls, path: str | Path) -> "SQLiteFileIdentity":
        canonical = canonical_sqlite_path(path, must_exist=True)
        metadata = os.lstat(canonical)
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError(f"SQLite database is not a regular file: {canonical}")
        return cls(canonical, metadata.st_dev, metadata.st_ino)


def canonical_sqlite_path(
    path: str | Path,
    *,
    must_exist: bool,
) -> Path:
    """Return one absolute canonical path without accepting symlink traversal."""

    raw_path = os.fspath(path)
    if raw_path.startswith("file:"):
        raise ValueError("SQLite filesystem path must not be a URI")
    if "\x00" in raw_path:
        raise ValueError("SQLite filesystem path must not contain NUL")
    value = Path(raw_path).expanduser()
    if not value.is_absolute():
        value = Path.cwd() / value
    value = Path(*value.parts)
    if any(part in {"", ".", ".."} for part in value.parts[1:]):
        raise ValueError(f"SQLite database path is not lexical-canonical: {value}")
    _reject_symlink_components(value)
    try:
        canonical = value.resolve(strict=must_exist)
    except FileNotFoundError:
        raise
    if not canonical.is_absolute():
        raise ValueError("SQLite database path must resolve to an absolute path")
    if canonical != value:
        raise ValueError(f"SQLite database path is not canonical: {value}")
    return canonical


def sqlite_file_uri(
    path: str | Path,
    *,
    parameters: Mapping[str, str | int] | None = None,
    must_exist: bool = False,
) -> str:
    """Build a SQLite URI with a percent-encoded absolute filesystem path."""

    canonical = canonical_sqlite_path(path, must_exist=must_exist)
    encoded_path = quote(str(canonical), safe="/", encoding="utf-8", errors="strict")
    query = urlencode(
        sorted((str(key), str(value)) for key, value in (parameters or {}).items()),
        doseq=False,
        safe="",
    )
    return f"file:{encoded_path}" + (f"?{query}" if query else "")


def connect_sqlite_file(
    path: str | Path,
    *,
    mode: str,
    timeout: float = 30,
    expected_identity: SQLiteFileIdentity | None = None,
    **kwargs: Any,
) -> sqlite3.Connection:
    """Open a filesystem DB and bind ``main`` to the intended path and inode."""

    if mode not in {"ro", "rw", "rwc"}:
        raise ValueError(f"Unsupported SQLite filesystem mode: {mode!r}")
    must_exist = mode in {"ro", "rw"}
    canonical = canonical_sqlite_path(path, must_exist=must_exist)
    before = SQLiteFileIdentity.capture(canonical) if canonical.exists() else None
    if expected_identity is not None:
        _assert_expected_identity(before, expected_identity)
    connection = sqlite3.connect(
        sqlite_file_uri(
            canonical,
            parameters={"mode": mode},
            must_exist=must_exist,
        ),
        uri=True,
        timeout=timeout,
        **kwargs,
    )
    try:
        current = assert_sqlite_connection_identity(
            connection,
            canonical,
            expected_identity=expected_identity or before,
        )
        if before is not None and current != before:
            raise RuntimeError("SQLite database identity changed while it was opened")
        if before is None and mode == "rwc":
            assert_sqlite_file_identity(current)
        return connection
    except Exception:
        connection.close()
        raise


def assert_sqlite_connection_identity(
    connection: sqlite3.Connection,
    intended_path: str | Path,
    *,
    expected_identity: SQLiteFileIdentity | None = None,
) -> SQLiteFileIdentity:
    """Assert that ``PRAGMA database_list`` main is the intended current inode."""

    intended = canonical_sqlite_path(intended_path, must_exist=True)
    rows = connection.execute("PRAGMA database_list").fetchall()
    mains = [row for row in rows if len(row) >= 3 and row[1] == "main"]
    if len(mains) != 1 or not isinstance(mains[0][2], str) or not mains[0][2]:
        raise RuntimeError("SQLite connection has no unique filesystem-backed main DB")
    reported_raw = Path(mains[0][2]).expanduser()
    if not reported_raw.is_absolute():
        reported_raw = (Path.cwd() / reported_raw)
    reported_lexical = Path(*reported_raw.parts)
    if reported_lexical != intended:
        raise RuntimeError(
            f"SQLite main path mismatch: intended {intended}, opened {reported_lexical}"
        )
    reported = canonical_sqlite_path(reported_lexical, must_exist=True)
    current = SQLiteFileIdentity.capture(reported)
    if current.path != intended:
        raise RuntimeError("SQLite main path did not resolve to the intended database")
    if expected_identity is not None:
        _assert_expected_identity(current, expected_identity)
    return current


def assert_sqlite_file_identity(identity: SQLiteFileIdentity) -> None:
    """Fail closed if a canonical path no longer names the captured inode."""

    _assert_expected_identity(SQLiteFileIdentity.capture(identity.path), identity)


def _assert_expected_identity(
    actual: SQLiteFileIdentity | None,
    expected: SQLiteFileIdentity,
) -> None:
    if actual is None or actual != expected:
        raise RuntimeError(
            "SQLite database path/device/inode changed since identity capture"
        )


def _reject_symlink_components(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if current.is_symlink():
            raise ValueError(
                f"SQLite database path must not traverse a symlink: {current}"
            )


_READONLY_SCRIPT_PRELUDE = r'''
import os
import sqlite3
import stat
from pathlib import Path
from urllib.parse import quote

def connect_sqlite_readonly(raw_path):
    raw = os.fspath(raw_path)
    if raw.startswith("file:") or "\x00" in raw:
        raise ValueError("SQLite path must be a plain filesystem path")
    intended = Path(raw).expanduser()
    if not intended.is_absolute():
        intended = Path.cwd() / intended
    intended = Path(*intended.parts)
    if any(part in {"", ".", ".."} for part in intended.parts[1:]):
        raise ValueError("SQLite path is not lexical-canonical")
    current = Path(intended.anchor)
    for part in intended.parts[1:]:
        current = current / part
        if current.is_symlink():
            raise ValueError("SQLite path must not traverse a symlink")
    canonical = intended.resolve(strict=True)
    if canonical != intended:
        raise ValueError("SQLite path is not canonical")
    before = os.stat(canonical, follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise RuntimeError("SQLite database is not a regular file")
    uri = "file:" + quote(str(canonical), safe="/") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    try:
        mains = [
            row for row in connection.execute("PRAGMA database_list").fetchall()
            if len(row) >= 3 and row[1] == "main"
        ]
        if len(mains) != 1 or not isinstance(mains[0][2], str) or not mains[0][2]:
            raise RuntimeError("SQLite connection has no unique filesystem main DB")
        reported = Path(mains[0][2])
        if not reported.is_absolute():
            reported = Path.cwd() / reported
        reported = Path(*reported.parts)
        if reported != canonical:
            raise RuntimeError("SQLite main path mismatch")
        after = os.stat(canonical, follow_symlinks=False)
        if (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
            raise RuntimeError("SQLite database inode changed while opening")
        return connection
    except Exception:
        connection.close()
        raise
'''.lstrip()


def sqlite_readonly_script_prelude() -> str:
    """Return the dependency-free safe opener used by injected pod scripts."""

    return _READONLY_SCRIPT_PRELUDE


__all__ = [
    "SQLiteFileIdentity",
    "assert_sqlite_connection_identity",
    "assert_sqlite_file_identity",
    "canonical_sqlite_path",
    "connect_sqlite_file",
    "sqlite_readonly_script_prelude",
    "sqlite_file_uri",
]
