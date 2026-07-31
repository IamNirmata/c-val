"""Side-effect-free SQLite reads for evaluator and adapter preflight.

SQLite ``mode=ro`` may still create ``-wal``/``-shm`` files when a database's
header selects WAL journaling.  U9 therefore copies a stable, checkpointed main
database image into shared memory and makes every catalog/adapter read use that
single in-memory snapshot.  No source sidecar is created, removed, or repaired.
"""

from __future__ import annotations

import os
import sqlite3
import stat
import uuid
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from cval.storage.sqlite_uri import SQLiteFileIdentity, assert_sqlite_file_identity


_SQLITE_HEADER = b"SQLite format 3\x00"
_ROLLBACK_JOURNAL_VERSION = 1
_WAL_JOURNAL_VERSION = 2
_SNAPSHOT_URI_PREFIX = "file:cval-immutable-snapshot-"
_CONNECTION_SOURCE_PATHS: dict[int, Path] = {}


@dataclass(frozen=True)
class ImmutableSQLiteSnapshot:
    """One process-local shared-memory copy of a stable SQLite main file."""

    source_path: Path
    source_identity: SQLiteFileIdentity
    uri: str
    size_bytes: int

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.uri, uri=True, timeout=30)


def is_snapshot_uri(value: str | Path) -> bool:
    return isinstance(value, str) and value.startswith(_SNAPSHOT_URI_PREFIX)


@contextmanager
def immutable_sqlite_snapshot(
    db_path: str | Path,
) -> Iterator[ImmutableSQLiteSnapshot]:
    """Yield one shared-memory snapshot without opening SQLite on the source.

    WAL databases are accepted only after checkpointing has removed both
    sidecars.  The copied header is normalized in memory so ``deserialize``
    cannot attempt to resolve a WAL file.  A source that changes while copied
    fails closed.
    """

    identity = SQLiteFileIdentity.capture(db_path)
    path = identity.path
    _require_absent_sidecars(path)
    before = os.lstat(path)
    image = _read_without_atime(path, before)
    after = os.lstat(path)
    _require_unchanged_source(path, before, after, len(image))
    _require_absent_sidecars(path)
    _normalize_snapshot_image(image)

    uri = f"{_SNAPSHOT_URI_PREFIX}{uuid.uuid4().hex}?mode=memory&cache=shared"
    with closing(sqlite3.connect(":memory:")) as source, closing(
        sqlite3.connect(uri, uri=True, timeout=30)
    ) as keeper:
        source.deserialize(bytes(image))
        source.execute("PRAGMA query_only=ON")
        if source.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise RuntimeError("SQLite snapshot source failed integrity_check")
        source.backup(keeper)
        keeper.execute("PRAGMA query_only=ON")
        snapshot = ImmutableSQLiteSnapshot(path, identity, uri, len(image))
        yield snapshot
        _require_absent_sidecars(path)
        assert_sqlite_file_identity(identity)


@contextmanager
def sqlite_connection_projection(
    connection: sqlite3.Connection,
) -> Iterator[str]:
    """Project an open transaction into shared memory for adapter evidence reads.

    The projection is serialized from the caller's already-open transaction.
    It never reopens the filesystem source, which is essential while a WAL
    write reservation has live sidecars.
    """

    if not isinstance(connection, sqlite3.Connection) or not connection.in_transaction:
        raise RuntimeError("SQLite connection projection requires an active transaction")
    image = bytearray(connection.serialize(name="main"))
    _normalize_snapshot_image(image)
    uri = f"{_SNAPSHOT_URI_PREFIX}{uuid.uuid4().hex}?mode=memory&cache=shared"
    with closing(sqlite3.connect(":memory:")) as source, closing(
        sqlite3.connect(uri, uri=True, timeout=30)
    ) as keeper:
        source.deserialize(bytes(image), name="main")
        source.execute("PRAGMA query_only=ON")
        if source.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise RuntimeError("SQLite connection projection failed integrity_check")
        source.backup(keeper)
        keeper.execute("PRAGMA query_only=ON")
        yield uri


def _normalize_snapshot_image(image: bytearray) -> None:
    """Validate one SQLite image and normalize a WAL header only in memory."""

    if len(image) < 100 or bytes(image[:16]) != _SQLITE_HEADER:
        raise RuntimeError("SQLite snapshot source has an invalid database header")
    read_version, write_version = image[18], image[19]
    if (read_version, write_version) == (
        _WAL_JOURNAL_VERSION,
        _WAL_JOURNAL_VERSION,
    ):
        image[18] = _ROLLBACK_JOURNAL_VERSION
        image[19] = _ROLLBACK_JOURNAL_VERSION
    elif (read_version, write_version) != (
        _ROLLBACK_JOURNAL_VERSION,
        _ROLLBACK_JOURNAL_VERSION,
    ):
        raise RuntimeError("SQLite snapshot source has unsupported journal header values")


@contextmanager
def health_read_connection(
    location: str | Path,
) -> Iterator[sqlite3.Connection]:
    """Open an adapter read on an existing evaluator snapshot or make one."""

    if is_snapshot_uri(location):
        with closing(sqlite3.connect(str(location), uri=True, timeout=30)) as connection:
            connection.execute("PRAGMA query_only=ON")
            yield connection
        return
    with immutable_sqlite_snapshot(location) as snapshot, closing(
        snapshot.connect()
    ) as connection:
        connection.execute("PRAGMA query_only=ON")
        yield connection


@contextmanager
def immutable_snapshot_connection(
    db_path: str | Path,
) -> Iterator[sqlite3.Connection]:
    """Open one registered in-memory snapshot connection for core readers."""

    with immutable_sqlite_snapshot(db_path) as snapshot, closing(
        snapshot.connect()
    ) as connection:
        _CONNECTION_SOURCE_PATHS[id(connection)] = snapshot.source_path
        try:
            connection.execute("PRAGMA query_only=ON")
            yield connection
        finally:
            _CONNECTION_SOURCE_PATHS.pop(id(connection), None)


def sqlite_connection_source_path(connection: sqlite3.Connection) -> Path | None:
    """Return the original file for a registered in-memory snapshot."""

    return _CONNECTION_SOURCE_PATHS.get(id(connection))


def _require_absent_sidecars(path: Path) -> None:
    present = [
        str(sidecar)
        for sidecar in (
            path.with_name(f"{path.name}-wal"),
            path.with_name(f"{path.name}-shm"),
        )
        if sidecar.exists() or sidecar.is_symlink()
    ]
    if present:
        raise RuntimeError(
            "SQLite snapshot requires a checkpointed database with absent WAL/SHM "
            f"sidecars; found: {', '.join(present)}"
        )


def _require_unchanged_source(
    path: Path,
    before: os.stat_result,
    after: os.stat_result,
    size_bytes: int,
) -> None:
    identity = lambda value: (  # noqa: E731 - compact immutable stat projection
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )
    if identity(before) != identity(after) or after.st_size != size_bytes:
        raise RuntimeError(f"SQLite snapshot source changed while being read: {path}")


def _read_without_atime(path: Path, expected: os.stat_result) -> bytearray:
    """Read one regular file without updating access time or following links."""

    required_flags = ("O_NOATIME", "O_NOFOLLOW")
    if any(not hasattr(os, name) for name in required_flags):
        raise RuntimeError(
            "Side-effect-free SQLite snapshots require O_NOATIME and O_NOFOLLOW"
        )
    flags = os.O_RDONLY | os.O_NOATIME | os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(path, flags)
    except PermissionError as exc:
        raise RuntimeError(
            "Side-effect-free SQLite snapshot read requires O_NOATIME ownership"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise FileNotFoundError(
                f"SQLite snapshot source is not a regular file: {path}"
            )
        _require_unchanged_source(path, expected, opened, expected.st_size)
        image = bytearray()
        while chunk := os.read(descriptor, 1024 * 1024):
            image.extend(chunk)
        finished = os.fstat(descriptor)
        _require_unchanged_source(path, opened, finished, len(image))
        return image
    finally:
        os.close(descriptor)


def read_regular_file_without_atime(
    path: str | Path,
    *,
    expected_mode: int | None = None,
    expected_size: int | None = None,
    description: str = "file",
) -> bytes:
    """Strictly read one owned regular file without atime changes or symlinks."""

    value = Path(path).expanduser()
    if not value.is_absolute():
        value = value.resolve(strict=False)
    try:
        before = os.lstat(value)
    except FileNotFoundError as exc:
        raise RuntimeError(f"{description.capitalize()} is missing or unsafe") from exc
    if not stat.S_ISREG(before.st_mode):
        raise RuntimeError(f"{description.capitalize()} is missing or unsafe")
    if before.st_uid != os.geteuid():
        raise RuntimeError(f"{description.capitalize()} must be owned by the evaluator")
    if expected_mode is not None and stat.S_IMODE(before.st_mode) != expected_mode:
        raise RuntimeError(
            f"{description.capitalize()} permissions must be {expected_mode:04o}"
        )
    if expected_size is not None and before.st_size != expected_size:
        raise RuntimeError(f"{description.capitalize()} has an invalid length")
    image = _read_without_atime(value, before)
    after = os.lstat(value)
    _require_unchanged_source(value, before, after, len(image))
    return bytes(image)


__all__ = [
    "ImmutableSQLiteSnapshot",
    "health_read_connection",
    "immutable_snapshot_connection",
    "immutable_sqlite_snapshot",
    "is_snapshot_uri",
    "read_regular_file_without_atime",
    "sqlite_connection_projection",
    "sqlite_connection_source_path",
]