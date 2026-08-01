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
from typing import Callable, Iterator

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
    *,
    expected_identity: SQLiteFileIdentity | None = None,
    source_fd: int | None = None,
    source_parent_fd: int | None = None,
    source_name: str | None = None,
    binding_guard: Callable[[], None] | None = None,
) -> Iterator[ImmutableSQLiteSnapshot]:
    """Yield one shared-memory snapshot without opening SQLite on the source.

    WAL databases are accepted only after checkpointing has removed both
    sidecars.  The copied header is normalized in memory so ``deserialize``
    cannot attempt to resolve a WAL file.  A source that changes while copied
    fails closed.
    """

    if source_fd is None:
        identity = SQLiteFileIdentity.capture(db_path)
        if expected_identity is not None and identity != expected_identity:
            raise RuntimeError(
                "SQLite snapshot source path/device/inode changed since binding capture"
            )
        path = identity.path
        _require_absent_sidecars(path)
        before = os.lstat(path)
        image = _read_without_atime(path, before)
        after = os.lstat(path)
        _require_unchanged_source(path, before, after, len(image))
        _require_absent_sidecars(path)
    else:
        if expected_identity is None or source_parent_fd is None or source_name is None:
            raise ValueError(
                "Descriptor SQLite snapshots require expected identity, parent fd, and name"
            )
        if binding_guard is not None:
            binding_guard()
        identity = expected_identity
        path = identity.path
        _require_absent_sidecars_at(source_parent_fd, source_name)
        before = os.fstat(source_fd)
        if (before.st_dev, before.st_ino) != (identity.device, identity.inode):
            raise RuntimeError("SQLite snapshot descriptor identity changed")
        image = _read_fd_without_atime(source_fd, before, path)
        after = os.fstat(source_fd)
        _require_unchanged_source(path, before, after, len(image))
        _require_absent_sidecars_at(source_parent_fd, source_name)
        if binding_guard is not None:
            binding_guard()
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
        if source_fd is None:
            _require_absent_sidecars(path)
            assert_sqlite_file_identity(identity)
            if expected_identity is not None:
                assert_sqlite_file_identity(expected_identity)
        else:
            _require_absent_sidecars_at(source_parent_fd, source_name)
            finished = os.fstat(source_fd)
            _require_unchanged_source(path, before, finished, len(image))
            if binding_guard is not None:
                binding_guard()


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
    *,
    expected_identity: SQLiteFileIdentity | None = None,
    source_fd: int | None = None,
    source_parent_fd: int | None = None,
    source_name: str | None = None,
    binding_guard: Callable[[], None] | None = None,
) -> Iterator[sqlite3.Connection]:
    """Open one registered in-memory snapshot connection for core readers."""

    with immutable_sqlite_snapshot(
        db_path,
        expected_identity=expected_identity,
        source_fd=source_fd,
        source_parent_fd=source_parent_fd,
        source_name=source_name,
        binding_guard=binding_guard,
    ) as snapshot, closing(
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
            path.with_name(f"{path.name}-journal"),
        )
        if sidecar.exists() or sidecar.is_symlink()
    ]
    if present:
        raise RuntimeError(
            "SQLite snapshot requires a quiescent database with absent WAL/SHM/journal "
            f"sidecars; found: {', '.join(present)}"
        )


def _require_absent_sidecars_at(parent_fd: int, name: str) -> None:
    present: list[str] = []
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = name + suffix
        try:
            os.stat(sidecar, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        present.append(sidecar)
    if present:
        raise RuntimeError(
            "SQLite snapshot requires a quiescent database with absent WAL/SHM/journal "
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


def _read_fd_without_atime(
    descriptor: int,
    expected: os.stat_result,
    path: Path,
) -> bytearray:
    """Read a retained O_NOATIME descriptor with pread and no shared offset."""

    if not hasattr(os, "pread"):
        raise RuntimeError("Descriptor snapshots require os.pread")
    opened = os.fstat(descriptor)
    if not stat.S_ISREG(opened.st_mode):
        raise RuntimeError(f"SQLite snapshot source is not a regular file: {path}")
    _require_unchanged_source(path, expected, opened, expected.st_size)
    image = bytearray()
    offset = 0
    while chunk := os.pread(descriptor, 1024 * 1024, offset):
        image.extend(chunk)
        offset += len(chunk)
    finished = os.fstat(descriptor)
    _require_unchanged_source(path, opened, finished, len(image))
    return image


def read_regular_file_without_atime(
    path: str | Path,
    *,
    expected_mode: int | None = None,
    expected_size: int | None = None,
    description: str = "file",
    expected_identity: SQLiteFileIdentity | None = None,
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
    if expected_identity is not None and (
        value,
        before.st_dev,
        before.st_ino,
    ) != (
        expected_identity.path,
        expected_identity.device,
        expected_identity.inode,
    ):
        raise RuntimeError(
            f"{description.capitalize()} path/device/inode changed since binding capture"
        )
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
    if expected_identity is not None:
        assert_sqlite_file_identity(expected_identity)
    return bytes(image)


def read_regular_file_descriptor_without_atime(
    descriptor: int,
    *,
    path: str | Path,
    expected_identity: SQLiteFileIdentity,
    expected_mode: int | None = None,
    expected_size: int | None = None,
    description: str = "file",
    binding_guard: Callable[[], None] | None = None,
) -> bytes:
    """Read one retained state descriptor with pread and binding checks."""

    if binding_guard is not None:
        binding_guard()
    value = Path(path)
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or (before.st_dev, before.st_ino)
        != (expected_identity.device, expected_identity.inode)
    ):
        raise RuntimeError(f"{description.capitalize()} descriptor identity changed")
    if before.st_uid != os.geteuid():
        raise RuntimeError(f"{description.capitalize()} must be owned by the evaluator")
    if before.st_nlink != 1:
        raise RuntimeError(f"{description.capitalize()} must have exactly one link")
    if expected_mode is not None and stat.S_IMODE(before.st_mode) != expected_mode:
        raise RuntimeError(
            f"{description.capitalize()} permissions must be {expected_mode:04o}"
        )
    if expected_size is not None and before.st_size != expected_size:
        raise RuntimeError(f"{description.capitalize()} has an invalid length")
    image = _read_fd_without_atime(descriptor, before, value)
    after = os.fstat(descriptor)
    _require_unchanged_source(value, before, after, len(image))
    if binding_guard is not None:
        binding_guard()
    return bytes(image)


__all__ = [
    "ImmutableSQLiteSnapshot",
    "health_read_connection",
    "immutable_snapshot_connection",
    "immutable_sqlite_snapshot",
    "is_snapshot_uri",
    "read_regular_file_without_atime",
    "read_regular_file_descriptor_without_atime",
    "sqlite_connection_projection",
    "sqlite_connection_source_path",
]