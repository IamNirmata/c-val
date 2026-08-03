#!/usr/bin/env python3
"""Inspect or explicitly apply a split of the former global classification database."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import re
import secrets
import signal
import sqlite3
import stat
import struct
import sys
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cval.baselines.storage import (
    CLASSIFICATION_RESULTS_COLUMNS,
    CLASSIFICATION_RESULTS_INDEX_NAME,
    baseline_root_path,
    default_classification_db_path,
)
from cval.config import load_config
from cval.storage.classification_legacy import legacy_classification_scalars
from cval.storage.sqlite_uri import SQLiteFileIdentity, connect_sqlite_file
from cval.validation.operational_targets import validate_operational_target_name
from cval.validation.secure_fs import (
    lexical_absolute,
    open_directory_no_symlinks,
    rename_noreplace_at,
    safe_relative_parts,
)

CONFIRMATION = "split-classifications"
BACKUP_SCHEMA = "cval.backup"
ROW_DIGEST_SCHEMA = b"cval.classification-rows.v1\0"
READ_CHUNK = 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
INDEX_NAME = CLASSIFICATION_RESULTS_INDEX_NAME
HISTORICAL_TARGET_FILENAMES = {"nccl": "nccl-classifications.db"}

BASE_COLUMNS = (
    ("classified_at", "INTEGER", 1, None, 1),
    ("node", "TEXT", 1, None, 2),
    ("test_type", "TEXT", 1, None, 3),
    ("baseline_id", "TEXT", 1, None, 4),
    ("status", "TEXT", 1, None, 0),
    ("passed", "INTEGER", 1, None, 0),
    ("n_compared", "INTEGER", 1, None, 0),
    ("n_degraded", "INTEGER", 1, None, 0),
    ("n_improved", "INTEGER", 1, None, 0),
)
OPTIONAL_COLUMNS = (
    ("n_band_degraded", "INTEGER", 1, "0", 0),
    ("degraded_metric_fraction", "REAL", 1, "0.0", 0),
    ("worst_pct_diff", "REAL", 1, "0.0", 0),
)
METRICS_COLUMN = ("metrics_json", "TEXT", 1, None, 0)
CURRENT_COLUMNS = CLASSIFICATION_RESULTS_COLUMNS
CURRENT_COLUMN_NAMES = tuple(column[0] for column in CURRENT_COLUMNS)
PRIMARY_KEY_COLUMNS = ("classified_at", "node", "test_type", "baseline_id")

CURRENT_TABLE_SQL = """
CREATE TABLE classification_results (
  classified_at INTEGER NOT NULL,
  node TEXT NOT NULL,
  test_type TEXT NOT NULL,
  baseline_id TEXT NOT NULL,
  status TEXT NOT NULL,
  passed INTEGER NOT NULL,
  n_compared INTEGER NOT NULL,
  n_degraded INTEGER NOT NULL,
  n_improved INTEGER NOT NULL,
  n_band_degraded INTEGER NOT NULL DEFAULT 0,
  degraded_metric_fraction REAL NOT NULL DEFAULT 0.0,
  worst_pct_diff REAL NOT NULL DEFAULT 0.0,
  metrics_json TEXT NOT NULL,
  PRIMARY KEY (classified_at, node, test_type, baseline_id)
)
""".strip()
INDEX_SQL = (
    "CREATE INDEX idx_classification_node_test_time "
    "ON classification_results(node, test_type, classified_at)"
)


@dataclass(frozen=True)
class FileState:
    device: int
    inode: int
    size: int
    mtime_ns: int

    @classmethod
    def from_stat(cls, value: os.stat_result) -> "FileState":
        return cls(value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


@dataclass
class OwnedFile:
    parent_fd: int
    parent_path: Path
    name: str
    state: FileState
    published_name: str | None = None

    @property
    def path(self) -> Path:
        return self.parent_path / (self.published_name or self.name)


@dataclass(frozen=True)
class SourceView:
    layout: tuple[tuple[str, str, int, str | None, int], ...]
    counts: dict[str, int]
    digests: dict[str, str]


@dataclass(frozen=True)
class TargetEntry:
    test_type: str
    path: Path
    parent_path: Path
    parent_fd: int
    existing_fd: int | None
    existing_state: FileState | None


class SplitInterrupted(InterruptedError):
    """Raised so SIGINT/SIGTERM use the normal owned-file cleanup path."""


class InterruptGuard:
    def __init__(self) -> None:
        self._previous: dict[int, object] = {}
        self.cleaning = False
        self.pending: int | None = None

    def __enter__(self) -> "InterruptGuard":
        for signum in (signal.SIGINT, signal.SIGTERM):
            self._previous[signum] = signal.getsignal(signum)
            signal.signal(signum, self._handle)
        return self

    def _handle(self, signum: int, _frame: object) -> None:
        self.pending = self.pending or signum

    def check(self) -> None:
        if self.pending is not None and not self.cleaning:
            raise SplitInterrupted(
                f"Classification split interrupted by signal {self.pending}"
            )

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        for signum, previous in self._previous.items():
            signal.signal(signum, previous)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser()
    value.add_argument("--source", type=Path, required=True)
    value.add_argument("--backup-manifest", type=Path)
    value.add_argument(
        "--use-backup-db",
        action="store_true",
        help="Split the manifest-verified backup DB instead of the current source",
    )
    value.add_argument("--config", type=Path)
    value.add_argument("--apply", action="store_true")
    value.add_argument("--confirm")
    return value


def _open_regular(path: str | Path) -> tuple[Path, int, int, FileState]:
    absolute = lexical_absolute(path)
    parent_path, parent_fd = open_directory_no_symlinks(absolute.parent)
    if parent_path != absolute.parent:
        os.close(parent_fd)
        raise ValueError(f"File parent is not lexical-canonical: {absolute.parent}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(absolute.name, flags, dir_fd=parent_fd)
    except BaseException:
        os.close(parent_fd)
        raise
    try:
        metadata = os.fstat(descriptor)
        named = os.stat(absolute.name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISREG(metadata.st_mode) or not stat.S_ISREG(named.st_mode):
            raise ValueError(f"Path must be an exact regular file: {absolute}")
        state = FileState.from_stat(metadata)
        if FileState.from_stat(named) != state:
            raise RuntimeError(f"File identity changed while opening: {absolute}")
        return absolute, parent_fd, descriptor, state
    except BaseException:
        os.close(descriptor)
        os.close(parent_fd)
        raise


def _read_fd(descriptor: int, *, max_bytes: int | None = None) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    payload = bytearray()
    while True:
        limit = READ_CHUNK
        if max_bytes is not None:
            limit = min(limit, max_bytes + 1 - len(payload))
            if limit <= 0:
                raise ValueError(f"Input exceeds {max_bytes} bytes")
        chunk = os.read(descriptor, limit)
        if not chunk:
            break
        payload.extend(chunk)
    if max_bytes is not None and len(payload) > max_bytes:
        raise ValueError(f"Input exceeds {max_bytes} bytes")
    return bytes(payload)


def _digest_fd(descriptor: int) -> str:
    os.lseek(descriptor, 0, os.SEEK_SET)
    value = hashlib.sha256()
    while chunk := os.read(descriptor, READ_CHUNK):
        value.update(chunk)
    return value.hexdigest()


def _assert_bound_file(
    path: Path,
    parent_fd: int,
    descriptor: int,
    expected: FileState,
) -> None:
    descriptor_state = FileState.from_stat(os.fstat(descriptor))
    named = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISREG(named.st_mode):
        raise RuntimeError(f"File is no longer regular: {path}")
    if descriptor_state != expected or FileState.from_stat(named) != expected:
        raise RuntimeError(f"File identity/size/mtime changed during split: {path}")


def _assert_no_sidecars(parent_fd: int, basename: str, description: str) -> None:
    for suffix in SIDECAR_SUFFIXES:
        try:
            os.stat(basename + suffix, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise ValueError(f"{description} has a SQLite sidecar: {basename + suffix}")


def _read_json_regular(path: Path) -> dict:
    absolute, parent_fd, descriptor, state = _open_regular(path)
    try:
        if state.size > MAX_MANIFEST_BYTES:
            raise ValueError("Backup manifest is too large")
        payload = _read_fd(descriptor, max_bytes=MAX_MANIFEST_BYTES)
        _assert_bound_file(absolute, parent_fd, descriptor, state)
    finally:
        os.close(descriptor)
        os.close(parent_fd)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Backup manifest is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("Backup manifest must be a JSON object")
    return value


def validate_backup_manifest(
    path: Path,
    source: Path,
    *,
    source_state: FileState,
    source_sha256: str,
    use_backup_db: bool = False,
) -> Path:
    payload = _read_json_regular(path)
    if payload.get("schema") != BACKUP_SCHEMA or not isinstance(
        payload.get("files"), list
    ):
        raise ValueError("Backup manifest is not a cval.backup manifest")
    required_manifest_keys = {
        "schema",
        "source_root",
        "destination",
        "excluded",
        "files",
    }
    if not required_manifest_keys.issubset(payload):
        raise ValueError("Backup manifest has missing required fields")
    if payload.get("excluded") != ["backups/"]:
        raise ValueError("Backup manifest has an unexpected exclusion manifest")
    destination = payload.get("destination")
    if not isinstance(destination, str) or not Path(destination).is_absolute():
        raise ValueError("Backup manifest destination must be absolute")
    required_entry_keys = {
        "path", "method", "size", "mode", "sha256", "source_size",
        "source_sha256", "source_dev", "source_inode", "source_mtime_ns",
        "backup_size", "backup_sha256",
    }
    manifest_paths: set[str] = set()
    for item in payload["files"]:
        if not isinstance(item, dict) or not required_entry_keys.issubset(item):
            raise ValueError("Backup manifest file entry has missing required fields")
        raw_entry_path = item.get("path")
        if not isinstance(raw_entry_path, str):
            raise ValueError("Backup manifest file entry path is invalid")
        entry_path = Path(
            *safe_relative_parts(raw_entry_path, field_name="backup file entry")
        ).as_posix()
        if entry_path != raw_entry_path or entry_path in manifest_paths:
            raise ValueError("Backup manifest file paths are invalid or duplicated")
        manifest_paths.add(entry_path)
        method = item.get("method")
        if method not in {"sqlite-backup", "copy2", "regular-copy", "hardlink"}:
            raise ValueError("Backup manifest file entry method is invalid")
        size = item.get("size")
        mode = item.get("mode")
        digest = item.get("sha256")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError("Backup manifest file entry size is invalid")
        if (
            isinstance(mode, bool)
            or not isinstance(mode, int)
            or mode < 0
            or mode > 0o7777
        ):
            raise ValueError("Backup manifest file entry mode is invalid")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("Backup manifest file entry sha256 is invalid")
        for field in (
            "source_size", "source_dev", "source_inode", "source_mtime_ns",
            "backup_size",
        ):
            scalar = item.get(field)
            if isinstance(scalar, bool) or not isinstance(scalar, int) or scalar < 0:
                raise ValueError(f"Backup manifest file entry {field} is invalid")
        for field in ("source_sha256", "backup_sha256"):
            scalar = item.get(field)
            if not isinstance(scalar, str) or not re.fullmatch(r"[0-9a-f]{64}", scalar):
                raise ValueError(f"Backup manifest file entry {field} is invalid")
        if item["backup_size"] != size or item["backup_sha256"] != digest:
            raise ValueError("Backup manifest backup evidence is inconsistent")
    raw_root = payload.get("source_root")
    if not isinstance(raw_root, str) or not Path(raw_root).is_absolute():
        raise ValueError("Backup manifest source_root must be an absolute directory")
    source_root = lexical_absolute(raw_root)
    if source_root != Path(raw_root):
        raise ValueError("Backup manifest source_root must be lexical-canonical")
    opened_root, root_fd = open_directory_no_symlinks(source_root)
    try:
        if opened_root != source_root:
            raise ValueError("Backup manifest source_root is not canonical")
    finally:
        os.close(root_fd)
    try:
        relative = source.relative_to(source_root)
    except ValueError as exc:
        raise ValueError(
            "Backup manifest source_root does not contain the source DB"
        ) from exc
    relative_name = Path(
        *safe_relative_parts(relative, field_name="backup source entry")
    ).as_posix()
    matches = [
        item
        for item in payload["files"]
        if isinstance(item, dict) and item.get("path") == relative_name
    ]
    if len(matches) != 1:
        raise ValueError(
            "Backup manifest must contain exactly one source classification DB entry"
        )
    entry = matches[0]
    if entry.get("method") != "sqlite-backup":
        raise ValueError("Backup manifest source entry is not a SQLite backup")
    expected_source = {
        "source_size": source_state.size,
        "source_sha256": source_sha256,
        "source_dev": source_state.device,
        "source_inode": source_state.inode,
        "source_mtime_ns": source_state.mtime_ns,
    }
    if any(entry.get(field) != expected for field, expected in expected_source.items()):
        raise ValueError(
            "Backup manifest source identity evidence does not match the current source DB"
        )
    backup_path = lexical_absolute(Path(destination) / relative_name)
    backup, backup_parent_fd, backup_fd, backup_state = _open_regular(backup_path)
    try:
        _assert_no_sidecars(
            backup_parent_fd, backup.name, "Backup classification DB"
        )
        if backup_state.size != entry["backup_size"]:
            raise ValueError("Backup classification DB size does not match the manifest")
        if _digest_fd(backup_fd) != entry["backup_sha256"]:
            raise ValueError("Backup classification DB sha256 does not match the manifest")
        _assert_bound_file(backup, backup_parent_fd, backup_fd, backup_state)
    finally:
        os.close(backup_fd)
        os.close(backup_parent_fd)
    return backup_path if use_backup_db else source


def _sql_tokens(statement: str) -> tuple[str, ...]:
    tokens = re.findall(
        r"[A-Za-z_][A-Za-z0-9_]*|[0-9]+(?:\.[0-9]+)?|[(),;]", statement
    )
    residue = re.sub(
        r"[A-Za-z_][A-Za-z0-9_]*|[0-9]+(?:\.[0-9]+)?|[(),;]|\s+",
        "",
        statement,
    )
    if residue:
        raise ValueError("Classification schema contains unsupported SQL syntax")
    lowered = [token.lower() for token in tokens if token != ";"]
    for marker in ("table", "index"):
        try:
            position = lowered.index(marker) + 1
        except ValueError:
            continue
        if lowered[position : position + 3] == ["if", "not", "exists"]:
            del lowered[position : position + 3]
        break
    return tuple(lowered)


def _table_sql_for_layout(
    layout: Sequence[tuple[str, str, int, str | None, int]],
) -> str:
    definitions = []
    for name, declared_type, not_null, default, _primary_key in layout:
        value = f"{name} {declared_type}"
        if not_null:
            value += " NOT NULL"
        if default is not None:
            value += f" DEFAULT {default}"
        definitions.append(value)
    definitions.append("PRIMARY KEY (classified_at, node, test_type, baseline_id)")
    return "CREATE TABLE classification_results (" + ", ".join(definitions) + ")"


def _known_source_layouts() -> tuple[
    tuple[tuple[str, str, int, str | None, int], ...], ...
]:
    legacy = tuple(
        BASE_COLUMNS + (METRICS_COLUMN,) + OPTIONAL_COLUMNS[:count]
        for count in range(len(OPTIONAL_COLUMNS) + 1)
    )
    return (CURRENT_COLUMNS,) + legacy


def _validate_index_columns(
    connection: sqlite3.Connection,
    index_name: str,
    expected: Sequence[str],
) -> None:
    rows = connection.execute(f"PRAGMA index_xinfo('{index_name}')").fetchall()
    keyed = [row for row in rows if int(row[5]) == 1]
    if tuple(str(row[2]) for row in keyed) != tuple(expected):
        raise ValueError(
            f"Classification index {index_name} has unexpected columns"
        )
    if any(
        int(row[3]) != 0 or str(row[4]).upper() != "BINARY" for row in keyed
    ):
        raise ValueError(
            f"Classification index {index_name} has unexpected ordering/collation"
        )
    auxiliary = [row for row in rows if int(row[5]) == 0]
    if len(auxiliary) != 1 or int(auxiliary[0][1]) != -1:
        raise ValueError(
            f"Classification index {index_name} has an unexpected rowid manifest"
        )


def _schema_layout(connection: sqlite3.Connection, *, allow_legacy: bool) -> tuple:
    objects = connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    expected_objects = {
        ("table", "classification_results", "classification_results"),
        ("index", INDEX_NAME, "classification_results"),
    }
    if {(row[0], row[1], row[2]) for row in objects} != expected_objects or len(
        objects
    ) != 2:
        raise ValueError(
            "Classification DB has an unexpected persistent-object manifest"
        )
    sql_by_name = {str(row[1]): row[3] for row in objects}
    if not all(isinstance(value, str) and value for value in sql_by_name.values()):
        raise ValueError("Classification DB schema SQL is incomplete")

    rows = connection.execute(
        "PRAGMA table_xinfo('classification_results')"
    ).fetchall()
    layout = tuple(
        (str(row[1]), str(row[2]).upper(), int(row[3]), row[4], int(row[5]))
        for row in rows
        if int(row[6]) == 0
    )
    if len(layout) != len(rows):
        raise ValueError(
            "Classification table must not contain hidden/generated columns"
        )
    accepted = _known_source_layouts() if allow_legacy else (CURRENT_COLUMNS,)
    if layout not in accepted:
        raise ValueError("Classification table does not match a known exact schema")
    allowed_table_tokens = {
        _sql_tokens(_table_sql_for_layout(candidate)) for candidate in accepted
    }
    if _sql_tokens(sql_by_name["classification_results"]) not in allowed_table_tokens:
        raise ValueError(
            "Classification table SQL does not match a known exact schema"
        )
    if _sql_tokens(sql_by_name[INDEX_NAME]) != _sql_tokens(INDEX_SQL):
        raise ValueError(
            "Classification index SQL does not match the exact manifest"
        )
    index_rows = connection.execute(
        "PRAGMA index_list('classification_results')"
    ).fetchall()
    index_manifest = {
        str(row[1]): (int(row[2]), str(row[3]), int(row[4]))
        for row in index_rows
    }
    if index_manifest != {
        INDEX_NAME: (0, "c", 0),
        "sqlite_autoindex_classification_results_1": (1, "pk", 0),
    }:
        raise ValueError(
            "Classification index list does not match the exact manifest"
        )
    _validate_index_columns(
        connection, INDEX_NAME, ("node", "test_type", "classified_at")
    )
    _validate_index_columns(
        connection,
        "sqlite_autoindex_classification_results_1",
        PRIMARY_KEY_COLUMNS,
    )
    if connection.execute(
        "PRAGMA foreign_key_list('classification_results')"
    ).fetchall():
        raise ValueError("Classification table must not contain foreign keys")
    return layout


def _quick_check(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
        raise ValueError("Classification DB failed PRAGMA quick_check")


def _projection(layout: Sequence[tuple[str, str, int, str | None, int]]) -> str:
    names = {column[0] for column in layout}
    expressions = []
    for name in CURRENT_COLUMN_NAMES:
        if name in names:
            expressions.append(f'"{name}"')
        elif name in {"n_band_degraded", "degraded_metric_fraction", "worst_pct_diff"}:
            expressions.append(f'NULL AS "{name}"')
        else:  # pragma: no cover - guarded by exact layouts
            raise AssertionError(f"No deterministic projection for {name}")
    return ", ".join(expressions)


def _project_row(
    row: Sequence[object],
    layout: Sequence[tuple[str, str, int, str | None, int]],
) -> tuple[object, ...]:
    projected = list(row)
    names = {column[0] for column in layout}
    if {
        "n_band_degraded", "degraded_metric_fraction", "worst_pct_diff"
    }.issubset(names):
        return tuple(projected)
    metrics_json = projected[CURRENT_COLUMN_NAMES.index("metrics_json")]
    n_compared = int(projected[CURRENT_COLUMN_NAMES.index("n_compared")])
    n_degraded = int(projected[CURRENT_COLUMN_NAMES.index("n_degraded")])
    band, fraction, worst = legacy_classification_scalars(
        metrics_json,
        n_compared=n_compared,
        n_degraded=n_degraded,
    )
    fallbacks = {
        "n_band_degraded": band,
        "degraded_metric_fraction": fraction,
        "worst_pct_diff": worst,
    }
    for name, value in fallbacks.items():
        if name not in names:
            projected[CURRENT_COLUMN_NAMES.index(name)] = value
    return tuple(projected)


def _new_row_digest() -> Any:
    value = hashlib.sha256(ROW_DIGEST_SCHEMA)
    for name in CURRENT_COLUMN_NAMES:
        encoded = name.encode("ascii")
        value.update(struct.pack(">I", len(encoded)))
        value.update(encoded)
    return value


def _validate_row(row: Sequence[object]) -> None:
    if len(row) != len(CURRENT_COLUMNS):
        raise ValueError("Classification row has an unexpected width")
    integer_positions = {0, 5, 6, 7, 8, 9}
    real_positions = {10, 11}
    for position, item in enumerate(row):
        if position in integer_positions:
            if not isinstance(item, int):
                raise ValueError(
                    f"Classification column {CURRENT_COLUMN_NAMES[position]} must be INTEGER"
                )
        elif position in real_positions:
            if not isinstance(item, (int, float)) or isinstance(item, bool):
                raise ValueError(
                    f"Classification column {CURRENT_COLUMN_NAMES[position]} must be REAL"
                )
        elif not isinstance(item, str):
            raise ValueError(
                f"Classification column {CURRENT_COLUMN_NAMES[position]} must be TEXT"
            )
    test_type = row[2]
    assert isinstance(test_type, str)
    if validate_operational_target_name(test_type) != test_type:
        raise ValueError(
            f"Classification test_type is not canonical: {test_type!r}"
        )


def _update_digest(value: Any, row: Sequence[object]) -> None:
    _validate_row(row)
    value.update(b"R")
    for item in row:
        if isinstance(item, int):
            encoded = b"I" + str(item).encode("ascii")
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError(
                    "Classification rows must contain finite real values"
                )
            encoded = b"F" + struct.pack(">d", item)
        elif isinstance(item, str):
            encoded = b"T" + item.encode("utf-8")
        else:
            raise ValueError(
                "Classification rows contain an unsupported SQLite value"
            )
        value.update(struct.pack(">Q", len(encoded)))
        value.update(encoded)


def _row_digest(rows: Iterable[Sequence[object]]) -> tuple[int, str]:
    value = _new_row_digest()
    count = 0
    for row in rows:
        count += 1
        _update_digest(value, row)
    return count, "sha256:" + value.hexdigest()


def _source_view(
    connection: sqlite3.Connection, *, allow_legacy: bool = True
) -> SourceView:
    layout = _schema_layout(connection, allow_legacy=allow_legacy)
    _quick_check(connection)
    cursor = connection.execute(
        f"SELECT {_projection(layout)} FROM classification_results "
        "ORDER BY test_type, classified_at, node, test_type, baseline_id"
    )
    counts: dict[str, int] = {}
    hashers: dict[str, Any] = {}
    previous_key: tuple[object, ...] | None = None
    key_positions = tuple(
        CURRENT_COLUMN_NAMES.index(name) for name in PRIMARY_KEY_COLUMNS
    )
    for raw_row in cursor:
        row = _project_row(raw_row, layout)
        _validate_row(row)
        key = tuple(row[index] for index in key_positions)
        if key == previous_key:
            raise ValueError("Classification source contains duplicate primary keys")
        previous_key = key
        test_type = str(row[2])
        if test_type not in hashers:
            hashers[test_type] = _new_row_digest()
            counts[test_type] = 0
        counts[test_type] += 1
        _update_digest(hashers[test_type], row)
    digests = {
        test_type: "sha256:" + hashers[test_type].hexdigest()
        for test_type in sorted(hashers)
    }
    return SourceView(
        layout=layout,
        counts=dict(sorted(counts.items())),
        digests=digests,
    )


def _source_rows(
    connection: sqlite3.Connection,
    layout: Sequence[tuple[str, str, int, str | None, int]],
    test_type: str,
) -> Iterable[tuple[object, ...]]:
    cursor = connection.execute(
        f"SELECT {_projection(layout)} FROM classification_results "
        "WHERE test_type=? ORDER BY classified_at, node, test_type, baseline_id",
        (test_type,),
    )
    return (_project_row(row, layout) for row in cursor)


def _target_paths(view: SourceView, config: object, source: Path) -> dict[str, Path]:
    targets: dict[str, Path] = {}
    owners: dict[Path, str] = {}
    for test_type in view.counts:
        if validate_operational_target_name(test_type) != test_type:
            raise ValueError(
                f"Classification test_type is not canonical: {test_type!r}"
            )
        if test_type in HISTORICAL_TARGET_FILENAMES:
            target = lexical_absolute(
                baseline_root_path(config) / HISTORICAL_TARGET_FILENAMES[test_type]
            )
        else:
            target = lexical_absolute(default_classification_db_path(test_type, config))
        if target == source:
            raise ValueError(
                "A per-target classification path collides with the source DB"
            )
        previous = owners.setdefault(target, test_type)
        if previous != test_type:
            raise ValueError(
                f"Classification targets {previous!r} and {test_type!r} collide"
            )
        targets[test_type] = target
    return targets


def _preflight_targets(
    targets: dict[str, Path],
    source_state: FileState,
) -> tuple[dict[str, TargetEntry], dict[Path, int]]:
    entries: dict[str, TargetEntry] = {}
    parent_fds: dict[Path, int] = {}
    try:
        for test_type, target in targets.items():
            parent = target.parent
            if parent not in parent_fds:
                opened_parent, descriptor = open_directory_no_symlinks(parent)
                if opened_parent != parent:
                    os.close(descriptor)
                    raise ValueError(f"Target parent is not canonical: {parent}")
                parent_fds[parent] = descriptor
            parent_fd = parent_fds[parent]
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            try:
                existing_fd = os.open(target.name, flags, dir_fd=parent_fd)
            except FileNotFoundError:
                entries[test_type] = TargetEntry(
                    test_type, target, parent, parent_fd, None, None
                )
                continue
            try:
                metadata = os.fstat(existing_fd)
                named = os.stat(
                    target.name, dir_fd=parent_fd, follow_symlinks=False
                )
                existing_state = FileState.from_stat(metadata)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or existing_state != FileState.from_stat(named)
                ):
                    raise ValueError(
                        f"Classification target must be an exact regular file: {target}"
                    )
                if (metadata.st_dev, metadata.st_ino) == (
                    source_state.device,
                    source_state.inode,
                ):
                    raise ValueError(
                        f"Classification target aliases the source DB: {target}"
                    )
                _assert_no_sidecars(
                    parent_fd, target.name, "Existing classification target"
                )
                entries[test_type] = TargetEntry(
                    test_type,
                    target,
                    parent,
                    parent_fd,
                    existing_fd,
                    existing_state,
                )
            except BaseException:
                os.close(existing_fd)
                raise
        return entries, parent_fds
    except BaseException:
        for entry in entries.values():
            if entry.existing_fd is not None:
                os.close(entry.existing_fd)
        for descriptor in parent_fds.values():
            os.close(descriptor)
        raise


def _plan(
    mode: str,
    source: Path,
    source_sha256: str,
    view: SourceView,
    targets: dict[str, Path],
    states: dict[str, str] | None = None,
) -> dict:
    tests = {
        test_type: {
            "rows": view.counts[test_type],
            "digest": view.digests[test_type],
            "target": str(targets[test_type]),
            **({"state": states[test_type]} if states is not None else {}),
        }
        for test_type in view.counts
    }
    return {
        "mode": mode,
        "source": str(source),
        "source_sha256": "sha256:" + source_sha256,
        "rows": view.counts,
        "digests": view.digests,
        "targets": {key: str(value) for key, value in targets.items()},
        "tests": tests,
    }


def _open_readonly(path: Path, state: FileState) -> sqlite3.Connection:
    connection = connect_sqlite_file(
        path,
        mode="ro",
        expected_identity=SQLiteFileIdentity(path, state.device, state.inode),
    )
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    return connection


def _inspect(source_arg: Path, config_path: Path | None) -> dict:
    source, parent_fd, source_fd, source_state = _open_regular(source_arg)
    try:
        _assert_no_sidecars(parent_fd, source.name, "Source classification DB")
        before_digest = _digest_fd(source_fd)
        with closing(_open_readonly(source, source_state)) as connection:
            view = _source_view(connection)
        _assert_bound_file(source, parent_fd, source_fd, source_state)
        if _digest_fd(source_fd) != before_digest:
            raise RuntimeError(
                "Source classification DB digest changed during inspection"
            )
        _assert_no_sidecars(parent_fd, source.name, "Source classification DB")
        config = load_config(config_path)
        targets = _target_paths(view, config, source)
        return _plan("inspect", source, before_digest, view, targets)
    finally:
        os.close(source_fd)
        os.close(parent_fd)


def _create_owned_file(parent_path: Path, parent_fd: int, prefix: str) -> OwnedFile:
    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    for _attempt in range(32):
        name = f".{prefix}.{secrets.token_hex(16)}"
        try:
            descriptor = os.open(name, flags, 0o600, dir_fd=parent_fd)
        except FileExistsError:
            continue
        try:
            os.fchmod(descriptor, 0o600)
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):  # pragma: no cover
                raise RuntimeError("Owned staging path is not a regular file")
            return OwnedFile(
                parent_fd,
                parent_path,
                name,
                FileState.from_stat(metadata),
            )
        finally:
            os.close(descriptor)
    raise FileExistsError(
        "Could not reserve a private classification staging name"
    )


def _remove_owned(owned: OwnedFile) -> None:
    name = owned.published_name or owned.name
    try:
        metadata = os.stat(name, dir_fd=owned.parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISREG(metadata.st_mode) or (
        metadata.st_dev,
        metadata.st_ino,
    ) != (owned.state.device, owned.state.inode):
        raise RuntimeError(
            f"Refusing to remove replaced owned path: {owned.parent_path / name}"
        )
    os.unlink(name, dir_fd=owned.parent_fd)
    for suffix in SIDECAR_SUFFIXES:
        sidecar = name + suffix
        try:
            sidecar_metadata = os.stat(
                sidecar, dir_fd=owned.parent_fd, follow_symlinks=False
            )
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(sidecar_metadata.st_mode):
            raise RuntimeError(
                f"Refusing to remove non-regular owned sidecar: {sidecar}"
            )
        os.unlink(sidecar, dir_fd=owned.parent_fd)


def _copy_snapshot(
    source_fd: int,
    source_state: FileState,
    source_parent: Path,
    source_parent_fd: int,
) -> OwnedFile:
    snapshot = _create_owned_file(
        source_parent, source_parent_fd, "cval-classification-snapshot"
    )
    flags = os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(snapshot.name, flags, dir_fd=source_parent_fd)
    try:
        os.lseek(source_fd, 0, os.SEEK_SET)
        copied = 0
        while chunk := os.read(source_fd, READ_CHUNK):
            view = memoryview(chunk)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("Short write while creating source snapshot")
                copied += written
                view = view[written:]
        if copied != source_state.size:
            raise RuntimeError(
                "Source size changed while creating immutable snapshot"
            )
        os.fsync(descriptor)
    except BaseException:
        _remove_owned(snapshot)
        raise
    finally:
        os.close(descriptor)
    metadata = os.stat(
        snapshot.name, dir_fd=source_parent_fd, follow_symlinks=False
    )
    snapshot.state = FileState.from_stat(metadata)
    return snapshot


def _digest_path(path: Path) -> str:
    absolute, parent_fd, descriptor, state = _open_regular(path)
    try:
        digest = _digest_fd(descriptor)
        _assert_bound_file(absolute, parent_fd, descriptor, state)
        return digest
    finally:
        os.close(descriptor)
        os.close(parent_fd)


def _validate_existing_target(
    entry: TargetEntry,
    snapshot: sqlite3.Connection,
    source_view: SourceView,
) -> str:
    target = entry.path
    target_parent_fd = entry.parent_fd
    target_fd = entry.existing_fd
    target_state = entry.existing_state
    test_type = entry.test_type
    if target_fd is None or target_state is None:  # pragma: no cover - caller contract
        raise AssertionError("Existing target validation requires a retained binding")
    _assert_no_sidecars(
        target_parent_fd, target.name, "Existing classification target"
    )
    with closing(_open_readonly(target, target_state)) as connection:
        _schema_layout(connection, allow_legacy=False)
        _quick_check(connection)
        foreign = connection.execute(
            "SELECT COUNT(*) FROM classification_results WHERE test_type<>?",
            (test_type,),
        ).fetchone()[0]
        if foreign:
            raise ValueError(
                f"Existing target {target} contains rows for another test_type"
            )
        source_cursor = _source_rows(snapshot, source_view.layout, test_type)
        quoted = ", ".join(f'"{name}"' for name in CURRENT_COLUMN_NAMES)
        target_cursor = connection.execute(
            f"SELECT {quoted} FROM classification_results "
            "ORDER BY classified_at, node, test_type, baseline_id"
        )
        target_iterator = iter(target_cursor)
        current_target = next(target_iterator, None)
        total = 0
        key_positions = tuple(
            CURRENT_COLUMN_NAMES.index(name) for name in PRIMARY_KEY_COLUMNS
        )
        for source_row in source_cursor:
            _validate_row(source_row)
            source_key = tuple(source_row[index] for index in key_positions)
            while current_target is not None:
                _validate_row(current_target)
                target_key = tuple(
                    current_target[index] for index in key_positions
                )
                if target_key >= source_key:
                    break
                total += 1
                current_target = next(target_iterator, None)
            if current_target is None:
                raise ValueError(f"Existing target {target} is missing source rows")
            target_key = tuple(current_target[index] for index in key_positions)
            if target_key != source_key:
                raise ValueError(f"Existing target {target} is missing source rows")
            if tuple(current_target) != tuple(source_row):
                raise ValueError(
                    f"Existing target {target} has a conflicting primary key"
                )
            total += 1
            current_target = next(target_iterator, None)
        while current_target is not None:
            _validate_row(current_target)
            total += 1
            current_target = next(target_iterator, None)
    _assert_bound_file(target, target_parent_fd, target_fd, target_state)
    _assert_no_sidecars(
        target_parent_fd, target.name, "Existing classification target"
    )
    return "exact" if total == source_view.counts[test_type] else "superset"


def _build_stage(
    target: Path,
    target_parent: Path,
    target_parent_fd: int,
    snapshot: sqlite3.Connection,
    source_view: SourceView,
    test_type: str,
) -> OwnedFile:
    stage = _create_owned_file(
        target_parent, target_parent_fd, target.name + ".split"
    )
    try:
        with closing(connect_sqlite_file(stage.path, mode="rw")) as output:
            output.execute("PRAGMA journal_mode=DELETE")
            output.execute("PRAGMA synchronous=FULL")
            output.execute("BEGIN IMMEDIATE")
            output.execute(CURRENT_TABLE_SQL)
            quoted = ", ".join(f'"{name}"' for name in CURRENT_COLUMN_NAMES)
            insert_sql = (
                f"INSERT INTO classification_results ({quoted}) VALUES ("
                + ", ".join("?" for _ in CURRENT_COLUMN_NAMES)
                + ")"
            )
            cursor = _source_rows(snapshot, source_view.layout, test_type)
            while rows := list(itertools.islice(cursor, 1000)):
                for row in rows:
                    _validate_row(row)
                output.executemany(insert_sql, rows)
            output.execute(INDEX_SQL)
            output.commit()
            _schema_layout(output, allow_legacy=False)
            _quick_check(output)
            actual_count, actual_digest = _row_digest(
                output.execute(
                    f"SELECT {quoted} FROM classification_results "
                    "ORDER BY classified_at, node, test_type, baseline_id"
                )
            )
            if actual_count != source_view.counts[test_type]:
                raise RuntimeError(f"Staged row-count mismatch for {test_type}")
            if actual_digest != source_view.digests[test_type]:
                raise RuntimeError(
                    f"Staged row-content digest mismatch for {test_type}"
                )
        _assert_no_sidecars(
            target_parent_fd, stage.name, "Staged classification target"
        )
        metadata = os.stat(
            stage.name, dir_fd=target_parent_fd, follow_symlinks=False
        )
        stage.state = FileState.from_stat(metadata)
        descriptor = os.open(
            stage.name,
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=target_parent_fd,
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return stage
    except BaseException:
        _remove_owned(stage)
        raise


def _validate_published_stage(
    stage: OwnedFile,
    target: Path,
    source_view: SourceView,
    test_type: str,
) -> None:
    if stage.published_name != target.name:
        raise RuntimeError("Published classification target is not registered")
    metadata = os.stat(
        target.name,
        dir_fd=stage.parent_fd,
        follow_symlinks=False,
    )
    if (
        not stat.S_ISREG(metadata.st_mode)
        or FileState.from_stat(metadata) != stage.state
    ):
        raise RuntimeError(f"Published classification target changed: {target}")
    _assert_no_sidecars(
        stage.parent_fd, target.name, "Published classification target"
    )
    with closing(_open_readonly(target, stage.state)) as connection:
        _schema_layout(connection, allow_legacy=False)
        _quick_check(connection)
        quoted = ", ".join(f'"{name}"' for name in CURRENT_COLUMN_NAMES)
        count, digest = _row_digest(
            connection.execute(
                f"SELECT {quoted} FROM classification_results "
                "ORDER BY classified_at, node, test_type, baseline_id"
            )
        )
    if count != source_view.counts[test_type] or digest != source_view.digests[test_type]:
        raise RuntimeError(f"Published classification target content changed: {target}")


def _apply(
    source_arg: Path,
    manifest_arg: Path,
    config_path: Path | None,
    *,
    use_backup_db: bool = False,
) -> dict:
    source, source_parent_fd, source_fd, source_state = _open_regular(source_arg)
    snapshot_file: OwnedFile | None = None
    stages: list[tuple[OwnedFile, Path, str]] = []
    parent_fds: dict[Path, int] = {}
    target_entries: dict[str, TargetEntry] = {}
    guard = InterruptGuard()
    try:
        _assert_no_sidecars(
            source_parent_fd, source.name, "Source classification DB"
        )
        authorized_digest = _digest_fd(source_fd)
        _assert_bound_file(source, source_parent_fd, source_fd, source_state)
        authorized_path = validate_backup_manifest(
            manifest_arg,
            source,
            source_state=source_state,
            source_sha256=authorized_digest,
            use_backup_db=use_backup_db,
        )
        if authorized_path != source:
            os.close(source_fd)
            os.close(source_parent_fd)
            source, source_parent_fd, source_fd, source_state = _open_regular(
                authorized_path
            )
            authorized_digest = _digest_fd(source_fd)
            _assert_bound_file(source, source_parent_fd, source_fd, source_state)
        with guard:
            try:
                snapshot_file = _copy_snapshot(
                    source_fd,
                    source_state,
                    source.parent,
                    source_parent_fd,
                )
                guard.check()
                if _digest_path(snapshot_file.path) != authorized_digest:
                    raise RuntimeError(
                        "Immutable snapshot digest does not match the authorized source"
                    )
                _assert_bound_file(
                    source, source_parent_fd, source_fd, source_state
                )
                if _digest_fd(source_fd) != authorized_digest:
                    raise RuntimeError(
                        "Source classification DB changed while snapshotting"
                    )
                _assert_no_sidecars(
                    source_parent_fd,
                    source.name,
                    "Source classification DB",
                )
                with closing(
                    _open_readonly(snapshot_file.path, snapshot_file.state)
                ) as snapshot:
                    view = _source_view(snapshot)
                    config = load_config(config_path)
                    targets = _target_paths(view, config, source)
                    target_entries, parent_fds = _preflight_targets(
                        targets, source_state
                    )
                    states: dict[str, str] = {}
                    missing: list[tuple[str, Path, Path, int]] = []
                    for test_type, entry in target_entries.items():
                        target = entry.path
                        if entry.existing_fd is None:
                            states[test_type] = "created"
                            missing.append(
                                (
                                    test_type,
                                    target,
                                    entry.parent_path,
                                    entry.parent_fd,
                                )
                            )
                            continue
                        states[test_type] = _validate_existing_target(
                            entry,
                            snapshot,
                            view,
                        )
                    for test_type, target, parent, parent_fd in missing:
                        stages.append(
                            (
                                _build_stage(
                                    target,
                                    parent,
                                    parent_fd,
                                    snapshot,
                                    view,
                                    test_type,
                                ),
                                target,
                                test_type,
                            )
                        )
                        guard.check()
                    _assert_bound_file(
                        source, source_parent_fd, source_fd, source_state
                    )
                    if _digest_fd(source_fd) != authorized_digest:
                        raise RuntimeError(
                            "Source classification DB changed before publication"
                        )
                    _assert_no_sidecars(
                        source_parent_fd,
                        source.name,
                        "Source classification DB",
                    )
                    for stage, target, _test_type in stages:
                        rename_noreplace_at(
                            stage.parent_fd,
                            stage.name,
                            stage.parent_fd,
                            target.name,
                        )
                        stage.published_name = target.name
                    for descriptor in set(parent_fds.values()):
                        os.fsync(descriptor)
                    for stage, target, test_type in stages:
                        _validate_published_stage(stage, target, view, test_type)
                    guard.check()
                    if snapshot_file is not None:
                        _remove_owned(snapshot_file)
                    result = _plan(
                        "apply",
                        source,
                        authorized_digest,
                        view,
                        targets,
                        states,
                    )
                    result["ok"] = True
                    return result
            except BaseException:
                guard.cleaning = True
                cleanup_errors = []
                for owned, _target, _test_type in reversed(stages):
                    try:
                        _remove_owned(owned)
                    except BaseException as cleanup_error:  # pragma: no cover
                        cleanup_errors.append(cleanup_error)
                if snapshot_file is not None:
                    try:
                        _remove_owned(snapshot_file)
                    except BaseException as cleanup_error:  # pragma: no cover
                        cleanup_errors.append(cleanup_error)
                if cleanup_errors:
                    raise RuntimeError(
                        "Classification split failed and owned-file cleanup was incomplete"
                    ) from cleanup_errors[0]
                raise
    finally:
        for entry in target_entries.values():
            if entry.existing_fd is not None:
                os.close(entry.existing_fd)
        for descriptor in parent_fds.values():
            os.close(descriptor)
        os.close(source_fd)
        os.close(source_parent_fd)


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not args.apply:
        if args.use_backup_db:
            raise ValueError("--use-backup-db requires --apply and --backup-manifest")
        result = _inspect(args.source, args.config)
    else:
        if args.confirm != CONFIRMATION:
            raise ValueError(f"Apply requires exact --confirm {CONFIRMATION}")
        if args.backup_manifest is None:
            raise ValueError("Apply requires --backup-manifest")
        result = _apply(
            args.source,
            args.backup_manifest,
            args.config,
            use_backup_db=args.use_backup_db,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
