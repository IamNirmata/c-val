"""Dry-run-first, lock-aware backup of disposable U7/U8 evaluator copies."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
from contextlib import ExitStack, closing
from dataclasses import replace
from pathlib import Path
from typing import Any

from cval.config import CvalConfig
from cval.health.evaluator import evaluator_test_lock
from cval.health.storage import load_baselines_generation, resolve_health_db_path
from cval.storage.per_test_results import (
    common_result_schema_version,
    resolve_test_results_db_path,
    validate_common_result_connection,
)
from cval.storage.sqlite_snapshot import (
    immutable_sqlite_snapshot,
    read_regular_file_without_atime,
)


BACKUP_SCHEMA = "cval.evaluator-backup.v1"
BACKUP_CONFIRMATION = "backup"
_DATABASE_MODE = 0o600
_DIRECTORY_UNSAFE_BITS = stat.S_IWGRP | stat.S_IWOTH


def backup_local_evaluator_state(
    config: CvalConfig,
    *,
    source_root: str | Path,
    destination: str | Path,
    apply: bool = False,
    confirmation: str | None = None,
) -> dict[str, Any]:
    """Plan or execute a backup only from an explicit non-live local copy."""

    source = _canonical_directory(Path(source_root), description="Backup source root")
    configured_root = _canonical_runtime_root(Path(config.runtime.validation_root))
    if source == configured_root or configured_root in source.parents:
        raise ValueError(
            "Backup refuses the configured runtime root or any descendant; "
            "use a separately copied local source"
        )
    target = _canonical_target(Path(destination))
    if target == configured_root or configured_root in target.parents:
        raise ValueError("Backup destination must be outside runtime.validation_root")
    if source == target or source in target.parents or target in source.parents:
        raise ValueError("Backup source and destination must be disjoint directory trees")
    if not apply and confirmation is not None:
        raise ValueError("Backup confirmation is valid only with --apply")
    if apply and confirmation != BACKUP_CONFIRMATION:
        raise ValueError("Backup apply requires exact confirmation 'backup'")

    local_config = replace(
        config,
        runtime=replace(config.runtime, validation_root=str(source)),
    )
    units = _discover_units(local_config)
    if not units:
        raise ValueError("No enabled health evaluator units were found")
    plan = [_inventory_unit(unit) for unit in units]
    report: dict[str, Any] = {
        "schema_version": BACKUP_SCHEMA,
        "ok": all(unit["ok"] for unit in plan),
        "mode": "apply" if apply else "dry-run",
        "source_root": str(source),
        "destination": str(target),
        "unit_count": len(plan),
        "units": plan,
        "executed": False,
        "restore_validated": False,
        "limitations": [
            "This API requires source and destination outside the configured runtime root and is for local/disposable copies only.",
            "Activation keys are copied as paired owner-only files; key bytes and key hashes are never reported.",
            "A local backup does not authorize or perform any live PVC backup, restore, evaluator apply, or cutover.",
        ],
    }
    if not report["ok"] or not apply:
        return report
    reservation = _reserve_destination(target)
    try:
        with ExitStack() as locks:
            lock_guards = []
            for unit in units:
                lock_guards.append(
                    locks.enter_context(
                        evaluator_test_lock(
                            unit["result_path"],
                            timeout_seconds=local_config.health_evaluator.lock_timeout_seconds,
                        )
                    )
                )
            for guard in lock_guards:
                guard()
            locked_plan = [_inventory_unit(unit) for unit in units]
            if locked_plan != plan:
                raise RuntimeError("Evaluator source inventory changed before backup lock")
            for unit, expected in zip(units, locked_plan, strict=True):
                for guard in lock_guards:
                    guard()
                _copy_unit(
                    unit,
                    source=source,
                    destination=target,
                    expected=expected,
                )
            final_source = [_inventory_unit(unit) for unit in units]
            for guard in lock_guards:
                guard()
            if final_source != locked_plan:
                raise RuntimeError("Evaluator source inventory changed while backup was copied")
            final_inventory = [
                _validate_copied_unit(unit, source, target, expected)
                for unit, expected in zip(units, locked_plan, strict=True)
            ]
            manifest = {
                "schema_version": BACKUP_SCHEMA,
                "source_root": str(source),
                "units": final_inventory,
                "restore_validated": True,
                "activation_key_hashes_recorded": False,
            }
            manifest_path = target / "inventory.json"
            manifest_path.write_text(
                json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            os.chmod(manifest_path, 0o600)
            _fsync_tree(target)
            _assert_directory_identity(target, reservation)
            _fsync_directory(target.parent)
    except BaseException:
        _remove_tree_if_identity(target, reservation)
        raise

    report.update(
        {
            "ok": True,
            "executed": True,
            "restore_validated": True,
            "inventory_path": str(target / "inventory.json"),
            "units": final_inventory,
        }
    )
    return report


def _discover_units(config: CvalConfig) -> list[dict[str, Any]]:
    units: list[dict[str, Any]] = []
    for registered in config.tests.registry.enabled:
        plugin = registered.definition.plugin
        health = registered.definition.health
        if not (
            plugin
            and health
            and health.enabled
            and {"health", "ingest"}.issubset(plugin.capabilities)
        ):
            continue
        result_path = resolve_test_results_db_path(config.runtime.validation_root, registered)
        health_path = resolve_health_db_path(config.runtime.validation_root, registered)
        units.append(
            {
                "test_id": registered.id,
                "result_path": result_path,
                "health_path": health_path,
            }
        )
    return units


def _inventory_unit(unit: dict[str, Any]) -> dict[str, Any]:
    result_path: Path = unit["result_path"]
    health_path: Path = unit["health_path"]
    key_path = health_path.with_name(f"{health_path.name}.activation.key")
    errors: list[str] = []
    result_inventory: dict[str, Any] | None = None
    health_inventory: dict[str, Any] | None = None
    key_inventory: dict[str, Any] | None = None
    try:
        result_inventory = _sqlite_inventory(result_path, kind="u7")
    except Exception as exc:  # noqa: BLE001 - complete dry-run report
        errors.append("U7: " + _safe_error(exc))
    if health_path.exists() or health_path.is_symlink() or key_path.exists() or key_path.is_symlink():
        try:
            health_inventory = _sqlite_inventory(health_path, kind="u8", test_id=unit["test_id"])
            key_inventory = _key_inventory(key_path)
        except Exception as exc:  # noqa: BLE001
            errors.append("U8 pair: " + _safe_error(exc))
    return {
        "test_id": unit["test_id"],
        "ok": not errors,
        "result_db": result_inventory,
        "health_db": health_inventory,
        "activation_key": key_inventory,
        "errors": errors,
    }


def _sqlite_inventory(path: Path, *, kind: str, test_id: str | None = None) -> dict[str, Any]:
    metadata = _owned_file_metadata(path, exact_mode=_DATABASE_MODE)
    _require_absent_sqlite_sidecars(path)
    schema_version: int
    baseline_count: int | None = None
    with immutable_sqlite_snapshot(path) as snapshot, closing(snapshot.connect()) as connection:
        logical = _logical_sqlite_inventory(connection)
        if kind == "u7":
            validate_common_result_connection(connection)
            schema_version = common_result_schema_version(connection)
        else:
            stored, _generation = load_baselines_generation(db_path=path, test_id=test_id)
            schema_version = 1
            baseline_count = len(stored)
    _require_absent_sqlite_sidecars(path)
    raw = read_regular_file_without_atime(path, description=f"{kind.upper()} database")
    _require_absent_sqlite_sidecars(path)
    if _owned_file_metadata(path, exact_mode=_DATABASE_MODE) != metadata:
        raise RuntimeError(f"{kind.upper()} database identity changed during inventory")
    return {
        "path": str(path),
        **metadata,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "schema_version": schema_version,
        "object_count": logical["object_count"],
        "baseline_count": baseline_count,
        "logical_inventory": logical,
        "sidecars": [],
    }


def _key_inventory(path: Path) -> dict[str, Any]:
    metadata = _owned_file_metadata(path, exact_mode=0o600, exact_size=32)
    # Verify a side-effect-free read but intentionally discard bytes and never hash them.
    read_regular_file_without_atime(
        path,
        expected_mode=0o600,
        expected_size=32,
        description="health activation key",
    )
    return {
        "path": str(path),
        **metadata,
        "paired": True,
        "content_hash_recorded": False,
    }


def _copy_unit(
    unit: dict[str, Any],
    *,
    source: Path,
    destination: Path,
    expected: dict[str, Any],
) -> None:
    result_path: Path = unit["result_path"]
    health_path: Path = unit["health_path"]
    _copy_sqlite(
        result_path,
        destination / result_path.relative_to(source),
        expected=expected["result_db"],
    )
    if expected["health_db"] is not None:
        health_target = destination / health_path.relative_to(source)
        key_path = health_path.with_name(f"{health_path.name}.activation.key")
        _copy_sqlite(health_path, health_target, expected=expected["health_db"])
        _copy_key(key_path, health_target.with_name(f"{health_target.name}.activation.key"))


def _copy_sqlite(source: Path, destination: Path, *, expected: dict[str, Any]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _validate_created_directory_chain(destination.parent)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"Backup target already exists: {destination}")
    if _owned_file_metadata(source, exact_mode=_DATABASE_MODE) != {
        key: expected[key]
        for key in ("owner_uid", "owner_gid", "mode", "size", "type", "device", "inode", "link_count")
    }:
        raise RuntimeError(f"SQLite source identity changed before copy: {source}")
    _require_absent_sqlite_sidecars(source)
    with immutable_sqlite_snapshot(source) as snapshot, closing(
        snapshot.connect()
    ) as source_connection, closing(sqlite3.connect(destination)) as target_connection:
        source_connection.backup(target_connection, pages=256, sleep=0.01)
        target_connection.commit()
        if target_connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise RuntimeError("Copied SQLite database failed integrity_check")
    _require_absent_sqlite_sidecars(source)
    os.chmod(destination, _DATABASE_MODE)
    _fsync_file(destination)


def _copy_key(source: Path, destination: Path) -> None:
    value = read_regular_file_without_atime(
        source,
        expected_mode=0o600,
        expected_size=32,
        description="health activation key",
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(destination, flags, 0o600)
    try:
        view = memoryview(value)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_copied_unit(
    unit: dict[str, Any],
    source: Path,
    destination: Path,
    expected: dict[str, Any],
) -> dict[str, Any]:
    copied = {
        "test_id": unit["test_id"],
        "result_path": destination / unit["result_path"].relative_to(source),
        "health_path": destination / unit["health_path"].relative_to(source),
    }
    inventory = _inventory_unit(copied)
    if not inventory["ok"]:
        raise RuntimeError(
            f"Copied evaluator unit {unit['test_id']} failed restore validation: "
            + "; ".join(inventory["errors"])
        )
    _assert_unit_logical_match(expected, inventory)
    return inventory


def _owned_file_metadata(
    path: Path,
    *,
    exact_mode: int | None = None,
    exact_size: int | None = None,
) -> dict[str, Any]:
    metadata = os.lstat(path)
    mode = stat.S_IMODE(metadata.st_mode)
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise RuntimeError(f"Backup source is not a regular no-symlink file: {path}")
    if metadata.st_uid != os.geteuid():
        raise RuntimeError(f"Backup source is not owned by evaluator uid: {path}")
    if metadata.st_nlink != 1:
        raise RuntimeError(f"Backup source must have exactly one hard link: {path}")
    if exact_mode is not None and mode != exact_mode:
        raise RuntimeError(f"Backup source mode must be {exact_mode:04o}: {path}")
    if exact_size is not None and metadata.st_size != exact_size:
        raise RuntimeError(f"Backup source size must be {exact_size}: {path}")
    return {
        "owner_uid": metadata.st_uid,
        "owner_gid": metadata.st_gid,
        "mode": f"{mode:04o}",
        "size": metadata.st_size,
        "type": "regular",
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "link_count": metadata.st_nlink,
    }


def _canonical_directory(path: Path, *, description: str) -> Path:
    lexical = _absolute_lexical_path(path, description=description)
    _reject_symlink_components(lexical, include_leaf=True, description=description)
    value = lexical.resolve(strict=True)
    metadata = os.lstat(value)
    if not stat.S_ISDIR(metadata.st_mode) or value.is_symlink():
        raise ValueError(f"{description} must be a canonical directory")
    if metadata.st_uid != os.geteuid():
        raise ValueError(f"{description} must be owned by the evaluator uid")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode & _DIRECTORY_UNSAFE_BITS:
        raise ValueError(f"{description} must not be group/world writable")
    if not mode & stat.S_IRUSR or not mode & stat.S_IXUSR:
        raise ValueError(f"{description} must be owner-readable and searchable")
    return value


def _canonical_target(path: Path) -> Path:
    value = _absolute_lexical_path(path, description="Backup destination")
    _reject_symlink_components(value, include_leaf=True, description="Backup destination")
    parent = _canonical_directory(value.parent, description="Backup destination parent")
    if not os.access(parent, os.W_OK):
        raise ValueError("Backup destination parent must be writable by the evaluator uid")
    target = parent / value.name
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"Backup destination already exists: {target}")
    return target


def _canonical_runtime_root(path: Path) -> Path:
    value = _absolute_lexical_path(path, description="runtime.validation_root")
    _reject_symlink_components(
        value,
        include_leaf=True,
        description="runtime.validation_root",
    )
    return value.resolve(strict=False)


def _absolute_lexical_path(path: Path, *, description: str) -> Path:
    raw = os.path.expanduser(os.fspath(path))
    if not os.path.isabs(raw):
        raw = os.path.join(os.fspath(Path.cwd()), raw)
    components = raw.split(os.sep)
    if any(component in {".", ".."} for component in components):
        raise ValueError(f"{description} must not contain traversal components")
    if any(component == "" for component in components[1:]):
        raise ValueError(f"{description} must be lexical-canonical")
    value = Path(raw)
    if value.name in {"", ".", ".."}:
        raise ValueError(f"{description} must name a concrete path")
    return value


def _reject_symlink_components(
    path: Path,
    *,
    include_leaf: bool,
    description: str,
) -> None:
    current = Path(path.anchor)
    parts = path.parts[1:] if path.is_absolute() else path.parts
    selected = parts if include_leaf else parts[:-1]
    for part in selected:
        current = current / part
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            break
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"{description} contains a symlink component: {current}")


def _reserve_destination(path: Path) -> tuple[int, int]:
    try:
        os.mkdir(path, 0o700)
    except FileExistsError as exc:
        raise FileExistsError(f"Backup destination already exists: {path}") from exc
    metadata = os.lstat(path)
    identity = (metadata.st_dev, metadata.st_ino)
    try:
        _assert_directory_identity(path, identity)
        _fsync_directory(path.parent)
    except BaseException:
        _remove_tree_if_identity(path, identity)
        raise
    return identity


def _assert_directory_identity(path: Path, identity: tuple[int, int]) -> None:
    metadata = os.lstat(path)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or (metadata.st_dev, metadata.st_ino) != identity
    ):
        raise RuntimeError("Backup destination reservation identity changed")


def _remove_tree_if_identity(path: Path, identity: tuple[int, int]) -> None:
    try:
        _assert_directory_identity(path, identity)
    except FileNotFoundError:
        return
    except RuntimeError:
        return
    try:
        shutil.rmtree(path)
    finally:
        _fsync_directory(path.parent)


def _validate_created_directory_chain(path: Path) -> None:
    metadata = os.lstat(path)
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or mode & _DIRECTORY_UNSAFE_BITS
    ):
        raise RuntimeError(f"Backup output directory is unsafe: {path}")


def _require_absent_sqlite_sidecars(path: Path) -> None:
    sidecars = [
        candidate.name
        for candidate in (
            path.with_name(path.name + "-wal"),
            path.with_name(path.name + "-shm"),
            path.with_name(path.name + "-journal"),
        )
        if candidate.exists() or candidate.is_symlink()
    ]
    if sidecars:
        raise RuntimeError("SQLite source has sidecars: " + ", ".join(sidecars))


def _logical_sqlite_inventory(connection: sqlite3.Connection) -> dict[str, Any]:
    objects = [
        {
            "type": str(row[0]),
            "name": str(row[1]),
            "table": str(row[2]),
            "sql": row[3],
        }
        for row in connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        )
    ]
    schema_payload = json.dumps(
        objects,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    tables: list[dict[str, Any]] = []
    for item in objects:
        if item["type"] != "table":
            continue
        table_name = item["name"]
        quoted_table = _quote_identifier(table_name)
        columns = [
            (str(row[1]), int(row[5]))
            for row in connection.execute(f"PRAGMA table_info({quoted_table})")
        ]
        if not columns:
            raise RuntimeError(f"SQLite table has no columns: {table_name}")
        column_names = [name for name, _pk_position in columns]
        primary_key = [
            name
            for name, pk_position in sorted(columns, key=lambda value: value[1] or 1 << 30)
            if pk_position
        ]
        without_rowid = "WITHOUT ROWID" in str(item["sql"] or "").upper()
        identity_columns = primary_key or ([] if without_rowid else ["rowid"])
        if not identity_columns:
            raise RuntimeError(f"SQLite WITHOUT ROWID table lacks a primary key: {table_name}")
        selected = identity_columns + column_names
        query = (
            "SELECT "
            + ", ".join(_quote_identifier(name) for name in selected)
            + f" FROM {quoted_table} ORDER BY "
            + ", ".join(_quote_identifier(name) for name in identity_columns)
        )
        content_digest = hashlib.sha256()
        identity_digest = hashlib.sha256()
        row_count = 0
        identity_width = len(identity_columns)
        for row in connection.execute(query):
            row_count += 1
            _update_row_digest(identity_digest, row[:identity_width])
            _update_row_digest(content_digest, row[identity_width:])
        tables.append(
            {
                "name": table_name,
                "column_count": len(column_names),
                "identity_columns": identity_columns,
                "row_count": row_count,
                "row_identity_sha256": identity_digest.hexdigest(),
                "content_sha256": content_digest.hexdigest(),
            }
        )
    return {
        "object_count": len(objects),
        "schema_sha256": hashlib.sha256(schema_payload).hexdigest(),
        "tables": tables,
    }


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _update_row_digest(digest: Any, values: Any) -> None:
    digest.update(len(values).to_bytes(4, "big"))
    for value in values:
        encoded = _encode_sql_value(value)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)


def _encode_sql_value(value: Any) -> bytes:
    if value is None:
        return b"n"
    if isinstance(value, bytes):
        return b"b" + value
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii")
    if isinstance(value, float):
        return b"f" + value.hex().encode("ascii")
    if isinstance(value, str):
        return b"s" + value.encode("utf-8")
    raise RuntimeError(f"Unsupported SQLite value type in backup inventory: {type(value).__name__}")


def _assert_unit_logical_match(
    source_inventory: dict[str, Any],
    copied_inventory: dict[str, Any],
) -> None:
    for label in ("result_db", "health_db"):
        source = source_inventory[label]
        copied = copied_inventory[label]
        if (source is None) != (copied is None):
            raise RuntimeError(f"Copied evaluator {label} presence differs from source")
        if source is None:
            continue
        for field in ("schema_version", "object_count", "baseline_count", "logical_inventory"):
            if source[field] != copied[field]:
                raise RuntimeError(
                    f"Copied evaluator {label} {field} differs from source"
                )


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    for directory, _subdirs, files in os.walk(root, topdown=False):
        current = Path(directory)
        for filename in files:
            _fsync_file(current / filename)
        _fsync_directory(current)


def _safe_error(exc: BaseException) -> str:
    return " ".join((str(exc).strip() or exc.__class__.__name__).splitlines())


__all__ = [
    "BACKUP_CONFIRMATION",
    "BACKUP_SCHEMA",
    "backup_local_evaluator_state",
]
