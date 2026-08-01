"""Dry-run-first, lock-aware backup of disposable U7/U8 evaluator copies."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from cval.config import CvalConfig
from cval.evaluator.secure_state import (
    create_published_directory_at,
    remove_entry_if_identity_at,
)
from cval.evaluator.signals import defer_creation_signals
from cval.evaluator.state import (
    StateFileIdentity,
    bind_state_target,
    inspect_state_target,
    open_state_root,
    state_test_lock,
)
from cval.health.storage import load_baselines_generation, resolve_health_db_path
from cval.storage.per_test_results import (
    common_result_schema_version,
    resolve_test_results_db_path,
    validate_common_result_connection,
)
from cval.storage.sqlite_snapshot import (
    immutable_sqlite_snapshot,
    read_regular_file_descriptor_without_atime,
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
    configured_runtime_root = _canonical_runtime_root(Path(config.runtime.validation_root))
    configured_state_root = _canonical_runtime_root(
        Path(config.health_evaluator.state_root)
    )
    protected_roots = (configured_runtime_root, configured_state_root)
    if any(_paths_overlap(source, root) for root in protected_roots):
        raise ValueError(
            "Backup refuses the configured live shared/state root or any descendant; "
            "ancestors are rejected too; "
            "use a separately copied local source"
        )
    target = _absolute_lexical_path(
        Path(destination),
        description="Backup destination",
    )
    if any(_paths_overlap(target, root) for root in protected_roots):
        raise ValueError(
            "Backup destination must be outside the configured live shared/state roots"
        )
    if source == target or source in target.parents or target in source.parents:
        raise ValueError("Backup source and destination must be disjoint directory trees")
    if not apply and confirmation is not None:
        raise ValueError("Backup confirmation is valid only with --apply")
    if apply and confirmation != BACKUP_CONFIRMATION:
        raise ValueError("Backup apply requires exact confirmation 'backup'")

    local_config = replace(
        config,
        health_evaluator=replace(
            config.health_evaluator,
            state_root=str(source),
        ),
    )
    with open_state_root(local_config, require_writable=apply):
        pass
    units = _discover_units(local_config, require_writable=apply)
    if not units:
        raise ValueError("No enabled health evaluator units were found")
    plan = [_inventory_unit(unit, config=local_config) for unit in units]
    destination_binding = _bind_destination_target(target)
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
            "This API requires source and destination outside the configured live shared/state roots and is for local/disposable copies only.",
            "Activation keys are copied as paired owner-only files; key bytes and key hashes are never reported.",
            "A local backup does not authorize or perform any live PVC backup, restore, evaluator apply, or cutover.",
        ],
    }
    if not report["ok"] or not apply:
        destination_binding.close()
        return report
    try:
        reservation = _reserve_destination(destination_binding)
    except BaseException:
        destination_binding.close()
        raise
    completed = False
    try:
        with ExitStack() as locks:
            lock_guards = []
            for unit in units:
                lock_guards.append(
                    locks.enter_context(
                        state_test_lock(local_config, unit["result_path"])
                    )
                )
            for guard in lock_guards:
                guard()
            locked_plan = [_inventory_unit(unit, config=local_config) for unit in units]
            if locked_plan != plan:
                raise RuntimeError("Evaluator source inventory changed before backup lock")
            for unit, expected in zip(units, locked_plan, strict=True):
                for guard in lock_guards:
                    guard()
                reservation.assert_binding()
                _copy_unit(
                    unit,
                    source=source,
                    destination=reservation,
                    expected=expected,
                    config=local_config,
                )
                reservation.assert_binding()
            reservation.assert_binding()
            final_source = [_inventory_unit(unit, config=local_config) for unit in units]
            for guard in lock_guards:
                guard()
            if final_source != locked_plan:
                raise RuntimeError("Evaluator source inventory changed while backup was copied")
            reservation.assert_binding()
            final_inventory = [
                _validate_copied_unit(unit, source, reservation, expected)
                for unit, expected in zip(units, locked_plan, strict=True)
            ]
            reservation.assert_binding()
            manifest = {
                "schema_version": BACKUP_SCHEMA,
                "source_root": str(source),
                "units": final_inventory,
                "restore_validated": True,
                "activation_key_hashes_recorded": False,
            }
            manifest_path = target / "inventory.json"
            _write_destination_bytes(
                reservation,
                Path("inventory.json"),
                (
                    json.dumps(manifest, sort_keys=True, separators=(",", ":"))
                    + "\n"
                ).encode("utf-8"),
            )
            reservation.assert_binding()
            _fsync_destination_tree(reservation, reservation.root_fd)
            reservation.assert_binding()
            _fsync_destination_descriptor(reservation, reservation.parent_fd)
            reservation.assert_binding()
            report.update(
                {
                    "ok": True,
                    "executed": True,
                    "restore_validated": True,
                    "inventory_path": str(target / "inventory.json"),
                    "units": final_inventory,
                }
            )
            reservation.assert_binding()
            completed = True
    except BaseException as primary_error:
        try:
            _remove_tree_if_identity(reservation)
        except BaseException as cleanup_error:
            _add_cleanup_note(
                primary_error,
                "Backup failure cleanup failed closed",
                cleanup_error,
            )
        raise
    finally:
        reservation.close()
    if completed:
        try:
            _assert_published_destination_path(reservation)
        except BaseException as primary_error:
            try:
                _remove_published_tree_if_identity(reservation)
            except BaseException as cleanup_error:
                _add_cleanup_note(
                    primary_error,
                    "Backup post-close cleanup failed closed",
                    cleanup_error,
                )
            raise
    return report


def _discover_units(
    config: CvalConfig,
    *,
    require_writable: bool,
) -> list[dict[str, Any]]:
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
        result_path = resolve_test_results_db_path(
            config.health_evaluator.state_root,
            registered,
        )
        health_path = resolve_health_db_path(
            config.health_evaluator.state_root,
            registered,
        )
        inspect_state_target(
            config,
            result_path,
            allow_missing=False,
            require_writable=require_writable,
        )
        inspect_state_target(
            config,
            health_path,
            allow_missing=True,
            require_writable=require_writable,
        )
        inspect_state_target(
            config,
            health_path.with_name(f"{health_path.name}.activation.key"),
            allow_missing=True,
            require_writable=require_writable,
        )
        units.append(
            {
                "test_id": registered.id,
                "result_path": result_path,
                "health_path": health_path,
            }
        )
    return units


def _inventory_unit(
    unit: dict[str, Any],
    *,
    config: CvalConfig | None = None,
) -> dict[str, Any]:
    result_path: Path = unit["result_path"]
    health_path: Path = unit["health_path"]
    key_path = health_path.with_name(f"{health_path.name}.activation.key")
    errors: list[str] = []
    result_inventory: dict[str, Any] | None = None
    health_inventory: dict[str, Any] | None = None
    key_inventory: dict[str, Any] | None = None
    with ExitStack() as bindings:
        result_binding = (
            bindings.enter_context(
                bind_state_target(
                    config,
                    result_path,
                    create=False,
                    allow_missing=False,
                    writable=False,
                    require_writable=False,
                )
            )
            if config is not None
            else None
        )
        health_binding = (
            bindings.enter_context(
                bind_state_target(
                    config,
                    health_path,
                    create=False,
                    allow_missing=True,
                    writable=False,
                    require_writable=False,
                )
            )
            if config is not None
            else None
        )
        key_binding = (
            bindings.enter_context(
                bind_state_target(
                    config,
                    key_path,
                    create=False,
                    allow_missing=True,
                    writable=False,
                    require_writable=False,
                )
            )
            if config is not None
            else None
        )
        try:
            result_inventory = _sqlite_inventory(
                result_path,
                kind="u7",
                expected_identity=(
                    result_binding.sqlite_identity
                    if result_binding is not None
                    else None
                ),
                state_binding=result_binding,
            )
        except Exception as exc:  # noqa: BLE001 - complete dry-run report
            errors.append("U7: " + _safe_error(exc))
        health_present = (
            health_binding.identity is not None
            if health_binding is not None
            else health_path.exists() or health_path.is_symlink()
        )
        key_present = (
            key_binding.identity is not None
            if key_binding is not None
            else key_path.exists() or key_path.is_symlink()
        )
        if health_present or key_present:
            try:
                health_inventory = _sqlite_inventory(
                    health_path,
                    kind="u8",
                    test_id=unit["test_id"],
                    expected_identity=(
                        health_binding.sqlite_identity
                        if health_binding is not None
                        else None
                    ),
                    state_binding=health_binding,
                    key_binding=key_binding,
                )
                key_inventory = _key_inventory(
                    key_path,
                    expected_identity=(
                        key_binding.sqlite_identity
                        if key_binding is not None
                        else None
                    ),
                    state_binding=key_binding,
                )
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


def _sqlite_inventory(
    path: Path,
    *,
    kind: str,
    test_id: str | None = None,
    expected_identity=None,
    state_binding=None,
    key_binding=None,
) -> dict[str, Any]:
    metadata = (
        _owned_binding_metadata(state_binding, exact_mode=_DATABASE_MODE)
        if state_binding is not None
        else _owned_file_metadata(path, exact_mode=_DATABASE_MODE)
    )
    _require_absent_sqlite_sidecars(path, state_binding=state_binding)
    schema_version: int
    baseline_count: int | None = None
    with immutable_sqlite_snapshot(
        path,
        expected_identity=expected_identity,
        source_fd=(state_binding.descriptor if state_binding is not None else None),
        source_parent_fd=(
            state_binding.directory.parent_fd if state_binding is not None else None
        ),
        source_name=(state_binding.name if state_binding is not None else None),
        binding_guard=(
            state_binding.assert_path_binding if state_binding is not None else None
        ),
    ) as snapshot, closing(snapshot.connect()) as connection:
        logical = _logical_sqlite_inventory(connection)
        if kind == "u7":
            validate_common_result_connection(connection)
            schema_version = common_result_schema_version(connection)
        else:
            stored, _generation = load_baselines_generation(
                db_path=path,
                test_id=test_id,
                expected_identity=expected_identity,
                state_binding=state_binding,
                key_binding=key_binding,
            )
            schema_version = 1
            baseline_count = len(stored)
    _require_absent_sqlite_sidecars(path, state_binding=state_binding)
    raw = (
        read_regular_file_descriptor_without_atime(
            state_binding.descriptor,
            path=path,
            description=f"{kind.upper()} database",
            expected_identity=expected_identity,
            expected_mode=_DATABASE_MODE,
            binding_guard=state_binding.assert_path_binding,
        )
        if state_binding is not None
        else read_regular_file_without_atime(
            path,
            description=f"{kind.upper()} database",
            expected_identity=expected_identity,
        )
    )
    _require_absent_sqlite_sidecars(path, state_binding=state_binding)
    current_metadata = (
        _owned_binding_metadata(state_binding, exact_mode=_DATABASE_MODE)
        if state_binding is not None
        else _owned_file_metadata(path, exact_mode=_DATABASE_MODE)
    )
    if current_metadata != metadata:
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


def _key_inventory(
    path: Path,
    *,
    expected_identity=None,
    state_binding=None,
) -> dict[str, Any]:
    metadata = (
        _owned_binding_metadata(state_binding, exact_mode=0o600, exact_size=32)
        if state_binding is not None
        else _owned_file_metadata(path, exact_mode=0o600, exact_size=32)
    )
    # Verify a side-effect-free read but intentionally discard bytes and never hash them.
    if state_binding is not None:
        read_regular_file_descriptor_without_atime(
            state_binding.descriptor,
            path=path,
            expected_mode=0o600,
            expected_size=32,
            description="health activation key",
            expected_identity=expected_identity,
            binding_guard=state_binding.assert_path_binding,
        )
    else:
        read_regular_file_without_atime(
            path,
            expected_mode=0o600,
            expected_size=32,
            description="health activation key",
            expected_identity=expected_identity,
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
    destination: "_DestinationReservation",
    expected: dict[str, Any],
    config: CvalConfig,
) -> None:
    result_path: Path = unit["result_path"]
    health_path: Path = unit["health_path"]
    with ExitStack() as bindings:
        result_binding = bindings.enter_context(
            bind_state_target(
                config,
                result_path,
                create=False,
                allow_missing=False,
                writable=False,
                require_writable=True,
            )
        )
        _copy_sqlite(
            result_path,
            destination.path / result_path.relative_to(source),
            expected=expected["result_db"],
            expected_identity=result_binding.sqlite_identity,
            source_binding=result_binding,
            reservation=destination,
        )
        if expected["health_db"] is not None:
            health_target = destination.path / health_path.relative_to(source)
            key_path = health_path.with_name(f"{health_path.name}.activation.key")
            health_binding = bindings.enter_context(
                bind_state_target(
                    config,
                    health_path,
                    create=False,
                    allow_missing=False,
                    writable=False,
                    require_writable=True,
                )
            )
            key_binding = bindings.enter_context(
                bind_state_target(
                    config,
                    key_path,
                    create=False,
                    allow_missing=False,
                    writable=False,
                    require_writable=True,
                )
            )
            _copy_sqlite(
                health_path,
                health_target,
                expected=expected["health_db"],
                expected_identity=health_binding.sqlite_identity,
                source_binding=health_binding,
                reservation=destination,
            )
            _copy_key(
                key_path,
                health_target.with_name(f"{health_target.name}.activation.key"),
                expected_identity=key_binding.sqlite_identity,
                source_binding=key_binding,
                reservation=destination,
            )


def _copy_sqlite(
    source: Path,
    destination: Path,
    *,
    expected: dict[str, Any],
    expected_identity=None,
    source_binding=None,
    reservation: "_DestinationReservation",
) -> None:
    target_context = _reserved_destination_file(
        reservation,
        destination.relative_to(reservation.path),
    )
    source_metadata = (
        _owned_binding_metadata(source_binding, exact_mode=_DATABASE_MODE)
        if source_binding is not None
        else _owned_file_metadata(source, exact_mode=_DATABASE_MODE)
    )
    if source_metadata != {
        key: expected[key]
        for key in ("owner_uid", "owner_gid", "mode", "size", "type", "device", "inode", "link_count")
    }:
        raise RuntimeError(f"SQLite source identity changed before copy: {source}")
    _require_absent_sqlite_sidecars(source, state_binding=source_binding)
    with target_context as (
        parent_fd,
        target_open_path,
        target_identity,
        target_artifacts,
    ):
        try:
            with immutable_sqlite_snapshot(
                source,
                expected_identity=expected_identity,
                source_fd=(source_binding.descriptor if source_binding is not None else None),
                source_parent_fd=(
                    source_binding.directory.parent_fd
                    if source_binding is not None
                    else None
                ),
                source_name=(source_binding.name if source_binding is not None else None),
                binding_guard=(
                    source_binding.assert_path_binding
                    if source_binding is not None
                    else None
                ),
            ) as snapshot, closing(
                snapshot.connect()
            ) as source_connection, closing(
                sqlite3.connect(target_open_path)
            ) as target_connection:
                journal_mode = target_connection.execute(
                    "PRAGMA journal_mode=OFF"
                ).fetchone()
                if journal_mode != ("off",):
                    raise RuntimeError(
                        "Backup destination SQLite copy could not disable journaling"
                    )
                target_connection.execute("PRAGMA synchronous=FULL")
                source_connection.backup(target_connection, pages=256, sleep=0.01)
                target_connection.commit()
                if target_connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                    raise RuntimeError("Copied SQLite database failed integrity_check")
        finally:
            if parent_fd is not None and target_artifacts is not None:
                _capture_destination_sidecars(
                    parent_fd,
                    destination.name,
                    target_artifacts,
                    reservation=reservation,
                    parent_parts=destination.relative_to(reservation.path).parts[:-1],
                )
        if target_artifacts is not None and len(target_artifacts) != 1:
            raise RuntimeError("Backup destination SQLite copy retained sidecars")
        if parent_fd is not None and target_identity is not None:
            _assert_entry_identity(parent_fd, destination.name, target_identity)
    _require_absent_sqlite_sidecars(source, state_binding=source_binding)
def _copy_key(
    source: Path,
    destination: Path,
    *,
    expected_identity=None,
    source_binding=None,
    reservation: "_DestinationReservation",
) -> None:
    value = (
        read_regular_file_descriptor_without_atime(
            source_binding.descriptor,
            path=source,
            expected_mode=0o600,
            expected_size=32,
            description="health activation key",
            expected_identity=expected_identity,
            binding_guard=source_binding.assert_path_binding,
        )
        if source_binding is not None
        else read_regular_file_without_atime(
            source,
            expected_mode=0o600,
            expected_size=32,
            description="health activation key",
            expected_identity=expected_identity,
        )
    )
    _write_destination_bytes(
        reservation,
        destination.relative_to(reservation.path),
        value,
    )


def _validate_copied_unit(
    unit: dict[str, Any],
    source: Path,
    destination: "_DestinationReservation",
    expected: dict[str, Any],
) -> dict[str, Any]:
    destination.assert_binding()
    result_relative = unit["result_path"].relative_to(source)
    health_relative = unit["health_path"].relative_to(source)
    result_path = destination.path / result_relative
    health_path = destination.path / health_relative
    with ExitStack() as bindings:
        result_binding = bindings.enter_context(
            _bind_reserved_file(destination, result_relative)
        )
        result_inventory = _sqlite_inventory(
            result_path,
            kind="u7",
            expected_identity=result_binding.sqlite_identity,
            state_binding=result_binding,
        )
        health_inventory = None
        key_inventory = None
        if expected["health_db"] is not None:
            key_relative = health_relative.with_name(
                f"{health_relative.name}.activation.key"
            )
            health_binding = bindings.enter_context(
                _bind_reserved_file(destination, health_relative)
            )
            key_binding = bindings.enter_context(
                _bind_reserved_file(destination, key_relative)
            )
            health_inventory = _sqlite_inventory(
                health_path,
                kind="u8",
                test_id=unit["test_id"],
                expected_identity=health_binding.sqlite_identity,
                state_binding=health_binding,
                key_binding=key_binding,
            )
            key_inventory = _key_inventory(
                destination.path / key_relative,
                expected_identity=key_binding.sqlite_identity,
                state_binding=key_binding,
            )
    inventory = {
        "test_id": unit["test_id"],
        "ok": True,
        "result_db": result_inventory,
        "health_db": health_inventory,
        "activation_key": key_inventory,
        "errors": [],
    }
    destination.assert_binding()
    _assert_unit_logical_match(expected, inventory)
    destination.assert_binding()
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


def _owned_binding_metadata(
    binding: Any,
    *,
    exact_mode: int | None = None,
    exact_size: int | None = None,
) -> dict[str, Any]:
    binding.assert_path_binding()
    if binding.descriptor is None or binding.identity is None:
        raise RuntimeError(f"Backup source is missing: {binding.path}")
    metadata = os.fstat(binding.descriptor)
    current = os.stat(
        binding.name,
        dir_fd=binding.directory.parent_fd,
        follow_symlinks=False,
    )
    if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
        raise RuntimeError("Backup source retained entry identity changed")
    mode = stat.S_IMODE(metadata.st_mode)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
        raise RuntimeError(f"Backup source is not an owned regular file: {binding.path}")
    if metadata.st_nlink != 1:
        raise RuntimeError(f"Backup source must have exactly one hard link: {binding.path}")
    if exact_mode is not None and mode != exact_mode:
        raise RuntimeError(f"Backup source mode must be {exact_mode:04o}: {binding.path}")
    if exact_size is not None and metadata.st_size != exact_size:
        raise RuntimeError(f"Backup source size must be {exact_size}: {binding.path}")
    binding.assert_path_binding()
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


@dataclass
class _DestinationAncestryBinding:
    path: Path
    descriptors: tuple[int, ...]
    identities: tuple[tuple[int, int, int, int, int], ...]
    parts: tuple[str, ...]
    ownership: ExitStack
    closed: bool = False

    @property
    def parent_fd(self) -> int:
        return self.descriptors[-1]

    def assert_binding(self, *, require_target_absent: bool = True) -> None:
        for descriptor, expected in zip(
            self.descriptors,
            self.identities,
            strict=True,
        ):
            metadata = os.fstat(descriptor)
            actual = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_uid,
                metadata.st_gid,
                stat.S_IMODE(metadata.st_mode),
            )
            if not stat.S_ISDIR(metadata.st_mode) or actual != expected:
                raise RuntimeError("Backup destination retained ancestry changed")
        with _open_exact_absolute_directory_chain(
            self.parts,
            self.identities,
            error_message="Backup destination pathname ancestry changed",
        ):
            pass
        if not require_target_absent:
            return
        try:
            os.stat(
                self.path.name,
                dir_fd=self.parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return
        raise FileExistsError(f"Backup destination already exists: {self.path}")

    def close(self) -> None:
        if self.closed:
            return
        self.ownership.close()
        self.closed = True


def _bind_destination_target(path: Path) -> _DestinationAncestryBinding:
    value = _absolute_lexical_path(path, description="Backup destination")
    ownership = ExitStack()
    descriptors: list[int] = []
    identities: list[tuple[int, int, int, int, int]] = []
    try:
        descriptors.append(
            ownership.enter_context(_opened_directory(os.path.sep))
        )
        root_metadata = os.fstat(descriptors[0])
        identities.append(
            (
                root_metadata.st_dev,
                root_metadata.st_ino,
                root_metadata.st_uid,
                root_metadata.st_gid,
                stat.S_IMODE(root_metadata.st_mode),
            )
        )
        for part in value.parent.parts[1:]:
            try:
                descriptor = ownership.enter_context(
                    _opened_directory(part, dir_fd=descriptors[-1])
                )
            except OSError as exc:
                try:
                    component = os.stat(
                        part,
                        dir_fd=descriptors[-1],
                        follow_symlinks=False,
                    )
                except OSError:
                    raise exc
                if stat.S_ISLNK(component.st_mode):
                    raise ValueError(
                        "Backup destination contains a symlink component"
                    ) from exc
                raise
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                os.close(descriptor)
                raise ValueError("Backup destination ancestry is unsafe")
            descriptors.append(descriptor)
            identities.append(
                (
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_uid,
                    metadata.st_gid,
                    stat.S_IMODE(metadata.st_mode),
                )
            )
        parent_metadata = os.fstat(descriptors[-1])
        parent_mode = stat.S_IMODE(parent_metadata.st_mode)
        if (
            parent_metadata.st_uid != os.geteuid()
            or parent_mode & _DIRECTORY_UNSAFE_BITS
            or not parent_mode & stat.S_IWUSR
            or not parent_mode & stat.S_IXUSR
        ):
            raise ValueError("Backup destination parent must be owned by evaluator uid")
        binding = _DestinationAncestryBinding(
            value,
            tuple(descriptors),
            tuple(identities),
            value.parent.parts[1:],
            ownership,
        )
        binding.assert_binding()
        return binding
    except BaseException:
        ownership.close()
        raise


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


def _paths_overlap(first: Path, second: Path) -> bool:
    return (
        first == second
        or first in second.parents
        or second in first.parents
    )


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


@dataclass
class _DestinationReservation:
    path: Path
    name: str
    parent_fd: int
    root_fd: int
    parent_identity: tuple[int, int]
    identity: tuple[int, int]
    ancestry: _DestinationAncestryBinding
    directories: dict[tuple[str, ...], tuple[int, int]]
    files: dict[tuple[str, ...], tuple[int, int]]

    def assert_binding(self) -> None:
        self.ancestry.assert_binding(require_target_absent=False)
        _assert_directory_identity(self)

    def close(self) -> None:
        for descriptor_name in ("root_fd", "parent_fd"):
            descriptor = getattr(self, descriptor_name)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, descriptor_name, -1)
        self.ancestry.close()


@dataclass
class _ReservedDirectoryBinding:
    reservation: _DestinationReservation
    parent_fd: int
    parts: tuple[str, ...]

    @property
    def path(self) -> Path:
        return self.reservation.path.joinpath(*self.parts)

    def assert_path_binding(self) -> None:
        self.reservation.assert_binding()
        metadata = os.fstat(self.parent_fd)
        expected = self.reservation.directories[self.parts]
        if (metadata.st_dev, metadata.st_ino) != expected:
            raise RuntimeError("Backup restore-validation parent changed")


@dataclass
class _ReservedFileBinding:
    directory: _ReservedDirectoryBinding
    name: str
    descriptor: int
    identity: StateFileIdentity

    @property
    def path(self) -> Path:
        return self.identity.path

    @property
    def sqlite_identity(self):
        return self.identity.sqlite_identity()

    def assert_path_binding(self) -> None:
        self.directory.assert_path_binding()
        metadata = os.fstat(self.descriptor)
        actual = (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_uid,
            metadata.st_gid,
            stat.S_IMODE(metadata.st_mode),
            metadata.st_nlink,
        )
        expected = (
            self.identity.device,
            self.identity.inode,
            self.identity.owner_uid,
            self.identity.owner_gid,
            self.identity.mode,
            self.identity.link_count,
        )
        if not stat.S_ISREG(metadata.st_mode) or actual != expected:
            raise RuntimeError("Backup restore-validation file changed")


@contextmanager
def _bind_reserved_file(
    reservation: _DestinationReservation,
    relative: Path,
):
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise ValueError("Backup restore-validation path is unsafe")
    with _destination_parent_fd(reservation, relative.parts[:-1]) as parent_fd:
        expected = reservation.files.get(tuple(relative.parts))
        if expected is None:
            raise RuntimeError("Backup restore-validation file was not reserved")
        flags = os.O_RDONLY
        if hasattr(os, "O_NOATIME"):
            flags |= os.O_NOATIME
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        descriptor = os.open(relative.name, flags, dir_fd=parent_fd)
        try:
            metadata = os.fstat(descriptor)
            mode = stat.S_IMODE(metadata.st_mode)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != expected
                or (metadata.st_uid, metadata.st_gid)
                != (os.geteuid(), os.getegid())
                or mode != 0o600
                or metadata.st_nlink != 1
            ):
                raise RuntimeError("Backup restore-validation file is not exact")
            directory = _ReservedDirectoryBinding(
                reservation,
                parent_fd,
                tuple(relative.parts[:-1]),
            )
            binding = _ReservedFileBinding(
                directory,
                relative.name,
                descriptor,
                StateFileIdentity(
                    path=reservation.path / relative,
                    device=metadata.st_dev,
                    inode=metadata.st_ino,
                    owner_uid=metadata.st_uid,
                    owner_gid=metadata.st_gid,
                    mode=mode,
                    link_count=metadata.st_nlink,
                ),
            )
            binding.assert_path_binding()
            yield binding
            binding.assert_path_binding()
        finally:
            os.close(descriptor)


def _reserve_destination(
    ancestry: _DestinationAncestryBinding,
) -> _DestinationReservation:
    path = ancestry.path
    ancestry.assert_binding()
    parent_fd = os.dup(ancestry.parent_fd)
    os.set_inheritable(parent_fd, False)
    created_identity: tuple[int, int] | None = None
    root_fd = -1
    try:
        parent_metadata = os.fstat(parent_fd)
        parent_identity = (parent_metadata.st_dev, parent_metadata.st_ino)
        try:
            published = create_published_directory_at(
                parent_fd,
                path.name,
                expected_uid=os.geteuid(),
                expected_gid=os.getegid(),
            )
            root_fd = published.descriptor
            created_identity = published.identity
        except FileExistsError as exc:
            raise FileExistsError(f"Backup destination already exists: {path}") from exc
        current_created = os.stat(
            path.name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        if (current_created.st_dev, current_created.st_ino) != created_identity:
            raise RuntimeError("Backup destination root was replaced during reservation")
        ancestry.assert_binding(require_target_absent=False)
        ancestry_metadata = os.fstat(ancestry.parent_fd)
        current_parent = os.fstat(parent_fd)
        if (ancestry_metadata.st_dev, ancestry_metadata.st_ino) != (
            current_parent.st_dev,
            current_parent.st_ino,
        ):
            raise RuntimeError("Backup destination parent binding changed at reservation")
        ancestry.assert_binding(require_target_absent=False)
    except BaseException as primary_error:
        cleanup_error: BaseException | None = None
        cleanup_fd = root_fd
        try:
            if created_identity is None and cleanup_fd >= 0:
                cleanup_metadata = os.fstat(cleanup_fd)
                created_identity = (
                    cleanup_metadata.st_dev,
                    cleanup_metadata.st_ino,
                )
            if created_identity is not None and cleanup_fd >= 0:
                cleanup_metadata = os.fstat(cleanup_fd)
                cleanup_identity = (
                    cleanup_metadata.st_dev,
                    cleanup_metadata.st_ino,
                )
                if (
                    cleanup_identity != created_identity
                    or not stat.S_ISDIR(cleanup_metadata.st_mode)
                ):
                    raise RuntimeError(
                        "Backup reservation cleanup target changed; preserved"
                    )
                remove_entry_if_identity_at(
                    parent_fd,
                    path.name,
                    created_identity,
                    is_directory=True,
                    description="Backup reservation cleanup target",
                )
        except FileNotFoundError:
            pass
        except BaseException as exc:
            cleanup_error = exc
        finally:
            if root_fd >= 0:
                os.close(root_fd)
            os.close(parent_fd)
        if cleanup_error is not None:
            _add_cleanup_note(
                primary_error,
                "Backup reservation cleanup failed closed",
                cleanup_error,
            )
        raise
    metadata = os.fstat(root_fd)
    reservation = _DestinationReservation(
        path=path,
        name=path.name,
        parent_fd=parent_fd,
        root_fd=root_fd,
        parent_identity=parent_identity,
        identity=(metadata.st_dev, metadata.st_ino),
        ancestry=ancestry,
        directories={(): (metadata.st_dev, metadata.st_ino)},
        files={},
    )
    try:
        reservation.assert_binding()
        _fsync_destination_descriptor(reservation, parent_fd)
        reservation.assert_binding()
    except BaseException:
        _remove_tree_if_identity(reservation)
        reservation.close()
        raise
    return reservation


@contextmanager
def _destination_parent_fd(
    reservation: _DestinationReservation,
    parts: tuple[str, ...],
):
    current_parts: list[str] = []
    reservation.assert_binding()

    @contextmanager
    def descend(parent_fd: int, index: int):
        if index == len(parts):
            yield parent_fd
            return
        part = parts[index]
        if not part or part in {".", ".."} or os.path.sep in part:
            raise ValueError("Backup destination contains an unsafe component")
        current_parts.append(part)
        try:
            parts_key = tuple(current_parts)
            expected = reservation.directories.get(parts_key)
            if expected is None:
                reservation.assert_binding()
                try:
                    published = create_published_directory_at(
                        parent_fd,
                        part,
                        expected_uid=os.geteuid(),
                        expected_gid=os.getegid(),
                    )
                    child_fd = published.descriptor
                    expected = published.identity
                    reservation.directories[parts_key] = expected
                except FileExistsError as exc:
                    raise RuntimeError(
                        "Backup destination contains an unreserved directory"
                    ) from exc
                try:
                    reservation.assert_binding()
                    _fsync_destination_descriptor(reservation, parent_fd)
                except BaseException as primary_error:
                    try:
                        _remove_new_destination_directory(
                            reservation,
                            parent_fd,
                            part,
                            expected,
                            parts_key,
                            child_fd,
                        )
                    except BaseException as cleanup_error:
                        _add_cleanup_note(
                            primary_error,
                            "Backup nested-directory post-registration cleanup failed closed",
                            cleanup_error,
                        )
                    raise
                finally:
                    os.close(child_fd)
            else:
                reservation.assert_binding()
            with _opened_directory(part, dir_fd=parent_fd) as child_fd:
                metadata = os.fstat(child_fd)
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or (metadata.st_uid, metadata.st_gid)
                    != (os.geteuid(), os.getegid())
                    or stat.S_IMODE(metadata.st_mode) != 0o700
                ):
                    raise RuntimeError(
                        "Backup destination directory is not exact owner 0700"
                    )
                identity = (metadata.st_dev, metadata.st_ino)
                if identity != expected:
                    raise RuntimeError(
                        "Backup destination directory identity changed"
                    )
                reservation.assert_binding()
                with descend(child_fd, index + 1) as leaf_fd:
                    yield leaf_fd
                reservation.assert_binding()
        finally:
            current_parts.pop()

    with descend(reservation.root_fd, 0) as descriptor:
        reservation.assert_binding()
        yield descriptor
        reservation.assert_binding()


@contextmanager
def _reserved_destination_file(
    reservation: _DestinationReservation,
    relative: Path,
):
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise ValueError("Backup destination file must be a confined relative path")
    reservation.assert_binding()
    with _destination_parent_fd(reservation, relative.parts[:-1]) as parent_fd:
        name = relative.name
        file_key = tuple(relative.parts)
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        reservation.assert_binding()
        descriptor: int | None = None
        identity: tuple[int, int] | None = None
        try:
            with defer_creation_signals():
                descriptor = os.open(name, flags, 0o600, dir_fd=parent_fd)
                metadata = os.fstat(descriptor)
                identity = (metadata.st_dev, metadata.st_ino)
                reservation.files[file_key] = identity
            os.fchmod(descriptor, 0o600)
            _fsync_destination_descriptor(reservation, descriptor)
        except BaseException as primary_error:
            if identity is None and descriptor is not None:
                try:
                    metadata = os.fstat(descriptor)
                    identity = (metadata.st_dev, metadata.st_ino)
                    reservation.files[file_key] = identity
                except BaseException as capture_error:
                    _add_cleanup_note(
                        primary_error,
                        "Backup destination-file identity recovery failed closed",
                        capture_error,
                    )
            if identity is not None:
                try:
                    _remove_new_destination_file(
                        reservation,
                        parent_fd,
                        name,
                        identity,
                        file_key,
                    )
                except BaseException as cleanup_error:
                    _add_cleanup_note(
                        primary_error,
                        "Backup destination-file creation cleanup failed closed",
                        cleanup_error,
                    )
            raise
        finally:
            if descriptor is not None:
                os.close(descriptor)
        assert identity is not None
        artifacts = {name: identity}
        try:
            reservation.assert_binding()
            _fsync_destination_descriptor(reservation, parent_fd)
            yield (
                parent_fd,
                Path(f"/proc/self/fd/{parent_fd}/{name}"),
                identity,
                artifacts,
            )
            reservation.assert_binding()
            _assert_entry_identity(parent_fd, name, identity)
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd)
            try:
                opened = os.fstat(descriptor)
                if (
                    (opened.st_dev, opened.st_ino) != identity
                    or not stat.S_ISREG(opened.st_mode)
                    or (opened.st_uid, opened.st_gid)
                    != (os.geteuid(), os.getegid())
                    or stat.S_IMODE(opened.st_mode) != 0o600
                    or opened.st_nlink != 1
                ):
                    raise RuntimeError("Backup destination file metadata changed")
                _fsync_destination_descriptor(reservation, descriptor)
            finally:
                os.close(descriptor)
            _fsync_destination_descriptor(reservation, parent_fd)
            reservation.assert_binding()
        except BaseException as original_error:
            cleanup_error: BaseException | None = None
            for artifact_name, artifact_identity in reversed(tuple(artifacts.items())):
                try:
                    removed = remove_entry_if_identity_at(
                        parent_fd,
                        artifact_name,
                        artifact_identity,
                        is_directory=False,
                        description="Backup destination artifact cleanup target",
                        binding_guard=reservation.assert_binding,
                    )
                    if removed:
                        reservation.files.pop(
                            tuple(relative.parts[:-1]) + (artifact_name,),
                            None,
                        )
                except BaseException as exc:
                    cleanup_error = cleanup_error or exc
            try:
                _fsync_destination_descriptor(reservation, parent_fd)
            except BaseException as exc:
                cleanup_error = cleanup_error or exc
            if cleanup_error is not None:
                raise cleanup_error from original_error
            raise


def _remove_new_destination_file(
    reservation: _DestinationReservation,
    parent_fd: int,
    name: str,
    expected: tuple[int, int],
    file_key: tuple[str, ...],
) -> bool:
    """Relocate and remove the exact expected destination public name."""

    removed = remove_entry_if_identity_at(
        parent_fd,
        name,
        expected,
        is_directory=False,
        description="Backup destination-file cleanup target",
        binding_guard=reservation.assert_binding,
    )
    if removed:
        reservation.files.pop(file_key, None)
    return removed


def _remove_new_destination_directory(
    reservation: _DestinationReservation,
    parent_fd: int,
    name: str,
    expected: tuple[int, int],
    parts_key: tuple[str, ...],
    descriptor: int,
) -> bool:
    """Relocate and remove one exact just-created empty directory."""

    retained = os.fstat(descriptor)
    if (retained.st_dev, retained.st_ino) != expected:
        raise RuntimeError(
            "Backup nested-directory cleanup retained target changed; preserved"
        )
    removed = remove_entry_if_identity_at(
        parent_fd,
        name,
        expected,
        is_directory=True,
        description="Backup nested-directory cleanup target",
        binding_guard=reservation.assert_binding,
    )
    if removed:
        reservation.directories.pop(parts_key, None)
    return removed


def _assert_entry_identity(
    parent_fd: int,
    name: str,
    identity: tuple[int, int],
) -> None:
    metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if (metadata.st_dev, metadata.st_ino) != identity:
        raise RuntimeError("Backup destination entry identity changed")


def _write_destination_bytes(
    reservation: _DestinationReservation,
    relative: Path,
    value: bytes,
) -> None:
    reservation.assert_binding()
    with _reserved_destination_file(reservation, relative) as (
        parent_fd,
        _open_path,
        identity,
        _artifacts,
    ):
        descriptor = os.open(
            relative.name,
            os.O_WRONLY | os.O_NOFOLLOW,
            dir_fd=parent_fd,
        )
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != identity:
                raise RuntimeError("Backup destination write binding changed")
            view = memoryview(value)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
            _fsync_destination_descriptor(reservation, descriptor)
        finally:
            os.close(descriptor)
    reservation.assert_binding()


def _capture_destination_sidecars(
    parent_fd: int,
    database_name: str,
    artifacts: dict[str, tuple[int, int]],
    *,
    reservation: _DestinationReservation,
    parent_parts: tuple[str, ...],
) -> None:
    present: list[str] = []
    for suffix in ("-journal", "-wal", "-shm"):
        name = database_name + suffix
        try:
            metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError("Backup destination SQLite sidecar is unsafe")
        present.append(name)
    reservation.assert_binding()
    if present:
        raise RuntimeError(
            "Backup destination SQLite copy retained unknown sidecars: "
            + ", ".join(sorted(present))
        )


def _fsync_destination_descriptor(
    reservation: _DestinationReservation,
    descriptor: int,
) -> None:
    reservation.assert_binding()
    os.fsync(descriptor)
    reservation.assert_binding()


def _fsync_destination_tree(
    reservation: _DestinationReservation,
    directory_fd: int,
    parts: tuple[str, ...] = (),
) -> None:
    reservation.assert_binding()
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    for name in os.listdir(directory_fd):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        entry_parts = parts + (name,)
        if stat.S_ISDIR(metadata.st_mode):
            if reservation.directories.get(entry_parts) != (
                metadata.st_dev,
                metadata.st_ino,
            ):
                raise RuntimeError("Backup destination contains an unknown directory")
            child = os.open(name, flags, dir_fd=directory_fd)
            try:
                _fsync_destination_tree(reservation, child, entry_parts)
            finally:
                os.close(child)
        elif stat.S_ISREG(metadata.st_mode):
            if reservation.files.get(entry_parts) != (
                metadata.st_dev,
                metadata.st_ino,
            ):
                raise RuntimeError("Backup destination contains an unknown file")
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
            try:
                _fsync_destination_descriptor(reservation, descriptor)
            finally:
                os.close(descriptor)
        else:
            raise RuntimeError("Backup destination contains an unsafe file type")
    _fsync_destination_descriptor(reservation, directory_fd)
    reservation.assert_binding()


def _assert_directory_identity(reservation: _DestinationReservation) -> None:
    parent = os.fstat(reservation.parent_fd)
    metadata = os.fstat(reservation.root_fd)
    current = os.stat(
        reservation.name,
        dir_fd=reservation.parent_fd,
        follow_symlinks=False,
    )
    if (
        (parent.st_dev, parent.st_ino) != reservation.parent_identity
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or (metadata.st_dev, metadata.st_ino) != reservation.identity
        or (current.st_dev, current.st_ino) != reservation.identity
    ):
        raise RuntimeError("Backup destination reservation identity changed")
    _assert_reserved_directory_tree(reservation)


def _assert_reserved_directory_tree(reservation: _DestinationReservation) -> None:
    for parts, expected in sorted(
        reservation.directories.items(),
        key=lambda item: (len(item[0]), item[0]),
    ):
        with _open_relative_directory_chain(
            reservation.root_fd,
            parts,
        ) as descriptor:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != expected
                or (metadata.st_uid, metadata.st_gid)
                != (os.geteuid(), os.getegid())
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise RuntimeError("Backup destination nested directory changed")
    for parts, expected in sorted(reservation.files.items()):
        parent_fd = _open_reserved_directory_for_cleanup(
            reservation,
            parts[:-1],
        )
        try:
            metadata = os.stat(
                parts[-1],
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != expected
            ):
                raise RuntimeError("Backup destination file identity changed")
        finally:
            os.close(parent_fd)
    _assert_reserved_entries(reservation, reservation.root_fd)


def _assert_reserved_entries(
    reservation: _DestinationReservation,
    directory_fd: int,
    parts: tuple[str, ...] = (),
) -> None:
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    for name in os.listdir(directory_fd):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        entry_parts = parts + (name,)
        identity = (metadata.st_dev, metadata.st_ino)
        if stat.S_ISDIR(metadata.st_mode):
            if reservation.directories.get(entry_parts) != identity:
                raise RuntimeError("Backup destination contains an unknown directory")
            child = os.open(name, flags, dir_fd=directory_fd)
            try:
                _assert_reserved_entries(reservation, child, entry_parts)
            finally:
                os.close(child)
        elif stat.S_ISREG(metadata.st_mode):
            if reservation.files.get(entry_parts) != identity:
                raise RuntimeError("Backup destination contains an unknown file")
        else:
            raise RuntimeError("Backup destination contains an unsafe entry")


def _assert_published_destination_path(
    reservation: _DestinationReservation,
) -> None:
    """Freshly reopen the complete destination after retained-fd teardown."""

    try:
        with _open_fresh_destination(reservation) as (_parent_fd, root_fd):
            _assert_published_destination_tree(reservation, root_fd)
    except (FileNotFoundError, NotADirectoryError, OSError) as exc:
        raise RuntimeError(
            "Backup destination changed during success finalization"
        ) from exc


def _assert_published_destination_tree(
    reservation: _DestinationReservation,
    root_fd: int,
) -> None:
    for parts, expected in sorted(
        reservation.directories.items(),
        key=lambda item: (len(item[0]), item[0]),
    ):
        with _open_relative_directory_chain(root_fd, parts) as descriptor:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != expected
                or (metadata.st_uid, metadata.st_gid)
                != (os.geteuid(), os.getegid())
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise RuntimeError(
                    "Backup destination changed during success finalization"
                )
    for parts, expected in sorted(reservation.files.items()):
        with _open_relative_directory_chain(root_fd, parts[:-1]) as descriptor:
            metadata = os.stat(
                parts[-1],
                dir_fd=descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != expected
                or (metadata.st_uid, metadata.st_gid)
                != (os.geteuid(), os.getegid())
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_nlink != 1
            ):
                raise RuntimeError(
                    "Backup destination changed during success finalization"
                )
    _assert_reserved_entries(reservation, root_fd)


def _remove_published_tree_if_identity(
    reservation: _DestinationReservation,
) -> None:
    """Remove an unchanged published tree after retained descriptors are closed."""

    try:
        with _open_fresh_destination(reservation) as (parent_fd, root_fd):
            _assert_published_destination_tree(reservation, root_fd)
        for parts, expected in sorted(
            reservation.files.items(),
            key=lambda item: (len(item[0]), item[0]),
            reverse=True,
        ):
            _published_cleanup_checkpoint("file_unlink", parts)
            with _open_fresh_cleanup_parent(
                reservation,
                parts[:-1],
            ) as (_destination_parent_fd, file_parent_fd):
                metadata = os.stat(
                    parts[-1],
                    dir_fd=file_parent_fd,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or (metadata.st_dev, metadata.st_ino) != expected
                    or (metadata.st_uid, metadata.st_gid)
                    != (os.geteuid(), os.getegid())
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                    or metadata.st_nlink != 1
                ):
                    raise RuntimeError(
                        "Backup post-close cleanup file identity changed"
                    )
                remove_entry_if_identity_at(
                    file_parent_fd,
                    parts[-1],
                    expected,
                    is_directory=False,
                    description="Backup post-close cleanup file",
                )
            _assert_fresh_cleanup_entry_absent(reservation, parts)
        for parts, expected in sorted(
            (
                item
                for item in reservation.directories.items()
                if item[0]
            ),
            key=lambda item: (len(item[0]), item[0]),
            reverse=True,
        ):
            _published_cleanup_checkpoint("directory_rmdir", parts)
            with _open_fresh_cleanup_parent(
                reservation,
                parts[:-1],
            ) as (_destination_parent_fd, directory_parent_fd):
                metadata = os.stat(
                    parts[-1],
                    dir_fd=directory_parent_fd,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISDIR(metadata.st_mode)
                    or (metadata.st_dev, metadata.st_ino) != expected
                    or (metadata.st_uid, metadata.st_gid)
                    != (os.geteuid(), os.getegid())
                    or stat.S_IMODE(metadata.st_mode) != 0o700
                ):
                    raise RuntimeError(
                        "Backup post-close cleanup directory identity changed"
                    )
                remove_entry_if_identity_at(
                    directory_parent_fd,
                    parts[-1],
                    expected,
                    is_directory=True,
                    description="Backup post-close cleanup directory",
                )
            _assert_fresh_cleanup_entry_absent(reservation, parts)
        _published_cleanup_checkpoint("root_rmdir", ())
        with _open_fresh_destination(reservation) as (parent_fd, root_fd):
            current = os.stat(
                reservation.name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            root_metadata = os.fstat(root_fd)
            if (
                (current.st_dev, current.st_ino) != reservation.identity
                or (root_metadata.st_dev, root_metadata.st_ino)
                != reservation.identity
                or os.listdir(root_fd)
            ):
                raise RuntimeError(
                    "Backup post-close cleanup root identity changed"
                )
            remove_entry_if_identity_at(
                parent_fd,
                reservation.name,
                reservation.identity,
                is_directory=True,
                description="Backup post-close cleanup root",
            )
        _assert_fresh_destination_absent(reservation)
    except BaseException as exc:
        raise RuntimeError(
            "Backup post-close cleanup failed closed: " + str(exc)
        ) from exc


def _remove_tree_if_identity(reservation: _DestinationReservation) -> None:
    cleanup_errors: list[BaseException] = []
    try:
        reservation.ancestry.assert_binding(require_target_absent=False)
    except BaseException as exc:
        cleanup_errors.append(exc)

    for parts, expected in sorted(
        tuple(reservation.files.items()),
        key=lambda item: (len(item[0]), item[0]),
        reverse=True,
    ):
        try:
            parent_fd = _open_reserved_directory_for_cleanup(
                reservation,
                parts[:-1],
            )
            try:
                try:
                    metadata = os.stat(
                        parts[-1],
                        dir_fd=parent_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    reservation.files.pop(parts, None)
                    continue
                if (metadata.st_dev, metadata.st_ino) != expected:
                    raise RuntimeError(
                        "Backup cleanup file was replaced; replacement preserved"
                    )
                if remove_entry_if_identity_at(
                    parent_fd,
                    parts[-1],
                    expected,
                    is_directory=False,
                    description="Backup cleanup file",
                    binding_guard=lambda: reservation.ancestry.assert_binding(
                        require_target_absent=False
                    ),
                ):
                    reservation.files.pop(parts, None)
            finally:
                os.close(parent_fd)
        except BaseException as exc:
            cleanup_errors.append(exc)

    for parts, expected in sorted(
        (
            item
            for item in tuple(reservation.directories.items())
            if item[0]
        ),
        key=lambda item: (len(item[0]), item[0]),
        reverse=True,
    ):
        try:
            parent_fd = _open_reserved_directory_for_cleanup(
                reservation,
                parts[:-1],
            )
            try:
                metadata = os.stat(
                    parts[-1],
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                if (metadata.st_dev, metadata.st_ino) != expected:
                    raise RuntimeError(
                        "Backup cleanup directory was replaced; replacement preserved"
                    )
                if remove_entry_if_identity_at(
                    parent_fd,
                    parts[-1],
                    expected,
                    is_directory=True,
                    description="Backup cleanup directory",
                    binding_guard=lambda: reservation.ancestry.assert_binding(
                        require_target_absent=False
                    ),
                ):
                    reservation.directories.pop(parts, None)
            finally:
                os.close(parent_fd)
        except FileNotFoundError:
            reservation.directories.pop(parts, None)
        except BaseException as exc:
            cleanup_errors.append(exc)

    try:
        retained = os.fstat(reservation.root_fd)
        parent = os.fstat(reservation.parent_fd)
        if (
            (retained.st_dev, retained.st_ino) != reservation.identity
            or (parent.st_dev, parent.st_ino) != reservation.parent_identity
        ):
            raise RuntimeError("Backup cleanup retained reservation changed")
        current = os.stat(
            reservation.name,
            dir_fd=reservation.parent_fd,
            follow_symlinks=False,
        )
        if (current.st_dev, current.st_ino) != reservation.identity:
            raise RuntimeError(
                "Backup cleanup root was replaced; replacement preserved"
            )
        if os.listdir(reservation.root_fd):
            raise RuntimeError("Backup cleanup found unknown entries; root preserved")
        if cleanup_errors:
            raise RuntimeError("Backup cleanup was incomplete; root preserved")
        remove_entry_if_identity_at(
            reservation.parent_fd,
            reservation.name,
            reservation.identity,
            is_directory=True,
            description="Backup cleanup root",
            binding_guard=lambda: reservation.ancestry.assert_binding(
                require_target_absent=False
            ),
        )
    except BaseException as exc:
        cleanup_errors.append(exc)
    if cleanup_errors:
        raise RuntimeError(
            "Backup cleanup failed closed: " + str(cleanup_errors[0])
        ) from cleanup_errors[0]


def _directory_open_flags() -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return flags


@contextmanager
def _opened_directory(path: str, *, dir_fd: int | None = None):
    descriptor = os.open(path, _directory_open_flags(), dir_fd=dir_fd)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


@contextmanager
def _open_relative_directory_chain(
    parent_fd: int,
    parts: tuple[str, ...],
):
    if not parts:
        yield parent_fd
        return
    with _opened_directory(parts[0], dir_fd=parent_fd) as child_fd:
        with _open_relative_directory_chain(child_fd, parts[1:]) as leaf_fd:
            yield leaf_fd


def _assert_exact_directory_metadata(
    descriptor: int,
    expected: tuple[int, int, int, int, int],
    *,
    error_message: str,
) -> None:
    metadata = os.fstat(descriptor)
    actual = (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    )
    if not stat.S_ISDIR(metadata.st_mode) or actual != expected:
        raise RuntimeError(error_message)


@contextmanager
def _open_exact_relative_directory_chain(
    parent_fd: int,
    parts: tuple[str, ...],
    identities: tuple[tuple[int, int, int, int, int], ...],
    *,
    error_message: str,
):
    if not parts:
        yield parent_fd
        return
    with _opened_directory(parts[0], dir_fd=parent_fd) as child_fd:
        _assert_exact_directory_metadata(
            child_fd,
            identities[0],
            error_message=error_message,
        )
        with _open_exact_relative_directory_chain(
            child_fd,
            parts[1:],
            identities[1:],
            error_message=error_message,
        ) as leaf_fd:
            yield leaf_fd


@contextmanager
def _open_exact_absolute_directory_chain(
    parts: tuple[str, ...],
    identities: tuple[tuple[int, int, int, int, int], ...],
    *,
    error_message: str,
):
    if len(identities) != len(parts) + 1:
        raise RuntimeError(error_message)
    with _opened_directory(os.path.sep) as root_fd:
        _assert_exact_directory_metadata(
            root_fd,
            identities[0],
            error_message=error_message,
        )
        with _open_exact_relative_directory_chain(
            root_fd,
            parts,
            identities[1:],
            error_message=error_message,
        ) as leaf_fd:
            yield leaf_fd


@contextmanager
def _open_fresh_destination(reservation: _DestinationReservation):
    error_message = "Backup destination changed during success finalization"
    with _open_exact_absolute_directory_chain(
        reservation.ancestry.parts,
        reservation.ancestry.identities,
        error_message=error_message,
    ) as parent_fd:
        with _opened_directory(reservation.name, dir_fd=parent_fd) as root_fd:
            metadata = os.fstat(root_fd)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != reservation.identity
                or (metadata.st_uid, metadata.st_gid)
                != (os.geteuid(), os.getegid())
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise RuntimeError(error_message)
            yield parent_fd, root_fd


@contextmanager
def _open_fresh_cleanup_parent(
    reservation: _DestinationReservation,
    parts: tuple[str, ...],
):
    """Freshly bind exact lexical ancestry, destination root, and one parent."""

    expected = reservation.directories.get(parts)
    if expected is None:
        raise RuntimeError("Backup post-close cleanup parent was not reserved")
    with _open_fresh_destination(reservation) as (destination_parent_fd, root_fd):
        with _open_relative_directory_chain(root_fd, parts) as mutation_parent_fd:
            metadata = os.fstat(mutation_parent_fd)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino) != expected
                or (metadata.st_uid, metadata.st_gid)
                != (os.geteuid(), os.getegid())
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise RuntimeError(
                    "Backup post-close cleanup parent binding changed"
                )
            yield destination_parent_fd, mutation_parent_fd


def _assert_fresh_cleanup_entry_absent(
    reservation: _DestinationReservation,
    parts: tuple[str, ...],
) -> None:
    """Revalidate the full lexical binding after one successful mutation."""

    with _open_fresh_cleanup_parent(
        reservation,
        parts[:-1],
    ) as (_destination_parent_fd, mutation_parent_fd):
        try:
            os.stat(
                parts[-1],
                dir_fd=mutation_parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return
        raise RuntimeError("Backup post-close cleanup mutation was not durable")


def _assert_fresh_destination_absent(
    reservation: _DestinationReservation,
) -> None:
    """Revalidate exact lexical ancestry and the final root removal."""

    with _open_exact_absolute_directory_chain(
        reservation.ancestry.parts,
        reservation.ancestry.identities,
        error_message="Backup destination changed during success finalization",
    ) as parent_fd:
        try:
            os.stat(
                reservation.name,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return
        raise RuntimeError("Backup post-close cleanup root still exists")


def _published_cleanup_checkpoint(
    _operation: str,
    _parts: tuple[str, ...],
) -> None:
    """Non-interrupting production checkpoint before fresh cleanup rebinding."""


def _add_cleanup_note(
    primary_error: BaseException,
    message: str,
    cleanup_error: BaseException,
) -> None:
    note = f"{message}: {type(cleanup_error).__name__}: {cleanup_error}"
    if hasattr(primary_error, "add_note"):
        primary_error.add_note(note)


def _open_reserved_directory_for_cleanup(
    reservation: _DestinationReservation,
    parts: tuple[str, ...],
) -> int:
    root_metadata = os.fstat(reservation.root_fd)
    if (root_metadata.st_dev, root_metadata.st_ino) != reservation.identity:
        raise RuntimeError("Backup cleanup root descriptor changed")

    expected_identities: list[tuple[int, int]] = []
    current_parts: list[str] = []
    for part in parts:
        current_parts.append(part)
        expected = reservation.directories.get(tuple(current_parts))
        if expected is None:
            raise RuntimeError("Backup cleanup parent was never reserved")
        expected_identities.append(expected)

    @contextmanager
    def descend(parent_fd: int, index: int):
        if index == len(parts):
            yield parent_fd
            return
        with _opened_directory(parts[index], dir_fd=parent_fd) as child_fd:
            metadata = os.fstat(child_fd)
            if (metadata.st_dev, metadata.st_ino) != expected_identities[index]:
                raise RuntimeError(
                    "Backup cleanup parent directory was replaced"
                )
            with descend(child_fd, index + 1) as leaf_fd:
                yield leaf_fd

    with descend(reservation.root_fd, 0) as descriptor:
        return os.dup(descriptor)


def _require_absent_sqlite_sidecars(path: Path, *, state_binding=None) -> None:
    if state_binding is None:
        sidecars = [
            candidate.name
            for candidate in (
                path.with_name(path.name + "-wal"),
                path.with_name(path.name + "-shm"),
                path.with_name(path.name + "-journal"),
            )
            if candidate.exists() or candidate.is_symlink()
        ]
    else:
        state_binding.directory.assert_path_binding()
        sidecars = []
        for suffix in ("-wal", "-shm", "-journal"):
            name = state_binding.name + suffix
            try:
                os.stat(
                    name,
                    dir_fd=state_binding.directory.parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                continue
            sidecars.append(name)
        state_binding.directory.assert_path_binding()
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
