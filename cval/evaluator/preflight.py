"""Read-only U11 deployment preflight over local or mounted evaluator inputs."""

from __future__ import annotations

import os
import stat
from contextlib import closing
from pathlib import Path
from typing import Any

from cval.config import CvalConfig
from cval.evaluator.release import effective_config_digest
from cval.health.storage import load_baselines_generation, resolve_health_db_path
from cval.storage.per_test_results import (
    common_result_schema_version,
    resolve_test_results_db_path,
    validate_common_result_connection,
    validate_test_result_owner_integrity,
)
from cval.storage.sqlite_snapshot import immutable_sqlite_snapshot
from cval.validation.plugins import validate_registry_plugins


PREFLIGHT_SCHEMA = "cval.evaluator-preflight.v1"
_OWNER_READ_ONLY_MODES = frozenset({0o400, 0o600})
_OWNER_READ_WRITE_MODE = 0o600
_DIRECTORY_UNSAFE_BITS = stat.S_IWGRP | stat.S_IWOTH
_TEST_OWNER_DIRECTORY_MODE = 0o700


def run_deployment_preflight(
    config: CvalConfig,
    *,
    access: str = "ro",
) -> dict[str, Any]:
    """Validate registry, source paths, ownership, modes, and SQLite inputs.

    The function creates no directories, locks, SQLite sidecars, or output files.
    It is intentionally suitable for a read-only PVC mount.
    """

    if access not in {"ro", "rw"}:
        raise ValueError("preflight access must be 'ro' or 'rw'")
    checks: list[dict[str, Any]] = []
    tests: list[dict[str, Any]] = []
    root = Path(config.runtime.validation_root).expanduser()
    approved_root_mode = int(config.health_evaluator.validation_root_mode, 8)

    try:
        plugins = validate_registry_plugins(config.tests.registry.tests)
        _check(checks, "registry", True, f"{len(plugins)} plugin(s) validated")
    except Exception as exc:  # noqa: BLE001 - structured preflight boundary
        plugins = ()
        _check(checks, "registry", False, _safe_error(exc))

    root_ok, root_detail = _directory_check(
        root,
        require_writable=access == "rw",
        exact_mode=approved_root_mode,
    )
    _check(checks, "validation-root", root_ok, root_detail)

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
        result_path = resolve_test_results_db_path(root, registered)
        health_path = resolve_health_db_path(root, registered)
        test_checks: list[dict[str, Any]] = []
        owner_ok, owner_detail, result_ancestry = _directory_ancestry_check(
            root,
            result_path.parent,
            approved_root_mode=approved_root_mode,
            allow_missing=False,
            require_leaf_writable=access == "rw",
        )
        _check(test_checks, "owner-directory", owner_ok, owner_detail)

        result_schema: int | None = None
        result_present = result_path.exists() or result_path.is_symlink()
        result_sidecars = _sqlite_sidecars(result_path)
        _check(
            test_checks,
            "result-db-sidecars",
            not result_sidecars,
            "absent" if not result_sidecars else "present: " + ", ".join(result_sidecars),
        )
        if not result_present:
            _check(test_checks, "result-db", False, "canonical U7 result DB is missing")
        else:
            file_ok, detail = _owned_regular_file(
                result_path,
                require_writable=access == "rw",
            )
            _check(test_checks, "result-db-file", file_ok, detail)
            if file_ok and not result_sidecars:
                try:
                    with immutable_sqlite_snapshot(result_path) as snapshot:
                        with closing(snapshot.connect()) as connection:
                            validate_common_result_connection(connection)
                            validate_test_result_owner_integrity(
                                connection,
                                test_id=registered.id,
                            )
                            result_schema = common_result_schema_version(connection)
                    _check(test_checks, "result-db-schema", True, f"exact v{result_schema}")
                except Exception as exc:  # noqa: BLE001
                    _check(test_checks, "result-db-schema", False, _safe_error(exc))

        health_present = health_path.exists() or health_path.is_symlink()
        health_parent_ok, health_parent_detail, health_ancestry = (
            _directory_ancestry_check(
                root,
                health_path.parent,
                approved_root_mode=approved_root_mode,
                allow_missing=not health_present,
                require_leaf_writable=True,
            )
        )
        _check(
            test_checks,
            "health-owner-directory",
            health_parent_ok,
            health_parent_detail,
        )
        key_path = health_path.with_name(f"{health_path.name}.activation.key")
        health_sidecars = _sqlite_sidecars(health_path)
        _check(
            test_checks,
            "health-db-sidecars",
            not health_sidecars,
            "absent" if not health_sidecars else "present: " + ", ".join(health_sidecars),
        )
        if health_present:
            file_ok, detail = _owned_regular_file(
                health_path,
                require_writable=access == "rw",
            )
            _check(test_checks, "health-db-file", file_ok, detail)
            key_ok, key_detail = _owned_regular_file(
                key_path,
                exact_mode=0o600,
                exact_size=32,
                require_writable=access == "rw",
            )
            _check(test_checks, "activation-key", key_ok, key_detail)
            if file_ok and not health_sidecars and key_ok:
                try:
                    stored, _generation = load_baselines_generation(
                        db_path=health_path,
                        test_id=registered.id,
                    )
                    _check(
                        test_checks,
                        "health-db-schema",
                        True,
                        f"exact v1; {len(stored)} baseline(s)",
                    )
                except Exception as exc:  # noqa: BLE001
                    _check(test_checks, "health-db-schema", False, _safe_error(exc))
        else:
            _check(
                test_checks,
                "health-db",
                not key_path.exists()
                and not key_path.is_symlink()
                and not health_sidecars,
                "absent; creation is apply-only"
                if not key_path.exists() and not key_path.is_symlink() and not health_sidecars
                else "activation key or SQLite sidecar exists without its U8 DB",
            )

        stable_ok, stable_detail = _revalidate_directory_ancestry(
            (*result_ancestry, *health_ancestry)
        )
        _check(
            test_checks,
            "directory-ancestry-stable",
            stable_ok,
            stable_detail,
        )

        tests.append(
            {
                "test_id": registered.id,
                "ready": all(item["ok"] for item in test_checks),
                "result_db_path": str(result_path),
                "health_db_path": str(health_path),
                "result_schema_version": result_schema,
                "health_db_present": health_present,
                "checks": test_checks,
            }
        )

    ready = all(item["ok"] for item in checks) and bool(tests) and all(
        item["ready"] for item in tests
    )
    return {
        "schema_version": PREFLIGHT_SCHEMA,
        "ok": ready,
        "access": access,
        "config_digest": effective_config_digest(config),
        "validation_root": str(root),
        "registered_count": len(config.tests.registry.tests),
        "enabled_count": len(config.tests.registry.enabled),
        "eligible_count": len(tests),
        "plugins": list(plugins),
        "checks": checks,
        "tests": tests,
        "limitations": [
            "Local filesystem facts only; no Kubernetes API, PVC, or storage-class facts are queried.",
            "A successful read-only preflight does not authorize backup, evaluator apply, activation, or cutover.",
        ],
    }


def _directory_check(
    path: Path,
    *,
    require_writable: bool,
    exact_mode: int,
) -> tuple[bool, str]:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        return False, _safe_error(exc)
    mode = stat.S_IMODE(metadata.st_mode)
    facts = f"uid={metadata.st_uid} gid={metadata.st_gid} mode={mode:04o}"
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink():
        return False, "not a canonical directory; " + facts
    if metadata.st_uid != os.geteuid():
        return False, "not owned by evaluator uid; " + facts
    if mode & _DIRECTORY_UNSAFE_BITS:
        return False, "group/world writable directory is unsafe; " + facts
    if mode != exact_mode:
        return False, f"mode must be {exact_mode:04o}; " + facts
    if not mode & stat.S_IRUSR or not mode & stat.S_IXUSR:
        return False, "owner read/search permissions are required; " + facts
    if require_writable and (
        not mode & stat.S_IWUSR or not os.access(path, os.W_OK | os.X_OK)
    ):
        return False, "not writable for apply; " + facts
    return True, facts


def _directory_ancestry_check(
    root: Path,
    target: Path,
    *,
    approved_root_mode: int,
    allow_missing: bool,
    require_leaf_writable: bool,
) -> tuple[bool, str, tuple[tuple[Path, int, int, int], ...]]:
    """Validate every existing root-to-target directory and capture identities."""

    if not root.is_absolute() or not target.is_absolute():
        return False, "directory ancestry paths must be absolute", ()
    try:
        relative = target.relative_to(root)
    except ValueError:
        return False, f"directory ancestry escapes validation root: {target}", ()
    paths = [root]
    current = root
    for part in relative.parts:
        current = current / part
        paths.append(current)
    identities: list[tuple[Path, int, int, int]] = []
    missing = 0
    for index, candidate in enumerate(paths):
        try:
            metadata = os.lstat(candidate)
        except FileNotFoundError:
            if not allow_missing:
                return False, f"directory ancestry component is missing: {candidate}", ()
            missing = len(paths) - index
            break
        except OSError as exc:
            return False, _safe_error(exc), ()
        expected_mode = approved_root_mode if index == 0 else _TEST_OWNER_DIRECTORY_MODE
        ok, detail = _directory_check(
            candidate,
            require_writable=require_leaf_writable and index == len(paths) - 1,
            exact_mode=expected_mode,
        )
        if not ok:
            return False, f"{candidate}: {detail}", ()
        identities.append(
            (candidate, metadata.st_dev, metadata.st_ino, stat.S_IMODE(metadata.st_mode))
        )
    if not identities:
        return False, "directory ancestry has no existing validation root", ()
    if missing and require_leaf_writable:
        nearest = identities[-1][0]
        mode = identities[-1][3]
        if not mode & stat.S_IWUSR or not os.access(nearest, os.W_OK | os.X_OK):
            return False, f"nearest creation ancestor is not writable: {nearest}", ()
    detail = f"validated {len(identities)} existing component(s)"
    if missing:
        detail += (
            f"; {missing} missing component(s); safe creation ancestor "
            f"{identities[-1][0]}"
        )
    return True, detail, tuple(identities)


def _revalidate_directory_ancestry(
    identities: tuple[tuple[Path, int, int, int], ...],
) -> tuple[bool, str]:
    """Detect a component replacement or symlink swap during preflight."""

    unique = {identity[0]: identity for identity in identities}
    for path, expected in sorted(unique.items(), key=lambda item: str(item[0])):
        try:
            metadata = os.lstat(path)
        except OSError as exc:
            return False, f"directory ancestry changed at {path}: {_safe_error(exc)}"
        actual = (
            path,
            metadata.st_dev,
            metadata.st_ino,
            stat.S_IMODE(metadata.st_mode),
        )
        if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink() or actual != expected:
            return False, f"directory ancestry changed or became a symlink at {path}"
        if metadata.st_uid != os.geteuid() or not metadata.st_mode & stat.S_IXUSR:
            return False, f"directory ancestry ownership/search changed at {path}"
    return True, f"revalidated {len(unique)} component(s)"


def _owned_regular_file(
    path: Path,
    *,
    exact_mode: int | None = None,
    exact_size: int | None = None,
    require_writable: bool = False,
) -> tuple[bool, str]:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        return False, _safe_error(exc)
    mode = stat.S_IMODE(metadata.st_mode)
    facts = (
        f"uid={metadata.st_uid} gid={metadata.st_gid} mode={mode:04o} "
        f"size={metadata.st_size} links={metadata.st_nlink}"
    )
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        return False, "not a regular no-symlink file; " + facts
    if metadata.st_uid != os.geteuid():
        return False, "not owned by evaluator uid; " + facts
    if metadata.st_nlink != 1:
        return False, "link count must be exactly 1; " + facts
    if exact_mode is not None and mode != exact_mode:
        return False, f"mode must be {exact_mode:04o}; " + facts
    if exact_mode is None:
        required_modes = (
            frozenset({_OWNER_READ_WRITE_MODE})
            if require_writable
            else _OWNER_READ_ONLY_MODES
        )
        if mode not in required_modes:
            allowed = "/".join(f"{value:04o}" for value in sorted(required_modes))
            return False, f"mode must be {allowed}; " + facts
    if exact_size is not None and metadata.st_size != exact_size:
        return False, f"size must be {exact_size}; " + facts
    if require_writable and (
        not mode & stat.S_IWUSR or not os.access(path, os.W_OK)
    ):
        return False, "not writable for apply; " + facts
    return True, facts


def _sqlite_sidecars(path: Path) -> list[str]:
    return [
        candidate.name
        for candidate in (
            path.with_name(path.name + "-wal"),
            path.with_name(path.name + "-shm"),
            path.with_name(path.name + "-journal"),
        )
        if candidate.exists() or candidate.is_symlink()
    ]


def _check(items: list[dict[str, Any]], name: str, ok: bool, detail: str) -> None:
    items.append({"name": name, "ok": bool(ok), "detail": detail})


def _safe_error(exc: BaseException) -> str:
    return " ".join((str(exc).strip() or exc.__class__.__name__).splitlines())


__all__ = ["PREFLIGHT_SCHEMA", "run_deployment_preflight"]
