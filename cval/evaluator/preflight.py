"""Read-only U11 deployment preflight over local or mounted evaluator inputs."""

from __future__ import annotations

import os
from contextlib import ExitStack, closing
from pathlib import Path
from typing import Any, Iterator

from cval.config import CvalConfig
from cval.evaluator.release import effective_config_digest
from cval.evaluator.state import (
    bind_state_target,
    StateDirectoryIdentity,
    StateFileIdentity,
    configured_state_root,
    inspect_state_ancestry,
    inspect_state_target,
    open_state_root,
)
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
    root = configured_state_root(config)

    try:
        plugins = validate_registry_plugins(config.tests.registry.tests)
        _check(checks, "registry", True, f"{len(plugins)} plugin(s) validated")
    except Exception as exc:  # noqa: BLE001 - structured preflight boundary
        plugins = ()
        _check(checks, "registry", False, _safe_error(exc))

    try:
        with open_state_root(config, require_writable=access == "rw") as (
            _descriptor,
            identity,
        ):
            root_detail = (
                f"uid={identity.owner_uid} gid={identity.owner_gid} "
                f"mode={identity.mode:04o} dev={identity.device} ino={identity.inode}"
            )
        root_ok = True
    except Exception as exc:  # noqa: BLE001 - complete structured report
        root_ok = False
        root_detail = _safe_error(exc)
    _check(checks, "state-root", root_ok, root_detail)

    for registered, retained in _preflight_test_stacks(
        config.tests.registry.enabled
    ):
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
        retained_bindings = []
        ancestry_captures: list[
            tuple[Path, bool, tuple[StateDirectoryIdentity, ...]]
        ] = []
        for target, allow_missing in (
            (result_path, False),
            (health_path, True),
        ):
            try:
                captured = inspect_state_ancestry(
                    config,
                    target,
                    allow_missing=allow_missing,
                    require_writable=access == "rw",
                )
                ancestry_captures.append((target, allow_missing, captured))
            except Exception as exc:  # noqa: BLE001 - structured report
                _check(
                    test_checks,
                    "state-directory-ancestry",
                    False,
                    _safe_error(exc),
                )
        if len(ancestry_captures) == 2:
            _check(
                test_checks,
                "state-directory-ancestry",
                True,
                "captured exact owner/gid/mode/device/inode ancestry",
            )
        result_schema: int | None = None
        result_identity: StateFileIdentity | None = None
        try:
            result_binding = retained.enter_context(
                bind_state_target(
                    config,
                    result_path,
                    create=False,
                    allow_missing=True,
                    writable=False,
                    require_writable=access == "rw",
                )
            )
            retained_bindings.append(result_binding)
            result_identity = result_binding.identity
        except Exception as exc:  # noqa: BLE001 - structured report
            result_binding = None
            _check(test_checks, "result-db-file", False, _safe_error(exc))
        result_present = result_identity is not None
        try:
            result_sidecars = (
                _sqlite_sidecars_binding(result_binding)
                if result_binding is not None
                else _sqlite_sidecars(result_path)
            )
        except Exception as exc:  # noqa: BLE001 - structured preflight boundary
            result_sidecars = ["inspection-failed"]
            _check(test_checks, "result-db-sidecars", False, _safe_error(exc))
        if result_sidecars != ["inspection-failed"]:
            _check(
                test_checks,
                "result-db-sidecars",
                not result_sidecars,
                "absent" if not result_sidecars else "present: " + ", ".join(result_sidecars),
            )
        if not result_present:
            _check(test_checks, "result-db", False, "canonical U7 result DB is missing")
        else:
            assert result_binding is not None and result_identity is not None
            try:
                size = os.fstat(result_binding.descriptor).st_size
                file_ok = True
                detail = (
                    f"uid={result_identity.owner_uid} gid={result_identity.owner_gid} "
                    f"mode={result_identity.mode:04o} links={result_identity.link_count} "
                    f"dev={result_identity.device} ino={result_identity.inode} size={size}"
                )
            except Exception as exc:  # noqa: BLE001
                file_ok = False
                detail = _safe_error(exc)
            _check(test_checks, "result-db-file", file_ok, detail)
            if file_ok and not result_sidecars:
                try:
                    result_binding.assert_path_binding()
                    with immutable_sqlite_snapshot(
                        result_path,
                        expected_identity=result_identity.sqlite_identity(),
                        source_fd=result_binding.descriptor,
                        source_parent_fd=result_binding.directory.parent_fd,
                        source_name=result_binding.name,
                        binding_guard=result_binding.assert_path_binding,
                    ) as snapshot:
                        with closing(snapshot.connect()) as connection:
                            validate_common_result_connection(connection)
                            validate_test_result_owner_integrity(
                                connection,
                                test_id=registered.id,
                            )
                            result_schema = common_result_schema_version(connection)
                            result_binding.assert_path_binding()
                            result_binding.assert_path_binding()
                    _check(test_checks, "result-db-schema", True, f"exact v{result_schema}")
                except Exception as exc:  # noqa: BLE001
                    _check(test_checks, "result-db-schema", False, _safe_error(exc))

        try:
            health_binding = retained.enter_context(
                bind_state_target(
                    config,
                    health_path,
                    create=False,
                    allow_missing=True,
                    writable=False,
                    require_writable=access == "rw",
                )
            )
            retained_bindings.append(health_binding)
            health_identity = health_binding.identity
        except Exception as exc:  # noqa: BLE001 - structured report
            health_binding = None
            health_identity = None
            _check(test_checks, "health-db-file", False, _safe_error(exc))
        health_present = health_identity is not None
        key_identity: StateFileIdentity | None = None
        key_path = health_path.with_name(f"{health_path.name}.activation.key")
        try:
            key_binding = retained.enter_context(
                bind_state_target(
                    config,
                    key_path,
                    create=False,
                    allow_missing=True,
                    writable=False,
                    require_writable=access == "rw",
                )
            )
            retained_bindings.append(key_binding)
            key_identity = key_binding.identity
        except Exception as exc:  # noqa: BLE001 - structured report
            key_binding = None
            key_identity = None
            _check(test_checks, "activation-key", False, _safe_error(exc))
        try:
            health_sidecars = (
                _sqlite_sidecars_binding(health_binding)
                if health_binding is not None
                else _sqlite_sidecars(health_path)
            )
        except Exception as exc:  # noqa: BLE001
            health_sidecars = ["inspection-failed"]
            _check(test_checks, "health-db-sidecars", False, _safe_error(exc))
        if health_sidecars != ["inspection-failed"]:
            _check(
                test_checks,
                "health-db-sidecars",
                not health_sidecars,
                "absent" if not health_sidecars else "present: " + ", ".join(health_sidecars),
            )
        if health_present:
            assert health_binding is not None and health_identity is not None
            try:
                health_size = os.fstat(health_binding.descriptor).st_size
                file_ok = True
                detail = (
                    f"uid={health_identity.owner_uid} gid={health_identity.owner_gid} "
                    f"mode={health_identity.mode:04o} links={health_identity.link_count} "
                    f"dev={health_identity.device} ino={health_identity.inode} "
                    f"size={health_size}"
                )
            except Exception as exc:  # noqa: BLE001
                file_ok = False
                detail = _safe_error(exc)
            _check(test_checks, "health-db-file", file_ok, detail)
            key_ok = key_identity is not None and key_binding is not None
            if key_ok:
                try:
                    key_size = os.fstat(key_binding.descriptor).st_size
                    key_ok = key_size == 32
                    key_detail = (
                        f"uid={key_identity.owner_uid} gid={key_identity.owner_gid} "
                        f"mode={key_identity.mode:04o} links={key_identity.link_count} "
                        f"dev={key_identity.device} ino={key_identity.inode} size={key_size}"
                        if key_ok
                        else f"Evaluator state file size must be 32, got {key_size}: {key_path}"
                    )
                except Exception as exc:  # noqa: BLE001
                    key_ok = False
                    key_detail = _safe_error(exc)
            else:
                key_detail = "Health activation key is missing"
            _check(test_checks, "activation-key", key_ok, key_detail)
            if file_ok and not health_sidecars and key_ok:
                try:
                    health_binding.assert_path_binding()
                    key_binding.assert_path_binding()
                    stored, _generation = load_baselines_generation(
                        db_path=health_path,
                        test_id=registered.id,
                        expected_identity=health_identity.sqlite_identity(),
                        state_binding=health_binding,
                        key_binding=key_binding,
                    )
                    health_binding.assert_path_binding()
                    key_binding.assert_path_binding()
                    _check(
                        test_checks,
                        "health-db-schema",
                        True,
                        f"exact v1; {len(stored)} baseline(s)",
                    )
                except Exception as exc:  # noqa: BLE001
                    _check(test_checks, "health-db-schema", False, _safe_error(exc))
        else:
            health_identity = None
            parent_ok, parent_detail = _inspect_missing_state_target(
                config,
                health_path,
                require_writable=access == "rw",
            )
            _check(test_checks, "health-owner-directory", parent_ok, parent_detail)
            _check(
                test_checks,
                "health-db",
                key_identity is None and not health_sidecars,
                "absent; creation is apply-only"
                if key_identity is None and not health_sidecars
                else "activation key or SQLite sidecar exists without its U8 DB",
            )

        try:
            for binding in retained_bindings:
                binding.assert_path_binding()
            retained.close()
            stable_ok = True
            stable_detail = (
                f"revalidated {len(retained_bindings)} retained state binding(s)"
            )
        except Exception as exc:  # noqa: BLE001 - structured report
            stable_ok = False
            stable_detail = _safe_error(exc)
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
        "state_root": str(root),
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


def _preflight_test_stacks(
    registered_tests: Any,
) -> Iterator[tuple[Any, ExitStack]]:
    """Yield one explicitly context-managed descriptor stack per test."""

    for registered in registered_tests:
        with ExitStack() as retained:
            try:
                yield registered, retained
            finally:
                try:
                    retained.close()
                except Exception:
                    # The main preflight body records close/revalidation errors as
                    # structured checks. This drain only prevents double-close.
                    pass


def _inspect_state_file(
    config: CvalConfig,
    path: Path,
    *,
    require_writable: bool,
    exact_size: int | None = None,
) -> tuple[StateFileIdentity | None, bool, str]:
    try:
        identity = inspect_state_target(
            config,
            path,
            allow_missing=False,
            require_writable=require_writable,
        )
        if identity is None:
            raise FileNotFoundError(path)
        size = os.lstat(path).st_size
        if exact_size is not None and size != exact_size:
            raise PermissionError(
                f"Evaluator state file size must be {exact_size}, got {size}: {path}"
            )
        return (
            identity,
            True,
            f"uid={identity.owner_uid} gid={identity.owner_gid} "
            f"mode={identity.mode:04o} links={identity.link_count} "
            f"dev={identity.device} ino={identity.inode} size={size}",
        )
    except Exception as exc:  # noqa: BLE001 - structured report
        return None, False, _safe_error(exc)


def _inspect_missing_state_target(
    config: CvalConfig,
    path: Path,
    *,
    require_writable: bool,
) -> tuple[bool, str]:
    try:
        identity = inspect_state_target(
            config,
            path,
            allow_missing=True,
            require_writable=require_writable,
        )
        if identity is not None:
            return False, "state target appeared during absent-target preflight"
        return True, "existing state ancestry is exact; target may be created only by apply"
    except Exception as exc:  # noqa: BLE001 - structured report
        return False, _safe_error(exc)


def _revalidate_state_files(
    config: CvalConfig,
    identities: tuple[StateFileIdentity, ...],
    ancestry_captures: tuple[
        tuple[Path, bool, tuple[StateDirectoryIdentity, ...]], ...
    ],
    *,
    require_writable: bool,
) -> tuple[bool, str]:
    try:
        for target, allow_missing, expected in ancestry_captures:
            actual_ancestry = inspect_state_ancestry(
                config,
                target,
                allow_missing=allow_missing,
                require_writable=require_writable,
            )
            if actual_ancestry != expected:
                raise RuntimeError(
                    f"Evaluator state directory ancestry changed: {target}"
                )
        for expected in identities:
            actual = inspect_state_target(
                config,
                expected.path,
                allow_missing=False,
                require_writable=require_writable,
            )
            if actual != expected:
                raise RuntimeError(
                    f"Evaluator state file identity changed: {expected.path}"
                )
        return (
            True,
            f"revalidated {len(ancestry_captures)} ancestries and "
            f"{len(identities)} state file(s)",
        )
    except Exception as exc:  # noqa: BLE001 - structured report
        return False, _safe_error(exc)


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


def _sqlite_sidecars_binding(binding: Any) -> list[str]:
    binding.directory.assert_path_binding()
    present: list[str] = []
    for suffix in ("-wal", "-shm", "-journal"):
        name = binding.name + suffix
        try:
            os.stat(
                name,
                dir_fd=binding.directory.parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            continue
        present.append(name)
    binding.directory.assert_path_binding()
    return present


def _check(items: list[dict[str, Any]], name: str, ok: bool, detail: str) -> None:
    items.append({"name": name, "ok": bool(ok), "detail": detail})


def _safe_error(exc: BaseException) -> str:
    return " ".join((str(exc).strip() or exc.__class__.__name__).splitlines())


__all__ = ["PREFLIGHT_SCHEMA", "run_deployment_preflight"]
