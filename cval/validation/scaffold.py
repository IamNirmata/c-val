"""Dry-run-first creation of a disabled pass/fail-only validation test."""

from __future__ import annotations

import os
import secrets
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from cval.validation.compatibility import (
    COMPATIBILITY_ALIAS_ROWS,
    DEFAULT_TEST_REGISTRATIONS,
)
from cval.validation.registry import TEST_ID_PATTERN
from cval.validation.secure_fs import (
    assert_lexical_directory_identity,
    descriptor_identity,
    mkdir_exact_at,
    open_directory_at,
    open_directory_no_symlinks,
    read_regular_file_at,
    remove_tree_at,
    rename_noreplace_at,
    safe_relative_parts,
    write_file_at,
)


SCAFFOLD_SCHEMA_VERSION = "cval.test-scaffold.v1"
SCAFFOLD_CONFIRMATION = "scaffold"
MAX_TEST_ORDER = 1_000_000
MAX_SCAFFOLD_CONFIG_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class ScaffoldPlan:
    test_id: str
    order: int
    target_dir: Path
    files: tuple[tuple[str, str, int], ...]

    @property
    def registry_stanza(self) -> str:
        return (
            f"[tests.{self.test_id}]\n"
            "enabled = false\n"
            f'config_path = "validation-tests/{self.test_id}/test_config.toml"'
        )

    def to_dict(self, *, applied: bool) -> dict[str, object]:
        return {
            "schema_version": SCAFFOLD_SCHEMA_VERSION,
            "mode": "apply" if applied else "dry-run",
            "applied": applied,
            "test_id": self.test_id,
            "order": self.order,
            "target_dir": str(self.target_dir),
            "files": [name for name, _content, _mode in self.files],
            "registry_stanza": self.registry_stanza,
            "global_config_mutated": False,
            "plugin_created": False,
            "health_created": False,
            "next_commands": [
                "Add the printed disabled stanza to config/cval.toml.",
                f"Implement validation-tests/{self.test_id}/tests/test.sh.",
                "python -m cval.cli tests validate --output json",
                "Review a dry-run render before explicitly enabling the test.",
            ],
        }

def build_scaffold_plan(
    test_id: str,
    order: int,
    *,
    repo_root: str | Path,
) -> ScaffoldPlan:
    """Validate identity/order/path and return a no-write scaffold plan."""

    _validate_identity_and_order(test_id, order)
    root_path, root_fd = open_directory_no_symlinks(repo_root)
    try:
        validation_fd = open_directory_at(root_fd, "validation-tests")
        try:
            _validate_order_collision(root_fd, test_id, order)
            _require_name_absent(validation_fd, test_id)
        finally:
            os.close(validation_fd)
    finally:
        os.close(root_fd)
    return ScaffoldPlan(
        test_id,
        order,
        root_path / "validation-tests" / test_id,
        _template_files(test_id, order),
    )


def scaffold_validation_test(
    test_id: str,
    order: int,
    *,
    repo_root: str | Path,
    apply: bool = False,
    confirmation: str | None = None,
) -> dict[str, object]:
    """Render a plan or atomically publish it after exact confirmation."""

    plan = build_scaffold_plan(test_id, order, repo_root=repo_root)
    if not apply:
        if confirmation is not None:
            raise ValueError("--confirm is valid only with --apply")
        return plan.to_dict(applied=False)
    if confirmation != SCAFFOLD_CONFIRMATION:
        raise ValueError("Scaffold apply requires exact --confirm scaffold")

    root_path, root_fd = open_directory_no_symlinks(repo_root)
    validation_fd = -1
    stage_fd = -1
    tests_fd = -1
    stage_name = ""
    published = False
    published_identity: tuple[int, int] | None = None
    try:
        validation_fd = open_directory_at(root_fd, "validation-tests")
        root_identity = descriptor_identity(root_fd)
        validation_identity = descriptor_identity(validation_fd)
        _validate_order_collision(root_fd, test_id, order)
        _require_name_absent(validation_fd, test_id)

        for _attempt in range(32):
            candidate = (
                f".cval-scaffold-{test_id}-{os.getpid()}-{secrets.token_hex(8)}"
            )
            try:
                stage_fd = mkdir_exact_at(validation_fd, candidate, 0o700)
                stage_name = candidate
                break
            except FileExistsError:
                continue
        else:
            raise FileExistsError("Could not reserve a unique scaffold staging directory")

        tests_fd = mkdir_exact_at(stage_fd, "tests", 0o700)
        for relative, content, mode in plan.files:
            parts = safe_relative_parts(relative, field_name="scaffold file")
            parent_fd = tests_fd if len(parts) == 2 and parts[0] == "tests" else stage_fd
            if len(parts) not in {1, 2}:
                raise ValueError(f"Unsupported scaffold template path: {relative}")
            _write_scaffold_file(parent_fd, parts[-1], content.encode("utf-8"), mode)

        os.fsync(tests_fd)
        os.close(tests_fd)
        tests_fd = -1
        os.fsync(stage_fd)
        published_identity = descriptor_identity(stage_fd)
        os.close(stage_fd)
        stage_fd = -1
        os.fsync(validation_fd)

        assert_lexical_directory_identity(root_path, root_identity)
        assert_lexical_directory_identity(
            root_path / "validation-tests", validation_identity
        )
        rename_noreplace_at(validation_fd, stage_name, validation_fd, test_id)
        published = True
        os.fsync(validation_fd)
        assert_lexical_directory_identity(root_path, root_identity)
        assert_lexical_directory_identity(
            root_path / "validation-tests", validation_identity
        )
    except BaseException:
        if validation_fd >= 0:
            cleanup_name = test_id if published else stage_name
            if cleanup_name:
                _rollback_tree(validation_fd, cleanup_name, published_identity)
                try:
                    os.fsync(validation_fd)
                except OSError:
                    pass
        raise
    finally:
        if tests_fd >= 0:
            os.close(tests_fd)
        if stage_fd >= 0:
            os.close(stage_fd)
        if validation_fd >= 0:
            os.close(validation_fd)
        os.close(root_fd)
    return plan.to_dict(applied=True)


def _write_scaffold_file(parent_fd: int, name: str, payload: bytes, mode: int) -> None:
    """Patchable write boundary used by rollback fault-injection tests."""

    write_file_at(parent_fd, name, payload, mode)


def _rollback_tree(
    parent_fd: int,
    name: str,
    expected_identity: tuple[int, int] | None,
) -> None:
    try:
        metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if expected_identity is not None and (metadata.st_dev, metadata.st_ino) != expected_identity:
        raise RuntimeError(f"Refusing to roll back replaced scaffold path: {name}")
    remove_tree_at(parent_fd, name)


def _require_name_absent(parent_fd: int, name: str) -> None:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise FileExistsError(f"Scaffold target already exists; refusing overwrite: {name}")


def _validate_identity_and_order(test_id: str, order: int) -> None:
    if not isinstance(test_id, str) or not TEST_ID_PATTERN.fullmatch(test_id):
        raise ValueError(f"Invalid validation test ID: {test_id!r}")
    reserved = {"all", "overall"} | {row[1] for row in COMPATIBILITY_ALIAS_ROWS}
    if test_id in reserved:
        raise ValueError(f"Reserved compatibility test ID: {test_id!r}")
    if isinstance(order, bool) or not isinstance(order, int) or not 0 <= order <= MAX_TEST_ORDER:
        raise ValueError(f"Test order must be an integer in [0,{MAX_TEST_ORDER}]")


def _validate_order_collision(root_fd: int, test_id: str, order: int) -> None:
    source_directories: dict[Path, tuple[int, int]] = {}
    registrations: dict[str, dict[str, object]] = {
        registered_id: dict(values)
        for registered_id, values in DEFAULT_TEST_REGISTRATIONS.items()
    }
    try:
        config_payload = _read_scaffold_source(
            root_fd,
            Path("config/cval.toml"),
            source_directories,
        )
    except FileNotFoundError:
        config_payload = b""
    if config_payload:
        try:
            data = tomllib.loads(config_payload.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise ValueError("Cannot inspect scaffold order: invalid config/cval.toml") from exc
        supplied = data.get("tests", {})
        if not isinstance(supplied, Mapping):
            raise ValueError("Config section [tests] must be a table")
        for registered_id, raw in supplied.items():
            if not isinstance(raw, Mapping):
                raise ValueError(f"Config section [tests.{registered_id}] must be a table")
            current = registrations.get(str(registered_id), {})
            current.update(raw)
            registrations[str(registered_id)] = current

    for registered_id, registration in registrations.items():
        config_path = registration.get("config_path")
        if not isinstance(config_path, str) or not config_path:
            continue
        try:
            descriptor_payload = _read_scaffold_source(
                root_fd,
                Path(*safe_relative_parts(config_path, field_name="config_path")),
                source_directories,
            )
        except FileNotFoundError:
            # Disposable scaffold tests need not reproduce compatibility built-ins;
            # explicitly declared missing descriptors still fail registry validation.
            if registered_id not in DEFAULT_TEST_REGISTRATIONS or config_payload:
                raise
            continue
        try:
            descriptor = tomllib.loads(descriptor_payload.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise ValueError(f"Cannot inspect test order for {registered_id!r}") from exc
        metadata = descriptor.get("test")
        if not isinstance(metadata, Mapping):
            raise ValueError(f"Descriptor for {registered_id!r} lacks [test]")
        declared_id = metadata.get("id")
        declared_order = metadata.get("order")
        if declared_id != registered_id:
            raise ValueError(
                f"Registered test ID {registered_id!r} does not match descriptor ID {declared_id!r}"
            )
        if isinstance(declared_order, bool) or not isinstance(declared_order, int):
            raise ValueError(f"Descriptor order for {registered_id!r} must be an integer")
        if declared_order == order and registered_id != test_id:
            raise ValueError(
                f"Test order {order} is already declared by {registered_id!r}"
            )
    _assert_scaffold_sources(root_fd, source_directories)


def _read_scaffold_source(
    root_fd: int,
    relative: Path,
    identities: dict[Path, tuple[int, int]],
) -> bytes:
    parent_fd = open_directory_at(root_fd, relative.parent)
    try:
        identity = descriptor_identity(parent_fd)
        previous = identities.setdefault(relative.parent, identity)
        if previous != identity:
            raise RuntimeError(f"Scaffold source directory changed: {relative.parent}")
        return read_regular_file_at(
            parent_fd,
            relative.name,
            max_bytes=MAX_SCAFFOLD_CONFIG_BYTES,
        )
    finally:
        os.close(parent_fd)


def _assert_scaffold_sources(
    root_fd: int,
    identities: Mapping[Path, tuple[int, int]],
) -> None:
    for relative, expected in identities.items():
        descriptor = open_directory_at(root_fd, relative)
        try:
            if descriptor_identity(descriptor) != expected:
                raise RuntimeError(f"Scaffold source directory changed: {relative}")
        finally:
            os.close(descriptor)


def _template_files(test_id: str, order: int) -> tuple[tuple[str, str, int], ...]:
    display_name = test_id.replace("-", " ").title()
    return (
        (
            "README.md",
            f"# {display_name}\n\n"
            "Pass/fail-only c-val test scaffold. Keep workload logic under `tests/`; "
            "write the canonical summary to `$CVAL_TEST_SUMMARY_FILE` and return zero "
            "only on success. Add plugin/health capabilities later as a separately "
            "reviewed compatibility change.\n",
            0o600,
        ),
        (
            "test_config.toml",
            f'''schema_version = "cval.test.v1"\n\n[test]\nid = "{test_id}"\n'''
            f'''display_name = "{display_name}"\ndescription = "Pass/fail validation test."\n'''
            f'''order = {order}\nentrypoint = "run-test.sh"\nsetup = "setup.sh"\n'''
            '''timeout_seconds = 300\ncontinue_on_failure = true\n\n[requirements]\n'''
            '''cpu = "1"\nmemory = "1Gi"\ngpu_count = 0\nrdma_count = 0\n'''
            '''shared_memory = "0"\nread_sysfs = false\n\n[artifacts]\n'''
            f'''results_db_path = "validation_tests/{test_id}/{test_id}_results.db"\n'''
            '''summary_filename = "summary.json"\n''',
            0o600,
        ),
        (
            "setup.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\n# Add deterministic dependency checks only; do not mutate global configuration.\n",
            0o755,
        ),
        (
            "run-test.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            ": \"${CVAL_TEST_SUMMARY_FILE:?CVAL_TEST_SUMMARY_FILE is required}\"\n"
            "bash \"${CVAL_TEST_DIR:?CVAL_TEST_DIR is required}/tests/test.sh\"\n",
            0o755,
        ),
        (
            "tests/README.md",
            "# Workload tests\n\nReplace the fail-closed placeholder with deterministic checks. "
            "Exit `0` for pass and non-zero for fail; never emit health classes here.\n",
            0o600,
        ),
        (
            "tests/test.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            "printf '%s\\n' '{\"status\":\"fail\",\"message\":\"placeholder not implemented\"}' "
            "> \"$CVAL_TEST_SUMMARY_FILE\"\n"
            "echo 'Scaffold workload is not implemented' >&2\nexit 1\n",
            0o755,
        ),
    )


__all__ = ["build_scaffold_plan", "scaffold_validation_test"]