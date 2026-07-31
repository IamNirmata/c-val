"""Assemble evaluator descriptors/plugins from the explicit test registry."""

from __future__ import annotations

import argparse
import os
import secrets
import tempfile
import tomllib
from collections.abc import Mapping
from pathlib import Path

from cval.validation.plugins import validate_registry_plugins
from cval.validation.registry import load_test_registry
from cval.validation.secure_fs import (
    assert_lexical_directory_identity,
    descriptor_identity,
    lexical_absolute,
    mkdir_exact_at,
    open_directory_at,
    open_directory_no_symlinks,
    read_regular_file_at,
    remove_tree_at,
    rename_noreplace_at,
    safe_relative_parts,
    write_file_at,
)


MAX_CATALOG_FILES = 256
MAX_CATALOG_FILE_BYTES = 16 * 1024 * 1024


def assemble_evaluator_catalog(
    *,
    source_root: str | Path,
    config_path: str | Path,
    destination_root: str | Path,
) -> tuple[str, ...]:
    """Validate then atomically publish registered descriptors/adapters."""

    source_path, source_fd = open_directory_no_symlinks(source_root)
    try:
        source_identity = descriptor_identity(source_fd)
        config_relative = _relative_under(source_path, config_path, "config")
        source_directories: dict[Path, tuple[int, int]] = {}
        config_payload = _read_source_file(
            source_fd, config_relative, source_directories
        )
        files, planned, tests, source_directories = _collect_source_files(
            source_fd, config_relative, config_payload, source_directories
        )
        _validate_snapshot(config_relative, config_payload, files, tests)
        _assert_source_directories(source_fd, source_directories)
        assert_lexical_directory_identity(source_path, source_identity)
    finally:
        os.close(source_fd)

    destination_path, destination_fd = open_directory_no_symlinks(destination_root)
    stage_fd = -1
    stage_name = ""
    published = False
    published_identity: tuple[int, int] | None = None
    try:
        destination_identity = descriptor_identity(destination_fd)
        _require_absent(destination_fd, "validation-tests")
        for _attempt in range(32):
            candidate = f".cval-catalog-{os.getpid()}-{secrets.token_hex(8)}"
            try:
                stage_fd = mkdir_exact_at(destination_fd, candidate, 0o700)
                stage_name = candidate
                break
            except FileExistsError:
                continue
        else:
            raise FileExistsError("Could not reserve evaluator catalog staging directory")

        for relative in planned:
            parts = safe_relative_parts(relative, field_name="catalog destination")
            parent_fd = _ensure_directory_path(stage_fd, parts[:-1])
            try:
                write_file_at(parent_fd, parts[-1], files[relative], 0o600)
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        os.fsync(stage_fd)
        catalog_fd = open_directory_at(stage_fd, "validation-tests")
        try:
            published_identity = descriptor_identity(catalog_fd)
        finally:
            os.close(catalog_fd)
        assert_lexical_directory_identity(destination_path, destination_identity)
        rename_noreplace_at(
            stage_fd,
            "validation-tests",
            destination_fd,
            "validation-tests",
        )
        published = True
        os.fsync(destination_fd)
        _validate_installed_catalog(destination_path, tests)
        os.close(stage_fd)
        stage_fd = -1
        os.rmdir(stage_name, dir_fd=destination_fd)
        stage_name = ""
        os.fsync(destination_fd)
        assert_lexical_directory_identity(destination_path, destination_identity)
    except BaseException:
        if stage_fd >= 0:
            os.close(stage_fd)
            stage_fd = -1
        if published:
            _remove_catalog_if_identity(
                destination_fd, "validation-tests", published_identity
            )
        if stage_name:
            remove_tree_at(destination_fd, stage_name)
        try:
            os.fsync(destination_fd)
        except OSError:
            pass
        raise
    finally:
        if stage_fd >= 0:
            os.close(stage_fd)
        os.close(destination_fd)
    return tuple(relative.as_posix() for relative in planned)


def _collect_source_files(
    source_fd: int,
    config_relative: Path,
    config_payload: bytes,
    source_directories: dict[Path, tuple[int, int]],
) -> tuple[
    dict[Path, bytes],
    tuple[Path, ...],
    Mapping[str, object],
    dict[Path, tuple[int, int]],
]:
    try:
        data = tomllib.loads(config_payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ValueError("Evaluator catalog config is not valid UTF-8 TOML") from exc
    tests = data.get("tests", {})
    if not isinstance(tests, Mapping):
        raise ValueError("Evaluator catalog [tests] must be a table")
    files: dict[Path, bytes] = {config_relative: config_payload}
    planned: list[Path] = []
    support: list[Path] = []
    for test_id, raw in tests.items():
        if not isinstance(raw, Mapping):
            raise ValueError(f"Evaluator catalog [tests.{test_id}] must be a table")
        config_path = raw.get("config_path")
        if not isinstance(config_path, str):
            raise ValueError(f"Evaluator catalog tests.{test_id}.config_path is required")
        relative_config = Path(*safe_relative_parts(config_path, field_name="config_path"))
        descriptor_payload = _read_source_file(
            source_fd, relative_config, source_directories
        )
        files[relative_config] = descriptor_payload
        planned.append(relative_config)
        try:
            descriptor = tomllib.loads(descriptor_payload.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise ValueError(f"Invalid evaluator descriptor for {test_id!r}") from exc
        metadata = descriptor.get("test")
        if not isinstance(metadata, Mapping):
            raise ValueError(f"Evaluator descriptor for {test_id!r} lacks [test]")
        for field in ("setup", "entrypoint"):
            value = metadata.get(field)
            if not isinstance(value, str):
                raise ValueError(f"Evaluator descriptor {test_id!r} lacks test.{field}")
            support.append(
                relative_config.parent
                / Path(*safe_relative_parts(value, field_name=f"test.{field}"))
            )
        plugin = descriptor.get("plugin")
        if plugin is not None:
            if not isinstance(plugin, Mapping) or not isinstance(plugin.get("adapter"), str):
                raise ValueError(f"Evaluator descriptor {test_id!r} has invalid [plugin]")
            adapter = relative_config.parent / Path(
                *safe_relative_parts(plugin["adapter"], field_name="plugin.adapter")
            )
            planned.append(adapter)
            support_files = plugin.get("support_files", ())
            if not isinstance(support_files, list | tuple) or not all(
                isinstance(value, str) for value in support_files
            ):
                raise ValueError(
                    f"Evaluator descriptor {test_id!r} has invalid plugin.support_files"
                )
            for index, value in enumerate(support_files):
                planned.append(
                    relative_config.parent
                    / Path(
                        *safe_relative_parts(
                            value,
                            field_name=f"plugin.support_files[{index}]",
                        )
                    )
                )
    ordered_inputs = tuple(dict.fromkeys((*planned, *support)))
    if len(ordered_inputs) + 1 > MAX_CATALOG_FILES:
        raise ValueError(f"Evaluator catalog exceeds {MAX_CATALOG_FILES} files")
    for relative in ordered_inputs:
        if relative not in files:
            files[relative] = _read_source_file(
                source_fd, relative, source_directories
            )
    planned_unique = ordered_inputs
    if any(relative.parts[0] != "validation-tests" for relative in planned_unique):
        raise ValueError("Evaluator descriptors/adapters must be under validation-tests/")
    return files, planned_unique, tests, source_directories


def _read_source_file(
    source_fd: int,
    relative: Path,
    identities: dict[Path, tuple[int, int]],
) -> bytes:
    parent = relative.parent
    parent_fd = open_directory_at(source_fd, parent)
    try:
        identity = descriptor_identity(parent_fd)
        previous = identities.setdefault(parent, identity)
        if previous != identity:
            raise RuntimeError(f"Evaluator source directory changed: {parent}")
        return read_regular_file_at(
            parent_fd,
            relative.name,
            max_bytes=MAX_CATALOG_FILE_BYTES,
        )
    finally:
        os.close(parent_fd)


def _assert_source_directories(
    source_fd: int,
    identities: Mapping[Path, tuple[int, int]],
) -> None:
    for relative, expected in identities.items():
        descriptor = open_directory_at(source_fd, relative)
        try:
            if descriptor_identity(descriptor) != expected:
                raise RuntimeError(f"Evaluator source directory changed: {relative}")
        finally:
            os.close(descriptor)


def _validate_snapshot(
    config_relative: Path,
    config_payload: bytes,
    files: Mapping[Path, bytes],
    tests: Mapping[str, object],
) -> None:
    with tempfile.TemporaryDirectory(prefix="cval-catalog-validation-") as tmpdir:
        root = Path(tmpdir)
        os.chmod(root, 0o700)
        for relative, payload in files.items():
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(target.parent, 0o700)
            target.write_bytes(payload)
            os.chmod(target, 0o600)
        if (root / config_relative).read_bytes() != config_payload:
            raise RuntimeError("Evaluator config snapshot identity mismatch")
        registry = load_test_registry(
            tests,
            repo_root=root,
            include_defaults=False,
            require_enabled=False,
        )
        validate_registry_plugins(registry.tests)


def _validate_installed_catalog(
    destination_root: Path,
    tests: Mapping[str, object],
) -> None:
    """Load descriptors and adapters from their final installed paths."""

    registry = load_test_registry(
        tests,
        repo_root=destination_root,
        include_defaults=False,
        require_enabled=False,
    )
    validate_registry_plugins(registry.tests)


def _relative_under(root: Path, candidate: str | Path, field_name: str) -> Path:
    absolute = lexical_absolute(candidate)
    try:
        common = Path(os.path.commonpath((root, absolute)))
    except ValueError as exc:
        raise ValueError(f"Evaluator catalog {field_name} is outside source root") from exc
    if common != root:
        raise ValueError(f"Evaluator catalog {field_name} is outside source root")
    relative = Path(os.path.relpath(absolute, root))
    safe_relative_parts(relative, field_name=field_name)
    return relative


def _ensure_directory_path(root_fd: int, parts: tuple[str, ...]) -> int:
    descriptor = os.dup(root_fd)
    try:
        for part in parts:
            try:
                next_descriptor = mkdir_exact_at(descriptor, part, 0o700)
                os.fsync(descriptor)
            except FileExistsError:
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
                next_descriptor = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _require_absent(parent_fd: int, name: str) -> None:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise FileExistsError(f"Evaluator catalog target exists: {name}")


def _remove_catalog_if_identity(
    parent_fd: int,
    name: str,
    expected: tuple[int, int] | None,
) -> None:
    metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if expected is None or (metadata.st_dev, metadata.st_ino) != expected:
        raise RuntimeError("Refusing to remove a replaced evaluator catalog")
    remove_tree_at(parent_fd, name)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--destination-root", type=Path, required=True)
    args = parser.parse_args()
    assemble_evaluator_catalog(
        source_root=args.source_root,
        config_path=args.config,
        destination_root=args.destination_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())