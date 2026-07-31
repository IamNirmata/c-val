"""U12A safe local scaffold, compatibility catalog, and fixed-name cleanup tests."""

from __future__ import annotations

import argparse
import errno
import json
import os
import re
import shlex
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from contextlib import closing
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import fields, replace
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import yaml

from cval.cli import build_parser, main
from cval.config import StorageConfig, TestsConfig, encode_config_snapshot, load_config
from cval.evaluator.catalog import assemble_evaluator_catalog
from cval.evaluator import catalog as catalog_module
from cval.jobs.renderer import render_validation_job
from cval.orchestrator.validate import (
    build_validation_report,
    parse_test_progress,
    raw_results_from_log,
    render_validation_report,
)
from cval.storage.run_history import ingest_run_history_file
from cval.validation.compatibility import (
    COMPATIBILITY_SURFACES,
    DEFAULT_TEST_REGISTRATIONS,
    INTERNAL_RUNTIME_PROTOCOL_NAMES,
    LEGACY_ENABLE_ENV,
    LEGACY_RESULT_ENV,
    LEGACY_RUNTIME_ENV_NAMES,
    audit_compatibility_inputs,
    compatibility_inventory,
)
from cval.validation.ingestion import ingest_test_results_file
from cval.validation.plugins import validate_registry_plugins
from cval.validation.registry import ValidationTestRegistry, load_test_registry
from cval.validation.results import (
    parse_validation_result,
    parse_validation_result_v2,
    validation_result_to_env,
    validation_result_v2_digest,
)
from cval.validation.runner import (
    _initial_result,
    _test_environment,
    _test_paths,
    run_validation_tests,
)
from cval.validation.runtime import _decode_runtime_environment
from cval.validation.scaffold import build_scaffold_plan, scaffold_validation_test
from cval.validation import scaffold as scaffold_module
from cval.validation.supervisor import supervise_validation_run


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (REPO_ROOT / "ymls/specific-node-job.yml").read_text(encoding="utf-8")

_STABLE_METADATA_FIELDS = (
    "st_dev",
    "st_ino",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
    "st_mode",
)


def _metadata_without_atime(value: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(value, field) for field in _STABLE_METADATA_FIELDS)


def _stat_until_stable(path: Path) -> os.stat_result:
    previous = path.stat()
    stable_samples = 0
    for _attempt in range(100):
        current = path.stat()
        if _metadata_without_atime(current) == _metadata_without_atime(previous):
            stable_samples += 1
            if stable_samples >= 3:
                return current
        else:
            stable_samples = 0
        previous = current
        time.sleep(0.002)
    raise AssertionError(f"metadata did not stabilize for {path}")


def _prepare_stable_metadata_fixture(path: Path) -> os.stat_result:
    timestamp = time.time_ns() - 24 * 60 * 60 * 1_000_000_000
    os.utime(path, ns=(timestamp, timestamp))
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    parent_fd = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
    _stat_until_stable(path.parent)
    return _stat_until_stable(path)


def _deterministic_noatime_supported(path: Path) -> bool:
    noatime = getattr(os, "O_NOATIME", 0)
    if not noatime:
        return False
    parent_fd = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY
                | noatime
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
        except OSError:
            return False
    finally:
        os.close(parent_fd)
    try:
        before = os.fstat(descriptor)
        for _read in range(3):
            os.lseek(descriptor, 0, os.SEEK_SET)
            while os.read(descriptor, 64 * 1024):
                pass
        after = os.fstat(descriptor)
        return (
            before.st_atime_ns == after.st_atime_ns
            and _metadata_without_atime(before) == _metadata_without_atime(after)
        )
    finally:
        os.close(descriptor)


class ScaffoldSafetyTests(unittest.TestCase):
    def _root(self, tmpdir: str) -> Path:
        root = Path(tmpdir)
        (root / "validation-tests").mkdir()
        return root

    def test_dry_run_and_confirmation_are_no_write_and_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            payload = scaffold_validation_test("smoke", 40, repo_root=root)
            self.assertFalse(payload["applied"])
            self.assertFalse((root / "validation-tests/smoke").exists())
            self.assertIn("enabled = false", payload["registry_stanza"])
            self.assertFalse(payload["plugin_created"])
            self.assertFalse(payload["health_created"])

            with self.assertRaisesRegex(ValueError, "exact --confirm scaffold"):
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="yes"
                )
            self.assertFalse((root / "validation-tests/smoke").exists())

    def test_rejects_unsafe_identity_order_path_and_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            for test_id in (
                "../smoke",
                "Smoke",
                "a" * 64,
                "all",
                "dltest-compute",
            ):
                with self.subTest(test_id=test_id), self.assertRaises(ValueError):
                    build_scaffold_plan(test_id, 40, repo_root=root)
            for order in (-1, True, 1_000_001):
                with self.subTest(order=order), self.assertRaises(ValueError):
                    build_scaffold_plan("smoke", order, repo_root=root)
            (root / "validation-tests/smoke").mkdir()
            marker = root / "validation-tests/smoke/keep"
            marker.write_text("historical", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                )
            self.assertEqual(marker.read_text(encoding="utf-8"), "historical")

    def test_apply_creates_only_pass_fail_template_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            payload = scaffold_validation_test(
                "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
            )
            target = root / "validation-tests/smoke"
            self.assertTrue(payload["applied"])
            self.assertEqual(
                sorted(path.relative_to(target).as_posix() for path in target.rglob("*") if path.is_file()),
                sorted(payload["files"]),
            )
            descriptor = (target / "test_config.toml").read_text(encoding="utf-8")
            self.assertNotIn("[plugin]", descriptor)
            self.assertNotIn("[health]", descriptor)
            self.assertTrue(os.access(target / "run-test.sh", os.X_OK))
            self.assertTrue(os.access(target / "tests/test.sh", os.X_OK))

    def test_default_registration_orders_are_included_in_collision_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            source = REPO_ROOT / "validation-tests/storage/test_config.toml"
            target = root / "validation-tests/storage/test_config.toml"
            target.parent.mkdir()
            shutil.copy2(source, target)
            order = tomllib.loads(source.read_text(encoding="utf-8"))["test"]["order"]
            with self.assertRaisesRegex(ValueError, "already declared by 'storage'"):
                build_scaffold_plan("smoke", order, repo_root=root)

    def test_umask_cannot_weaken_exact_scaffold_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            previous = os.umask(0o777)
            try:
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                )
            finally:
                os.umask(previous)
            target = root / "validation-tests/smoke"
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((target / "tests").stat().st_mode), 0o700)
            for path in target.rglob("*"):
                expected = 0o700 if path.is_dir() else 0o755 if path.suffix == ".sh" else 0o600
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected, path)

    def test_third_file_failure_rolls_back_complete_staging_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            original = scaffold_module._write_scaffold_file
            calls = 0

            def fail_third(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 3:
                    raise OSError("injected third-file failure")
                return original(*args, **kwargs)

            with patch.object(scaffold_module, "_write_scaffold_file", side_effect=fail_third):
                with self.assertRaisesRegex(OSError, "third-file"):
                    scaffold_validation_test(
                        "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                    )
            self.assertEqual(list((root / "validation-tests").iterdir()), [])

    def test_ancestor_swap_fails_closed_and_never_writes_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            replacement = root / "replacement"
            replacement.mkdir()
            original = scaffold_module._write_scaffold_file
            swapped = False

            def swap_ancestor(*args, **kwargs):
                nonlocal swapped
                if not swapped:
                    swapped = True
                    (root / "validation-tests").rename(root / "validation-tests-old")
                    (root / "validation-tests").symlink_to(replacement, target_is_directory=True)
                return original(*args, **kwargs)

            with patch.object(scaffold_module, "_write_scaffold_file", side_effect=swap_ancestor):
                with self.assertRaises(OSError):
                    scaffold_validation_test(
                        "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                    )
            self.assertEqual(list(replacement.iterdir()), [])
            self.assertEqual(list((root / "validation-tests-old").iterdir()), [])

    def test_publish_race_never_overwrites_and_removes_only_staging(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            original = scaffold_module.rename_noreplace_at

            def race(source_fd, source_name, destination_fd, destination_name):
                os.mkdir(destination_name, dir_fd=destination_fd)
                child_fd = os.open(
                    destination_name,
                    os.O_RDONLY | os.O_DIRECTORY,
                    dir_fd=destination_fd,
                )
                try:
                    descriptor = os.open(
                        "winner",
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                        dir_fd=child_fd,
                    )
                    os.close(descriptor)
                finally:
                    os.close(child_fd)
                return original(source_fd, source_name, destination_fd, destination_name)

            with patch.object(scaffold_module, "rename_noreplace_at", side_effect=race):
                with self.assertRaises(FileExistsError):
                    scaffold_validation_test(
                        "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                    )
            target = root / "validation-tests/smoke"
            self.assertEqual([path.name for path in target.iterdir()], ["winner"])
            self.assertFalse(any(path.name.startswith(".cval-scaffold-") for path in target.parent.iterdir()))

    def test_files_and_directories_are_fsynced_before_and_after_publish(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = self._root(tmpdir)
            original = os.fsync
            synced: list[int] = []

            def record(descriptor):
                synced.append(os.fstat(descriptor).st_mode)
                return original(descriptor)

            with patch("cval.validation.secure_fs.os.fsync", side_effect=record):
                scaffold_validation_test(
                    "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
                )
            self.assertGreaterEqual(sum(stat.S_ISREG(mode) for mode in synced), 6)
            self.assertGreaterEqual(sum(stat.S_ISDIR(mode) for mode in synced), 4)
            self.assertFalse(
                any(path.name.startswith(".cval-scaffold-") for path in (root / "validation-tests").iterdir())
            )


class ScaffoldEndToEndTests(unittest.TestCase):
    def test_fourth_test_disabled_load_render_runner_v2_history_and_u7(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "validation-tests").mkdir()
            scaffold_validation_test(
                "smoke", 40, repo_root=root, apply=True, confirmation="scaffold"
            )
            registration = {
                "smoke": {
                    "enabled": False,
                    "config_path": "validation-tests/smoke/test_config.toml",
                }
            }
            disabled = load_test_registry(
                registration,
                repo_root=root,
                include_defaults=False,
                require_enabled=False,
            )
            self.assertFalse(disabled.require("smoke").enabled)

            registration["smoke"]["enabled"] = True
            enabled = load_test_registry(
                registration,
                repo_root=root,
                include_defaults=False,
            )
            smoke = enabled.require("smoke")

            base = load_config()
            render_registry = ValidationTestRegistry(base.tests.registry.tests + (smoke,))
            render_config = replace(base, tests=TestsConfig(registry=render_registry))
            rendered = render_validation_job(
                TEMPLATE,
                "node-a",
                timestamp=123,
                cval_config=render_config,
            )
            manifest = yaml.safe_load(rendered.yaml_text)
            runtime_value = next(
                item["value"]
                for item in manifest["spec"]["tasks"][0]["template"]["spec"][
                    "containers"
                ][0]["env"]
                if item["name"] == "CVAL_RUNTIME_ENV_B64"
            )
            runtime_exports = _decode_runtime_environment(runtime_value)
            self.assertIn('"smoke"', runtime_exports)
            self.assertIn("smoke", render_config.tests.registry.to_dict())

            (root / "validation-tests/smoke/tests/test.sh").write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\n"
                "printf '%s\\n' '{\"status\":\"pass\"}' > \"$CVAL_TEST_SUMMARY_FILE\"\n",
                encoding="utf-8",
            )
            os.chmod(root / "validation-tests/smoke/tests/test.sh", 0o755)
            validation_root = root / "data"
            history_db = root / "history.db"
            run_config = replace(
                base,
                storage=replace(
                    base.storage,
                    run_history_enabled=True,
                    per_test_ingestion_enabled=True,
                    run_history_db_path=str(history_db),
                ),
                runtime=replace(base.runtime, validation_root=str(validation_root)),
                tests=TestsConfig(registry=enabled),
            )
            result_payload = run_validation_tests(
                config=run_config,
                registry=enabled,
                environ={
                    "CVAL_NODE": "node-a",
                    "CVAL_TIMESTAMP": "123",
                    "CVAL_RUN_ID": "node-a-123",
                    "CVAL_VALIDATION_ROOT": str(validation_root),
                    "CVAL_PYTORCH_VERSION": "2.8",
                    "CVAL_CUDA_VERSION": "12.9",
                },
            )
            result = parse_validation_result_v2(result_payload)
            self.assertEqual(result.overall, "pass")
            self.assertEqual(list(result.tests), ["smoke"])
            self.assertEqual(result.tests["smoke"].status, "pass")
            result_path = validation_root / "logs/job_logs/node-a/node-a-123/result.json"
            digest = validation_result_v2_digest(result)
            snapshot = encode_config_snapshot(run_config)

            ingest_run_history_file(
                result_path,
                db_path=history_db,
                config=run_config,
                result_digest=digest,
                config_snapshot_b64=snapshot,
            )
            report = ingest_test_results_file(
                result_path,
                config=run_config,
                result_digest=digest,
                config_snapshot_b64=snapshot,
            )
            self.assertTrue(report.ok)
            self.assertEqual(report.outcomes[0].test_id, "smoke")
            with closing(sqlite3.connect(history_db)) as connection:
                self.assertEqual(
                    connection.execute("SELECT test_id, status FROM run_tests").fetchone(),
                    ("smoke", "pass"),
                )
            with closing(
                sqlite3.connect(
                    validation_root / "validation_tests/smoke/smoke_results.db"
                )
            ) as connection:
                self.assertEqual(
                    connection.execute("SELECT test_id, status FROM test_results").fetchone(),
                    ("smoke", "pass"),
                )


class CompatibilityInventoryAuditTests(unittest.TestCase):
    def test_cli_scaffold_inventory_and_audit_are_structured_and_local(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(
                main(
                    [
                        "tests",
                        "scaffold",
                        "u12-cli-smoke",
                        "--order",
                        "900",
                        "--output",
                        "json",
                    ]
                ),
                0,
            )
        scaffold = json.loads(output.getvalue())
        self.assertEqual(scaffold["mode"], "dry-run")
        self.assertFalse(Path(scaffold["target_dir"]).exists())

        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(
                main(["compatibility", "inventory", "--output", "json"]),
                0,
            )
        self.assertEqual(
            json.loads(output.getvalue())["schema_version"],
            "cval.compatibility-catalog.v1",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            copied = Path(tmpdir) / "copied.txt"
            copied.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            output = StringIO()
            errors = StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                self.assertEqual(
                    main(
                        [
                            "compatibility",
                            "audit",
                            "--input",
                            str(copied),
                            "--output",
                            "json",
                        ]
                    ),
                    0,
                )
            audit = json.loads(output.getvalue())
            self.assertEqual(errors.getvalue(), "")
            self.assertTrue(audit["explicit_inputs_only"])

    def test_compatibility_cli_dispatches_before_config_or_plugin_loading(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            copied = Path(tmpdir) / "copied.log"
            copied.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            commands = (
                ["compatibility", "inventory", "--output", "json"],
                [
                    "--config",
                    "/definitely/not/read.toml",
                    "compatibility",
                    "audit",
                    "--input",
                    str(copied),
                    "--output",
                    "json",
                ],
            )
            for argv in commands:
                with (
                    self.subTest(argv=argv),
                    patch(
                        "cval.cli.load_config",
                        side_effect=AssertionError("configuration must not load"),
                    ),
                    patch(
                        "cval.validation.plugins.load_registered_plugin",
                        side_effect=AssertionError("plugin must not import"),
                    ),
                ):
                    output = StringIO()
                    with redirect_stdout(output):
                        self.assertEqual(main(argv), 0)
                    self.assertTrue(json.loads(output.getvalue()))

    def test_inventory_is_deterministic_immutable_and_blocked(self) -> None:
        first = compatibility_inventory()
        second = compatibility_inventory()
        self.assertEqual(first, second)
        self.assertFalse(first["removal_eligible"])
        self.assertEqual(len(first["surfaces"]), len(COMPATIBILITY_SURFACES))
        self.assertTrue(all(not row["removal_eligible"] for row in first["surfaces"]))
        self.assertTrue(
            all(
                row["token_classification"] == "compatibility-legacy"
                for row in first["surfaces"]
            )
        )
        internal = first["internal_runtime_protocol"]
        self.assertEqual(internal["token_classification"], "internal-current-protocol")
        self.assertFalse(internal["legacy_removal_candidate"])
        self.assertEqual(internal["tokens"], list(INTERNAL_RUNTIME_PROTOCOL_NAMES))
        with self.assertRaises(TypeError):
            LEGACY_RESULT_ENV["storage"] = "changed"  # type: ignore[index]
        with self.assertRaises(TypeError):
            DEFAULT_TEST_REGISTRATIONS["storage"] = {}  # type: ignore[index]

    def test_audit_is_bounded_deterministic_explicit_and_no_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "copied.log"
            path.write_text("RUN_STORAGE=true\nStorage test is complete.\n", encoding="utf-8")
            before = path.stat()
            first = audit_compatibility_inputs([path])
            second = audit_compatibility_inputs([path])
            after = path.stat()
            self.assertEqual(first, second)
            self.assertEqual(
                (before.st_mtime_ns, before.st_size, before.st_mode),
                (after.st_mtime_ns, after.st_size, after.st_mode),
            )
            self.assertFalse(first["removal_eligible"])
            self.assertTrue(first["offline"])
            self.assertTrue(first["explicit_inputs_only"])
            self.assertTrue(any(row["observed"] for row in first["surfaces"]))
            self.assertEqual(sorted(Path(tmpdir).iterdir()), [path])

            with self.assertRaisesRegex(ValueError, "at least one explicit"):
                audit_compatibility_inputs([])
            symlink = Path(tmpdir) / "link"
            symlink.symlink_to(path)
            with self.assertRaises(OSError):
                audit_compatibility_inputs([symlink])
            oversized = Path(tmpdir) / "large"
            with oversized.open("wb") as handle:
                handle.truncate(first["limits"]["max_file_bytes"] + 1)
            with self.assertRaisesRegex(ValueError, "exceeds"):
                audit_compatibility_inputs([oversized])

    def test_reader_rejects_symlink_ancestors_and_unsafe_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            real = root / "real"
            real.mkdir()
            path = real / "copied.log"
            path.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            alias = root / "alias"
            alias.symlink_to(real, target_is_directory=True)
            with self.assertRaises(OSError):
                audit_compatibility_inputs([alias / path.name])
            os.chmod(path, 0o622)
            with self.assertRaisesRegex(ValueError, "group/world writable"):
                audit_compatibility_inputs([path])

    def test_fifo_and_device_fail_quickly_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fifo = Path(tmpdir) / "input.log"
            os.mkfifo(fifo, 0o600)
            started = time.monotonic()
            with self.assertRaisesRegex(ValueError, "not a regular file"):
                audit_compatibility_inputs([fifo])
            self.assertLess(time.monotonic() - started, 1.0)
        started = time.monotonic()
        with self.assertRaises((OSError, ValueError)):
            audit_compatibility_inputs([Path("/dev/null")])
        self.assertLess(time.monotonic() - started, 1.0)

    def test_metadata_identity_change_during_read_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "copied.log"
            path.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            original = os.read
            changed = False

            def mutate(descriptor, count):
                nonlocal changed
                payload = original(descriptor, count)
                if not changed:
                    changed = True
                    with path.open("ab") as handle:
                        handle.write(b"changed\n")
                return payload

            with patch("cval.validation.secure_fs.os.read", side_effect=mutate):
                with self.assertRaisesRegex(RuntimeError, "changed while reading"):
                    audit_compatibility_inputs([path])

    def test_parent_rename_and_replacement_during_read_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parent = root / "inputs"
            parent.mkdir()
            path = parent / "copied.log"
            path.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            original = os.read
            replaced = False

            def replace_parent(descriptor, count):
                nonlocal replaced
                payload = original(descriptor, count)
                if not replaced:
                    replaced = True
                    parent.rename(root / "inputs-old")
                    parent.mkdir()
                    (parent / path.name).write_text("replacement\n", encoding="utf-8")
                return payload

            with patch("cval.validation.secure_fs.os.read", side_effect=replace_parent):
                with self.assertRaisesRegex(RuntimeError, "identity changed"):
                    audit_compatibility_inputs([path])

    def test_noatime_permission_failure_never_reopens_or_changes_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "copied.log"
            path.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            before = _prepare_stable_metadata_fixture(path)
            original = os.open
            target_flags: list[int] = []

            def deny_noatime(name, flags, mode=0o777, *, dir_fd=None):
                if name == path.name and dir_fd is not None:
                    target_flags.append(flags)
                    raise PermissionError(errno.EPERM, "mocked O_NOATIME denial")
                return original(name, flags, mode, dir_fd=dir_fd)

            with patch("cval.validation.secure_fs.os.open", side_effect=deny_noatime):
                with self.assertRaisesRegex(OSError, "O_NOATIME.*required"):
                    audit_compatibility_inputs([path])
            after = path.stat()
            self.assertEqual(len(target_flags), 1)
            self.assertTrue(target_flags[0] & getattr(os, "O_NOATIME", 0))
            self.assertEqual(
                _metadata_without_atime(before),
                _metadata_without_atime(after),
            )

    def test_audit_preserves_leaf_and_directory_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir) / "inputs"
            parent.mkdir()
            path = parent / "copied.log"
            path.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            before_leaf = _prepare_stable_metadata_fixture(path)
            before_parent = _stat_until_stable(parent)
            deterministic_noatime = _deterministic_noatime_supported(path)
            before_leaf = _stat_until_stable(path)

            for _read in range(3):
                audit_compatibility_inputs([path])

            after_parent = parent.stat()
            after_leaf = path.stat()
            self.assertEqual(
                _metadata_without_atime(after_parent),
                _metadata_without_atime(before_parent),
            )
            self.assertEqual(
                _metadata_without_atime(after_leaf),
                _metadata_without_atime(before_leaf),
            )
            if deterministic_noatime:
                self.assertEqual(after_leaf.st_atime_ns, before_leaf.st_atime_ns)

    def test_directory_traversal_uses_nonblocking_no_follow_descriptors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "copied.log"
            path.write_text("GCRRESULT1=pass\n", encoding="utf-8")
            original = os.open
            directory_flags: list[int] = []

            def record(name, flags, mode=0o777, *, dir_fd=None):
                if flags & getattr(os, "O_DIRECTORY", 0):
                    directory_flags.append(flags)
                return original(name, flags, mode, dir_fd=dir_fd)

            with patch("cval.validation.secure_fs.os.open", side_effect=record):
                audit_compatibility_inputs([path])
            self.assertTrue(directory_flags)
            for flags in directory_flags:
                self.assertTrue(flags & getattr(os, "O_DIRECTORY", 0))
                self.assertTrue(flags & getattr(os, "O_NOFOLLOW", 0))
                self.assertTrue(flags & getattr(os, "O_CLOEXEC", 0))
                self.assertTrue(flags & getattr(os, "O_NONBLOCK", 0))

    def test_skipped_markers_and_artifact_aliases_detect_exact_surfaces(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            skipped = root / "skipped.log"
            skipped.write_text(
                "Storage test SKIPPED (disabled by config).\n",
                encoding="utf-8",
            )
            alias = root / "alias.env"
            alias.write_text(
                "STORAGE_OUTPUT_DIR=/copy/artifacts\n",
                encoding="utf-8",
            )
            report = audit_compatibility_inputs([skipped, alias])
            surfaces = {row["surface_id"]: row for row in report["surfaces"]}
            self.assertEqual(
                surfaces["legacy-log-markers"]["observations"],
                [
                    {
                        "path": str(skipped),
                        "tokens": ["Storage test SKIPPED (disabled by config)."],
                    }
                ],
            )
            self.assertEqual(
                surfaces["legacy-runtime-environment"]["observations"],
                [{"path": str(alias), "tokens": ["STORAGE_OUTPUT_DIR"]}],
            )
            self.assertEqual(
                parse_test_progress(skipped.read_text(encoding="utf-8"))["storage"],
                "incomplete",
            )
            self.assertEqual(
                raw_results_from_log(
                    skipped.read_text(encoding="utf-8"), enabled_tests=set()
                ),
                {"storage": "incomplete", "all": "incomplete"},
            )

    def test_runtime_token_allowlist_exactly_matches_independent_fixtures(self) -> None:
        config = load_config()
        rendered = render_validation_job(
            TEMPLATE,
            "node-a",
            timestamp=123,
            cval_config=config,
        )
        manifest = yaml.safe_load(rendered.yaml_text)
        template_environment = {
            item["name"]
            for item in manifest["spec"]["tasks"][0]["template"]["spec"][
                "containers"
            ][0]["env"]
        }
        runtime_payload = next(
            item["value"]
            for item in manifest["spec"]["tasks"][0]["template"]["spec"][
                "containers"
            ][0]["env"]
            if item["name"] == "CVAL_RUNTIME_ENV_B64"
        )
        payload_environment = set(
            re.findall(
                r"^export ([A-Z_][A-Z0-9_]*)=",
                _decode_runtime_environment(runtime_payload),
                flags=re.MULTILINE,
            )
        )

        env_source = (REPO_ROOT / "validation-tests/0-env.sh").read_text(
            encoding="utf-8"
        )
        shell_environment = set(
            re.findall(
                r"^(?:export )?([A-Z_][A-Z0-9_]*)=",
                env_source,
                flags=re.MULTILINE,
            )
        )
        for names in re.findall(r"^export (.+)$", env_source, flags=re.MULTILINE):
            shell_environment.update(
                name for name in names.split() if re.fullmatch(r"[A-Z_][A-Z0-9_]*", name)
            )

        v1_fixture = parse_validation_result(
            {
                "schema_version": "cval.results.v1",
                "node": "node-a",
                "timestamp": "123",
                "overall": "pass",
                "tests": {
                    test_id: {"status": "pass", "enabled": True}
                    for test_id in ("storage", "nccl", "dltest")
                },
            }
        )
        runtime_environment: dict[str, str] = {}
        for line in _decode_runtime_environment(runtime_payload).splitlines():
            assignment = shlex.split(line)[1]
            name, _separator, value = assignment.partition("=")
            runtime_environment[name] = value
        v2_fixture = parse_validation_result_v2(
            _initial_result(
                config=config,
                registry=config.tests.registry,
                runtime_env=runtime_environment,
                node="node-a",
                timestamp=123,
                run_id="node-a-123",
                validation_root=Path(config.runtime.validation_root),
            )
        )
        result_environment = set(validation_result_to_env(v1_fixture))
        self.assertEqual(
            set(validation_result_to_env(v2_fixture)), result_environment
        )

        registered = config.tests.registry.require("storage")
        paths = _test_paths(
            Path(config.runtime.validation_root),
            "node-a",
            "node-a-123",
            registered,
        )
        runner_environment = set(
            _test_environment(
                runtime_environment,
                registered_test=registered,
                test_paths=paths,
                validation_root=Path(config.runtime.validation_root),
                node="node-a",
                timestamp=123,
                run_id="node-a-123",
            )
        )
        fallback_source = (REPO_ROOT / "validation-tests/db-update.sh").read_text(
            encoding="utf-8"
        )
        fallback_environment = set(
            re.findall(r"\$\{(CVAL_ALLOW_[A-Z0-9_]+)", fallback_source)
        )

        observed = (
            template_environment
            | payload_environment
            | shell_environment
            | result_environment
            | runner_environment
            | fallback_environment
        )
        audited_exceptions = {
            "dynamic-test-identity": {
                "CVAL_NODE",
                "CVAL_TIMESTAMP",
                "CVAL_TEST_ID",
                "CVAL_TEST_DIR",
                "CVAL_TEST_CONFIG",
            },
        }
        excluded = set().union(*audited_exceptions.values())
        self.assertEqual(observed & excluded, excluded)
        self.assertEqual(set(LEGACY_RUNTIME_ENV_NAMES), observed - excluded)
        self.assertEqual(len(LEGACY_RUNTIME_ENV_NAMES), len(set(LEGACY_RUNTIME_ENV_NAMES)))
        self.assertTrue(
            set(LEGACY_RUNTIME_ENV_NAMES).isdisjoint(INTERNAL_RUNTIME_PROTOCOL_NAMES)
        )
        self.assertFalse(any(name.endswith(("CVAL_", "RUN_")) for name in LEGACY_RUNTIME_ENV_NAMES))

    def test_internal_protocol_exactly_matches_supervisor_template_and_ingestion_fixtures(self) -> None:
        config = load_config()
        registry_json = json.dumps(
            config.tests.registry.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
        )
        captured: dict[str, str] = {}

        def capture_child(_command, **kwargs):
            captured.update(kwargs["environment"])
            return 0

        with tempfile.TemporaryDirectory() as tmpdir:
            validation_root = Path(tmpdir) / "data"
            validation_root.mkdir()
            environment = {
                "CVAL_VALIDATION_ROOT": str(validation_root),
                "CVAL_TEST_REGISTRY_JSON": registry_json,
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_REPO_DIR": str(REPO_ROOT),
                "GCRNODE": "node-a",
                "GCRTIME": "123",
            }
            with patch(
                "cval.validation.supervisor._run_child",
                side_effect=capture_child,
            ):
                self.assertEqual(
                    supervise_validation_run(
                        environment=environment,
                        runner_command=("supervisor-fixture",),
                        db_update_command=None,
                        validation_tests_dir=REPO_ROOT / "validation-tests",
                    ),
                    0,
                )

        supervisor_controls = {
            "CVAL_EXTERNAL_GLOBAL_LOGGING",
            "CVAL_RUN_MARKER_PREACQUIRED",
        }
        supervisor_protocol = {
            name
            for name in captured
            if name.startswith(("CVAL_SECURE_", "CVAL_CANONICAL_"))
            or name in supervisor_controls
        }

        manifest = yaml.safe_load(
            render_validation_job(
                TEMPLATE,
                "node-a",
                timestamp=123,
                cval_config=config,
            ).yaml_text
        )
        template_names = {
            item["name"]
            for item in manifest["spec"]["tasks"][0]["template"]["spec"][
                "containers"
            ][0]["env"]
        }
        self.assertFalse(
            template_names & supervisor_protocol,
            "the static manifest must not predeclare descriptor-derived protocol values",
        )

        ingestion_source = (REPO_ROOT / "validation-tests/db-update.sh").read_text(
            encoding="utf-8"
        )
        ingestion_guards = set(
            re.findall(r"CVAL_CANONICAL_[A-Z0-9_]+", ingestion_source)
        )
        expected = supervisor_protocol | ingestion_guards
        self.assertEqual(set(INTERNAL_RUNTIME_PROTOCOL_NAMES), expected)
        self.assertEqual(
            len(INTERNAL_RUNTIME_PROTOCOL_NAMES),
            len(set(INTERNAL_RUNTIME_PROTOCOL_NAMES)),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            copied = Path(tmpdir) / "protocol.env"
            copied.write_text(
                "CVAL_SECURE_RUN_LAYOUT_JSON={}\nGCRRESULT1=pass\n",
                encoding="utf-8",
            )
            report = audit_compatibility_inputs([copied])
            internal = report["internal_runtime_protocol"]
            self.assertEqual(
                internal["token_classification"], "internal-current-protocol"
            )
            self.assertFalse(internal["legacy_removal_candidate"])
            self.assertEqual(
                internal["observations"],
                [
                    {
                        "path": str(copied),
                        "tokens": ["CVAL_SECURE_RUN_LAYOUT_JSON"],
                    }
                ],
            )
            historical = next(
                row
                for row in report["surfaces"]
                if row["surface_id"] == "historical-v1-result-reader"
            )
            self.assertEqual(
                historical["token_classification"], "compatibility-legacy"
            )
            self.assertTrue(historical["observed"])

    def test_historical_nccl_table_tokens_exactly_match_schema_fixture(self) -> None:
        schema = (REPO_ROOT / "docs/result-schema.md").read_text(encoding="utf-8")
        expected = set(re.findall(r"`(OLD_nccl_[A-Za-z0-9_]+)`", schema))
        historical = next(
            surface
            for surface in COMPATIBILITY_SURFACES
            if surface.surface_id == "historical-dl-artifact-reader"
        )
        actual = {token for token in historical.tokens if token.startswith("OLD_nccl_")}
        self.assertEqual(
            expected,
            {"OLD_nccl_performance", "OLD_nccl_ib_port_performance"},
        )
        self.assertEqual(actual, expected)

    def test_compatibility_cli_and_dl_db_tokens_exactly_match_live_definitions(self) -> None:
        surface = next(
            item
            for item in COMPATIBILITY_SURFACES
            if item.surface_id == "compatibility-cli-and-writers"
        )

        config = load_config()
        parser = build_parser(config)
        subparsers = next(
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        documented_commands = {
            action.dest for action in subparsers._choices_actions
        }
        hidden_commands = set(subparsers.choices) - documented_commands
        expected_compatibility_commands = {
            command
            for command in hidden_commands
            if command.startswith("db-add-")
        }
        self.assertIn("db-rebuild-dltest-metrics", hidden_commands)
        expected_compatibility_commands.add("db-rebuild-dltest-metrics")
        self.assertEqual(
            {token for token in surface.tokens if token.startswith("db-")},
            expected_compatibility_commands,
        )

        configured_dl_db_names = {
            Path(getattr(config.storage, field.name)).name
            for field in fields(StorageConfig)
            if field.name.startswith("dl_") and field.name.endswith("_db_path")
        }
        self.assertEqual(
            configured_dl_db_names,
            {
                "dltest_numerical_correctness.db",
                "dltest_compute_performance.db",
                "dltest_collective_performance.db",
                "dltest_overlap_performance.db",
            },
        )
        self.assertEqual(
            {
                token
                for token in surface.tokens
                if token.startswith("dltest_") and token.endswith(".db")
            },
            configured_dl_db_names,
        )

    def test_token_boundaries_and_unscannable_inputs_are_conservative(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            near_miss = root / "near.log"
            near_miss.write_text("GCRRESULT10=pass\n", encoding="utf-8")
            malformed = root / "bad.json"
            malformed.write_text('{"GCRRESULT1":', encoding="utf-8")
            binary = root / "binary.log"
            binary.write_bytes(b"GCRRESULT1\x00pass")
            unsupported = root / "copy.sqlite"
            unsupported.write_text("GCRRESULT1", encoding="utf-8")
            exact = root / "exact.json"
            exact.write_text('{"GCRRESULT1":"pass"}', encoding="utf-8")

            report = audit_compatibility_inputs(
                [near_miss, malformed, binary, unsupported, exact]
            )
            rows = {Path(row["path"]).name: row for row in report["inputs"]}
            self.assertEqual(rows["near.log"]["scan_status"], "scanned")
            for name in ("bad.json", "binary.log", "copy.sqlite"):
                self.assertEqual(rows[name]["scan_status"], "unscannable")
                self.assertEqual(rows[name]["classification"], "unknown")
            historical = next(
                row for row in report["surfaces"]
                if row["surface_id"] == "historical-v1-result-reader"
            )
            self.assertEqual(
                [Path(item["path"]).name for item in historical["observations"]],
                ["exact.json"],
            )
            self.assertFalse(report["removal_eligible"])

    def test_path_separators_are_boundaries_without_identifier_near_misses(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            positive = root / "positive.log"
            positive.write_text(
                "\n".join(
                    (
                        "/copy/validation-tests/storage/storage.sh",
                        "validation-tests/nccl/run-nccl-allreduce.sh",
                        "./validation-tests/dltest/dltest.sh",
                        "/copy/metadata/validation.db",
                        "metadata/test-storage.db",
                        "./metadata/test-nccl.db",
                        "python -m cval.cli db-rebuild-dltest-metrics --output json",
                        "/copy/metadata/dltest_numerical_correctness.db",
                        "metadata/dltest_compute_performance.db",
                        "./metadata/dltest_collective_performance.db",
                        "/copy/metadata/dltest_overlap_performance.db",
                        "/copy/dltest-summary-node-a-123.json",
                        "dltest/node-a/dltest-123/workdir/result.json",
                        "/copy/dltest/node-b/dltest-456/workdir/result.json",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            negative = root / "negative.log"
            negative.write_text(
                "\n".join(
                    (
                        "/copy/mystorage.sh",
                        "/copy/run-nccl-allreduce.shx",
                        "/copy/dltest.sh_backup",
                        "/copy/myvalidation.db",
                        "/copy/test-storage.db-wal",
                        "/copy/xtest-nccl.db",
                        "run-db-rebuild-dltest-metrics",
                        "db-rebuild-dltest-metrics-old",
                        "/copy/metadata/mydltest_numerical_correctness.db",
                        "/copy/metadata/dltest_compute_performance.db-wal",
                        "/copy/metadata/xdltest_collective_performance.db",
                        "/copy/metadata/dltest_overlap_performance.db_backup",
                        "/copy/mydltest-summary-node-a.json",
                        "/copy/mydltest/node-a/result.json",
                        "/copy/dltestish/node-a/result.json",
                    )
                )
                + "\n",
                encoding="utf-8",
            )

            report = audit_compatibility_inputs([positive, negative])
            surfaces = {row["surface_id"]: row for row in report["surfaces"]}
            self.assertEqual(
                surfaces["legacy-wrapper-entrypoints"]["observations"],
                [
                    {
                        "path": str(positive),
                        "tokens": [
                            "storage.sh",
                            "run-nccl-allreduce.sh",
                            "dltest.sh",
                        ],
                    }
                ],
            )
            self.assertEqual(
                surfaces["compatibility-cli-and-writers"]["observations"],
                [
                    {
                        "path": str(positive),
                        "tokens": [
                            "db-rebuild-dltest-metrics",
                            "validation.db",
                            "test-storage.db",
                            "test-nccl.db",
                            "dltest_numerical_correctness.db",
                            "dltest_compute_performance.db",
                            "dltest_collective_performance.db",
                            "dltest_overlap_performance.db",
                        ],
                    }
                ],
            )
            self.assertEqual(
                surfaces["historical-dl-artifact-reader"]["observations"],
                [
                    {
                        "path": str(positive),
                        "tokens": ["dltest-summary-", "dltest/"],
                    }
                ],
            )

    def test_audit_file_count_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = []
            for index in range(65):
                path = Path(tmpdir) / f"{index}.log"
                path.write_text("safe\n", encoding="utf-8")
                paths.append(path)
            with self.assertRaisesRegex(ValueError, "at most 64"):
                audit_compatibility_inputs(paths)

    def test_fixed_name_definitions_exist_only_in_central_catalog(self) -> None:
        forbidden_definitions = (
            "class StorageTestConfig",
            "class NcclTestConfig",
            "class DlTestConfig",
            "_TEST_DONE_MARKERS =",
            "_COMPATIBILITY_ALIAS_ROWS =",
        )
        for path in (REPO_ROOT / "cval").rglob("*.py"):
            if path.name == "compatibility.py":
                continue
            text = path.read_text(encoding="utf-8")
            for token in forbidden_definitions:
                with self.subTest(path=path, token=token):
                    self.assertNotIn(token, text)

        routed_consumers = {
            "cval/validation/results.py": ('("storage", "nccl", "dltest")',),
            "cval/storage/ingest.py": ('("storage", "nccl", "dltest", "all")',),
            "cval/orchestrator/validate.py": (
                'enabled_tests & {"storage", "nccl", "dltest"}',
                'r"\\s*storage=(\\w+)\\s+nccl=(\\w+)\\s+dltest=(\\w+)"',
            ),
            "cval/validation/supervisor.py": ('{"storage", "nccl", "dltest"}',),
        }
        for relative, tokens in routed_consumers.items():
            text = (REPO_ROOT / relative).read_text(encoding="utf-8")
            for token in tokens:
                with self.subTest(relative=relative, token=token):
                    self.assertNotIn(token, text)

    def test_list_and_describe_remain_descriptor_only(self) -> None:
        for argv in (
            ["tests", "list", "--output", "json"],
            ["tests", "describe", "storage", "--output", "json"],
        ):
            with self.subTest(argv=argv), patch(
                "cval.validation.plugins.load_registered_plugin",
                side_effect=AssertionError("descriptor inspection imported a plugin"),
            ):
                output = StringIO()
                with redirect_stdout(output):
                    self.assertEqual(main(argv), 0)
                self.assertTrue(json.loads(output.getvalue()))

    def test_historical_v1_and_builtin_projection_remain_exact(self) -> None:
        payload = {
            "schema_version": "cval.results.v1",
            "node": "node-a",
            "timestamp": "123",
            "overall": "pass",
            "tests": {
                test_id: {"status": "pass", "enabled": True}
                for test_id in ("storage", "nccl", "dltest")
            },
        }
        parsed = parse_validation_result(payload)
        self.assertEqual(parsed.schema_version, "cval.results.v1")
        self.assertEqual(
            dict(LEGACY_RESULT_ENV),
            {"storage": "GCRRESULT1", "nccl": "GCRRESULT2", "dltest": "GCRRESULT3"},
        )
        self.assertEqual(
            dict(LEGACY_ENABLE_ENV),
            {"storage": "RUN_STORAGE", "nccl": "RUN_NCCL", "dltest": "RUN_DLTEST"},
        )


class TargetedReportAndEvaluatorCatalogTests(unittest.TestCase):
    def test_fourth_pass_fail_only_test_is_reported_without_classification(self) -> None:
        report = build_validation_report(
            node="node-a",
            timestamp=123,
            job_name="cval-node-a-123",
            job_phase="Completed",
            schedulability={},
            raw_results={
                "storage": "pass",
                "nccl": "pass",
                "dltest": "pass",
                "smoke": "pass",
                "all": "pass",
            },
            verdicts={
                "storage": {"status": "normal"},
                "nccl": None,
                "dltest": None,
                "smoke": None,
            },
            test_ids=["storage", "nccl", "dltest", "smoke"],
        )
        self.assertEqual(report["test_order"], ["storage", "nccl", "dltest", "smoke"])
        self.assertEqual(report["classification"]["smoke"]["status"], "unknown")
        self.assertIn("smoke", render_validation_report(report))

    def test_evaluator_catalog_assembly_follows_synthetic_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            destination = root / "destination"
            test_dir = source / "validation-tests/synthetic"
            test_dir.mkdir(parents=True)
            destination.mkdir()
            (source / "config").mkdir()
            execution_marker = root / "workload-executed"
            for name in ("setup.sh", "run-test.sh"):
                (test_dir / name).write_text(
                    f"#!/bin/bash\nprintf ran > {execution_marker}\n",
                    encoding="utf-8",
                )
            (test_dir / "support.py").write_text("VALUE = 1\n", encoding="utf-8")
            (test_dir / "plugin.py").write_text(
                '''from support import VALUE
CVAL_PLUGIN_API = "cval.plugin.v1"
class Plugin:
    plugin_id = "synthetic"
    capabilities = frozenset()
PLUGIN = Plugin()
assert VALUE == 1
''',
                encoding="utf-8",
            )
            (test_dir / "ignored.txt").write_text("not packaged", encoding="utf-8")
            (test_dir / "test_config.toml").write_text(
                '''schema_version = "cval.test.v1"
[test]
id = "synthetic"
display_name = "Synthetic"
order = 40
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/synthetic/synthetic_results.db"
[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
capabilities = []
support_files = ["support.py"]
''',
                encoding="utf-8",
            )
            config = source / "config/cval.toml"
            config.write_text(
                '''[tests.synthetic]
enabled = false
config_path = "validation-tests/synthetic/test_config.toml"
''',
                encoding="utf-8",
            )
            copied = assemble_evaluator_catalog(
                source_root=source,
                config_path=config,
                destination_root=destination,
            )
            self.assertEqual(
                copied,
                (
                    "validation-tests/synthetic/test_config.toml",
                    "validation-tests/synthetic/plugin.py",
                    "validation-tests/synthetic/support.py",
                    "validation-tests/synthetic/setup.sh",
                    "validation-tests/synthetic/run-test.sh",
                ),
            )
            self.assertEqual(len(copied), len(set(copied)))
            self.assertFalse((destination / "validation-tests/synthetic/ignored.txt").exists())
            self.assertFalse(execution_marker.exists())
            for relative in copied:
                path = destination / relative
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                self.assertFalse(os.access(path, os.X_OK))
            installed = load_test_registry(
                {
                    "synthetic": {
                        "enabled": False,
                        "config_path": "validation-tests/synthetic/test_config.toml",
                    }
                },
                repo_root=destination,
                include_defaults=False,
                require_enabled=False,
            )
            self.assertEqual(validate_registry_plugins(installed.tests), ("synthetic",))
            with self.assertRaises(FileExistsError):
                assemble_evaluator_catalog(
                    source_root=source,
                    config_path=config,
                    destination_root=destination,
                )

    def test_docker_context_includes_future_catalog_and_builder_filters_assets(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            filtered = root / "filtered-context"
            catalog = root / "catalog"
            destination = root / "installed"
            test_dir = source / "validation-tests/future"
            test_dir.mkdir(parents=True)
            (source / "validation-tests/unregistered").mkdir()
            (source / "config").mkdir()
            (source / "ymls").mkdir()
            shutil.copy2(REPO_ROOT / ".dockerignore", source / ".dockerignore")
            (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (test_dir / "run-test.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (test_dir / "support.py").write_text("VALUE = 1\n", encoding="utf-8")
            (test_dir / "plugin.py").write_text(
                '''from support import VALUE
CVAL_PLUGIN_API = "cval.plugin.v1"
class Plugin:
    plugin_id = "future"
    capabilities = frozenset()
PLUGIN = Plugin()
assert VALUE == 1
''',
                encoding="utf-8",
            )
            (test_dir / "workload.bin").write_bytes(b"not evaluator catalog data")
            (source / "validation-tests/unregistered/asset.bin").write_bytes(
                b"unregistered workload"
            )
            (source / "ymls/unrelated-workload.yml").write_text(
                "kind: Job\n", encoding="utf-8"
            )
            (test_dir / "test_config.toml").write_text(
                '''schema_version = "cval.test.v1"
[test]
id = "future"
display_name = "Future"
order = 40
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/future/future_results.db"
[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
capabilities = []
support_files = ["support.py"]
''',
                encoding="utf-8",
            )
            (source / "config/cval.toml").write_text(
                '''[tests.future]
enabled = false
config_path = "validation-tests/future/test_config.toml"
''',
                encoding="utf-8",
            )

            rules = [
                line.strip()
                for line in (source / ".dockerignore").read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]

            def included(relative: str) -> bool:
                keep = True
                for raw in rules:
                    negate = raw.startswith("!")
                    pattern = raw[1:] if negate else raw
                    if pattern == "**":
                        matched = True
                    elif pattern.endswith("/**"):
                        prefix = pattern[:-3].rstrip("/")
                        matched = relative.startswith(prefix + "/")
                    elif pattern.endswith("/"):
                        prefix = pattern.rstrip("/")
                        matched = relative == prefix or relative.startswith(prefix + "/")
                    else:
                        matched = relative == pattern
                    if matched:
                        keep = negate
                return keep

            for path in source.rglob("*"):
                if not path.is_file():
                    continue
                relative = path.relative_to(source).as_posix()
                if included(relative):
                    target = filtered / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, target)

            self.assertTrue((filtered / "validation-tests/future/support.py").is_file())
            self.assertTrue((filtered / "validation-tests/future/workload.bin").is_file())
            self.assertTrue(
                (filtered / "validation-tests/unregistered/asset.bin").is_file()
            )
            self.assertFalse((filtered / "ymls/unrelated-workload.yml").exists())

            (catalog / "config").mkdir(parents=True)
            shutil.copy2(filtered / "config/cval.toml", catalog / "config/cval.toml")
            shutil.copytree(
                filtered / "validation-tests", catalog / "validation-tests"
            )
            destination.mkdir()
            dockerfile = (REPO_ROOT / "deploy/cval-evaluator/Dockerfile").read_text(
                encoding="utf-8"
            )
            exact_catalog_command = """RUN PYTHONPATH=/workspace/c-val python -m cval.evaluator.catalog \\
    --source-root /catalog \\
    --config /catalog/config/cval.toml \\
    --destination-root /workspace/c-val"""
            self.assertEqual(dockerfile.count(exact_catalog_command), 1)
            command = [
                sys.executable,
                "-m",
                "cval.evaluator.catalog",
                "--source-root",
                str(catalog),
                "--config",
                str(catalog / "config/cval.toml"),
                "--destination-root",
                str(destination),
            ]
            completed = subprocess.run(
                command,
                cwd=root,
                env=os.environ | {"PYTHONPATH": str(REPO_ROOT)},
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(
                (destination / "validation-tests/future/support.py").is_file()
            )
            self.assertFalse(
                (destination / "validation-tests/future/workload.bin").exists()
            )
            self.assertFalse(
                (destination / "validation-tests/unregistered").exists()
            )
            installed = load_test_registry(
                {
                    "future": {
                        "enabled": False,
                        "config_path": "validation-tests/future/test_config.toml",
                    }
                },
                repo_root=destination,
                include_defaults=False,
                require_enabled=False,
            )
            self.assertEqual(validate_registry_plugins(installed.tests), ("future",))

    def test_builder_catalog_command_runs_from_staged_target_outside_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            staged = root / "installed"
            outside = root / "outside"
            source = root / "catalog"
            package_source = root / "package-source"
            outside.mkdir()
            staged.mkdir()
            package_source.mkdir()
            shutil.copytree(REPO_ROOT / "cval", package_source / "cval")
            shutil.copy2(REPO_ROOT / "pyproject.toml", package_source / "pyproject.toml")
            shutil.copy2(REPO_ROOT / "README.md", package_source / "README.md")
            self._write_catalog_source(source, malformed_plugin=False)
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--no-deps",
                    "--no-build-isolation",
                    "--target",
                    str(staged),
                    str(package_source),
                ],
                cwd=outside,
                check=True,
                capture_output=True,
                text=True,
            )
            command = [
                sys.executable,
                "-m",
                "cval.evaluator.catalog",
                "--source-root",
                str(source),
                "--config",
                str(source / "config/cval.toml"),
                "--destination-root",
                str(staged),
            ]
            completed = subprocess.run(
                command,
                cwd=outside,
                env=os.environ | {"PYTHONPATH": str(staged)},
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertTrue(
                (staged / "validation-tests/synthetic/test_config.toml").is_file()
            )
            self.assertTrue((staged / "validation-tests/synthetic/plugin.py").is_file())
            self.assertTrue((staged / "validation-tests/synthetic/setup.sh").is_file())
            self.assertTrue((staged / "validation-tests/synthetic/run-test.sh").is_file())

            builtin_destination = root / "builtin-installed"
            builtin_destination.mkdir()
            shutil.copytree(staged / "cval", builtin_destination / "cval")
            builtin = subprocess.run(
                [
                    *command[:3],
                    "--source-root",
                    str(REPO_ROOT),
                    "--config",
                    str(REPO_ROOT / "config/cval.toml"),
                    "--destination-root",
                    str(builtin_destination),
                ],
                cwd=outside,
                env=os.environ | {"PYTHONPATH": str(builtin_destination)},
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(builtin.returncode, 0, builtin.stderr)
            for test_id in ("storage", "nccl", "dltest"):
                self.assertTrue(
                    (
                        builtin_destination
                        / f"validation-tests/{test_id}/test_config.toml"
                    ).is_file()
                )
                self.assertTrue(
                    (builtin_destination / f"validation-tests/{test_id}/plugin.py").is_file()
                )
                self.assertTrue(
                    (builtin_destination / f"validation-tests/{test_id}/setup.sh").is_file()
                )
                self.assertTrue(
                    (builtin_destination / f"validation-tests/{test_id}/run-test.sh").is_file()
                )
            builtin_tests = tomllib.loads(
                (REPO_ROOT / "config/cval.toml").read_text(encoding="utf-8")
            )["tests"]
            installed_builtin = load_test_registry(
                builtin_tests,
                repo_root=builtin_destination,
                include_defaults=False,
            )
            self.assertEqual(
                validate_registry_plugins(installed_builtin.tests),
                ("storage", "nccl", "dltest"),
            )

            malformed_source = root / "malformed-catalog"
            malformed_destination = root / "malformed-installed"
            malformed_destination.mkdir()
            shutil.copytree(staged / "cval", malformed_destination / "cval")
            self._write_catalog_source(malformed_source, malformed_plugin=True)
            malformed = subprocess.run(
                [
                    *command[:3],
                    "--source-root",
                    str(malformed_source),
                    "--config",
                    str(malformed_source / "config/cval.toml"),
                    "--destination-root",
                    str(malformed_destination),
                ],
                cwd=outside,
                env=os.environ | {"PYTHONPATH": str(malformed_destination)},
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(malformed.returncode, 0)
            self.assertIn("must export CVAL_PLUGIN_API", malformed.stderr)
            self.assertFalse((malformed_destination / "validation-tests").exists())

    def test_catalog_rejects_symlinked_source_and_destination_ancestors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            destination = root / "destination"
            destination.mkdir()
            self._write_catalog_source(source, malformed_plugin=False)
            source_alias = root / "source-alias"
            source_alias.symlink_to(source, target_is_directory=True)
            with self.assertRaises(OSError):
                assemble_evaluator_catalog(
                    source_root=source_alias,
                    config_path=source_alias / "config/cval.toml",
                    destination_root=destination,
                )
            destination_alias = root / "destination-alias"
            destination_alias.symlink_to(destination, target_is_directory=True)
            with self.assertRaises(OSError):
                assemble_evaluator_catalog(
                    source_root=source,
                    config_path=source / "config/cval.toml",
                    destination_root=destination_alias,
                )
            self.assertEqual(list(destination.iterdir()), [])

    def test_catalog_validates_plugin_config_before_destination_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            destination = root / "destination"
            destination.mkdir()
            self._write_catalog_source(source, malformed_plugin=False)
            test_dir = source / "validation-tests/synthetic"
            (test_dir / "plugin.py").write_text(
                '''from cval.validation.plugins import ConfigIssue
CVAL_PLUGIN_API = "cval.plugin.v1"
class Plugin:
    plugin_id = "synthetic"
    capabilities = frozenset({"config"})
    def validate_config(self, definition):
        return (ConfigIssue("invalid_iterations", "iterations must be positive"),)
PLUGIN = Plugin()
''',
                encoding="utf-8",
            )
            descriptor = test_dir / "test_config.toml"
            descriptor.write_text(
                descriptor.read_text(encoding="utf-8").replace(
                    "capabilities = []", 'capabilities = ["config"]'
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "invalid_iterations"):
                assemble_evaluator_catalog(
                    source_root=source,
                    config_path=source / "config/cval.toml",
                    destination_root=destination,
                )
            self.assertEqual(list(destination.iterdir()), [])

    def test_catalog_source_ancestor_swap_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            destination = root / "destination"
            destination.mkdir()
            self._write_catalog_source(source, malformed_plugin=False)
            original = catalog_module._read_source_file
            calls = 0

            def swap_after_descriptor(*args, **kwargs):
                nonlocal calls
                payload = original(*args, **kwargs)
                calls += 1
                if calls == 2:
                    test_dir = source / "validation-tests/synthetic"
                    test_dir.rename(source / "validation-tests/synthetic-old")
                    test_dir.mkdir()
                    (test_dir / "plugin.py").write_text("replacement\n", encoding="utf-8")
                return payload

            with patch.object(catalog_module, "_read_source_file", side_effect=swap_after_descriptor):
                with self.assertRaisesRegex(RuntimeError, "source directory changed"):
                    assemble_evaluator_catalog(
                        source_root=source,
                        config_path=source / "config/cval.toml",
                        destination_root=destination,
                    )
            self.assertEqual(list(destination.iterdir()), [])

    def test_catalog_destination_swap_and_third_file_failure_roll_back(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            destination = root / "destination"
            replacement = root / "replacement"
            destination.mkdir()
            replacement.mkdir()
            self._write_catalog_source(source, malformed_plugin=False)
            original = catalog_module.write_file_at
            swapped = False

            def swap_destination(*args, **kwargs):
                nonlocal swapped
                result = original(*args, **kwargs)
                if not swapped:
                    swapped = True
                    destination.rename(root / "destination-old")
                    destination.symlink_to(replacement, target_is_directory=True)
                return result

            with patch.object(catalog_module, "write_file_at", side_effect=swap_destination):
                with self.assertRaises(OSError):
                    assemble_evaluator_catalog(
                        source_root=source,
                        config_path=source / "config/cval.toml",
                        destination_root=destination,
                    )
            self.assertEqual(list(replacement.iterdir()), [])
            self.assertEqual(list((root / "destination-old").iterdir()), [])

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            destination = root / "destination"
            destination.mkdir()
            original = catalog_module.write_file_at
            calls = 0

            def fail_third(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 3:
                    raise OSError("injected catalog third-file failure")
                return original(*args, **kwargs)

            with patch.object(catalog_module, "write_file_at", side_effect=fail_third):
                with self.assertRaisesRegex(OSError, "third-file"):
                    assemble_evaluator_catalog(
                        source_root=REPO_ROOT,
                        config_path=REPO_ROOT / "config/cval.toml",
                        destination_root=destination,
                    )
            self.assertEqual(list(destination.iterdir()), [])

    def test_catalog_publication_race_does_not_overwrite_winner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            destination = root / "destination"
            destination.mkdir()
            self._write_catalog_source(source, malformed_plugin=False)
            original = catalog_module.rename_noreplace_at

            def race(source_fd, source_name, destination_fd, destination_name):
                winner_fd = catalog_module.mkdir_exact_at(
                    destination_fd, destination_name, 0o700
                )
                try:
                    catalog_module.write_file_at(winner_fd, "winner", b"kept", 0o600)
                finally:
                    os.close(winner_fd)
                return original(source_fd, source_name, destination_fd, destination_name)

            with patch.object(catalog_module, "rename_noreplace_at", side_effect=race):
                with self.assertRaises(FileExistsError):
                    assemble_evaluator_catalog(
                        source_root=source,
                        config_path=source / "config/cval.toml",
                        destination_root=destination,
                    )
            self.assertEqual(
                (destination / "validation-tests/winner").read_bytes(), b"kept"
            )
            self.assertFalse(any(path.name.startswith(".cval-catalog-") for path in destination.iterdir()))

    @staticmethod
    def _write_catalog_source(root: Path, *, malformed_plugin: bool) -> None:
        test_dir = root / "validation-tests/synthetic"
        test_dir.mkdir(parents=True)
        (root / "config").mkdir()
        for name in ("setup.sh", "run-test.sh"):
            (test_dir / name).write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
        plugin = (
            "PLUGIN = object()\n"
            if malformed_plugin
            else '''CVAL_PLUGIN_API = "cval.plugin.v1"
class Plugin:
    plugin_id = "synthetic"
    capabilities = frozenset()
PLUGIN = Plugin()
'''
        )
        (test_dir / "plugin.py").write_text(plugin, encoding="utf-8")
        (test_dir / "test_config.toml").write_text(
            '''schema_version = "cval.test.v1"
[test]
id = "synthetic"
display_name = "Synthetic"
order = 40
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/synthetic/synthetic_results.db"
[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
capabilities = []
''',
            encoding="utf-8",
        )
        (root / "config/cval.toml").write_text(
            '''[tests.synthetic]
enabled = false
config_path = "validation-tests/synthetic/test_config.toml"
''',
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
