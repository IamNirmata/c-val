from __future__ import annotations

import io
import hashlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
import zipfile
from contextlib import closing, contextmanager, redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from cval.cli import main
from cval.config import TestsConfig, default_config
from cval.evaluator.backup import backup_local_evaluator_state
from cval.evaluator.parity import SQLITE_SIGNED_INT64_MAX, build_shadow_parity_report
from cval.evaluator.preflight import run_deployment_preflight
from cval.evaluator.release import read_verified_release_identity
from cval.evaluator.service import run_evaluator_service
from cval.health.combination import canonicalize_factors
from cval.health.engine import _build_declarative_candidate, metric_specs_from_definition
from cval.health.models import ClassificationHistoryRecord
from cval.health.storage import _store_candidate
from cval.storage.per_test_results import (
    PerTestResultRecord,
    _classification_evidence_digest,
    migrate_per_test_results_to_v2,
    store_classification_history,
    write_per_test_result,
)
from cval.storage.sqlite_snapshot import immutable_sqlite_snapshot
from cval.validation.registry import (
    RegisteredValidationTest,
    ValidationTestRegistry,
)
from tests.test_health_engine import definition, observations, snapshot


REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOT = REPO_ROOT / "deploy/cval-evaluator"
COMMIT = "1" * 40
IMAGE = "registry.example/cval-evaluator@sha256:" + "2" * 64


def _raw_record(test_id: str = "smoke") -> PerTestResultRecord:
    raw_result_json = json.dumps(
        {"schema_version": "cval.test-result.v1", "test_id": test_id},
        sort_keys=True,
        separators=(",", ":"),
    )
    return PerTestResultRecord(
        run_id=f"run-{test_id}",
        test_id=test_id,
        node="node-a",
        run_timestamp=1,
        started_timestamp=1,
        completed_timestamp=2,
        status="pass",
        exit_code=0,
        image_name="image",
        pytorch_version="2.8",
        cuda_version="12.9",
        test_config_digest="sha256:" + "3" * 64,
        result_path="/copy/result.json",
        summary_path="/copy/summary.json",
        artifacts_path="/copy/artifacts",
        raw_result_json=raw_result_json,
        result_digest="sha256:" + "4" * 64,
        combination_key="sha256:" + "5" * 64,
    )


def _smoke_config(source: Path):
    active_definition = definition()
    active_definition = replace(
        active_definition,
        plugin=replace(active_definition.plugin, capabilities=("health", "ingest")),
    )
    registered = RegisteredValidationTest(
        enabled=True,
        config_path="validation-tests/smoke/test_config.toml",
        resolved_config_path=REPO_ROOT / "validation-tests/storage/test_config.toml",
        test_dir=REPO_ROOT / "validation-tests/storage",
        definition=active_definition,
    )
    config = default_config()
    config = replace(
        config,
        runtime=replace(config.runtime, validation_root="/configured-runtime-not-copy"),
        health_evaluator=replace(
            config.health_evaluator,
            state_root="/configured-state-not-copy",
            state_owner_uid=os.geteuid(),
            state_owner_gid=os.getegid(),
        ),
        tests=TestsConfig(registry=ValidationTestRegistry((registered,))),
    )
    result_path = source / active_definition.artifacts.results_db_path
    result_path.parent.mkdir(parents=True)
    current = source
    os.chmod(current, 0o700)
    for part in result_path.parent.relative_to(source).parts:
        current = current / part
        os.chmod(current, 0o700)
    write_per_test_result(_raw_record(), db_path=result_path, now=3)
    os.chmod(result_path, 0o600)
    return config, registered, result_path


def _add_health_pair(source: Path, registered: RegisteredValidationTest) -> tuple[Path, Path]:
    active_definition = registered.definition
    combination = canonicalize_factors({"image_name": "img"})
    source_snapshot = snapshot(3, active_definition, combination)
    built = _build_declarative_candidate(
        active_definition,
        combination,
        metric_specs_from_definition(active_definition),
        observations([100.0, 101.0, 99.0]),
        source_snapshot,
        parent_baseline_id=None,
        created_at=200,
    )
    health_path = source / active_definition.artifacts.health_classes_db_path
    self_stored = _store_candidate(
        built,
        active_definition,
        db_path=health_path,
        now=200,
    )
    if not self_stored:
        raise AssertionError("health fixture was not stored")
    return health_path, health_path.with_name(f"{health_path.name}.activation.key")


def _u8_history_db(
    path: Path,
    *,
    class_code: object,
    class_name: object,
    dnr_reason: object,
    baseline_id: object,
) -> Path:
    write_per_test_result(_raw_record(), db_path=path, now=3)
    migrate_per_test_results_to_v2(path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(
            """
            INSERT INTO classification_history (
                classification_key, result_id, run_id, baseline_id,
                baseline_identity, target_digest, evidence_digest,
                combination_key, health_class_name, health_class_numerical,
                dnr_reason, classified_at, evaluator_version,
                metric_verdicts_json, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sha256:" + "6" * 64,
                1,
                "run-smoke",
                baseline_id,
                "ht1:" + "7" * 64,
                "sha256:" + "8" * 64,
                "sha256:" + "9" * 64,
                "sha256:" + "a" * 64,
                class_name,
                class_code,
                dnr_reason,
                4,
                "health-engine.v1",
                "[]",
                "{}",
            ),
        )
        connection.commit()
    os.chmod(path, 0o600)
    return path


def _classification_record(
    *,
    result_id: int = 1,
    run_id: str = "run-smoke",
    identity_suffix: str = "7",
) -> ClassificationHistoryRecord:
    baseline_identity = "ht1:" + identity_suffix * 64
    identity_payload = json.dumps(
        {"run_id": run_id, "baseline_identity": baseline_identity},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    record = ClassificationHistoryRecord(
        classification_key="sha256:" + hashlib.sha256(identity_payload).hexdigest(),
        result_id=result_id,
        run_id=run_id,
        baseline_id=None,
        baseline_identity=baseline_identity,
        target_digest="sha256:" + "8" * 64,
        evidence_digest="sha256:" + "0" * 64,
        combination_key="sha256:" + "5" * 64,
        health_class_name="DNR",
        health_class_numerical=5,
        dnr_reason="raw_failed",
        classified_at=4,
        evaluator_version="health-engine.v1",
        metric_verdicts_json="[]",
        details_json='{"dnr_reason":"raw_failed"}',
    )
    return replace(record, evidence_digest=_classification_evidence_digest(record))


class EvaluatorReleaseAndServiceTests(unittest.TestCase):
    def test_release_marker_must_match_non_placeholder_digest_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            marker = Path(tmpdir) / "BUILD_COMMIT"
            marker.write_text(COMMIT + "\n", encoding="ascii")
            self.assertEqual(
                read_verified_release_identity(
                    expected_commit=COMMIT,
                    image_ref=IMAGE,
                    marker_path=marker,
                ),
                {"commit": COMMIT, "image": IMAGE},
            )
            marker.write_text("0" * 40 + "\n", encoding="ascii")
            with self.assertRaisesRegex(RuntimeError, "not a built release"):
                read_verified_release_identity(
                    expected_commit=COMMIT,
                    image_ref=IMAGE,
                    marker_path=marker,
                )

    def test_wheel_and_sdist_contain_release_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            output = root / "dist"
            source.mkdir()
            output.mkdir()
            for name in ("pyproject.toml", "README.md", "MANIFEST.in"):
                shutil.copy2(REPO_ROOT / name, source / name)
            shutil.copytree(REPO_ROOT / "cval", source / "cval")
            script = (
                "import os; from setuptools.build_meta import build_sdist, build_wheel; "
                f"os.chdir({str(source)!r}); "
                f"build_wheel({str(output)!r}); build_sdist({str(output)!r})"
            )
            completed = subprocess.run(
                [sys.executable, "-c", script],
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "PIP_NO_INDEX": "1"},
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            wheel = next(output.glob("cval-*.whl"))
            sdist = next(output.glob("cval-*.tar.gz"))
            with zipfile.ZipFile(wheel) as archive:
                self.assertEqual(
                    archive.read("cval/evaluator/BUILD_COMMIT"),
                    b"0" * 40 + b"\n",
                )
            with tarfile.open(sdist, "r:gz") as archive:
                marker = next(
                    member
                    for member in archive.getmembers()
                    if member.name.endswith("/cval/evaluator/BUILD_COMMIT")
                )
                extracted = archive.extractfile(marker)
                self.assertIsNotNone(extracted)
                self.assertEqual(extracted.read(), b"0" * 40 + b"\n")

    def test_service_envelope_is_stdout_only_and_contains_release_preflight_u9(self) -> None:
        class Report:
            ok = True

            @staticmethod
            def to_dict():
                return {"mode": "dry-run", "ok": True, "tests": []}

        with tempfile.TemporaryDirectory() as tmpdir:
            marker = Path(tmpdir) / "BUILD_COMMIT"
            marker.write_text(COMMIT, encoding="ascii")
            ticks = iter((1_000_000_000, 1_125_000_000))
            envelope = run_evaluator_service(
                default_config(),
                expected_commit=COMMIT,
                image_ref=IMAGE,
                marker_path=marker,
                wall_clock=lambda: 10.9,
                monotonic_ns=lambda: next(ticks),
                preflight_runner=lambda _config, access: {
                    "schema_version": "cval.evaluator-preflight.v1",
                    "ok": access == "ro",
                },
                evaluator=lambda *_args, **_kwargs: Report(),
            )
        self.assertEqual(envelope["schema_version"], "cval.evaluator-cycle.v1")
        self.assertEqual(envelope["duration_ms"], 125)
        self.assertEqual(envelope["exit_code"], 0)
        self.assertEqual(envelope["release"]["commit"], COMMIT)
        self.assertEqual(envelope["log_persistence"], "stdout-only")
        self.assertEqual(envelope["u9_report"]["mode"], "dry-run")

    def test_service_apply_and_preflight_fail_closed(self) -> None:
        envelope = run_evaluator_service(
            default_config(),
            apply=True,
            confirmation="evaluate",
            write_enabled=False,
        )
        self.assertEqual(envelope["exit_code"], 2)
        self.assertIsNone(envelope["u9_report"])
        self.assertIn("write gate", envelope["error"])

    def test_service_suppresses_noisy_dependencies_without_exposing_content(self) -> None:
        class Report:
            ok = True

            @staticmethod
            def to_dict():
                print("u9-secret-output")
                return {"ok": True}

        with tempfile.TemporaryDirectory() as tmpdir:
            marker = Path(tmpdir) / "BUILD_COMMIT"
            marker.write_text(COMMIT, encoding="ascii")
            stdout = io.StringIO()
            stderr = io.StringIO()

            def noisy_preflight(_config, *, access):
                print("preflight-secret-output")
                print("preflight-secret-error", file=os.sys.stderr)
                return {"ok": access == "ro"}

            def noisy_evaluator(*_args, **_kwargs):
                print("evaluator-secret-output")
                return Report()

            with redirect_stdout(stdout), redirect_stderr(stderr):
                envelope = run_evaluator_service(
                    default_config(),
                    expected_commit=COMMIT,
                    image_ref=IMAGE,
                    marker_path=marker,
                    preflight_runner=noisy_preflight,
                    evaluator=noisy_evaluator,
                )
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")
        rendered = json.dumps(envelope, sort_keys=True)
        self.assertNotIn("secret", rendered)
        self.assertEqual([item["stage"] for item in envelope["diagnostics"]], ["preflight", "evaluator"])
        self.assertGreater(envelope["diagnostics"][0]["stdout_characters"], 0)
        self.assertGreater(envelope["diagnostics"][0]["stderr_characters"], 0)

    def test_service_signal_returns_final_structured_envelope_and_restores_handler(self) -> None:
        class NeverReport:
            ok = True

            @staticmethod
            def to_dict():
                raise AssertionError("signal must interrupt before report conversion")

        for signal_number in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signal.Signals(signal_number).name), tempfile.TemporaryDirectory() as tmpdir:
                marker = Path(tmpdir) / "BUILD_COMMIT"
                marker.write_text(COMMIT, encoding="ascii")
                previous = signal.getsignal(signal_number)

                def interrupted_evaluator(*_args, **_kwargs):
                    os.kill(os.getpid(), signal_number)
                    return NeverReport()

                envelope = run_evaluator_service(
                    default_config(),
                    expected_commit=COMMIT,
                    image_ref=IMAGE,
                    marker_path=marker,
                    preflight_runner=lambda *_args, **_kwargs: {"ok": True},
                    evaluator=interrupted_evaluator,
                )
            self.assertEqual(envelope["exit_code"], 128 + signal_number)
            self.assertEqual(
                envelope["signal"],
                {"number": signal_number, "name": signal.Signals(signal_number).name},
            )
            self.assertEqual(signal.getsignal(signal_number), previous)
            self.assertIsNone(envelope["u9_report"])

    def test_service_redacts_dependency_exception_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            marker = Path(tmpdir) / "BUILD_COMMIT"
            marker.write_text(COMMIT, encoding="ascii")

            def failing_preflight(*_args, **_kwargs):
                print("token=stdout-secret")
                raise ValueError("token=exception-secret")

            envelope = run_evaluator_service(
                default_config(),
                expected_commit=COMMIT,
                image_ref=IMAGE,
                marker_path=marker,
                preflight_runner=failing_preflight,
            )
        rendered = json.dumps(envelope, sort_keys=True)
        self.assertNotIn("secret", rendered)
        self.assertEqual(envelope["error"], "preflight dependency failed (ValueError)")

    def test_service_captures_preflight_system_exit_in_redacted_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            marker = Path(tmpdir) / "BUILD_COMMIT"
            marker.write_text(COMMIT, encoding="ascii")

            def exiting_preflight(*_args, **_kwargs):
                raise SystemExit("preflight-secret")

            envelope = run_evaluator_service(
                default_config(),
                expected_commit=COMMIT,
                image_ref=IMAGE,
                marker_path=marker,
                preflight_runner=exiting_preflight,
            )
        rendered = json.dumps(envelope, sort_keys=True)
        self.assertNotIn("secret", rendered)
        self.assertEqual(envelope["schema_version"], "cval.evaluator-cycle.v1")
        self.assertEqual(envelope["exit_code"], 2)
        self.assertEqual(envelope["error"], "preflight dependency failed (SystemExit)")
        self.assertEqual([item["stage"] for item in envelope["diagnostics"]], ["preflight"])

    def test_service_captures_evaluator_system_exit_in_redacted_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            marker = Path(tmpdir) / "BUILD_COMMIT"
            marker.write_text(COMMIT, encoding="ascii")

            def exiting_evaluator(*_args, **_kwargs):
                raise SystemExit("evaluator-secret")

            envelope = run_evaluator_service(
                default_config(),
                expected_commit=COMMIT,
                image_ref=IMAGE,
                marker_path=marker,
                preflight_runner=lambda *_args, **_kwargs: {"ok": True},
                evaluator=exiting_evaluator,
            )
        rendered = json.dumps(envelope, sort_keys=True)
        self.assertNotIn("secret", rendered)
        self.assertEqual(envelope["schema_version"], "cval.evaluator-cycle.v1")
        self.assertEqual(envelope["exit_code"], 2)
        self.assertEqual(envelope["error"], "evaluator dependency failed (SystemExit)")
        self.assertEqual(
            [item["stage"] for item in envelope["diagnostics"]],
            ["preflight", "evaluator"],
        )


class EvaluatorPreflightAndParityTests(unittest.TestCase):
    def test_preflight_missing_sources_has_no_filesystem_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = replace(
                default_config(),
                runtime=replace(
                    default_config().runtime,
                    validation_root="/configured-shared-not-state",
                ),
                health_evaluator=replace(
                    default_config().health_evaluator,
                    state_root=str(root),
                    state_owner_uid=os.geteuid(),
                    state_owner_gid=os.getegid(),
                ),
            )
            before = sorted(str(path.relative_to(root)) for path in root.rglob("*"))
            report = run_deployment_preflight(config)
            after = sorted(str(path.relative_to(root)) for path in root.rglob("*"))
        self.assertFalse(report["ok"])
        self.assertEqual(before, after)
        self.assertTrue(all(not test["health_db_present"] for test in report["tests"]))

    def test_preflight_retained_traceback_closes_all_test_bindings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = Path(tmpdir) / "state"
            state.mkdir(mode=0o700)
            config, _registered, _result_path = _smoke_config(state)
            config = replace(
                config,
                health_evaluator=replace(
                    config.health_evaluator,
                    state_root=str(state),
                ),
            )
            held: list[BaseException] = []
            before = len(list(Path("/proc/self/fd").iterdir()))

            def fail_snapshot(*_args, **_kwargs):
                try:
                    raise RuntimeError("retained snapshot traceback")
                except RuntimeError as exc:
                    held.append(exc)
                    raise

            with patch(
                "cval.evaluator.preflight.immutable_sqlite_snapshot",
                side_effect=fail_snapshot,
            ):
                report = run_deployment_preflight(config)
            after = len(list(Path("/proc/self/fd").iterdir()))
            self.assertFalse(report["ok"])
            self.assertEqual(before, after)
            self.assertEqual(len(held), 1)

    def test_shadow_needs_no_write_but_apply_requires_writable_state_mount(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shared = root / "shared-evidence"
            shared.mkdir(mode=0o755)
            os.chmod(shared, 0o755)
            state = root / "evaluator-state"
            state.mkdir(mode=0o700)
            os.chmod(state, 0o700)
            config, _registered, _result_path = _smoke_config(state)
            config = replace(
                config,
                runtime=replace(config.runtime, validation_root=str(shared)),
                health_evaluator=replace(
                    config.health_evaluator,
                    state_root=str(state),
                ),
            )
            readonly = SimpleNamespace(f_flag=getattr(os, "ST_RDONLY", 1))
            with patch(
                "cval.evaluator.state.os.statvfs",
                return_value=readonly,
            ), patch(
                "cval.evaluator.preflight.validate_registry_plugins",
                return_value=("smoke",),
            ):
                shadow = run_deployment_preflight(config, access="ro")
                apply = run_deployment_preflight(config, access="rw")

        self.assertTrue(shadow["ok"])
        self.assertEqual(shadow["state_root"], str(state))
        self.assertFalse(shadow["tests"][0]["health_db_present"])
        self.assertFalse(apply["ok"])
        self.assertIn("read-only", apply["checks"][1]["detail"])

    def test_preflight_rejects_wrong_fixed_process_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = Path(tmpdir) / "evaluator-state"
            state.mkdir(mode=0o700)
            config, _registered, _result_path = _smoke_config(state)
            config = replace(
                config,
                health_evaluator=replace(
                    config.health_evaluator,
                    state_root=str(state),
                    state_owner_uid=os.geteuid() + 1,
                ),
            )
            report = run_deployment_preflight(config, access="ro")

        self.assertFalse(report["ok"])
        self.assertIn("process owner mismatch", report["checks"][1]["detail"])

    def test_parity_preserves_originals_dnr_and_is_deterministic(self) -> None:
        u8 = [
            {
                "node": "node-a",
                "test_id": "storage",
                "run_id": "run-a",
                "class_code": 1,
                "class_name": "Nominal",
                "dnr_reason": None,
                "baseline_id": "hb1:" + "a" * 64,
            },
            {
                "node": "node-b",
                "test_id": "storage",
                "run_id": "run-b",
                "class_code": 5,
                "class_name": "DNR",
                "dnr_reason": "raw_failed",
                "baseline_id": None,
            },
        ]
        compatibility = [
            {
                "node": "node-a",
                "test_type": "storage",
                "status": "degraded",
                "baseline_id": "storage-1",
            },
            {
                "node": "node-c",
                "test_type": "storage",
                "status": "normal",
                "baseline_id": "storage-1",
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            u8_path = root / "u8.json"
            compat_path = root / "compat.json"
            u8_path.write_text(json.dumps(u8), encoding="utf-8")
            compat_path.write_text(json.dumps(compatibility), encoding="utf-8")
            first = build_shadow_parity_report(
                u8_json_paths=[u8_path], compatibility_json_paths=[compat_path]
            )
            second = build_shadow_parity_report(
                u8_json_paths=[u8_path], compatibility_json_paths=[compat_path]
            )
        self.assertEqual(first, second)
        self.assertFalse(first["authoritative"])
        self.assertEqual(first["coverage"]["paired"], 1)
        self.assertEqual(first["coverage"]["direction_divergences"], 1)
        self.assertEqual(first["unpaired"]["u8_only"], [{"node": "node-b", "test_id": "storage"}])
        self.assertEqual(first["bucket_counts"]["u8"][0]["bucket"], "dnr")

    def test_parity_reads_compatibility_db_from_immutable_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            u8_path = root / "u8.json"
            compat_db = root / "compat.db"
            u8_path.write_text(
                json.dumps(
                    [
                        {
                            "node": "node-a",
                            "test_id": "storage",
                            "run_id": "run-a",
                            "class_code": 0,
                            "class_name": "Excellent",
                            "dnr_reason": None,
                            "baseline_id": "hb1:" + "a" * 64,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with closing(sqlite3.connect(compat_db)) as connection:
                connection.execute(
                    "CREATE TABLE classification_results (classified_at INTEGER, "
                    "node TEXT, test_type TEXT, baseline_id TEXT, status TEXT)"
                )
                connection.execute(
                    "INSERT INTO classification_results VALUES (1, 'node-a', "
                    "'storage', 's1', 'improved')"
                )
                connection.commit()
            before = sorted(path.name for path in root.iterdir())
            report = build_shadow_parity_report(
                u8_json_paths=[u8_path], compatibility_db_paths=[compat_db]
            )
            after = sorted(path.name for path in root.iterdir())
        self.assertEqual(before, after)
        self.assertEqual(report["coverage"]["direction_matches"], 1)

    def test_parity_compatibility_db_rejects_nonexact_storage_classes(self) -> None:
        invalid = (
            ("node-blob", sqlite3.Binary(b"node-a"), "storage", "normal", "s1", 1),
            ("test-integer", "node-a", 7, "normal", "s1", 1),
            ("status-blob", "node-a", "storage", sqlite3.Binary(b"normal"), "s1", 1),
            ("baseline-integer", "node-a", "storage", "normal", 7, 1),
            ("baseline-blob", "node-a", "storage", "normal", sqlite3.Binary(b"s1"), 1),
            ("timestamp-text", "node-a", "storage", "normal", "s1", "1"),
            ("timestamp-blob", "node-a", "storage", "normal", "s1", sqlite3.Binary(b"1")),
        )
        for label, node, test_id, status, baseline_id, classified_at in invalid:
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "compat.db"
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        "CREATE TABLE classification_results (classified_at, node, "
                        "test_type, baseline_id, status)"
                    )
                    connection.execute(
                        "INSERT INTO classification_results VALUES (?, ?, ?, ?, ?)",
                        (classified_at, node, test_id, baseline_id, status),
                    )
                    connection.commit()
                os.chmod(path, 0o600)
                with self.assertRaisesRegex(ValueError, "storage class"):
                    build_shadow_parity_report(compatibility_db_paths=[path])

    def test_parity_compatibility_db_rejects_empty_identity_and_invalid_status(self) -> None:
        for label, node, status in (
            ("empty-node", "", "normal"),
            ("empty-baseline", "node-a", "normal"),
            ("uppercase-status", "node-a", "NORMAL"),
        ):
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "compat.db"
                baseline_id = "" if label == "empty-baseline" else "s1"
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(
                        "CREATE TABLE classification_results (classified_at INTEGER, "
                        "node TEXT, test_type TEXT, baseline_id TEXT, status TEXT)"
                    )
                    connection.execute(
                        "INSERT INTO classification_results VALUES (?, ?, ?, ?, ?)",
                        (1, node, "storage", baseline_id, status),
                    )
                    connection.commit()
                os.chmod(path, 0o600)
                with self.assertRaisesRegex(ValueError, "row evidence"):
                    build_shadow_parity_report(compatibility_db_paths=[path])

    def test_parity_rejects_boolean_class_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "u8.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "node": "node-a",
                            "test_id": "storage",
                            "run_id": "run-a",
                            "class_code": True,
                            "class_name": "Nominal",
                            "dnr_reason": None,
                            "baseline_id": "hb1:" + "a" * 64,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "not Boolean"):
                build_shadow_parity_report(u8_json_paths=[path])

    def test_parity_rejects_float_class_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "u8.json"
            path.write_text(
                json.dumps(
                    [
                        {
                            "node": "node-a",
                            "test_id": "storage",
                            "run_id": "run-a",
                            "class_code": 1.0,
                            "class_name": "Nominal",
                            "dnr_reason": None,
                            "baseline_id": "hb1:" + "a" * 64,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "type exactly int"):
                build_shadow_parity_report(u8_json_paths=[path])

    def test_parity_api_strict_json_loader_rejects_ambiguous_inputs(self) -> None:
        valid = {
            "node": "node-a",
            "test_id": "storage",
            "run_id": "run-a",
            "class_code": 1,
            "class_name": "Nominal",
            "dnr_reason": None,
            "baseline_id": "hb1:" + "a" * 64,
        }
        encoded = json.dumps([valid])
        cases = (
            (
                "duplicate-key",
                encoded.replace(
                    '"node": "node-a"',
                    '"node": "node-a", "node": "node-b"',
                ),
                "duplicate object key",
            ),
            (
                "nan",
                encoded.replace('"class_code": 1', '"class_code": NaN'),
                "non-standard numeric constant: NaN",
            ),
            (
                "infinity",
                encoded.replace('"class_code": 1', '"class_code": Infinity'),
                "non-standard numeric constant: Infinity",
            ),
            ("object-wrapper", json.dumps({"records": [valid]}), "exactly an array"),
            ("scalar", "null", "exactly an array"),
            ("non-object-record", "[1]", "array of record objects"),
        )
        for label, raw, error in cases:
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "u8.json"
                path.write_text(raw, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, error):
                    build_shadow_parity_report(u8_json_paths=[path])

    def test_parity_api_bounds_every_json_timestamp_to_sqlite_int64(self) -> None:
        u8 = {
            "node": "node-a",
            "test_id": "storage",
            "run_id": "run-a",
            "class_code": 1,
            "class_name": "Nominal",
            "dnr_reason": None,
            "baseline_id": "hb1:" + "a" * 64,
        }
        compatibility = {
            "node": "node-a",
            "test_type": "storage",
            "status": "normal",
            "baseline_id": "storage-1",
        }
        invalid_values = (
            ("negative", -1),
            ("boolean", True),
            ("huge", SQLITE_SIGNED_INT64_MAX + 1),
        )
        timestamp_fields = (
            "classified_at",
            "run_timestamp",
            "started_timestamp",
            "completed_timestamp",
        )
        for source, record, argument in (
            ("u8", u8, "u8_json_paths"),
            ("compatibility", compatibility, "compatibility_json_paths"),
        ):
            for field in timestamp_fields:
                for label, value in invalid_values:
                    with (
                        self.subTest(source=source, field=field, case=label),
                        tempfile.TemporaryDirectory() as tmpdir,
                    ):
                        path = Path(tmpdir) / f"{source}.json"
                        path.write_text(
                            json.dumps([record | {field: value}]),
                            encoding="utf-8",
                        )
                        with self.assertRaisesRegex(
                            ValueError,
                            "exactly non-negative int.*SQLite signed 64-bit maximum",
                        ):
                            build_shadow_parity_report(**{argument: [path]})

        for source, record, argument in (
            ("u8", u8, "u8_json_paths"),
            ("compatibility", compatibility, "compatibility_json_paths"),
        ):
            with (
                self.subTest(source=source, case="maximum"),
                tempfile.TemporaryDirectory() as tmpdir,
            ):
                path = Path(tmpdir) / f"{source}.json"
                path.write_text(
                    json.dumps([record | {"classified_at": SQLITE_SIGNED_INT64_MAX}]),
                    encoding="utf-8",
                )
                report = build_shadow_parity_report(**{argument: [path]})
                self.assertEqual(report["coverage"][source], 1)

    def test_parity_json_requires_exact_run_baseline_reason_and_timestamp_types(self) -> None:
        valid = {
            "node": "node-a",
            "test_id": "storage",
            "run_id": "run-a",
            "class_code": 1,
            "class_name": "Nominal",
            "dnr_reason": None,
            "baseline_id": "hb1:" + "a" * 64,
            "classified_at": 4,
        }
        cases = (
            ("empty-run", {"run_id": ""}, "run_id"),
            ("numeric-run", {"run_id": 7}, "run_id"),
            ("missing-baseline", {"baseline_id": None}, "baseline"),
            ("bad-baseline", {"baseline_id": "hb1:bad"}, "baseline"),
            ("reason-on-class", {"dnr_reason": "raw_failed"}, "only for class 5"),
            ("class-name", {"class_name": "Excellent"}, "mismatch"),
            ("bool-timestamp", {"classified_at": True}, "exactly non-negative int"),
            ("float-timestamp", {"completed_timestamp": 4.0}, "exactly non-negative int"),
        )
        for label, changes, error in cases:
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "u8.json"
                path.write_text(json.dumps([valid | changes]), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, error):
                    build_shadow_parity_report(u8_json_paths=[path])

        dnr = valid | {
            "class_code": 5,
            "class_name": "DNR",
            "dnr_reason": "not-a-stable-reason",
            "baseline_id": None,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "u8.json"
            path.write_text(json.dumps([dnr]), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "stable DnrReason"):
                build_shadow_parity_report(u8_json_paths=[path])

    def test_parity_compatibility_json_rejects_coercion_and_invalid_types(self) -> None:
        valid = {
            "node": "node-a",
            "test_type": "storage",
            "status": "normal",
            "baseline_id": "storage-1",
            "classified_at": 4,
        }
        for label, changes, error in (
            ("uppercase", {"status": "NORMAL"}, "Unknown compatibility"),
            ("empty-baseline", {"baseline_id": ""}, "baseline_id"),
            ("bool-timestamp", {"classified_at": False}, "exactly non-negative int"),
            ("float-timestamp", {"classified_at": 4.0}, "exactly non-negative int"),
        ):
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "compat.json"
                path.write_text(json.dumps([valid | changes]), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, error):
                    build_shadow_parity_report(compatibility_json_paths=[path])

    def test_parity_u8_db_rejects_non_integer_class_code_storage(self) -> None:
        invalid_codes = (
            ("real", 1.5),
            ("blob", sqlite3.Binary(b"\x01")),
            ("text", "one"),
            ("bool-like", "true"),
        )
        for label, class_code in invalid_codes:
            with self.subTest(storage=label), tempfile.TemporaryDirectory() as tmpdir:
                path = _u8_history_db(
                    Path(tmpdir) / "copied-u8.db",
                    class_code=class_code,
                    class_name="Nominal",
                    dnr_reason=None,
                    baseline_id="hb1:" + "b" * 64,
                )
                with self.assertRaises((ValueError, RuntimeError)) as caught:
                    build_shadow_parity_report(
                        u8_db_paths=[path], registered_test_ids=("smoke",)
                    )
                self.assertRegex(
                    str(caught.exception),
                    "storage class INTEGER|integrity_check|invalid typed evidence",
                )

    def test_parity_u8_db_rejects_mismatched_class_name_and_dnr(self) -> None:
        invalid = (
            {
                "label": "name",
                "class_code": 1,
                "class_name": "Excellent",
                "dnr_reason": None,
                "baseline_id": "hb1:" + "b" * 64,
                "error": "stable code",
            },
            {
                "label": "missing-dnr",
                "class_code": 5,
                "class_name": "DNR",
                "dnr_reason": None,
                "baseline_id": None,
                "error": "requires a non-empty reason",
            },
            {
                "label": "unexpected-dnr",
                "class_code": 1,
                "class_name": "Nominal",
                "dnr_reason": "raw_failed",
                "baseline_id": "hb1:" + "b" * 64,
                "error": "only for class 5",
            },
        )
        for case in invalid:
            with self.subTest(case=case["label"]), tempfile.TemporaryDirectory() as tmpdir:
                path = _u8_history_db(
                    Path(tmpdir) / "copied-u8.db",
                    class_code=case["class_code"],
                    class_name=case["class_name"],
                    dnr_reason=case["dnr_reason"],
                    baseline_id=case["baseline_id"],
                )
                with self.assertRaises((ValueError, RuntimeError)) as caught:
                    build_shadow_parity_report(
                        u8_db_paths=[path], registered_test_ids=("smoke",)
                    )
                self.assertRegex(
                    str(caught.exception),
                    case["error"] + "|integrity_check",
                )

    def test_parity_u8_db_enforces_baseline_semantics(self) -> None:
        invalid = (
            ("evaluated", 1, "Nominal", None, None),
            ("raw-dnr", 5, "DNR", "raw_failed", "hb1:" + "b" * 64),
            ("evaluated-dnr", 5, "DNR", "no_observations", None),
        )
        for label, code, name, reason, baseline_id in invalid:
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmpdir:
                path = _u8_history_db(
                    Path(tmpdir) / "copied-u8.db",
                    class_code=code,
                    class_name=name,
                    dnr_reason=reason,
                    baseline_id=baseline_id,
                )
                with self.assertRaises((ValueError, RuntimeError)) as caught:
                    build_shadow_parity_report(
                        u8_db_paths=[path], registered_test_ids=("smoke",)
                    )
                self.assertRegex(
                    str(caught.exception),
                    "baseline|invalid typed evidence",
                )

    def test_parity_u8_db_audits_history_owner_before_latest_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "copied-u8.db"
            write_per_test_result(_raw_record(), db_path=path, now=3)
            migrate_per_test_results_to_v2(path)
            record = _classification_record(result_id=2)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "INSERT INTO classification_history (classification_key, result_id, "
                    "run_id, baseline_id, baseline_identity, target_digest, "
                    "evidence_digest, combination_key, health_class_name, "
                    "health_class_numerical, dnr_reason, classified_at, "
                    "evaluator_version, metric_verdicts_json, details_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        record.classification_key,
                        record.result_id,
                        record.run_id,
                        record.baseline_id,
                        record.baseline_identity,
                        record.target_digest,
                        record.evidence_digest,
                        record.combination_key,
                        record.health_class_name,
                        record.health_class_numerical,
                        record.dnr_reason,
                        record.classified_at,
                        record.evaluator_version,
                        record.metric_verdicts_json,
                        record.details_json,
                    ),
                )
                connection.commit()
            os.chmod(path, 0o600)
            with self.assertRaisesRegex(RuntimeError, "owner identity"):
                build_shadow_parity_report(
                    u8_db_paths=[path], registered_test_ids=("smoke",)
                )

    def test_parity_u8_db_rejects_malformed_history_and_mixed_test_owners(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            malformed = _u8_history_db(
                Path(tmpdir) / "malformed.db",
                class_code=5,
                class_name="DNR",
                dnr_reason="raw_failed",
                baseline_id=None,
            )
            with self.assertRaisesRegex(RuntimeError, "invalid typed evidence"):
                build_shadow_parity_report(
                    u8_db_paths=[malformed], registered_test_ids=("smoke",)
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "mixed-owner.db"
            write_per_test_result(_raw_record(), db_path=path, now=3)
            migrate_per_test_results_to_v2(path)
            store_classification_history((_classification_record(),), db_path=path)
            write_per_test_result(_raw_record("other"), db_path=path, now=4)
            store_classification_history(
                (
                    _classification_record(
                        result_id=2,
                        run_id="run-other",
                        identity_suffix="6",
                    ),
                ),
                db_path=path,
            )
            os.chmod(path, 0o600)
            with self.assertRaisesRegex(RuntimeError, "test owner"):
                build_shadow_parity_report(
                    u8_db_paths=[path], registered_test_ids=("smoke", "other")
                )

    def test_preflight_and_parity_reject_mixed_u7_owner_without_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, _registered, result_path = _smoke_config(root)
            write_per_test_result(_raw_record("other"), db_path=result_path, now=4)
            os.chmod(result_path, 0o600)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
            )
            with patch(
                "cval.evaluator.preflight.validate_registry_plugins",
                return_value=("smoke",),
            ):
                report = run_deployment_preflight(local)
            schema_check = next(
                item
                for item in report["tests"][0]["checks"]
                if item["name"] == "result-db-schema"
            )
            self.assertFalse(schema_check["ok"])
            self.assertIn("owner integrity", schema_check["detail"])
            with self.assertRaisesRegex(RuntimeError, "registered test|owner integrity"):
                build_shadow_parity_report(
                    u8_db_paths=[result_path],
                    registered_test_ids=("smoke", "other"),
                )

    def test_preflight_and_parity_reject_foreign_receipt_owner_without_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, _registered, result_path = _smoke_config(root)
            with closing(sqlite3.connect(result_path)) as connection:
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute(
                    "INSERT INTO metric_ingestion_receipts (run_id, test_id, "
                    "adapter_api_version, evidence_digest, inserted_count, "
                    "updated_count, metric_names_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "run-smoke",
                        "other",
                        "cval.plugin.v1",
                        "sha256:" + "a" * 64,
                        1,
                        0,
                        '["metric"]',
                        4,
                    ),
                )
                connection.commit()
            os.chmod(result_path, 0o600)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
            )
            with patch(
                "cval.evaluator.preflight.validate_registry_plugins",
                return_value=("smoke",),
            ):
                report = run_deployment_preflight(local)
            schema_check = next(
                item
                for item in report["tests"][0]["checks"]
                if item["name"] == "result-db-schema"
            )
            self.assertFalse(schema_check["ok"])
            self.assertIn("receipt owner/parent integrity", schema_check["detail"])
            with self.assertRaisesRegex(RuntimeError, "receipt owner/parent integrity"):
                build_shadow_parity_report(
                    u8_db_paths=[result_path],
                    registered_test_ids=("smoke", "other"),
                )

    def test_preflight_accepts_safe_nested_missing_health_parent_without_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, registered, _result_path = _smoke_config(root)
            nested_definition = replace(
                registered.definition,
                artifacts=replace(
                    registered.definition.artifacts,
                    health_classes_db_path=(
                        "validation_tests/smoke/missing/nested/smoke_health_classes.db"
                    ),
                ),
            )
            nested_registered = replace(registered, definition=nested_definition)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
                tests=TestsConfig(
                    registry=ValidationTestRegistry((nested_registered,))
                ),
            )
            before = sorted(str(item.relative_to(root)) for item in root.rglob("*"))
            with patch(
                "cval.evaluator.preflight.validate_registry_plugins",
                return_value=("smoke",),
            ):
                report = run_deployment_preflight(local)
            after = sorted(str(item.relative_to(root)) for item in root.rglob("*"))
        check = next(
            item
            for item in report["tests"][0]["checks"]
            if item["name"] == "health-owner-directory"
        )
        self.assertTrue(check["ok"])
        self.assertIn("state ancestry", check["detail"])
        self.assertEqual(before, after)

    def test_preflight_rejects_unsafe_intermediate_0770_ancestry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, _registered, _result_path = _smoke_config(root)
            intermediate = root / "validation_tests"
            os.chmod(intermediate, 0o770)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
            )
            report = run_deployment_preflight(local)
        owner = next(
            item
            for item in report["tests"][0]["checks"]
            if item["name"] == "result-db-file"
        )
        self.assertFalse(owner["ok"])
        self.assertIn("exact owner 0700", owner["detail"])

    def test_preflight_rejects_foreign_owner_ancestry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, _registered, _result_path = _smoke_config(root)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
            )
            with patch(
                "cval.evaluator.state.os.geteuid",
                return_value=os.geteuid() + 1,
            ):
                report = run_deployment_preflight(local)
        self.assertFalse(report["checks"][1]["ok"])
        self.assertIn("process owner mismatch", report["checks"][1]["detail"])

    def test_preflight_detects_intermediate_symlink_swap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, _registered, _result_path = _smoke_config(root)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
            )
            owner = root / "validation_tests/smoke"
            parked = root / "validation_tests/smoke-original"
            from cval.evaluator import preflight as preflight_module

            original_snapshot = preflight_module.immutable_sqlite_snapshot
            swapped = False

            @contextmanager
            def swapping_snapshot(path, **kwargs):
                nonlocal swapped
                with original_snapshot(path, **kwargs) as snapshot_value:
                    yield snapshot_value
                if not swapped:
                    owner.rename(parked)
                    owner.symlink_to(parked, target_is_directory=True)
                    swapped = True

            try:
                with patch(
                    "cval.evaluator.preflight.immutable_sqlite_snapshot",
                    swapping_snapshot,
                ):
                    report = run_deployment_preflight(local)
            finally:
                if owner.is_symlink():
                    owner.unlink()
                if parked.exists():
                    parked.rename(owner)
        stable = next(
            item
            for item in report["tests"][0]["checks"]
            if item["name"] == "directory-ancestry-stable"
        )
        self.assertFalse(stable["ok"])
        self.assertIn("symlink", stable["detail"])

    def test_preflight_rejects_nonwritable_0500_health_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, registered, _result_path = _smoke_config(root)
            health_parent = root / "validation_tests/smoke/health"
            health_parent.mkdir()
            os.chmod(health_parent, 0o700)
            health_definition = replace(
                registered.definition,
                artifacts=replace(
                    registered.definition.artifacts,
                    health_classes_db_path=(
                        "validation_tests/smoke/health/smoke_health_classes.db"
                    ),
                ),
            )
            health_registered = replace(registered, definition=health_definition)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
                tests=TestsConfig(
                    registry=ValidationTestRegistry((health_registered,))
                ),
            )
            os.chmod(health_parent, 0o500)
            try:
                with patch(
                    "cval.evaluator.preflight.validate_registry_plugins",
                    return_value=("smoke",),
                ):
                    report = run_deployment_preflight(local)
            finally:
                os.chmod(health_parent, 0o700)
        check = next(
            item
            for item in report["tests"][0]["checks"]
            if item["name"] == "health-owner-directory"
        )
        self.assertFalse(check["ok"])
        self.assertIn("exact owner 0700", check["detail"])

    def test_rw_preflight_rejects_0400_group_world_modes_and_hardlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, _registered, result_path = _smoke_config(root)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
            )
            for mode in (0o400, 0o640, 0o606):
                with self.subTest(mode=f"{mode:04o}"):
                    os.chmod(result_path, mode)
                    report = run_deployment_preflight(local, access="rw")
                    self.assertFalse(report["ok"])
                    result_check = next(
                        item for item in report["tests"][0]["checks"]
                        if item["name"] == "result-db-file"
                    )
                    self.assertFalse(result_check["ok"])
            os.chmod(result_path, 0o600)
            alias = result_path.with_name("result-hardlink.db")
            os.link(result_path, alias)
            report = run_deployment_preflight(local, access="rw")
        result_check = next(
            item for item in report["tests"][0]["checks"]
            if item["name"] == "result-db-file"
        )
        self.assertFalse(result_check["ok"])
        self.assertIn("link count", result_check["detail"])

    def test_preflight_rejects_orphan_and_present_db_sidecars(self) -> None:
        for suffix, health_sidecar in (("-journal", False), ("-wal", True), ("-shm", True)):
            with self.subTest(suffix=suffix, health_sidecar=health_sidecar), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir) / "source"
                root.mkdir()
                config, registered, result_path = _smoke_config(root)
                local = replace(
                    config,
                    health_evaluator=replace(config.health_evaluator, state_root=str(root)),
                )
                if health_sidecar:
                    health_path = root / registered.definition.artifacts.health_classes_db_path
                    sidecar = health_path.with_name(health_path.name + suffix)
                else:
                    sidecar = result_path.with_name(result_path.name + suffix)
                sidecar.parent.mkdir(parents=True, exist_ok=True)
                sidecar.touch()
                report = run_deployment_preflight(local)
                self.assertFalse(report["ok"])
                self.assertTrue(any(
                    not check["ok"] and "sidecar" in check["name"]
                    for check in report["tests"][0]["checks"]
                ))

    def test_shadow_preflight_preserves_source_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            config, registered, result_path = _smoke_config(root)
            health_path, key_path = _add_health_pair(root, registered)
            local = replace(
                config,
                health_evaluator=replace(config.health_evaluator, state_root=str(root)),
            )
            for path in (result_path, health_path, key_path):
                metadata = path.stat()
                os.utime(path, ns=(1_000_000_000, metadata.st_mtime_ns))
            before = {
                path: (path.stat().st_atime_ns, path.stat().st_mtime_ns, path.stat().st_ctime_ns, path.stat().st_mode)
                for path in (result_path, health_path, key_path)
            }
            with patch(
                "cval.evaluator.preflight.validate_registry_plugins",
                return_value=("smoke",),
            ):
                report = run_deployment_preflight(local, access="ro")
            after = {
                path: (path.stat().st_atime_ns, path.stat().st_mtime_ns, path.stat().st_ctime_ns, path.stat().st_mode)
                for path in (result_path, health_path, key_path)
            }
        self.assertTrue(report["ok"])
        self.assertEqual(before, after)

    def test_immutable_snapshot_rejects_rollback_journal_race(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "source"
            root.mkdir()
            _config, _registered, result_path = _smoke_config(root)
            journal = result_path.with_name(result_path.name + "-journal")
            with self.assertRaisesRegex(RuntimeError, "journal"):
                with immutable_sqlite_snapshot(result_path):
                    journal.touch()


class EvaluatorBackupTests(unittest.TestCase):
    def test_backup_dry_run_has_no_lock_or_destination_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, result_path = _smoke_config(source)
            target = root / "backup"
            before = sorted(str(path.relative_to(source)) for path in source.rglob("*"))
            report = backup_local_evaluator_state(
                config, source_root=source, destination=target
            )
            after = sorted(str(path.relative_to(source)) for path in source.rglob("*"))
        self.assertTrue(report["ok"])
        self.assertFalse(report["executed"])
        self.assertEqual(before, after)
        self.assertFalse(target.exists())
        self.assertFalse(result_path.with_name(".smoke_results.health-evaluator.lock").exists())

    def test_backup_apply_copies_and_restore_validates_u8_pair_without_key_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, registered, _result_path = _smoke_config(source)
            health_path, key_path = _add_health_pair(source, registered)
            target = root / "backup"
            report = backup_local_evaluator_state(
                config,
                source_root=source,
                destination=target,
                apply=True,
                confirmation="backup",
            )
            copied_health = target / health_path.relative_to(source)
            copied_key = target / key_path.relative_to(source)
            manifest = json.loads((target / "inventory.json").read_text(encoding="utf-8"))
            self.assertTrue(report["restore_validated"])
            self.assertTrue(copied_health.is_file())
            self.assertTrue(copied_key.is_file())
            self.assertEqual(copied_key.stat().st_mode & 0o777, 0o600)
            self.assertEqual(report["units"][0]["health_db"]["baseline_count"], 1)
            self.assertEqual(
                report["units"][0]["result_db"]["logical_inventory"],
                manifest["units"][0]["result_db"]["logical_inventory"],
            )
            rendered = json.dumps(manifest, sort_keys=True)
            self.assertNotIn("activation_key_digest", rendered)
            self.assertIn('"content_hash_recorded": false', rendered)
            with self.assertRaises(FileExistsError):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )

    def test_backup_apply_rejects_runtime_descendant_without_lock_or_destination(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runtime_root = root / "runtime"
            source = runtime_root / "nested-source"
            source.mkdir(parents=True)
            config, _registered, result_path = _smoke_config(source)
            config = replace(
                config,
                runtime=replace(config.runtime, validation_root=str(runtime_root)),
            )
            target = root / "backup"
            lock_path = result_path.with_name(".smoke_results.health-evaluator.lock")
            with self.assertRaisesRegex(ValueError, "or any descendant"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            self.assertFalse(lock_path.exists())
            self.assertFalse(target.exists())

    def test_backup_gate_and_forced_termination_leave_no_partial_restore(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, result_path = _smoke_config(source)
            target = root / "backup"
            with self.assertRaisesRegex(ValueError, "exact confirmation"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="wrong",
                )
            with patch(
                "cval.evaluator.backup._copy_sqlite",
                side_effect=KeyboardInterrupt(),
            ), self.assertRaises(KeyboardInterrupt):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            with closing(sqlite3.connect(result_path)) as connection:
                integrity = connection.execute("PRAGMA integrity_check").fetchone()
            staging = list(root.glob(".backup.*.staging"))
        self.assertFalse(target.exists())
        self.assertEqual(staging, [])
        self.assertEqual(integrity, ("ok",))

    def test_backup_rejects_runtime_root_destination_symlink_parent_and_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            config = replace(config, runtime=replace(config.runtime, validation_root=str(runtime_root)))
            with self.assertRaisesRegex(ValueError, "outside the configured live shared/state roots"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=runtime_root / "backup",
                )
            actual = root / "actual"
            actual.mkdir()
            symlink = root / "linked"
            symlink.symlink_to(actual, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "symlink component"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=symlink / "backup",
                )
            with self.assertRaisesRegex(ValueError, "traversal"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=Path(str(root / "actual") + "/../backup"),
                )
        self.assertFalse((actual / "backup").exists())

    def test_backup_destination_race_does_not_overwrite_or_remove_racer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            target = root / "backup"
            from cval.evaluator import backup as backup_module

            original_copy = backup_module._copy_sqlite
            parked = root / "reserved-original"
            raced = False

            def replace_reserved_root(*args, **kwargs):
                nonlocal raced
                if not raced:
                    raced = True
                    target.rename(parked)
                    target.mkdir(mode=0o700)
                    (target / "racer-owned").write_text("keep", encoding="utf-8")
                return original_copy(*args, **kwargs)

            with patch(
                "cval.evaluator.backup._copy_sqlite",
                side_effect=replace_reserved_root,
            ):
                with self.assertRaises(RuntimeError):
                    backup_local_evaluator_state(
                        config,
                        source_root=source,
                        destination=target,
                        apply=True,
                        confirmation="backup",
                    )
            self.assertEqual((target / "racer-owned").read_text(encoding="utf-8"), "keep")
            self.assertTrue(parked.is_dir())

    def test_backup_rejects_parent_and_higher_ancestor_replacement_before_reservation(self) -> None:
        for level in ("parent", "higher"):
            with self.subTest(level=level), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                source = root / "source"
                source.mkdir()
                config, _registered, _result_path = _smoke_config(source)
                higher = root / "destination-tree"
                parent = higher / "parent"
                parent.mkdir(parents=True, mode=0o700)
                os.chmod(higher, 0o700)
                os.chmod(parent, 0o700)
                target = parent / "backup"
                from cval.evaluator import backup as backup_module

                original_reserve = backup_module._reserve_destination
                parked = root / f"parked-{level}"

                def replace_then_reserve(binding):
                    if level == "parent":
                        parent.rename(parked)
                        parent.mkdir(mode=0o700)
                    else:
                        higher.rename(parked)
                        higher.mkdir(mode=0o700)
                        (higher / "parent").mkdir(mode=0o700)
                    return original_reserve(binding)

                with patch(
                    "cval.evaluator.backup._reserve_destination",
                    side_effect=replace_then_reserve,
                ):
                    with self.assertRaises(RuntimeError):
                        backup_local_evaluator_state(
                            config,
                            source_root=source,
                            destination=target,
                            apply=True,
                            confirmation="backup",
                        )
                self.assertFalse(target.exists())
                parked_target = (
                    parked / "backup"
                    if level == "parent"
                    else parked / "parent/backup"
                )
                self.assertFalse(parked_target.exists())

    def test_backup_root_final_name_racer_is_preserved_and_stage_is_cleaned(self) -> None:
        from cval.evaluator import backup as backup_module
        from cval.evaluator import secure_state as secure_state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parent = root / "destination-tree/parent"
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(parent.parent, 0o700)
            os.chmod(parent, 0o700)
            target = parent / "backup"
            before = len(list(Path("/proc/self/fd").iterdir()))
            binding = backup_module._bind_destination_target(target)
            original_publish = secure_state_module.rename_noreplace_at
            raced = False

            def publish_after_racer(source_parent_fd, source_name, destination_parent_fd, destination_name):
                nonlocal raced
                if destination_name == target.name and not raced:
                    raced = True
                    os.mkdir(destination_name, 0o700, dir_fd=destination_parent_fd)
                    (target / "racer-owned").write_text("keep", encoding="utf-8")
                return original_publish(
                    source_parent_fd,
                    source_name,
                    destination_parent_fd,
                    destination_name,
                )

            try:
                with patch.object(
                    secure_state_module,
                    "rename_noreplace_at",
                    side_effect=publish_after_racer,
                ), self.assertRaises(FileExistsError):
                    backup_module._reserve_destination(binding)
                self.assertTrue(raced)
                self.assertTrue(target.is_dir())
                self.assertEqual(
                    (target / "racer-owned").read_text(encoding="utf-8"),
                    "keep",
                )
                self.assertEqual(
                    list(parent.glob(".cval-dir-stage-*")),
                    [],
                )
            finally:
                binding.close()
            self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), before)

    def test_backup_destination_file_fchmod_interruption_cleans_registered_inode(self) -> None:
        from cval.evaluator import backup as backup_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            target = root / "backup"
            ancestry = backup_module._bind_destination_target(target)
            reservation = backup_module._reserve_destination(ancestry)
            primary = KeyboardInterrupt("backup fchmod interrupted")
            before = len(list(Path("/proc/self/fd").iterdir()))
            try:
                with patch.object(
                    backup_module.os,
                    "fchmod",
                    side_effect=primary,
                ), self.assertRaises(KeyboardInterrupt) as raised:
                    with backup_module._reserved_destination_file(
                        reservation,
                        Path("artifact.db"),
                    ):
                        self.fail("interrupted destination creation must not yield")
                self.assertIs(raised.exception, primary)
                self.assertFalse((target / "artifact.db").exists())
                self.assertEqual(reservation.files, {})
                self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), before)
            finally:
                backup_module._remove_tree_if_identity(reservation)
                reservation.close()

    def test_backup_destination_file_interruption_preserves_racer(self) -> None:
        from cval.evaluator import backup as backup_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            target = root / "backup"
            ancestry = backup_module._bind_destination_target(target)
            reservation = backup_module._reserve_destination(ancestry)
            original_fchmod = backup_module.os.fchmod
            raced = False

            def replace_then_interrupt(descriptor, mode):
                nonlocal raced
                if not raced:
                    raced = True
                    created = target / "artifact.db"
                    relocated = target / "relocated-created.db"
                    created.rename(relocated)
                    created.write_bytes(b"racer")
                    os.chmod(created, 0o600)
                    raise SystemExit("backup racer interruption")
                return original_fchmod(descriptor, mode)

            try:
                with patch.object(
                    backup_module.os,
                    "fchmod",
                    side_effect=replace_then_interrupt,
                ), self.assertRaises(SystemExit):
                    with backup_module._reserved_destination_file(
                        reservation,
                        Path("artifact.db"),
                    ):
                        self.fail("interrupted destination creation must not yield")
                self.assertTrue(raced)
                self.assertEqual((target / "artifact.db").read_bytes(), b"racer")
                self.assertTrue((target / "relocated-created.db").exists())
            finally:
                reservation.close()

    def test_backup_nested_final_name_racer_is_preserved_and_stage_is_cleaned(self) -> None:
        from cval.evaluator import backup as backup_module
        from cval.evaluator import secure_state as secure_state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            target = root / "backup"
            ancestry = backup_module._bind_destination_target(target)
            reservation = backup_module._reserve_destination(ancestry)
            original_publish = secure_state_module.rename_noreplace_at
            raced = False

            def publish_after_racer(source_parent_fd, source_name, destination_parent_fd, destination_name):
                nonlocal raced
                if destination_name == "nested" and not raced:
                    raced = True
                    os.mkdir(destination_name, 0o700, dir_fd=destination_parent_fd)
                    (target / "nested/racer-owned").write_text("keep", encoding="utf-8")
                return original_publish(
                    source_parent_fd,
                    source_name,
                    destination_parent_fd,
                    destination_name,
                )

            before = len(list(Path("/proc/self/fd").iterdir()))
            try:
                with patch.object(
                    secure_state_module,
                    "rename_noreplace_at",
                    side_effect=publish_after_racer,
                ), self.assertRaisesRegex(RuntimeError, "unreserved directory"):
                    with backup_module._destination_parent_fd(
                        reservation,
                        ("nested",),
                    ):
                        self.fail("raced nested creation must not yield")
                self.assertTrue(raced)
                self.assertEqual(
                    (target / "nested/racer-owned").read_text(encoding="utf-8"),
                    "keep",
                )
                self.assertEqual(
                    list(target.glob(".cval-dir-stage-*")),
                    [],
                )
                self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), before)
            finally:
                reservation.close()

    def test_backup_rejects_higher_ancestor_replacement_during_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            higher = root / "destination-tree"
            parent = higher / "parent"
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(higher, 0o700)
            os.chmod(parent, 0o700)
            target = parent / "backup"
            parked = root / "parked-destination-tree"
            replacement_marker = higher / "replacement-owned"
            from cval.evaluator import backup as backup_module

            original_copy = backup_module._copy_sqlite
            raced = False

            def replace_higher_then_copy(*args, **kwargs):
                nonlocal raced
                if not raced:
                    raced = True
                    higher.rename(parked)
                    parent.mkdir(parents=True, mode=0o700)
                    replacement_marker.write_text("keep", encoding="utf-8")
                return original_copy(*args, **kwargs)

            with patch(
                "cval.evaluator.backup._copy_sqlite",
                side_effect=replace_higher_then_copy,
            ), self.assertRaisesRegex(RuntimeError, "ancestry|cleanup"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            self.assertTrue(raced)
            self.assertEqual(replacement_marker.read_text(encoding="utf-8"), "keep")
            self.assertFalse(target.exists())
            self.assertTrue((parked / "parent/backup").is_dir())

    def test_backup_cleanup_preserves_unknown_file_and_nested_directory(self) -> None:
        for unknown_kind in ("file", "directory"):
            with self.subTest(kind=unknown_kind), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                source = root / "source"
                source.mkdir()
                config, _registered, _result_path = _smoke_config(source)
                target = root / "backup"
                from cval.evaluator import backup as backup_module

                original_copy = backup_module._copy_sqlite
                unknown = (
                    target / "unknown.txt"
                    if unknown_kind == "file"
                    else target / "unknown-dir"
                )

                def inject_unknown_then_fail(*args, **kwargs):
                    original_copy(*args, **kwargs)
                    if unknown_kind == "file":
                        unknown.write_text("keep", encoding="utf-8")
                    else:
                        unknown.mkdir(mode=0o700)
                        (unknown / "keep.txt").write_text("keep", encoding="utf-8")
                    raise RuntimeError("forced failure after unknown injection")

                with patch(
                    "cval.evaluator.backup._copy_sqlite",
                    side_effect=inject_unknown_then_fail,
                ), self.assertRaisesRegex(
                    RuntimeError,
                    "forced failure after unknown injection",
                ) as raised:
                    backup_local_evaluator_state(
                        config,
                        source_root=source,
                        destination=target,
                        apply=True,
                        confirmation="backup",
                    )
                self.assertTrue(
                    any(
                        "cleanup failed closed" in note
                        for note in getattr(raised.exception, "__notes__", ())
                    )
                )
                if unknown_kind == "file":
                    self.assertEqual(unknown.read_text(encoding="utf-8"), "keep")
                else:
                    self.assertEqual(
                        (unknown / "keep.txt").read_text(encoding="utf-8"),
                        "keep",
                    )
                self.assertTrue(target.is_dir())

    def test_backup_cleanup_removes_all_exact_created_entries_without_racer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            target = root / "backup"
            from cval.evaluator import backup as backup_module

            original_copy = backup_module._copy_sqlite

            def copy_then_fail(*args, **kwargs):
                original_copy(*args, **kwargs)
                raise RuntimeError("forced post-copy failure")

            with patch(
                "cval.evaluator.backup._copy_sqlite",
                side_effect=copy_then_fail,
            ), self.assertRaisesRegex(RuntimeError, "forced post-copy failure"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            self.assertFalse(target.exists())

    def test_backup_never_adopts_or_removes_raced_sqlite_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, registered, _result_path = _smoke_config(source)
            target = root / "backup"
            from cval.evaluator import backup as backup_module

            original_capture = backup_module._capture_destination_sidecars
            raced_path = (
                target
                / registered.definition.artifacts.results_db_path
            ).with_name(
                Path(registered.definition.artifacts.results_db_path).name
                + "-journal"
            )
            injected = False

            def inject_racer(parent_fd, database_name, artifacts, **kwargs):
                nonlocal injected
                if not injected:
                    injected = True
                    descriptor = os.open(
                        database_name + "-journal",
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                        dir_fd=parent_fd,
                    )
                    try:
                        os.write(descriptor, b"racer-owned")
                        os.fsync(descriptor)
                    finally:
                        os.close(descriptor)
                return original_capture(
                    parent_fd,
                    database_name,
                    artifacts,
                    **kwargs,
                )

            with patch(
                "cval.evaluator.backup._capture_destination_sidecars",
                side_effect=inject_racer,
            ), self.assertRaisesRegex(
                RuntimeError,
                "unknown file",
            ) as raised:
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            self.assertTrue(
                any(
                    "cleanup failed closed" in note
                    for note in getattr(raised.exception, "__notes__", ())
                )
            )
            self.assertTrue(injected)
            self.assertEqual(raced_path.read_bytes(), b"racer-owned")
            self.assertTrue(target.is_dir())

    def test_backup_revalidates_lexical_destination_after_descriptor_teardown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            higher = root / "destination-tree"
            parent = higher / "parent"
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(higher, 0o700)
            os.chmod(parent, 0o700)
            target = parent / "backup"
            parked = root / "parked-destination-tree"
            racer = target / "racer-owned"
            from cval.evaluator import backup as backup_module

            original_close = backup_module._DestinationReservation.close
            replaced = False

            def replace_during_close(reservation):
                nonlocal replaced
                if not replaced:
                    replaced = True
                    higher.rename(parked)
                    target.mkdir(parents=True, mode=0o700)
                    racer.write_text("keep", encoding="utf-8")
                return original_close(reservation)

            with patch.object(
                backup_module._DestinationReservation,
                "close",
                replace_during_close,
            ), self.assertRaisesRegex(RuntimeError, "success finalization"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            self.assertTrue(replaced)
            self.assertEqual(racer.read_text(encoding="utf-8"), "keep")
            self.assertTrue((parked / "parent/backup").is_dir())

    def test_backup_rejects_exact_root_relocated_under_replacement_ancestry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            higher = root / "destination-tree"
            parent = higher / "parent"
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(higher, 0o700)
            os.chmod(parent, 0o700)
            target = parent / "backup"
            parked = root / "parked-destination-tree"
            parked_marker = parked / "parked-owned"
            replacement_marker = higher / "replacement-owned"
            relocated_identity: tuple[int, int] | None = None
            from cval.evaluator import backup as backup_module

            original_close = backup_module._DestinationReservation.close
            replaced = False

            def relocate_exact_root_during_close(reservation):
                nonlocal replaced, relocated_identity
                if not replaced:
                    replaced = True
                    higher.rename(parked)
                    parked_marker.write_text("keep-parked", encoding="utf-8")
                    parent.mkdir(parents=True, mode=0o700)
                    os.chmod(higher, 0o700)
                    os.chmod(parent, 0o700)
                    replacement_marker.write_text("keep-replacement", encoding="utf-8")
                    original_root = parked / "parent/backup"
                    relocated_identity = (
                        original_root.stat().st_dev,
                        original_root.stat().st_ino,
                    )
                    original_root.rename(target)
                return original_close(reservation)

            with patch.object(
                backup_module._DestinationReservation,
                "close",
                relocate_exact_root_during_close,
            ), self.assertRaisesRegex(
                RuntimeError,
                "success finalization",
            ) as raised:
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            self.assertTrue(replaced)
            self.assertIsNotNone(relocated_identity)
            self.assertEqual(
                (target.stat().st_dev, target.stat().st_ino),
                relocated_identity,
            )
            self.assertTrue((target / "inventory.json").is_file())
            self.assertEqual(parked_marker.read_text(encoding="utf-8"), "keep-parked")
            self.assertEqual(
                replacement_marker.read_text(encoding="utf-8"),
                "keep-replacement",
            )
            self.assertTrue(
                any(
                    "post-close cleanup failed closed" in note
                    for note in getattr(raised.exception, "__notes__", ())
                )
            )

    def test_backup_fresh_validator_errors_cleanup_exact_tree_and_preserve_primary(self) -> None:
        from cval.evaluator import backup as backup_module

        for primary in (
            RuntimeError("forced fresh validation failure"),
            KeyboardInterrupt("forced fresh validation interrupt"),
        ):
            with self.subTest(error=type(primary).__name__), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                source = root / "source"
                source.mkdir()
                config, _registered, _result_path = _smoke_config(source)
                target = root / "backup"
                with patch(
                    "cval.evaluator.backup._assert_published_destination_path",
                    side_effect=primary,
                ), self.assertRaises(type(primary)) as raised:
                    backup_local_evaluator_state(
                        config,
                        source_root=source,
                        destination=target,
                        apply=True,
                        confirmation="backup",
                    )
                self.assertIs(raised.exception, primary)
                self.assertFalse(target.exists())

    def test_backup_post_close_cleanup_stops_before_relocated_root_file_deletion(self) -> None:
        from cval.evaluator import backup as backup_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            target = root / "backup"
            relocated = root / "relocated-original"
            racer_marker = target / "racer-owned"
            primary = RuntimeError("force post-close cleanup")
            original_cleanup = backup_module._remove_published_tree_if_identity
            expected_files: dict[Path, bytes] = {}
            raced = False

            def racing_cleanup(reservation):
                def relocate_at_first_unlink(
                    operation: str,
                    _parts: tuple[str, ...],
                ) -> None:
                    nonlocal raced, expected_files
                    if operation == "file_unlink" and not raced:
                        raced = True
                        expected_files = {
                            item.relative_to(target): item.read_bytes()
                            for item in target.rglob("*")
                            if item.is_file()
                        }
                        target.rename(relocated)
                        target.mkdir(mode=0o700)
                        racer_marker.write_text("keep", encoding="utf-8")

                with patch.object(
                    backup_module,
                    "_published_cleanup_checkpoint",
                    side_effect=relocate_at_first_unlink,
                ):
                    return original_cleanup(reservation)

            with patch.object(
                backup_module,
                "_assert_published_destination_path",
                side_effect=primary,
            ), patch.object(
                backup_module,
                "_remove_published_tree_if_identity",
                side_effect=racing_cleanup,
            ), self.assertRaisesRegex(RuntimeError, "force post-close cleanup") as raised:
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )

            self.assertTrue(raced)
            self.assertTrue(expected_files)
            self.assertEqual(racer_marker.read_text(encoding="utf-8"), "keep")
            self.assertEqual(
                {
                    item.relative_to(relocated): item.read_bytes()
                    for item in relocated.rglob("*")
                    if item.is_file()
                },
                expected_files,
            )
            self.assertIs(raised.exception, primary)
            self.assertTrue(
                any(
                    "post-close cleanup failed closed" in note
                    for note in getattr(raised.exception, "__notes__", ())
                )
            )

    def test_backup_fresh_ancestry_interruption_closes_descriptors_and_preserves_primary(self) -> None:
        from cval.evaluator import backup as backup_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            parent = root / "destination-tree/parent"
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(parent.parent, 0o700)
            os.chmod(parent, 0o700)
            target = parent / "backup"
            primary = KeyboardInterrupt("fresh ancestry transfer interrupted")
            original_validator = backup_module._assert_published_destination_path
            before = len(list(Path("/proc/self/fd").iterdir()))

            def interrupt_fresh_ancestry(reservation):
                with patch.object(
                    backup_module,
                    "_assert_exact_directory_metadata",
                    side_effect=primary,
                ):
                    return original_validator(reservation)

            with patch.object(
                backup_module,
                "_assert_published_destination_path",
                side_effect=interrupt_fresh_ancestry,
            ), self.assertRaises(KeyboardInterrupt) as raised:
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            after = len(list(Path("/proc/self/fd").iterdir()))
            self.assertIs(raised.exception, primary)
            self.assertEqual(after, before)
            self.assertFalse(target.exists())

    def test_backup_retained_ancestry_interruption_closes_fresh_walk_descriptors(self) -> None:
        from cval.evaluator import backup as backup_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parent = root / "destination-tree/parent"
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(parent.parent, 0o700)
            os.chmod(parent, 0o700)
            baseline = len(list(Path("/proc/self/fd").iterdir()))
            binding = backup_module._bind_destination_target(parent / "backup")
            try:
                retained = len(list(Path("/proc/self/fd").iterdir()))
                primary = SystemExit("destination ancestry transfer interrupted")
                with patch.object(
                    backup_module,
                    "_assert_exact_directory_metadata",
                    side_effect=primary,
                ), self.assertRaises(SystemExit) as raised:
                    binding.assert_binding()
                self.assertIs(raised.exception, primary)
                self.assertEqual(
                    len(list(Path("/proc/self/fd").iterdir())),
                    retained,
                )
            finally:
                binding.close()
            self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), baseline)

    def test_backup_destination_binding_identity_interrupt_closes_open_descriptor(self) -> None:
        from cval.evaluator import backup as backup_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parent = root / "destination-tree/parent"
            parent.mkdir(parents=True, mode=0o700)
            os.chmod(parent.parent, 0o700)
            os.chmod(parent, 0o700)
            before = len(list(Path("/proc/self/fd").iterdir()))
            primary = KeyboardInterrupt("ancestry identity capture interrupted")
            original_fstat = backup_module.os.fstat
            calls = 0

            def interrupt_after_child_open(descriptor):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise primary
                return original_fstat(descriptor)

            with patch.object(
                backup_module.os,
                "fstat",
                side_effect=interrupt_after_child_open,
            ), self.assertRaises(KeyboardInterrupt) as raised:
                backup_module._bind_destination_target(parent / "backup")
            self.assertIs(raised.exception, primary)
            self.assertEqual(len(list(Path("/proc/self/fd").iterdir())), before)

    def test_backup_nested_destination_replacement_survives_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            target = root / "backup"
            from cval.evaluator import backup as backup_module

            original_write = backup_module._write_destination_bytes
            parked = target / "validation_tests-original"
            raced = False

            def replace_nested(reservation, relative, value):
                nonlocal raced
                nested = target / "validation_tests"
                if relative == Path("inventory.json") and not raced:
                    raced = True
                    nested.rename(parked)
                    nested.mkdir(mode=0o700)
                    (nested / "racer-owned").write_text("keep", encoding="utf-8")
                return original_write(reservation, relative, value)

            with patch(
                "cval.evaluator.backup._write_destination_bytes",
                side_effect=replace_nested,
            ), self.assertRaises(RuntimeError):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )

            self.assertEqual(
                (target / "validation_tests/racer-owned").read_text(encoding="utf-8"),
                "keep",
            )
            self.assertTrue(parked.is_dir())

    def test_backup_rejects_rollback_journal_race_and_cleans_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, result_path = _smoke_config(source)
            target = root / "backup"
            journal = result_path.with_name(result_path.name + "-journal")
            from cval.evaluator import backup as backup_module

            original_snapshot = backup_module.immutable_sqlite_snapshot
            source_calls = 0

            @contextmanager
            def racing_snapshot(path, **kwargs):
                nonlocal source_calls
                if Path(path) == result_path:
                    source_calls += 1
                if Path(path) == result_path and source_calls == 2:
                    journal.touch()
                with original_snapshot(path, **kwargs) as value:
                    yield value

            with patch(
                "cval.evaluator.backup.immutable_sqlite_snapshot",
                racing_snapshot,
            ), self.assertRaisesRegex(RuntimeError, "journal|inventory changed"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=target,
                    apply=True,
                    confirmation="backup",
                )
            self.assertTrue(journal.exists())
            self.assertFalse(target.exists())

    def test_backup_rejects_inverse_live_root_overlaps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            protected_descendant = source / "configured-live"
            config = replace(
                config,
                runtime=replace(
                    config.runtime,
                    validation_root=str(protected_descendant),
                ),
            )
            with self.assertRaisesRegex(ValueError, "ancestors are rejected"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=root / "backup",
                )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source"
            source.mkdir()
            config, _registered, _result_path = _smoke_config(source)
            destination = root / "future-parent"
            config = replace(
                config,
                runtime=replace(
                    config.runtime,
                    validation_root=str(destination / "live-runtime"),
                ),
            )
            with self.assertRaisesRegex(ValueError, "outside"):
                backup_local_evaluator_state(
                    config,
                    source_root=source,
                    destination=destination,
                )
            self.assertFalse(destination.exists())


class EvaluatorManifestAndCliTests(unittest.TestCase):
    def test_dockerfile_is_offline_nonroot_and_matches_manifest_paths(self) -> None:
        dockerfile = (DEPLOY_ROOT / "Dockerfile").read_text(encoding="utf-8")
        lower = dockerfile.lower()
        self.assertEqual(
            sum(line.startswith("from ") for line in lower.splitlines()),
            2,
        )
        self.assertIn("distroless/python3-debian12:nonroot@sha256:" + "0" * 64, lower)
        self.assertIn("python:3.11-slim-bookworm@sha256:" + "0" * 64, lower)
        self.assertIn("--no-index", dockerfile)
        self.assertIn("ARG PYYAML_WHEEL_SHA256", dockerfile)
        self.assertIn("sha256sum -c -", dockerfile)
        self.assertIn("org.opencontainers.image.revision=\"${BUILD_COMMIT}\"", dockerfile)
        self.assertIn("USER 65532:65532", dockerfile)
        self.assertIn("/workspace/c-val/cval/evaluator/BUILD_COMMIT", dockerfile)
        self.assertIn("COPY config/cval.toml /app/config/cval.toml", dockerfile)
        self.assertIn("COPY validation-tests/ /catalog/validation-tests/", dockerfile)
        self.assertIn(
            "PYTHONPATH=/workspace/c-val python -m cval.evaluator.catalog",
            dockerfile,
        )
        exact_catalog_command = """RUN PYTHONPATH=/workspace/c-val python -m cval.evaluator.catalog \\
    --source-root /catalog \\
    --config /catalog/config/cval.toml \\
    --destination-root /workspace/c-val"""
        self.assertEqual(dockerfile.count(exact_catalog_command), 1)
        self.assertNotIn("COPY validation-tests/storage/test_config.toml", dockerfile)
        self.assertIn(
            "find /workspace/c-val /app -type f -exec chmod 0444 {} +",
            dockerfile,
        )
        runtime_stage = dockerfile.split(" AS evaluator", 1)[1].lower()
        for forbidden in (" run ", "pip", "git", "curl", "wget", "apt", "apk"):
            self.assertNotIn(forbidden, runtime_stage)
        self.assertNotIn("cval.validation.runner", runtime_stage)
        self.assertIn('entrypoint ["/usr/bin/python3", "-m", "cval.cli"]', runtime_stage)
        lock = (DEPLOY_ROOT / "requirements-evaluator.lock").read_text(encoding="utf-8")
        requirements = [line for line in lock.splitlines() if line and not line.startswith("#")]
        self.assertEqual(requirements, ["PyYAML==6.0.2"])
        package_data = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(
            package_data["tool"]["setuptools"]["package-data"]["cval.evaluator"],
            ["BUILD_COMMIT"],
        )
        base = yaml.safe_load((DEPLOY_ROOT / "base/cronjob.yaml").read_text(encoding="utf-8"))
        container = base["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        config_index = container["args"].index("--config") + 1
        self.assertEqual(container["args"][config_index], "/app/config/cval.toml")
        self.assertIn("/app/config/cval.toml", dockerfile)

    def test_manifests_are_restricted_suspended_and_have_distinct_modes(self) -> None:
        base = yaml.safe_load((DEPLOY_ROOT / "base/cronjob.yaml").read_text(encoding="utf-8"))
        service_account = yaml.safe_load(
            (DEPLOY_ROOT / "base/service-account.yaml").read_text(encoding="utf-8")
        )
        policy = yaml.safe_load(
            (DEPLOY_ROOT / "base/network-policy.yaml").read_text(encoding="utf-8")
        )
        apply_patch = yaml.safe_load(
            (DEPLOY_ROOT / "overlays/apply/apply-patch.yaml").read_text(encoding="utf-8")
        )
        shadow_patch = yaml.safe_load(
            (DEPLOY_ROOT / "overlays/shadow/shadow-patch.yaml").read_text(encoding="utf-8")
        )
        pod = base["spec"]["jobTemplate"]["spec"]["template"]["spec"]
        container = pod["containers"][0]
        config_data = tomllib.loads(
            (REPO_ROOT / "config/cval.toml").read_text(encoding="utf-8")
        )
        owner_uid = config_data["health_evaluator"]["state_owner_uid"]
        owner_gid = config_data["health_evaluator"]["state_owner_gid"]
        self.assertEqual(base["apiVersion"], "batch/v1")
        self.assertTrue(base["spec"]["suspend"])
        self.assertEqual(base["spec"]["concurrencyPolicy"], "Forbid")
        self.assertEqual(base["spec"]["jobTemplate"]["spec"]["backoffLimit"], 0)
        self.assertFalse(service_account["automountServiceAccountToken"])
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertTrue(pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(pod["securityContext"]["runAsUser"], owner_uid)
        self.assertEqual(pod["securityContext"]["runAsGroup"], owner_gid)
        self.assertEqual(pod["securityContext"]["seccompProfile"]["type"], "RuntimeDefault")
        self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
        self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])
        self.assertRegex(container["image"], r"@sha256:[0-9a-f]{64}$")
        self.assertTrue(container["image"].endswith("0" * 64))
        env = {item["name"]: item["value"] for item in container["env"]}
        self.assertEqual(env["CVAL_EXPECTED_COMMIT"], "0" * 40)
        self.assertEqual(env["CVAL_IMAGE_REF"], container["image"])
        self.assertNotIn("CVAL_EVALUATOR_WRITE_ENABLED", env)
        self.assertEqual(container["resources"]["requests"]["ephemeral-storage"], "64Mi")
        self.assertEqual(container["resources"]["limits"]["ephemeral-storage"], "256Mi")
        self.assertEqual(len(container["volumeMounts"]), 2)
        self.assertEqual(
            container["volumeMounts"][0]["mountPath"],
            "/data/continuous_validation/evaluator_state",
        )
        self.assertEqual(
            container["volumeMounts"][0]["subPath"],
            "continuous_validation/evaluator_state",
        )
        self.assertTrue(container["volumeMounts"][0]["readOnly"])
        self.assertTrue(
            pod["volumes"][0]["persistentVolumeClaim"]["readOnly"]
        )
        self.assertNotIn("ports", container)
        rendered_base = json.dumps(base).lower()
        self.assertNotIn("hostpath", rendered_base)
        self.assertNotIn("nvidia.com/gpu", rendered_base)
        self.assertNotIn("rdma", rendered_base)
        for forbidden in ("kubectl", "git clone", "git checkout", "pip install", "curl", "wget"):
            self.assertNotIn(forbidden, rendered_base)
        self.assertEqual(policy["spec"]["policyTypes"], ["Ingress", "Egress"])
        self.assertEqual(policy["spec"]["ingress"], [])
        self.assertEqual(policy["spec"]["egress"], [])
        self.assertTrue(apply_patch["spec"]["suspend"])
        apply_args = apply_patch["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertEqual(apply_args[-3:], ["--apply", "--confirm", "evaluate"])
        self.assertIn("--write-enabled", apply_args)
        apply_container = apply_patch["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        self.assertFalse(apply_container["volumeMounts"][0]["readOnly"])
        self.assertNotIn("--apply", shadow_patch["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["args"])
        self.assertTrue(
            shadow_patch["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            ["volumes"][0]["persistentVolumeClaim"]["readOnly"]
        )
        self.assertFalse(any(DEPLOY_ROOT.rglob("*RoleBinding*.yaml")))
        dockerfile = (DEPLOY_ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn(f"USER {owner_uid}:{owner_gid}", dockerfile)
        validation_job = yaml.safe_load(
            (REPO_ROOT / "ymls/specific-node-job.yml").read_text(encoding="utf-8")
        )
        producer_pod = validation_job["spec"]["tasks"][0]["template"]["spec"]
        producer_container = producer_pod["containers"][0]
        self.assertNotIn("runAsUser", producer_pod.get("securityContext", {}))
        self.assertNotIn("runAsGroup", producer_pod.get("securityContext", {}))
        self.assertNotIn("runAsUser", producer_container.get("securityContext", {}))
        self.assertNotIn("runAsGroup", producer_container.get("securityContext", {}))
        rollout = (REPO_ROOT / "docs/u11-evaluator-rollout.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("validation workload execution UID/GID is unspecified", rollout)
        self.assertIn("NFSv4", rollout)
        self.assertIn("U7 activation is blocked", rollout)

        rendered = {}
        for variant in ("shadow", "apply"):
            completed = subprocess.run(
                ["kubectl", "kustomize", str(DEPLOY_ROOT / "overlays" / variant)],
                check=True,
                capture_output=True,
                text=True,
            )
            documents = [item for item in yaml.safe_load_all(completed.stdout) if item]
            cronjob = next(item for item in documents if item["kind"] == "CronJob")
            rendered_account = next(
                item for item in documents if item["kind"] == "ServiceAccount"
            )
            rendered_policy = next(
                item for item in documents if item["kind"] == "NetworkPolicy"
            )
            rendered[variant] = cronjob
            rendered_pod = cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            rendered_container = rendered_pod["containers"][0]
            rendered_env = {item["name"]: item["value"] for item in rendered_container["env"]}
            self.assertTrue(cronjob["spec"]["suspend"])
            self.assertFalse(rendered_account["automountServiceAccountToken"])
            self.assertFalse(rendered_pod["automountServiceAccountToken"])
            self.assertTrue(rendered_pod["securityContext"]["runAsNonRoot"])
            self.assertEqual(
                rendered_pod["securityContext"]["seccompProfile"]["type"],
                "RuntimeDefault",
            )
            self.assertFalse(rendered_container["securityContext"]["allowPrivilegeEscalation"])
            self.assertTrue(rendered_container["securityContext"]["readOnlyRootFilesystem"])
            self.assertEqual(rendered_container["securityContext"]["capabilities"]["drop"], ["ALL"])
            self.assertEqual(rendered_policy["spec"]["ingress"], [])
            self.assertEqual(rendered_policy["spec"]["egress"], [])
            self.assertEqual(rendered_env["CVAL_IMAGE_REF"], rendered_container["image"])
            self.assertNotIn("CVAL_EVALUATOR_WRITE_ENABLED", rendered_env)
            self.assertIn("ephemeral-storage", rendered_container["resources"]["requests"])
            self.assertIn("ephemeral-storage", rendered_container["resources"]["limits"])
            state_mounts = [
                mount
                for mount in rendered_container["volumeMounts"]
                if mount["name"] == "evaluator-state"
            ]
            self.assertEqual(len(state_mounts), 1)
            self.assertEqual(
                state_mounts[0]["mountPath"],
                "/data/continuous_validation/evaluator_state",
            )
            self.assertEqual(
                state_mounts[0]["subPath"],
                "continuous_validation/evaluator_state",
            )
            self.assertFalse(
                any(
                    mount["mountPath"] == "/data/continuous_validation"
                    for mount in rendered_container["volumeMounts"]
                )
            )
        shadow_container = rendered["shadow"]["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        apply_container = rendered["apply"]["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        self.assertNotIn("--apply", shadow_container["args"])
        self.assertTrue(shadow_container["volumeMounts"][0]["readOnly"])
        self.assertEqual(apply_container["args"][-3:], ["--apply", "--confirm", "evaluate"])
        self.assertIn("--write-enabled", apply_container["args"])
        self.assertFalse(apply_container["volumeMounts"][0]["readOnly"])

    def test_internal_cli_emits_one_json_value_on_error(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["evaluator-parity"])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 2)
        self.assertFalse(payload["ok"])
        self.assertEqual(stderr.getvalue(), "")

    def test_internal_parity_cli_rejects_strict_json_and_invalid_timestamps(self) -> None:
        valid = {
            "node": "node-a",
            "test_id": "storage",
            "run_id": "run-a",
            "class_code": 1,
            "class_name": "Nominal",
            "dnr_reason": None,
            "baseline_id": "hb1:" + "a" * 64,
        }
        encoded = json.dumps([valid])
        cases = (
            (
                "duplicate-key",
                encoded.replace(
                    '"node": "node-a"',
                    '"node": "node-a", "node": "node-b"',
                ),
                "duplicate object key",
            ),
            (
                "nan",
                encoded.replace('"class_code": 1', '"class_code": NaN'),
                "non-standard numeric constant: NaN",
            ),
            (
                "infinity",
                encoded.replace('"class_code": 1', '"class_code": Infinity'),
                "non-standard numeric constant: Infinity",
            ),
            ("object-wrapper", json.dumps({"records": [valid]}), "exactly an array"),
            (
                "huge-timestamp",
                json.dumps([valid | {"classified_at": SQLITE_SIGNED_INT64_MAX + 1}]),
                "SQLite signed 64-bit maximum",
            ),
            (
                "negative-timestamp",
                json.dumps([valid | {"classified_at": -1}]),
                "exactly non-negative int",
            ),
            (
                "boolean-timestamp",
                json.dumps([valid | {"classified_at": True}]),
                "exactly non-negative int",
            ),
        )
        for label, raw, error in cases:
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "u8.json"
                path.write_text(raw, encoding="utf-8")
                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = main(["evaluator-parity", "--u8-json", str(path)])
                payload = json.loads(stdout.getvalue())
                self.assertEqual(code, 2)
                self.assertFalse(payload["ok"])
                self.assertIn(error, payload["error"])
                self.assertEqual(stderr.getvalue(), "")

    def test_internal_cli_argument_errors_are_strict_json(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["evaluator-backup"])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 2)
        self.assertFalse(payload["ok"])
        self.assertIn("required", payload["error"])
        self.assertEqual(stderr.getvalue(), "")

    def test_internal_cli_contains_system_exit_as_one_redacted_json_value(self) -> None:
        def noisy_system_exit(*_args, **_kwargs):
            print("token=stdout-secret")
            print("token=stderr-secret", file=os.sys.stderr)
            raise SystemExit("token=exception-secret")

        cases = (
            (
                "cval.evaluator.preflight.run_deployment_preflight",
                ["evaluator-preflight"],
                "evaluator preflight failed (SystemExit)",
            ),
            (
                "cval.evaluator.parity.build_shadow_parity_report",
                ["evaluator-parity"],
                "evaluator parity failed (SystemExit)",
            ),
            (
                "cval.evaluator.backup.backup_local_evaluator_state",
                [
                    "evaluator-backup",
                    "--source-root",
                    "/copied-source",
                    "--destination",
                    "/copied-backup",
                ],
                "evaluator backup failed (SystemExit)",
            ),
            (
                "cval.evaluator.service.run_evaluator_service",
                ["evaluator-service"],
                "evaluator service failed (SystemExit)",
            ),
        )
        for target, argv, expected_error in cases:
            with self.subTest(command=argv[0]):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with patch(target, side_effect=noisy_system_exit), redirect_stdout(
                    stdout
                ), redirect_stderr(stderr):
                    code = main(argv)
                payload = json.loads(stdout.getvalue())
                self.assertEqual(code, 2)
                self.assertEqual(payload, {"ok": False, "error": expected_error})
                self.assertNotIn("secret", stdout.getvalue())
                self.assertEqual(stderr.getvalue(), "")

    def test_internal_cli_contains_other_baseexception_but_not_keyboard_interrupt(self) -> None:
        fatal = BaseException("token=secret")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch(
            "cval.evaluator.preflight.run_deployment_preflight",
            side_effect=fatal,
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(["evaluator-preflight"])
        self.assertEqual(code, 2)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "ok": False,
                "error": "evaluator preflight failed (BaseException)",
            },
        )
        self.assertNotIn("secret", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

        for target, argv in (
            (
                "cval.evaluator.preflight.run_deployment_preflight",
                ["evaluator-preflight"],
            ),
            (
                "cval.evaluator.parity.build_shadow_parity_report",
                ["evaluator-parity"],
            ),
            (
                "cval.evaluator.backup.backup_local_evaluator_state",
                [
                    "evaluator-backup",
                    "--source-root",
                    "/copied-source",
                    "--destination",
                    "/copied-backup",
                ],
            ),
            (
                "cval.evaluator.service.run_evaluator_service",
                ["evaluator-service"],
            ),
        ):
            with self.subTest(interrupt=argv[0]), patch(
                target,
                side_effect=KeyboardInterrupt(),
            ), self.assertRaises(KeyboardInterrupt):
                main(argv)


if __name__ == "__main__":
    unittest.main()
