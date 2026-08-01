from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cval.config import encode_config_snapshot, load_config
from cval.evaluator.state import bind_state_target, state_test_lock
from cval.storage.per_test_results import (
    PerTestResultRecord,
    resolve_test_results_db_path,
    write_per_test_result,
)
from cval.storage.ingest import NCCL_IB_PORT_COLUMNS
from cval.validation.ingestion import (
    ingest_test_results_file,
    preflight_test_results_file,
)
from cval.validation.registry import load_test_registry
from cval.validation.runner import run_validation_tests
from cval.validation.runtime import effective_config_digest
from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    validation_result_v2_digest,
)


def _ingest(result_path: Path, config):
    result = load_validation_result(result_path)
    assert isinstance(result, ValidationResultV2)
    return ingest_test_results_file(
        result_path,
        config=config,
        result_digest=validation_result_v2_digest(result),
        config_snapshot_b64=encode_config_snapshot(config),
    )


class ModularPerTestIngestionTests(unittest.TestCase):
    def test_gate_off_preflight_creates_no_state_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=False)
            state_root = Path(config.health_evaluator.state_root)
            result_path = self._write_builtin_result(root, config)
            result = load_validation_result(result_path)
            assert isinstance(result, ValidationResultV2)

            run_id = preflight_test_results_file(
                result_path,
                config=config,
                result_digest=validation_result_v2_digest(result),
                config_snapshot_b64=encode_config_snapshot(config),
            )

            self.assertEqual(run_id, result.run_id)
            self.assertFalse(state_root.exists())

    def test_gate_on_wrong_owner_fails_before_compatibility_target_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            compatibility = root / "compatibility.db"
            compatibility.write_bytes(b"unchanged")
            config = replace(
                config,
                storage=replace(config.storage, validation_db_path=str(compatibility)),
                health_evaluator=replace(
                    config.health_evaluator,
                    state_owner_uid=os.geteuid() + 1,
                ),
            )
            result_path = self._write_builtin_result(root, config)
            result = load_validation_result(result_path)
            assert isinstance(result, ValidationResultV2)

            with self.assertRaisesRegex(PermissionError, "process owner mismatch"):
                preflight_test_results_file(
                    result_path,
                    config=config,
                    result_digest=validation_result_v2_digest(result),
                    config_snapshot_b64=encode_config_snapshot(config),
                )

            self.assertEqual(compatibility.read_bytes(), b"unchanged")

    def test_secure_u7_first_creation_modes_and_identity_binding(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            registered = config.tests.registry.require("storage")
            path = resolve_test_results_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            record = PerTestResultRecord(
                run_id="identity-race",
                test_id="storage",
                node="node-a",
                run_timestamp=1,
                started_timestamp=1,
                completed_timestamp=2,
                status="fail",
                exit_code=1,
                image_name="image",
                pytorch_version="2.8",
                cuda_version="12.9",
                test_config_digest="sha256:" + "1" * 64,
                result_path=str(root / "result.json"),
                summary_path="",
                artifacts_path=str(root / "artifacts"),
                raw_result_json=(
                    '{"schema_version":"cval.test-result.v1",'
                    '"test_id":"storage"}'
                ),
                result_digest="sha256:" + "2" * 64,
            )
            with state_test_lock(config, path) as lock_guard, bind_state_target(
                config,
                path,
                create=True,
                allow_missing=False,
                writable=True,
                require_writable=True,
            ) as binding:
                identity = binding.sqlite_identity
                assert identity is not None
                write_per_test_result(
                    record,
                    db_path=path,
                    expected_identity=identity,
                    state_guard=lambda: (
                        lock_guard(),
                        binding.assert_path_binding(),
                    ),
                )
            current = Path(config.health_evaluator.state_root)
            for part in path.parent.relative_to(current).parts:
                current = current / part
                self.assertEqual(current.stat().st_mode & 0o777, 0o700)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.stat().st_nlink, 1)

            replacement = path.with_name("replacement.db")
            replacement.touch(mode=0o600)
            os.chmod(replacement, 0o600)
            os.replace(replacement, path)
            with self.assertRaisesRegex(RuntimeError, "path/device/inode changed"):
                write_per_test_result(
                    record,
                    db_path=path,
                    expected_identity=identity,
                )

    def test_u7_paths_use_state_root_while_result_evidence_stays_shared(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            report = _ingest(result_path, config)
            state_root = Path(config.health_evaluator.state_root)

            self.assertTrue(report.ok)
            self.assertTrue(result_path.is_relative_to(Path(config.runtime.validation_root)))
            self.assertFalse(result_path.is_relative_to(state_root))
            self.assertTrue(
                (state_root / "validation_tests/storage/storage_results.db").is_file()
            )

    def test_direct_ingestion_rejects_missing_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            result = load_validation_result(result_path)
            assert isinstance(result, ValidationResultV2)

            with self.assertRaisesRegex(ValueError, "snapshot"):
                ingest_test_results_file(
                    result_path,
                    config=config,
                    result_digest=validation_result_v2_digest(result),
                    config_snapshot_b64="",
                )

            self.assertEqual(list(root.glob("validation_tests/**/*.db")), [])

    def test_preflight_rejects_each_evaluator_state_snapshot_field_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            result = load_validation_result(result_path)
            assert isinstance(result, ValidationResultV2)
            variants = {
                "state_root": str(root / "different-state"),
                "state_owner_uid": config.health_evaluator.state_owner_uid + 1,
                "state_owner_gid": config.health_evaluator.state_owner_gid + 1,
                "validation_root_mode": "0710",
            }
            for field, value in variants.items():
                with self.subTest(field=field):
                    altered = replace(
                        config,
                        health_evaluator=replace(
                            config.health_evaluator,
                            **{field: value},
                        ),
                    )
                    with self.assertRaisesRegex(ValueError, "immutable snapshot"):
                        preflight_test_results_file(
                            result_path,
                            config=config,
                            result_digest=validation_result_v2_digest(result),
                            config_snapshot_b64=encode_config_snapshot(altered),
                        )

    def test_synthetic_fourth_test_gets_raw_persistence_without_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "repo"
            test_dir = repo / "validation-tests/smoke"
            test_dir.mkdir(parents=True)
            (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (test_dir / "run-test.sh").write_text(
                "#!/bin/bash\nexit 0\n", encoding="utf-8"
            )
            (test_dir / "test_config.toml").write_text(
                '''
schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 40
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/smoke/smoke_results.db"
''',
                encoding="utf-8",
            )
            registry = load_test_registry(
                {
                    "smoke": {
                        "enabled": True,
                        "config_path": "validation-tests/smoke/test_config.toml",
                    }
                },
                repo_root=repo,
                include_defaults=False,
            )
            base = self._config(root, enabled=True)
            config = replace(base, tests=replace(base.tests, registry=registry))
            environment = os.environ | {
                "CVAL_NODE": "node-a",
                "CVAL_TIMESTAMP": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_VALIDATION_ROOT": str(root),
                "CVAL_CONFIG_DIGEST": effective_config_digest(config),
            }

            def passed_process(*_args, **_kwargs):
                from cval.validation.execution import ProcessOutcome

                return ProcessOutcome(0, False, 1)

            with patch(
                "cval.validation.execution.RunLogger.stream_process",
                new=passed_process,
            ):
                run_validation_tests(
                    config=config,
                    registry=registry,
                    environ=environment,
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"

            report = _ingest(result_path, config)
            with closing(
                sqlite3.connect(root / "evaluator_state/validation_tests/smoke/smoke_results.db")
            ) as connection:
                row = connection.execute(
                    "SELECT run_id, test_id, status FROM test_results"
                ).fetchone()
                metric_receipts = connection.execute(
                    "SELECT COUNT(*) FROM metric_ingestion_receipts"
                ).fetchone()[0]

        self.assertTrue(report.ok)
        self.assertEqual(len(report.outcomes), 1)
        self.assertFalse(report.outcomes[0].adapter_called)
        self.assertEqual(row, ("node-a-123", "smoke", "pass"))
        self.assertEqual(metric_receipts, 0)

    def test_plugin_free_test_rejects_unmanifested_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "repo"
            test_dir = repo / "validation-tests/smoke"
            test_dir.mkdir(parents=True)
            (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (test_dir / "run-test.sh").write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (test_dir / "test_config.toml").write_text(
                '''
schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 40
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/smoke/smoke_results.db"
''',
                encoding="utf-8",
            )
            registry = load_test_registry(
                {"smoke": {"enabled": True, "config_path": "validation-tests/smoke/test_config.toml"}},
                repo_root=repo,
                include_defaults=False,
            )
            base = self._config(root, enabled=True)
            config = replace(base, tests=replace(base.tests, registry=registry))
            environment = os.environ | {
                "CVAL_NODE": "node-a",
                "CVAL_TIMESTAMP": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_VALIDATION_ROOT": str(root),
                "CVAL_CONFIG_DIGEST": effective_config_digest(config),
            }
            from cval.validation.execution import ProcessOutcome

            with patch(
                "cval.validation.execution.RunLogger.stream_process",
                return_value=ProcessOutcome(0, False, 1),
            ):
                run_validation_tests(
                    config=config,
                    registry=registry,
                    environ=environment,
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"
            self.assertTrue(_ingest(result_path, config).ok)
            db_path = root / "evaluator_state/validation_tests/smoke/smoke_results.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("CREATE VIEW unexpected AS SELECT * FROM test_results")
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "Database views"):
                _ingest(result_path, config)

    def test_enabled_dispatch_preserves_builtin_metrics_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)

            first = _ingest(result_path, config)
            second = _ingest(result_path, config)

            self.assertTrue(first.ok)
            self.assertTrue(second.ok)
            self.assertEqual(
                [outcome.test_id for outcome in first.outcomes],
                ["storage", "nccl", "dltest"],
            )
            self.assertTrue(all(outcome.adapter_called for outcome in first.outcomes))
            self.assertTrue(all(outcome.receipt is not None for outcome in first.outcomes))
            self.assertTrue(
                all(
                    outcome.receipt is not None
                    and outcome.receipt.inserted_count > 0
                    and outcome.receipt.message == "idempotent retry"
                    for outcome in second.outcomes
                )
            )

            storage_db = root / "evaluator_state/validation_tests/storage/storage_results.db"
            with closing(sqlite3.connect(storage_db)) as connection:
                common = connection.execute(
                    "SELECT status, image_name, pytorch_version, cuda_version, "
                    "health_class_name, health_class_numerical FROM test_results"
                ).fetchone()
                storage = connection.execute(
                    "SELECT randread_iops, randread_bw, run_id "
                    "FROM storage_performance"
                ).fetchone()
                storage_receipts = connection.execute(
                    "SELECT COUNT(*) FROM metric_ingestion_receipts"
                ).fetchone()[0]

            nccl_db = root / "evaluator_state/validation_tests/nccl/nccl_results.db"
            with closing(sqlite3.connect(nccl_db)) as connection:
                nccl = connection.execute(
                    "SELECT iterations, BUS_BW, LATENCY, mlx5_0, mlx5_13, run_id "
                    "FROM IB_HEALTH"
                ).fetchone()
                views = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='view'"
                    )
                }
                columns = [
                    row[1]
                    for row in connection.execute("PRAGMA table_info(IB_HEALTH)")
                ]

            dl_db = root / "evaluator_state/validation_tests/dltest/dltest_results.db"
            with closing(sqlite3.connect(dl_db)) as connection:
                dl_common = connection.execute(
                    "SELECT status, health_class_name, evaluated_at FROM test_results"
                ).fetchone()
                dl_counts = {
                    table: connection.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                    for table in (
                        "numerical_correctness",
                        "compute_performance",
                        "collective_performance",
                        "overlap_performance",
                    )
                }

        self.assertEqual(common, ("pass", "image", "2.8", "12.9", None, None))
        self.assertEqual(storage, (15.0, 27.0, "node-a-123"))
        self.assertEqual(storage_receipts, 1)
        self.assertEqual(nccl, (20, 44.5, 628.2, 46.1, 46.3, "node-a-123"))
        self.assertTrue(set(NCCL_IB_PORT_COLUMNS).issubset(columns))
        self.assertEqual(views, {"LATEST_NODE_STATUS", "NODE_RANKING"})
        self.assertEqual(dl_common, ("pass", None, None))
        self.assertTrue(all(count > 0 for count in dl_counts.values()))

    def test_reserved_uri_root_supports_two_successive_ingestions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "validation %?# root"
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)

            first = _ingest(result_path, config)
            second = _ingest(result_path, config)
            counts = {}
            for test_id in ("storage", "nccl", "dltest"):
                db_path = root / f"evaluator_state/validation_tests/{test_id}/{test_id}_results.db"
                with closing(sqlite3.connect(db_path)) as connection:
                    counts[test_id] = connection.execute(
                        "SELECT COUNT(*) FROM test_results"
                    ).fetchone()[0]

        self.assertTrue(first.ok)
        self.assertTrue(second.ok)
        self.assertEqual(counts, {"storage": 1, "nccl": 1, "dltest": 1})
        self.assertTrue(
            all(
                outcome.receipt is not None
                and outcome.receipt.message == "idempotent retry"
                for outcome in second.outcomes
            )
        )

    def test_builtin_common_rows_store_canonical_health_combination_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)

            report = _ingest(result_path, config)
            keys = {}
            for test_id in ("storage", "nccl", "dltest"):
                with closing(
                    sqlite3.connect(
                        root / f"evaluator_state/validation_tests/{test_id}/{test_id}_results.db"
                    )
                ) as connection:
                    keys[test_id] = connection.execute(
                        "SELECT combination_key FROM test_results"
                    ).fetchone()[0]

            self.assertTrue(report.ok)
            self.assertTrue(
                all(
                    len(value) == 71 and value.startswith("sha256:")
                    for value in keys.values()
                )
            )
            self.assertEqual(len(set(keys.values())), 3)

    def test_adapter_failure_isolated_and_raw_status_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            nccl_summary = (
                root
                / "validation_tests/nccl/runs/node-a/node-a-123/summary.json"
            )
            nccl_summary.write_text("{not-json}", encoding="utf-8")

            report = _ingest(result_path, config)

            by_test = {outcome.test_id: outcome for outcome in report.outcomes}
            self.assertFalse(report.ok)
            self.assertFalse(by_test["storage"].error)
            self.assertTrue(by_test["nccl"].error)
            self.assertFalse(by_test["dltest"].error)
            with closing(
                sqlite3.connect(root / "evaluator_state/validation_tests/nccl/nccl_results.db")
            ) as connection:
                raw = connection.execute(
                    "SELECT status, health_class_name FROM test_results"
                ).fetchone()
                has_metrics = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='IB_HEALTH'"
                ).fetchone()
            self.assertEqual(raw, ("pass", None))
            self.assertIsNone(has_metrics)
            self.assertTrue(
                (root / "evaluator_state/validation_tests/storage/storage_results.db").is_file()
            )
            self.assertTrue(
                (root / "evaluator_state/validation_tests/dltest/dltest_results.db").is_file()
            )

    def test_adapter_cannot_recover_and_commit_parent_sqlite_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "repo"
            test_dir = repo / "validation-tests/escape"
            test_dir.mkdir(parents=True)
            (test_dir / "setup.sh").write_text(
                "#!/bin/bash\nexit 0\n",
                encoding="utf-8",
            )
            (test_dir / "run-test.sh").write_text(
                "#!/bin/bash\nexit 0\n",
                encoding="utf-8",
            )
            (test_dir / "test_config.toml").write_text(
                '''
schema_version = "cval.test.v1"
[test]
id = "escape"
display_name = "Escape"
order = 40
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/escape/escape_results.db"
[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
capabilities = ["config", "ingest"]
''',
                encoding="utf-8",
            )
            (test_dir / "plugin.py").write_text(
                '''
from cval.storage.per_test_results import metric_ingestion_transaction

CVAL_PLUGIN_API = "cval.plugin.v1"

class EscapePlugin:
    plugin_id = "escape"
    capabilities = frozenset({"config", "ingest"})

    def validate_config(self, _definition):
        return ()

    def validate_schema(self, connection, _allow_missing):
        if connection.__class__.__module__ == "sqlite3":
            raise RuntimeError("schema preflight received raw parent connection")
        method_globals = connection.execute.__func__.__globals__
        if method_globals.get("_ADAPTER_CONNECTIONS"):
            raise RuntimeError("schema preflight recovered parent connection map")
        return False

    def ingest(self, context):
        with metric_ingestion_transaction(
            context.result_db_path,
            test_id=self.plugin_id,
            adapter_schema_version=1,
            validate_adapter_schema=self.validate_schema,
        ) as connection:
            connection.execute("CREATE TABLE escape_marker(value TEXT)")
            method_globals = connection.execute.__func__.__globals__
            leaked = method_globals.get("_ADAPTER_CONNECTIONS", {})
            if leaked:
                raw_connection = next(iter(leaked.values()))
                raw_connection.set_authorizer(None)
                raw_connection.commit()
                raise RuntimeError("escaped raw parent connection")
            raise RuntimeError("raw parent connection unavailable")

PLUGIN = EscapePlugin()
''',
                encoding="utf-8",
            )
            registry = load_test_registry(
                {
                    "escape": {
                        "enabled": True,
                        "config_path": "validation-tests/escape/test_config.toml",
                    }
                },
                repo_root=repo,
                include_defaults=False,
            )
            base = self._config(root, enabled=True)
            config = replace(base, tests=replace(base.tests, registry=registry))
            environment = os.environ | {
                "CVAL_NODE": "node-a",
                "CVAL_TIMESTAMP": "123",
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_VALIDATION_ROOT": str(root),
                "CVAL_CONFIG_DIGEST": effective_config_digest(config),
            }
            from cval.validation.execution import ProcessOutcome

            with patch(
                "cval.validation.execution.RunLogger.stream_process",
                return_value=ProcessOutcome(0, False, 1),
            ):
                run_validation_tests(
                    config=config,
                    registry=registry,
                    environ=environment,
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"

            report = _ingest(result_path, config)
            retry = _ingest(result_path, config)
            db_path = root / "evaluator_state/validation_tests/escape/escape_results.db"
            with closing(sqlite3.connect(db_path)) as connection:
                marker = connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='escape_marker'"
                ).fetchone()
                raw_status = connection.execute(
                    "SELECT status FROM test_results WHERE run_id='node-a-123'"
                ).fetchone()

            self.assertFalse(report.ok)
            self.assertFalse(retry.ok)
            self.assertIn(
                "raw parent connection unavailable",
                report.outcomes[0].error,
            )
            self.assertIn(
                "raw parent connection unavailable",
                retry.outcomes[0].error,
            )
            self.assertIsNone(marker)
            self.assertEqual(raw_status, ("pass",))

    def test_persisted_metric_row_update_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            first = _ingest(result_path, config)
            self.assertTrue(first.ok)
            storage_db = root / "evaluator_state/validation_tests/storage/storage_results.db"
            with closing(sqlite3.connect(storage_db)) as connection, self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute(
                    "UPDATE storage_performance SET randread_iops=999999 "
                    "WHERE run_id='node-a-123'"
                )

    def test_retry_rejects_envelope_only_result_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            self.assertTrue(_ingest(result_path, config).ok)
            value = json.loads(result_path.read_text(encoding="utf-8"))
            value["generated_at"] = "2026-07-28T16:00:02Z"
            result_path.write_text(json.dumps(value), encoding="utf-8")

            retry = _ingest(result_path, config)

            self.assertFalse(retry.ok)
            self.assertTrue(
                all("different raw evidence" in outcome.error for outcome in retry.outcomes)
            )

    def test_durable_receipt_update_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            self.assertTrue(_ingest(result_path, config).ok)
            storage_db = root / "evaluator_state/validation_tests/storage/storage_results.db"
            with closing(sqlite3.connect(storage_db)) as connection, self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute(
                    "UPDATE metric_ingestion_receipts SET "
                    "inserted_count=999, metric_names_json='[\"bogus\"]' "
                    "WHERE run_id='node-a-123'"
                )

    def test_insert_or_replace_cannot_rebind_canonical_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            self.assertTrue(_ingest(result_path, config).ok)
            storage_db = root / "evaluator_state/validation_tests/storage/storage_results.db"
            statements = (
                "INSERT OR REPLACE INTO test_results "
                "SELECT result_id, run_id, test_id, node, run_timestamp, "
                "started_timestamp, completed_timestamp, status, exit_code, "
                "image_name, pytorch_version, cuda_version, test_config_digest, "
                "combination_key, result_path, summary_path, artifacts_path, "
                "raw_result_json, result_digest, health_class_name, "
                "health_class_numerical, health_baseline_id, evaluated_at, "
                "created_at, updated_at FROM test_results WHERE run_id='node-a-123'",
                "INSERT OR REPLACE INTO metric_ingestion_receipts "
                "SELECT run_id, test_id, adapter_api_version, evidence_digest, "
                "inserted_count, updated_count, metric_names_json, created_at "
                "FROM metric_ingestion_receipts WHERE run_id='node-a-123'",
                "INSERT OR REPLACE INTO storage_performance "
                "SELECT node, timestamp, image_name, "
                "iodepth_read_1file_iops, iodepth_read_1file_bw, "
                "iodepth_write_1file_iops, iodepth_write_1file_bw, "
                "numjobs_read_nfiles_iops, numjobs_read_nfiles_bw, "
                "numjobs_write_nfiles_iops, numjobs_write_nfiles_bw, "
                "777, randread_bw, randwrite_iops, randwrite_bw, run_id "
                "FROM storage_performance WHERE run_id='node-a-123'",
                "INSERT OR REPLACE INTO storage_performance "
                "(rowid, node, timestamp, image_name, "
                "iodepth_read_1file_iops, iodepth_read_1file_bw, "
                "iodepth_write_1file_iops, iodepth_write_1file_bw, "
                "numjobs_read_nfiles_iops, numjobs_read_nfiles_bw, "
                "numjobs_write_nfiles_iops, numjobs_write_nfiles_bw, "
                "randread_iops, randread_bw, randwrite_iops, randwrite_bw, run_id) "
                "SELECT rowid, node || '-forged', timestamp + 1, image_name, "
                "iodepth_read_1file_iops, iodepth_read_1file_bw, "
                "iodepth_write_1file_iops, iodepth_write_1file_bw, "
                "numjobs_read_nfiles_iops, numjobs_read_nfiles_bw, "
                "numjobs_write_nfiles_iops, numjobs_write_nfiles_bw, "
                "777, randread_bw, randwrite_iops, randwrite_bw, 'forged-run' "
                "FROM storage_performance WHERE run_id='node-a-123'",
            )
            with closing(sqlite3.connect(storage_db)) as connection:
                for statement in statements:
                    with self.subTest(statement=statement), self.assertRaises(
                        sqlite3.IntegrityError
                    ):
                        connection.execute(statement)

    def test_retry_rejects_weakened_partial_unique_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            self.assertTrue(_ingest(result_path, config).ok)
            storage_db = root / "evaluator_state/validation_tests/storage/storage_results.db"
            with closing(sqlite3.connect(storage_db)) as connection:
                connection.execute("DROP INDEX idx_storage_performance_run_id")
                connection.execute(
                    "CREATE UNIQUE INDEX idx_storage_performance_run_id "
                    "ON storage_performance(run_id) "
                    "WHERE run_id IS NOT NULL AND 0"
                )
                connection.commit()

            with self.assertRaisesRegex(
                RuntimeError,
                "Index idx_storage_performance_run_id",
            ):
                _ingest(result_path, config)

    def test_retry_rejects_replaced_nccl_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            self.assertTrue(_ingest(result_path, config).ok)
            nccl_db = root / "evaluator_state/validation_tests/nccl/nccl_results.db"
            with closing(sqlite3.connect(nccl_db)) as connection:
                connection.execute("DROP VIEW LATEST_NODE_STATUS")
                connection.execute(
                    "CREATE VIEW LATEST_NODE_STATUS AS SELECT * FROM IB_HEALTH"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "View LATEST_NODE_STATUS"):
                _ingest(result_path, config)

    def test_retry_rejects_unmanifested_adapter_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            self.assertTrue(_ingest(result_path, config).ok)
            storage_db = root / "evaluator_state/validation_tests/storage/storage_results.db"
            with closing(sqlite3.connect(storage_db)) as connection:
                connection.execute(
                    "CREATE TRIGGER delete_storage AFTER INSERT ON storage_performance "
                    "BEGIN DELETE FROM storage_performance WHERE run_id=NEW.run_id; END"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "trigger manifest"):
                _ingest(result_path, config)

    def test_failed_test_gets_common_row_without_metric_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(
                root,
                config,
                statuses={"storage": "fail", "nccl": "pass", "dltest": "pass"},
            )

            report = _ingest(result_path, config)

            storage = next(
                outcome for outcome in report.outcomes if outcome.test_id == "storage"
            )
            self.assertEqual(storage.status, "fail")
            self.assertFalse(storage.adapter_called)
            with closing(
                sqlite3.connect(root / "evaluator_state/validation_tests/storage/storage_results.db")
            ) as connection:
                row = connection.execute(
                    "SELECT status, health_class_name FROM test_results"
                ).fetchone()
                metric_table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='storage_performance'"
                ).fetchone()
            self.assertEqual(row, ("fail", None))
            self.assertIsNone(metric_table)

    def test_nonpass_run_preserves_existing_adapter_schema_without_ingesting(self) -> None:
        for terminal_status in ("fail", "incomplete"):
            with self.subTest(status=terminal_status), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config = self._config(root, enabled=True)
                passing_path = self._write_builtin_result(
                    root,
                    config,
                    run_id="node-a-123",
                    timestamp=123,
                )
                self.assertTrue(_ingest(passing_path, config).ok)
                nonpass_path = self._write_builtin_result(
                    root,
                    config,
                    run_id="node-a-124",
                    timestamp=124,
                    statuses={"storage": "fail", "nccl": "pass", "dltest": "pass"},
                )
                if terminal_status == "incomplete":
                    global_payload = json.loads(
                        nonpass_path.read_text(encoding="utf-8")
                    )
                    test_payload = global_payload["tests"]["storage"]
                    test_payload["status"] = "incomplete"
                    test_payload["phase"] = "interrupted"
                    global_payload["overall"] = "incomplete"
                    nonpass_path.write_text(
                        json.dumps(global_payload),
                        encoding="utf-8",
                    )
                    raw_path = Path(test_payload["result"])
                    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
                    raw_payload["status"] = "incomplete"
                    raw_payload["phase"] = "interrupted"
                    raw_path.write_text(json.dumps(raw_payload), encoding="utf-8")

                report = _ingest(nonpass_path, config)
                storage = next(
                    outcome for outcome in report.outcomes if outcome.test_id == "storage"
                )
                db_path = root / "evaluator_state/validation_tests/storage/storage_results.db"
                with closing(sqlite3.connect(db_path)) as connection:
                    statuses = connection.execute(
                        "SELECT run_id, status FROM test_results ORDER BY run_id"
                    ).fetchall()
                    metric_count = connection.execute(
                        "SELECT COUNT(*) FROM storage_performance"
                    ).fetchone()[0]

                self.assertTrue(report.ok)
                self.assertFalse(storage.adapter_called)
                self.assertEqual(
                    statuses,
                    [("node-a-123", "pass"), ("node-a-124", terminal_status)],
                )
                self.assertEqual(metric_count, 1)

    def test_result_path_escape_is_rejected_before_any_target_db_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            payload["tests"]["storage"]["artifacts"] = str(root / "outside")
            result_path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "canonical run path"):
                _ingest(result_path, config)

            self.assertFalse(
                (root / "evaluator_state/validation_tests/storage/storage_results.db").exists()
            )

    def test_storage_adapter_rejects_symlinked_child_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            artifact = (
                root
                / "validation_tests/storage/runs/node-a/node-a-123/artifacts/randread.json"
            )
            external = root / "external.json"
            external.write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "read": {"iops": 999999, "bw": 999999},
                                "write": {},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            artifact.unlink()
            artifact.symlink_to(external)

            with self.assertRaisesRegex(Exception, "symlink"):
                _ingest(result_path, config)

            self.assertFalse(
                (root / "evaluator_state/validation_tests/storage/storage_results.db").exists()
            )
            self.assertFalse(
                (root / "evaluator_state/validation_tests/nccl/nccl_results.db").exists()
            )
            self.assertFalse(
                (root / "evaluator_state/validation_tests/dltest/dltest_results.db").exists()
            )

    def test_dl_adapter_rejects_rank_plan_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            rank_path = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/artifacts/"
                "workdir/test_plans/80gb-example/runs/rank_RANK0.json"
            )
            rank = json.loads(rank_path.read_text(encoding="utf-8"))
            rank["test_plan"] = "different-plan"
            rank_path.write_text(json.dumps(rank), encoding="utf-8")

            report = _ingest(result_path, config)

            dltest = next(
                outcome for outcome in report.outcomes if outcome.test_id == "dltest"
            )
            self.assertIn("does not match", dltest.error)
            with closing(
                sqlite3.connect(root / "evaluator_state/validation_tests/dltest/dltest_results.db")
            ) as connection:
                metric_table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='numerical_correctness'"
                ).fetchone()
            self.assertIsNone(metric_table)

    def test_dl_adapter_rejects_historical_summary_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            canonical = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/summary.json"
            )
            canonical.rename(canonical.with_name("dltest-summary-fallback.json"))

            report = _ingest(result_path, config)

            dltest = next(
                outcome for outcome in report.outcomes if outcome.test_id == "dltest"
            )
            self.assertFalse(report.ok)
            self.assertIn("declared canonical summary", dltest.error)

    def test_dl_adapter_rejects_missing_metric_component(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            run_dir = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/artifacts/"
                "workdir/test_plans/80gb-example/runs"
            )
            for rank_path in run_dir.glob("*.json"):
                rank = json.loads(rank_path.read_text(encoding="utf-8"))
                rank["overlap_tasks"] = []
                rank_path.write_text(json.dumps(rank), encoding="utf-8")
            summary_path = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/summary.json"
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["task_counts"]["overlap_tasks"] = 0
            summary["status_counts"] = {"completed": 16}
            for rank_result in summary["rank_results"]:
                rank_result["task_counts"]["overlap_tasks"] = 0
                rank_result["status_counts"] = {"completed": 2}
            summary_path.write_text(json.dumps(summary), encoding="utf-8")

            report = _ingest(result_path, config)

            dltest = next(
                outcome for outcome in report.outcomes if outcome.test_id == "dltest"
            )
            self.assertIn("overlap_performance", dltest.error)

    def test_dl_adapter_rejects_coercive_or_forged_summary(self) -> None:
        for mutation, expected in (
            (lambda summary: summary.__setitem__("iterations", 100.9), "integer"),
            (
                lambda summary: summary["expected_ranks"].__setitem__(0, False),
                "integer",
            ),
            (
                lambda summary: summary["rank_results"][0]["task_counts"].__setitem__(
                    "f_tasks", False
                ),
                "integer",
            ),
            (
                lambda summary: summary["rank_results"][0].__setitem__(
                    "file",
                    summary["rank_results"][0]["file"].replace(
                        "/runs/rank_RANK0.json", "/runs/./rank_RANK0.json"
                    ),
                ),
                "source evidence",
            ),
            (
                lambda summary: summary["rank_results"][0]["task_counts"].__setitem__(
                    "nn_tasks", 999
                ),
                "source evidence",
            ),
        ):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config = self._config(root, enabled=True)
                result_path = self._write_builtin_result(root, config)
                summary_path = (
                    root
                    / "validation_tests/dltest/runs/node-a/node-a-123/summary.json"
                )
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                mutation(summary)
                summary_path.write_text(json.dumps(summary), encoding="utf-8")

                report = _ingest(result_path, config)

                dltest = next(
                    outcome for outcome in report.outcomes if outcome.test_id == "dltest"
                )
                self.assertIn(expected, dltest.error)

    def test_dl_adapter_rejects_non_finite_rank_metric(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            rank_path = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/artifacts/"
                "workdir/test_plans/80gb-example/runs/rank_RANK0.json"
            )
            rank = json.loads(rank_path.read_text(encoding="utf-8"))
            rank["nn_tasks"][0]["norm_output"] = float("nan")
            rank_path.write_text(json.dumps(rank), encoding="utf-8")

            report = _ingest(result_path, config)

            dltest = next(
                outcome for outcome in report.outcomes if outcome.test_id == "dltest"
            )
            self.assertIn("non-finite", dltest.error)

    def test_dl_adapter_rejects_non_string_metric_identities(self) -> None:
        for field, value, expected in (
            ("task_name", 42, "task_name"),
            ("coll_name", True, "coll_name"),
            ("layer_name", 7, "layer_name"),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config = self._config(root, enabled=True)
                result_path = self._write_builtin_result(root, config)
                rank_path = (
                    root
                    / "validation_tests/dltest/runs/node-a/node-a-123/artifacts/"
                    "workdir/test_plans/80gb-example/runs/rank_RANK0.json"
                )
                rank = json.loads(rank_path.read_text(encoding="utf-8"))
                if field == "task_name":
                    rank["nn_tasks"][0][field] = value
                else:
                    rank["overlap_tasks"][0][field] = value
                rank_path.write_text(json.dumps(rank), encoding="utf-8")

                report = _ingest(result_path, config)

                dltest = next(
                    outcome for outcome in report.outcomes if outcome.test_id == "dltest"
                )
                self.assertIn(expected, dltest.error)

    def test_dl_adapter_rejects_mixed_rank_invocation_prefixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            runs_dir = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/artifacts/"
                "workdir/test_plans/80gb-example/runs"
            )
            summary_path = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/summary.json"
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            for rank_id, rank_result in enumerate(summary["rank_results"]):
                prefix = "invocation-a" if rank_id < 4 else "invocation-b"
                run_id = f"{prefix}_RANK{rank_id}"
                rank_path = runs_dir / f"rank_RANK{rank_id}.json"
                rank = json.loads(rank_path.read_text(encoding="utf-8"))
                rank["runID"] = run_id
                rank_path.write_text(json.dumps(rank), encoding="utf-8")
                rank_result["run_id"] = run_id
            summary_path.write_text(json.dumps(summary), encoding="utf-8")

            report = _ingest(result_path, config)

            dltest = next(
                outcome for outcome in report.outcomes if outcome.test_id == "dltest"
            )
            self.assertFalse(report.ok)
            self.assertIn("multiple invocation prefixes", dltest.error)

    def test_dl_adapter_rejects_zero_padded_rank_suffixes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root, enabled=True)
            result_path = self._write_builtin_result(root, config)
            runs_dir = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/artifacts/"
                "workdir/test_plans/80gb-example/runs"
            )
            summary_path = (
                root
                / "validation_tests/dltest/runs/node-a/node-a-123/summary.json"
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            for rank_id, rank_result in enumerate(summary["rank_results"]):
                run_id = f"invocation_RANK{rank_id:02d}"
                rank_path = runs_dir / f"rank_RANK{rank_id}.json"
                rank = json.loads(rank_path.read_text(encoding="utf-8"))
                rank["runID"] = run_id
                rank_path.write_text(json.dumps(rank), encoding="utf-8")
                rank_result["run_id"] = run_id
            summary_path.write_text(json.dumps(summary), encoding="utf-8")

            report = _ingest(result_path, config)

            dltest = next(
                outcome for outcome in report.outcomes if outcome.test_id == "dltest"
            )
            self.assertFalse(report.ok)
            self.assertIn("must end exactly", dltest.error)

    @staticmethod
    def _config(root: Path, *, enabled: bool):
        base = load_config()
        root.mkdir(parents=True, exist_ok=True)
        state_root = root / "evaluator_state"
        if enabled:
            state_root.mkdir(mode=0o700)
            os.chmod(state_root, 0o700)
        return replace(
            base,
            storage=replace(base.storage, per_test_ingestion_enabled=enabled),
            runtime=replace(
                base.runtime,
                validation_root=str(root),
                dl_results_root_path=str(
                    root / "validation_tests/dltest/runs"
                ),
            ),
            health_evaluator=replace(
                base.health_evaluator,
                state_root=str(state_root),
                state_owner_uid=os.geteuid(),
                state_owner_gid=os.getegid(),
            ),
        )

    def _write_builtin_result(
        self,
        root: Path,
        config,
        *,
        statuses: dict[str, str] | None = None,
        run_id: str = "node-a-123",
        timestamp: int = 123,
    ) -> Path:
        statuses = statuses or {
            "storage": "pass",
            "nccl": "pass",
            "dltest": "pass",
        }
        env = {
            "CVAL_NODE": "node-a",
            "CVAL_TIMESTAMP": str(timestamp),
            "CVAL_RUN_ID": run_id,
            "CVAL_VALIDATION_ROOT": str(root),
            "CVAL_IMAGE_NAME": "image",
            "CVAL_PYTORCH_VERSION": "2.8",
            "CVAL_CUDA_VERSION": "12.9",
            "CVAL_GIT_REF": "test-ref",
            "CVAL_CONFIG_DIGEST": effective_config_digest(config),
        }

        def fake_process(
            _self,
            command,
            *,
            cwd,
            environment,
            timeout_seconds,
            test_paths,
            label,
        ):
            from cval.validation.execution import ProcessOutcome

            test_id = label.split(":", 1)[0]
            phase = label.split(":", 1)[1]
            if phase == "run":
                if test_id == "storage":
                    self._write_storage_artifacts(Path(environment["CVAL_TEST_OUTPUT_DIR"]))
                elif test_id == "nccl":
                    self._write_nccl_summary(Path(environment["CVAL_TEST_SUMMARY_FILE"]))
                elif test_id == "dltest":
                    self._write_dl_artifacts(
                        Path(environment["CVAL_TEST_OUTPUT_DIR"]),
                        Path(environment["CVAL_TEST_SUMMARY_FILE"]),
                    )
            status = statuses[test_id]
            return ProcessOutcome(
                exit_code=0 if phase == "setup" or status == "pass" else 1,
                timed_out=False,
                duration_ms=1,
                message="" if status == "pass" else "synthetic failure",
            )

        with patch(
            "cval.validation.execution.RunLogger.stream_process",
            new=fake_process,
        ):
            run_validation_tests(
                config=config,
                registry=config.tests.registry,
                environ=os.environ | env,
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
        return root / "logs/job_logs/node-a" / run_id / "result.json"

    @staticmethod
    def _write_storage_artifacts(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        for index, filename in enumerate(
            (
                "iodepth_read_1file.json",
                "iodepth_write_1file.json",
                "numjobs_read_nfiles.json",
                "numjobs_write_nfiles.json",
                "randread.json",
                "randwrite.json",
            ),
            start=1,
        ):
            read_iops = 10.0 if filename == "randread.json" else float(index)
            read_bw = 20.0 if filename == "randread.json" else float(index * 2)
            write_iops = 5.0 if filename == "randread.json" else 0.0
            write_bw = 7.0 if filename == "randread.json" else 0.0
            (path / filename).write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "read": {"iops": read_iops, "bw": read_bw},
                                "write": {"iops": write_iops, "bw": write_bw},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

    @staticmethod
    def _write_nccl_summary(path: Path) -> None:
        path.write_text(
            json.dumps(
                {
                    "GCR_ITERATIONS": 20,
                    "GCR_DATA_SIZE_GB": 8,
                    "GCR_BUSBW": 44.5,
                    "GCR_ALGBW": 25.4,
                    "GCR_LATENCY": 628.2,
                    "GCR_IB_PORT_BW_GBPS": {
                        "mlx5_0": {"avg_gbps": 20.0, "max_gbps": 46.1, "last_gbps": 45.9, "samples": 26},
                        "mlx5_13": {"avg_gbps": 20.2, "max_gbps": 46.3, "last_gbps": 46.0, "samples": 26},
                        "mlx5_5.2": {"avg_gbps": 50.0, "max_gbps": 99.0, "last_gbps": 98.0, "samples": 26},
                    },
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _write_dl_artifacts(artifacts: Path, summary: Path) -> None:
        runs = artifacts / "workdir/test_plans/80gb-example/runs"
        runs.mkdir(parents=True)
        rank_results = []
        for rank_id in range(8):
            rank = {
                "runID": f"synthetic_RANK{rank_id}",
                "test_plan": "80gb-example",
                "nn_tasks": [
                    {
                        "task_name": "linear",
                        "status": "completed",
                        "norm_output": 0.5 + rank_id,
                        "fp_cpu_time": 10.0 + rank_id,
                        "fp_gpu_time": 1.0 + rank_id,
                        "bp_cpu_time": 11.0 + rank_id,
                        "bp_gpu_time": 1.1 + rank_id,
                    }
                ],
                "f_tasks": [],
                "coll_tasks": [
                    {
                        "task_name": "allreduce",
                        "status": "completed",
                        "norm_output": 0.7 + rank_id,
                        "cpu_time": 30.0 + rank_id,
                        "gpu_time": 3.0 + rank_id,
                    }
                ],
                "overlap_tasks": [
                    {
                        "task_name": "overlap",
                        "status": "completed",
                        "coll_name": "allreduce",
                        "layer_name": "linear",
                        "coll_mean": 4.0 + rank_id,
                        "coll_stdev": 0.4 + rank_id,
                        "layer_mean": 5.0 + rank_id,
                        "layer_stdev": 0.5 + rank_id,
                    }
                ],
            }
            rank_path = runs / f"rank_RANK{rank_id}.json"
            rank_path.write_text(
                json.dumps(rank), encoding="utf-8"
            )
            rank_results.append(
                {
                    "rank": rank_id,
                    "run_id": f"synthetic_RANK{rank_id}",
                    "file": str(rank_path),
                    "test_plan": "80gb-example",
                    "task_counts": {
                        "nn_tasks": 1,
                        "f_tasks": 0,
                        "coll_tasks": 1,
                        "overlap_tasks": 1,
                    },
                    "status_counts": {"completed": 3},
                    "tasks_valid": True,
                }
            )
        summary.write_text(
            json.dumps(
                {
                    "schema_version": "cval.dltest.summary.v1",
                    "status": "pass",
                    "test_plan": "80gb-example",
                    "iterations": 100,
                    "gpu_count": 8,
                    "rank_result_count": 8,
                    "expected_ranks": list(range(8)),
                    "observed_ranks": list(range(8)),
                    "rank_coverage_valid": True,
                    "test_plans_match": True,
                    "tasks_complete": True,
                    "task_counts": {
                        "nn_tasks": 8,
                        "f_tasks": 0,
                        "coll_tasks": 8,
                        "overlap_tasks": 8,
                    },
                    "status_counts": {"completed": 24},
                    "rank_results": rank_results,
                }
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
