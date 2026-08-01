from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager, redirect_stdout
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cval.config import load_config
from cval.evaluator.state import (
    StateLockError,
    bind_state_target,
    state_test_lock,
)
from cval.health.combination import resolve_environment_combination
from cval.health.evaluator import (
    ACTIVATE_CONFIRMATION,
    EVALUATE_CONFIRMATION,
    CatalogResult,
    CandidateAction,
    EvaluationCycleReport,
    HealthEvaluatorLockError,
    HealthEvaluatorPolicyError,
    ResultCatalog,
    TestEvaluationReport,
    _evaluate_classifications,
    _evaluate_candidates,
    _evaluate_registered_test,
    _catalog_result_from_row,
    _load_result_catalog,
    activate_health_candidate,
    evaluate_health_cycle,
)
from cval.health.models import (
    ActivationPreflight,
    BaselineLifecycle,
    HealthChainCursor,
    HealthContext,
    MetricObservation,
    SourceResult,
    SourceSnapshot,
)
from cval.health.engine import build_candidate_from_plugin, metric_specs_from_definition
from cval.health.storage import _store_candidate, resolve_health_db_path
from cval.storage.per_test_results import (
    COMMON_IMMUTABLE_KEY_GROUPS,
    PerTestResultRecord,
    _classification_evidence_digest,
    prepare_immutable_table_triggers,
    resolve_test_results_db_path,
    write_per_test_result,
)
from cval.storage.sqlite_snapshot import (
    _require_unchanged_source,
    immutable_sqlite_snapshot,
)
from cval.validation.plugins import load_registered_plugin
from cval.validation.registry import load_test_registry, validation_test_config_digest
from tests import test_health_plugins, test_per_test_ingestion


class HealthEvaluatorTests(unittest.TestCase):
    @staticmethod
    def _state_config(base, root: Path, *, write_enabled: bool = False):
        root.mkdir(mode=0o700, exist_ok=True)
        os.chmod(root, 0o700)
        return replace(
            base,
            runtime=replace(
                base.runtime,
                validation_root=str(root.parent / f"{root.name}-shared-evidence"),
            ),
            health_evaluator=replace(
                base.health_evaluator,
                write_enabled=write_enabled,
                state_root=str(root),
                state_owner_uid=os.geteuid(),
                state_owner_gid=os.getegid(),
            ),
        )

    def _prepared(self, root: Path):
        case = test_health_plugins.BuiltinHealthPluginTests()
        case.fixture = test_per_test_ingestion.ModularPerTestIngestionTests()
        return case._prepared(root)

    def _append_result(
        self,
        config,
        *,
        run_id: str,
        status: str,
        with_receipt: bool = False,
    ) -> Path:
        registered = config.tests.registry.require("storage")
        combination = resolve_environment_combination(
            registered.definition,
            {"image_name": "image", "cuda_version": "12.9", "pytorch_version": "2.8"},
        )
        assert combination is not None
        timestamp = 1_000 + sum(run_id.encode("utf-8"))
        payload = json.dumps(
            {
                "schema_version": "cval.test-result.v1",
                "status": status,
                "test_id": "storage",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        path = resolve_test_results_db_path(
            config.health_evaluator.state_root,
            registered,
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
                PerTestResultRecord(
                    run_id=run_id,
                    test_id="storage",
                    node="node-a",
                    run_timestamp=timestamp,
                    started_timestamp=timestamp,
                    completed_timestamp=timestamp + 1,
                    status=status,
                    exit_code=0 if status == "pass" else 1,
                    image_name="image",
                    pytorch_version="2.8",
                    cuda_version="12.9",
                    test_config_digest=validation_test_config_digest(registered),
                    combination_key=combination.key,
                    result_path=f"/tmp/{run_id}/result.json",
                    summary_path=f"/tmp/{run_id}/summary.json",
                    artifacts_path=f"/tmp/{run_id}/artifacts",
                    raw_result_json=payload,
                    result_digest=(
                        "sha256:" + hashlib.sha256(run_id.encode()).hexdigest()
                    ),
                ),
                db_path=path,
                now=timestamp,
                expected_identity=identity,
                state_guard=lambda: (
                    lock_guard(),
                    binding.assert_path_binding(),
                ),
            )
        if with_receipt:
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "INSERT INTO metric_ingestion_receipts("
                    "run_id,test_id,adapter_api_version,evidence_digest,inserted_count,"
                    "updated_count,metric_names_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        run_id,
                        "storage",
                        "cval.plugin.v1",
                        "sha256:" + hashlib.sha256(f"receipt:{run_id}".encode()).hexdigest(),
                        1,
                        0,
                        '["metric"]',
                        timestamp,
                    ),
                )
                connection.commit()
        return path

    def test_dry_run_has_no_lock_migration_health_db_or_key_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            before = {
                path.relative_to(root): (
                    path.stat().st_atime_ns,
                    path.stat().st_mtime_ns,
                    path.stat().st_ctime_ns,
                )
                for path in root.rglob("*")
                if path.is_file()
            }
            locks_before = sorted(
                str(path.relative_to(root))
                for path in root.rglob("*.health-evaluator.lock")
            )

            report = evaluate_health_cycle(config, now=9_999_999_999)

            after = {
                path.relative_to(root): (
                    path.stat().st_atime_ns,
                    path.stat().st_mtime_ns,
                    path.stat().st_ctime_ns,
                )
                for path in root.rglob("*")
                if path.is_file()
            }
            locks_after = sorted(
                str(path.relative_to(root))
                for path in root.rglob("*.health-evaluator.lock")
            )
            migrations = {}
            for test_id in ("storage", "nccl", "dltest"):
                db_path = root / f"evaluator_state/validation_tests/{test_id}/{test_id}_results.db"
                with closing(sqlite3.connect(db_path)) as connection:
                    migrations[test_id] = connection.execute(
                        "SELECT version FROM schema_migrations ORDER BY version"
                    ).fetchall()

        self.assertTrue(report.ok)
        self.assertEqual(before, after)
        self.assertEqual(migrations, {"storage": [(1,)], "nccl": [(1,)], "dltest": [(1,)]})
        self.assertEqual(locks_after, locks_before)
        self.assertFalse(any("health_classes" in str(path) for path in after))
        self.assertFalse(any(str(path).endswith("activation.key") for path in after))

    def test_checkpointed_wal_dry_run_and_adapter_snapshot_create_no_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            for test_id in ("storage", "nccl", "dltest"):
                path = root / f"evaluator_state/validation_tests/{test_id}/{test_id}_results.db"
                with closing(sqlite3.connect(path)) as connection:
                    self.assertEqual(connection.execute("PRAGMA journal_mode=WAL").fetchone(), ("wal",))
                    self.assertEqual(connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(), (0, 0, 0))
                self.assertEqual(path.read_bytes()[18:20], b"\x02\x02")
                self.assertFalse(path.with_name(f"{path.name}-wal").exists())
                self.assertFalse(path.with_name(f"{path.name}-shm").exists())
                metadata = path.stat()
                os.utime(path, ns=(1_000_000_000, metadata.st_mtime_ns))
            before = {
                path.relative_to(root): (
                    path.stat().st_atime_ns,
                    path.stat().st_mtime_ns,
                )
                for path in root.rglob("*")
                if path.is_file()
            }

            report = evaluate_health_cycle(config, now=9_999_999_999)
            registered = config.tests.registry.require("storage")
            plugin = load_registered_plugin(registered)
            self.assertIsNotNone(plugin)
            storage_path = resolve_test_results_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            with immutable_sqlite_snapshot(storage_path) as snapshot:
                catalog = _load_result_catalog(
                    snapshot.uri,
                    registered,
                    plugin,
                    active_baseline_ids={},
                    limit=1,
                )
                source = catalog.candidate_results[0].source_result
                assert source is not None
                context = HealthContext(
                    definition=registered.definition,
                    result_db_path=snapshot.uri,
                    combination=catalog.candidate_results[0].combination,
                    source_snapshot=SourceSnapshot((source,)),
                )
                self.assertTrue(plugin.load_observations(context))

            after = {
                path.relative_to(root): (
                    path.stat().st_atime_ns,
                    path.stat().st_mtime_ns,
                )
                for path in root.rglob("*")
                if path.is_file()
            }
        self.assertTrue(report.ok)
        self.assertEqual(before, after)
        self.assertFalse(any(str(path).endswith(("-wal", "-shm")) for path in after))

    def test_snapshot_source_change_identity_excludes_atime(self) -> None:
        before = SimpleNamespace(
            st_dev=1,
            st_ino=2,
            st_size=4096,
            st_atime_ns=3,
            st_mtime_ns=4,
            st_ctime_ns=5,
        )
        after = SimpleNamespace(**{**vars(before), "st_atime_ns": 999})

        _require_unchanged_source(Path("/unused.db"), before, after, 4096)

    def test_checkpointed_wal_apply_migrates_and_stores_without_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            config = replace(
                config,
                health_evaluator=replace(config.health_evaluator, write_enabled=True),
            )
            db_paths = tuple(
                root / f"evaluator_state/validation_tests/{test_id}/{test_id}_results.db"
                for test_id in ("storage", "nccl", "dltest")
            )
            for path in db_paths:
                with closing(sqlite3.connect(path)) as connection:
                    self.assertEqual(
                        connection.execute("PRAGMA journal_mode=WAL").fetchone(),
                        ("wal",),
                    )
                    self.assertEqual(
                        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(),
                        (0, 0, 0),
                    )
                self.assertFalse(path.with_name(f"{path.name}-wal").exists())
                self.assertFalse(path.with_name(f"{path.name}-shm").exists())

            report = evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=9_999_999_999,
            )
            stored = {}
            for path in db_paths:
                with closing(sqlite3.connect(path)) as connection:
                    stored[path.name] = (
                        connection.execute(
                            "SELECT version FROM schema_migrations ORDER BY version"
                        ).fetchall(),
                        connection.execute(
                            "SELECT COUNT(*) FROM classification_history"
                        ).fetchone()[0],
                    )
            sidecars_absent = all(
                not path.with_name(f"{path.name}{suffix}").exists()
                for path in db_paths
                for suffix in ("-wal", "-shm")
            )

        self.assertTrue(report.ok)
        self.assertTrue(all(test.migrated_to_v2 for test in report.tests))
        self.assertEqual(sum(test.history_inserted for test in report.tests), 3)
        self.assertEqual(
            stored,
            {
                "storage_results.db": ([(1,), (2,)], 1),
                "nccl_results.db": ([(1,), (2,)], 1),
                "dltest_results.db": ([(1,), (2,)], 1),
            },
        )
        self.assertTrue(sidecars_absent)

    def test_eligible_candidate_apply_uses_guarded_wal_projection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixture = test_per_test_ingestion.ModularPerTestIngestionTests()
            config = fixture._config(root, enabled=True)
            for index in range(10):
                result_path = fixture._write_builtin_result(
                    root,
                    config,
                    statuses={
                        "storage": "pass",
                        "nccl": "fail",
                        "dltest": "fail",
                    },
                    run_id=f"wal-node-{index + 1}",
                    timestamp=1_000 + index,
                )
                self.assertTrue(test_per_test_ingestion._ingest(result_path, config).ok)
            config = replace(
                config,
                health_evaluator=replace(config.health_evaluator, write_enabled=True),
            )
            registered = config.tests.registry.require("storage")
            result_db = resolve_test_results_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            health_db = resolve_health_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            with closing(sqlite3.connect(result_db)) as connection:
                self.assertEqual(
                    connection.execute("PRAGMA journal_mode=WAL").fetchone(),
                    ("wal",),
                )
                self.assertEqual(
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone(),
                    (0, 0, 0),
                )
            self.assertFalse(result_db.with_name(f"{result_db.name}-wal").exists())
            self.assertFalse(result_db.with_name(f"{result_db.name}-shm").exists())

            report = evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=30_000,
            )
            storage = next(test for test in report.tests if test.test_id == "storage")
            with closing(sqlite3.connect(result_db)) as connection:
                migrations = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                history_count = connection.execute(
                    "SELECT COUNT(*) FROM classification_history"
                ).fetchone()[0]
            with closing(sqlite3.connect(health_db)) as connection:
                candidate_count = connection.execute(
                    "SELECT COUNT(*) FROM health_baselines"
                ).fetchone()[0]
                source_count = connection.execute(
                    "SELECT COUNT(*) FROM health_baseline_sources"
                ).fetchone()[0]
                observation_count = connection.execute(
                    "SELECT COUNT(*) FROM health_observations"
                ).fetchone()[0]
            residual_sidecars = tuple(
                path
                for path in root.rglob("*")
                if path.name.endswith(("-wal", "-shm", "-journal"))
                or ".staging" in path.name
            )

        self.assertEqual(storage.status, "processed", storage)
        self.assertTrue(storage.migrated_to_v2)
        self.assertEqual(storage.candidate_source_count, 10)
        self.assertEqual(storage.candidates_inserted, 1)
        self.assertEqual(storage.history_inserted, 10)
        self.assertEqual(migrations, [(1,), (2,)])
        self.assertEqual(history_count, 10)
        self.assertEqual(candidate_count, 1)
        self.assertEqual(source_count, 10)
        self.assertGreater(observation_count, 0)
        self.assertEqual(residual_sidecars, ())

    def test_repeated_dry_run_preserves_existing_health_db_and_key_atime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            registered = config.tests.registry.require("storage")
            combination = resolve_environment_combination(
                registered.definition,
                {
                    "image_name": "image",
                    "cuda_version": "12.9",
                    "pytorch_version": "2.8",
                },
            )
            assert combination is not None
            health = registered.definition.health
            assert health is not None
            sources = tuple(
                SourceResult(
                    100 + index,
                    f"existing-{index}",
                    10_000 + index,
                    "sha256:" + f"{100 + index:064x}",
                    "sha256:" + f"{200 + index:064x}",
                    validation_test_config_digest(registered),
                    combination.key,
                    1,
                    "sha256:" + f"{300 + index:064x}",
                )
                for index in range(max(health.min_samples, health.min_new_results))
            )
            specs = metric_specs_from_definition(registered.definition)
            observations = tuple(
                MetricObservation(
                    result_id=source.result_id,
                    run_id=source.run_id,
                    completed_timestamp=source.completed_timestamp,
                    source=spec.source,
                    metric_name=spec.name,
                    sample_key=spec.name,
                    value=100.0 + index,
                )
                for index, source in enumerate(sources)
                for spec in specs
            )

            class ExistingPlugin:
                health_policy_version = health.policy_version

                def metric_specs(self, definition):
                    return metric_specs_from_definition(definition)

                def load_observations(self, _context):
                    return observations

            candidate = build_candidate_from_plugin(
                ExistingPlugin(),
                HealthContext(
                    definition=registered.definition,
                    result_db_path=root / "unused.db",
                    combination=combination,
                    source_snapshot=SourceSnapshot(sources),
                    robust_z_threshold=config.baseline.robust_z_threshold,
                    created_at=20_000,
                ),
            )
            health_path = resolve_health_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            _store_candidate(
                candidate,
                registered.definition,
                db_path=health_path,
                now=20_000,
                robust_z_threshold=config.baseline.robust_z_threshold,
            )
            key_path = health_path.with_name(f"{health_path.name}.activation.key")
            for path in (health_path, key_path):
                metadata = path.stat()
                os.utime(path, ns=(1_000_000_000, metadata.st_mtime_ns))
            before = {
                path: (path.stat().st_atime_ns, path.stat().st_mtime_ns)
                for path in (health_path, key_path)
            }

            reports = [
                evaluate_health_cycle(config, now=30_000 + index)
                for index in range(5)
            ]
            after = {
                path: (path.stat().st_atime_ns, path.stat().st_mtime_ns)
                for path in (health_path, key_path)
            }

        self.assertTrue(all(report.ok for report in reports))
        self.assertEqual(before, after)

    def test_complete_candidate_catalog_and_oldest_pending_pages_drain_backlog(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            for index in range(3):
                self._append_result(
                    config,
                    run_id=f"pass-{index}",
                    status="pass",
                    with_receipt=True,
                )
            for index in range(5):
                self._append_result(config, run_id=f"fail-{index}", status="fail")
            config = replace(
                config,
                health_evaluator=replace(
                    config.health_evaluator,
                    write_enabled=True,
                    max_classifications_per_test=2,
                ),
            )
            selected: list[str] = []
            reports = []
            for cycle in range(5):
                report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=10_000 + cycle,
                )
                storage = next(test for test in report.tests if test.test_id == "storage")
                reports.append(storage)
                selected.extend(action.run_id for action in storage.classifications)

        self.assertTrue(all(report.candidate_source_count == 4 for report in reports))
        self.assertEqual(reports[0].classification_selected_count, 2)
        self.assertEqual(reports[0].classification_backlog, 9)
        self.assertTrue(reports[0].classification_truncated)
        self.assertEqual(reports[-1].classification_remaining, 0)
        self.assertEqual(len(selected), 9)
        self.assertEqual(len(set(selected)), 9)

    def test_history_lookups_are_page_bounded_not_per_result(self) -> None:
        from cval.health import evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            for index in range(8):
                self._append_result(config, run_id=f"existing-{index}", status="fail")
            evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=12_000,
            )
            for index in range(7):
                self._append_result(config, run_id=f"pending-{index}", status="fail")

            original_lookup = evaluator_module._load_existing_classification_targets
            page_sizes: list[int] = []

            def bounded_lookup(connection, targets):
                page_sizes.append(len(targets))
                return original_lookup(connection, targets)

            with patch.object(
                evaluator_module,
                "_CATALOG_QUERY_PAGE_SIZE",
                3,
            ), patch.object(
                evaluator_module,
                "_load_existing_classification_targets",
                side_effect=bounded_lookup,
            ):
                report = evaluate_health_cycle(config, now=12_001)
            storage = next(test for test in report.tests if test.test_id == "storage")

        self.assertEqual(storage.classification_backlog, 7)
        self.assertEqual(page_sizes, [3, 3, 3, 3, 3])
        self.assertTrue(all(size <= 3 for size in page_sizes))

    def test_classification_race_preserves_mixed_per_record_store_outcomes(self) -> None:
        from cval.health import evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            self._append_result(config, run_id="race-first", status="fail")
            self._append_result(config, run_id="race-second", status="fail")
            original_store = evaluator_module.store_classification_history
            raced = False

            def race_one_record(records, **kwargs):
                nonlocal raced
                if not raced:
                    raced = True
                    original_store((records[0],), db_path=kwargs["db_path"])
                return original_store(records, **kwargs)

            with patch(
                "cval.health.evaluator.store_classification_history",
                side_effect=race_one_record,
            ):
                report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=12_002,
                )
            storage = next(test for test in report.tests if test.test_id == "storage")

        self.assertTrue(raced)
        self.assertEqual(storage.status, "processed")
        self.assertEqual(storage.history_inserted, 1)
        self.assertEqual(storage.history_idempotent, 1)
        self.assertEqual(
            [(action.run_id, action.action) for action in storage.classifications],
            [("race-first", "idempotent"), ("race-second", "stored")],
        )

    def test_absent_adapter_allows_raw_dnr_but_defers_passing_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root)
            for run_id, status in (("failed", "fail"), ("unfinished", "incomplete"), ("passed", "pass")):
                self._append_result(config, run_id=run_id, status=status)
            config = replace(
                config,
                health_evaluator=replace(config.health_evaluator, write_enabled=True),
            )

            report = evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=20_000,
            )
            storage = next(test for test in report.tests if test.test_id == "storage")
            path = resolve_test_results_db_path(
                config.health_evaluator.state_root,
                config.tests.registry.require("storage"),
            )
            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute(
                    "SELECT run_id,dnr_reason FROM classification_history ORDER BY result_id"
                ).fetchall()

        self.assertEqual(storage.status, "processed")
        self.assertFalse(storage.adapter_schema_initialized)
        self.assertEqual(storage.history_inserted, 2)
        self.assertEqual(storage.deferred_count, 1)
        self.assertEqual(storage.classification_remaining, 1)
        self.assertEqual(
            [
                (action.run_id, action.action, action.reason)
                for action in storage.classifications
            ],
            [
                ("failed", "stored", ""),
                ("unfinished", "stored", ""),
                (
                    "passed",
                    "deferred",
                    "passing result requires initialized adapter schema and receipt",
                ),
            ],
        )
        self.assertEqual(report.to_dict()["summary"]["deferred_count"], 1)
        self.assertEqual(rows, [("failed", "raw_failed"), ("unfinished", "raw_incomplete")])

    def test_absent_adapter_deferred_report_is_bounded_and_not_drained(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root)
            config = replace(
                config,
                health_evaluator=replace(
                    config.health_evaluator,
                    max_classifications_per_test=2,
                ),
            )
            for index in range(5):
                self._append_result(
                    config,
                    run_id=f"deferred-{index}",
                    status="pass",
                )

            report = evaluate_health_cycle(config, now=20_000)
            storage = next(test for test in report.tests if test.test_id == "storage")

        self.assertEqual(storage.status, "processed")
        self.assertEqual(storage.classification_selected_count, 0)
        self.assertEqual(storage.classification_backlog, 0)
        self.assertEqual(storage.deferred_count, 5)
        self.assertEqual(storage.classification_remaining, 5)
        self.assertTrue(storage.classification_truncated)
        self.assertEqual(len(storage.classifications), 2)
        self.assertTrue(
            all(action.action == "deferred" for action in storage.classifications)
        )
        self.assertTrue(
            all(
                action.reason
                == "passing result requires initialized adapter schema and receipt"
                for action in storage.classifications
            )
        )

    def test_absent_adapter_with_receipt_is_partial_state_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root)
            self._append_result(
                config,
                run_id="failed-with-receipt",
                status="fail",
                with_receipt=True,
            )

            report = evaluate_health_cycle(config, now=20_001)
            storage = next(test for test in report.tests if test.test_id == "storage")

        self.assertEqual(storage.status, "error")
        self.assertEqual(storage.error_stage, "read-preflight")
        self.assertIn("schema/version/receipt state is partial", storage.error)

    def test_initialized_adapter_orphan_receipt_fails_before_any_u9_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            path = self._append_result(
                config,
                run_id="nonpassing-parent",
                status="fail",
            )
            registry = replace(
                config.tests.registry,
                tests=tuple(
                    replace(test, enabled=test.id == "storage")
                    for test in config.tests.registry.tests
                ),
            )
            config = replace(
                config,
                tests=replace(config.tests, registry=registry),
                health_evaluator=replace(
                    config.health_evaluator,
                    write_enabled=True,
                ),
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA foreign_keys=OFF")
                connection.execute(
                    "INSERT INTO metric_ingestion_receipts("
                    "run_id,test_id,adapter_api_version,evidence_digest,inserted_count,"
                    "updated_count,metric_names_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        "orphan-receipt",
                        "storage",
                        "cval.plugin.v1",
                        "sha256:" + hashlib.sha256(b"orphan-receipt").hexdigest(),
                        1,
                        0,
                        '["metric"]',
                        20_002,
                    ),
                )
                connection.commit()
                self.assertIsNotNone(
                    connection.execute("PRAGMA foreign_key_check").fetchone()
                )

            dry = evaluate_health_cycle(config, now=20_003)
            applied = evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=20_004,
            )
            with closing(sqlite3.connect(path)) as connection:
                migrations = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                history_table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='classification_history'"
                ).fetchone()
            health_path = resolve_health_db_path(
                root,
                config.tests.registry.require("storage"),
            )

        for report in (dry, applied):
            with self.subTest(mode=report.mode):
                payload = report.to_dict()
                self.assertFalse(payload["ok"])
                self.assertEqual(len(payload["tests"]), 1)
                storage = payload["tests"][0]
                self.assertEqual(storage["status"], "error")
                self.assertEqual(storage["error_stage"], "read-preflight")
                self.assertEqual(storage["write_atomicity"], "no-writes")
                self.assertFalse(storage["partial_writes"])
                self.assertIn("foreign_key_check", storage["error"])
        self.assertEqual(migrations, [(1,)])
        self.assertIsNone(history_table)
        self.assertFalse(health_path.exists())
        self.assertFalse(
            health_path.with_name(f"{health_path.name}.activation.key").exists()
        )

    def test_initialized_adapter_malformed_nonpass_receipt_fails_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            path = self._append_result(
                config,
                run_id="malformed-nonpass",
                status="fail",
            )
            registry = replace(
                config.tests.registry,
                tests=tuple(
                    replace(test, enabled=test.id == "storage")
                    for test in config.tests.registry.tests
                ),
            )
            config = replace(
                config,
                tests=replace(config.tests, registry=registry),
                health_evaluator=replace(
                    config.health_evaluator,
                    write_enabled=True,
                ),
            )
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute(
                    "INSERT INTO metric_ingestion_receipts("
                    "run_id,test_id,adapter_api_version,evidence_digest,inserted_count,"
                    "updated_count,metric_names_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        "malformed-nonpass",
                        "storage",
                        "future.plugin.v2",
                        "sha256:" + hashlib.sha256(b"malformed-nonpass").hexdigest(),
                        1,
                        0,
                        '["metric"]',
                        20_005,
                    ),
                )
                connection.commit()
                self.assertIsNone(
                    connection.execute("PRAGMA foreign_key_check").fetchone()
                )

            reports = (
                evaluate_health_cycle(config, now=20_006),
                evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=20_007,
                ),
            )
            with closing(sqlite3.connect(path)) as connection:
                migrations = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                history_table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='classification_history'"
                ).fetchone()
            health_path = resolve_health_db_path(
                root,
                config.tests.registry.require("storage"),
            )

        for report in reports:
            with self.subTest(mode=report.mode):
                storage = report.tests[0]
                self.assertEqual(storage.status, "error")
                self.assertEqual(storage.error_stage, "read-preflight")
                self.assertEqual(storage.write_atomicity, "no-writes")
                self.assertFalse(storage.partial_writes)
                self.assertIn("durable receipt manifest is invalid", storage.error)
        self.assertEqual(migrations, [(1,)])
        self.assertIsNone(history_table)
        self.assertFalse(health_path.exists())

    def test_nonpassing_receipt_validates_every_manifest_field(self) -> None:
        registered = load_config().tests.registry.require("storage")
        combination = resolve_environment_combination(
            registered.definition,
            {"image_name": "image", "cuda_version": "12.9", "pytorch_version": "2.8"},
        )
        assert combination is not None
        raw_json = json.dumps(
            {
                "schema_version": "cval.test-result.v1",
                "status": "fail",
                "test_id": "storage",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        row = (
            1,
            "failed-with-receipt",
            "storage",
            "node-a",
            1,
            1,
            2,
            "fail",
            "image",
            "2.8",
            "12.9",
            validation_test_config_digest(registered),
            combination.key,
            raw_json,
            "sha256:" + hashlib.sha256(b"failed-with-receipt").hexdigest(),
            "storage",
            "cval.plugin.v1",
            "sha256:" + hashlib.sha256(b"receipt").hexdigest(),
            1,
            0,
            '["metric"]',
            2,
        )
        valid = _catalog_result_from_row(
            row,
            registered,
            adapter_version=1,
            expected_config_digest=validation_test_config_digest(registered),
        )
        self.assertEqual(valid.status, "fail")
        corruptions = {
            "owner": (15, "nccl"),
            "api": (16, "future.plugin.v2"),
            "digest": (17, "bad"),
            "inserted_count": (18, 0),
            "updated_count": (19, 1),
            "metric_names": (20, "[]"),
            "created_at": (21, -1),
        }
        for field_name, (index, value) in corruptions.items():
            malformed = list(row)
            malformed[index] = value
            with self.subTest(field=field_name), self.assertRaisesRegex(
                RuntimeError,
                "receipt",
            ):
                _catalog_result_from_row(
                    tuple(malformed),
                    registered,
                    adapter_version=1,
                    expected_config_digest=validation_test_config_digest(registered),
                )

    def test_reserved_uri_root_evaluates_and_applies_to_exact_u7_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "validation ?#% root"
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            path = self._append_result(config, run_id="failed", status="fail")

            dry = evaluate_health_cycle(config, now=20_002)
            applied = evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=20_003,
            )
            with closing(sqlite3.connect(path)) as connection:
                history_count = connection.execute(
                    "SELECT COUNT(*) FROM classification_history"
                ).fetchone()[0]

        dry_storage = next(test for test in dry.tests if test.test_id == "storage")
        applied_storage = next(
            test for test in applied.tests if test.test_id == "storage"
        )
        self.assertEqual(dry_storage.status, "processed")
        self.assertEqual(applied_storage.history_inserted, 1)
        self.assertEqual(history_count, 1)

    def test_u7_inode_and_selected_evidence_are_revalidated_before_writes(self) -> None:
        from cval.health import evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            path = self._append_result(config, run_id="failed", status="fail")
            replacement = path.with_name("replacement.db")
            shutil.copy2(path, replacement)
            original_migrate = evaluator_module.migrate_per_test_results_to_v2

            def replace_before_migration(*args, **kwargs):
                os.replace(replacement, path)
                return original_migrate(*args, **kwargs)

            with patch(
                "cval.health.evaluator.migrate_per_test_results_to_v2",
                side_effect=replace_before_migration,
            ):
                inode_report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=20_004,
                )
            inode_storage = next(
                test for test in inode_report.tests if test.test_id == "storage"
            )
            self.assertEqual(inode_storage.error_stage, "result-db-migration")
            self.assertIn("path/device/inode changed", inode_storage.error)

            original_store_history = evaluator_module.store_classification_history

            def mutate_before_history(records, **kwargs):
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute("DROP TRIGGER trg_test_results_immutable_update")
                    connection.execute(
                        "UPDATE test_results SET result_digest=? WHERE run_id='failed'",
                        ("sha256:" + "f" * 64,),
                    )
                    prepare_immutable_table_triggers(
                        connection,
                        "test_results",
                        COMMON_IMMUTABLE_KEY_GROUPS["test_results"],
                    )
                    connection.commit()
                return original_store_history(records, **kwargs)

            with patch(
                "cval.health.evaluator.store_classification_history",
                side_effect=mutate_before_history,
            ):
                evidence_report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=20_005,
                )
            evidence_storage = next(
                test for test in evidence_report.tests if test.test_id == "storage"
            )

        self.assertEqual(evidence_storage.error_stage, "classification-history")
        self.assertIn("Selected U7 raw/receipt evidence changed", evidence_storage.error)
        self.assertEqual(evidence_storage.history_inserted, 0)

    def test_candidate_store_precondition_and_u8_generation_block_stale_writes(self) -> None:
        from cval.health.storage import (
            assert_health_database_generation as real_generation_assert,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            path = self._append_result(config, run_id="failed", status="fail")
            dummy = SimpleNamespace(
                combination=SimpleNamespace(key="sha256:" + "a" * 64),
                source_snapshot=SourceSnapshot(()),
                baseline_id="hb1:" + "b" * 64,
            )
            planned = CandidateAction(
                dummy.combination.key,
                "would-store",
                dummy.baseline_id,
                1,
                1,
            )
            with patch(
                "cval.health.evaluator._plan_candidates",
                return_value=((planned,), (dummy,)),
            ), patch(
                "cval.health.evaluator.selected_result_evidence_guard",
                side_effect=RuntimeError("candidate U7 evidence changed"),
            ), patch("cval.health.evaluator._persist_candidate") as persist:
                candidate_report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=20_006,
                )
            persist.assert_not_called()
            candidate_storage = next(
                test for test in candidate_report.tests if test.test_id == "storage"
            )

            health_path = resolve_health_db_path(
                root,
                config.tests.registry.require("storage"),
            )

            def create_racing_generation(expected):
                health_path.touch()
                real_generation_assert(expected)

            with patch(
                "cval.health.evaluator.assert_health_database_generation",
                side_effect=create_racing_generation,
            ):
                generation_report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=20_007,
                )
            generation_storage = next(
                test for test in generation_report.tests if test.test_id == "storage"
            )
            with closing(sqlite3.connect(path)) as connection:
                generation_history_count = connection.execute(
                    "SELECT COUNT(*) FROM classification_history"
                ).fetchone()[0]

        self.assertEqual(candidate_storage.error_stage.split(":", 1)[0], "health-candidate")
        self.assertIn("candidate U7 evidence changed", candidate_storage.error)
        self.assertIsNone(candidate_storage.creation_cleanup_completed)
        self.assertEqual(generation_storage.error_stage, "health-generation-revalidation")
        self.assertEqual(generation_storage.history_inserted, 0)
        self.assertEqual(generation_history_count, 0)

    def test_candidate_store_reloads_current_adapter_metric_evidence(self) -> None:
        from cval.health import evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fixture = test_per_test_ingestion.ModularPerTestIngestionTests()
            config = fixture._config(root, enabled=True)
            for index in range(10):
                result_path = fixture._write_builtin_result(
                    root,
                    config,
                    statuses={
                        "storage": "pass",
                        "nccl": "fail",
                        "dltest": "fail",
                    },
                    run_id=f"node-a-{index + 1}",
                    timestamp=1_000 + index,
                )
                self.assertTrue(
                    test_per_test_ingestion._ingest(result_path, config).ok
                )
            config = replace(
                config,
                health_evaluator=replace(config.health_evaluator, write_enabled=True),
            )
            registered = config.tests.registry.require("storage")
            storage_path = resolve_test_results_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            health_path = resolve_health_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            original_guard = evaluator_module.selected_result_evidence_guard
            mutated = False

            @contextmanager
            def corrupt_before_guard(db_path, **kwargs):
                nonlocal mutated
                if Path(db_path) == storage_path and not mutated:
                    with closing(sqlite3.connect(storage_path)) as connection:
                        test_health_plugins._mutate_behind_immutable_trigger(
                            connection,
                            "UPDATE storage_performance "
                            "SET randread_iops=randread_iops+1 "
                            "WHERE run_id='node-a-1'",
                        )
                        connection.commit()
                    mutated = True
                with original_guard(db_path, **kwargs) as connection:
                    yield connection

            with patch(
                "cval.health.evaluator.selected_result_evidence_guard",
                new=corrupt_before_guard,
            ):
                report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=30_000,
                )
            storage = next(test for test in report.tests if test.test_id == "storage")
            health_exists = health_path.exists()

        self.assertTrue(mutated)
        self.assertEqual(storage.status, "error")
        self.assertTrue(
            "receipt does not match metric content" in storage.error
            or "adapter evidence changed" in storage.error
        )
        self.assertEqual(storage.candidates_inserted, 0)
        self.assertFalse(storage.health_db_present)
        self.assertFalse(storage.activation_key_present)
        self.assertFalse(health_exists)

    def test_failed_first_health_creation_report_matches_staged_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            self._append_result(config, run_id="failed", status="fail")
            dummy = SimpleNamespace(
                combination=SimpleNamespace(key="sha256:" + "c" * 64),
                source_snapshot=SourceSnapshot(()),
                baseline_id="hb1:" + "d" * 64,
            )
            planned = CandidateAction(
                dummy.combination.key,
                "would-store",
                dummy.baseline_id,
                1,
                1,
            )
            with patch(
                "cval.health.evaluator._plan_candidates",
                return_value=((planned,), (dummy,)),
            ), patch(
                "cval.health.evaluator.selected_result_evidence_guard"
            ), patch(
                "cval.health.evaluator._assert_candidate_rebuild"
            ), patch(
                "cval.health.evaluator._persist_candidate",
                side_effect=RuntimeError("staged first-create failure"),
            ):
                report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=20_008,
                )
            storage = next(test for test in report.tests if test.test_id == "storage")

        self.assertEqual(storage.status, "error")
        self.assertFalse(storage.health_db_present)
        self.assertFalse(storage.activation_key_present)
        self.assertTrue(storage.creation_cleanup_completed)
        self.assertEqual(storage.candidates_inserted, 0)

    def test_versioned_target_appends_and_changed_same_target_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            path = self._append_result(config, run_id="failed", status="fail")
            first = evaluate_health_cycle(
                config, apply=True, confirmation=EVALUATE_CONFIRMATION, now=30_000
            )
            second = evaluate_health_cycle(
                config, apply=True, confirmation=EVALUATE_CONFIRMATION, now=30_001
            )
            from cval.health import evaluator as evaluator_module

            original_history_record = evaluator_module._history_record

            def changed_verdict_evidence(*args, **kwargs):
                record = original_history_record(*args, **kwargs)
                changed = replace(
                    record,
                    dnr_reason="no_observations",
                    details_json='{"dnr_reason":"no_observations"}',
                )
                return replace(
                    changed,
                    evidence_digest=_classification_evidence_digest(changed),
                )

            with patch(
                "cval.health.evaluator._history_record",
                side_effect=changed_verdict_evidence,
            ):
                changed_verdict = evaluate_health_cycle(
                    config, apply=True, confirmation=EVALUATE_CONFIRMATION, now=30_002
                )
            with patch("cval.health.evaluator.HEALTH_ENGINE_VERSION", "cval.health.v2"):
                versioned = evaluate_health_cycle(
                    config, apply=True, confirmation=EVALUATE_CONFIRMATION, now=30_003
                )
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute("DROP TRIGGER trg_test_results_immutable_update")
                    connection.execute(
                        "UPDATE test_results SET result_digest=? WHERE run_id='failed'",
                        ("sha256:" + "f" * 64,),
                    )
                    prepare_immutable_table_triggers(
                        connection,
                        "test_results",
                        COMMON_IMMUTABLE_KEY_GROUPS["test_results"],
                    )
                    connection.commit()
                conflict = evaluate_health_cycle(
                    config, apply=True, confirmation=EVALUATE_CONFIRMATION, now=30_004
                )
            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute(
                    "SELECT evaluator_version,baseline_identity,target_digest "
                    "FROM classification_history ORDER BY classification_id"
                ).fetchall()

        first_storage = next(test for test in first.tests if test.test_id == "storage")
        second_storage = next(test for test in second.tests if test.test_id == "storage")
        versioned_storage = next(test for test in versioned.tests if test.test_id == "storage")
        changed_verdict_storage = next(
            test for test in changed_verdict.tests if test.test_id == "storage"
        )
        conflict_storage = next(test for test in conflict.tests if test.test_id == "storage")
        self.assertEqual(first_storage.history_inserted, 1)
        self.assertEqual(second_storage.history_idempotent, 1)
        self.assertEqual(changed_verdict_storage.status, "error")
        self.assertIn("different verdict evidence", changed_verdict_storage.error)
        self.assertFalse(changed_verdict_storage.partial_writes)
        self.assertEqual(versioned_storage.history_inserted, 1)
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0][1:], rows[1][1:])
        self.assertEqual(conflict_storage.status, "error")
        self.assertEqual(conflict_storage.error_stage, "read-preflight")
        self.assertIn("different canonical evidence", conflict_storage.error)

    def test_preflight_precedes_writes_and_late_failure_reports_partial_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            path = self._append_result(config, run_id="failed", status="fail")
            with patch(
                "cval.health.evaluator._evaluate_classifications",
                side_effect=RuntimeError("preflight probe"),
            ):
                preflight = evaluate_health_cycle(
                    config, apply=True, confirmation=EVALUATE_CONFIRMATION, now=40_000
                )
            with closing(sqlite3.connect(path)) as connection:
                versions_before = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
            with patch(
                "cval.health.evaluator.store_classification_history",
                side_effect=RuntimeError("late history probe"),
            ):
                late = evaluate_health_cycle(
                    config, apply=True, confirmation=EVALUATE_CONFIRMATION, now=40_001
                )
            with closing(sqlite3.connect(path)) as connection:
                versions_after = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()

        preflight_storage = next(test for test in preflight.tests if test.test_id == "storage")
        late_storage = next(test for test in late.tests if test.test_id == "storage")
        self.assertEqual(versions_before, [(1,)])
        self.assertEqual(preflight_storage.error_stage, "read-preflight")
        self.assertFalse(preflight_storage.partial_writes)
        self.assertEqual(versions_after, [(1,), (2,)])
        self.assertEqual(late_storage.error_stage, "classification-history")
        self.assertTrue(late_storage.migrated_to_v2)
        self.assertTrue(late_storage.partial_writes)
        self.assertIn("cross-database non-atomic", late_storage.write_atomicity)

    def test_table_output_reports_backlog_migration_counts_and_partial_writes(self) -> None:
        from cval.cli import handle_health_evaluate

        test_report = TestEvaluationReport(
            test_id="storage",
            status="error",
            result_db_path="/tmp/storage_results.db",
            health_db_path="/tmp/storage_health.db",
            source_schema_version=2,
            result_count=20,
            candidate_source_count=10,
            classification_selected_count=3,
            deferred_count=4,
            classification_backlog=9,
            classification_remaining=6,
            classification_truncated=True,
            candidates=(CandidateAction("sha256:" + "a" * 64, "stored", None, 10, 2),),
            history_inserted=2,
            history_idempotent=1,
            migrated_to_v2=True,
            candidates_inserted=1,
            candidates_idempotent=2,
            partial_writes=True,
            error_stage="classification-history",
            write_atomicity="per-database transactions; cross-database non-atomic",
            error="late failure",
        )
        cycle = EvaluationCycleReport(
            mode="apply",
            evaluator_version="cval.health.v1",
            write_enabled=True,
            started_at=1,
            tests=(test_report,),
        )
        args = SimpleNamespace(
            cval_config=load_config(),
            apply=True,
            confirm=EVALUATE_CONFIRMATION,
            output="table",
        )
        output = io.StringIO()
        with patch(
            "cval.health.evaluator.evaluate_health_cycle",
            return_value=cycle,
        ), redirect_stdout(output):
            exit_code = handle_health_evaluate(args)
        rendered = output.getvalue()

        self.assertEqual(exit_code, 1)
        for fragment in (
            "deferred=4",
            "backlog=9",
            "remaining=6",
            "migrated_to_v2=true",
            "candidates=1",
            "candidate_inserted=1",
            "candidate_idempotent=2",
            "history_inserted=2",
            "history_idempotent=1",
            "partial_durable_writes=true",
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, rendered)

    def test_apply_requires_independent_gate_and_exact_confirmation(self) -> None:
        config = load_config()
        with self.assertRaises(HealthEvaluatorPolicyError):
            evaluate_health_cycle(config, apply=True, confirmation=EVALUATE_CONFIRMATION)
        enabled = replace(
            config,
            health_evaluator=replace(config.health_evaluator, write_enabled=True),
        )
        for confirmation in (None, "Evaluate", "submit"):
            with self.subTest(confirmation=confirmation), self.assertRaises(
                HealthEvaluatorPolicyError
            ):
                evaluate_health_cycle(enabled, apply=True, confirmation=confirmation)
        with self.assertRaises(HealthEvaluatorPolicyError):
            evaluate_health_cycle(enabled, confirmation=EVALUATE_CONFIRMATION)

    def test_apply_migrates_stores_dnr_idempotently_and_never_updates_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            config = replace(
                config,
                health_evaluator=replace(config.health_evaluator, write_enabled=True),
            )

            first = evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=9_999_999_999,
            )
            second = evaluate_health_cycle(
                config,
                apply=True,
                confirmation=EVALUATE_CONFIRMATION,
                now=10_000_000_000,
            )
            rows = {}
            caches = {}
            for test_id in ("storage", "nccl", "dltest"):
                db_path = root / f"evaluator_state/validation_tests/{test_id}/{test_id}_results.db"
                with closing(sqlite3.connect(db_path)) as connection:
                    rows[test_id] = connection.execute(
                        "SELECT health_class_numerical, dnr_reason, baseline_id "
                        "FROM classification_history"
                    ).fetchall()
                    caches[test_id] = connection.execute(
                        "SELECT health_class_name, health_class_numerical, "
                        "health_baseline_id, evaluated_at FROM test_results"
                    ).fetchall()

        self.assertEqual(sum(test.history_inserted for test in first.tests), 3)
        self.assertTrue(all(test.migrated_to_v2 for test in first.tests))
        self.assertEqual(sum(test.history_inserted for test in second.tests), 0)
        self.assertTrue(
            all(
                len(test.classifications) == 1
                and test.classifications[0].action == "idempotent"
                and test.history_idempotent == 1
                for test in second.tests
            )
        )
        self.assertTrue(
            all(value == [(5, "no_active_baseline", None)] for value in rows.values())
        )
        self.assertTrue(all(value == [(None, None, None, None)] for value in caches.values()))

    def test_classes_raw_fail_incomplete_missing_combination_no_baseline_and_defer(self) -> None:
        registered = load_config().tests.registry.require("storage")
        combination = resolve_environment_combination(
            registered.definition,
            {"image_name": "image", "cuda_version": "12.9", "pytorch_version": "2.8"},
        )
        assert combination is not None
        source = SourceResult(
            3,
            "pass",
            3,
            "sha256:" + "1" * 64,
            "sha256:" + "2" * 64,
            validation_test_config_digest(registered),
            combination.key,
            1,
            "sha256:" + "3" * 64,
        )
        classification_results = (
                CatalogResult(1, "failed", "fail", combination, None, None),
                CatalogResult(2, "incomplete", "incomplete", combination, None, None),
                CatalogResult(3, "pass", "pass", combination, source, None),
                CatalogResult(4, "missing-combination", "pass", None, None, None),
            )
        catalog = ResultCatalog(
            schema_version=1,
            result_count=5,
            candidate_results=(),
            classification_results=classification_results,
            classification_backlog=4,
            adapter_schema_initialized=True,
            deferred_results=(
                CatalogResult(
                    5,
                    "deferred",
                    "pass",
                    combination,
                    None,
                    "missing receipt",
                ),
            ),
            deferred_count=1,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            actions, records, idempotent = _evaluate_classifications(
                load_config(),
                registered,
                object(),
                catalog,
                health_path=Path(tmpdir) / "missing-health.db",
                result_path=Path(tmpdir) / "unused-results.db",
                now=10,
            )

        self.assertEqual(
            [record.dnr_reason for record in records],
            ["raw_failed", "raw_incomplete", "no_active_baseline", "missing_combination"],
        )
        self.assertTrue(all(record.health_class_numerical == 5 for record in records))
        self.assertEqual(actions[-1].action, "deferred")
        self.assertEqual(idempotent, 0)

    def test_candidate_trigger_builds_via_plugin_and_never_auto_activates(self) -> None:
        config = load_config()
        registered = config.tests.registry.require("storage")
        self.assertFalse(registered.definition.health.auto_activate)
        combination = resolve_environment_combination(
            registered.definition,
            {"image_name": "image", "cuda_version": "12.9", "pytorch_version": "2.8"},
        )
        assert combination is not None
        sources = tuple(
            SourceResult(
                index,
                f"run-{index}",
                index,
                "sha256:" + f"{index:064x}",
                "sha256:" + f"{index + 100:064x}",
                validation_test_config_digest(registered),
                combination.key,
                1,
                "sha256:" + f"{index + 200:064x}",
            )
            for index in range(1, 11)
        )
        catalog = ResultCatalog(
            schema_version=1,
            result_count=len(sources),
            candidate_results=tuple(
                CatalogResult(
                    source.result_id,
                    source.run_id,
                    "pass",
                    combination,
                    source,
                    None,
                )
                for source in sources
            ),
            classification_results=(),
            classification_backlog=0,
            adapter_schema_initialized=True,
        )

        class Plugin:
            health_policy_version = "storage.health.v1"

            def metric_specs(self, definition):
                return metric_specs_from_definition(definition)

            def load_observations(self, context):
                return tuple(
                    MetricObservation(
                        result_id=source.result_id,
                        run_id=source.run_id,
                        completed_timestamp=source.completed_timestamp,
                        source="storage_performance",
                        metric_name="randread_iops",
                        sample_key="randread_iops",
                        value=100.0 + source.result_id,
                    )
                    for source in context.source_snapshot.results
                )

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "cval.health.evaluator.get_chain_cursor",
            return_value=HealthChainCursor(
                "storage", combination.key, None, None, ()
            ),
        ), patch("cval.health.evaluator._activate_candidate") as activate:
            actions = _evaluate_candidates(
                config,
                registered,
                Plugin(),
                catalog,
                health_path=Path(tmpdir) / "missing-health.db",
                result_path=Path(tmpdir) / "results.db",
                apply=False,
                now=100,
            )

        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].action, "would-store")
        self.assertTrue(actions[0].baseline_id.startswith("hb1:"))
        activate.assert_not_called()

    def test_one_test_failure_does_not_block_other_registry_tests(self) -> None:
        config = load_config()

        def outcome(_config, registered, **_kwargs):
            if registered.id == "nccl":
                raise RuntimeError("broken adapter")
            return TestEvaluationReport(
                registered.id,
                "skipped",
                "result.db",
                "health.db",
                None,
                0,
            )

        with patch("cval.health.evaluator._evaluate_registered_test", side_effect=outcome):
            report = evaluate_health_cycle(config, now=10)

        self.assertEqual([test.test_id for test in report.tests], ["storage", "nccl", "dltest"])
        self.assertEqual([test.status for test in report.tests], ["skipped", "error", "skipped"])
        self.assertNotIn("Traceback", report.tests[1].error)

    def test_synthetic_fourth_health_test_is_registry_enumerated_without_core_edit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "repo"
            test_dir = repo / "validation-tests/fourth"
            test_dir.mkdir(parents=True)
            for name in ("setup.sh", "run-test.sh"):
                (test_dir / name).write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
            (test_dir / "plugin.py").write_text(
                "CVAL_PLUGIN_API='cval.plugin.v1'\nPLUGIN=object()\n",
                encoding="utf-8",
            )
            (test_dir / "test_config.toml").write_text(
                '''
schema_version = "cval.test.v1"
[test]
id = "fourth"
display_name = "Fourth"
order = 40
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
results_db_path = "validation_tests/fourth/fourth_results.db"
health_classes_db_path = "validation_tests/fourth/fourth_health_classes.db"
[plugin]
adapter = "plugin.py"
api_version = "cval.plugin.v1"
capabilities = ["ingest", "health"]
[health]
enabled = true
policy_version = "fourth.health.v1"
min_samples = 3
min_new_results = 1
combination_factors = ["image_name"]
auto_activate = false
[[health.metrics]]
name = "metric"
source = "metric"
direction = "low_bad"
tolerance_pct = 1.0
''',
                encoding="utf-8",
            )
            registry = load_test_registry(
                {
                    "fourth": {
                        "enabled": True,
                        "config_path": "validation-tests/fourth/test_config.toml",
                    }
                },
                repo_root=repo,
                include_defaults=False,
            )
            base = load_config()
            config = replace(
                base,
                runtime=replace(base.runtime, validation_root=str(root)),
                tests=replace(base.tests, registry=registry),
            )

            report = evaluate_health_cycle(config, now=10)

        self.assertEqual(len(report.tests), 1)
        self.assertEqual(report.tests[0].test_id, "fourth")
        self.assertEqual(report.tests[0].status, "skipped")
        self.assertIn("missing", report.tests[0].error)

    def test_manual_activation_is_dry_run_then_double_gated_apply(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root)
            registered = config.tests.registry.require("storage")
            result_path = resolve_test_results_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            with state_test_lock(config, result_path), bind_state_target(
                config,
                result_path,
                create=True,
                allow_missing=False,
                writable=True,
                require_writable=True,
            ):
                pass
            locks_before = set(result_path.parent.glob("*.lock"))
            health_path = resolve_health_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            baseline_id = "hb1:" + "a" * 64
            preflight = ActivationPreflight(
                baseline_id,
                "storage",
                "sha256:" + "b" * 64,
                BaselineLifecycle.CANDIDATE,
                True,
                False,
                None,
            )
            active = replace(
                preflight,
                lifecycle=BaselineLifecycle.ACTIVE,
                already_active=True,
                current_active_baseline_id=baseline_id,
            )
            with patch(
                "cval.health.evaluator.preflight_activation",
                return_value=preflight,
            ) as check, patch("cval.health.evaluator._activate_candidate") as activate:
                dry = activate_health_candidate(config, "storage", baseline_id, now=10)
            self.assertEqual(dry.mode, "dry-run")
            self.assertFalse(dry.activated)
            check.assert_called_once()
            activate.assert_not_called()
            self.assertEqual(set(result_path.parent.glob("*.lock")), locks_before)
            self.assertFalse(health_path.exists())

            enabled = replace(
                config,
                health_evaluator=replace(config.health_evaluator, write_enabled=True),
            )
            health_path.touch(mode=0o600)
            os.chmod(health_path, 0o600)
            key_path = health_path.with_name(f"{health_path.name}.activation.key")
            key_path.write_bytes(b"k" * 32)
            os.chmod(key_path, 0o600)
            with self.assertRaises(HealthEvaluatorPolicyError):
                activate_health_candidate(
                    enabled,
                    "storage",
                    baseline_id,
                    apply=True,
                    confirmation="wrong",
                )
            with patch(
                "cval.health.evaluator.preflight_activation",
                side_effect=(preflight, active),
            ), patch(
                "cval.health.evaluator._activate_candidate",
                return_value=True,
            ) as activate:
                applied = activate_health_candidate(
                    enabled,
                    "storage",
                    baseline_id,
                    apply=True,
                    confirmation=ACTIVATE_CONFIRMATION,
                    now=20,
                )
            self.assertTrue(applied.activated)
            activate.assert_called_once()

    def test_production_activation_has_no_public_or_unlocked_path(self) -> None:
        import inspect
        import cval.health.storage as storage_module

        self.assertFalse(hasattr(storage_module, "activate_candidate"))
        signature = inspect.signature(storage_module._activate_candidate)
        for name in ("lock_guard", "state_binding", "key_binding"):
            self.assertIs(signature.parameters[name].default, inspect.Parameter.empty)
        with self.assertRaisesRegex(RuntimeError, "retained DB/key bindings"):
            storage_module._activate_candidate(
                "hb1:" + "a" * 64,
                load_config().tests.registry.require("storage").definition,
                db_path=Path("/must-not-open.db"),
                lock_guard=None,
                state_binding=None,
                key_binding=None,
            )

    def test_u9_apply_and_activation_reject_wrong_fixed_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            wrong_owner = replace(
                config,
                health_evaluator=replace(
                    config.health_evaluator,
                    write_enabled=True,
                    state_owner_uid=os.geteuid() + 1,
                ),
            )

            dry = evaluate_health_cycle(wrong_owner, now=50_000)
            self.assertTrue(dry.ok)
            with self.assertRaisesRegex(PermissionError, "process owner mismatch"):
                evaluate_health_cycle(
                    wrong_owner,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=50_001,
                )
            with self.assertRaisesRegex(PermissionError, "process owner mismatch"):
                activate_health_candidate(
                    wrong_owner,
                    "storage",
                    "hb1:" + "a" * 64,
                    apply=True,
                    confirmation=ACTIVATE_CONFIRMATION,
                    now=50_002,
                )

    def test_apply_creates_nested_health_ancestry_only_after_lock_is_held(self) -> None:
        import cval.health.evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            registered = config.tests.registry.require("storage")
            result_path = resolve_test_results_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            health_path = result_path.parent / "nested/health.db"
            self.assertFalse(health_path.parent.exists())
            original_lock = evaluator_module.state_test_lock
            original_bind_directory = evaluator_module.bind_state_directory
            lock_held = False
            creation_observed = False

            @contextmanager
            def observed_lock(*args, **kwargs):
                nonlocal lock_held
                self.assertFalse(health_path.parent.exists())
                with original_lock(*args, **kwargs) as guard:
                    lock_held = True
                    try:
                        yield guard
                    finally:
                        lock_held = False

            @contextmanager
            def observed_directory(*args, **kwargs):
                nonlocal creation_observed
                self.assertTrue(lock_held)
                self.assertFalse(health_path.parent.exists())
                with original_bind_directory(*args, **kwargs) as binding:
                    creation_observed = True
                    self.assertTrue(health_path.parent.is_dir())
                    yield binding

            with patch.object(
                evaluator_module,
                "resolve_health_db_path",
                return_value=health_path,
            ), patch.object(
                evaluator_module,
                "state_test_lock",
                observed_lock,
            ), patch.object(
                evaluator_module,
                "bind_state_directory",
                observed_directory,
            ):
                _evaluate_registered_test(
                    config,
                    registered,
                    apply=True,
                    now=50_100,
                )
            self.assertTrue(creation_observed)

    def test_activation_binds_existing_state_only_after_lock_is_held(self) -> None:
        import cval.health.evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._prepared(root)
            config = replace(
                config,
                health_evaluator=replace(
                    config.health_evaluator,
                    write_enabled=True,
                ),
            )
            registered = config.tests.registry.require("storage")
            health_path = resolve_health_db_path(
                config.health_evaluator.state_root,
                registered,
            )
            health_path.write_bytes(b"existing")
            os.chmod(health_path, 0o600)
            key_path = health_path.with_name(f"{health_path.name}.activation.key")
            key_path.write_bytes(b"k" * 32)
            os.chmod(key_path, 0o600)
            baseline_id = "hb1:" + "a" * 64
            preflight = ActivationPreflight(
                baseline_id,
                "storage",
                "sha256:" + "b" * 64,
                BaselineLifecycle.CANDIDATE,
                True,
                False,
                None,
            )
            active = replace(
                preflight,
                lifecycle=BaselineLifecycle.ACTIVE,
                already_active=True,
                current_active_baseline_id=baseline_id,
            )
            original_lock = evaluator_module.state_test_lock
            original_bind_target = evaluator_module.bind_state_target
            lock_held = False
            bound_paths: list[Path] = []

            @contextmanager
            def observed_lock(*args, **kwargs):
                nonlocal lock_held
                self.assertEqual(bound_paths, [])
                with original_lock(*args, **kwargs) as guard:
                    lock_held = True
                    try:
                        yield guard
                    finally:
                        lock_held = False

            @contextmanager
            def observed_target(*args, **kwargs):
                self.assertTrue(lock_held)
                bound_paths.append(Path(args[1]))
                with original_bind_target(*args, **kwargs) as binding:
                    yield binding

            with patch.object(
                evaluator_module,
                "state_test_lock",
                observed_lock,
            ), patch.object(
                evaluator_module,
                "bind_state_target",
                observed_target,
            ), patch.object(
                evaluator_module,
                "preflight_activation",
                side_effect=(preflight, active),
            ), patch.object(
                evaluator_module,
                "_activate_candidate",
                return_value=True,
            ):
                report = activate_health_candidate(
                    config,
                    "storage",
                    baseline_id,
                    apply=True,
                    confirmation=ACTIVATE_CONFIRMATION,
                    now=50_200,
                )
            self.assertTrue(report.activated)
            self.assertEqual(len(bound_paths), 3)

    def test_apply_lock_is_owner_only_bounded_and_rejects_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._state_config(load_config(), root)
            result_path = root / "test_results.db"
            result_path.touch()
            with state_test_lock(config, result_path, timeout_seconds=1) as lock_guard:
                lock_path = lock_guard.path
                lock_guard()
                self.assertEqual(lock_path.stat().st_mode & 0o777, 0o600)
                descriptor = os.open(lock_path, os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(descriptor)

            lock_path.unlink()
            outside = Path(tmpdir) / "outside.lock"
            outside.touch(mode=0o600)
            lock_path.symlink_to(outside)
            with self.assertRaises((ValueError, OSError, StateLockError)):
                with state_test_lock(config, result_path, timeout_seconds=1):
                    pass

            lock_path.unlink()
            wrong_owner = replace(
                config,
                health_evaluator=replace(
                    config.health_evaluator,
                    state_owner_gid=os.getegid() + 1,
                ),
            )
            with self.assertRaises(PermissionError):
                with state_test_lock(wrong_owner, result_path, timeout_seconds=1):
                    pass

    def test_lock_replacement_allows_split_open_but_blocks_commit_and_completion(self) -> None:
        from cval.health import evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            result_path = self._append_result(config, run_id="failed", status="fail")
            original_migrate = evaluator_module.migrate_per_test_results_to_v2
            split_entered = False

            def split_lock_before_migration(db_path, **kwargs):
                nonlocal split_entered
                first_guard = kwargs["lock_guard"]
                lock_path = first_guard.path
                lock_path.unlink()
                descriptor = os.open(
                    lock_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                os.close(descriptor)
                with state_test_lock(
                    config,
                    result_path,
                    timeout_seconds=1,
                ) as second_guard:
                    split_entered = True
                    second_guard()
                    return original_migrate(db_path, **kwargs)

            with patch(
                "cval.health.evaluator.migrate_per_test_results_to_v2",
                side_effect=split_lock_before_migration,
            ):
                report = evaluate_health_cycle(
                    config,
                    apply=True,
                    confirmation=EVALUATE_CONFIRMATION,
                    now=20_008,
                )
            storage = next(test for test in report.tests if test.test_id == "storage")
            with closing(sqlite3.connect(result_path)) as connection:
                versions = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                history_table = connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='classification_history'"
                ).fetchone()

        self.assertTrue(split_entered)
        self.assertEqual(storage.status, "error")
        self.assertIn("lock", storage.error.lower())
        self.assertIn("changed", storage.error.lower())
        self.assertEqual(versions, [(1,)])
        self.assertIsNone(history_table)

    def test_committed_migration_report_survives_split_lock_finalization(self) -> None:
        from cval.health import evaluator as evaluator_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = load_config()
            config = self._state_config(base, root, write_enabled=True)
            result_path = self._append_result(config, run_id="failed", status="fail")
            original_migrate = evaluator_module.migrate_per_test_results_to_v2
            competitor_lock = None
            competitor_guard = None

            def migrate_then_split(db_path, **kwargs):
                nonlocal competitor_lock, competitor_guard
                migrated = original_migrate(db_path, **kwargs)
                first_guard = kwargs["lock_guard"]
                first_guard.path.unlink()
                replacement = os.open(
                    first_guard.path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                os.close(replacement)
                competitor_lock = state_test_lock(
                    config,
                    result_path,
                    timeout_seconds=1,
                )
                competitor_guard = competitor_lock.__enter__()
                competitor_guard()
                return migrated

            try:
                with patch(
                    "cval.health.evaluator.migrate_per_test_results_to_v2",
                    side_effect=migrate_then_split,
                ):
                    report = evaluate_health_cycle(
                        config,
                        apply=True,
                        confirmation=EVALUATE_CONFIRMATION,
                        now=20_009,
                    )
            finally:
                if competitor_lock is not None:
                    competitor_lock.__exit__(None, None, None)
            storage = next(test for test in report.tests if test.test_id == "storage")
            with closing(sqlite3.connect(result_path)) as connection:
                versions = connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()
                history_count = connection.execute(
                    "SELECT COUNT(*) FROM classification_history"
                ).fetchone()[0]

        self.assertIsNotNone(competitor_guard)
        self.assertEqual(storage.status, "error")
        self.assertEqual(storage.error_stage, "classification-history")
        self.assertEqual(storage.source_schema_version, 1)
        self.assertTrue(storage.migrated_to_v2)
        self.assertTrue(storage.partial_writes)
        self.assertEqual(
            storage.write_stages_completed,
            ("result-db-migration",),
        )
        self.assertEqual(
            storage.write_atomicity,
            "per-database transactions; cross-database non-atomic",
        )
        self.assertIn("lock finalization failed", storage.error.lower())
        self.assertIn("lock", storage.error.lower())
        self.assertEqual(versions, [(1,), (2,)])
        self.assertEqual(history_count, 0)

    def test_cli_health_gate_error_is_structured_without_trace(self) -> None:
        from cval.cli import main

        output = io.StringIO()
        with redirect_stdout(output):
            exit_code = main(
                ["health", "evaluate", "--apply", "--confirm", "evaluate", "--output", "json"]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 2)
        self.assertFalse(payload["ok"])
        self.assertNotIn("Traceback", payload["error"])


if __name__ == "__main__":
    unittest.main()
