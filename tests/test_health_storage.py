from __future__ import annotations

import os
import shutil
import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import cval.health.storage as health_storage
from cval.health.combination import canonicalize_factors
from cval.health.engine import (
    _build_declarative_candidate,
    _candidate_identity,
    metric_specs_from_definition,
)
from cval.health.evaluator import HealthEvaluatorLockError, evaluator_test_lock
from cval.health.models import (
    BaselineLifecycle,
    HealthContext,
    MetricObservation,
    SourceResult,
    SourceSnapshot,
)
from cval.health.storage import (
    activate_candidate,
    get_active_baseline,
    get_chain_cursor,
    list_baselines,
    load_baseline,
    load_build_state,
    persist_candidate_from_plugin,
    preflight_activation,
    _store_candidate as store_candidate,
    store_candidate_from_plugin,
)
from tests.test_health_engine import definition
from cval.validation.registry import validation_test_config_digest


def mutate_behind_trigger(
    connection: sqlite3.Connection,
    trigger_name: str,
    sql: str,
    parameters: tuple = (),
) -> None:
    """Model offline file corruption while restoring the exact trigger manifest."""

    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
        (trigger_name,),
    ).fetchone()
    if row is None or not isinstance(row[0], str):
        raise AssertionError(f"missing trigger {trigger_name}")
    connection.execute(f'DROP TRIGGER "{trigger_name}"')
    connection.execute(sql, parameters)
    connection.execute(row[0])


def candidate(
    *,
    value: float = 100.0,
    count: int = 3,
    parent: str | None = None,
    image: str = "img",
    created_at: int = 100,
    min_new_results: int = 2,
):
    test_definition = definition(min_samples=3)
    test_definition = replace(
        test_definition,
        health=replace(
            test_definition.health,
            min_new_results=min_new_results,
        ),
    )
    combination = canonicalize_factors({"image_name": image})
    source = SourceSnapshot(
        tuple(
            SourceResult(
                index,
                f"run-{image}-{index}",
                index,
                "sha256:" + f"{index:064x}",
                "sha256:" + f"{index + 100:064x}",
                validation_test_config_digest(test_definition),
                combination.key,
                1,
                "sha256:" + f"{index + 200:064x}",
            )
            for index in range(1, count + 1)
        )
    )
    values = tuple(
        MetricObservation(
            result_id=index,
            run_id=f"run-{image}-{index}",
            completed_timestamp=index,
            source="source-a",
            metric_name="expanded-metric",
            sample_key="sample",
            value=value,
        )
        for index in range(1, count + 1)
    )
    return test_definition, _build_declarative_candidate(
        test_definition,
        combination,
        metric_specs_from_definition(test_definition),
        values,
        source,
        parent_baseline_id=parent,
        created_at=created_at,
    )


class HealthStorageTests(unittest.TestCase):
    def test_typed_store_outcome_authoritative_cursor_and_activation_preflight(self) -> None:
        test_definition, built = candidate()

        class Plugin:
            health_policy_version = "smoke.health.v1"

            def metric_specs(self, active_definition):
                return metric_specs_from_definition(active_definition)

            def load_observations(self, _context):
                return built.observations

        context = HealthContext(
            definition=test_definition,
            result_db_path=Path("/tmp/unused.db"),
            combination=built.combination,
            source_snapshot=built.source_snapshot,
            created_at=built.created_at,
        )
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            first = persist_candidate_from_plugin(Plugin(), context, db_path=path, now=150)
            second = persist_candidate_from_plugin(Plugin(), context, db_path=path, now=160)
            cursor = get_chain_cursor(
                "smoke",
                built.combination.key,
                db_path=path,
            )
            readiness = preflight_activation(
                built.baseline_id,
                test_definition,
                db_path=path,
            )

        self.assertTrue(first.stored)
        self.assertFalse(second.stored)
        self.assertEqual(cursor.latest_candidate_id, built.baseline_id)
        self.assertIsNone(cursor.active_baseline_id)
        self.assertEqual(cursor.latest_source_result_ids, (1, 2, 3))
        self.assertTrue(readiness.activation_ready)
        self.assertFalse(readiness.already_active)
    def test_public_store_loads_plugin_observations_and_binds_activation_key(self) -> None:
        test_definition, built = candidate()

        class Plugin:
            health_policy_version = "smoke.health.v1"

            def metric_specs(self, active_definition):
                return metric_specs_from_definition(active_definition)

            def load_observations(self, _context):
                return built.observations

        context = HealthContext(
            definition=test_definition,
            result_db_path=Path("/tmp/unused.db"),
            combination=built.combination,
            source_snapshot=built.source_snapshot,
            created_at=built.created_at,
        )
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            stored = store_candidate_from_plugin(
                Plugin(),
                context,
                db_path=path,
                now=150,
            )
            key_path = path.with_name(f"{path.name}.activation.key")
            self.assertEqual(key_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(stored.baseline_id, built.baseline_id)
            key_path.unlink()
            with self.assertRaisesRegex(RuntimeError, "activation key"):
                load_baseline(stored.baseline_id, db_path=path)

    def test_activation_key_read_is_noatime_nofollow_and_exact_owner_mode(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path, now=150)
            key_path = path.with_name(f"{path.name}.activation.key")
            metadata = key_path.stat()
            os.utime(
                key_path,
                ns=(1_000_000_000, metadata.st_mtime_ns),
            )
            before_atime = key_path.stat().st_atime_ns
            self.assertIsNotNone(load_baseline(built.baseline_id, db_path=path))
            self.assertEqual(key_path.stat().st_atime_ns, before_atime)

            key_path.chmod(0o640)
            with self.assertRaisesRegex(RuntimeError, "permissions"):
                load_baseline(built.baseline_id, db_path=path)
            key_path.chmod(0o600)
            real_key = path.with_name("outside.key")
            key_path.rename(real_key)
            key_path.symlink_to(real_key)
            with self.assertRaisesRegex((ValueError, RuntimeError), "symlink|unsafe"):
                load_baseline(built.baseline_id, db_path=path)

    def test_activation_key_value_detects_mode_size_unlink_and_replacement(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path, now=150)
            key_path = path.with_name(f"{path.name}.activation.key")
            captured = health_storage._load_activation_key(path)
            self.assertEqual(captured.bytes, key_path.read_bytes())
            self.assertEqual(captured.path, key_path)
            self.assertEqual(captured.mode, 0o600)
            self.assertEqual(captured.size, 32)

            key_path.chmod(0o640)
            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                health_storage._assert_activation_key_identity(captured)
            key_path.chmod(0o600)

            key_path.write_bytes(captured.bytes + b"x")
            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                health_storage._assert_activation_key_identity(captured)
            key_path.write_bytes(captured.bytes)

            displaced = key_path.with_name("displaced.key")
            key_path.rename(displaced)
            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                health_storage._assert_activation_key_identity(captured)
            key_path.write_bytes(captured.bytes)
            key_path.chmod(0o600)
            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                health_storage._assert_activation_key_identity(captured)
            key_path.unlink()
            displaced.rename(key_path)
            health_storage._assert_activation_key_identity(captured)

    def test_reserved_sqlite_path_roundtrips_and_failed_first_create_cleans_staging(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health ?#% literal.db"
            with patch(
                "cval.health.storage._prepare_schema",
                side_effect=RuntimeError("first-create probe"),
            ), self.assertRaisesRegex(RuntimeError, "first-create probe"):
                store_candidate(built, test_definition, db_path=path, now=150)
            key_path = path.with_name(f"{path.name}.activation.key")
            self.assertFalse(path.exists())
            self.assertFalse(key_path.exists())
            self.assertEqual(list(path.parent.glob(f".{path.name}.*.staging*")), [])

            self.assertTrue(
                store_candidate(built, test_definition, db_path=path, now=150)
            )
            self.assertEqual(
                load_baseline(built.baseline_id, db_path=path).candidate.baseline_id,
                built.baseline_id,
            )
            self.assertEqual(
                {item.name for item in path.parent.iterdir()},
                {path.name, key_path.name},
            )

    def test_rejected_initial_trigger_creates_no_database_or_key(self) -> None:
        test_definition, built = candidate(count=3, min_new_results=4)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            with self.assertRaisesRegex(ValueError, "insufficient_new_results"):
                store_candidate(built, test_definition, db_path=path, now=150)
            self.assertFalse(path.exists())
            self.assertFalse(
                path.with_name(f"{path.name}.activation.key").exists()
            )

    def test_integer_and_negative_zero_observations_roundtrip_canonically(self) -> None:
        test_definition = definition(min_samples=3)
        combination = canonicalize_factors({"image_name": "img"})
        source = SourceSnapshot(
            tuple(
                SourceResult(
                    index,
                    f"run-{index}",
                    index,
                    "sha256:" + f"{index:064x}",
                    "sha256:" + f"{index + 100:064x}",
                    validation_test_config_digest(test_definition),
                    combination.key,
                    1,
                    "sha256:" + f"{index + 200:064x}",
                )
                for index in range(1, 4)
            )
        )
        observations = tuple(
            MetricObservation(
                result_id=index,
                run_id=f"run-{index}",
                completed_timestamp=index,
                source="source-a",
                metric_name="expanded-metric",
                sample_key="sample",
                value=value,
            )
            for index, value in enumerate((100, -0.0, 50), start=1)
        )
        built = _build_declarative_candidate(
            test_definition,
            combination,
            metric_specs_from_definition(test_definition),
            observations,
            source,
            created_at=100,
        )
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path, now=150)
            loaded = load_baseline(built.baseline_id, db_path=path)

        self.assertEqual(loaded.candidate.observations, built.observations)
        self.assertTrue(all(type(item.value) is float for item in built.observations))
        self.assertEqual(str(built.observations[1].value), "0.0")

    def test_missing_read_is_side_effect_free(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "missing.db"
            self.assertIsNone(load_baseline("hb1:" + "0" * 64, db_path=path))
            self.assertEqual(list_baselines(db_path=path), [])
            self.assertFalse(path.exists())

    def test_store_and_activation_timestamps_require_nonnegative_integers(self) -> None:
        test_definition, built = candidate()
        for timestamp in (True, 1.5, -1):
            with self.subTest(timestamp=timestamp), TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "health.db"
                with self.assertRaisesRegex(ValueError, "timestamp"):
                    store_candidate(
                        built,
                        test_definition,
                        db_path=path,
                        now=timestamp,
                    )
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path)
            with self.assertRaisesRegex(ValueError, "timestamp"):
                activate_candidate(
                    built.baseline_id,
                    test_definition,
                    db_path=path,
                    now=1.5,
                )

    def test_fractional_persisted_timestamp_is_rejected_without_coercion(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection:
                with self.assertRaises((sqlite3.IntegrityError, sqlite3.OperationalError)):
                    connection.execute("UPDATE health_baselines SET updated_at=1.5")
                mutate_behind_trigger(
                    connection,
                    "trg_health_baselines_legal_update",
                    "UPDATE health_baselines SET updated_at=1.5",
                )
                connection.commit()

            with self.assertRaisesRegex(ValueError, "non-negative integer"):
                load_baseline(built.baseline_id, db_path=path)

    def test_store_candidate_roundtrip_and_exact_retry(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"

            self.assertTrue(
                store_candidate(built, test_definition, db_path=path, now=200)
            )
            retry = replace(built, created_at=999)
            self.assertFalse(
                store_candidate(retry, test_definition, db_path=path, now=1000)
            )
            loaded = load_baseline(built.baseline_id, db_path=path)
            state = load_build_state(
                "smoke",
                built.combination.key,
                db_path=path,
            )

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.lifecycle, BaselineLifecycle.CANDIDATE)
        self.assertTrue(loaded.quality.activation_ready)
        self.assertEqual(loaded.candidate.baseline_id, built.baseline_id)
        self.assertEqual(loaded.candidate.health_policy_version, "smoke.health.v1")
        self.assertEqual(loaded.candidate.adapter_schema_version, 1)
        self.assertEqual(loaded.candidate.observations, built.observations)
        self.assertEqual(
            loaded.candidate.source_coverage[0].expected_sample_keys,
            ("sample",),
        )
        self.assertEqual(state.last_candidate_id, built.baseline_id)
        self.assertEqual(state.candidate_source_result_ids, (1, 2, 3))
        self.assertEqual(state.new_result_count, 3)

    def test_candidate_store_rejects_path_replacement_inside_transaction(self) -> None:
        test_definition, first = candidate(value=100.0, count=3)
        _definition, second = candidate(value=101.0, count=5, created_at=200)
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "health.db"
            displaced = root / "displaced.db"
            replacement = root / "replacement.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            shutil.copy2(path, replacement)
            replacement_bytes = replacement.read_bytes()

            def replace_path(connection: sqlite3.Connection) -> None:
                self.assertTrue(connection.in_transaction)
                path.rename(displaced)
                shutil.copy2(replacement, path)

            with self.assertRaisesRegex(RuntimeError, "path/device/inode changed"):
                store_candidate(
                    second,
                    test_definition,
                    db_path=path,
                    now=250,
                    pre_commit=replace_path,
                )

            self.assertEqual(path.read_bytes(), replacement_bytes)
            for db_path in (path, displaced):
                with closing(sqlite3.connect(db_path)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT baseline_id FROM health_baselines"
                        ).fetchall(),
                        [(first.baseline_id,)],
                    )

    def test_activation_rejects_path_replacement_inside_transaction(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "health.db"
            displaced = root / "displaced.db"
            replacement = root / "replacement.db"
            store_candidate(built, test_definition, db_path=path, now=150)
            shutil.copy2(path, replacement)
            replacement_bytes = replacement.read_bytes()

            def replace_path(connection: sqlite3.Connection) -> None:
                self.assertTrue(connection.in_transaction)
                path.rename(displaced)
                shutil.copy2(replacement, path)

            with self.assertRaisesRegex(RuntimeError, "path/device/inode changed"):
                activate_candidate(
                    built.baseline_id,
                    test_definition,
                    db_path=path,
                    now=200,
                    pre_commit=replace_path,
                )

            self.assertEqual(path.read_bytes(), replacement_bytes)
            for db_path in (path, displaced):
                with closing(sqlite3.connect(db_path)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT lifecycle_state FROM health_baselines"
                        ).fetchall(),
                        [("candidate",)],
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM health_activation_evidence"
                        ).fetchone(),
                        (0,),
                    )

    def test_candidate_rejects_activation_key_replacement_on_write_and_retry(self) -> None:
        test_definition, first = candidate(value=100.0, count=3)
        _definition, second = candidate(value=101.0, count=5, created_at=200)
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "health.db"
            key_path = path.with_name(f"{path.name}.activation.key")
            displaced_key = root / "displaced.key"
            store_candidate(first, test_definition, db_path=path, now=150)

            def replace_key(connection: sqlite3.Connection) -> None:
                self.assertTrue(connection.in_transaction)
                key_bytes = key_path.read_bytes()
                key_path.rename(displaced_key)
                key_path.write_bytes(key_bytes)
                key_path.chmod(0o600)

            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                store_candidate(
                    second,
                    test_definition,
                    db_path=path,
                    now=250,
                    transaction_open=replace_key,
                )
            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT baseline_id FROM health_baselines ORDER BY baseline_id"
                    ).fetchall(),
                    [(first.baseline_id,)],
                )
            key_path.unlink()
            displaced_key.rename(key_path)
            self.assertEqual(
                load_baseline(first.baseline_id, db_path=path).candidate.baseline_id,
                first.baseline_id,
            )

            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                store_candidate(
                    replace(first, created_at=999),
                    test_definition,
                    db_path=path,
                    now=1000,
                    transaction_open=replace_key,
                )
            key_path.unlink()
            displaced_key.rename(key_path)
            self.assertEqual(len(list_baselines(db_path=path)), 1)

    def test_activation_rejects_activation_key_replacement_on_write_and_retry(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "health.db"
            key_path = path.with_name(f"{path.name}.activation.key")
            displaced_key = root / "displaced.key"
            store_candidate(built, test_definition, db_path=path, now=150)

            def replace_key(connection: sqlite3.Connection) -> None:
                self.assertTrue(connection.in_transaction)
                key_bytes = key_path.read_bytes()
                key_path.rename(displaced_key)
                key_path.write_bytes(key_bytes)
                key_path.chmod(0o600)

            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                activate_candidate(
                    built.baseline_id,
                    test_definition,
                    db_path=path,
                    now=200,
                    transaction_open=replace_key,
                )
            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT lifecycle_state FROM health_baselines"
                    ).fetchone(),
                    ("candidate",),
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM health_activation_evidence"
                    ).fetchone(),
                    (0,),
                )
            key_path.unlink()
            displaced_key.rename(key_path)
            self.assertEqual(
                load_baseline(built.baseline_id, db_path=path).lifecycle,
                BaselineLifecycle.CANDIDATE,
            )

            self.assertTrue(
                activate_candidate(
                    built.baseline_id,
                    test_definition,
                    db_path=path,
                    now=200,
                )
            )
            with self.assertRaisesRegex(RuntimeError, "activation key.*changed"):
                activate_candidate(
                    built.baseline_id,
                    test_definition,
                    db_path=path,
                    now=250,
                    transaction_open=replace_key,
                )
            key_path.unlink()
            displaced_key.rename(key_path)
            loaded = load_baseline(built.baseline_id, db_path=path)
            self.assertEqual(loaded.lifecycle, BaselineLifecycle.ACTIVE)
            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM health_activation_evidence"
                    ).fetchone(),
                    (1,),
                )

    def test_health_write_lock_guard_blocks_commits_and_exact_retries(self) -> None:
        test_definition, first = candidate(value=100.0, count=3)
        _definition, second = candidate(value=101.0, count=5, created_at=200)

        def reject_lock() -> None:
            raise RuntimeError("lock guard probe")

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            with self.assertRaisesRegex(RuntimeError, "lock guard probe"):
                store_candidate(
                    second,
                    test_definition,
                    db_path=path,
                    now=250,
                    lock_guard=reject_lock,
                )
            with self.assertRaisesRegex(RuntimeError, "lock guard probe"):
                store_candidate(
                    replace(first, created_at=999),
                    test_definition,
                    db_path=path,
                    now=1000,
                    lock_guard=reject_lock,
                )
            self.assertEqual(len(list_baselines(db_path=path)), 1)

            with self.assertRaisesRegex(RuntimeError, "lock guard probe"):
                activate_candidate(
                    first.baseline_id,
                    test_definition,
                    db_path=path,
                    now=200,
                    lock_guard=reject_lock,
                )
            self.assertEqual(
                load_baseline(first.baseline_id, db_path=path).lifecycle,
                BaselineLifecycle.CANDIDATE,
            )
            self.assertTrue(
                activate_candidate(
                    first.baseline_id,
                    test_definition,
                    db_path=path,
                    now=200,
                )
            )
            with self.assertRaisesRegex(RuntimeError, "lock guard probe"):
                activate_candidate(
                    first.baseline_id,
                    test_definition,
                    db_path=path,
                    now=250,
                    lock_guard=reject_lock,
                )
            self.assertEqual(
                load_baseline(first.baseline_id, db_path=path).lifecycle,
                BaselineLifecycle.ACTIVE,
            )

    def test_split_lock_before_first_publication_leaves_no_canonical_pair(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            result_path = root / "result.db"
            result_path.touch()
            health_path = root / "health.db"
            key_path = health_path.with_name(f"{health_path.name}.activation.key")
            first_lock = evaluator_test_lock(result_path, timeout_seconds=1)
            first_guard = first_lock.__enter__()
            competitor_lock = None
            competitor_guard = None

            def split_after_staged_commit() -> None:
                nonlocal competitor_lock, competitor_guard
                first_guard.path.unlink()
                replacement = os.open(
                    first_guard.path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                os.close(replacement)
                competitor_lock = evaluator_test_lock(result_path, timeout_seconds=1)
                competitor_guard = competitor_lock.__enter__()
                competitor_guard()

            try:
                with self.assertRaisesRegex(
                    HealthEvaluatorLockError,
                    "lock.*changed",
                ):
                    store_candidate(
                        built,
                        test_definition,
                        db_path=health_path,
                        now=150,
                        lock_guard=first_guard,
                        pre_publish=split_after_staged_commit,
                    )
                self.assertIsNotNone(competitor_guard)
                self.assertFalse(health_path.exists())
                self.assertFalse(key_path.exists())
                self.assertEqual(
                    list(root.glob(f".{health_path.name}.*.staging*")),
                    [],
                )
            finally:
                if competitor_lock is not None:
                    competitor_lock.__exit__(None, None, None)
                with self.assertRaises(HealthEvaluatorLockError):
                    first_lock.__exit__(None, None, None)

    def test_candidate_storage_never_silently_activates(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path)

            self.assertIsNone(
                get_active_baseline("smoke", built.combination.key, db_path=path)
            )
            self.assertEqual(
                list_baselines(db_path=path)[0].lifecycle,
                BaselineLifecycle.CANDIDATE,
            )

    def test_trigger_evidence_cannot_be_deleted_and_is_required_on_read(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "DELETE FROM health_candidate_triggers WHERE baseline_id=?",
                        (built.baseline_id,),
                    )
                mutate_behind_trigger(
                    connection,
                    "trg_health_candidate_triggers_no_delete",
                    "DELETE FROM health_candidate_triggers WHERE baseline_id=?",
                    (built.baseline_id,),
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "trigger evidence"):
                load_baseline(built.baseline_id, db_path=path)

    def test_direct_sql_cannot_activate_without_framework_evidence(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path, now=150)
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                (sqlite3.IntegrityError, sqlite3.OperationalError)
            ):
                connection.execute(
                    "UPDATE health_baselines "
                    "SET lifecycle_state='active', updated_at=200, activated_at=200 "
                    "WHERE baseline_id=?",
                    (built.baseline_id,),
                )
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                sqlite3.OperationalError
            ):
                connection.execute(
                    """
                    INSERT INTO health_activation_evidence (
                        baseline_id, test_id, combination_key,
                        test_config_digest, health_policy_version,
                        adapter_schema_version, evaluator_version,
                        activated_at, quality_json
                    )
                    SELECT baseline_id, test_id, combination_key,
                           test_config_digest, health_policy_version,
                           adapter_schema_version, evaluator_version,
                           200, quality_json
                    FROM health_baselines WHERE baseline_id=?
                    """,
                    (built.baseline_id,),
                )

            stored = load_baseline(built.baseline_id, db_path=path)
        self.assertEqual(stored.lifecycle, BaselineLifecycle.CANDIDATE)

    def test_observations_and_activation_evidence_are_immutable(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path, now=150)
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute(
                    "UPDATE health_observations SET value=0 "
                    "WHERE baseline_id=?",
                    (built.baseline_id,),
                )
            activate_candidate(
                built.baseline_id,
                test_definition,
                db_path=path,
                now=200,
            )
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute(
                    "DELETE FROM health_activation_evidence WHERE baseline_id=?",
                    (built.baseline_id,),
                )

    def test_insert_or_replace_cannot_rebind_health_evidence_or_lifecycle(self) -> None:
        test_definition, first = candidate(value=100.0, created_at=100)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, child = candidate(
                value=95.0,
                count=5,
                parent=first.baseline_id,
                created_at=300,
            )
            store_candidate(child, test_definition, db_path=path, now=350)
            activate_candidate(child.baseline_id, test_definition, db_path=path, now=400)
            with closing(sqlite3.connect(path)) as connection:
                statements = (
                    (
                        "INSERT OR REPLACE INTO health_baselines "
                        "SELECT baseline_id, payload_digest, test_id, combination_key, "
                        "combination_factors_json, 'active', method, robust_z_threshold, "
                        "observation_digest, source_result_count, excluded_result_count, "
                        "source_first_timestamp, source_last_timestamp, source_max_result_id, "
                        "test_config_digest, health_policy_version, adapter_schema_version, "
                        "evaluator_version, parent_baseline_id, created_at, updated_at, "
                        "activated_at, NULL, quality_json FROM health_baselines "
                        "WHERE baseline_id=?",
                        (first.baseline_id,),
                    ),
                    (
                        "INSERT OR REPLACE INTO health_candidate_triggers "
                        "SELECT baseline_id, previous_candidate_id, 1, 1, "
                        "qualifying_result_count, new_result_count "
                        "FROM health_candidate_triggers WHERE baseline_id=?",
                        (child.baseline_id,),
                    ),
                    (
                        "INSERT OR REPLACE INTO health_candidate_triggers "
                        "(rowid, baseline_id, previous_candidate_id, min_samples, "
                        "min_new_results, qualifying_result_count, new_result_count) "
                        "SELECT rowid, ?, previous_candidate_id, min_samples, "
                        "min_new_results, qualifying_result_count, new_result_count "
                        "FROM health_candidate_triggers WHERE baseline_id=?",
                        ("hb1:" + "f" * 64, child.baseline_id),
                    ),
                )
                for statement, parameters in statements:
                    with self.subTest(statement=statement), self.assertRaises(
                        sqlite3.IntegrityError
                    ):
                        connection.execute(statement, parameters)

    def test_spoofed_sql_authorization_cannot_create_accepted_activation(self) -> None:
        test_definition, first = candidate(value=100.0, created_at=100)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, child = candidate(
                value=95.0,
                count=5,
                parent=first.baseline_id,
                created_at=300,
            )
            store_candidate(child, test_definition, db_path=path, now=350)
            with closing(sqlite3.connect(path)) as connection:
                connection.create_function("cval_activation_authorized", 0, lambda: 1)
                connection.execute(
                    """
                    INSERT INTO health_activation_evidence (
                        baseline_id, test_id, combination_key,
                        test_config_digest, health_policy_version,
                        adapter_schema_version, evaluator_version,
                        activated_at, quality_json, signature
                    )
                    SELECT baseline_id, test_id, combination_key,
                           test_config_digest, health_policy_version,
                           adapter_schema_version, evaluator_version,
                           400, quality_json, ?
                    FROM health_baselines WHERE baseline_id=?
                    """,
                    ("hmac-sha256:" + "0" * 64, child.baseline_id),
                )
                connection.execute(
                    "UPDATE health_baselines SET lifecycle_state='superseded', "
                    "updated_at=400, superseded_at=400 WHERE baseline_id=?",
                    (first.baseline_id,),
                )
                connection.execute(
                    "UPDATE health_baselines SET lifecycle_state='active', "
                    "updated_at=400, activated_at=400 WHERE baseline_id=?",
                    (child.baseline_id,),
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "activation evidence"):
                get_active_baseline(
                    "smoke",
                    first.combination.key,
                    db_path=path,
                )

    def test_active_read_rejects_corrupt_lifecycle_ancestor(self) -> None:
        test_definition, first = candidate(value=100.0, created_at=100)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, child = candidate(
                value=95.0,
                count=5,
                parent=first.baseline_id,
                created_at=300,
            )
            store_candidate(child, test_definition, db_path=path, now=350)
            activate_candidate(child.baseline_id, test_definition, db_path=path, now=400)
            with closing(sqlite3.connect(path)) as connection:
                mutate_behind_trigger(
                    connection,
                    "trg_health_baselines_legal_update",
                    "UPDATE health_baselines SET lifecycle_state='candidate', "
                    "updated_at=created_at, activated_at=NULL, superseded_at=NULL "
                    "WHERE baseline_id=?",
                    (first.baseline_id,),
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "activation evidence|lifecycle"):
                get_active_baseline(
                    "smoke",
                    child.combination.key,
                    db_path=path,
                )

    def test_activation_supersedes_only_same_combination_parent(self) -> None:
        test_definition, first = candidate(value=100.0, created_at=100)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            self.assertTrue(
                activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            )
            _definition, child = candidate(
                value=95.0,
                count=5,
                parent=first.baseline_id,
                created_at=300,
            )
            store_candidate(child, test_definition, db_path=path, now=350)
            self.assertTrue(
                activate_candidate(child.baseline_id, test_definition, db_path=path, now=400)
            )
            _other_definition, other = candidate(image="other", value=90.0)
            store_candidate(other, test_definition, db_path=path, now=450)
            activate_candidate(other.baseline_id, test_definition, db_path=path, now=500)

            active = get_active_baseline(
                "smoke",
                first.combination.key,
                db_path=path,
            )
            old = load_baseline(first.baseline_id, db_path=path)
            other_active = get_active_baseline(
                "smoke",
                other.combination.key,
                db_path=path,
            )

        self.assertEqual(active.candidate.baseline_id, child.baseline_id)
        self.assertEqual(old.lifecycle, BaselineLifecycle.SUPERSEDED)
        self.assertEqual(other_active.candidate.baseline_id, other.baseline_id)

    def test_active_reader_keeps_one_snapshot_during_concurrent_activation(self) -> None:
        test_definition, first = candidate(value=100.0, created_at=100)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, child = candidate(
                value=95.0,
                count=5,
                parent=first.baseline_id,
                created_at=300,
            )
            store_candidate(child, test_definition, db_path=path, now=350)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")

            original_loader = health_storage._load_stored_from_connection
            activation_started = False

            def load_after_activation(connection, baseline_id):
                nonlocal activation_started
                if not activation_started:
                    activation_started = True
                    activate_candidate(
                        child.baseline_id,
                        test_definition,
                        db_path=path,
                        now=400,
                    )
                return original_loader(connection, baseline_id)

            with patch.object(
                health_storage,
                "_load_stored_from_connection",
                side_effect=load_after_activation,
            ):
                observed = get_active_baseline(
                    "smoke",
                    first.combination.key,
                    db_path=path,
                )

            latest = get_active_baseline(
                "smoke",
                first.combination.key,
                db_path=path,
            )

        self.assertEqual(observed.candidate.baseline_id, first.baseline_id)
        self.assertEqual(observed.lifecycle, BaselineLifecycle.ACTIVE)
        self.assertEqual(latest.candidate.baseline_id, child.baseline_id)

    def test_direct_sql_cannot_supersede_active_parent_with_only_candidate_child(self) -> None:
        test_definition, first = candidate(value=100.0, created_at=100)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, child = candidate(
                value=95.0,
                count=5,
                parent=first.baseline_id,
                created_at=300,
            )
            store_candidate(child, test_definition, db_path=path, now=350)
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                sqlite3.OperationalError
            ):
                connection.execute(
                    "UPDATE health_baselines SET lifecycle_state='superseded', "
                    "updated_at=400, superseded_at=400 WHERE baseline_id=?",
                    (first.baseline_id,),
                )
            active = get_active_baseline(
                "smoke",
                first.combination.key,
                db_path=path,
            )

        self.assertEqual(active.candidate.baseline_id, first.baseline_id)

    def test_stale_parent_and_superseded_reactivation_are_rejected(self) -> None:
        test_definition, first = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, stale = candidate(
                value=90.0,
                count=5,
                parent=None,
                created_at=200,
            )
            with self.assertRaisesRegex(ValueError, "lifecycle parent"):
                store_candidate(stale, test_definition, db_path=path, now=250)

            _definition, child = candidate(
                value=95.0,
                count=7,
                parent=first.baseline_id,
                created_at=300,
            )
            store_candidate(child, test_definition, db_path=path, now=350)
            activate_candidate(child.baseline_id, test_definition, db_path=path, now=400)
            with self.assertRaisesRegex(ValueError, "Only candidate"):
                activate_candidate(first.baseline_id, test_definition, db_path=path)

    def test_under_sampled_candidate_is_not_stored(self) -> None:
        test_definition, built = candidate(count=2)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            with self.assertRaisesRegex(ValueError, "build trigger"):
                store_candidate(built, test_definition, db_path=path)

            self.assertIsNone(
                get_active_baseline("smoke", built.combination.key, db_path=path)
            )

    def test_config_drift_blocks_activation(self) -> None:
        test_definition, built = candidate()
        changed = replace(
            test_definition,
            health=replace(test_definition.health, min_samples=4),
        )
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path)

            with self.assertRaisesRegex(ValueError, "stale"):
                activate_candidate(built.baseline_id, changed, db_path=path)

    def test_extra_or_future_schema_fails_closed_and_seed_updates_are_blocked(self) -> None:
        test_definition, built = candidate()
        mutations = (
            lambda connection: connection.execute("CREATE TABLE unexpected(value TEXT)"),
            lambda connection: connection.execute(
                "INSERT INTO schema_migrations VALUES (99, 'future', 1)"
            ),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "health.db"
                store_candidate(built, test_definition, db_path=path)
                with closing(sqlite3.connect(path)) as connection:
                    mutation(connection)
                    connection.commit()

                with self.assertRaises(RuntimeError):
                    load_baseline(built.baseline_id, db_path=path)
                with self.assertRaises(RuntimeError):
                    store_candidate(built, test_definition, db_path=path)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute(
                    "UPDATE health_class_definitions "
                    "SET class_name='Okay' WHERE class_code=1"
                )

    def test_source_metadata_corruption_is_detected(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(built, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection:
                with self.assertRaises((sqlite3.IntegrityError, sqlite3.OperationalError)):
                    connection.execute(
                        "UPDATE health_baselines SET source_result_count=999"
                    )
                mutate_behind_trigger(
                    connection,
                    "trg_health_baselines_legal_update",
                    "UPDATE health_baselines SET source_result_count=999"
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "source metadata"):
                load_baseline(built.baseline_id, db_path=path)

    def test_build_state_compares_most_recent_candidate_sources(self) -> None:
        test_definition, first = candidate(value=100.0, count=3)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path)
            _definition, second = candidate(value=101.0, count=5, created_at=200)
            store_candidate(second, test_definition, db_path=path)
            state = load_build_state(
                "smoke",
                first.combination.key,
                db_path=path,
            )

        self.assertEqual(state.last_candidate_id, second.baseline_id)
        self.assertEqual(state.candidate_source_result_ids, (1, 2, 3, 4, 5))
        self.assertEqual(state.new_result_count, 2)

    def test_readers_reject_symlinked_health_database(self) -> None:
        test_definition, built = candidate()
        with TemporaryDirectory() as tmpdir:
            external = Path(tmpdir) / "external.db"
            store_candidate(built, test_definition, db_path=external)
            link = Path(tmpdir) / "health.db"
            link.symlink_to(external)

            with self.assertRaisesRegex(ValueError, "symlink"):
                load_baseline(built.baseline_id, db_path=link)

    def test_zero_new_result_candidate_is_rejected(self) -> None:
        test_definition, first = candidate(value=100.0, count=3)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path)
            _definition, no_new = candidate(value=101.0, count=3, created_at=200)

            with self.assertRaisesRegex(ValueError, "insufficient_new_results"):
                store_candidate(no_new, test_definition, db_path=path)

            state = load_build_state(
                "smoke",
                first.combination.key,
                db_path=path,
            )
        self.assertEqual(state.last_candidate_id, first.baseline_id)

    def test_fractional_previous_source_id_is_not_truncated(self) -> None:
        test_definition, first = candidate(value=100.0, count=3)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE health_baseline_sources SET result_id=3.5 "
                        "WHERE baseline_id=? AND result_id=3",
                        (first.baseline_id,),
                    )
                mutate_behind_trigger(
                    connection,
                    "trg_health_baseline_sources_no_update",
                    "UPDATE health_baseline_sources SET result_id=3.5 "
                    "WHERE baseline_id=? AND result_id=3",
                    (first.baseline_id,),
                )
                connection.commit()
            _definition, second = candidate(value=101.0, count=5, created_at=200)

            with self.assertRaisesRegex((ValueError, RuntimeError), "positive|foreign-key"):
                store_candidate(second, test_definition, db_path=path)

    def test_stale_parent_candidate_does_not_advance_build_state(self) -> None:
        test_definition, first = candidate(count=3)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, stale = candidate(count=5, parent=None, created_at=250)

            with self.assertRaisesRegex(ValueError, "lifecycle parent"):
                store_candidate(stale, test_definition, db_path=path, now=300)

            state = load_build_state(
                "smoke",
                first.combination.key,
                db_path=path,
            )
        self.assertEqual(state.last_candidate_id, first.baseline_id)

    def test_health_database_rejects_cross_test_owner(self) -> None:
        test_definition, first = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path)
            other_definition = replace(
                test_definition,
                metadata=replace(test_definition.metadata, id="other"),
            )
            other = replace(
                first,
                baseline_id="",
                payload_digest="",
                test_id="other",
                test_config_digest=validation_test_config_digest(other_definition),
                source_snapshot=SourceSnapshot(
                    tuple(
                        replace(
                            result,
                            test_config_digest=validation_test_config_digest(
                                other_definition
                            ),
                        )
                        for result in first.source_snapshot.results
                    )
                ),
            )
            digest, baseline_id = _candidate_identity(other)
            other = replace(other, payload_digest=digest, baseline_id=baseline_id)

            with self.assertRaisesRegex(ValueError, "different validation test"):
                store_candidate(other, other_definition, db_path=path)

    def test_health_database_owner_cannot_be_deleted(self) -> None:
        test_definition, first = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute("DELETE FROM health_database_owner")

    def test_owner_delete_is_blocked_even_with_foreign_keys_disabled(self) -> None:
        test_definition, first = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection, self.assertRaises(
                sqlite3.IntegrityError
            ):
                connection.execute("PRAGMA foreign_keys=OFF")
                connection.execute("DELETE FROM health_database_owner")

    def test_build_state_blob_text_is_rejected_not_stringified(self) -> None:
        test_definition, first = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("UPDATE health_build_state SET last_error=x'6572726f72'")
                connection.commit()

            with self.assertRaisesRegex(ValueError, "text"):
                load_build_state("smoke", first.combination.key, db_path=path)

    def test_fractional_advisory_state_does_not_control_activation(self) -> None:
        test_definition, first = candidate()
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE health_build_state SET last_seen_result_id=3.5, "
                    "last_checked_at=1.5, last_built_at=1.5"
                )
                connection.commit()

            self.assertTrue(
                activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            )
            with self.assertRaisesRegex(ValueError, "integer"):
                load_build_state("smoke", first.combination.key, db_path=path)

    def test_idempotent_activation_still_rejects_current_config_drift(self) -> None:
        test_definition, first = candidate()
        changed = replace(
            test_definition,
            health=replace(test_definition.health, min_samples=4),
        )
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)

            with self.assertRaisesRegex(ValueError, "stale"):
                activate_candidate(first.baseline_id, changed, db_path=path, now=250)

    def test_activation_rejects_corrupt_active_parent_without_superseding(self) -> None:
        test_definition, first = candidate(count=3)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=150)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=200)
            _definition, child = candidate(
                value=95.0,
                count=5,
                parent=first.baseline_id,
                created_at=250,
            )
            store_candidate(child, test_definition, db_path=path, now=300)
            with closing(sqlite3.connect(path)) as connection:
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(
                        "UPDATE health_metric_statistics SET center=999 "
                        "WHERE baseline_id=?",
                        (first.baseline_id,),
                    )
                mutate_behind_trigger(
                    connection,
                    "trg_health_metric_statistics_no_update",
                    "UPDATE health_metric_statistics SET center=999 "
                    "WHERE baseline_id=?",
                    (first.baseline_id,),
                )
                connection.commit()

            with self.assertRaises(ValueError):
                activate_candidate(child.baseline_id, test_definition, db_path=path, now=350)
            with closing(sqlite3.connect(path)) as connection:
                state = connection.execute(
                    "SELECT lifecycle_state FROM health_baselines WHERE baseline_id=?",
                    (first.baseline_id,),
                ).fetchone()[0]
        self.assertEqual(state, "active")

    def test_child_activation_cannot_precede_active_parent_timestamp(self) -> None:
        test_definition, first = candidate(count=3, created_at=100)
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "health.db"
            store_candidate(first, test_definition, db_path=path, now=900)
            activate_candidate(first.baseline_id, test_definition, db_path=path, now=1000)
            _definition, child = candidate(
                count=5,
                value=95.0,
                parent=first.baseline_id,
                created_at=500,
            )
            with self.assertRaisesRegex(ValueError, "active parent"):
                store_candidate(child, test_definition, db_path=path, now=600)

if __name__ == "__main__":
    unittest.main()
