from __future__ import annotations

import re
import unittest
from contextlib import contextmanager
from pathlib import Path

from cval.nccl_eval.config import NcclEvaluationConfig
from cval.nccl_eval.integration import (
    CLEANUP_CONFIRMATION,
    clean_nccl_schemas,
    require_disposable_test_url,
)
from cval.nccl_eval.schema import (
    _attest_runtime_role_reuse,
    apply_migrations,
    migration_plan,
    migrations,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class NcclEvalConfigTests(unittest.TestCase):
    def test_defaults_are_strict_safe_and_database_optional(self) -> None:
        config = NcclEvaluationConfig.from_env({})
        self.assertIsNone(config.database_url)
        self.assertEqual(config.baseline_minimum_results, 40)
        self.assertEqual(config.baseline_update_increment, 10)
        self.assertFalse(config.public_dict()["database_configured"])
        self.assertNotIn("database_url", repr(config))
        with self.assertRaisesRegex(ValueError, "DATABASE_URL"):
            NcclEvaluationConfig.from_env({}, require_database=True)

    def test_database_url_is_never_in_public_config_or_repr(self) -> None:
        secret = "postgresql://secret-user:secret-password@db/cval"
        config = NcclEvaluationConfig.from_env({"DATABASE_URL": secret})
        self.assertNotIn(secret, repr(config))
        self.assertNotIn(secret, str(config.public_dict()))
        self.assertEqual(config.require_database_url(), secret)

    def test_bounds_and_fixed_baseline_cadence_fail_closed(self) -> None:
        invalid = (
            {"EVALUATOR_BATCH_SIZE": "0"},
            {"EVALUATOR_MAX_ATTEMPTS": "101"},
            {"BASELINE_MINIMUM_RESULTS": "39"},
            {"BASELINE_MINIMUM_RESULTS": "41"},
            {"BASELINE_UPDATE_INCREMENT": "11"},
            {"DATABASE_POOL_MIN_SIZE": "5", "DATABASE_POOL_MAX_SIZE": "4"},
            {"EVALUATOR_RETRY_BASE_SECONDS": "10", "EVALUATOR_RETRY_MAX_SECONDS": "2"},
            {"DATABASE_URL": "sqlite:///not-postgresql"},
            {"DATABASE_URL": "postgresql://example/not-cval"},
        )
        for environ in invalid:
            with self.subTest(environ=environ), self.assertRaises(ValueError):
                NcclEvaluationConfig.from_env(environ)

    def test_disposable_database_guard_is_anchored_and_cleanup_needs_exact_token(self) -> None:
        self.assertEqual(
            require_disposable_test_url("postgresql://db/cval_test_nccl_01"),
            "cval_test_nccl_01",
        )
        for database in ("financial", "contest", "tmp_prod", "cval", "cval_test_"):
            with self.subTest(database=database), self.assertRaises(ValueError):
                require_disposable_test_url(f"postgresql://db/{database}")

        class NeverPool:
            def connection(self):
                raise AssertionError("wrong confirmation must fail before connection")

        with self.assertRaisesRegex(ValueError, CLEANUP_CONFIRMATION):
            clean_nccl_schemas(NeverPool(), confirm="wrong")


class NcclEvalMigrationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        loaded = migrations()
        if len(loaded) != 1:
            raise AssertionError(f"expected one initial migration, found {len(loaded)}")
        cls.sql = loaded[0].sql

    def test_plan_has_stable_identity_without_database_connection(self) -> None:
        plan = migration_plan()
        self.assertEqual(plan["database_expected"], "cval")
        self.assertEqual(plan["schemas"], ["nccl_raw", "nccl_baseline", "nccl_validation"])
        self.assertRegex(plan["migrations"][0]["sha256"], r"^[0-9a-f]{64}$")

    def test_apply_takes_advisory_lock_before_ledger_inspection(self) -> None:
        class Result:
            def __init__(self, one=None):
                self.one = one

            def fetchone(self):
                return self.one

        class Connection:
            def __init__(self):
                self.sql: list[str] = []

            @contextmanager
            def transaction(self):
                yield

            def execute(self, sql, params=None):
                normalized = " ".join(sql.split())
                self.sql.append(normalized)
                if normalized == "SELECT current_database()":
                    return Result(("cval_test_unit",))
                if "to_regclass" in normalized:
                    return Result((None,))
                return Result()

        class Pool:
            def __init__(self):
                self.connection_value = Connection()

            @contextmanager
            def connection(self):
                yield self.connection_value

        pool = Pool()
        receipt = apply_migrations(
            pool, allow_disposable_test_database=True
        )
        self.assertEqual(receipt["applied"], ["001_initial.sql"])
        lock_index = next(
            index for index, sql in enumerate(pool.connection_value.sql)
            if "pg_advisory_xact_lock" in sql
        )
        ledger_index = next(
            index for index, sql in enumerate(pool.connection_value.sql)
            if "to_regclass" in sql
        )
        self.assertLess(lock_index, ledger_index)

    def test_all_required_schemas_tables_foreign_keys_and_uniques_exist(self) -> None:
        for schema in ("nccl_raw", "nccl_baseline", "nccl_validation"):
            self.assertIn(f"CREATE SCHEMA IF NOT EXISTS {schema}", self.sql)
        required_tables = (
            "nccl_raw.test_run",
            "nccl_raw.outbox_receipt",
            "nccl_raw.outbox_scan_cursor",
            "nccl_raw.node_result",
            "nccl_raw.nic_result",
            "nccl_baseline.baseline_profile",
            "nccl_baseline.calibration_decision",
            "nccl_baseline.baseline_version",
            "nccl_baseline.baseline_version_sample",
            "nccl_baseline.metric_threshold",
            "nccl_validation.health_class",
            "nccl_validation.evaluation_job",
            "nccl_validation.evaluation",
        )
        for table in required_tables:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)
        for unique in (
            "UNIQUE (run_id, node_name)",
            "UNIQUE (profile_id, version_number)",
            "UNIQUE (result_id, baseline_version_id)",
        ):
            self.assertIn(unique, self.sql)
        self.assertIn("baseline_profile_active_version_fk", self.sql)
        self.assertIn("REFERENCES nccl_raw.test_run(run_id)", self.sql)
        self.assertIn("REFERENCES nccl_raw.node_result(result_id)", self.sql)
        self.assertIn("REFERENCES nccl_baseline.baseline_version", self.sql)

    def test_fixed_classes_required_indexes_and_read_views_exist(self) -> None:
        for class_id, code in enumerate(
            ("EXCEEDING", "WITHIN", "UNDERPERFORMING", "DEGRADED", "CRITICAL"),
            start=1,
        ):
            self.assertRegex(self.sql, rf"\({class_id}, '{code}'")
        for index in (
            "node_result_node_time_idx",
            "node_result_created_at_idx",
            "baseline_profile_lookup_idx",
            "evaluation_job_pending_idx",
            "evaluation_job_waiting_idx",
            "evaluation_result_idx",
            "evaluation_health_time_idx",
        ):
            self.assertIn(f"INDEX IF NOT EXISTS {index}", self.sql)
        self.assertIn("VIEW nccl_validation.latest_result_view", self.sql)
        self.assertIn("VIEW nccl_validation.raw_result_status_view", self.sql)
        self.assertIn("(job.status = 'COMPLETED') AS classified", self.sql)

    def test_threshold_coverage_lineage_and_immutability_are_database_guarded(self) -> None:
        self.assertIn("metric_threshold_complete", self.sql)
        self.assertIn("DEFERRABLE INITIALLY DEFERRED", self.sql)
        self.assertRegex(
            self.sql,
            r"CREATE CONSTRAINT TRIGGER metric_threshold_complete\s+"
            r"AFTER INSERT OR UPDATE OR DELETE[\s\S]*?DEFERRABLE INITIALLY DEFERRED",
        )
        self.assertIn("five contiguous semantic ranges covering [0,infinity)", self.sql)
        self.assertIn("baseline_version_sample", self.sql)
        self.assertIn("included BOOLEAN NOT NULL", self.sql)
        self.assertIn("exclusion_reason TEXT", self.sql)
        self.assertIn("baseline_version_immutable", self.sql)
        self.assertIn("baseline_sample_building_only", self.sql)
        self.assertIn("metric_threshold_building_only", self.sql)
        self.assertIn("NCCL raw rows are append-only", self.sql)
        self.assertIn("NCCL health classes are fixed", self.sql)
        self.assertIn("evaluation_immutable", self.sql)
        self.assertIn("NCCL historical evaluations are immutable", self.sql)
        self.assertIn("baseline_one_active_per_profile_idx", self.sql)
        self.assertIn("baseline versions must be inserted in BUILDING state", self.sql)
        self.assertIn("included lineage count must equal sample_count", self.sql)
        self.assertIn("complete semantic thresholds", self.sql)
        self.assertIn("same profile and be older", self.sql)
        self.assertIn("ACTIVE profile must point to its sole ACTIVE", self.sql)
        self.assertIn("failure_reason TEXT CHECK", self.sql)
        self.assertIn("ACTIVE baseline failure requires controlled calibration revocation", self.sql)
        self.assertIn("replacement baseline must supersede the latest failed version", self.sql)

    def test_calibration_state_machine_and_security_definer_boundary_are_present(self) -> None:
        self.assertIn("calibration_decision_sequence_guard", self.sql)
        self.assertIn("BEFORE INSERT ON nccl_baseline.calibration_decision", self.sql)
        self.assertIn("first calibration decision must be version 1 APPROVE", self.sql)
        self.assertIn("calibration decision version must be latest plus one", self.sql)
        self.assertIn("calibration decisions must alternate effective action", self.sql)
        self.assertIn("FUNCTION nccl_baseline.apply_calibration_decision", self.sql)
        self.assertIn("SECURITY DEFINER", self.sql)
        self.assertIn("SET search_path = pg_catalog", self.sql)
        self.assertIn("SET status = 'FAILED'", self.sql)
        self.assertIn("active_baseline_version_id = NULL", self.sql)
        self.assertIn("status IN ('PENDING', 'RETRY')", self.sql)
        self.assertIn("REVOKE ALL ON FUNCTION", self.sql)

    def test_baseline_child_update_requires_both_building_parents_and_preserves_keys(self) -> None:
        function = re.search(
            r"CREATE OR REPLACE FUNCTION nccl_baseline\.guard_building_child_mutation\(\)"
            r"(?P<body>.*?)\n\$\$;",
            self.sql,
            re.DOTALL,
        )
        self.assertIsNotNone(function)
        body = function.group("body")
        self.assertIn("OLD.baseline_version_id", body)
        self.assertIn("NEW.baseline_version_id", body)
        self.assertIn("old_parent_status IS DISTINCT FROM 'BUILDING'", body)
        self.assertIn("new_parent_status IS DISTINCT FROM 'BUILDING'", body)
        self.assertIn(
            "ROW(NEW.baseline_version_id, NEW.result_id) IS DISTINCT FROM",
            body,
        )
        self.assertIn(
            "ROW(NEW.baseline_version_id, NEW.metric_name, NEW.class_id) IS DISTINCT FROM",
            body,
        )
        self.assertIn("baseline sample key identity is immutable", body)
        self.assertIn("metric threshold key identity is immutable", body)

    def test_ledger_claim_evaluation_and_truncate_guards_are_database_enforced(self) -> None:
        self.assertIn("test_config_fingerprint TEXT NOT NULL", self.sql)
        self.assertIn("claim_token UUID", self.sql)
        self.assertIn("claim_token IS NOT NULL", self.sql)
        self.assertRegex(self.sql, r"bus_bw_class SMALLINT NOT NULL")
        self.assertRegex(self.sql, r"latency_class SMALLINT NOT NULL")
        self.assertRegex(self.sql, r"overall_health_class SMALLINT NOT NULL")
        self.assertIn("evaluation_overall_class_is_worst", self.sql)
        self.assertIn("evaluation_overall_severity_is_worst", self.sql)
        self.assertIn("schema_migration_immutable", self.sql)
        self.assertIn("outbox_receipt_immutable", self.sql)
        self.assertIn("content_sha256 TEXT CHECK", self.sql)
        self.assertIn("calibration_decision_immutable", self.sql)
        self.assertIn("outbox_scan_cursor_protected", self.sql)
        self.assertIn("outbox_scan_cursor_no_truncate", self.sql)
        self.assertIn("NCCL protected tables cannot be truncated", self.sql)
        self.assertEqual(self.sql.count("BEFORE TRUNCATE ON"), 14)
        self.assertIn("LEFT JOIN nccl_validation.health_class", self.sql)

    def test_queue_and_baseline_concurrency_contract_is_present(self) -> None:
        self.assertIn("WAITING_FOR_BASELINE", self.sql)
        self.assertIn("PROCESSING", self.sql)
        self.assertIn("attempt_count INTEGER NOT NULL DEFAULT 0", self.sql)
        self.assertIn("active_baseline_version_id UUID", self.sql)
        self.assertIn("sample_count INTEGER NOT NULL CHECK (sample_count >= 40)", self.sql)
        self.assertIn("overall_health_class = GREATEST", self.sql)
        self.assertIn("WHERE status = 'ACTIVE'", self.sql)
        self.assertNotRegex(self.sql, r"(?i)CREATE\s+DATABASE")

    def test_runtime_role_provisioning_is_identifier_safe_and_non_owner(self) -> None:
        source = (REPO_ROOT / "cval/nccl_eval/schema.py").read_text(encoding="utf-8")
        self.assertIn("sql.Identifier(username)", source)
        self.assertIn("sql.Literal(password)", source)
        self.assertNotIn("PASSWORD %s", source)
        self.assertIn("runtime role credential statement failed", source)
        self.assertIn("NOSUPERUSER NOCREATEDB", source)
        self.assertIn("NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS", source)
        self.assertIn("pg_auth_members", source)
        self.assertIn("member = %s OR roleid = %s", source)
        self.assertIn("pg_database WHERE datdba", source)
        self.assertIn("pg_namespace", source)
        self.assertIn("relation.relowner", source)
        self.assertIn("REVOKE INSERT ON nccl_baseline.calibration_decision", source)
        self.assertIn("GRANT EXECUTE ON FUNCTION nccl_baseline.apply_calibration_decision", source)
        self.assertIn("REVOKE ALL ON DATABASE", source)
        self.assertIn("REVOKE ALL ON SCHEMA public FROM PUBLIC", source)
        self.assertIn("GRANT SELECT ON nccl_validation.raw_result_status_view", source)
        self.assertLess(
            source.index("attestation = _attest_runtime_role_reuse"),
            source.index("connection.execute(credential_statement)"),
        )
        self.assertNotIn("GRANT ALL", source)
        self.assertNotIn("GRANT TRUNCATE", source)

    def test_runtime_role_reuse_attestation_rejects_each_unsafe_condition(self) -> None:
        class Result:
            def __init__(self, row):
                self.row = row

            def fetchone(self):
                return self.row

            def fetchall(self):
                return self.row

        class Connection:
            def __init__(self, rows):
                self.rows = list(rows)
                self.sql = []

            def execute(self, sql, params=None):
                self.sql.append(" ".join(sql.split()))
                return Result(self.rows.pop(0))

        unsafe = (
            ("superuser", [(42, True, False, False, False, False)], "unsafe"),
            ("createdb", [(42, False, True, False, False, False)], "unsafe"),
            ("createrole", [(42, False, False, True, False, False)], "unsafe"),
            ("replication", [(42, False, False, False, True, False)], "unsafe"),
            ("bypassrls", [(42, False, False, False, False, True)], "unsafe"),
            (
                "membership",
                [(42, False, False, False, False, False), (1,)],
                "memberships",
            ),
            (
                "database ownership",
                [(42, False, False, False, False, False), None, ("owned",)],
                "owns a database",
            ),
            (
                "schema ownership",
                [(42, False, False, False, False, False), None, None, ("nccl_raw",)],
                "owns a schema",
            ),
            (
                "relation ownership",
                [
                    (42, False, False, False, False, False),
                    None,
                    None,
                    None,
                    ("nccl_raw", "node_result"),
                ],
                "owns a relation",
            ),
        )
        for label, rows, message in unsafe:
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, message):
                _attest_runtime_role_reuse(Connection(rows), "cval_runtime")

        clean = Connection([
            (42, False, False, False, False, False),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [],
            ("cval_test_nccl",),
            [],
            [],
            [],
        ])
        receipt = _attest_runtime_role_reuse(clean, "cval_runtime")
        self.assertEqual(receipt["memberships"], 0)
        self.assertEqual(receipt["owned_objects"], 0)
        self.assertEqual(receipt["unexpected_direct_privileges"], 0)
        self.assertTrue(any("rolbypassrls" in sql for sql in clean.sql))


if __name__ == "__main__":
    unittest.main()
