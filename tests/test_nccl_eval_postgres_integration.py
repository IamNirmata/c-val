from __future__ import annotations

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import UUID, uuid4, uuid5

from cval.nccl_eval.config import NcclEvaluationConfig
from cval.nccl_eval.integration import (
    CLEANUP_CONFIRMATION,
    clean_nccl_schemas,
    require_disposable_test_url,
)
from cval.nccl_eval.models import IngestionBatch, NicResult, NodeResult, TestRun
from cval.nccl_eval.repository import (
    CalibrationDecision,
    NcclEvaluationRepository,
    RawConflictError,
    create_pool,
)
from cval.nccl_eval.schema import (
    Migration,
    apply_migrations,
    migrations,
    provision_runtime_role,
)


_CALIBRATION_DECISION_NAMESPACE = UUID("98b88b3e-69c9-5ac3-b6d7-83c64eb1537f")


def _calibration_decision_id(key: str) -> UUID:
    return uuid5(_CALIBRATION_DECISION_NAMESPACE, key)


class NcclEvalPostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        url = os.environ.get("CVAL_NCCL_TEST_DATABASE_URL")
        if not url:
            raise unittest.SkipTest("CVAL_NCCL_TEST_DATABASE_URL is not set")
        require_disposable_test_url(url)
        try:
            import psycopg  # noqa: F401
            import psycopg_pool  # noqa: F401
        except ImportError as exc:
            raise unittest.SkipTest(f"Psycopg integration dependencies unavailable: {exc}")
        cls.config = NcclEvaluationConfig(
            database_url=url,
            pool_min_size=1,
            pool_max_size=4,
        )
        cls.pool = create_pool(cls.config)

    @classmethod
    def tearDownClass(cls) -> None:
        if hasattr(cls, "pool"):
            clean_nccl_schemas(cls.pool, confirm=CLEANUP_CONFIRMATION)
            cls.pool.close()

    def setUp(self) -> None:
        clean_nccl_schemas(self.pool, confirm=CLEANUP_CONFIRMATION)
        apply_migrations(self.pool, allow_disposable_test_database=True)
        self.repository = NcclEvaluationRepository(self.pool, self.config)

    def batch(
        self,
        *,
        run_id=None,
        node_name="node-a",
        bus=44.0,
        config_extra: dict[str, object] | None = None,
        nics: tuple[NicResult, ...] = (),
    ) -> IngestionBatch:
        test_config: dict[str, object] = {
            "collective": "all_reduce",
            "datatype": "bfloat16",
            "reduction": "sum",
            "message_size": "16GiB",
            "warmup_iterations": 1,
            "latency_unit": "us",
        }
        test_config.update(config_extra or {})
        return IngestionBatch(
            TestRun(
                run_id=run_id or uuid4(),
                test_name="nccl-loopback-allreduce",
                test_definition_version="v1",
                started_at=datetime.now(timezone.utc),
                completed_at=None,
                image_name="example/image@sha256:" + "b" * 64,
                image_digest="sha256:" + "b" * 64,
                cuda_version="13.2",
                pytorch_version="2.12",
                compiled_nccl_version="2.27",
                runtime_nccl_package_version="nvidia-nccl-cu13==2.27.7",
                driver_version="600.1",
                driver_version_group="r600",
                topology_class="loopback-v1",
                gpu_model="B200",
                gpus_per_node=8,
                iterations=20,
                samples=20,
                cval_run_id=f"cval-{run_id or uuid4()}",
                cval_result_digest="sha256:" + "c" * 64,
                summary_sha256="sha256:" + "d" * 64,
                runtime_evidence_sha256="sha256:" + "e" * 64,
                source_commit="a" * 40,
                implementation_identity="sha256:" + "f" * 64,
                legacy_source=False,
                test_config=test_config,
            ),
            (
                NodeResult(
                    node_name,
                    datetime.now(timezone.utc),
                    bus,
                    600.0,
                    nics=nics,
                ),
            ),
        )

    def assert_db_rejects(self, sql: str, params=()) -> None:
        with self.assertRaises(Exception):
            with self.pool.connection() as connection:
                with connection.transaction():
                    connection.execute(sql, params)

    def ingest_calibration(
        self, *, prefix: str = "cal", start: int = 0, count: int = 40
    ) -> None:
        decisions = []
        for index in range(start, start + count):
            receipt = self.repository.ingest_batch(
                self.batch(
                    node_name=f"{prefix}-{index:02d}",
                    bus=40.0 + index / 10,
                )
            )
            decisions.append(
                CalibrationDecision(
                    decision_id=_calibration_decision_id(
                        f"{prefix}:approve:{index}"
                    ),
                    result_id=receipt["result_ids"][0],
                    action="APPROVE",
                    actor="integration-test",
                    reason="known-good disposable calibration fixture",
                    evidence={"fixture": prefix, "index": index},
                )
            )
        self.repository.apply_calibration(decisions)

    def test_calibration_ledger_controls_v1_revoke_and_v2_lineage(self) -> None:
        result_ids = []
        for index in range(40):
            receipt = self.repository.ingest_batch(
                self.batch(node_name=f"ledger-{index:02d}", bus=40.0 + index / 10)
            )
            result_ids.append(receipt["result_ids"][0])

        before = self.repository.build_baselines()
        self.assertEqual(before["built_count"], 0)
        self.assertEqual(
            before["profiles"][0]["eligible_result_count"], 0
        )
        approvals = tuple(
            CalibrationDecision(
                decision_id=_calibration_decision_id(f"ledger:v1:approve:{index}"),
                result_id=result_id,
                action="APPROVE",
                actor="integration-test",
                reason="known-good disposable calibration fixture",
                evidence={"cohort": "v1", "ordinal": index},
            )
            for index, result_id in enumerate(result_ids)
        )
        self.repository.apply_calibration(approvals)
        first = self.repository.build_baselines()
        self.assertEqual(first["built_count"], 1)
        self.assertEqual(first["profiles"][0]["version_number"], 1)
        self.assertEqual(first["profiles"][0]["sample_count"], 40)
        claim = self.repository.claim_jobs("history-worker", batch_size=1)[0]
        evaluated = self.repository.evaluate_claimed(claim)
        self.assertEqual(evaluated["job_status"], "COMPLETED")
        version_one_id = evaluated["baseline_version_id"]
        self.assert_db_rejects(
            """
            UPDATE nccl_baseline.baseline_version
            SET status = 'FAILED', failure_reason = 'direct mutation is forbidden'
            WHERE baseline_version_id = %s
            """,
            (version_one_id,),
        )

        revoke = self.repository.apply_calibration(
            (
                CalibrationDecision(
                    decision_id=_calibration_decision_id("ledger:v2:revoke:0"),
                    result_id=result_ids[0],
                    action="REVOKE",
                    actor="integration-test",
                    reason="fixture invalidated after review",
                    evidence={"cohort": "v2", "revokes": str(approvals[0].decision_id)},
                ),
            )
        )
        self.assertEqual(
            revoke["decisions"][0]["invalidated_baseline_version_id"],
            version_one_id,
        )
        replay = self.repository.apply_calibration((approvals[0],))
        self.assertFalse(replay["decisions"][0]["created"])
        self.assertEqual(replay["decisions"][0]["requested_action"], "APPROVE")
        self.assertEqual(replay["decisions"][0]["effective_action"], "REVOKE")
        with self.pool.connection() as connection:
            profile_after_revoke = connection.execute(
                """
                SELECT status, active_baseline_version_id
                FROM nccl_baseline.baseline_profile
                """
            ).fetchone()
            failed_version = connection.execute(
                """
                SELECT status, failure_reason
                FROM nccl_baseline.baseline_version
                WHERE version_number = 1
                """
            ).fetchone()
            queue_after_revoke = dict(
                connection.execute(
                    """
                    SELECT status, count(*) FROM nccl_validation.evaluation_job
                    GROUP BY status
                    """
                ).fetchall()
            )
        self.assertEqual(profile_after_revoke, ("COLLECTING", None))
        self.assertEqual(failed_version[0], "FAILED")
        self.assertIn("CALIBRATION_REVOKED_RESULT", failed_version[1])
        self.assertNotIn("PENDING", queue_after_revoke)
        self.assertNotIn("RETRY", queue_after_revoke)
        self.assertEqual(queue_after_revoke["WAITING_FOR_BASELINE"], 39)
        self.assertEqual(queue_after_revoke["COMPLETED"], 1)

        new_decisions = []
        for index in range(1):
            receipt = self.repository.ingest_batch(
                self.batch(node_name=f"ledger-new-{index:02d}", bus=45.0 + index / 10)
            )
            new_decisions.append(
                CalibrationDecision(
                    decision_id=_calibration_decision_id(
                        f"ledger:v2:approve:{index}"
                    ),
                    result_id=receipt["result_ids"][0],
                    action="APPROVE",
                    actor="integration-test",
                    reason="known-good v2 calibration fixture",
                    evidence={"cohort": "v2", "ordinal": index},
                )
            )
        self.repository.apply_calibration(tuple(new_decisions))
        second = self.repository.build_baselines()
        self.assertEqual(second["built_count"], 1)
        self.assertEqual(second["profiles"][0]["version_number"], 2)
        self.assertEqual(second["profiles"][0]["sample_count"], 40)
        self.assertEqual(second["profiles"][0]["excluded_result_count"], 1)
        self.assertEqual(second["profiles"][0]["superseded_version_id"], version_one_id)
        with self.pool.connection() as connection:
            events = connection.execute(
                "SELECT action, decision_version FROM nccl_baseline.calibration_decision "
                "WHERE result_id = %s ORDER BY decision_version",
                (result_ids[0],),
            ).fetchall()
            lineage = connection.execute(
                """
                SELECT sample.included, sample.exclusion_reason
                FROM nccl_baseline.baseline_version_sample AS sample
                JOIN nccl_baseline.baseline_version AS version USING (baseline_version_id)
                WHERE version.version_number = 2 AND sample.result_id = %s
                """,
                (result_ids[0],),
            ).fetchone()
            versions = connection.execute(
                """
                SELECT version_number, status, sample_count, supersedes_version_id
                FROM nccl_baseline.baseline_version ORDER BY version_number
                """
            ).fetchall()
            historical_evaluations = connection.execute(
                """
                SELECT count(*) FROM nccl_validation.evaluation AS evaluation
                JOIN nccl_baseline.baseline_version AS version USING (baseline_version_id)
                WHERE version.version_number = 1
                """
            ).fetchone()[0]
        self.assertEqual(events, [("APPROVE", 1), ("REVOKE", 2)])
        self.assertEqual(lineage, (False, "NOT_EXPLICITLY_APPROVED"))
        self.assertEqual(versions[0][1:3], ("FAILED", 40))
        self.assertEqual(versions[1][1:3], ("ACTIVE", 40))
        self.assertEqual(str(versions[1][3]), version_one_id)
        self.assertEqual(historical_evaluations, 1)

    def test_runtime_role_can_operate_but_cannot_administer_schema(self) -> None:
        import psycopg
        from psycopg import sql

        username = f"cval_runtime_{uuid4().hex[:12]}"
        password = "disposable-'quoted-create-password"
        created = provision_runtime_role(
            self.pool,
            username=username,
            password=password,
            allow_disposable_test_database=True,
        )
        self.assertTrue(created["created"])
        self.assertNotIn(password, repr(created))
        member_role = f"runtime_member_{uuid4().hex[:12]}"
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    sql.SQL("CREATE ROLE {}").format(sql.Identifier(member_role))
                )
                connection.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(username), sql.Identifier(member_role)
                    )
                )
        with self.assertRaisesRegex(ValueError, "memberships"):
            provision_runtime_role(
                self.pool,
                username=username,
                password="must-not-rotate-with-reverse-membership",
                allow_disposable_test_database=True,
            )
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(username), sql.Identifier(member_role)
                    )
                )
                connection.execute(
                    sql.SQL("DROP ROLE {}").format(sql.Identifier(member_role))
                )
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    sql.SQL("GRANT DELETE ON nccl_raw.node_result TO {}").format(
                        sql.Identifier(username)
                    )
                )
        with self.assertRaisesRegex(ValueError, "unexpected direct privilege"):
            provision_runtime_role(
                self.pool,
                username=username,
                password="must-not-rotate-after-unsafe-acl",
                allow_disposable_test_database=True,
            )
        owned_function = f"owned_by_{uuid4().hex[:12]}"
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    sql.SQL("REVOKE DELETE ON nccl_raw.node_result FROM {}").format(
                        sql.Identifier(username)
                    )
                )
                connection.execute(
                    sql.SQL("CREATE FUNCTION public.{}() RETURNS integer LANGUAGE SQL AS 'SELECT 1'").format(
                        sql.Identifier(owned_function)
                    )
                )
                connection.execute(
                    sql.SQL("ALTER FUNCTION public.{}() OWNER TO {}").format(
                        sql.Identifier(owned_function), sql.Identifier(username)
                    )
                )
        with self.assertRaisesRegex(ValueError, "owns a function"):
            provision_runtime_role(
                self.pool,
                username=username,
                password="must-not-rotate-after-owned-function",
                allow_disposable_test_database=True,
            )
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    sql.SQL("DROP FUNCTION public.{}()").format(
                        sql.Identifier(owned_function)
                    )
                )
        rotated_password = "disposable-'quoted-rotate-password"
        rotated = provision_runtime_role(
            self.pool,
            username=username,
            password=rotated_password,
            allow_disposable_test_database=True,
        )
        self.assertFalse(rotated["created"])
        self.assertNotIn(rotated_password, repr(rotated))
        runtime_url = psycopg.conninfo.make_conninfo(
            self.config.require_database_url(), user=username, password=rotated_password
        )
        runtime_config = replace(
            self.config,
            database_url=runtime_url,
            pool_min_size=1,
            pool_max_size=2,
        )
        runtime_pool = create_pool(runtime_config)
        runtime_repository = NcclEvaluationRepository(runtime_pool, runtime_config)
        try:
            rejected_password = "must-not-leak-'quoted-password"
            with self.assertRaisesRegex(
                RuntimeError, "^runtime role credential statement failed$"
            ) as caught:
                provision_runtime_role(
                    runtime_pool,
                    username=f"denied_role_{uuid4().hex[:12]}",
                    password=rejected_password,
                    allow_disposable_test_database=True,
                )
            self.assertNotIn(rejected_password, str(caught.exception))
            with self.assertRaises(Exception), runtime_pool.connection() as connection:
                with connection.transaction():
                    connection.execute(
                        """
                        INSERT INTO nccl_baseline.calibration_decision (
                            decision_id, result_id, decision_version, action,
                            actor, reason, evidence
                        ) VALUES (%s, 1, 1, 'APPROVE', 'runtime', 'forbidden', '{}'::jsonb)
                        """,
                        (uuid4(),),
                    )
            decisions = []
            for index in range(40):
                receipt = runtime_repository.ingest_batch(
                    self.batch(node_name=f"runtime-{index:02d}", bus=40 + index / 10)
                )
                decisions.append(
                    CalibrationDecision(
                        decision_id=_calibration_decision_id(
                            f"runtime:approve:{index}"
                        ),
                        result_id=receipt["result_ids"][0],
                        action="APPROVE",
                        actor="runtime-integration",
                        reason="disposable runtime calibration",
                        evidence={"ordinal": index},
                    )
                )
            runtime_repository.apply_calibration(tuple(decisions))
            self.assertEqual(runtime_repository.build_baselines()["built_count"], 1)
            claims = runtime_repository.claim_jobs("runtime-worker", batch_size=1)
            self.assertEqual(
                runtime_repository.evaluate_claimed(claims[0])["job_status"],
                "COMPLETED",
            )
            self.assertIn("profiles", runtime_repository.status(latest_limit=1))
            with self.assertRaises(Exception):
                apply_migrations(
                    runtime_pool, allow_disposable_test_database=True
                )
            for statement in (
                "TRUNCATE nccl_raw.node_result",
                "ALTER TABLE nccl_raw.node_result ADD COLUMN forbidden integer",
                "DROP TABLE nccl_raw.node_result",
                "ALTER TABLE nccl_raw.node_result DISABLE TRIGGER ALL",
            ):
                with self.assertRaises(Exception), runtime_pool.connection() as connection:
                    with connection.transaction():
                        connection.execute(statement)
        finally:
            runtime_repository.close()
            with self.pool.connection() as connection:
                with connection.transaction():
                    connection.execute(
                        sql.SQL("DROP OWNED BY {}").format(sql.Identifier(username))
                    )
                    connection.execute(
                        sql.SQL("DROP ROLE {}").format(sql.Identifier(username))
                    )

    def test_outbox_exact_replay_has_zero_database_progression_writes(self) -> None:
        batch = self.batch(node_name="outbox-replay")
        first = self.repository.ingest_outbox_batch(
            batch,
            outbox_name="outbox-replay.json",
            content_sha256="a" * 64,
            observed_fingerprint="sha256:" + "b" * 64,
        )
        with self.pool.connection() as connection:
            before = tuple(
                connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in (
                    "nccl_raw.test_run",
                    "nccl_raw.node_result",
                    "nccl_baseline.baseline_profile",
                    "nccl_validation.evaluation_job",
                    "nccl_raw.outbox_receipt",
                )
            )
        second = self.repository.ingest_outbox_batch(
            batch,
            outbox_name="outbox-replay.json",
            content_sha256="a" * 64,
            observed_fingerprint="sha256:" + "b" * 64,
        )
        with self.pool.connection() as connection:
            after = tuple(
                connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in (
                    "nccl_raw.test_run",
                    "nccl_raw.node_result",
                    "nccl_baseline.baseline_profile",
                    "nccl_validation.evaluation_job",
                    "nccl_raw.outbox_receipt",
                )
            )
        self.assertTrue(first["receipt_created"])
        self.assertFalse(second["receipt_created"])
        self.assertEqual(before, after)

    def test_migration_replay_checksum_mismatch_and_concurrent_apply(self) -> None:
        replay = apply_migrations(
            self.pool, allow_disposable_test_database=True
        )
        self.assertEqual(replay["applied"], [])
        self.assertEqual(
            replay["already_applied"],
            ["001_initial.sql", "002_native_only_ingestion.sql"],
        )

        original = migrations()[0]
        altered = Migration(original.migration_id, "0" * 64, original.sql)
        with patch("cval.nccl_eval.schema.migrations", return_value=(altered,)):
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                apply_migrations(
                    self.pool, allow_disposable_test_database=True
                )

        clean_nccl_schemas(self.pool, confirm=CLEANUP_CONFIRMATION)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    apply_migrations,
                    self.pool,
                    allow_disposable_test_database=True,
                )
                for _ in range(2)
            ]
            receipts = [future.result() for future in futures]
        self.assertEqual(sum(len(item["applied"]) for item in receipts), 1)
        self.assertEqual(
            sum(len(item["already_applied"]) for item in receipts), 1
        )

    def test_foreign_keys_immutability_ledger_and_truncate_guards(self) -> None:
        self.assert_db_rejects(
            """
            INSERT INTO nccl_raw.node_result
                (run_id, node_name, test_timestamp, result_status)
            VALUES (%s, 'orphan', now(), 'NO_RESULT')
            """,
            (uuid4(),),
        )
        receipt = self.repository.ingest_batch(self.batch())
        self.assert_db_rejects(
            "UPDATE nccl_raw.node_result SET node_name = 'changed' WHERE result_id = %s",
            (receipt["result_ids"][0],),
        )
        self.assert_db_rejects(
            "UPDATE nccl_raw.schema_migration SET sha256 = %s",
            ("0" * 64,),
        )
        self.assert_db_rejects(
            "UPDATE nccl_baseline.baseline_profile SET test_name = 'changed'"
        )
        self.assert_db_rejects("DELETE FROM nccl_baseline.baseline_profile")
        for table in (
            "nccl_raw.schema_migration",
            "nccl_raw.test_run",
            "nccl_raw.outbox_receipt",
            "nccl_raw.node_result",
            "nccl_raw.nic_result",
            "nccl_baseline.baseline_profile",
            "nccl_baseline.baseline_version",
            "nccl_baseline.baseline_version_sample",
            "nccl_baseline.metric_threshold",
            "nccl_validation.health_class",
            "nccl_validation.evaluation_job",
            "nccl_validation.evaluation",
        ):
            with self.subTest(table=table):
                self.assert_db_rejects(f"TRUNCATE {table} CASCADE")

    def test_admin_direct_calibration_dml_rejects_invalid_sequence_and_action(self) -> None:
        receipt = self.repository.ingest_batch(self.batch(node_name="direct-calibration"))
        result_id = receipt["result_ids"][0]
        insert = """
            INSERT INTO nccl_baseline.calibration_decision (
                decision_id, result_id, decision_version, action, actor, reason, evidence
            ) VALUES (%s, %s, %s, %s, 'integration-test', 'direct guard test', '{}'::jsonb)
        """
        self.assert_db_rejects(insert, (uuid4(), result_id, 1, "REVOKE"))
        first_decision_id = uuid4()
        self.repository.apply_calibration(
            (
                CalibrationDecision(
                    decision_id=first_decision_id,
                    result_id=result_id,
                    action="APPROVE",
                    actor="integration-test",
                    reason="valid first decision",
                    evidence={},
                ),
            )
        )
        self.assert_db_rejects(
            insert, (first_decision_id, result_id, 1, "APPROVE")
        )
        self.assert_db_rejects(insert, (uuid4(), result_id, 2, "APPROVE"))
        self.assert_db_rejects(insert, (uuid4(), result_id, 3, "REVOKE"))

    def test_run_fingerprint_rejects_type_and_signed_zero_changes_before_new_node(self) -> None:
        for left, right in ((True, 1), (1, 1.0), (-0.0, 0.0)):
            with self.subTest(left=left, right=right):
                original = self.batch(
                    node_name=f"original-{type(left).__name__}",
                    config_extra={"variant": left},
                )
                self.repository.ingest_batch(original)
                changed_run = replace(
                    original.test_run,
                    test_config=dict(original.test_run.test_config)
                    | {"variant": right},
                )
                retry = IngestionBatch(
                    changed_run,
                    (
                        NodeResult(
                            "new-node",
                            datetime.now(timezone.utc),
                            44.0,
                            600.0,
                        ),
                    ),
                )
                with self.assertRaises(RawConflictError):
                    self.repository.ingest_batch(retry)
                with self.pool.connection() as connection:
                    with connection.transaction():
                        count = connection.execute(
                            """
                            SELECT count(*) FROM nccl_raw.node_result
                            WHERE run_id = %s AND node_name = 'new-node'
                            """,
                            (original.test_run.run_id,),
                        ).fetchone()[0]
                self.assertEqual(count, 0)

    def test_foreign_keys_fixed_classes_and_ingestion_idempotency(self) -> None:
        batch = self.batch()
        first = self.repository.ingest_batch(batch)
        second = self.repository.ingest_batch(batch)
        self.assertEqual(first["inserted_node_count"], 1)
        self.assertEqual(second["inserted_node_count"], 0)
        with self.pool.connection() as connection:
            with connection.transaction():
                classes = connection.execute(
                    "SELECT class_id, class_code FROM nccl_validation.health_class ORDER BY class_id"
                ).fetchall()
        self.assertEqual([row[0] for row in classes], [1, 2, 3, 4, 5])
        self.assertEqual(classes[-1][1], "CRITICAL")

        conflicting = IngestionBatch(
            batch.test_run,
            (
                NodeResult(
                    "node-a",
                    batch.node_results[0].test_timestamp,
                    1.0,
                    600.0,
                ),
            ),
        )
        with self.assertRaises(RawConflictError):
            self.repository.ingest_batch(conflicting)

    def test_multi_node_and_nic_ingestion_rolls_back_on_late_conflict(self) -> None:
        original = self.batch(node_name="node-b")
        self.repository.ingest_batch(original)
        retry = IngestionBatch(
            original.test_run,
            (
                NodeResult(
                    "node-a",
                    datetime.now(timezone.utc),
                    44.0,
                    600.0,
                    nics=(NicResult("mlx5_0", 43.0),),
                ),
                NodeResult(
                    "node-b",
                    original.node_results[0].test_timestamp,
                    1.0,
                    600.0,
                ),
            ),
        )
        with self.assertRaises(RawConflictError):
            self.repository.ingest_batch(retry)
        with self.pool.connection() as connection:
            with connection.transaction():
                node_a = connection.execute(
                    """
                    SELECT result_id FROM nccl_raw.node_result
                    WHERE run_id = %s AND node_name = 'node-a'
                    """,
                    (original.test_run.run_id,),
                ).fetchone()
                nic_count = connection.execute(
                    "SELECT count(*) FROM nccl_raw.nic_result"
                ).fetchone()[0]
        self.assertIsNone(node_a)
        self.assertEqual(nic_count, 0)

    def test_baseline_40_then_50_versions_and_history(self) -> None:
        self.ingest_calibration(prefix="version-history", count=40)
        first = self.repository.build_baselines()
        self.assertEqual(first["built_count"], 1)
        self.assertEqual(first["profiles"][0]["sample_count"], 40)
        self.ingest_calibration(prefix="version-history", start=40, count=9)
        self.assertEqual(self.repository.build_baselines()["built_count"], 0)
        self.ingest_calibration(prefix="version-history", start=49, count=1)
        second = self.repository.build_baselines()
        self.assertEqual(second["built_count"], 1)
        self.assertEqual(second["profiles"][0]["sample_count"], 50)
        with self.pool.connection() as connection:
            with connection.transaction():
                versions = connection.execute(
                    """
                    SELECT version_number, status, sample_count
                    FROM nccl_baseline.baseline_version ORDER BY version_number
                    """
                ).fetchall()
                lineage = connection.execute(
                    """
                    SELECT version.version_number, count(*) FILTER (WHERE sample.included)
                    FROM nccl_baseline.baseline_version AS version
                    JOIN nccl_baseline.baseline_version_sample AS sample USING (baseline_version_id)
                    GROUP BY version.version_number ORDER BY version.version_number
                    """
                ).fetchall()
        self.assertEqual(versions, [(1, "SUPERSEDED", 40), (2, "ACTIVE", 50)])
        self.assertEqual(lineage, [(1, 40), (2, 50)])

    def test_active_baseline_children_cannot_be_reparented_to_building_version(self) -> None:
        self.ingest_calibration(prefix="immutable-child")
        self.assertEqual(self.repository.build_baselines()["built_count"], 1)
        building_id = uuid4()
        with self.pool.connection() as connection:
            with connection.transaction():
                profile_id, active_id = connection.execute(
                    """
                    SELECT profile_id, active_baseline_version_id
                    FROM nccl_baseline.baseline_profile
                    """
                ).fetchone()
                connection.execute(
                    """
                    INSERT INTO nccl_baseline.baseline_version (
                        baseline_version_id, profile_id, version_number, status,
                        sample_count, supersedes_version_id, derivation_method_version,
                        bus_bw_mean, bus_bw_p05, bus_bw_p50, bus_bw_p95,
                        latency_mean, latency_p05, latency_p50, latency_p95
                    )
                    SELECT %s, profile_id, 2, 'BUILDING', sample_count,
                           baseline_version_id, derivation_method_version,
                           bus_bw_mean, bus_bw_p05, bus_bw_p50, bus_bw_p95,
                           latency_mean, latency_p05, latency_p50, latency_p95
                    FROM nccl_baseline.baseline_version
                    WHERE baseline_version_id = %s
                    """,
                    (building_id, active_id),
                )

        self.assert_db_rejects(
            """
            UPDATE nccl_baseline.baseline_version_sample
            SET baseline_version_id = %s
            WHERE baseline_version_id = %s
              AND result_id = (
                  SELECT min(result_id)
                  FROM nccl_baseline.baseline_version_sample
                  WHERE baseline_version_id = %s AND included
              )
            """,
            (building_id, active_id, active_id),
        )
        self.assert_db_rejects(
            """
            UPDATE nccl_baseline.metric_threshold
            SET baseline_version_id = %s
            WHERE baseline_version_id = %s
            """,
            (building_id, active_id),
        )

        with self.pool.connection() as connection:
            with connection.transaction():
                active_state = connection.execute(
                    """
                    SELECT version.status,
                           count(sample.result_id) FILTER (WHERE sample.included),
                           (SELECT count(*)
                            FROM nccl_baseline.metric_threshold AS threshold
                            WHERE threshold.baseline_version_id = version.baseline_version_id)
                    FROM nccl_baseline.baseline_version AS version
                    LEFT JOIN nccl_baseline.baseline_version_sample AS sample
                      USING (baseline_version_id)
                    WHERE version.baseline_version_id = %s
                    GROUP BY version.baseline_version_id, version.status
                    """,
                    (active_id,),
                ).fetchone()
                building_children = connection.execute(
                    """
                    SELECT
                        (SELECT count(*) FROM nccl_baseline.baseline_version_sample
                         WHERE baseline_version_id = %s),
                        (SELECT count(*) FROM nccl_baseline.metric_threshold
                         WHERE baseline_version_id = %s)
                    """,
                    (building_id, building_id),
                ).fetchone()
        self.assertEqual(active_state, ("ACTIVE", 40, 10))
        self.assertEqual(building_children, (0, 0))

    def test_threshold_activation_and_evaluation_constraints_reject_invalid_rows(self) -> None:
        self.ingest_calibration()
        self.assertEqual(self.repository.build_baselines()["built_count"], 1)
        with self.pool.connection() as connection:
            with connection.transaction():
                profile_id, baseline_id = connection.execute(
                    """
                    SELECT profile_id, active_baseline_version_id
                    FROM nccl_baseline.baseline_profile
                    """
                ).fetchone()
                result_id = connection.execute(
                    "SELECT min(result_id) FROM nccl_raw.node_result"
                ).fetchone()[0]

        direct_active = """
            INSERT INTO nccl_baseline.baseline_version (
                baseline_version_id, profile_id, version_number, status,
                sample_count, activated_at, supersedes_version_id,
                derivation_method_version,
                bus_bw_mean, bus_bw_p05, bus_bw_p50, bus_bw_p95,
                latency_mean, latency_p05, latency_p50, latency_p95
            ) VALUES (
                %s, %s, 2, 'ACTIVE', 40, now(), %s, 'invalid',
                1, 1, 1, 1, 1, 1, 1, 1
            )
        """
        self.assert_db_rejects(direct_active, (uuid4(), profile_id, baseline_id))

        with self.assertRaises(Exception):
            with self.pool.connection() as connection:
                with connection.transaction():
                    version_id = uuid4()
                    connection.execute(
                        """
                        INSERT INTO nccl_baseline.baseline_version (
                            baseline_version_id, profile_id, version_number, status,
                            sample_count, supersedes_version_id, derivation_method_version,
                            bus_bw_mean, bus_bw_p05, bus_bw_p50, bus_bw_p95,
                            latency_mean, latency_p05, latency_p50, latency_p95
                        ) VALUES (%s, %s, 2, 'BUILDING', 40, %s, 'invalid',
                                  1, 1, 1, 1, 1, 1, 1, 1)
                        """,
                        (version_id, profile_id, baseline_id),
                    )
                    connection.execute(
                        """
                        INSERT INTO nccl_baseline.metric_threshold
                            (baseline_version_id, metric_name, class_id,
                             lower_bound, upper_bound, unit)
                        VALUES (%s, 'BUS_BW', 5, 0, 1, 'GB/s')
                        """,
                        (version_id,),
                    )

        with self.assertRaises(Exception):
            with self.pool.connection() as connection:
                with connection.transaction():
                    version_id = uuid4()
                    connection.execute(
                        """
                        INSERT INTO nccl_baseline.baseline_version (
                            baseline_version_id, profile_id, version_number, status,
                            sample_count, supersedes_version_id, derivation_method_version,
                            bus_bw_mean, bus_bw_p05, bus_bw_p50, bus_bw_p95,
                            latency_mean, latency_p05, latency_p50, latency_p95
                        ) VALUES (%s, %s, 2, 'BUILDING', 40, %s, 'invalid',
                                  1, 1, 1, 1, 1, 1, 1, 1)
                        """,
                        (version_id, profile_id, baseline_id),
                    )
                    connection.execute(
                        """
                        INSERT INTO nccl_baseline.metric_threshold
                            (baseline_version_id, metric_name, class_id,
                             lower_bound, upper_bound, unit)
                        VALUES
                            (%s, 'BUS_BW', 1, 0, 1, 'GB/s'),
                            (%s, 'BUS_BW', 2, 1, 2, 'GB/s'),
                            (%s, 'BUS_BW', 3, 2, 3, 'GB/s'),
                            (%s, 'BUS_BW', 4, 3, 4, 'GB/s'),
                            (%s, 'BUS_BW', 5, 4, NULL, 'GB/s')
                        """,
                        (version_id,) * 5,
                    )

        with self.assertRaises(Exception):
            with self.pool.connection() as connection:
                with connection.transaction():
                    version_id = uuid4()
                    connection.execute(
                        """
                        INSERT INTO nccl_baseline.baseline_version (
                            baseline_version_id, profile_id, version_number, status,
                            sample_count, supersedes_version_id, derivation_method_version,
                            bus_bw_mean, bus_bw_p05, bus_bw_p50, bus_bw_p95,
                            latency_mean, latency_p05, latency_p50, latency_p95
                        ) VALUES (%s, %s, 2, 'BUILDING', 40, %s, 'invalid',
                                  1, 1, 1, 1, 1, 1, 1, 1)
                        """,
                        (version_id, profile_id, baseline_id),
                    )
                    connection.execute(
                        """
                        UPDATE nccl_baseline.baseline_version
                        SET status = 'ACTIVE', activated_at = now()
                        WHERE baseline_version_id = %s
                        """,
                        (version_id,),
                    )

        evaluation_sql = """
            INSERT INTO nccl_validation.evaluation (
                result_id, baseline_version_id, evaluation_scope,
                bus_bw_class, bus_bw_severity_percentile,
                latency_class, latency_severity_percentile,
                overall_health_class, overall_severity_percentile,
                evaluator_version
            ) VALUES (%s, %s, 'IN_SAMPLE', %s, %s, %s, %s, %s, %s, 'test')
        """
        self.assert_db_rejects(
            evaluation_sql,
            (result_id, baseline_id, None, 10.0, 2, 20.0, 2, 20.0),
        )
        self.assert_db_rejects(
            evaluation_sql,
            (result_id, baseline_id, 1, 10.0, 2, 20.0, 1, 20.0),
        )
        self.assert_db_rejects(
            evaluation_sql,
            (result_id, baseline_id, 1, 10.0, 2, 20.0, 2, 10.0),
        )

    def test_concurrent_builders_create_one_active_version(self) -> None:
        self.ingest_calibration(prefix="builder")

        def build():
            return self.repository.build_baselines()

        with ThreadPoolExecutor(max_workers=2) as executor:
            receipts = [future.result() for future in (executor.submit(build), executor.submit(build))]
        self.assertEqual(sum(item["built_count"] for item in receipts), 1)
        with self.pool.connection() as connection:
            with connection.transaction():
                active = connection.execute(
                    """
                    SELECT count(*) FROM nccl_baseline.baseline_version
                    WHERE status = 'ACTIVE'
                    """
                ).fetchone()[0]
        self.assertEqual(active, 1)

    def test_concurrent_workers_claim_disjoint_jobs(self) -> None:
        for index in range(8):
            self.repository.ingest_batch(self.batch(node_name=f"claim-{index}"))
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    "UPDATE nccl_validation.evaluation_job SET status = 'PENDING'"
                )

        def claim(worker: str):
            return self.repository.claim_jobs(worker, batch_size=4)

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(claim, "worker-a")
            second_future = executor.submit(claim, "worker-b")
            first = {item.result_id for item in first_future.result()}
            second = {item.result_id for item in second_future.result()}
        self.assertEqual(len(first), 4)
        self.assertEqual(len(second), 4)
        self.assertFalse(first & second)

    def test_claim_fencing_blocks_stale_worker_after_recovery_and_reclaim(self) -> None:
        self.ingest_calibration(prefix="fence")
        self.assertEqual(self.repository.build_baselines()["built_count"], 1)
        stale = self.repository.claim_jobs("same-worker", batch_size=1)[0]
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    """
                    UPDATE nccl_validation.evaluation_job
                    SET status = 'WAITING_FOR_BASELINE'
                    WHERE result_id <> %s AND status = 'PENDING'
                    """,
                    (stale.result_id,),
                )
                connection.execute(
                    """
                    UPDATE nccl_validation.evaluation_job
                    SET claimed_at = now() - interval '1 hour'
                    WHERE result_id = %s
                    """,
                    (stale.result_id,),
                )
        recovered = self.repository.recover_stale_claims()
        self.assertEqual(recovered["recovered_count"], 1)
        fresh = self.repository.claim_jobs("same-worker", batch_size=1)[0]
        self.assertEqual(fresh.result_id, stale.result_id)
        self.assertEqual(fresh.attempt_count, stale.attempt_count + 1)
        self.assertNotEqual(fresh.claim_token, stale.claim_token)

        with self.assertRaisesRegex(RuntimeError, "stale or invalid"):
            self.repository._load_evaluation_work(stale)
        with self.assertRaisesRegex(RuntimeError, "not claimed"):
            self.repository.schedule_retry(stale, RuntimeError("late retry"))
        with self.assertRaisesRegex(RuntimeError, "stale or invalid"):
            self.repository.evaluate_claimed(stale)

        completed = self.repository.evaluate_claimed(fresh)
        self.assertEqual(completed["job_status"], "COMPLETED")
        with self.pool.connection() as connection:
            with connection.transaction():
                token = connection.execute(
                    """
                    SELECT claim_token FROM nccl_validation.evaluation_job
                    WHERE result_id = %s
                    """,
                    (fresh.result_id,),
                ).fetchone()[0]
        self.assertIsNone(token)

    def test_retry_reclaim_always_rotates_claim_token(self) -> None:
        self.ingest_calibration(prefix="retry")
        self.repository.build_baselines()
        first = self.repository.claim_jobs("retry-worker", batch_size=1)[0]
        retry = self.repository.schedule_retry(first, RuntimeError("temporary"))
        self.assertEqual(retry["job_status"], "RETRY")
        with self.pool.connection() as connection:
            with connection.transaction():
                connection.execute(
                    """
                    UPDATE nccl_validation.evaluation_job
                    SET next_attempt_at = now()
                    WHERE result_id = %s
                    """,
                    (first.result_id,),
                )
        second = self.repository.claim_jobs("retry-worker", batch_size=1)[0]
        self.assertEqual(second.result_id, first.result_id)
        self.assertEqual(second.attempt_count, first.attempt_count + 1)
        self.assertNotEqual(second.claim_token, first.claim_token)


if __name__ == "__main__":
    unittest.main()
