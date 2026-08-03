from __future__ import annotations

import sys
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID, uuid4

from cval.nccl_eval.config import NcclEvaluationConfig
from cval.nccl_eval.models import IngestionBatch, NodeResult, TestRun
from cval.nccl_eval.repository import (
    CLAIM_JOBS_SQL,
    CLAIM_JOB_SQL,
    CalibrationDecision,
    ClaimReceipt,
    EvaluationWork,
    NcclEvaluationRepository,
    RawConflictError,
    assess_eligibility,
    baseline_build_due,
    create_pool,
    default_worker_id,
    parse_calibration_input,
)
from cval.nccl_eval.thresholds import MetricName, derive_thresholds


UTC = timezone.utc


def run() -> TestRun:
    return TestRun(
        run_id=UUID("11111111-1111-4111-8111-111111111111"),
        test_name="nccl-loopback-allreduce",
        test_definition_version="v1",
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
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
        cval_run_id="node-a-123",
        cval_result_digest="sha256:" + "c" * 64,
        summary_sha256="sha256:" + "d" * 64,
        runtime_evidence_sha256="sha256:" + "e" * 64,
        source_commit="a" * 40,
        implementation_identity="sha256:" + "f" * 64,
        legacy_source=False,
        test_config={
            "collective": "all_reduce",
            "datatype": "bfloat16",
            "reduction": "sum",
            "message_size": "16GiB",
            "warmup_iterations": 1,
            "latency_unit": "us",
        },
    )


def node() -> NodeResult:
    return NodeResult(
        "node-a",
        datetime(2026, 1, 1, tzinfo=UTC),
        44.0,
        600.0,
    )


def claim(*, attempt_count: int = 1, token: UUID | None = None) -> ClaimReceipt:
    return ClaimReceipt(1, attempt_count, token or uuid4(), "worker-1")


class Result:
    def __init__(self, *, one=None, all_rows=(), rowcount=0):
        self._one = one
        self._all = list(all_rows)
        self.rowcount = rowcount

    def fetchone(self):
        return self._one

    def fetchall(self):
        return list(self._all)


class ScriptedConnection:
    def __init__(self, scripts):
        self.scripts = list(scripts)
        self.executed: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, object]] = []
        self.commits = 0
        self.rollbacks = 0

    @contextmanager
    def transaction(self):
        try:
            yield
        except BaseException:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.executed.append((normalized, params))
        if not self.scripts:
            return Result()
        script = self.scripts.pop(0)
        expected = script.get("contains")
        if expected:
            assert expected in normalized, (expected, normalized)
        if "raise" in script:
            raise script["raise"]
        return Result(
            one=script.get("one"),
            all_rows=script.get("all", ()),
            rowcount=script.get("rowcount", 0),
        )

    def executemany(self, sql, params):
        self.executemany_calls.append((" ".join(sql.split()), list(params)))

    @contextmanager
    def cursor(self):
        yield self


class Pool:
    def __init__(self, *connections):
        self.connections = list(connections)

    @contextmanager
    def connection(self):
        yield self.connections.pop(0)

    def close(self):
        pass


class NcclEvalRepositoryTests(unittest.TestCase):
    def test_calibration_payload_is_exact_bounded_and_idempotency_keyed(self) -> None:
        decision_id = uuid4()
        decisions = parse_calibration_input(
            {
                "decisions": [
                    {
                        "decision_id": str(decision_id),
                        "result_id": 7,
                        "action": "APPROVE",
                        "actor": "operator@example",
                        "reason": "reviewed known-good calibration result",
                        "evidence": {"ticket": "CAL-7"},
                    }
                ]
            }
        )
        self.assertEqual(decisions[0].decision_id, decision_id)
        with self.assertRaisesRegex(ValueError, "invalid calibration decision fields"):
            parse_calibration_input(
                {
                    "decisions": [
                        decisions[0].__dict__ | {"unexpected": True}
                    ]
                }
            )
        with self.assertRaisesRegex(ValueError, "actor"):
            CalibrationDecision(
                decision_id=uuid4(),
                result_id=7,
                action="APPROVE",
                actor="secret actor with spaces",
                reason="reviewed",
                evidence={},
            )

    def test_calibration_approve_and_revoke_append_and_adjust_eligibility(self) -> None:
        approve = CalibrationDecision(
            decision_id=uuid4(),
            result_id=7,
            action="APPROVE",
            actor="operator",
            reason="known-good calibration",
            evidence={"ticket": "CAL-7"},
        )
        approve_connection = ScriptedConnection(
            [
                {"contains": "WHERE decision_id", "one": None},
                {
                    "contains": "apply_calibration_decision",
                    "one": (1, "APPROVE", "APPROVE", True, 1, None, 0),
                },
            ]
        )
        repository = NcclEvaluationRepository(
            Pool(approve_connection), NcclEvaluationConfig()
        )
        receipt = repository.apply_calibration((approve,))
        self.assertEqual(receipt["decisions"][0]["eligible_delta"], 1)

        revoke = CalibrationDecision(
            decision_id=uuid4(),
            result_id=7,
            action="REVOKE",
            actor="operator",
            reason="calibration evidence invalidated",
            evidence={"ticket": "CAL-8"},
        )
        revoke_connection = ScriptedConnection(
            [
                {"contains": "WHERE decision_id", "one": None},
                {
                    "contains": "apply_calibration_decision",
                    "one": (2, "REVOKE", "REVOKE", True, -1, uuid4(), 3),
                },
            ]
        )
        repository = NcclEvaluationRepository(
            Pool(revoke_connection), NcclEvaluationConfig()
        )
        receipt = repository.apply_calibration((revoke,))
        self.assertEqual(receipt["decisions"][0]["decision_version"], 2)
        self.assertEqual(receipt["decisions"][0]["eligible_delta"], -1)
        self.assertEqual(receipt["decisions"][0]["waiting_jobs_updated"], 3)

    def test_old_approve_replay_reports_latest_effective_revoke(self) -> None:
        approve = CalibrationDecision(
            decision_id=uuid4(),
            result_id=7,
            action="APPROVE",
            actor="operator",
            reason="known-good calibration",
            evidence={"ticket": "CAL-7"},
        )
        existing = (
            approve.decision_id,
            approve.result_id,
            1,
            approve.action,
            approve.actor,
            approve.reason,
            dict(approve.evidence),
        )
        connection = ScriptedConnection(
            [
                {"contains": "WHERE decision_id", "one": existing},
                {
                    "contains": "apply_calibration_decision",
                    "one": (1, "APPROVE", "REVOKE", False, 0, None, 0),
                },
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        receipt = repository.apply_calibration((approve,))

        event = receipt["decisions"][0]
        self.assertFalse(event["created"])
        self.assertEqual(event["requested_action"], "APPROVE")
        self.assertEqual(event["action"], "REVOKE")
        self.assertEqual(event["effective_action"], "REVOKE")

    def test_calibration_conflicting_decision_id_retry_fails(self) -> None:
        decision = CalibrationDecision(
            decision_id=uuid4(),
            result_id=7,
            action="APPROVE",
            actor="operator",
            reason="reviewed",
            evidence={"ticket": "CAL-7"},
        )
        connection = ScriptedConnection(
            [
                {
                    "contains": "WHERE decision_id",
                    "one": (
                        decision.decision_id, 7, 1, "APPROVE", "other-actor",
                        "reviewed", {"ticket": "CAL-7"},
                    ),
                },
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())
        with self.assertRaises(RawConflictError):
            repository.apply_calibration((decision,))

    def test_baseline_gate_exact_39_40_49_50_and_refinement(self) -> None:
        self.assertFalse(baseline_build_due(39, 0, has_active_baseline=False))
        self.assertTrue(baseline_build_due(40, 0, has_active_baseline=False))
        self.assertFalse(baseline_build_due(49, 40, has_active_baseline=True))
        self.assertTrue(baseline_build_due(50, 40, has_active_baseline=True))
        self.assertFalse(baseline_build_due(59, 50, has_active_baseline=True))
        self.assertTrue(baseline_build_due(60, 50, has_active_baseline=True))

    def test_eligibility_records_exact_inclusion_and_exclusion_reasons(self) -> None:
        included = assess_eligibility(
            1,
            result_status="SUCCESS",
            bus_bw_gbps=44.0,
            latency_us=600.0,
            error_code=None,
            effective_calibration_action="APPROVE",
        )
        self.assertTrue(included.included)
        cases = (
            ("TIMEOUT", 1.0, 1.0, None, True, "RESULT_STATUS_TIMEOUT"),
            ("SUCCESS", None, 1.0, None, True, "MISSING_BUS_BW"),
            ("SUCCESS", 1.0, None, None, True, "MISSING_LATENCY"),
            ("SUCCESS", 0.0, 1.0, None, True, "INVALID_BUS_BW"),
            ("SUCCESS", 1.0, 0.0, None, True, "INVALID_LATENCY"),
            ("SUCCESS", 1.0, 1.0, "NCCL_ERROR", True, "BLOCKING_ERROR"),
            ("SUCCESS", 1.0, 1.0, None, False, "NOT_EXPLICITLY_APPROVED"),
        )
        for status, bus, latency, error, approved, reason in cases:
            with self.subTest(reason=reason):
                decision = assess_eligibility(
                    2,
                    result_status=status,
                    bus_bw_gbps=bus,
                    latency_us=latency,
                    error_code=error,
                    effective_calibration_action=("APPROVE" if approved else None),
                )
                self.assertFalse(decision.included)
                self.assertEqual(decision.exclusion_reason, reason)

    def test_claim_is_one_short_explicit_transaction_with_skip_locked(self) -> None:
        first_token = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        second_token = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
        connection = ScriptedConnection(
            [
                {"contains": "FOR UPDATE SKIP LOCKED", "all": [(10,), (11,)]},
                {"contains": "claim_token", "one": (10, 1, first_token)},
                {"contains": "claim_token", "one": (11, 1, second_token)},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        claimed = repository.claim_jobs("worker-1", batch_size=2)

        self.assertEqual(
            claimed,
            [
                ClaimReceipt(10, 1, first_token, "worker-1"),
                ClaimReceipt(11, 1, second_token, "worker-1"),
            ],
        )
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        self.assertIn("FOR UPDATE SKIP LOCKED", CLAIM_JOBS_SQL)
        self.assertIn("claim_token = %s", CLAIM_JOB_SQL)
        self.assertEqual(connection.executed[0][1], (8, 2))
        self.assertEqual(connection.executed[1][1][0], "worker-1")
        self.assertIsInstance(connection.executed[1][1][1], UUID)

    def test_ingestion_failure_rolls_back_the_whole_batch(self) -> None:
        profile_id = UUID("de8650a8-dbd2-5e93-aec6-a4dca57600ae")
        # Profile identity is deterministic but calculated by production code;
        # return its exact payload after the profile insert.
        from cval.nccl_eval.profile import build_profile_identity

        profile = build_profile_identity(run())
        connection = ScriptedConnection(
            [
                {"contains": "INSERT INTO nccl_raw.test_run", "one": (run().run_id,)},
                {"contains": "INSERT INTO nccl_baseline.baseline_profile"},
                {
                    "contains": "FROM nccl_baseline.baseline_profile WHERE profile_key",
                    "one": (
                        profile.profile_id,
                        profile.test_name,
                        profile.test_definition_version,
                        profile.gpu_model,
                        profile.gpus_per_node,
                        profile.cuda_version,
                        profile.pytorch_version,
                        profile.compiled_nccl_version,
                        profile.runtime_nccl_package_version,
                        profile.driver_version_group,
                        profile.topology_class,
                        profile.source_commit,
                        profile.image_digest,
                        profile.implementation_identity,
                        profile.test_config_fingerprint,
                        dict(profile.test_config),
                    ),
                },
                {"contains": "FOR KEY SHARE", "one": (None,)},
                {
                    "contains": "INSERT INTO nccl_raw.node_result",
                    "raise": RuntimeError("injected node insert failure"),
                },
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        with self.assertRaisesRegex(RuntimeError, "injected"):
            repository.ingest_batch(IngestionBatch(run(), (node(),)))

        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)
        self.assertFalse(connection.scripts)
        self.assertNotEqual(profile_id, profile.profile_id)  # fixed fake IDs are never trusted

    def test_outbox_receipt_commits_with_ingestion_and_rejects_content_conflict(self) -> None:
        from cval.nccl_eval.profile import build_profile_identity

        batch = IngestionBatch(run(), (node(),))
        profile = build_profile_identity(batch.test_run)
        connection = ScriptedConnection(
            [
                {"contains": "FROM nccl_raw.outbox_receipt", "one": None},
                {"contains": "INSERT INTO nccl_raw.outbox_receipt"},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())
        with patch.object(
            repository,
            "_ingest_batch",
            return_value={"run_id": str(batch.test_run.run_id)},
        ):
            receipt = repository.ingest_outbox_batch(
                batch,
                outbox_name="node-a.json",
                content_sha256="a" * 64,
                observed_fingerprint="sha256:" + "b" * 64,
            )
        self.assertTrue(receipt["receipt_created"])
        self.assertEqual(connection.commits, 1)

        conflict_connection = ScriptedConnection(
            [
                {
                    "contains": "FROM nccl_raw.outbox_receipt",
                    "one": (
                        "INGESTED",
                        "b" * 64,
                        batch.test_run.run_id,
                        profile.profile_id,
                        "sha256:" + "b" * 64,
                    ),
                },
            ]
        )
        repository = NcclEvaluationRepository(
            Pool(conflict_connection), NcclEvaluationConfig()
        )
        with patch.object(
            repository,
            "_ingest_batch",
            return_value={"run_id": str(batch.test_run.run_id)},
        ), self.assertRaises(RawConflictError):
            repository.ingest_outbox_batch(
                batch,
                outbox_name="node-a.json",
                content_sha256="a" * 64,
                observed_fingerprint="sha256:" + "b" * 64,
            )
        self.assertEqual(conflict_connection.rollbacks, 1)

    def test_exact_outbox_replay_performs_zero_raw_or_profile_updates(self) -> None:
        from cval.nccl_eval.profile import build_profile_identity

        batch = IngestionBatch(run(), (node(),))
        profile = build_profile_identity(batch.test_run)
        fingerprint = "sha256:" + "b" * 64
        connection = ScriptedConnection(
            [
                {
                    "contains": "FROM nccl_raw.outbox_receipt",
                    "one": (
                        "INGESTED",
                        "a" * 64,
                        batch.test_run.run_id,
                        profile.profile_id,
                        fingerprint,
                    ),
                }
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())
        with patch.object(repository, "_ingest_batch") as ingest_batch:
            receipt = repository.ingest_outbox_batch(
                batch,
                outbox_name="node-a.json",
                content_sha256="a" * 64,
                observed_fingerprint=fingerprint,
            )

        ingest_batch.assert_not_called()
        self.assertFalse(receipt["receipt_created"])
        self.assertEqual(len(connection.executed), 1)

    def test_missing_evaluation_job_return_rolls_back_raw_node(self) -> None:
        from cval.nccl_eval.profile import build_profile_identity

        profile = build_profile_identity(run())
        connection = ScriptedConnection(
            [
                {"contains": "INSERT INTO nccl_raw.test_run", "one": (run().run_id,)},
                {"contains": "INSERT INTO nccl_baseline.baseline_profile"},
                {
                    "contains": "FROM nccl_baseline.baseline_profile WHERE profile_key",
                    "one": (
                        profile.profile_id,
                        profile.test_name,
                        profile.test_definition_version,
                        profile.gpu_model,
                        profile.gpus_per_node,
                        profile.cuda_version,
                        profile.pytorch_version,
                        profile.compiled_nccl_version,
                        profile.runtime_nccl_package_version,
                        profile.driver_version_group,
                        profile.topology_class,
                        profile.source_commit,
                        profile.image_digest,
                        profile.implementation_identity,
                        profile.test_config_fingerprint,
                        dict(profile.test_config),
                    ),
                },
                {"contains": "FOR KEY SHARE", "one": (None,)},
                {"contains": "INSERT INTO nccl_raw.node_result", "one": (101,)},
                {"contains": "INSERT INTO nccl_validation.evaluation_job", "one": None},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        with self.assertRaisesRegex(RuntimeError, "job insertion"):
            repository.ingest_batch(IngestionBatch(run(), (node(),)))
        self.assertEqual(connection.rollbacks, 1)

    def test_stale_receipt_with_same_worker_cannot_load_or_retry(self) -> None:
        stale = claim(attempt_count=1)
        load_connection = ScriptedConnection(
            [{"contains": "claim_token", "one": None}]
        )
        repository = NcclEvaluationRepository(
            Pool(load_connection), NcclEvaluationConfig()
        )
        with self.assertRaisesRegex(RuntimeError, "stale or invalid"):
            repository._load_evaluation_work(stale)

        retry_connection = ScriptedConnection(
            [{"contains": "SELECT attempt_count", "one": None}]
        )
        repository = NcclEvaluationRepository(
            Pool(retry_connection), NcclEvaluationConfig()
        )
        with self.assertRaisesRegex(RuntimeError, "not claimed"):
            repository.schedule_retry(stale, RuntimeError("late"))

    def test_default_worker_id_contains_host_identity_and_random_suffix(self) -> None:
        first = default_worker_id()
        second = default_worker_id()
        self.assertNotEqual(first, second)
        self.assertRegex(
            first,
            r"^cval-nccl-[A-Za-z0-9._-]+-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        )
        self.assertLessEqual(len(first), 64)

    def test_raw_retry_mismatch_is_rejected_without_overwrite(self) -> None:
        current = run()
        mismatched_existing = (
            current.test_name,
            current.test_definition_version,
            current.started_at,
            current.completed_at,
            current.image_name,
            current.image_digest,
            current.cuda_version,
            current.pytorch_version,
            "DIFFERENT-NCCL",
            current.runtime_nccl_package_version,
            current.driver_version,
            current.driver_version_group,
            current.topology_class,
            current.gpu_model,
            current.gpus_per_node,
            current.iterations,
            current.samples,
            "sha256:" + "0" * 64,
            dict(current.test_config),
            current.cval_run_id,
            current.cval_result_digest,
            current.summary_sha256,
            current.runtime_evidence_sha256,
            current.source_commit,
            current.implementation_identity,
            current.legacy_source,
        )
        connection = ScriptedConnection(
            [
                {"contains": "INSERT INTO nccl_raw.test_run", "one": None},
                {"contains": "FROM nccl_raw.test_run WHERE run_id", "one": mismatched_existing},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        with self.assertRaises(RawConflictError):
            repository.ingest_batch(IngestionBatch(current, (node(),)))

        self.assertEqual(connection.rollbacks, 1)
        self.assertTrue(all(" DO UPDATE " not in sql for sql, _ in connection.executed))

    def test_exact_raw_retry_is_idempotent_and_creates_no_duplicate_job(self) -> None:
        from cval.nccl_eval.profile import build_profile_identity

        current = run()
        profile = build_profile_identity(current)
        existing_run = (
            current.test_name,
            current.test_definition_version,
            current.started_at,
            current.completed_at,
            current.image_name,
            current.image_digest,
            current.cuda_version,
            current.pytorch_version,
            current.compiled_nccl_version,
            current.runtime_nccl_package_version,
            current.driver_version,
            current.driver_version_group,
            current.topology_class,
            current.gpu_model,
            current.gpus_per_node,
            current.iterations,
            current.samples,
            profile.test_config_fingerprint,
            dict(current.test_config),
            current.cval_run_id,
            current.cval_result_digest,
            current.summary_sha256,
            current.runtime_evidence_sha256,
            current.source_commit,
            current.implementation_identity,
            current.legacy_source,
        )
        current_node = node()
        connection = ScriptedConnection(
            [
                {"contains": "INSERT INTO nccl_raw.test_run", "one": None},
                {"contains": "FROM nccl_raw.test_run WHERE run_id", "one": existing_run},
                {"contains": "INSERT INTO nccl_baseline.baseline_profile"},
                {
                    "contains": "FROM nccl_baseline.baseline_profile WHERE profile_key",
                    "one": (
                        profile.profile_id,
                        profile.test_name,
                        profile.test_definition_version,
                        profile.gpu_model,
                        profile.gpus_per_node,
                        profile.cuda_version,
                        profile.pytorch_version,
                        profile.compiled_nccl_version,
                        profile.runtime_nccl_package_version,
                        profile.driver_version_group,
                        profile.topology_class,
                        profile.source_commit,
                        profile.image_digest,
                        profile.implementation_identity,
                        profile.test_config_fingerprint,
                        dict(profile.test_config),
                    ),
                },
                {"contains": "FOR KEY SHARE", "one": (None,)},
                {"contains": "INSERT INTO nccl_raw.node_result", "one": None},
                {
                    "contains": "FROM nccl_raw.node_result",
                    "one": (
                        101,
                        current_node.test_timestamp,
                        current_node.la_timestamp,
                        current_node.bus_bw_gbps,
                        current_node.latency_us,
                        current_node.result_status.value,
                        current_node.error_code,
                        current_node.error_message,
                    ),
                },
                {"contains": "FROM nccl_raw.nic_result", "all": []},
                {
                    "contains": "FROM nccl_validation.evaluation_job",
                    "one": (profile.profile_id,),
                },
                {"contains": "UPDATE nccl_baseline.baseline_profile"},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        receipt = repository.ingest_batch(IngestionBatch(current, (current_node,)))

        self.assertEqual(receipt["inserted_node_count"], 0)
        self.assertEqual(receipt["idempotent_node_count"], 1)
        self.assertEqual(receipt["result_ids"], [101])
        self.assertFalse(any("INSERT INTO nccl_validation.evaluation_job" in sql for sql, _ in connection.executed))

    def test_refinement_build_records_full_lineage_and_supersedes_without_reactivation(self) -> None:
        profile_id = UUID("33333333-3333-4333-8333-333333333333")
        old_version = UUID("44444444-4444-4444-8444-444444444444")
        candidates = [
            (index, "SUCCESS", 40.0 + index / 10, 600.0 + index, None, "APPROVE")
            for index in range(1, 51)
        ]
        connection = ScriptedConnection(
            [
                {"contains": "pg_advisory_xact_lock"},
                {
                    "contains": "FROM nccl_baseline.baseline_profile",
                    "one": ("profile-key", "ACTIVE", old_version, 40, old_version),
                },
                {"contains": "FROM nccl_validation.evaluation_job", "all": candidates},
                {"contains": "UPDATE nccl_baseline.baseline_profile"},
                {"contains": "COALESCE(max(version_number), 0) + 1", "one": (2,)},
                {"contains": "INSERT INTO nccl_baseline.baseline_version"},
                {"contains": "SET status = 'SUPERSEDED'", "rowcount": 1},
                {"contains": "SET status = 'ACTIVE'", "rowcount": 1},
                {
                    "contains": "SET status = 'ACTIVE', active_baseline_version_id",
                    "rowcount": 1,
                },
                {"contains": "status = 'WAITING_FOR_BASELINE'", "rowcount": 7},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        receipt = repository._build_profile(profile_id)

        self.assertTrue(receipt["built"])
        self.assertEqual(receipt["version_number"], 2)
        self.assertEqual(receipt["sample_count"], 50)
        self.assertEqual(receipt["superseded_version_id"], str(old_version))
        self.assertEqual(receipt["waiting_jobs_requeued"], 7)
        lineage = connection.executemany_calls[0][1]
        thresholds = connection.executemany_calls[1][1]
        self.assertEqual(len(lineage), 50)
        self.assertTrue(all(row[2] and row[3] is None for row in lineage))
        self.assertEqual(len(thresholds), 10)
        self.assertTrue(any("pg_advisory_xact_lock" in sql for sql, _ in connection.executed))

    def test_failed_baseline_replacement_builds_at_40_and_links_failed_version(self) -> None:
        profile_id = UUID("33333333-3333-4333-8333-333333333333")
        failed_version = UUID("44444444-4444-4444-8444-444444444444")
        candidates = [
            (index, "SUCCESS", 40.0 + index / 10, 600.0 + index, None, "APPROVE")
            for index in range(1, 41)
        ]
        connection = ScriptedConnection(
            [
                {"contains": "pg_advisory_xact_lock"},
                {
                    "contains": "FROM nccl_baseline.baseline_profile",
                    "one": ("profile-key", "COLLECTING", None, 40, failed_version),
                },
                {"contains": "FROM nccl_validation.evaluation_job", "all": candidates},
                {"contains": "UPDATE nccl_baseline.baseline_profile"},
                {"contains": "COALESCE(max(version_number), 0) + 1", "one": (2,)},
                {"contains": "INSERT INTO nccl_baseline.baseline_version"},
                {"contains": "SET status = 'ACTIVE'", "rowcount": 1},
                {
                    "contains": "SET status = 'ACTIVE', active_baseline_version_id",
                    "rowcount": 1,
                },
                {"contains": "status = 'WAITING_FOR_BASELINE'", "rowcount": 40},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        receipt = repository._build_profile(profile_id)

        self.assertTrue(receipt["built"])
        self.assertEqual(receipt["version_number"], 2)
        self.assertEqual(receipt["sample_count"], 40)
        self.assertEqual(receipt["superseded_version_id"], str(failed_version))
        insert = next(
            params
            for sql, params in connection.executed
            if "INSERT INTO nccl_baseline.baseline_version" in sql
        )
        self.assertEqual(insert[4], failed_version)
        self.assertFalse(
            any("SET status = 'SUPERSEDED'" in sql for sql, _ in connection.executed)
        )

    def test_missing_baseline_transitions_claim_to_waiting_without_evaluation(self) -> None:
        connection = ScriptedConnection(
            [
                {
                    "contains": "FOR UPDATE OF job",
                    "one": ("SUCCESS", 44.0, 600.0, None),
                },
                {"contains": "SET status = 'WAITING_FOR_BASELINE'", "rowcount": 1},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        result = repository._load_evaluation_work(claim())

        self.assertEqual(result, {"result_id": 1, "job_status": "WAITING_FOR_BASELINE"})
        self.assertEqual(connection.commits, 1)
        self.assertFalse(any("INSERT INTO nccl_validation.evaluation" in sql for sql, _ in connection.executed))

    def test_raw_failure_becomes_failed_without_synthetic_class_five(self) -> None:
        connection = ScriptedConnection(
            [
                {
                    "contains": "FOR UPDATE OF job",
                    "one": ("TIMEOUT", None, None, None),
                },
                {"contains": "SET status = 'FAILED'", "rowcount": 1},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        result = repository._load_evaluation_work(claim())

        self.assertEqual(result["job_status"], "FAILED")
        self.assertEqual(result["failure_code"], "RAW_RESULT_TIMEOUT")
        self.assertFalse(any("INSERT INTO nccl_validation.evaluation" in sql for sql, _ in connection.executed))

    def test_evaluation_insert_and_completion_share_one_transaction(self) -> None:
        bus = derive_thresholds(MetricName.BUS_BW, range(40, 80), derivation_method_version="v1")
        latency = derive_thresholds(MetricName.LATENCY, range(500, 540), derivation_method_version="v1")
        work = EvaluationWork(
            result_id=1,
            baseline_version_id=UUID("22222222-2222-4222-8222-222222222222"),
            bus_bw_gbps=60.0,
            latency_us=520.0,
            scope=__import__("cval.nccl_eval.models", fromlist=["EvaluationScope"]).EvaluationScope.OUT_OF_SAMPLE,
            bus_summary=bus.summary,
            latency_summary=latency.summary,
            bus_ranges=bus.ranges,
            latency_ranges=latency.ranges,
        )
        connection = ScriptedConnection(
            [
                {"contains": "INSERT INTO nccl_validation.evaluation", "one": (99,)},
                {"contains": "SET status = 'COMPLETED'", "rowcount": 1},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())

        with patch.object(repository, "_load_evaluation_work", return_value=work):
            result = repository.evaluate_claimed(claim())

        self.assertEqual(result["job_status"], "COMPLETED")
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)

    def test_stale_receipt_cannot_complete_and_evaluation_insert_rolls_back(self) -> None:
        bus = derive_thresholds(
            MetricName.BUS_BW, range(40, 80), derivation_method_version="v1"
        )
        latency = derive_thresholds(
            MetricName.LATENCY, range(500, 540), derivation_method_version="v1"
        )
        work = EvaluationWork(
            result_id=1,
            baseline_version_id=UUID("22222222-2222-4222-8222-222222222222"),
            bus_bw_gbps=60.0,
            latency_us=520.0,
            scope=__import__(
                "cval.nccl_eval.models", fromlist=["EvaluationScope"]
            ).EvaluationScope.OUT_OF_SAMPLE,
            bus_summary=bus.summary,
            latency_summary=latency.summary,
            bus_ranges=bus.ranges,
            latency_ranges=latency.ranges,
        )
        connection = ScriptedConnection(
            [
                {"contains": "INSERT INTO nccl_validation.evaluation", "one": (99,)},
                {"contains": "claim_token = %s", "rowcount": 0},
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())
        stale = claim()

        with patch.object(repository, "_load_evaluation_work", return_value=work):
            with self.assertRaisesRegex(RuntimeError, "claim changed"):
                repository.evaluate_claimed(stale)

        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)

    def test_retry_backoff_caps_and_max_attempt_fails(self) -> None:
        retry_connection = ScriptedConnection(
            [
                {"contains": "SELECT attempt_count", "one": (3,)},
                {"contains": "UPDATE nccl_validation.evaluation_job", "rowcount": 1},
            ]
        )
        config = NcclEvaluationConfig(
            evaluator_max_attempts=4,
            evaluator_retry_base_seconds=2.0,
            evaluator_retry_max_seconds=5.0,
        )
        repository = NcclEvaluationRepository(Pool(retry_connection), config)
        receipt = repository.schedule_retry(
            claim(attempt_count=3), RuntimeError("temporary")
        )
        self.assertEqual(receipt["job_status"], "RETRY")
        self.assertEqual(receipt["retry_delay_seconds"], 5.0)

        failed_connection = ScriptedConnection(
            [
                {"contains": "SELECT attempt_count", "one": (4,)},
                {"contains": "UPDATE nccl_validation.evaluation_job", "rowcount": 1},
            ]
        )
        repository = NcclEvaluationRepository(Pool(failed_connection), config)
        receipt = repository.schedule_retry(
            claim(attempt_count=4), RuntimeError("temporary")
        )
        self.assertEqual(receipt["job_status"], "FAILED")

    def test_stale_recovery_sql_retries_or_fails_at_attempt_limit(self) -> None:
        connection = ScriptedConnection(
            [
                {
                    "contains": "status = CASE WHEN attempt_count >=",
                    "all": [(1, "RETRY", 2), (2, "FAILED", 8)],
                }
            ]
        )
        repository = NcclEvaluationRepository(Pool(connection), NcclEvaluationConfig())
        receipt = repository.recover_stale_claims()
        self.assertEqual(receipt["recovered_count"], 2)
        self.assertEqual([item["job_status"] for item in receipt["jobs"]], ["RETRY", "FAILED"])

    def test_pool_is_separate_explicitly_opened_waited_and_bounded(self) -> None:
        instances = []

        class FakeConnectionPool:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.opened = False
                self.waited = False
                instances.append(self)

            def open(self):
                self.opened = True

            def wait(self, *, timeout):
                self.waited = timeout

            def close(self):
                pass

        module = SimpleNamespace(ConnectionPool=FakeConnectionPool)
        config = NcclEvaluationConfig(
            database_url="postgresql://user:password@example/cval",
            pool_min_size=2,
            pool_max_size=5,
            pool_startup_timeout_seconds=7.0,
        )
        with patch.dict(sys.modules, {"psycopg_pool": module}):
            pool = create_pool(config)

        self.assertIs(pool, instances[0])
        self.assertFalse(instances[0].kwargs["open"])
        self.assertEqual(instances[0].kwargs["min_size"], 2)
        self.assertEqual(instances[0].kwargs["max_size"], 5)
        self.assertTrue(instances[0].opened)
        self.assertEqual(instances[0].waited, 7.0)


if __name__ == "__main__":
    unittest.main()
