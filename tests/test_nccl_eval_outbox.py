from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from cval.config import load_config
from cval.nccl_eval.outbox import (
    OUTBOX_FILE_MODE,
    OutboxIngestionError,
    build_ingestion_batch,
    commit_outbox,
    commit_outbox_plan,
    emit_outbox,
    ingest_outbox_progression,
    ingest_scanned_outbox,
    scan_outbox,
)
import cval.nccl_eval.outbox as outbox_module
from cval.nccl_eval.runtime_evidence import RuntimeEvidence, write_runtime_evidence
from cval.validation.registry import validation_test_config_digest
from cval.validation.runtime import effective_config_digest
from cval.validation.results import load_validation_result, validation_result_v2_digest


UTC = timezone.utc


class OutboxTests(unittest.TestCase):
    def config(self, root: Path):
        base = load_config()
        return replace(base, runtime=replace(base.runtime, validation_root=str(root)))

    def evidence(self, path: Path) -> None:
        write_runtime_evidence(
            path,
            RuntimeEvidence(
                schema_version="cval.nccl-runtime-evidence.v1",
                gpu_model="NVIDIA B200",
                compiled_nccl_version="2.27.7",
                runtime_nccl_package_version="nvidia-nccl-cu13==2.27.7",
                driver_version="600.12",
                driver_version_group="600.12",
                topology_class="nvidia-topo-sha256:" + "a" * 64,
            ),
        )

    def write_run(self, root: Path, *, status: str = "pass", phase: str = "finished"):
        config = self.config(root)
        registered = config.tests.registry.require("nccl")
        run_id = "node-a-123"
        run_dir = root / "logs/job_logs/node-a" / run_id
        nccl_dir = root / "validation_tests/nccl/runs/node-a" / run_id
        artifacts = nccl_dir / "artifacts"
        run_dir.mkdir(parents=True)
        artifacts.mkdir(parents=True)
        summary = nccl_dir / "summary.json"
        evidence = artifacts / "runtime-evidence.json"
        self.evidence(evidence)
        if status == "pass":
            summary.write_text(
                json.dumps(
                    {
                        "GCR_ITERATIONS": 20,
                        "GCR_DATA_SIZE_GB": 8,
                        "GCR_LATENCY": 1.25,
                        "GCR_ALGBW": 10.0,
                        "GCR_BUSBW": 20.0,
                        "GCR_IB_PORT_BW_GBPS": {
                            "mlx5_0": {"avg_gbps": 9.0, "max_gbps": 11.0, "last_gbps": 10.0, "samples": 3},
                            "mlx5_1.2": {"avg_gbps": 7.0, "max_gbps": 8.0, "last_gbps": 6.0, "samples": 3},
                        },
                    }
                ),
                encoding="utf-8",
            )
        started = "2026-01-01T00:00:00+00:00"
        completed = "2026-01-01T00:01:00+00:00"
        exit_code = 0 if status == "pass" else 7
        payload = {
            "schema_version": "cval.results",
            "run_id": run_id,
            "node": "node-a",
            "timestamp": 123,
            "timestamp_la": datetime.fromtimestamp(123, UTC).isoformat(),
            "generated_at": started,
            "completed_at": completed,
            "overall": status,
            "image_name": "example/image@sha256:" + "b" * 64,
            "pytorch_version": "2.12",
            "cuda_version": "13.2",
            "git_ref": "a" * 40,
            "global_config_digest": effective_config_digest(config),
            "tests": {
                "nccl": {
                    "display_name": "NCCL",
                    "enabled": True,
                    "selected": True,
                    "order": 20,
                    "status": status,
                    "phase": phase,
                    "started_at": started,
                    "completed_at": completed,
                    "duration_ms": 60000,
                    "exit_code": exit_code,
                    "config_path": registered.config_path,
                    "config_digest": validation_test_config_digest(registered),
                    "stdout": str(root / "logs/nccl/node-a/node-a-123/stdout.log"),
                    "stderr": str(root / "logs/nccl/node-a/node-a-123/stderr.log"),
                    "log": str(root / "logs/nccl/node-a/node-a-123/events.jsonl"),
                    "summary": str(summary),
                    "result": str(nccl_dir / "result.json"),
                    "artifacts": str(artifacts),
                    "message": "" if status == "pass" else "collective failed",
                }
            },
            "errors": [],
        }
        result = run_dir / "result.json"
        result.write_text(json.dumps(payload), encoding="utf-8")
        return config, result, summary, evidence

    def batch(self, config, result, summary, evidence):
        loaded = load_validation_result(result)
        return build_ingestion_batch(
            result_json=result,
            summary=summary,
            runtime_evidence=evidence,
            result_digest=validation_result_v2_digest(loaded),
            config=config,
        )

    def test_pass_batch_converts_latency_and_normalizes_nic_maxima(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(root)
            batch = self.batch(config, result, summary, evidence)

        run = batch.test_run
        node = batch.node_results[0]
        self.assertEqual(node.bus_bw_gbps, 20.0)
        self.assertEqual(node.latency_us, 1250.0)
        self.assertEqual([(nic.device_name, nic.max_bus_bw_gbps) for nic in node.nics], [("mlx5_0", 11.0), ("mlx5_1.2", 8.0)])
        self.assertEqual(run.samples, 1)
        self.assertEqual(run.test_config["message_size_bytes"], 17179869184)
        self.assertEqual(run.test_config["latency_unit"], "us")
        self.assertEqual(run.test_config["iteration_semantics"], "timed_collective_repetitions")
        self.assertEqual(run.cval_run_id, "node-a-123")
        self.assertRegex(run.summary_sha256, r"^sha256:[0-9a-f]{64}$")

    def test_native_batch_rejects_wrong_result_digest_moving_commit_and_image(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(root)
            with self.assertRaisesRegex(ValueError, "result digest"):
                build_ingestion_batch(
                    result_json=result,
                    summary=summary,
                    runtime_evidence=evidence,
                    result_digest="sha256:" + "0" * 64,
                    config=config,
                )
            payload = json.loads(result.read_text(encoding="utf-8"))
            payload["git_ref"] = "main"
            result.write_text(json.dumps(payload), encoding="utf-8")
            loaded = load_validation_result(result)
            with self.assertRaisesRegex(ValueError, "source_commit"):
                build_ingestion_batch(
                    result_json=result,
                    summary=summary,
                    runtime_evidence=evidence,
                    result_digest=validation_result_v2_digest(loaded),
                    config=config,
                )

            payload["git_ref"] = "a" * 40
            payload["image_name"] = "example/image:latest"
            result.write_text(json.dumps(payload), encoding="utf-8")
            loaded = load_validation_result(result)
            with self.assertRaisesRegex(ValueError, "immutable image"):
                build_ingestion_batch(
                    result_json=result,
                    summary=summary,
                    runtime_evidence=evidence,
                    result_digest=validation_result_v2_digest(loaded),
                    config=config,
                )

    def test_failed_selected_test_emits_null_metrics_with_exact_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(root, status="fail")
            batch = self.batch(config, result, summary, evidence)

        node = batch.node_results[0]
        self.assertEqual(node.result_status.value, "TEST_ERROR")
        self.assertEqual(node.error_code, "CVAL_NCCL_FINISHED_EXIT_7")
        self.assertIsNone(node.bus_bw_gbps)
        self.assertIsNone(node.latency_us)

    def test_timed_out_selected_test_uses_timeout_without_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(
                root, status="fail", phase="timed_out"
            )
            batch = self.batch(config, result, summary, evidence)
            outbox = root / "nccl_eval/outbox"
            emit_outbox(outbox, "node-a-123", batch)
            commit_outbox(
                outbox,
                pending=outbox / "pending/node-a-123.json",
                result_digest=batch.test_run.cval_result_digest,
            )
            scanned = scan_outbox(outbox, limit=1)

        node = batch.node_results[0]
        self.assertEqual(node.result_status.value, "TIMEOUT")
        self.assertEqual(node.error_code, "CVAL_NCCL_TIMED_OUT")
        self.assertIsNone(node.bus_bw_gbps)
        self.assertIsNone(node.latency_us)
        self.assertEqual(scanned.files[0].batch.node_results[0].result_status.value, "TIMEOUT")
        from cval.nccl_eval.repository import assess_eligibility

        eligibility = assess_eligibility(
            1,
            result_status=node.result_status.value,
            bus_bw_gbps=node.bus_bw_gbps,
            latency_us=node.latency_us,
            error_code=node.error_code,
            effective_calibration_action="APPROVE",
        )
        self.assertFalse(eligibility.included)
        self.assertEqual(eligibility.exclusion_reason, "RESULT_STATUS_TIMEOUT")

    def test_emit_is_no_replace_byte_equal_and_scanner_is_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(root)
            batch = self.batch(config, result, summary, evidence)
            outbox = root / "nccl_eval/outbox"
            first = emit_outbox(outbox, "node-a-123", batch)
            second = emit_outbox(outbox, "node-a-123", batch)
            self.assertTrue(first["created"])
            self.assertFalse(second["created"])
            path = outbox / "pending/node-a-123.json"
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), OUTBOX_FILE_MODE)
            self.assertEqual(scan_outbox(outbox, limit=2).files, ())
            first_commit = commit_outbox(
                outbox,
                pending=path,
                result_digest=batch.test_run.cval_result_digest,
            )
            second_commit = commit_outbox(
                outbox,
                pending=path,
                result_digest=batch.test_run.cval_result_digest,
            )
            self.assertTrue(first_commit["created"])
            self.assertFalse(second_commit["created"])

            copied = outbox / "pending/a-run.json"
            copied.write_bytes(path.read_bytes())
            copied.chmod(OUTBOX_FILE_MODE)
            raw = json.loads(copied.read_text())
            raw["test_run"]["cval_run_id"] = "a-run"
            copied.write_text(json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n")
            copied.chmod(OUTBOX_FILE_MODE)
            # This copied payload deliberately lacks a valid marker binding.
            scan = scan_outbox(outbox, limit=2)

        self.assertEqual([item.name for item in scan.files], ["node-a-123.json"])
        self.assertEqual(len(scan.public_dict()["profile_ids"]), 1)

    def test_scanner_rejects_symlink_bad_mode_and_nonrecursive_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            root.chmod(0o755)
            (root / "pending").mkdir()
            (root / "committed").mkdir()
            external = Path(tmpdir).parent / f"{root.name}-external.json"
            external.write_text("{}", encoding="utf-8")
            try:
                (root / "committed/link.json").symlink_to(external)
                bad = root / "committed/bad.json"
                bad.write_text("{}", encoding="utf-8")
                bad.chmod(0o600)
                nested = root / "nested"
                nested.mkdir()
                (nested / "ignored.json").write_text("{}", encoding="utf-8")
                scan = scan_outbox(root, limit=5000)
            finally:
                external.unlink(missing_ok=True)

        self.assertEqual(scan.discovered_json_count, 2)
        self.assertEqual(scan.public_dict()["invalid_count"], 2)
        self.assertFalse(any(item.get("file") == "ignored.json" for item in scan.invalid))

    def test_descriptor_read_rejects_replace_rename_and_same_size_byte_races(self) -> None:
        original_reader = outbox_module._read_fd_exact
        for operation in ("replace", "rename", "bytes"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "value.json"
                path.write_bytes(b'{"a":1}\n')

                def race(descriptor, size):
                    payload = original_reader(descriptor, size)
                    if operation == "replace":
                        replacement = path.with_name("replacement.json")
                        replacement.write_bytes(b'{"b":2}\n')
                        replacement.replace(path)
                    elif operation == "rename":
                        path.rename(path.with_name("renamed.json"))
                    else:
                        with path.open("r+b") as handle:
                            handle.write(b'{"c":3}\n')
                            handle.flush()
                            os.fsync(handle.fileno())
                    return payload

                with patch(
                    "cval.nccl_eval.outbox._read_fd_exact", side_effect=race
                ), self.assertRaises(OSError):
                    outbox_module._read_stable_regular_file(
                        path, maximum_bytes=1024
                    )

    def test_apply_stops_after_first_error_and_keeps_prior_receipt(self) -> None:
        class Repository:
            def __init__(self):
                self.calls = 0

            def ingest_outbox_batch(self, batch, **kwargs):
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("second failed")
                return {"receipt_created": True}

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(root)
            batch = self.batch(config, result, summary, evidence)
            outbox = root / "nccl_eval/outbox"
            emit_outbox(outbox, "node-a-123", batch)
            pending = outbox / "pending/node-a-123.json"
            commit_outbox(
                outbox,
                pending=pending,
                result_digest=batch.test_run.cval_result_digest,
            )
            second_batch = replace(
                batch,
                test_run=replace(
                    batch.test_run,
                    run_id=uuid4(),
                    cval_run_id="node-b-124",
                ),
            )
            emit_outbox(outbox, "node-b-124", second_batch)
            commit_outbox(
                outbox,
                pending=outbox / "pending/node-b-124.json",
                result_digest=second_batch.test_run.cval_result_digest,
            )
            scan = scan_outbox(outbox, limit=2)
            repository = Repository()

            class Context:
                def __enter__(self):
                    return repository

                def __exit__(self, *args):
                    return False

            from cval.nccl_eval.config import NcclEvaluationConfig

            with patch("cval.nccl_eval.service.open_repository", return_value=Context()):
                with self.assertRaises(OutboxIngestionError) as caught:
                    ingest_scanned_outbox(NcclEvaluationConfig(), scan)

            self.assertEqual(caught.exception.receipt["ingested_count"], 1)
            self.assertEqual(caught.exception.receipt["attempted_count"], 2)
            self.assertEqual(repository.calls, 2)

    def test_progression_skips_first_5000_terminal_and_reaches_5001(self) -> None:
            class Repository:
                def __init__(self):
                    self.cursor = None
                    self.ingested = []

                def get_outbox_cursor(self):
                    return self.cursor

                def outbox_terminal_states(self, names):
                    return {name: "INGESTED" for name in names if name.startswith("a")}

                def set_outbox_cursor(self, name):
                    self.cursor = name

                def ingest_outbox_batch(self, batch, **kwargs):
                    self.ingested.append(kwargs["outbox_name"])
                    return {"receipt_created": True}

            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config, result, summary, evidence = self.write_run(root)
                batch = self.batch(config, result, summary, evidence)
                outbox = root / "nccl_eval/outbox"
                emit_outbox(outbox, "node-a-123", batch)
                commit_outbox(
                    outbox,
                    pending=outbox / "pending/node-a-123.json",
                    result_digest=batch.test_run.cval_result_digest,
                )
                for index in range(5000):
                    marker = outbox / "committed" / f"a{index:04d}.json"
                    marker.write_text("{}\n", encoding="utf-8")
                    marker.chmod(OUTBOX_FILE_MODE)
                repository = Repository()

                class Context:
                    def __enter__(self):
                        return repository

                    def __exit__(self, *args):
                        return False

                from cval.nccl_eval.config import NcclEvaluationConfig

                with patch("cval.nccl_eval.service.open_repository", return_value=Context()):
                    receipt = ingest_outbox_progression(
                        NcclEvaluationConfig(), outbox, limit=1
                    )

            self.assertEqual(receipt["skipped_terminal_count"], 5000)
            self.assertEqual(repository.ingested, ["node-a-123.json"])
            self.assertEqual(repository.cursor, "node-a-123.json")

    def test_progression_wraps_to_new_lexically_earlier_marker(self) -> None:
            class Repository:
                cursor = "z-last.json"

                def get_outbox_cursor(self):
                    return self.cursor

                def outbox_terminal_states(self, names):
                    return {}

                def set_outbox_cursor(self, name):
                    self.cursor = name

                def ingest_outbox_batch(self, batch, **kwargs):
                    return {"receipt_created": True, "file": kwargs["outbox_name"]}

            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config, result, summary, evidence = self.write_run(root)
                batch = self.batch(config, result, summary, evidence)
                outbox = root / "nccl_eval/outbox"
                emit_outbox(outbox, "node-a-123", batch)
                commit_outbox(
                    outbox,
                    pending=outbox / "pending/node-a-123.json",
                    result_digest=batch.test_run.cval_result_digest,
                )
                repository = Repository()

                class Context:
                    def __enter__(self): return repository
                    def __exit__(self, *args): return False

                from cval.nccl_eval.config import NcclEvaluationConfig
                with patch("cval.nccl_eval.service.open_repository", return_value=Context()):
                    receipt = ingest_outbox_progression(NcclEvaluationConfig(), outbox, limit=1)

            self.assertEqual(receipt["ingested_count"], 1)
            self.assertEqual(repository.cursor, "node-a-123.json")

    def test_fifo_is_rejected_without_blocking_then_valid_marker_progresses(self) -> None:
            class Repository:
                def __init__(self): self.cursor = None
                def get_outbox_cursor(self): return self.cursor
                def outbox_terminal_states(self, names): return {}
                def set_outbox_cursor(self, name): self.cursor = name
                def record_outbox_rejection(self, **kwargs):
                    return {"outbox_name": kwargs["outbox_name"], "status": "REJECTED"}
                def ingest_outbox_batch(self, batch, **kwargs): return {"receipt_created": True}

            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config, result, summary, evidence = self.write_run(root)
                batch = self.batch(config, result, summary, evidence)
                outbox = root / "nccl_eval/outbox"
                emit_outbox(outbox, "node-a-123", batch)
                commit_outbox(outbox, pending=outbox / "pending/node-a-123.json", result_digest=batch.test_run.cval_result_digest)
                fifo = outbox / "committed/a-fifo.json"
                os.mkfifo(fifo, OUTBOX_FILE_MODE)
                repository = Repository()

                class Context:
                    def __enter__(self): return repository
                    def __exit__(self, *args): return False

                from cval.nccl_eval.config import NcclEvaluationConfig
                with patch("cval.nccl_eval.service.open_repository", return_value=Context()):
                    receipt = ingest_outbox_progression(NcclEvaluationConfig(), outbox, limit=2)

            self.assertEqual(receipt["rejected_count"], 1)
            self.assertEqual(receipt["ingested_count"], 1)
            self.assertEqual(repository.cursor, "node-a-123.json")

    def test_database_failure_never_advances_cursor_past_valid_file(self) -> None:
            class Repository:
                cursor = None
                def get_outbox_cursor(self): return self.cursor
                def outbox_terminal_states(self, names): return {}
                def set_outbox_cursor(self, name): self.cursor = name
                def ingest_outbox_batch(self, batch, **kwargs): raise RuntimeError("database down")

            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config, result, summary, evidence = self.write_run(root)
                batch = self.batch(config, result, summary, evidence)
                outbox = root / "nccl_eval/outbox"
                emit_outbox(outbox, "node-a-123", batch)
                commit_outbox(outbox, pending=outbox / "pending/node-a-123.json", result_digest=batch.test_run.cval_result_digest)
                repository = Repository()

                class Context:
                    def __enter__(self): return repository
                    def __exit__(self, *args): return False

                from cval.nccl_eval.config import NcclEvaluationConfig
                with patch("cval.nccl_eval.service.open_repository", return_value=Context()):
                    with self.assertRaisesRegex(RuntimeError, "database down"):
                        ingest_outbox_progression(NcclEvaluationConfig(), outbox, limit=1)

            self.assertIsNone(repository.cursor)

    def test_crash_after_durable_receipt_replays_without_duplicate_ingestion(self) -> None:
            class Repository:
                def __init__(self):
                    self.cursor = None
                    self.terminal = {}
                    self.ingest_calls = 0
                    self.crash_once = True

                def get_outbox_cursor(self):
                    return self.cursor

                def outbox_terminal_states(self, names):
                    return {name: self.terminal[name] for name in names if name in self.terminal}

                def set_outbox_cursor(self, name):
                    if self.crash_once:
                        self.crash_once = False
                        raise RuntimeError("crash before cursor advancement")
                    self.cursor = name

                def ingest_outbox_batch(self, batch, **kwargs):
                    self.ingest_calls += 1
                    self.terminal[kwargs["outbox_name"]] = "INGESTED"
                    return {"receipt_created": True}

            with tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                config, result, summary, evidence = self.write_run(root)
                batch = self.batch(config, result, summary, evidence)
                outbox = root / "nccl_eval/outbox"
                emit_outbox(outbox, "node-a-123", batch)
                commit_outbox(
                    outbox,
                    pending=outbox / "pending/node-a-123.json",
                    result_digest=batch.test_run.cval_result_digest,
                )
                repository = Repository()

                class Context:
                    def __enter__(self): return repository
                    def __exit__(self, *args): return False

                from cval.nccl_eval.config import NcclEvaluationConfig
                with patch("cval.nccl_eval.service.open_repository", return_value=Context()):
                    with self.assertRaisesRegex(RuntimeError, "cursor advancement"):
                        ingest_outbox_progression(NcclEvaluationConfig(), outbox, limit=1)
                    receipt = ingest_outbox_progression(
                        NcclEvaluationConfig(), outbox, limit=1
                    )

            self.assertEqual(repository.ingest_calls, 1)
            self.assertEqual(receipt["ingested_count"], 0)
            self.assertEqual(receipt["skipped_terminal_count"], 1)
            self.assertEqual(repository.cursor, "node-a-123.json")

    def test_commit_plan_validates_pending_without_creating_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(root)
            batch = self.batch(config, result, summary, evidence)
            outbox = root / "nccl_eval/outbox"
            emit_outbox(outbox, "node-a-123", batch)
            plan = commit_outbox_plan(
                outbox,
                pending=outbox / "pending/node-a-123.json",
                result_digest=batch.test_run.cval_result_digest,
            )
            self.assertTrue(plan["valid"])
            self.assertFalse((outbox / "committed/node-a-123.json").exists())
            with self.assertRaisesRegex(ValueError, "result digest"):
                commit_outbox_plan(
                    outbox,
                    pending=outbox / "pending/node-a-123.json",
                    result_digest="sha256:" + "0" * 64,
                )

    def test_progression_continue_on_database_error_reports_without_cursor_advance(self) -> None:
        class Repository:
            cursor = None
            def get_outbox_cursor(self): return self.cursor
            def outbox_terminal_states(self, names): return {}
            def set_outbox_cursor(self, name): self.cursor = name
            def ingest_outbox_batch(self, batch, **kwargs): raise RuntimeError("database down")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config, result, summary, evidence = self.write_run(root)
            batch = self.batch(config, result, summary, evidence)
            outbox = root / "nccl_eval/outbox"
            emit_outbox(outbox, "node-a-123", batch)
            commit_outbox(
                outbox,
                pending=outbox / "pending/node-a-123.json",
                result_digest=batch.test_run.cval_result_digest,
            )
            repository = Repository()

            class Context:
                def __enter__(self): return repository
                def __exit__(self, *args): return False

            from cval.nccl_eval.config import NcclEvaluationConfig
            with patch("cval.nccl_eval.service.open_repository", return_value=Context()):
                receipt = ingest_outbox_progression(
                    NcclEvaluationConfig(),
                    outbox,
                    limit=1,
                    continue_on_error=True,
                )

        self.assertEqual(receipt["error_count"], 1)
        self.assertEqual(receipt["processed_count"], 1)
        self.assertIsNone(repository.cursor)


if __name__ == "__main__":
    unittest.main()
