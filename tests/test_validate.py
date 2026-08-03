"""Tests for targeted single-node validation orchestration and helpers."""

from __future__ import annotations

import base64
import io
import json
import unittest
import zipfile
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.config import CvalConfig, JobTemplateConfig, load_config
from cval.k8s.client import CommandResult
from cval.k8s.discovery import describe_node_from_outputs
from cval.orchestrator.validate import (
    build_validation_report,
    degraded_metrics_from_verdict,
    finalize_download_zip,
    log_signals_db_updated,
    parse_test_progress,
    raw_results_from_log,
    render_test_progress_line,
    render_validation_report,
    run_node_validation,
)
from cval.policy import PolicyViolation
from cval.validation.registry import ValidationTestRegistry


STORAGE_DONE = "Storage test is complete. Log file: x Summary file: y"
NCCL_DONE = "NCCL test is complete. Log file: x Summary file: y"
FINAL_LINE = "Final c-val test results: storage=pass nccl=pass dltest=fail"


class TestParseProgress(unittest.TestCase):
    def test_individual_markers(self):
        text = "\n".join([STORAGE_DONE, "Running DL Test..."])
        progress = parse_test_progress(text)
        self.assertEqual(progress["storage"], "pass")
        self.assertEqual(progress["dltest"], "running")
        self.assertNotIn("nccl", progress)

    def test_final_line_is_authoritative(self):
        text = "\n".join([STORAGE_DONE, NCCL_DONE, FINAL_LINE])
        progress = parse_test_progress(text)
        self.assertEqual(progress, {"storage": "pass", "nccl": "pass", "dltest": "fail"})

    def test_failed_marker(self):
        progress = parse_test_progress("Storage test FAILED. Log file: x Summary file: y")
        self.assertEqual(progress["storage"], "fail")

    def test_empty_log(self):
        self.assertEqual(parse_test_progress(""), {})

    def test_structured_events_support_dynamic_test_ids(self):
        started = {
            "schema_version": "cval.event.v1",
            "event": "test_started",
            "run_id": "node-a-123",
            "test": "smoke",
            "timestamp": "2026-07-28T16:00:00Z",
            "status": "incomplete",
            "message": "",
        }
        finished = started | {
            "event": "test_finished",
            "timestamp": "2026-07-28T16:00:01Z",
            "status": "pass",
        }
        text = "\n".join(
            f"CVAL_EVENT {json.dumps(event)}" for event in (started, finished)
        )

        self.assertEqual(parse_test_progress(text), {"smoke": "pass"})

    def test_malformed_structured_event_is_ignored(self):
        self.assertEqual(parse_test_progress("CVAL_EVENT {not-json}"), {})

    def test_dynamic_progress_line_uses_registry_order(self):
        line = render_test_progress_line(
            12.0,
            "Running",
            ["storage", "smoke"],
            {"storage": "pass", "smoke": "running"},
        )

        self.assertIn("storage=PASS smoke=RUNNING", line)


class TestRawResults(unittest.TestCase):
    def test_raw_results_from_final_line(self):
        raw = raw_results_from_log(FINAL_LINE)
        self.assertEqual(raw["storage"], "pass")
        self.assertEqual(raw["dltest"], "fail")
        self.assertEqual(raw["all"], "fail")

    def test_all_pass(self):
        raw = raw_results_from_log("Final c-val test results: storage=pass nccl=pass dltest=pass")
        self.assertEqual(raw["all"], "pass")

    def test_disabled_phase_is_ignored_for_aggregate(self):
        raw = raw_results_from_log(
            "Final c-val test results: storage=pass nccl=pass dltest=incomplete",
            enabled_tests={"storage", "nccl"},
        )
        self.assertEqual(raw["all"], "pass")

    def test_missing_line(self):
        self.assertEqual(raw_results_from_log("nothing here"), {})

    def test_raw_results_from_structured_events(self):
        events = [
            {
                "schema_version": "cval.event.v1",
                "event": "test_finished",
                "run_id": "node-a-123",
                "test": "smoke",
                "timestamp": "2026-07-28T16:00:01Z",
                "status": "pass",
                "message": "",
            },
            {
                "schema_version": "cval.event.v1",
                "event": "run_finished",
                "run_id": "node-a-123",
                "test": None,
                "timestamp": "2026-07-28T16:00:01Z",
                "status": "pass",
                "overall": "pass",
                "message": "",
            },
        ]
        text = "\n".join(f"CVAL_EVENT {json.dumps(event)}" for event in events)

        self.assertEqual(
            raw_results_from_log(text, enabled_tests={"smoke"}),
            {"smoke": "pass", "all": "pass"},
        )

    def test_structured_ingestion_failure_overrides_legacy_marker(self):
        event = {
            "schema_version": "cval.event.v1",
            "event": "ingestion_finished",
            "run_id": "node-a-123",
            "test": None,
            "timestamp": "2026-07-28T16:00:01Z",
            "status": "fail",
            "message": "write failed",
        }
        text = "Main DB update completed.\nCVAL_EVENT " + json.dumps(event)

        self.assertFalse(log_signals_db_updated(text))


class TestDegradedMetrics(unittest.TestCase):
    def test_sorted_and_limited(self):
        verdict = {
            "metrics": [
                {"metric": "a", "status": "normal", "pct_diff": 1.0},
                {"metric": "b", "status": "degraded", "pct_diff": 5.0, "component": "compute_performance"},
                {"metric": "c", "status": "degraded", "pct_diff": -20.0, "component": "compute_performance"},
            ]
        }
        degraded = degraded_metrics_from_verdict(verdict, limit=1)
        self.assertEqual(len(degraded), 1)
        self.assertEqual(degraded[0]["metric"], "c")  # |−20| is worst

    def test_none_verdict(self):
        self.assertEqual(degraded_metrics_from_verdict(None), [])


class TestBuildAndRenderReport(unittest.TestCase):
    def _verdicts(self):
        return {
            "storage": {"status": "normal", "n_compared": 12, "n_degraded": 0, "metrics": []},
            "nccl": {"status": "normal", "n_compared": 2, "n_degraded": 0, "metrics": []},
            "dltest": {
                "status": "degraded",
                "n_compared": 2788,
                "n_degraded": 12,
                "degraded_metric_percent": 0.43,
                "worst_pct_diff": 18.5,
                "components": {
                    "compute_performance": {
                        "status": "degraded",
                        "n_degraded": 12,
                        "degraded_metric_percent": 0.9,
                        "worst_pct_diff": 18.5,
                    }
                },
                "metrics": [
                    {
                        "metric": "nn_tasks/instance_norm/elapsed_ms",
                        "component": "compute_performance",
                        "status": "degraded",
                        "pct_diff": 18.5,
                        "direction": "high_bad",
                    }
                ],
            },
        }

    def test_report_health_degraded(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={"fully_free": True, "schedulable": True, "resource_ready": True, "reason": "free"},
            raw_results={"storage": "pass", "nccl": "pass", "dltest": "pass", "all": "pass"},
            verdicts=self._verdicts(),
        )
        self.assertTrue(report["ok"])
        self.assertEqual(report["health"], "degraded")
        self.assertEqual(report["raw_overall"], "pass")
        self.assertEqual(report["classification"]["dltest"]["n_degraded"], 12)
        self.assertEqual(len(report["classification"]["dltest"]["degraded_metrics"]), 1)

    def test_render_contains_sections(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={"fully_free": True, "schedulable": True, "resource_ready": True, "reason": "free"},
            raw_results={"storage": "pass", "nccl": "pass", "dltest": "pass", "all": "pass"},
            verdicts=self._verdicts(),
        )
        text = render_validation_report(report)
        for token in ("validation report", "node-x", "storage", "nccl", "dltest", "DL components", "instance_norm"):
            self.assertIn(token, text)

    def test_completed_job_with_failed_raw_result_is_not_ok(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={},
            raw_results={"storage": "pass", "nccl": "fail", "all": "fail"},
            verdicts={"storage": None, "nccl": None, "dltest": None},
        )

        self.assertFalse(report["ok"])

    def test_completed_job_without_successful_ingestion_is_not_ok(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={},
            raw_results={"storage": "pass", "nccl": "pass", "all": "pass"},
            verdicts={"storage": None, "nccl": None, "dltest": None},
            ingestion_complete=False,
        )

        self.assertFalse(report["ok"])


class TestDescribeNode(unittest.TestCase):
    def _config(self):
        return CvalConfig(
            job_template=JobTemplateConfig(
                cpu="100",
                memory="1500Gi",
                gpu_resource_name="nvidia.com/gpu",
                gpu_count="8",
                rdma_resource_name="rdma/rdma_shared_device_a",
                rdma_count="1",
            )
        )

    def _nodes_json(self, name, *, unschedulable=False, allocatable=None, ready=True):
        spec = {"unschedulable": True} if unschedulable else {}
        return {
            "items": [
                {
                    "metadata": {"name": name},
                    "spec": spec,
                    "status": {
                        "conditions": [
                            {"type": "Ready", "status": "True" if ready else "False"}
                        ],
                        "allocatable": allocatable
                        or {
                            "cpu": "110",
                            "memory": "3036180572Ki",
                            "nvidia.com/gpu": "8",
                            "rdma/rdma_shared_device_a": "63",
                        }
                    },
                }
            ]
        }

    def test_free_node(self):
        status = describe_node_from_outputs(
            "node-x",
            {"items": []},
            "node-x 8 8",
            self._nodes_json("node-x"),
            self._config(),
        )
        self.assertTrue(status.found)
        self.assertTrue(status.fully_free)
        self.assertTrue(status.schedulable)
        self.assertTrue(status.resource_ready)
        self.assertTrue(status.ready)
        self.assertFalse(status.cordoned)
        self.assertEqual(status.status_label, "ready")

    def test_cordoned_node(self):
        status = describe_node_from_outputs(
            "node-x",
            {"items": []},
            "node-x 8 8",
            self._nodes_json("node-x", unschedulable=True),
            self._config(),
        )
        self.assertTrue(status.schedulable)
        self.assertTrue(status.fully_free)
        self.assertTrue(status.cordoned)
        self.assertEqual(status.status_label, "cordoned")

    def test_not_ready_node(self):
        status = describe_node_from_outputs(
            "node-x",
            {"items": []},
            "node-x 8 8",
            self._nodes_json("node-x", ready=False),
            self._config(),
        )
        self.assertFalse(status.ready)
        self.assertEqual(status.status_label, "not_ready")

    def test_resource_blocked_node(self):
        pods = {
            "items": [
                {
                    "spec": {
                        "nodeName": "node-x",
                        "containers": [
                            {"resources": {"requests": {"cpu": "108", "memory": "3000000000Ki"}}}
                        ],
                    },
                    "status": {"phase": "Running"},
                }
            ]
        }
        status = describe_node_from_outputs(
            "node-x", pods, "node-x 8 8", self._nodes_json("node-x"), self._config()
        )
        self.assertFalse(status.resource_ready)
        self.assertFalse(status.fully_free)

    def test_not_found(self):
        status = describe_node_from_outputs(
            "missing", {"items": []}, "node-x 8 8", self._nodes_json("node-x"), self._config()
        )
        self.assertFalse(status.found)


class FakeValidateClient:
    """Scripted kubectl stand-in for the full validate orchestration path."""

    def __init__(self):
        self.calls: list[list[str]] = []
        self._phases = iter(["Running", "Completed"])
        self.pods_json = {"items": []}
        self.nodes_table = "node-x 8 8"
        self.nodes_json = {
            "items": [
                {
                    "metadata": {"name": "node-x"},
                    "spec": {},
                    "status": {
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "allocatable": {
                            "cpu": "110",
                            "memory": "3036180572Ki",
                            "nvidia.com/gpu": "8",
                            "rdma/rdma_shared_device_a": "63",
                        }
                    },
                }
            ]
        }
        self.logs = "\n".join([STORAGE_DONE, NCCL_DONE, "Final c-val test results: storage=pass nccl=pass dltest=pass", "Main DB update completed."])

    def _result(self, stdout="", returncode=0):
        return CommandResult(args=["kubectl"], stdout=stdout, stderr="", returncode=returncode)

    def run(self, args, check=True, input_text=None, timeout=None):
        self.calls.append(list(args))
        joined = " ".join(args)
        if "get pods -A" in joined:
            return self._result(json.dumps(self.pods_json))
        if "get nodes --no-headers" in joined:
            return self._result(self.nodes_table)
        if "get nodes -o json" in joined:
            return self._result(json.dumps(self.nodes_json))
        if joined.startswith("create -n"):
            return self._result("vcjob.batch.volcano.sh/cval created")
        if "get vcjob" in joined and "jsonpath" in joined:
            return self._result(next(self._phases, "Completed"))
        if joined.startswith("logs -n"):
            return self._result(self.logs)
        if "get pod -n" in joined:
            # resolve_status_pod probes; only the -server-0 candidate is "Running".
            candidate = args[4]
            phase = "Running" if candidate.endswith("-server-0") else "Pending"
            return self._result(json.dumps({"status": {"phase": phase}}))
        if "exec" in joined and "db-rebuild-dltest-metrics" in joined:
            return self._result(
                json.dumps(
                    {
                        "runs": 1,
                        "rank_files": 8,
                        "generation_id": "generation-1",
                        "db_paths": {
                            "numerical_correctness": "/data/continuous_validation/metadata/dltest_numerical_correctness.db",
                            "compute_performance": "/data/continuous_validation/metadata/dltest_compute_performance.db",
                            "collective_performance": "/data/continuous_validation/metadata/dltest_collective_performance.db",
                            "overlap_performance": "/data/continuous_validation/metadata/dltest_overlap_performance.db",
                        },
                    }
                )
            )
        if "exec" in joined and "baseline classify" in joined:
            test = args[args.index("--test-type") + 1]
            verdict = {"node": "node-x", "test_type": test, "status": "normal", "n_compared": 5, "n_degraded": 0, "metrics": []}
            if test == "dltest":
                verdict = {
                    "node": "node-x",
                    "test_type": "dltest",
                    "status": "degraded",
                    "n_compared": 2788,
                    "n_degraded": 12,
                    "degraded_metric_percent": 0.43,
                    "worst_pct_diff": 18.5,
                    "components": {"compute_performance": {"status": "degraded", "n_degraded": 12, "worst_pct_diff": 18.5, "degraded_metric_percent": 0.9}},
                    "metrics": [
                        {"metric": "nn_tasks/x/elapsed_ms", "component": "compute_performance", "status": "degraded", "pct_diff": 18.5, "direction": "high_bad"}
                    ],
                }
            payload = {"verdicts": [verdict], "stored_count": 1, "classification_db_path": "/data/.../classification-results.db"}
            return self._result(json.dumps(payload))
        if "exec" in joined and "latest_status" in (input_text or ""):
            rows = [{"node": "node-x", "test": t, "latest_timestamp": 123, "result": "pass"} for t in ("storage", "nccl", "dltest", "all")]
            return self._result(json.dumps(rows))
        if "exec" in joined and "python3 -c" in joined:
            rows = [{"node": "node-x", "test": t, "latest_timestamp": 123, "result": "pass"} for t in ("storage", "nccl", "dltest", "all")]
            return self._result(json.dumps(rows))
        return self._result("")

    # Convenience methods used by discovery.describe_node.
    def get_pods_json(self):
        self.calls.append(["get", "pods", "-A", "-o", "json"])
        return self.pods_json

    def get_nodes_capacity_table(self):
        self.calls.append(["get", "nodes", "--no-headers"])
        return self.nodes_table

    def get_nodes_json(self):
        self.calls.append(["get", "nodes", "-o", "json"])
        return self.nodes_json


class TestRunNodeValidation(unittest.TestCase):
    COMMIT = "a" * 40

    def test_missing_submission_gate_does_not_submit(self):
        client = FakeValidateClient()

        with self.assertRaises(PolicyViolation):
            run_node_validation(
                "node-x",
                client=client,
                git_ref=self.COMMIT,
                verbose=False,
                sleeper=lambda _s: None,
            )

        self.assertFalse(any(" ".join(call).startswith("create") for call in client.calls))

    def test_moving_or_invalid_ref_does_not_submit(self):
        for ref in ("main", "abc123", "A" * 40, "0" * 40):
            client = FakeValidateClient()
            with self.subTest(ref=ref), self.assertRaises(PolicyViolation):
                run_node_validation(
                    "node-x",
                    client=client,
                    git_ref=ref,
                    submit=True,
                    confirmation="submit",
                    verbose=False,
                )
            self.assertFalse(
                any(" ".join(call).startswith("create") for call in client.calls)
            )

    def test_wrong_confirmation_does_not_submit(self):
        client = FakeValidateClient()

        with self.assertRaises(PolicyViolation):
            run_node_validation(
                "node-x",
                client=client,
                git_ref=self.COMMIT,
                submit=True,
                confirmation="wrong",
                verbose=False,
            )

        self.assertFalse(
            any(" ".join(call).startswith("create") for call in client.calls)
        )

    def test_ineligible_node_does_not_submit(self):
        scenarios = {
            "missing": ("other 8 8", {"items": []}, {"items": []}),
            "busy": (
                "node-x 8 8",
                {
                    "items": [
                        {
                            "spec": {
                                "nodeName": "node-x",
                                "containers": [
                                    {
                                        "resources": {
                                            "requests": {"nvidia.com/gpu": "1"}
                                        }
                                    }
                                ],
                            },
                            "status": {"phase": "Running"},
                        }
                    ]
                },
                FakeValidateClient().nodes_json,
            ),
            "not-ready": (
                "node-x 8 8",
                {"items": []},
                {
                    "items": [
                        {
                            "metadata": {"name": "node-x"},
                            "spec": {},
                            "status": {
                                "conditions": [
                                    {"type": "Ready", "status": "False"}
                                ],
                                "allocatable": {
                                    "cpu": "110",
                                    "memory": "3036180572Ki",
                                    "nvidia.com/gpu": "8",
                                    "rdma/rdma_shared_device_a": "63",
                                },
                            },
                        }
                    ]
                },
            ),
        }
        for label, (table, pods, nodes) in scenarios.items():
            client = FakeValidateClient()
            client.nodes_table = table
            client.pods_json = pods
            client.nodes_json = nodes
            with self.subTest(label=label), self.assertRaises(PolicyViolation):
                run_node_validation(
                    "node-x",
                    client=client,
                    git_ref=self.COMMIT,
                    submit=True,
                    confirmation="submit",
                    verbose=False,
                )
            self.assertFalse(
                any(" ".join(call).startswith("create") for call in client.calls)
            )

    def test_full_happy_path(self):
        client = FakeValidateClient()
        ticks = iter(range(0, 100))
        report = run_node_validation(
            "node-x",
            client=client,
            git_ref=self.COMMIT,
            submit=True,
            confirmation="submit",
            timestamp=123,
            poll_interval=0.001,
            verbose=False,
            clock=lambda: next(ticks),
            sleeper=lambda _s: None,
        )
        self.assertEqual(report["job_phase"], "Completed")
        self.assertEqual(report["git_ref"], self.COMMIT)
        self.assertTrue(report["ok"])
        self.assertEqual(report["raw_overall"], "pass")
        self.assertEqual(report["classification"]["storage"]["status"], "unknown")
        self.assertEqual(report["classification"]["dltest"]["status"], "unknown")
        self.assertEqual(report["health"], "unknown")
        # One real job was created; derived writes were not coupled to validation.
        self.assertTrue(any(" ".join(call).startswith("create -n") for call in client.calls))
        self.assertTrue(
            any("--tail=2000" in call for call in client.calls if call and call[0] == "logs")
        )
        classify_calls = [c for c in client.calls if "baseline" in c and "classify" in c]
        self.assertEqual(classify_calls, [])
        self.assertFalse(
            any("db-rebuild-dltest-metrics" in call for call in client.calls)
        )

    def test_fully_free_cordoned_node_is_eligible(self):
        client = FakeValidateClient()
        client.nodes_json["items"][0]["spec"] = {"unschedulable": True}
        ticks = iter(range(0, 100))

        report = run_node_validation(
            "node-x",
            client=client,
            git_ref=self.COMMIT,
            submit=True,
            confirmation="submit",
            timestamp=123,
            poll_interval=0.001,
            verbose=False,
            clock=lambda: next(ticks),
            sleeper=lambda _s: None,
        )

        self.assertEqual(report["schedulability"]["status_label"], "cordoned")
        self.assertTrue(
            any(" ".join(call).startswith("create -n") for call in client.calls)
        )

    def test_stale_status_rows_do_not_produce_success_or_classification(self):
        client = FakeValidateClient()
        ticks = iter(range(0, 100))

        report = run_node_validation(
            "node-x",
            client=client,
            git_ref=self.COMMIT,
            submit=True,
            confirmation="submit",
            timestamp=124,
            poll_interval=0.001,
            verbose=False,
            clock=lambda: next(ticks),
            sleeper=lambda _s: None,
        )

        self.assertFalse(report["ok"])
        self.assertFalse(report["fresh_status_complete"])
        classify_calls = [
            call for call in client.calls if "baseline" in call and "classify" in call
        ]
        self.assertEqual(classify_calls, [])
        self.assertTrue(any("ignored stale" in note for note in report["notes"]))

    def test_dynamic_test_does_not_require_legacy_status_row(self):
        base = load_config()
        storage = base.tests.registry.require("storage")
        smoke = replace(
            storage,
            config_path="validation-tests/smoke/test_config.toml",
            definition=replace(
                storage.definition,
                metadata=replace(
                    storage.definition.metadata,
                    id="smoke",
                    display_name="Smoke",
                    order=40,
                ),
                settings={},
                plugin=None,
            ),
        )
        registry = ValidationTestRegistry(base.tests.registry.tests + (smoke,))
        config = replace(base, tests=replace(base.tests, registry=registry))
        client = FakeValidateClient()
        client.logs += "\nCVAL_EVENT " + json.dumps(
            {
                "schema_version": "cval.event.v1",
                "event": "test_finished",
                "run_id": "node-x-123",
                "test": "smoke",
                "timestamp": "2026-07-28T16:00:00Z",
                "status": "pass",
                "message": "",
            },
            separators=(",", ":"),
        )
        ticks = iter(range(0, 100))

        report = run_node_validation(
            "node-x",
            config=config,
            client=client,
            git_ref=self.COMMIT,
            submit=True,
            confirmation="submit",
            timestamp=123,
            poll_interval=0.001,
            verbose=False,
            clock=lambda: next(ticks),
            sleeper=lambda _s: None,
        )

        self.assertTrue(report["fresh_status_complete"])
        self.assertEqual(report["raw_results"]["smoke"], "pass")


class TestFinalizeDownloadZip(unittest.TestCase):
    def _pod_zip_b64(self, files: dict[str, str]) -> str:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, content in files.items():
                archive.writestr(name, content)
        return base64.b64encode(buffer.getvalue()).decode("ascii")

    def test_adds_report_and_preserves_artifacts(self):
        pod_b64 = self._pod_zip_b64(
            {
                "nccl/node-x/nccl-node-x-123/nccl-node-x-123.log": "nccl log body",
                "results/node-x/cval-results-node-x-123.json": '{"overall": "pass"}',
            }
        )
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={"status_label": "cordoned", "reason": "cordoned"},
            raw_results={"storage": "pass", "nccl": "pass", "dltest": "pass"},
            verdicts={"storage": None, "nccl": None, "dltest": None},
        )

        with TemporaryDirectory() as tmp:
            out = Path(tmp) / "cval-node-x-123.zip"
            info = finalize_download_zip(pod_b64, report, out)

            self.assertTrue(out.exists())
            with zipfile.ZipFile(out) as archive:
                names = set(archive.namelist())
                report_json = archive.read("report.json").decode("utf-8")
                log_body = archive.read("nccl/node-x/nccl-node-x-123/nccl-node-x-123.log").decode()

        self.assertIn("report.json", names)
        self.assertIn("report.txt", names)
        self.assertIn("nccl/node-x/nccl-node-x-123/nccl-node-x-123.log", names)
        self.assertEqual(log_body, "nccl log body")
        self.assertEqual(json.loads(report_json)["node"], "node-x")
        self.assertEqual(info["files"], len(names))
        self.assertGreater(info["bytes"], 0)

    def test_empty_pod_archive_still_writes_report(self):
        report = build_validation_report(
            node="node-y",
            timestamp=9,
            job_name="cval-node-y-9",
            job_phase="Completed",
            schedulability={},
            raw_results={"storage": "pass", "nccl": "pass", "dltest": "pass"},
            verdicts={"storage": None, "nccl": None, "dltest": None},
        )
        with TemporaryDirectory() as tmp:
            out = Path(tmp) / "cval-node-y-9.zip"
            info = finalize_download_zip("", report, out)
            with zipfile.ZipFile(out) as archive:
                names = set(archive.namelist())
        self.assertEqual(names, {"report.json", "report.txt"})
        self.assertEqual(info["files"], 2)


if __name__ == "__main__":
    unittest.main()
