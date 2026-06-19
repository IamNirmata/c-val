"""Tests for targeted single-node validation orchestration and helpers."""

from __future__ import annotations

import json
import unittest

from cval.config import CvalConfig, JobTemplateConfig
from cval.k8s.client import CommandResult
from cval.k8s.discovery import describe_node_from_outputs
from cval.orchestrator.validate import (
    build_validation_report,
    degraded_metrics_from_verdict,
    parse_test_progress,
    raw_results_from_log,
    render_validation_report,
    run_node_validation,
    _parse_classify_verdict,
)


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


class TestRawResults(unittest.TestCase):
    def test_raw_results_from_final_line(self):
        raw = raw_results_from_log(FINAL_LINE)
        self.assertEqual(raw["storage"], "pass")
        self.assertEqual(raw["dltest"], "fail")
        self.assertEqual(raw["all"], "fail")

    def test_all_pass(self):
        raw = raw_results_from_log("Final c-val test results: storage=pass nccl=pass dltest=pass")
        self.assertEqual(raw["all"], "pass")

    def test_missing_line(self):
        self.assertEqual(raw_results_from_log("nothing here"), {})


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


class TestParseClassifyVerdict(unittest.TestCase):
    def test_payload_dict(self):
        payload = {"verdicts": [{"node": "n", "status": "normal"}], "stored_count": 1}
        verdict = _parse_classify_verdict(json.dumps(payload))
        self.assertEqual(verdict["status"], "normal")

    def test_plain_list(self):
        verdict = _parse_classify_verdict(json.dumps([{"node": "n", "status": "degraded"}]))
        self.assertEqual(verdict["status"], "degraded")

    def test_bad_json(self):
        self.assertIsNone(_parse_classify_verdict("not json"))


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

    def test_dry_run_render(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="DryRun",
            schedulability={"fully_free": True, "schedulable": True, "resource_ready": True, "reason": "free"},
            raw_results={},
            verdicts={"storage": None, "nccl": None, "dltest": None},
            dry_run=True,
        )
        self.assertIn("DRY RUN", render_validation_report(report))


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

    def _nodes_json(self, name, *, unschedulable=False, allocatable=None):
        spec = {"unschedulable": True} if unschedulable else {}
        return {
            "items": [
                {
                    "metadata": {"name": name},
                    "spec": spec,
                    "status": {
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

    def test_cordoned_node(self):
        status = describe_node_from_outputs(
            "node-x",
            {"items": []},
            "node-x 8 8",
            self._nodes_json("node-x", unschedulable=True),
            self._config(),
        )
        self.assertFalse(status.schedulable)
        self.assertFalse(status.fully_free)

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
            return self._result(json.dumps({"runs": 1, "rank_files": 8}))
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
    def test_dry_run_does_not_submit(self):
        client = FakeValidateClient()
        report = run_node_validation(
            "node-x",
            client=client,
            dry_run=True,
            verbose=False,
            sleeper=lambda _s: None,
        )
        self.assertTrue(report["dry_run"])
        self.assertTrue(report["job_name"].startswith("cval-node-x-"))
        self.assertFalse(any(" ".join(call).startswith("create") for call in client.calls))

    def test_full_happy_path(self):
        client = FakeValidateClient()
        ticks = iter(range(0, 100))
        report = run_node_validation(
            "node-x",
            client=client,
            poll_interval=0.0,
            verbose=False,
            clock=lambda: next(ticks),
            sleeper=lambda _s: None,
        )
        self.assertEqual(report["job_phase"], "Completed")
        self.assertTrue(report["ok"])
        self.assertEqual(report["raw_overall"], "pass")
        self.assertEqual(report["classification"]["storage"]["status"], "normal")
        self.assertEqual(report["classification"]["dltest"]["status"], "degraded")
        self.assertEqual(report["health"], "degraded")
        # A job was created and classification ran for all three tests.
        self.assertTrue(any(" ".join(call).startswith("create -n") for call in client.calls))
        classify_calls = [c for c in client.calls if "baseline" in c and "classify" in c]
        self.assertEqual(len(classify_calls), 3)


if __name__ == "__main__":
    unittest.main()
