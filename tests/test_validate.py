"""Tests for targeted single-node validation orchestration and helpers."""

from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.config import CvalConfig, JobTemplateConfig, load_config
from cval.k8s.client import CommandResult
from cval.k8s.discovery import describe_node_from_outputs
from cval.jobs.monitor import _runtime_progress
from cval.orchestrator.validate import (
    build_validation_report,
    render_test_progress_line,
    render_validation_report,
    run_node_validation,
)
from cval.policy import PolicyViolation
from cval.validation.registry import ValidationTestRegistry


STORAGE_DONE = "Storage test is complete. Log file: x Summary file: y"
NCCL_DONE = "NCCL test is complete. Log file: x Summary file: y"
FINAL_LINE = "Final c-val test results: storage=pass nccl=pass dltest=fail"


class TestStructuredProgress(unittest.TestCase):
    def setUp(self):
        self.config = load_config()
        self.intent = {"node_name": "node-a", "run_id": "node-a-123", "config_digest": "sha256:" + "a" * 64}

    def test_legacy_messages_do_not_authorize_completion(self):
        runtime = _runtime_progress("\n".join([STORAGE_DONE, NCCL_DONE, FINAL_LINE, "Main DB update completed."]), self.intent, self.config)
        self.assertEqual(runtime["progress"], {})
        self.assertEqual(runtime["tests_outcome"], "unconfirmed")
        self.assertEqual(runtime["ingestion_outcome"], "unconfirmed")
        self.assertFalse(runtime["completion_confirmed"])

    def test_structured_events_support_dynamic_test_ids(self):
        source = self.config.tests.registry.require("storage")
        test = replace(source, definition=replace(source.definition, metadata=replace(source.definition.metadata, id="smoke")))
        config = replace(self.config, tests=replace(self.config.tests, registry=ValidationTestRegistry((test,))))
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

        runtime = _runtime_progress(text, self.intent, config)
        self.assertEqual(runtime["progress"], {"smoke": "pass"})
        self.assertFalse(runtime["completion_confirmed"])
        other_run = text.replace("node-a-123", "node-a-124")
        self.assertEqual(_runtime_progress(other_run, self.intent, config)["progress"], {})

    def test_malformed_structured_event_is_ignored(self):
        self.assertEqual(_runtime_progress("CVAL_EVENT {not-json}", self.intent, self.config)["progress"], {})

    def test_dynamic_progress_line_uses_registry_order(self):
        line = render_test_progress_line(
            12.0,
            "Running",
            ["storage", "smoke"],
            {"storage": "pass", "smoke": "running"},
        )

        self.assertIn("storage=PASS smoke=RUNNING", line)


    def test_structured_ingestion_failure_overrides_legacy_marker(self):
        event = {
            "schema_version": "cval.event.v1",
            "event": "ingestion_finished",
            "run_id": "node-a-123",
            "test": None,
            "timestamp": "2026-07-28T16:00:01Z",
            "status": "fail",
            "message": "write failed",
            "producer": "ingestion",
            "node_name": "node-a",
            "config_digest": self.intent["config_digest"],
            "result_digest": "sha256:" + "b" * 64,
            "stores": {name: "unconfirmed" for name in ("storage", "nccl", "validation", "numerical_correctness", "compute_performance", "collective_performance", "overlap_performance")},
        }
        text = "Main DB update completed.\nCVAL_EVENT " + json.dumps(event)

        runtime = _runtime_progress(text, self.intent, self.config)
        self.assertEqual(runtime["ingestion_outcome"], "fail")
        self.assertFalse(runtime["completion_confirmed"])



class TestBuildAndRenderReport(unittest.TestCase):
    def test_report_contains_only_raw_results(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={"fully_free": True, "schedulable": True, "resource_ready": True, "reason": "free"},
            raw_results={"storage": "pass", "nccl": "pass", "dltest": "pass", "all": "pass"},
        )
        self.assertTrue(report["ok"])
        self.assertEqual(report["raw_overall"], "pass")
        self.assertNotIn("health", report)
        self.assertNotIn("classification", report)

    def test_render_contains_sections(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={"fully_free": True, "schedulable": True, "resource_ready": True, "reason": "free"},
            raw_results={"storage": "pass", "nccl": "pass", "dltest": "pass", "all": "pass"},
        )
        text = render_validation_report(report)
        for token in ("validation report", "node-x", "storage", "nccl", "dltest"):
            self.assertIn(token, text)
        self.assertNotIn("VERDICT", text)
        self.assertNotIn("HEALTH", text)

    def test_completed_job_with_failed_raw_result_is_not_ok(self):
        report = build_validation_report(
            node="node-x",
            timestamp=123,
            job_name="cval-node-x-123",
            job_phase="Completed",
            schedulability={},
            raw_results={"storage": "pass", "nccl": "fail", "all": "fail"},
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
        spec = (
            {
                "unschedulable": True,
                "taints": [
                    {
                        "key": "node.kubernetes.io/unschedulable",
                        "effect": "NoSchedule",
                    }
                ],
            }
            if unschedulable
            else {}
        )
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
        self.assertNotIn("classification", report)
        self.assertNotIn("health", report)
        # One real job was created; raw recovery was not coupled to validation.
        self.assertTrue(any(" ".join(call).startswith("create -n") for call in client.calls))
        self.assertTrue(
            any("--tail=2000" in call for call in client.calls if call and call[0] == "logs")
        )
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

    def test_stale_status_rows_do_not_produce_success(self):
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


if __name__ == "__main__":
    unittest.main()
