from __future__ import annotations

import unittest
import json
import tempfile
import sqlite3
import hashlib
from pathlib import Path
from dataclasses import replace
from unittest.mock import Mock, patch

from cval.storage.status import (
    _read_recording_state,
    build_status_snapshot,
    collect_status_observation,
    read_local_coverage,
    get_latest_status_rows,
    latest_status_rows_to_node_map,
    parse_latest_status_rows_json,
    resolve_status_pod,
)
from cval.config import load_config
from cval.k8s.client import CommandResult
from cval.storage.metrics import (
    get_latest_nccl_health_metrics,
    get_latest_nccl_metrics,
    get_latest_storage_metrics,
)


class FakeKubectlClient:
    def __init__(self, responses: dict[tuple[str, ...], CommandResult]) -> None:
        self.responses = responses
        self.calls = []

    def run(self, args, check=True, input_text=None):
        self.calls.append((tuple(args), input_text))
        result = self.responses.get(
            tuple(args),
            CommandResult(args=("kubectl", *args), stdout="", stderr="not found", returncode=1),
        )
        if check and result.returncode != 0:
            raise RuntimeError(result.stderr)
        return result


class StatusParsingTests(unittest.TestCase):
    def test_operational_status_recorded_failure_keeps_older_successful_coverage(self) -> None:
        from cval.validation.results import parse_validation_result_v2, validation_result_digest
        from test_results_v2 import payload

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "validation.db"
            connection = sqlite3.connect(database)
            connection.execute("CREATE TABLE runs (node TEXT, test TEXT, timestamp INTEGER, result TEXT, image_name TEXT, pytorch_version TEXT, cuda_version TEXT)")
            for timestamp in (123, 124):
                result = payload()
                run_id = f"node-a-{timestamp}"
                result.update(timestamp=timestamp, run_id=run_id, timestamp_la=f"1969-12-31T16:02:0{timestamp - 120}-08:00")
                if timestamp == 124:
                    result["tests"]["nccl"].update(status="fail", exit_code=1)
                    result["overall"] = "fail"
                for name in ("storage", "nccl", "dltest"):
                    summary = root / "logs/validation_tests" / name / "runs/node-a" / run_id / "summary.json"
                    summary.parent.mkdir(parents=True)
                    summary.write_text("{}\n")
                    result["tests"][name]["summary"] = str(summary)
                run = root / "logs/job_logs/node-a" / run_id
                run.mkdir(parents=True)
                (run / "result.json").write_text(json.dumps(result))
                digest = validation_result_digest(parse_validation_result_v2(result))
                (run / ".ingestion-result-digest").write_text(digest + "\n")
                for name in ("all", "storage", "nccl", "dltest"):
                    connection.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)", (
                        "node-a", name, timestamp, result["overall"] if name == "all" else result["tests"][name]["status"],
                        result["image_name"], result["pytorch_version"], result["cuda_version"],
                    ))
            connection.commit()
            connection.close()
            before = hashlib.sha256(database.read_bytes()).hexdigest()
            report = read_local_coverage({"root": directory, "db_path": str(database), "nodes": ["node-a"],
                "tests": ["storage", "nccl", "dltest"], "max_runs": 100, "timeout_seconds": 30,
                "sqlite_timeout_seconds": 5, "max_files": 50, "max_bytes": 1000000, "include_recording": True,
                "attempts": {"node-a": 124}})
            self.assertEqual(report["state"], "queried", report)
            self.assertEqual(report["timestamps"], {"node-a": 123})
            self.assertEqual(report["nodes"][0]["latest_record_timestamp"], 124)
            self.assertEqual(report["nodes"][0]["recording_state"], "fully_recorded")
            self.assertEqual(report["nodes"][0]["test_outcome"], "fail")
            self.assertEqual(hashlib.sha256(database.read_bytes()).hexdigest(), before)

    def test_operational_status_collects_shared_inventory_and_preserves_unknown_readiness(self) -> None:
        config = load_config()
        resources = {"nvidia.com/gpu": "8", "cpu": "128", "memory": "1800Gi", "rdma/rdma_shared_device_a": "1"}
        client = Mock()
        client.get_nodes_json.return_value = {"items": [
            {"metadata": {"name": "hgx-a", "uid": "uid-a"}, "spec": {"unschedulable": True},
             "status": {"capacity": resources, "allocatable": resources, "conditions": [{"type": "Ready", "status": "True"}]}},
            {"metadata": {"name": "hgx-b", "uid": "uid-b"}, "spec": {},
             "status": {"capacity": resources, "allocatable": resources, "conditions": [{"type": "Ready", "status": "Unknown"}]}},
        ]}
        client.get_pods_json.return_value = {"items": []}
        with tempfile.TemporaryDirectory() as directory, patch("cval.storage.status.get_coverage_report", return_value={"state": "empty"}) as coverage:
            config = replace(config, live=replace(config.live, log_dir=directory))
            snapshot = collect_status_observation(config, client, {"intents": {}}, [])
        client.get_nodes_json.assert_called_once()
        client.get_pods_json.assert_called_once()
        self.assertEqual(coverage.call_args.kwargs["nodes"], ["hgx-a", "hgx-b"])
        self.assertTrue(coverage.call_args.kwargs["include_recording"])
        self.assertEqual(snapshot["nodes"][0]["k8s_ready"], "true")
        self.assertEqual(snapshot["nodes"][0]["scheduling_enabled"], "false")
        self.assertEqual(snapshot["nodes"][1]["k8s_ready"], "unknown")
        client.get_nodes_json.side_effect = RuntimeError("Forbidden: nodes denied")
        failed = collect_status_observation(config, client, {"intents": {}}, snapshot["nodes"])
        self.assertEqual(failed["inventory_state"], "forbidden")
        self.assertEqual(failed["nodes"][0]["node_uid"], "uid-a")
        self.assertEqual(failed["nodes"][0]["k8s_ready"], "unknown")

    def test_operational_status_recording_requires_canonical_and_bound_receipt(self) -> None:
        from cval.storage.verification import EvidenceReader
        from cval.validation.results import parse_validation_result_v2, validation_result_digest
        from test_results_v2 import payload

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request = {"root": directory, "timeout_seconds": 30, "max_files": 20, "max_bytes": 100000}
            observe = lambda rows: _read_recording_state(EvidenceReader(request), "node-a", 123, rows)
            self.assertEqual(observe([])["recording_state"], "no_results")
            result = payload()
            result["tests"]["nccl"].update(status="fail", exit_code=1)
            result["overall"] = "fail"
            run = root / "logs/job_logs/node-a/node-a-123"
            run.mkdir(parents=True)
            (run / "result.json").write_text(json.dumps(result))
            self.assertEqual(observe([])["recording_state"], "results_without_db_update")
            rows = [{"test": name, "result": result["overall"] if name == "all" else result["tests"][name]["status"]}
                    for name in ("all", "storage", "nccl", "dltest")]
            self.assertEqual(observe(rows[:1])["recording_state"], "partial_db_update")
            self.assertEqual(observe(rows)["recording_state"], "partial_db_update")
            digest = validation_result_digest(parse_validation_result_v2(result))
            (run / ".ingestion-result-digest").write_text(digest + "\n")
            recorded = observe(rows)
            self.assertEqual(recorded["recording_state"], "fully_recorded")
            self.assertEqual(recorded["test_outcome"], "fail")
            self.assertEqual(recorded["latest_record_timestamp"], 123)
            (run / ".ingestion-result-digest").write_text("wrong\n")
            with self.assertRaisesRegex(ValueError, "acknowledgement"):
                observe(rows)
            (run / "result.json").write_text(json.dumps({"schema_version": "cval.results.v1"}))
            self.assertEqual(observe(rows)["recording_state"], "unverified")

    def test_operational_status_keeps_attempt_and_record_separate(self) -> None:
        config = load_config()
        now = 1790370000
        successful = now - 100
        expired = now - 8 * 86400
        coverage = {
            "schema_version": "cval.coverage.v1", "state": "queried", "policy": "successful_complete_run",
            "requested_tests": sorted(test.id for test in config.tests.registry.enabled),
            "timestamps": {"hgx-fresh": successful, "hgx-old": expired},
            "nodes": [
                {"node": "hgx-fresh", "state": "queried", "coverage_timestamp": successful, "outcome": "pass",
                 "reason": "complete_canonical_run", "latest_attempt_timestamp": successful},
                {"node": "hgx-old", "state": "queried", "coverage_timestamp": expired, "outcome": "pass", "reason": "complete_canonical_run"},
                {"node": "hgx-new", "state": "empty", "coverage_timestamp": None},
            ],
        }
        observations = [{"node_name": name, "node_uid": name + "-uid", "observed_at_epoch": now,
                         "k8s_ready": "true", "scheduling_enabled": "false" if name == "hgx-old" else "true"}
                        for name in ("hgx-fresh", "hgx-old", "hgx-new")]
        intent = {"node_name": "hgx-fresh", "timestamp": now, "job_name": "new-attempt", "git_ref": "a" * 40,
                  "config_digest": "sha256:config", "events": [{"state": "prepared"}]}
        rows, cluster = build_status_snapshot(observations, coverage, {"intents": {"new-attempt": intent}}, config, now=now)
        indexed = {row["node_name"]: row for row in rows}
        self.assertEqual(indexed["hgx-fresh"]["latest_record_epoch"], successful)
        self.assertEqual(indexed["hgx-fresh"]["job_run_epoch"], now)
        self.assertEqual(indexed["hgx-fresh"]["job_phase"], "prepared")
        self.assertEqual(indexed["hgx-fresh"]["test_outcome"], "unknown")
        self.assertEqual(indexed["hgx-fresh"]["priority_rank"], "")
        self.assertEqual(indexed["hgx-new"]["priority_rank"], 1)
        self.assertEqual(indexed["hgx-old"]["priority_rank"], 2)
        self.assertTrue(indexed["hgx-fresh"]["latest_record_la"].endswith("-07:00"))
        self.assertEqual(cluster["complete_recorded_24h_nodes"], 1)
        self.assertEqual(cluster["ready_nodes"], 3)
        self.assertEqual(cluster["scheduling_disabled_nodes"], 1)
        self.assertEqual({row["snapshot_id"] for row in rows}, {cluster["snapshot_id"]})
        observations[0]["node_created_at_epoch"] = now - 50
        replaced_rows, replaced_cluster = build_status_snapshot(observations, coverage, {"intents": {"new-attempt": intent}}, config, now=now)
        current = next(row for row in replaced_rows if row["node_name"] == "hgx-fresh")
        self.assertEqual(current["latest_record_epoch"], "")
        self.assertEqual(current["coverage_state"], "missing")
        self.assertEqual(current["job_phase"], "prepared")
        self.assertEqual(replaced_cluster["complete_recorded_24h_nodes"], 0)

    def test_operational_status_does_not_convert_unavailable_to_missing(self) -> None:
        rows, cluster = build_status_snapshot(
            [{"node_name": "hgx-unknown", "observation_state": "forbidden"}],
            {"state": "forbidden", "reason": "PVC read denied"}, {}, load_config(), now=1790370000,
        )
        self.assertEqual(rows[0]["coverage_state"], "unverified")
        self.assertEqual(rows[0]["observation_state"], "forbidden")
        self.assertEqual(rows[0]["recording_state"], "unverified")
        self.assertEqual(rows[0]["priority_rank"], "")
        self.assertEqual(cluster["coverage_missing"], 0)
        self.assertEqual(cluster["readiness_unknown_nodes"], 1)
        self.assertFalse(cluster["observation_complete"])

    def test_latest_status_json_uses_max_timestamp_per_node(self) -> None:
        rows = parse_latest_status_rows_json(
            """
            [
                            {
                                "node": "slc01-cl02-hgx-0001",
                                "test": "storage",
                                "latest_timestamp": 100,
                                "result": "pass"
                            },
                            {
                                "node": "slc01-cl02-hgx-0001",
                                "test": "nccl",
                                "latest_timestamp": 200,
                                "result": "pass"
                            },
                            {
                                "node": "slc01-cl02-hgx-0002",
                                "test": "storage",
                                "latest_timestamp": null,
                                "result": "fail"
                            }
            ]
            """
        )

        self.assertEqual(
            latest_status_rows_to_node_map(rows),
            {"slc01-cl02-hgx-0001": 200, "slc01-cl02-hgx-0002": 0},
        )

    def test_resolve_status_pod_uses_volcano_access_pod(self) -> None:
        client = FakeKubectlClient(
            {
                (
                    "get",
                    "pod",
                    "-n",
                    "gcr-admin",
                    "gcr-admin-pvc-access-server-0",
                    "-o",
                    "json",
                ): CommandResult(
                    args=(),
                    stdout='{"status":{"phase":"Running"}}',
                    stderr="",
                    returncode=0,
                )
            }
        )

        self.assertEqual(
            resolve_status_pod(client, "gcr-admin", "gcr-admin-pvc-access"),
            "gcr-admin-pvc-access-server-0",
        )

    def test_resolve_status_pod_uses_label_selector(self) -> None:
        client = FakeKubectlClient(
            {
                (
                    "get",
                    "pods",
                    "-n",
                    "gcr-admin",
                    "-l",
                    "volcano.sh/job-name=gcr-admin-pvc-access",
                    "-o",
                    "json",
                ): CommandResult(
                    args=(),
                    stdout=(
                        '{"items":[{"metadata":{"name":"access-pod"},'
                        '"status":{"phase":"Running"}}]}'
                    ),
                    stderr="",
                    returncode=0,
                )
            }
        )

        self.assertEqual(
            resolve_status_pod(client, "gcr-admin", "gcr-admin-pvc-access"),
            "access-pod",
        )

    def test_status_and_metric_helpers_prefer_explicit_config(self) -> None:
        base = load_config()
        config = replace(
            base,
            cluster=replace(
                base.cluster,
                namespace="explicit-namespace",
                pvc_access_pod="explicit-pod",
            ),
            storage=replace(
                base.storage,
                validation_db_path="/explicit/status ?#%.db",
                nccl_db_path="/explicit/nccl ?#%.db",
                storage_db_path="/explicit/storage ?#%.db",
            ),
        )
        client = FakeKubectlClient({})
        response = CommandResult(args=(), stdout="[]", stderr="", returncode=0)

        def run(args, check=True, input_text=None):
            client.calls.append((tuple(args), input_text))
            return response

        client.run = run  # type: ignore[method-assign]
        with patch(
            "cval.storage.status.resolve_status_pod",
            return_value="resolved-explicit-pod",
        ), patch(
            "cval.storage.metrics.resolve_status_pod",
            return_value="resolved-explicit-pod",
        ), patch(
            "cval.storage.status.load_config",
            side_effect=AssertionError("ambient status config reload"),
        ), patch(
            "cval.storage.metrics.load_config",
            side_effect=AssertionError("ambient metric config reload"),
        ):
            self.assertEqual(get_latest_status_rows(client=client, config=config), [])
            self.assertEqual(get_latest_nccl_metrics(client=client, config=config), {})
            self.assertEqual(get_latest_storage_metrics(client=client, config=config), {})
            self.assertEqual(
                get_latest_nccl_health_metrics(client=client, config=config), {}
            )

        commands = [call[0] for call in client.calls]
        self.assertTrue(all("explicit-namespace" in command for command in commands))
        self.assertEqual(
            [command[-1] for command in commands],
            [
                "/explicit/status ?#%.db",
                "/explicit/nccl ?#%.db",
                "/explicit/storage ?#%.db",
                "/explicit/nccl ?#%.db",
            ],
        )
        self.assertTrue(
            all("connect_sqlite_readonly" in (call[1] or "") for call in client.calls)
        )


if __name__ == "__main__":
    unittest.main()