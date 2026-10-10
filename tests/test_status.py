from __future__ import annotations

import unittest
import json
import tempfile
import sqlite3
import hashlib
import subprocess
import sys
import base64
import zlib
from pathlib import Path
from dataclasses import replace
from types import SimpleNamespace
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
    def test_evidence_transport_rejects_corruption_and_oversized_reports(self) -> None:
        from cval.storage.verification import collect_exact_evidence

        config = load_config()
        report = json.dumps({"state": "queried", "nodes": [], "timestamps": {}}).encode()
        compressed = zlib.compress(report)
        envelope = {"encoding": "zlib+base64", "sha256": hashlib.sha256(report).hexdigest(),
                    "data": base64.b64encode(compressed).decode()}
        invalid_responses = {
            "empty": "",
            "truncated_json": json.dumps(envelope)[:-1],
            "checksum": json.dumps(envelope | {"sha256": "0" * 64}),
            "truncated_payload": json.dumps(envelope | {"data": base64.b64encode(compressed[:-1]).decode()}),
            "trailing_payload": json.dumps(envelope | {"data": base64.b64encode(compressed + b"extra").decode()}),
            "encoding": json.dumps(envelope | {"encoding": "plain"}),
            "oversized": json.dumps(envelope),
        }
        for name, stdout in invalid_responses.items():
            with self.subTest(name=name):
                client = SimpleNamespace(run=lambda *_args, **_kwargs: CommandResult([], stdout, "", 0))
                response = collect_exact_evidence({"timeout_seconds": 10, "max_bytes": len(report) - 1 if name == "oversized" else 4096}, config, client)
                self.assertEqual(response["state"], "unavailable")
                self.assertFalse(response["accepted"])
                self.assertFalse(response["complete"])
        client = SimpleNamespace(run=lambda *_args, **_kwargs: CommandResult([], "", "Forbidden", 1))
        self.assertEqual(collect_exact_evidence({"timeout_seconds": 10, "max_bytes": 4096}, config, client)["state"], "forbidden")

    def test_fleet_coverage_reads_mounted_pvc_without_access_pod(self) -> None:
        from cval.storage.status import get_coverage_report

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "validation.db"
            nodes = [f"hgx-{index:04d}" for index in range(500)]
            connection = sqlite3.connect(database)
            try:
                connection.execute("CREATE TABLE runs (node TEXT, test TEXT, timestamp INTEGER, result TEXT, image_name TEXT, pytorch_version TEXT, cuda_version TEXT)")
                connection.executemany("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                                       [(node, test, 123, "pass", "image", "torch", "cuda")
                                        for node in nodes for test in ("all", "storage", "nccl", "dltest")])
                connection.commit()
            finally:
                connection.close()
            config = load_config()
            config = replace(config, runtime=replace(config.runtime, validation_root=str(root)),
                             storage=replace(config.storage, validation_db_path=str(database)))
            client = SimpleNamespace(run=Mock(side_effect=AssertionError("mounted PVC coverage must not exec into the reader pod")))
            report = get_coverage_report(config, client, nodes=nodes)
            self.assertEqual(report["state"], "queried", report)
            self.assertEqual(report["timestamps"], {node: 123 for node in nodes})
            self.assertEqual(len(report["nodes"]), 500)
            client.run.assert_not_called()

    def test_periodic_projection_keeps_completed_collection_during_next_refresh(self) -> None:
        from cval.config import encode_config_snapshot
        from cval.storage.status import project_status_csvs, read_status_csvs

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = root / "session"
            session.mkdir()
            base = load_config()
            config = replace(base, live=replace(base.live, status_dir=str(root / "reports")))
            timestamp = 900
            coverage = {
                "schema_version": "cval.coverage.v1", "state": "queried", "policy": "successful_complete_run",
                "requested_tests": sorted(test.id for test in config.tests.registry.enabled),
                "timestamps": {"hgx-a": timestamp},
                "nodes": [{"node": "hgx-a", "state": "queried", "coverage_timestamp": timestamp,
                           "reason": "complete_canonical_run", "outcome": "pass", "recording_state": "fully_recorded",
                           "test_outcome": "pass", "latest_record_timestamp": timestamp}],
            }
            snapshot = {"observed_at_epoch": 1000, "collected_at_epoch": 1170, "inventory_state": "queried",
                        "nodes": [{"node_name": "hgx-a", "node_uid": "uid-a", "observed_at_epoch": 1000,
                                   "observation_state": "queried"}], "coverage": coverage}
            (session / "status-observation.json").write_text(json.dumps(snapshot))
            state = {"config_snapshot_b64": encode_config_snapshot(config), "intents": {}}
            with patch("cval.storage.status.time.time", return_value=1301):
                project_status_csvs(session, state)
            rows, _cluster = read_status_csvs(Path(config.live.status_dir))
            self.assertEqual(rows[0]["coverage_state"], "fresh")
            self.assertEqual(rows[0]["recording_state"], "fully_recorded")
            self.assertEqual(rows[0]["observed_at_epoch"], "1000")
            with patch("cval.storage.status.time.time", return_value=1471):
                project_status_csvs(session, state)
            expired, _cluster = read_status_csvs(Path(config.live.status_dir))
            self.assertEqual(expired[0]["coverage_state"], "unverified")
            self.assertEqual(expired[0]["observation_state"], "stale")
            del snapshot["collected_at_epoch"]
            (session / "status-observation.json").write_text(json.dumps(snapshot))
            for now, expected in ((1200, "fresh"), (1301, "unverified")):
                with self.subTest(historical_cache_at=now), patch("cval.storage.status.time.time", return_value=now):
                    project_status_csvs(session, state)
                    historical, _cluster = read_status_csvs(Path(config.live.status_dir))
                    self.assertEqual(historical[0]["coverage_state"], expected)

    def test_operational_status_preserves_then_counts_and_hashes_one_generation(self) -> None:
        now = 1790370000
        previous = [{"node_name": "hgx-a", "node_uid": "uid-a", "latest_record_epoch": str(now - 60),
                     "latest_record_la": "2026-09-25T13:59:00-07:00", "job_name": "old-job", "job_uid": "old-uid",
                     "job_run_epoch": str(now - 60), "job_phase": "Completed", "source_sha": "a" * 40,
                     "config_digest": "sha256:retained", "cooldown_until_epoch": str(now + 100)}]
        observations = [{"node_name": "hgx-a", "node_uid": "uid-a", "observation_state": "stale"}]
        config = load_config()
        with patch("cval.storage.status.hashlib.sha256", wraps=hashlib.sha256) as digest:
            rows, cluster = build_status_snapshot(observations, {"state": "stale"}, {}, config,
                now=now, previous=previous, inventory_state="unavailable")
        self.assertEqual(digest.call_count, 1)
        self.assertEqual(rows[0]["latest_record_epoch"], now - 60)
        self.assertEqual(rows[0]["job_name"], "old-job")
        self.assertEqual(rows[0]["coverage_state"], "unverified")
        self.assertEqual(cluster["complete_recorded_24h_nodes"], 1)
        self.assertFalse(cluster["observation_complete"])
        self.assertEqual(rows[0]["snapshot_id"], cluster["snapshot_id"])
        self.assertEqual(list(cluster)[-2:], ["snapshot_id", "inventory_state"])
        observations[0]["node_uid"] = "replacement-uid"
        rows, cluster = build_status_snapshot(observations, {"state": "stale"}, {}, config, now=now, previous=previous)
        self.assertEqual(rows[0]["latest_record_epoch"], "")
        self.assertEqual(rows[0]["job_name"], "")
        self.assertEqual(cluster["complete_recorded_24h_nodes"], 0)

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
        self.assertGreaterEqual(snapshot["collected_at_epoch"], snapshot["observed_at_epoch"])
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
        expected_paths = [
            "/explicit/status ?#%.db",
            "/explicit/nccl ?#%.db",
            "/explicit/storage ?#%.db",
            "/explicit/nccl ?#%.db",
        ]
        for command, expected_path in zip(commands, expected_paths):
            self.assertIn(expected_path, command)
        self.assertTrue(
            all("connect_sqlite_readonly" in (call[1] or "") for call in client.calls)
        )

    def test_latest_status_reads_mounted_pvc_without_access_pod_exec(self) -> None:
        from cval.models import LatestStatusRow
        from cval.storage.status import get_latest_status_rows

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "validation.db"
            connection = sqlite3.connect(database)
            connection.execute(
                "CREATE TABLE latest_status (node TEXT, test TEXT, latest_timestamp INTEGER, result TEXT)"
            )
            connection.execute(
                "INSERT INTO latest_status VALUES (?, ?, ?, ?)",
                ("hgx-test", "storage", 123, "pass"),
            )
            connection.commit()
            connection.close()
            before = hashlib.sha256(database.read_bytes()).hexdigest()
            config = load_config()
            config = replace(
                config,
                runtime=replace(config.runtime, validation_root=str(root)),
                storage=replace(config.storage, validation_db_path=str(database)),
            )
            client = Mock()

            rows = get_latest_status_rows(client=client, config=config)

            self.assertEqual(rows, [LatestStatusRow("hgx-test", "storage", 123, "pass")])
            client.run.assert_not_called()
            self.assertEqual(hashlib.sha256(database.read_bytes()).hexdigest(), before)


if __name__ == "__main__":
    unittest.main()