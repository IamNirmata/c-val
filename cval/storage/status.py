"""Read and format c-val latest validation status.

Status reads are intentionally opened through SQLite `mode=ro` so operators and
agents can inspect validation history without mutating DB tables or views.
"""

from __future__ import annotations

import json
import re
from itertools import groupby
from pathlib import Path

from cval.config import load_config
from cval.k8s.client import KubectlClient
from cval.models import LatestStatusRow
from cval.storage.sqlite_uri import sqlite_readonly_script_prelude


def _failed_workload_completed(test_id: str, summary: Path, reader) -> bool:
    if test_id == "dltest":
        value = reader.json(summary)
        count = value.get("gpu_count")
        return (
            value.get("schema_version") == "cval.dltest.summary.v1"
            and all(value.get(key) is True for key in ("rank_coverage_valid", "test_plans_match", "tasks_complete"))
            and type(count) is int and count > 0 and value.get("rank_result_count") == count
            and value.get("observed_ranks") == list(range(count))
        )
    if test_id == "nccl":
        from cval.storage.ingest import parse_nccl_health_summary

        parse_nccl_health_summary(summary)
        return True
    if test_id == "storage":
        from cval.storage.ingest import STORAGE_FILE_PREFIXES, parse_storage_metrics

        for name in STORAGE_FILE_PREFIXES:
            reader.read(summary.parent / "artifacts" / name)
        parse_storage_metrics(summary.parent / "artifacts")
        return True
    return False


def read_local_coverage(request: dict) -> dict:
    """Choose one complete requested-test run, retaining the declared legacy age policy."""

    from cval.storage.verification import EvidenceReader, _failure
    from cval.storage.paths import safe_existing_evidence_path
    from cval.validation.results import parse_validation_result_v2, validation_result_digest

    reader = EvidenceReader(request)
    report = {"schema_version": "cval.coverage.v1", "state": "queried", "policy": "legacy_complete_run",
              "compatibility": "legacy_age_only; dependency provenance not yet produced", "requested_tests": sorted(request["tests"]), "nodes": [], "timestamps": {}}
    try:
        required = set(request["tests"])
        if not required:
            raise ValueError("Coverage requires a nonempty requested test set")
        parameters = []
        where = ""
        if request["nodes"] is not None:
            where = " WHERE node IN (" + ",".join("?" for node in request["nodes"]) + ")"
            parameters.extend(request["nodes"])
        sql = "WITH history AS (SELECT node,test,timestamp,result,image_name,pytorch_version,cuda_version,DENSE_RANK() OVER (PARTITION BY node ORDER BY timestamp DESC) AS rank FROM runs" + where + ") SELECT * FROM history WHERE rank<=? ORDER BY node,timestamp DESC,test"
        parameters.append(request["max_runs"] + 1)
        rows = reader.rows(request["db_path"], sql, tuple(parameters))
        grouped = {node: list(items) for node, items in groupby(rows, lambda row: row["node"])}
        for node in sorted(request["nodes"] if request["nodes"] is not None else grouped):
            if not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", node):
                raise ValueError("Invalid node identity in raw history")
            history = grouped.get(node, [])
            item = {"node": node, "state": "queried" if history else "empty", "coverage_timestamp": None,
                    "latest_attempt_timestamp": history[0]["timestamp"] if history else None,
                    "reason": "no_complete_requested_run" if history else "no_history", "skipped": []}
            for timestamp, run_rows in groupby(history, lambda row: row["timestamp"]):
                reader.remaining()
                run_rows = list(run_rows)
                if run_rows[0]["rank"] > request["max_runs"]:
                    raise RuntimeError(f"Coverage scan limit reached for {node}; history is not empty")
                if type(timestamp) is not int or timestamp <= 0:
                    raise ValueError("Invalid raw history timestamp")
                values = {}
                conflicting = False
                for row in run_rows:
                    outcome = (row["result"], row["image_name"], row["pytorch_version"], row["cuda_version"])
                    if row["test"] in values and values[row["test"]] != outcome:
                        conflicting = True
                    values[row["test"]] = outcome
                reason = "incomplete_status_set"
                status_tests = (required & {"storage", "nccl", "dltest"}) | {"all"}
                if conflicting:
                    reason = "conflicting_status_duplicates"
                elif status_tests.issubset(values) and all(values[test][0] in {"pass", "fail"} for test in status_tests):
                    root = Path(request["root"])
                    run_id = f"{node}-{timestamp}"
                    canonical = root / "logs/job_logs" / node / run_id / "result.json"
                    legacy = True
                    try:
                        payload = reader.json(canonical)
                        if payload.get("schema_version") not in {"cval.results", "cval.results.v2"}:
                            raise FileNotFoundError("Historical envelope lacks completion provenance")
                        legacy = False
                        result = parse_validation_result_v2(payload)
                        if (result.node, result.timestamp, result.run_id) != (node, timestamp, run_id):
                            raise ValueError("Coverage canonical run identity mismatch")
                        complete = not result.errors and result.completed_at and required.issubset(result.tests) and all(
                            result.tests[test].enabled and result.tests[test].selected and result.tests[test].phase == "finished"
                            and result.tests[test].status in {"pass", "fail"} for test in required
                        )
                        if complete and any(values[test][0] != (result.overall if test == "all" else result.tests[test].status) for test in status_tests):
                            raise ValueError("Coverage canonical outcome differs from exact status")
                        if complete:
                            for test_id in required:
                                test = result.tests[test_id]
                                expected_root = root / "logs/validation_tests" / test_id / "runs" / node / run_id
                                historical_root = root / "validation_tests" / test_id / "runs" / node / run_id
                                if Path(test.summary).parent not in {expected_root, historical_root}:
                                    raise ValueError("Coverage summary belongs to another run")
                                summary = safe_existing_evidence_path(test.summary, expected_path=test.summary,
                                                                     allowed_root=root, expect_directory=False, allow_relocated=True)
                                if not reader.read(summary).strip():
                                    raise ValueError("Coverage lacks canonical test summary")
                                if test.status == "fail" and not _failed_workload_completed(test_id, summary, reader):
                                    raise ValueError("Failed test lacks full-workload completion evidence")
                            if reader.read(canonical.parent / ".ingestion-result-digest") != (validation_result_digest(result) + "\n").encode():
                                raise ValueError("Coverage ingestion digest mismatch")
                            item.update(coverage_timestamp=timestamp, reason="complete_canonical_run", outcome=result.overall)
                            break
                        reason = "not_fully_exercised"
                    except FileNotFoundError:
                        if legacy and required <= {"storage", "nccl", "dltest"} and all(values[test][0] == "pass" for test in status_tests):
                            item.update(coverage_timestamp=timestamp, reason="legacy_complete_passing_status", outcome="pass")
                            break
                        reason = "missing_completion_provenance"
                    except (ValueError, KeyError, TypeError) as error:
                        from cval.storage.verification import EvidenceError

                        if isinstance(error, EvidenceError) and error.state in {"forbidden", "unavailable"}:
                            raise
                        reason = "conflicting_canonical_evidence: " + str(error)
                item["skipped"].append({"timestamp": timestamp, "reason": reason})
            report["nodes"].append(item)
            if item["coverage_timestamp"] is not None:
                report["timestamps"][node] = item["coverage_timestamp"]
        reader.unchanged()
        if not rows:
            report["state"] = "empty"
    except Exception as error:
        report.update(_failure(error))
        report["timestamps"] = {}
    return report


def get_coverage_report(config=None, client=None, nodes: list[str] | None = None) -> dict:
    from cval.storage.verification import collect_exact_evidence

    active = config or load_config()
    if nodes is not None and (len(set(nodes)) != len(nodes) or any(not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", node) for node in nodes)):
        raise ValueError("Coverage nodes must be unique exact node names")
    request = {"nodes": nodes, "tests": [test.id for test in active.tests.registry.enabled],
               "db_path": active.storage.validation_db_path, "root": active.runtime.validation_root,
               "max_runs": active.scheduling.coverage_max_runs_per_node,
               "timeout_seconds": active.cluster.kubectl_timeout_seconds, "sqlite_timeout_seconds": active.sqlite_retry.timeout_seconds,
               "max_bytes": active.monitoring.verification_max_bytes, "max_files": active.monitoring.verification_max_files}
    return collect_exact_evidence(request, active, client or KubectlClient(config=active), operation="coverage")


def coverage_timestamps(report: dict, config=None) -> dict[str, int]:
    if report.get("schema_version") != "cval.coverage.v1" or report.get("state") not in {"queried", "empty"}:
        raise RuntimeError(f"{report.get('state', 'unavailable')}: coverage cannot authorize planning: {report.get('reason', '')}")
    if report.get("policy") != "legacy_complete_run" or not isinstance(report.get("nodes"), list):
        raise ValueError("Invalid coverage policy or rows")
    if config is not None and report.get("requested_tests") != sorted(test.id for test in config.tests.registry.enabled):
        raise ValueError("Coverage request test set differs from current selection")
    timestamps = {}
    seen = set()
    for item in report["nodes"]:
        if not isinstance(item, dict) or not isinstance(item.get("node"), str) or not item["node"] or item["node"] in seen or item.get("state") not in {"queried", "empty"}:
            raise ValueError("Invalid or duplicate node coverage row")
        seen.add(item["node"])
        timestamp = item.get("coverage_timestamp")
        if timestamp is not None:
            if type(timestamp) is not int or timestamp <= 0:
                raise ValueError("Invalid coverage timestamp")
            timestamps[item["node"]] = timestamp
    if report.get("timestamps") != timestamps or (report["state"] == "empty" and timestamps):
        raise ValueError("Coverage timestamp projection conflicts with observations")
    return timestamps


def parse_latest_status_rows_json(output: str) -> list[LatestStatusRow]:
    """Parse JSON rows emitted by the read-only status helper."""

    data = json.loads(output)
    if not isinstance(data, list):
        raise ValueError("latest status JSON must be a list")
    rows: dict[tuple[str, str], LatestStatusRow] = {}
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("Malformed status row")
        timestamp = item.get("latest_timestamp")
        if not isinstance(item.get("node"), str) or not item["node"] or not isinstance(item.get("test"), str) or not item["test"] or type(timestamp) is not int or timestamp <= 0 or item.get("result") not in {"pass", "fail", "incomplete"}:
            raise ValueError("Invalid status identity or outcome")
        row = LatestStatusRow(item["node"], item["test"], timestamp, item["result"])
        key = (row.node, row.test)
        if key in rows and rows[key] != row:
            raise ValueError("Conflicting status rows for one requested run")
        rows[key] = row
    return sorted(rows.values(), key=lambda row: (row.node, row.test))


def latest_status_rows_to_node_map(rows: list[LatestStatusRow]) -> dict[str, int]:
    """Collapse per-test rows into node -> newest timestamp for scheduling."""

    latest_by_node: dict[str, int] = {}
    for row in rows:
        timestamp = row.latest_timestamp or 0
        latest_by_node[row.node] = max(latest_by_node.get(row.node, 0), timestamp)
    return latest_by_node


def get_latest_status_rows(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
    config=None,
    *,
    node: str | None = None,
    timestamp: int | None = None,
) -> list[LatestStatusRow]:
    """Read latest status rows from the PVC access pod using SQLite read-only mode."""

    active_config = config or load_config()
    if (node is None) != (timestamp is None):
        raise ValueError("Exact status requires both node and timestamp")
    if node is not None and (not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", node) or type(timestamp) is not int or timestamp <= 0):
        raise ValueError("Invalid exact status identity")
    pod = pod or active_config.cluster.pvc_access_pod
    namespace = namespace or active_config.cluster.namespace
    db_path = db_path or active_config.storage.validation_db_path
    kubectl = client or KubectlClient(config=active_config)
    status_pod = resolve_status_pod(kubectl, namespace, pod)
    code = f"CVAL_SQLITE_TIMEOUT = {active_config.sqlite_retry.timeout_seconds!r}\n" + sqlite_readonly_script_prelude() + r'''
import json
import sys,time

db_path,node,timestamp,limit = sys.argv[1:]
deadline = time.monotonic() + float(limit)
rows_out = []
conn = None
try:
    conn = connect_sqlite_readonly(db_path)
    conn.row_factory = sqlite3.Row
    conn.set_progress_handler(lambda: int(time.monotonic() >= deadline),1000)
    if node:
        rows = conn.execute("SELECT node,test,timestamp AS latest_timestamp,result FROM runs WHERE node=? AND timestamp=? ORDER BY test",(node,int(timestamp))).fetchall()
    else:
        rows = conn.execute("SELECT node, test, latest_timestamp, result FROM latest_status ORDER BY node, test").fetchall()
    for row in rows:
        rows_out.append(
            {
                "node": row["node"],
                "test": row["test"],
                "latest_timestamp": row["latest_timestamp"],
                "result": row["result"],
            }
        )
except Exception as exc:
    print(f"Error reading latest_status from {db_path}: {exc}", file=sys.stderr)
    sys.exit(1)
finally:
    if conn is not None:
        conn.close()

print(json.dumps(rows_out))
'''
    result = kubectl.run(
        ["exec", "-i", "-n", namespace, status_pod, "--", "python3", "-", db_path, node or "", str(timestamp or ""), str(active_config.cluster.kubectl_timeout_seconds)],
        input_text=code,
        check=False,
    )
    if result.returncode:
        state = "forbidden" if "forbidden" in result.stderr.lower() or "permission denied" in result.stderr.lower() else "unavailable"
        raise RuntimeError(f"{state}: status read failed: {result.stderr.strip()}")
    return parse_latest_status_rows_json(result.stdout)


def resolve_status_pod(kubectl: KubectlClient, namespace: str, pod: str) -> str:
    """Resolve the configured status pod or the pod created by its access job."""

    direct_candidates = (pod, f"{pod}-server-0")
    for candidate in direct_candidates:
        if _pod_is_running(kubectl, namespace, candidate):
            return candidate

    selectors = (
        f"volcano.sh/job-name={pod}",
        f"job-name={pod}",
        f"app.kubernetes.io/name={pod}",
    )
    for selector in selectors:
        selected = _running_pod_for_selector(kubectl, namespace, selector)
        if selected:
            return selected

    raise RuntimeError(
        f"Could not find a running status pod for {pod!r} in namespace {namespace!r}"
    )


def _pod_is_running(kubectl: KubectlClient, namespace: str, pod: str) -> bool:
    result = kubectl.run(
        ["get", "pod", "-n", namespace, pod, "-o", "json"],
        check=False,
    )
    if result.returncode != 0:
        if "(NotFound)" in result.stderr:
            return False
        state = "forbidden" if "forbidden" in result.stderr.lower() else "unavailable"
        raise RuntimeError(f"{state}: cannot inspect PVC reader: {result.stderr.strip()}")
    payload = json.loads(result.stdout or "{}")
    return payload.get("status", {}).get("phase") == "Running"


def _running_pod_for_selector(
    kubectl: KubectlClient,
    namespace: str,
    selector: str,
) -> str:
    result = kubectl.run(
        ["get", "pods", "-n", namespace, "-l", selector, "-o", "json"],
        check=False,
    )
    if result.returncode != 0:
        state = "forbidden" if "forbidden" in result.stderr.lower() else "unavailable"
        raise RuntimeError(f"{state}: cannot resolve PVC reader: {result.stderr.strip()}")
    payload = json.loads(result.stdout or "{}")
    for item in payload.get("items", []):
        if item.get("status", {}).get("phase") == "Running":
            return str(item.get("metadata", {}).get("name", ""))
    return ""
