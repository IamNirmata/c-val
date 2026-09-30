"""Read and format c-val latest validation status.

Status reads are intentionally opened through SQLite `mode=ro` so operators and
agents can inspect validation history without mutating DB tables or views.
"""

from __future__ import annotations

import json
import re
import hashlib
import csv
import io
import time
import runpy
from datetime import datetime, timezone
from itertools import groupby
from pathlib import Path
from zoneinfo import ZoneInfo

from cval.config import load_config, load_config_snapshot
from cval.k8s.client import KubectlClient
from cval.models import LatestStatusRow
from cval.storage.sqlite_uri import sqlite_readonly_script_prelude


NODE_STATUS_FIELDS = (
    "node_name", "node_uid", "latest_record_epoch", "latest_record_la", "coverage_state", "priority_rank",
    "job_name", "job_uid", "job_run_epoch", "job_phase", "recording_state", "test_outcome",
    "cooldown_until_epoch", "k8s_ready", "scheduling_enabled", "resources_available", "blocked_reason",
    "observed_at_epoch", "source_sha", "config_digest", "observation_state", "snapshot_id",
)


def collect_status_observation(config, client, state: dict, previous: list[dict]) -> dict:
    """Read shared node/pod inventories and recording evidence without any writes."""

    from cval.jobs.manager import intent_finished
    from cval.k8s.discovery import _nodes_by_name, describe_nodes_from_outputs, parse_gpu_quantity

    observed_at = int(time.time())
    observations = []
    try:
        inventory = _nodes_by_name(client.get_nodes_json())
        nodes = {name: node for name, node in inventory.items()
                 if (not config.cluster.node_filter or config.cluster.node_filter in name)
                 and parse_gpu_quantity(node.get("status", {}).get("capacity", {}).get(config.job_template.gpu_resource_name, 0)) > 0}
    except (RuntimeError, ValueError, OSError) as error:
        unavailable = "forbidden" if "forbidden" in str(error).lower() else "unavailable"
        for row in previous:
            observations.append({**row, "observation_state": unavailable, "k8s_ready": "unknown",
                                 "scheduling_enabled": "unknown", "resources_available": "unknown", "blocked_reason": str(error)[:400]})
        return {"observed_at_epoch": observed_at, "nodes": observations, "coverage": {"state": unavailable}, "inventory_state": unavailable}
    resource_state, resource_reason = "queried", ""
    try:
        statuses = {status.name: status for status in describe_nodes_from_outputs(
            client.get_pods_json(), {"items": list(nodes.values())}, config, skip_unverified=True,
        )}
    except (RuntimeError, ValueError, OSError) as error:
        statuses = {}
        resource_state = "forbidden" if "forbidden" in str(error).lower() else "unavailable"
        resource_reason = str(error)[:400]
    helper = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/cval-node-cooldown.py"))
    cooldowns = helper["load_state"](Path(config.live.log_dir) / "node_cool_down.csv")
    for name, node in sorted(nodes.items()):
        metadata = node["metadata"]
        status = statuses.get(name)
        readiness = [condition.get("status") for condition in node.get("status", {}).get("conditions", []) if condition.get("type") == "Ready"]
        cordoned = node.get("spec", {}).get("unschedulable", False)
        created = metadata.get("creationTimestamp")
        observations.append({
            "node_name": name, "node_uid": metadata.get("uid", ""),
            "node_created_at_epoch": int(datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()) if created else 0,
            "k8s_ready": "true" if readiness == ["True"] else "false" if readiness == ["False"] else "unknown",
            "scheduling_enabled": str(not cordoned).lower() if type(cordoned) is bool else "unknown",
            "resources_available": str(status.resource_ready and status.used == 0).lower() if status else "unknown",
            "blocked_reason": status.reason if status and not status.eligible else "" if status else resource_reason or "eligibility unverified",
            "observed_at_epoch": observed_at,
            "cooldown_until_epoch": cooldowns[name] + config.scheduling.node_cooldown_seconds if name in cooldowns else "",
            "observation_state": resource_state if resource_state != "queried" else "queried" if status and metadata.get("uid") else "unverified",
        })
    attempts = {}
    for intent in sorted(state.get("intents", {}).values(), key=lambda item: (intent_finished(item), -item["timestamp"])):
        attempts.setdefault(intent["node_name"], intent["timestamp"])
    coverage = get_coverage_report(config, client, nodes=sorted(nodes), include_recording=True, attempts=attempts)
    return {"observed_at_epoch": observed_at, "collected_at_epoch": int(time.time()),
            "nodes": observations, "coverage": coverage, "inventory_state": "queried"}


def refresh_status_csvs(session_dir: Path, *, client=None) -> dict:
    """Collect outside the lock, then publish against the latest durable intents."""

    from cval.jobs.manager import admission_lock, atomic_session_text, check_admission_cluster, read_admission_session, read_session_state

    state = read_session_state(session_dir)
    config = load_config_snapshot(state["config_snapshot_b64"])
    if not config.live.status_dir:
        return {"state": "disabled_historical_snapshot"}
    client = client or KubectlClient(config=config)
    check_admission_cluster(config, client)
    with admission_lock(config, timeout_seconds=min(5, config.live.status_refresh_seconds)):
        active, state = read_admission_session(config)
        if active != session_dir:
            raise ValueError("Status refresh does not own the active session")
        previous, _cluster = read_status_csvs(Path(config.live.status_dir))
        check_status_reconciliation(previous, state)
    snapshot = collect_status_observation(config, client, state, previous)
    with admission_lock(config, timeout_seconds=min(5, config.live.status_refresh_seconds)):
        active, current = read_admission_session(config)
        if active != session_dir or current["config_snapshot_b64"] != state["config_snapshot_b64"]:
            raise ValueError("Active session changed during status collection")
        atomic_session_text(session_dir / "status-observation.json", json.dumps(snapshot, sort_keys=True) + "\n")
        project_status_csvs(session_dir, current)
        return {"state": snapshot["inventory_state"], "coverage_state": snapshot["coverage"]["state"],
            "coverage_reason": snapshot["coverage"].get("reason", ""), "node_count": len(snapshot["nodes"]),
            "observed_at_epoch": snapshot["observed_at_epoch"],
            "collected_at_epoch": snapshot.get("collected_at_epoch", snapshot["observed_at_epoch"])}


def read_status_csvs(directory: Path) -> tuple[list[dict], dict]:
    """Read one coherent generation; callers serialize with the admission lock."""

    paths = [directory / name for name in ("node-status.csv", "cluster-status.csv")]
    if not any(path.exists() or path.is_symlink() for path in paths):
        return [], {}
    if any(path.is_symlink() or not path.is_file() for path in paths):
        raise ValueError("Status CSV pair is missing or not regular; reconcile retained session evidence")
    with paths[0].open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != list(NODE_STATUS_FIELDS):
            raise ValueError("Unexpected node status CSV columns")
        nodes = list(reader)
    with paths[1].open(newline="", encoding="utf-8") as handle:
        clusters = list(csv.DictReader(handle))
    if len(clusters) != 1 or not clusters[0].get("snapshot_id") or any(row.get("snapshot_id") != clusters[0]["snapshot_id"] for row in nodes):
        raise ValueError("Status CSV snapshot IDs disagree; reconcile retained session evidence")
    if len({row["node_name"] for row in nodes}) != len(nodes):
        raise ValueError("Duplicate node status identity")
    return nodes, clusters[0]


def check_status_reconciliation(rows: list[dict], state: dict) -> None:
    """Never adopt an unresolved CSV attempt without its authoritative journal."""

    for row in rows:
        name = row["job_name"]
        if not name:
            continue
        intent = state.get("intents", {}).get(name)
        if intent is None:
            if row["job_phase"] not in {"Completed", "Succeeded", "Failed", "Aborted", "Terminated", "none"}:
                raise ValueError(f"Unresolved status attempt {name!r} lacks its retained submission journal")
            continue
        if (row["node_name"], row["job_run_epoch"], row["source_sha"], row["config_digest"]) != (
            intent["node_name"], str(intent["timestamp"]), intent["git_ref"], intent["config_digest"],
        ):
            raise ValueError(f"Status attempt {name!r} conflicts with its retained submission journal")
        uids = {event["uid"] for event in intent["events"] if event.get("uid")}
        if row["job_uid"] and uids and uids != {row["job_uid"]}:
            raise ValueError(f"Status Job UID conflicts with retained receipts for {name!r}")


def project_status_csvs(session_dir: Path, state: dict) -> None:
    """Publish both summaries under the caller's already-held admission lock."""

    from cval.jobs.manager import atomic_session_text

    if not state.get("config_snapshot_b64"):
        return
    config = load_config_snapshot(state["config_snapshot_b64"])
    if not config.live.status_dir:
        return
    directory = Path(config.live.status_dir)
    if directory.resolve() != directory:
        raise ValueError("Status directory must not contain symlinks")
    previous, _cluster = read_status_csvs(directory)
    check_status_reconciliation(previous, state)
    cache_path = session_dir / "status-observation.json"
    snapshot = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    now = int(time.time())
    coverage = snapshot.get("coverage", {"state": "unavailable"})
    observations = {item["node_name"]: dict(item) for item in snapshot.get("nodes", [])}
    collected_at = snapshot.get("collected_at_epoch", snapshot.get("observed_at_epoch", 0))
    if now - collected_at > config.live.status_refresh_seconds:
        coverage = {"state": "stale"}
        for observation in observations.values():
            observation["observation_state"] = "stale"
    if not observations:
        for row in previous:
            observations[row["node_name"]] = {**row, "observation_state": "stale"}
    for intent in state.get("intents", {}).values():
        observations.setdefault(intent["node_name"], {"node_name": intent["node_name"], "observation_state": "unavailable"})
    rows, cluster = build_status_snapshot(
        list(observations.values()), coverage, state, config, now=now,
        previous=previous, inventory_state=snapshot.get("inventory_state", "unavailable"),
    )
    directory.mkdir(parents=True, exist_ok=True)
    for filename, entries, fields in (("node-status.csv", rows, NODE_STATUS_FIELDS), ("cluster-status.csv", [cluster], tuple(cluster))):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(entries)
        atomic_session_text(directory / filename, output.getvalue())


def build_status_snapshot(observations: list[dict], coverage: dict, state: dict, config, *, now: int,
                          previous: list[dict] | None = None, inventory_state: str = "queried") -> tuple[list[dict], dict]:
    """Project node and cluster CSV rows without making them admission authority."""

    from cval.jobs.manager import intent_finished
    from cval.scheduler.priority import build_priority_queue

    try:
        timestamps = coverage_timestamps(coverage, config)
        coverage_available = True
    except (RuntimeError, ValueError):
        timestamps, coverage_available = {}, False
    history = {item["node"]: item for item in coverage.get("nodes", [])}
    previous_by_name = {row["node_name"]: row for row in previous or []}
    node_rows = []
    for observation in sorted(observations, key=lambda item: item["node_name"]):
        name = observation["node_name"]
        recorded = history.get(name, {}) if coverage_available else {}
        created = observation.get("node_created_at_epoch", 0)
        timestamp = timestamps.get(name)
        replaced = observation.get("identity_changed", False)
        if replaced or (timestamp and timestamp < created):
            timestamp = None
        coverage_state = "unverified"
        if coverage_available and name in history and not replaced:
            coverage_state = "missing" if timestamp is None else (
                "fresh" if 0 <= now - timestamp <= config.scheduling.days_threshold * 86400 else "expired"
            )
            if timestamp is not None and timestamp > now:
                coverage_state = "unverified"
        latest_record = recorded.get("latest_record_timestamp")
        if latest_record is None and recorded.get("reason") == "complete_canonical_run":
            latest_record = timestamp
        if replaced or (latest_record and latest_record < created):
            latest_record = None
        attempts = sorted(
            (intent for intent in state.get("intents", {}).values() if intent["node_name"] == name),
            key=lambda intent: (intent_finished(intent), -intent["timestamp"]),
        )
        intent = attempts[0] if attempts else None
        row = {
            "node_name": name, "node_uid": observation.get("node_uid", ""),
            "latest_record_epoch": latest_record or "",
            "latest_record_la": datetime.fromtimestamp(latest_record, ZoneInfo("America/Los_Angeles")).isoformat() if latest_record else "",
            "coverage_state": coverage_state, "priority_rank": "",
            "job_name": "", "job_uid": "", "job_run_epoch": "", "job_phase": "none",
            "recording_state": recorded.get("recording_state", "unverified"),
            "test_outcome": recorded.get("test_outcome", "unknown"),
            "cooldown_until_epoch": observation.get("cooldown_until_epoch", ""),
            "k8s_ready": observation.get("k8s_ready", "unknown"),
            "scheduling_enabled": observation.get("scheduling_enabled", "unknown"),
            "resources_available": observation.get("resources_available", "unknown"),
            "blocked_reason": observation.get("blocked_reason", ""),
            "observed_at_epoch": observation.get("observed_at_epoch", ""),
            "source_sha": recorded.get("source_sha", ""), "config_digest": recorded.get("config_digest", ""),
            "observation_state": observation.get("observation_state", "queried"),
        }
        if not coverage_available or name not in history:
            source_state = coverage.get("state", "unavailable")
            source_state = source_state if source_state not in {"queried", "empty"} else "unverified"
            if row["observation_state"] == "queried":
                row["observation_state"] = source_state
            row["blocked_reason"] = "; ".join(filter(None, (row["blocked_reason"], f"coverage {source_state}")))
        if intent:
            events = intent["events"]
            event = events[-1]
            phase = event.get("phase", "prepared" if event["state"] == "prepared" else "unknown")
            phase = {"Missing": "missing", "Unknown": "unknown"}.get(phase, phase)
            if event["state"] == "not_created":
                phase = "none"
            row.update(job_name=intent["job_name"], job_run_epoch=intent["timestamp"], job_phase=phase,
                       source_sha=intent["git_ref"], config_digest=intent["config_digest"])
            for receipt in events:
                if receipt.get("uid"):
                    row["job_uid"] = receipt["uid"]
                elif receipt.get("stdout", "").strip().startswith("{"):
                    try:
                        receipt_uid = json.loads(receipt["stdout"]).get("metadata", {}).get("uid")
                    except (ValueError, AttributeError):
                        receipt_uid = None
                    if receipt_uid:
                        row["job_uid"] = receipt_uid
                if receipt.get("created_at"):
                    submitted_at = int(datetime.fromisoformat(receipt["created_at"].replace("Z", "+00:00")).timestamp())
                    row["cooldown_until_epoch"] = max(int(row["cooldown_until_epoch"] or 0), submitted_at + config.scheduling.node_cooldown_seconds)
            if recorded.get("latest_attempt_timestamp") != intent["timestamp"]:
                row.update(recording_state="no_results" if event["state"] == "prepared" else "unverified", test_outcome="unknown")
            runtime = event.get("runtime", {})
            if runtime.get("tests_outcome") in {"pass", "fail", "incomplete"}:
                row["test_outcome"] = runtime["tests_outcome"]
            verification = event.get("verification", {})
            if verification.get("complete") is True and verification.get("identity_verified") is True:
                row["recording_state"] = "fully_recorded"
            if len([attempt for attempt in attempts if not intent_finished(attempt)]) > 1:
                row["blocked_reason"] = "multiple unresolved attempts; see retained submission journal"
                row["observation_state"] = "conflicting"
        if replaced or (intent and intent["timestamp"] < created):
            row.update(recording_state="unverified", test_outcome="unknown", observation_state="unverified",
                       blocked_reason="node UID changed; retained attempt belongs to the previous node identity")
        old = previous_by_name.get(name, {})
        same_node = not old.get("node_uid") or row["node_uid"] == old["node_uid"]
        if same_node and row["coverage_state"] == "unverified" and not row["latest_record_epoch"] and old.get("latest_record_epoch"):
            row["latest_record_epoch"] = int(old["latest_record_epoch"])
            row["latest_record_la"] = old["latest_record_la"]
        if same_node and not row["job_name"] and old.get("job_name"):
            for field in ("job_name", "job_uid", "job_run_epoch", "job_phase", "source_sha", "config_digest", "cooldown_until_epoch"):
                row[field] = old[field]
        node_rows.append(row)
    known_rows = {row["node_name"]: row for row in node_rows if row["coverage_state"] in {"missing", "expired"}}
    ranked = build_priority_queue(
        sorted(known_rows), {name: timestamps[name] for name, row in known_rows.items() if row["coverage_state"] == "expired"},
        config.scheduling.days_threshold, now=datetime.fromtimestamp(now, timezone.utc),
    )
    for candidate in ranked:
        known_rows[candidate.node]["priority_rank"] = candidate.priority
    cluster = {
        "total_hgx_nodes": len(node_rows),
        "complete_recorded_24h_nodes": sum(bool(row["latest_record_epoch"]) and 0 <= now - row["latest_record_epoch"] <= 86400 for row in node_rows),
        **{f"coverage_{label}": sum(row["coverage_state"] == label for row in node_rows) for label in ("fresh", "expired", "missing", "unverified")},
        "ready_nodes": sum(row["k8s_ready"] == "true" for row in node_rows),
        "not_ready_nodes": sum(row["k8s_ready"] == "false" for row in node_rows),
        "readiness_unknown_nodes": sum(row["k8s_ready"] == "unknown" for row in node_rows),
        "scheduling_disabled_nodes": sum(row["scheduling_enabled"] == "false" for row in node_rows),
        "generated_at_epoch": now,
        "observation_complete": inventory_state == "queried" and coverage_available and all(row["observation_state"] == "queried" for row in node_rows),
    }
    snapshot_id = hashlib.sha256(json.dumps([node_rows, cluster | {"inventory_state": inventory_state}], sort_keys=True).encode()).hexdigest()
    for row in [*node_rows, cluster]:
        row["snapshot_id"] = snapshot_id
    cluster["inventory_state"] = inventory_state
    return node_rows, cluster


def _read_recording_state(reader, node: str, timestamp: int, rows: list[dict]) -> dict:
    from cval.validation.results import COMPACT_RESULT_SCHEMA, parse_validation_result_v2, validation_result_digest, recording_complete

    result_path = Path(reader.request["root"]) / "logs/job_logs" / node / f"{node}-{timestamp}" / "result.json"
    status = {"recording_state": "unverified", "test_outcome": "unknown"}
    try:
        payload = reader.json(result_path)
    except FileNotFoundError:
        status["recording_state"] = "unverified" if rows else "no_results"
        return status
    if payload.get("schema_version") not in {"cval.results", "cval.results.v2", COMPACT_RESULT_SCHEMA}:
        return status
    result = parse_validation_result_v2(payload)
    if (result.node, result.timestamp, result.run_id) != (node, timestamp, f"{node}-{timestamp}"):
        raise ValueError("Recorded result identity differs from the requested node and run")
    status.update(test_outcome=result.overall, source_sha=result.git_ref, config_digest=result.global_config_digest)
    status["recording_state"] = "partial_db_update" if rows else "results_without_db_update"
    if not result.completed_at:
        return status
    expected = {"all": result.overall} | {
        name: test.status for name, test in result.tests.items() if name in {"storage", "nccl", "dltest"}
    }
    actual = {}
    for row in rows:
        if row["test"] in actual and actual[row["test"]] != row["result"]:
            raise ValueError("Conflicting exact status rows")
        actual[row["test"]] = row["result"]
    if any(actual.get(name) != value for name, value in expected.items()):
        return status
    if result.schema_version == COMPACT_RESULT_SCHEMA:
        if recording_complete(result):
            status.update(recording_state="fully_recorded", latest_record_timestamp=timestamp)
        return status
    try:
        receipt = reader.read(result_path.parent / ".ingestion-result-digest")
    except FileNotFoundError:
        return status
    if receipt != (validation_result_digest(result) + "\n").encode():
        raise ValueError("Recorded ingestion acknowledgement differs from canonical result")
    status.update(recording_state="fully_recorded", latest_record_timestamp=timestamp)
    return status


def read_local_coverage(request: dict) -> dict:
    """Choose one complete passing run; failed attempts never refresh successful coverage."""

    from cval.storage.verification import EvidenceReader, _failure
    from cval.storage.paths import safe_existing_evidence_path
    from cval.validation.results import COMPACT_RESULT_SCHEMA, parse_validation_result_v2, validation_result_digest, recording_complete

    reader = EvidenceReader(request)
    report = {"schema_version": "cval.coverage.v1", "state": "queried", "policy": "successful_complete_run",
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
                    "reason": "no_successful_requested_run" if history else "no_history", "skipped": []}
            if request.get("include_recording"):
                attempt = request.get("attempts", {}).get(node, item["latest_attempt_timestamp"])
                item.update(latest_attempt_timestamp=attempt, recording_state="no_results", test_outcome="unknown")
                recordings = {}
                if attempt is not None:
                    recordings[attempt] = _read_recording_state(reader, node, attempt, [row for row in history if row["timestamp"] == attempt])
                    item.update(recordings[attempt])
                for recorded_timestamp, recorded_rows in groupby(history, lambda row: row["timestamp"]):
                    if item.get("latest_record_timestamp") is not None:
                        break
                    recording = recordings.get(recorded_timestamp)
                    if recording is None:
                        recording = _read_recording_state(reader, node, recorded_timestamp, list(recorded_rows))
                    if recording["recording_state"] == "fully_recorded":
                        item["latest_record_timestamp"] = recorded_timestamp
                    elif recording["recording_state"] == "unverified":
                        break
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
                elif status_tests.issubset(values) and any(values[test][0] == "fail" for test in status_tests):
                    reason = "unsuccessful_status_set"
                elif status_tests.issubset(values) and all(values[test][0] == "pass" for test in status_tests):
                    root = Path(request["root"])
                    run_id = f"{node}-{timestamp}"
                    canonical = root / "logs/job_logs" / node / run_id / "result.json"
                    legacy = not canonical.parent.exists()
                    try:
                        payload = reader.json(canonical)
                        if payload.get("schema_version") == "cval.results.v1":
                            legacy = True
                        if payload.get("schema_version") not in {"cval.results", "cval.results.v2", COMPACT_RESULT_SCHEMA}:
                            raise FileNotFoundError("Historical envelope lacks completion provenance")
                        legacy = False
                        result = parse_validation_result_v2(payload)
                        if (result.node, result.timestamp, result.run_id) != (node, timestamp, run_id):
                            raise ValueError("Coverage canonical run identity mismatch")
                        complete = not result.errors and result.completed_at and result.overall == "pass" and required.issubset(result.tests) and all(
                            result.tests[test].enabled and result.tests[test].selected and result.tests[test].phase == "finished"
                            and result.tests[test].status == "pass" for test in required
                        )
                        if complete and any(values[test][0] != (result.overall if test == "all" else result.tests[test].status) for test in status_tests):
                            raise ValueError("Coverage canonical outcome differs from exact status")
                        if complete:
                            for test_id in required:
                                test = result.tests[test_id]
                                expected_root = root / "logs/validation_tests" / test_id / "runs" / node / run_id
                                if result.schema_version == COMPACT_RESULT_SCHEMA:
                                    expected_root = root / "logs" / test_id / node / run_id
                                historical_root = root / "validation_tests" / test_id / "runs" / node / run_id
                                if Path(test.summary).parent not in {expected_root, historical_root}:
                                    raise ValueError("Coverage summary belongs to another run")
                                summary = safe_existing_evidence_path(test.summary, expected_path=test.summary,
                                                                     allowed_root=root, expect_directory=False, allow_relocated=True)
                                if not reader.read(summary).strip():
                                    raise ValueError("Coverage lacks canonical test summary")
                            if result.schema_version == COMPACT_RESULT_SCHEMA:
                                if not recording_complete(result):
                                    raise ValueError("Coverage lacks complete final ingestion/log evidence")
                            elif reader.read(canonical.parent / ".ingestion-result-digest") != (validation_result_digest(result) + "\n").encode():
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


def get_coverage_report(config=None, client=None, nodes: list[str] | None = None, *, include_recording: bool = False, attempts: dict[str, int] | None = None) -> dict:
    from cval.storage.verification import collect_exact_evidence

    active = config or load_config()
    if nodes is not None and (len(set(nodes)) != len(nodes) or any(not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", node) for node in nodes)):
        raise ValueError("Coverage nodes must be unique exact node names")
    request = {"nodes": nodes, "tests": [test.id for test in active.tests.registry.enabled],
               "db_path": active.storage.validation_db_path, "root": active.runtime.validation_root,
               "max_runs": active.scheduling.coverage_max_runs_per_node,
               "timeout_seconds": active.cluster.kubectl_timeout_seconds, "sqlite_timeout_seconds": active.sqlite_retry.timeout_seconds,
               "max_bytes": active.monitoring.verification_max_bytes, "max_files": active.monitoring.verification_max_files}
    if include_recording:
        if attempts and any(not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", node) or type(timestamp) is not int or timestamp <= 0 for node, timestamp in attempts.items()):
            raise ValueError("Invalid tracked recording identity")
        request.update(include_recording=True, attempts=attempts or {})
    return collect_exact_evidence(request, active, client or KubectlClient(config=active), operation="coverage")


def coverage_timestamps(report: dict, config=None) -> dict[str, int]:
    if report.get("schema_version") != "cval.coverage.v1" or report.get("state") not in {"queried", "empty"}:
        raise RuntimeError(f"{report.get('state', 'unavailable')}: coverage cannot authorize planning: {report.get('reason', '')}")
    if report.get("policy") != "successful_complete_run" or not isinstance(report.get("nodes"), list):
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
            if type(timestamp) is not int or timestamp <= 0 or item.get("outcome") != "pass":
                raise ValueError("Invalid successful coverage timestamp or outcome")
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
