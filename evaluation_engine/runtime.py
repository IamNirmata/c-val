"""Bounded raw readers and derived publication primitives for test scripts."""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from collections import defaultdict
from contextlib import closing
from pathlib import Path

from cval.storage.paths import safe_existing_evidence_path
from cval.storage.retry import retry_sqlite
from cval.storage.sqlite_uri import canonical_sqlite_path, connect_sqlite_file
from evaluation_engine.config import digest
from evaluation_engine.database import Database
from evaluation_engine.statistics import build_rule, classify


class EvidenceError(ValueError):
    pass


class Context:
    def __init__(self, settings, revision, now=None):
        self.settings = settings
        self.revision = revision
        self.now = int(time.time()) if now is None else now
        self.database = Database(settings)

    def read(self, filename, sql, parameters=()):
        path = canonical_sqlite_path(filename, must_exist=True)
        if self.settings.shared_raw_storage:
            with path.open("rb") as handle:
                header = handle.read(20)
            if header[18:20] == b"\x02\x02":
                raise EvidenceError("shared_storage_WAL_requires_quiesced_migration_or_snapshot")
        def operation():
            with closing(connect_sqlite_file(filename, mode="ro", timeout=self.settings.retry.timeout_seconds)) as connection:
                connection.execute("PRAGMA query_only=ON")
                connection.row_factory = sqlite3.Row
                deadline = time.monotonic() + self.settings.query_timeout_seconds
                connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
                rows = connection.execute(sql, parameters).fetchmany(self.settings.max_metric_rows_per_run + 1)
                if len(rows) > self.settings.max_metric_rows_per_run:
                    raise EvidenceError("row_limit_exceeded")
                return [dict(row) for row in rows]
        return retry_sqlite(operation, self.settings.retry)

    def json_file(self, path):
        root = Path(self.settings.raw.runtime.validation_root)
        path = safe_existing_evidence_path(path, expected_path=path, allowed_root=root, expect_directory=False, description="evaluation evidence")
        if path.stat().st_size > 8 * 1024 * 1024:
            raise EvidenceError("oversized_provenance")
        return json.loads(path.read_text(encoding="utf-8"))

    def candidates(self):
        rows = self.read(self.settings.raw.storage.validation_db_path, "SELECT rowid,node,timestamp,test,result,image_name,pytorch_version,cuda_version FROM runs ORDER BY rowid")
        groups = {}
        for row in rows:
            key = row["node"], row["timestamp"]
            run = groups.setdefault(key, {"node": key[0], "timestamp": key[1], "rows": {}, "conflict": False, "sequence": row["rowid"]})
            identity = {field: row[field] for field in row if field != "rowid"}
            previous = run["rows"].get(row["test"])
            run["conflict"] |= previous is not None and previous != identity
            run["rows"][row["test"]] = identity
        return sorted(groups.values(), key=lambda run: run["sequence"])

    def provenance(self, run, test):
        if run["conflict"] or set(run["rows"]) != {"storage", "nccl", "dltest", "all"}:
            raise EvidenceError("incomplete_or_conflicting_status_set")
        node, timestamp = run["node"], run["timestamp"]
        if not isinstance(node, str) or re.fullmatch(r"[a-z0-9][a-z0-9.-]*", node) is None:
            raise EvidenceError("invalid_node_identity")
        run_id = f"{node}-{timestamp}"
        root = Path(self.settings.raw.runtime.validation_root)
        result = self.json_file(root / "logs/job_logs" / node / run_id / "result.json")
        if result.get("node") != node or int(result.get("timestamp", -1)) != timestamp or result.get("run_id") != run_id:
            raise EvidenceError("provenance_identity_mismatch")
        for name, row in run["rows"].items():
            status = result.get("overall") if name == "all" else result.get("tests", {}).get(name, {}).get("status")
            if row["result"] != status:
                raise EvidenceError("raw_status_provenance_mismatch")
        row = run["rows"][test.name]
        for field in ("image_name", "pytorch_version", "cuda_version"):
            if not row[field] or row[field] != result.get(field):
                raise EvidenceError("missing_or_mismatched_environment")
        test_result = result["tests"][test.name]
        cohort = {field: row[field] for field in ("image_name", "pytorch_version", "cuda_version")}
        cohort |= {"test": test.name, "config_digest": test_result.get("config_digest"), "raw_root": str(root)}
        if not cohort["config_digest"]:
            raise EvidenceError("missing_test_config_digest")
        if test.settings["require_hardware"] and row["result"] == "pass":
            summary = self.json_file(root / "validation_tests/dltest/runs" / node / run_id / "summary.json")
            models = set()
            for rank in summary.get("rank_results", []):
                match = re.search(r"_(NVIDIA-[^_]+)_CUDA-", rank.get("run_id", ""))
                if not match:
                    raise EvidenceError("missing_gpu_model_provenance")
                models.add(match.group(1))
            if len(models) != 1 or not summary.get("rank_coverage_valid") or summary.get("gpu_count") != test.settings["expected_ranks"]:
                raise EvidenceError("hardware_or_rank_coverage_mismatch")
            cohort |= {"gpu_model": models.pop(), "gpu_count": summary["gpu_count"]}
        return result, cohort

    def dl_generation(self):
        generations = []
        for field in ("dl_numerical_db_path", "dl_compute_db_path", "dl_collective_db_path", "dl_overlap_db_path"):
            rows = self.read(getattr(self.settings.raw.storage, field), "SELECT generation_id,state FROM cval_ingest_metadata WHERE id=1")
            if len(rows) != 1 or rows[0]["state"] != "complete":
                raise EvidenceError("dl_generation_incomplete")
            generations.append(rows[0]["generation_id"])
        if len(set(generations)) != 1:
            raise EvidenceError("dl_generation_mismatch")
        return generations[0]

    def observation(self, component, plan, group, task, metric, rank, value, direction):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            value = None
        return {"key": json.dumps([component, plan, group, task, metric, rank], separators=(",", ":")), "rank": rank, "value": value, "direction": direction}

    def read_dl(self, run, component, storage_field, directions, expected_ranks):
        rows = self.read(getattr(self.settings.raw.storage, storage_field), f'SELECT run_key,rank,test_plan,iterations,task_group,task_name,metric_name,metric_value,status FROM "{component}" WHERE node=? AND cval_timestamp=?', (run["node"], run["timestamp"]))
        if not rows or len({row["run_key"] for row in rows}) != 1:
            raise EvidenceError("missing_or_ambiguous_dl_run")
        receipt = self.read(getattr(self.settings.raw.storage, storage_field), "SELECT run_key FROM cval_ingested_runs WHERE run_key=?", (rows[0]["run_key"],))
        if len(receipt) != 1 or any(row["status"] != "completed" for row in rows):
            raise EvidenceError("incomplete_dl_receipt_or_tasks")
        coverage = defaultdict(set)
        observations = []
        for row in rows:
            identity = row["test_plan"], row["iterations"], row["task_group"], row["task_name"], row["metric_name"]
            coverage[identity].add(row["rank"])
            if row["metric_name"] in directions:
                observations.append(self.observation(component, row["test_plan"], row["task_group"], row["task_name"], row["metric_name"], row["rank"], row["metric_value"], directions[row["metric_name"]]))
        if any(ranks != set(range(expected_ranks)) for ranks in coverage.values()) or not observations:
            raise EvidenceError("dl_metric_rank_coverage_mismatch")
        return observations, sorted({(row["test_plan"], row["iterations"]) for row in rows})

    def read_wide(self, run, component, storage_field, table, node_column, directions):
        rows = self.read(getattr(self.settings.raw.storage, storage_field), f'SELECT * FROM "{table}" WHERE "{node_column}"=? AND timestamp=?', (run["node"], run["timestamp"]))
        if len(rows) != 1:
            raise EvidenceError("missing_or_ambiguous_metric_row")
        row = rows[0]
        return [self.observation(component, "", "", "", name, -1, row.get(name), direction) for name, direction in directions.items()]

    def snapshot(self, run, test):
        result, cohort = self.provenance(run, test)
        raw_status = run["rows"][test.name]["result"]
        enabled = result["tests"][test.name].get("enabled", True)
        observations = []
        if raw_status == "pass" and enabled:
            observations, metadata = test.baseline.read(self, run, test)
            cohort["metric_context"] = metadata
        keys = [item["key"] for item in observations]
        if len(keys) != len(set(keys)):
            raise EvidenceError("duplicate_metric_identity")
        return {"run": run, "cohort": cohort, "cohort_key": digest(cohort), "observations": observations, "raw_status": raw_status, "enabled": enabled, "source_digest": digest([result, observations, cohort])}

    def build_baselines(self, test, runs):
        policy = test.settings
        cutoff = self.now - policy["holdout_seconds"]
        start = cutoff - policy["window_seconds"]
        training = [run for run in runs if start <= run["timestamp"] < cutoff]
        cohorts = {}
        errors = defaultdict(int)
        for run in sorted(training, key=lambda item: item["timestamp"], reverse=True)[:self.settings.max_training_runs]:
            try:
                snapshot = self.snapshot(run, test)
                if snapshot["raw_status"] != "pass" or not snapshot["enabled"]:
                    continue
                cohort = cohorts.setdefault(snapshot["cohort_key"], {"metadata": snapshot["cohort"], "nodes": {}, "metrics": defaultdict(list), "directions": {}})
                if run["node"] in cohort["nodes"]:
                    continue
                cohort["nodes"][run["node"]] = (run["node"], run["timestamp"], snapshot["source_digest"])
                for item in snapshot["observations"]:
                    if item["value"] is not None:
                        cohort["metrics"][item["key"]].append(item["value"])
                    cohort["directions"][item["key"]] = item["direction"]
            except (EvidenceError, FileNotFoundError, ValueError) as exc:
                errors[str(exc)] += 1
        published = 0
        for cohort_key, cohort in cohorts.items():
            old = self.database.read("SELECT built_at,policy_digest,evaluator_sha,expires_at FROM baseline_version WHERE test=? AND cohort_key=? AND active=1", (test.name, cohort_key))
            if old and old[0][1] == test.policy_digest and old[0][2] == self.revision and old[0][3] > self.now and self.now - old[0][0] < policy["refresh_seconds"]:
                continue
            metrics = cohort["metrics"]
            directions = cohort["directions"]
            thresholds = {key: build_rule(metrics[key], direction, policy) for key, direction in directions.items()}
            if not thresholds or any(rule is None for rule in thresholds.values()):
                errors["insufficient_baseline_coverage"] += 1
                continue
            manifest = sorted(cohort["nodes"].values())
            baseline_id = digest([test.name, cohort_key, test.policy_digest, self.revision, cutoff, manifest])

            def publish(connection):
                connection.execute("UPDATE baseline_version SET active=0 WHERE test=? AND cohort_key=?", (test.name, cohort_key))
                connection.execute("INSERT INTO baseline_version VALUES (?,?,?,?,?,?,?,?,?,?,?,1) ON CONFLICT(baseline_id) DO UPDATE SET active=1", (baseline_id, test.name, cohort_key, test.policy_digest, self.revision, json.dumps(cohort["metadata"], sort_keys=True), json.dumps(policy, sort_keys=True), json.dumps(manifest), self.now, cutoff, self.now + policy["expires_seconds"]))
                connection.executemany("INSERT OR IGNORE INTO threshold VALUES (?,?,?)", [(baseline_id, key, json.dumps(rule, sort_keys=True)) for key, rule in thresholds.items()])
            self.database.transaction(publish)
            published += 1
        return {"published": published, "excluded": dict(errors), "training_candidates": len(training), "bounded": len(training) > self.settings.max_training_runs}

    def classify_run(self, run, test):
        baseline_id = None
        thresholds = {}
        rows = []
        expected = 0
        try:
            snapshot = self.snapshot(run, test)
            raw_status = snapshot["raw_status"]
            source_digest = snapshot["source_digest"]
            candidates = self.database.read("SELECT baseline_id,cutoff FROM baseline_version WHERE test=? AND cohort_key=? AND policy_digest=? AND active=1 AND expires_at>?", (test.name, snapshot["cohort_key"], test.policy_digest, self.now))
            if candidates and run["timestamp"] >= candidates[0][1]:
                baseline_id = candidates[0][0]
                thresholds = {key: json.loads(rule) for key, rule in self.database.read("SELECT metric_key,rule_json FROM threshold WHERE baseline_id=?", (baseline_id,))}
            observations = snapshot["observations"]
            expected = len(set(thresholds) | {item["key"] for item in observations})
            rows = [(item["key"], item["rank"], item["value"], *classify(item["value"], thresholds.get(item["key"]))) for item in observations]
            bad = sum(row[3] == "bad" for row in rows)
            classified = sum(row[3] is not None for row in rows)
            if not snapshot["enabled"]:
                status, health, reason = "not_applicable", None, "test_disabled"
            elif raw_status == "fail":
                status, health, reason = "complete", "bad", "raw_test_failed"
            elif raw_status != "pass":
                status, health, reason = "unclassified", None, "raw_test_incomplete"
            elif bad:
                status, health, reason = "complete", "bad", "required_metric_failed"
            elif not thresholds or classified < expected or not rows:
                status, health, reason = "unclassified", None, "missing_threshold_or_metric_coverage"
            else:
                performance = [row for row in rows if thresholds[row[0]]["direction"] != "reference"]
                health = "excellent" if performance and all(row[3] == "excellent" for row in performance) else "good"
                status, reason = "complete", "full_coverage"
        except (ValueError, OSError, sqlite3.Error) as exc:
            raw_status = run["rows"].get(test.name, {}).get("result", "incomplete")
            source_digest = digest(run)
            status = "error" if isinstance(exc, (sqlite3.Error, PermissionError)) else "unclassified"
            health, reason = None, str(exc)
            bad = classified = 0
        identity = digest([run["node"], run["timestamp"], test.name, source_digest, baseline_id, test.policy_digest, self.revision, status, reason])

        def publish(connection):
            if connection.execute("SELECT 1 FROM evaluation WHERE evaluation_id=?", (identity,)).fetchone():
                connection.execute("UPDATE evaluation SET evaluated_at=? WHERE evaluation_id=?", (self.now, identity))
                return False
            connection.execute("INSERT INTO evaluation VALUES (?,?,?,?,?,?,?,?,?,?,?)", (identity, run["node"], run["timestamp"], test.name, source_digest, baseline_id, test.policy_digest, self.revision, status, reason, self.now))
            connection.executemany("INSERT INTO metric_classification VALUES (?,?,?,?,?,?)", [(identity, *row) for row in rows])
            connection.execute("INSERT INTO test_classification VALUES (?,?,?,?,?,?,?)", (identity, raw_status, health, expected, len(rows), classified, bad))
            return True
        return self.database.transaction(publish)