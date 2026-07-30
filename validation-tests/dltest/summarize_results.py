#!/usr/bin/env python3
"""Build a compact c-val summary from dl_unit_test rank JSON files."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any

TASK_GROUPS = ("nn_tasks", "f_tasks", "coll_tasks", "overlap_tasks")
RANK_PATTERN = re.compile(r"(?:^|_)RANK(?P<rank>\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize dl_unit_test JSON outputs")
    parser.add_argument("--runs-dir", type=Path, required=True)
    parser.add_argument("--summary-file", type=Path, required=True)
    parser.add_argument("--status", choices=("pass", "fail"), required=True)
    parser.add_argument("--test-plan", required=True)
    parser.add_argument("--iterations", type=int, required=True)
    parser.add_argument("--gpu-count", type=int, required=True)
    parser.add_argument("--log-file", default="")
    parser.add_argument("--source-dir", default="")
    parser.add_argument("--work-dir", default="")
    return parser.parse_args()


def load_rank_results(runs_dir: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path in sorted(runs_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        run_id = str(payload.get("runID", path.stem))
        results.append(
            {
                "rank": _rank_from_run_id(run_id),
                "run_id": run_id,
                "file": str(path),
                "test_plan": str(payload.get("test_plan", "")),
                "task_counts": _task_counts(payload),
                "status_counts": _status_counts(payload),
                "tasks_valid": _tasks_valid(payload),
            }
        )
    return results


def build_summary(args: argparse.Namespace) -> dict[str, Any]:
    rank_results = load_rank_results(args.runs_dir)
    aggregate_counts: Counter[str] = Counter()
    aggregate_statuses: Counter[str] = Counter()
    for result in rank_results:
        aggregate_counts.update(result["task_counts"])
        aggregate_statuses.update(result["status_counts"])

    failed_statuses = {
        status: count
        for status, count in aggregate_statuses.items()
        if status and status != "completed" and count
    }
    ranks = [result["rank"] for result in rank_results]
    expected_ranks = set(range(args.gpu_count))
    observed_ranks = {rank for rank in ranks if isinstance(rank, int)}
    rank_coverage_valid = (
        len(ranks) == args.gpu_count
        and len(observed_ranks) == args.gpu_count
        and observed_ranks == expected_ranks
    )
    plans_match = all(
        result["test_plan"] == args.test_plan for result in rank_results
    )
    tasks_complete = all(result["tasks_valid"] for result in rank_results)
    effective_status = args.status
    if (
        not rank_results
        or failed_statuses
        or not rank_coverage_valid
        or not plans_match
        or not tasks_complete
    ):
        effective_status = "fail"

    return {
        "schema_version": "cval.dltest.summary.v1",
        "status": effective_status,
        "test_plan": args.test_plan,
        "iterations": args.iterations,
        "gpu_count": args.gpu_count,
        "source_dir": args.source_dir,
        "work_dir": args.work_dir,
        "runs_dir": str(args.runs_dir),
        "log_file": args.log_file,
        "rank_result_count": len(rank_results),
        "expected_ranks": sorted(expected_ranks),
        "observed_ranks": sorted(observed_ranks),
        "rank_coverage_valid": rank_coverage_valid,
        "test_plans_match": plans_match,
        "tasks_complete": tasks_complete,
        "task_counts": dict(sorted(aggregate_counts.items())),
        "status_counts": dict(sorted(aggregate_statuses.items())),
        "rank_results": rank_results,
    }


def _task_counts(payload: dict[str, Any]) -> dict[str, int]:
    return {
        group: len(payload.get(group, []))
        for group in TASK_GROUPS
        if isinstance(payload.get(group, []), list)
    }


def _status_counts(payload: dict[str, Any]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for group in TASK_GROUPS:
        tasks = payload.get(group, [])
        if not isinstance(tasks, list):
            continue
        for task in tasks:
            if isinstance(task, dict):
                counts[str(task.get("status", "missing"))] += 1
    return dict(counts)


def _tasks_valid(payload: dict[str, Any]) -> bool:
    """Return true when at least one task exists and every task completed."""

    count = 0
    for group in TASK_GROUPS:
        tasks = payload.get(group, [])
        if not isinstance(tasks, list):
            return False
        for task in tasks:
            if not isinstance(task, dict) or task.get("status") != "completed":
                return False
            count += 1
    return count > 0


def _rank_from_run_id(run_id: str) -> int | None:
    match = RANK_PATTERN.search(run_id)
    return int(match.group("rank")) if match else None


def main() -> int:
    args = parse_args()
    summary = build_summary(args)
    args.summary_file.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.summary_file.with_name(f".{args.summary_file.name}.tmp-{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, args.summary_file)
    finally:
        temporary.unlink(missing_ok=True)
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
