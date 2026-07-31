"""Ingest DL unit-test rank JSON outputs into c-val SQLite metric DBs.

The DL artifact layout has changed over time. The scanner is intentionally
recursive and accepts both of these shapes:

  /data/continuous_validation/dltest/<node>/dltest-<node>-<timestamp>/workdir/test_plans/<plan>/runs/*.json
    /data/continuous_validation/validation_tests/dltest/runs/<node>/<node>-<timestamp>/artifacts/workdir/test_plans/<plan>/runs/*.json

The four output DBs mirror the DL metric categories used by baseline building:
``numerical_correctness``, ``compute_performance``, ``collective_performance``,
and ``overlap_performance``.
"""

from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
import time
import uuid
from collections import Counter
from contextlib import closing
from dataclasses import astuple, dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from cval.config import CvalConfig, load_config
from cval.storage.paths import safe_existing_evidence_path, safe_writable_file_path
from cval.storage.write_provenance import (
    DlRebuildAuthorization,
    require_dl_rebuild_authorization,
)

COMPUTE_METRICS = frozenset(("fp_cpu_time", "fp_gpu_time", "bp_cpu_time", "bp_gpu_time"))
COLLECTIVE_METRICS = frozenset(("cpu_time", "gpu_time"))
OVERLAP_METRICS = frozenset(("coll_mean", "coll_stdev", "layer_mean", "layer_stdev"))
METADATA_FIELDS = frozenset(("task_name", "status", "error_msg", "coll_name", "layer_name"))
TASK_GROUPS = ("nn_tasks", "f_tasks", "coll_tasks", "overlap_tasks")
RANK_PATTERN = re.compile(r"(?:^|_)rank(?P<rank>\d+)(?:_|$)", re.IGNORECASE)
CANONICAL_RANK_RUN_ID_PATTERN = re.compile(
    r"^(?P<prefix>.+)_RANK(?P<rank>\d+)$"
)
RUN_DIR_PATTERN = re.compile(r"^dltest-(?P<node>.+)-(?P<timestamp>\d+)$")
CANONICAL_RUN_DIR_PATTERN = re.compile(r"^(?P<node>.+)-(?P<timestamp>\d+)$")
HISTORICAL_DL_ITERATIONS = 20
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RankFile:
    run_key: str
    node: str
    cval_timestamp: int | None
    iterations: int
    sample_dir: str
    test_plan: str
    dltest_run_id: str
    rank: int
    path: Path
    payload: dict[str, Any]


@dataclass(frozen=True)
class StandardMetricRow:
    run_key: str
    node: str
    cval_timestamp: int | None
    iterations: int
    sample_dir: str
    test_plan: str
    dltest_run_id: str
    rank: int
    task_group: str
    task_name: str
    status: str
    metric_name: str
    metric_value: float | None
    source_file: str


@dataclass(frozen=True)
class OverlapMetricRow:
    run_key: str
    node: str
    cval_timestamp: int | None
    iterations: int
    sample_dir: str
    test_plan: str
    dltest_run_id: str
    rank: int
    task_group: str
    task_name: str
    status: str
    coll_name: str
    layer_name: str
    metric_name: str
    metric_value: float | None
    source_file: str


@dataclass(frozen=True)
class DlRunMetricBundle:
    """All four metric components parsed from one canonical passing DL run."""

    run_id: str
    node: str
    cval_timestamp: int
    rank_file_count: int
    numerical_rows: tuple[StandardMetricRow, ...]
    compute_rows: tuple[StandardMetricRow, ...]
    collective_rows: tuple[StandardMetricRow, ...]
    overlap_rows: tuple[OverlapMetricRow, ...]


def default_dl_metric_db_paths(
    config: CvalConfig | None = None,
) -> dict[str, Path]:
    """Return configured output paths for the four DL metric DBs."""

    config = config or load_config()
    return {
        "numerical_correctness": Path(config.storage.dl_numerical_db_path),
        "compute_performance": Path(config.storage.dl_compute_db_path),
        "collective_performance": Path(config.storage.dl_collective_db_path),
        "overlap_performance": Path(config.storage.dl_overlap_db_path),
    }


def default_dl_results_root() -> Path:
    """Return the configured DL rank-JSON root."""

    return Path(load_config().runtime.dl_results_root_path)


def parse_run_dir(run_dir: Path) -> tuple[str, str, int | None]:
    """Parse a c-val DL run directory name."""

    match = RUN_DIR_PATTERN.match(run_dir.name)
    if match:
        return run_dir.name, match.group("node"), int(match.group("timestamp"))
    canonical = CANONICAL_RUN_DIR_PATTERN.match(run_dir.name)
    if canonical and run_dir.parent.name == canonical.group("node"):
        return (
            run_dir.name,
            canonical.group("node"),
            int(canonical.group("timestamp")),
        )
    return run_dir.name, "", None


def parse_rank(run_id: str) -> int:
    """Extract rank from a run id or filename stem."""

    match = RANK_PATTERN.search(run_id)
    return int(match.group("rank")) if match else -1


def find_dl_run_dirs(results_root: Path) -> list[Path]:
    """Return DL run directories below ``results_root`` that contain rank JSONs."""

    if not results_root.exists():
        return []
    run_dirs: set[Path] = set()
    for runs_dir in results_root.rglob("runs"):
        if not runs_dir.is_dir() or not any(runs_dir.glob("*.json")):
            continue
        if runs_dir.parent.parent.name != "test_plans":
            continue
        workdir = runs_dir.parents[2]
        if workdir.name != "workdir":
            continue
        parent = workdir.parent
        run_dir = parent.parent if parent.name == "artifacts" else parent
        run_dirs.add(run_dir)
    return sorted(run_dirs)


def dl_run_iterations(run_dir: Path) -> int:
    """Read the run's summary iteration count, falling back for old artifacts."""

    summary = load_dl_summary(run_dir)
    if summary is not None:
        try:
            value = summary.get("iterations")
            if value is not None and int(value) > 0:
                return int(value)
        except (ValueError, TypeError):
            pass
    return HISTORICAL_DL_ITERATIONS


def load_dl_summary(run_dir: Path) -> dict[str, Any] | None:
    """Load a canonical or legacy DL summary, returning None when unavailable."""

    summary_paths = [run_dir / "summary.json"]
    summary_paths.extend(sorted(run_dir.glob("dltest-summary-*.json")))
    for summary_path in summary_paths:
        if not summary_path.is_file():
            continue
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Invalid DL summary: {summary_path}") from exc
        if isinstance(payload, dict):
            return payload
    return None


def load_rank_files(results_root: Path) -> Iterable[RankFile]:
    """Yield rank JSON payloads from all c-val DL run directories."""

    for run_dir in find_dl_run_dirs(results_root):
        run_key, node, cval_timestamp = parse_run_dir(run_dir)
        try:
            summary = load_dl_summary(run_dir)
        except ValueError as exc:
            logger.warning("Skipping DL run with malformed summary: %s", exc)
            continue
        canonical_run = (
            RUN_DIR_PATTERN.match(run_dir.name) is None
            and CANONICAL_RUN_DIR_PATTERN.match(run_dir.name) is not None
            and bool(node)
        )
        if canonical_run and summary is None:
            logger.info("Skipping incomplete canonical DL run without summary: %s", run_dir)
            continue
        if summary is not None and (
            (canonical_run and summary.get("status") != "pass")
            or (
                not canonical_run
                and summary.get("status") is not None
                and summary.get("status") != "pass"
            )
        ):
            logger.info("Skipping non-passing DL run: %s", run_dir)
            continue
        iterations = dl_run_iterations(run_dir)
        run_patterns = (
            "workdir/test_plans/*/runs",
            "artifacts/workdir/test_plans/*/runs",
        )
        runs_dirs = {
            path for pattern in run_patterns for path in run_dir.glob(pattern)
        }
        current_run: list[RankFile] = []
        malformed = False
        for runs_dir in sorted(runs_dirs):
            for rank_path in sorted(runs_dir.glob("*.json")):
                try:
                    payload = json.loads(rank_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    logger.warning("Skipping malformed DL run %s due to %s", run_dir, rank_path)
                    malformed = True
                    break
                if not isinstance(payload, dict) or not _payload_tasks_completed(payload):
                    logger.info("Skipping incomplete DL task payload: %s", rank_path)
                    malformed = True
                    break
                dltest_run_id = _strict_nonempty_string(
                    payload.get("runID"),
                    f"DL rank runID in {rank_path}",
                )
                test_plan = _strict_nonempty_string(
                    payload.get("test_plan"),
                    f"DL rank test_plan in {rank_path}",
                )
                current_run.append(RankFile(
                    run_key=run_key,
                    node=node,
                    cval_timestamp=cval_timestamp,
                    iterations=iterations,
                    sample_dir=str(run_dir),
                    test_plan=test_plan,
                    dltest_run_id=dltest_run_id,
                    rank=parse_rank(dltest_run_id),
                    path=rank_path,
                    payload=payload,
                ))
            if malformed:
                break
        if malformed or not current_run:
            continue
        if canonical_run:
            assert summary is not None
            try:
                gpu_count = int(summary["gpu_count"])
            except (KeyError, TypeError, ValueError):
                logger.warning("Skipping DL run with invalid gpu_count: %s", run_dir)
                continue
            ranks = [rank_file.rank for rank_file in current_run]
            if (
                len(ranks) != gpu_count
                or set(ranks) != set(range(gpu_count))
                or summary.get("rank_result_count") != gpu_count
            ):
                logger.warning("Skipping DL run with incomplete rank coverage: %s", run_dir)
                continue
        yield from current_run


def _payload_tasks_completed(payload: dict[str, Any]) -> bool:
    """Return true when a rank payload has tasks and all are completed."""

    task_count = 0
    for task_group in TASK_GROUPS:
        tasks = payload.get(task_group, [])
        if not isinstance(tasks, list):
            return False
        for task in tasks:
            if not isinstance(task, dict) or task.get("status") != "completed":
                return False
            task_count += 1
    return task_count > 0


def iter_tasks(payload: dict[str, Any], task_group: str) -> Iterable[dict[str, Any]]:
    """Yield task dictionaries for one group."""

    tasks = payload.get(task_group, [])
    if not isinstance(tasks, list):
        return ()
    return (task for task in tasks if isinstance(task, dict))


def is_metric_value(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def sqlite_metric_value(value: int | float) -> float | None:
    metric_value = float(value)
    return metric_value if math.isfinite(metric_value) else None


def is_numerical_metric(metric_name: str, value: Any) -> bool:
    if metric_name in METADATA_FIELDS:
        return False
    if metric_name in COMPUTE_METRICS | COLLECTIVE_METRICS | OVERLAP_METRICS:
        return False
    return is_metric_value(value)


def standard_rows(
    rank_file: RankFile,
    task_group: str,
    task: dict[str, Any],
    include_metric: Callable[[str, Any], bool],
) -> list[StandardMetricRow]:
    """Return standard metric rows from one task."""

    task_name = _strict_nonempty_string(task.get("task_name"), "DL task_name")
    status = _strict_nonempty_string(task.get("status"), "DL task status")
    rows: list[StandardMetricRow] = []
    for metric_name, raw_value in task.items():
        if not include_metric(str(metric_name), raw_value):
            continue
        if not is_metric_value(raw_value):
            continue
        rows.append(
            StandardMetricRow(
                run_key=rank_file.run_key,
                node=rank_file.node,
                cval_timestamp=rank_file.cval_timestamp,
                iterations=rank_file.iterations,
                sample_dir=rank_file.sample_dir,
                test_plan=rank_file.test_plan,
                dltest_run_id=rank_file.dltest_run_id,
                rank=rank_file.rank,
                task_group=task_group,
                task_name=task_name,
                status=status,
                metric_name=str(metric_name),
                metric_value=sqlite_metric_value(raw_value),
                source_file=str(rank_file.path),
            )
        )
    return rows


def overlap_metric_rows(rank_file: RankFile, task: dict[str, Any]) -> list[OverlapMetricRow]:
    """Return overlap metric rows from one task."""

    rows: list[OverlapMetricRow] = []
    task_name = _strict_nonempty_string(task.get("task_name"), "DL overlap task_name")
    coll_name = _strict_nonempty_string(task.get("coll_name"), "DL overlap coll_name")
    layer_name = _strict_nonempty_string(task.get("layer_name"), "DL overlap layer_name")
    for metric_name in sorted(OVERLAP_METRICS):
        raw_value = task.get(metric_name)
        if not is_metric_value(raw_value):
            continue
        rows.append(
            OverlapMetricRow(
                run_key=rank_file.run_key,
                node=rank_file.node,
                cval_timestamp=rank_file.cval_timestamp,
                iterations=rank_file.iterations,
                sample_dir=rank_file.sample_dir,
                test_plan=rank_file.test_plan,
                dltest_run_id=rank_file.dltest_run_id,
                rank=rank_file.rank,
                task_group="overlap_tasks",
                task_name=task_name,
                status=_strict_nonempty_string(task.get("status"), "DL task status"),
                coll_name=coll_name,
                layer_name=layer_name,
                metric_name=metric_name,
                metric_value=sqlite_metric_value(raw_value),
                source_file=str(rank_file.path),
            )
        )
    return rows


def classify_rank_files(
    rank_files: Iterable[RankFile],
) -> tuple[
    list[StandardMetricRow],
    list[StandardMetricRow],
    list[StandardMetricRow],
    list[OverlapMetricRow],
]:
    """Split rank payloads into numerical, compute, collective, and overlap rows."""

    numerical_rows: list[StandardMetricRow] = []
    compute_rows: list[StandardMetricRow] = []
    collective_rows: list[StandardMetricRow] = []
    overlap_rows: list[OverlapMetricRow] = []

    for rank_file in rank_files:
        for task_group in ("nn_tasks", "f_tasks"):
            for task in iter_tasks(rank_file.payload, task_group):
                if task.get("status") != "completed":
                    continue
                numerical_rows.extend(standard_rows(rank_file, task_group, task, is_numerical_metric))
                compute_rows.extend(
                    standard_rows(
                        rank_file,
                        task_group,
                        task,
                        lambda name, _value: name in COMPUTE_METRICS,
                    )
                )

        for task in iter_tasks(rank_file.payload, "coll_tasks"):
            if task.get("status") != "completed":
                continue
            numerical_rows.extend(standard_rows(rank_file, "coll_tasks", task, is_numerical_metric))
            collective_rows.extend(
                standard_rows(
                    rank_file,
                    "coll_tasks",
                    task,
                    lambda name, _value: name in COLLECTIVE_METRICS,
                )
            )

        for task in iter_tasks(rank_file.payload, "overlap_tasks"):
            if task.get("status") != "completed":
                continue
            overlap_rows.extend(overlap_metric_rows(rank_file, task))

    return numerical_rows, compute_rows, collective_rows, overlap_rows


def load_canonical_dl_run_metrics(
    run_dir: str | Path,
    *,
    summary_path: str | Path,
    expected_run_id: str,
    expected_node: str,
    expected_timestamp: int,
    expected_test_plan: str,
    expected_iterations: int,
    expected_gpu_count: int,
) -> DlRunMetricBundle:
    """Parse exactly one canonical passing run for modular current-run ingestion."""

    path = Path(run_dir).expanduser().resolve()
    if not path.is_dir():
        raise FileNotFoundError(f"DL run directory not found: {path}")
    canonical_summary = Path(summary_path).expanduser()
    if (
        canonical_summary.parent != path
        or canonical_summary.is_symlink()
        or not canonical_summary.is_file()
    ):
        raise ValueError(
            f"DL run is missing its declared canonical summary: {canonical_summary}"
        )
    try:
        summary = json.loads(canonical_summary.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid canonical DL summary: {canonical_summary}") from exc
    if not isinstance(summary, dict):
        raise ValueError(f"Canonical DL summary must be an object: {canonical_summary}")
    if summary.get("schema_version") != "cval.dltest.summary.v1":
        raise ValueError(f"DL run has unsupported summary schema: {path}")
    if summary.get("status") != "pass":
        raise ValueError(f"DL run summary is not passing: {path}")
    if not isinstance(summary.get("test_plan"), str):
        raise ValueError(f"DL run summary test_plan must be a string: {path}")
    summary_identity = (
        summary["test_plan"],
        _strict_json_int(summary.get("iterations"), "DL summary iterations"),
        _strict_json_int(summary.get("gpu_count"), "DL summary gpu_count"),
    )
    expected_summary_identity = (
        expected_test_plan,
        expected_iterations,
        expected_gpu_count,
    )
    if summary_identity != expected_summary_identity:
        raise ValueError(
            f"DL summary workload identity {summary_identity!r} does not match "
            f"descriptor {expected_summary_identity!r}"
        )
    expected_ranks = list(range(expected_gpu_count))
    summary_expected_ranks = _strict_json_int_list(
        summary.get("expected_ranks"), "DL summary expected_ranks"
    )
    summary_observed_ranks = _strict_json_int_list(
        summary.get("observed_ranks"), "DL summary observed_ranks"
    )
    if (
        _strict_json_int(
            summary.get("rank_result_count"), "DL summary rank_result_count"
        )
        != expected_gpu_count
        or summary_expected_ranks != expected_ranks
        or summary_observed_ranks != expected_ranks
        or summary.get("rank_coverage_valid") is not True
        or summary.get("test_plans_match") is not True
        or summary.get("tasks_complete") is not True
    ):
        raise ValueError(f"DL run summary does not prove complete rank/task coverage: {path}")
    rank_files = list(load_rank_files(path))
    if not rank_files:
        raise ValueError(f"DL run has no complete passing rank evidence: {path}")
    identities = {
        (rank_file.run_key, rank_file.node, rank_file.cval_timestamp)
        for rank_file in rank_files
    }
    expected = (expected_run_id, expected_node, expected_timestamp)
    if identities != {expected}:
        raise ValueError(
            f"DL rank evidence identity {sorted(identities)!r} does not match {expected!r}"
        )
    ranks = [rank_file.rank for rank_file in rank_files]
    if sorted(ranks) != expected_ranks or len(set(ranks)) != expected_gpu_count:
        raise ValueError(f"DL rank evidence is not exactly {expected_ranks!r}: {path}")
    invocation_prefixes: set[str] = set()
    for rank_file in rank_files:
        run_id_match = CANONICAL_RANK_RUN_ID_PATTERN.fullmatch(
            rank_file.dltest_run_id
        )
        if (
            run_id_match is None
            or int(run_id_match.group("rank")) != rank_file.rank
            or run_id_match.group("rank") != str(rank_file.rank)
        ):
            raise ValueError(
                f"DL rank {rank_file.rank} runID must end exactly in "
                f"_RANK{rank_file.rank}"
            )
        invocation_prefixes.add(run_id_match.group("prefix"))
        if rank_file.test_plan != expected_test_plan:
            raise ValueError(
                f"DL rank {rank_file.rank} test_plan {rank_file.test_plan!r} does not "
                f"match {expected_test_plan!r}"
            )
        if rank_file.path.parent.parent.name != expected_test_plan:
            raise ValueError(
                f"DL rank {rank_file.rank} is stored under the wrong test-plan directory"
            )
        numerical_rank, compute_rank, collective_rank, overlap_rank = classify_rank_files(
            (rank_file,)
        )
        rank_components = {
            "numerical_correctness": numerical_rank,
            "compute_performance": compute_rank,
            "collective_performance": collective_rank,
            "overlap_performance": overlap_rank,
        }
        missing_rank_components = sorted(
            component for component, rows in rank_components.items() if not rows
        )
        if missing_rank_components:
            raise ValueError(
                f"DL rank {rank_file.rank} is missing metric component(s): "
                f"{', '.join(missing_rank_components)}"
            )
        if any(
            row.metric_value is None
            for rows in rank_components.values()
            for row in rows
        ):
            raise ValueError(f"DL rank {rank_file.rank} contains a non-finite metric")
    if len(invocation_prefixes) != 1:
        raise ValueError(
            "DL rank evidence mixes multiple invocation prefixes: "
            f"{sorted(invocation_prefixes)}"
        )
    summary_ranks = summary.get("rank_results")
    if not isinstance(summary_ranks, list) or len(summary_ranks) != expected_gpu_count:
        raise ValueError(f"DL summary rank_results is incomplete: {path}")
    by_rank = {rank_file.rank: rank_file for rank_file in rank_files}
    seen_summary_ranks: set[int] = set()
    aggregate_task_counts: Counter[str] = Counter()
    aggregate_status_counts: Counter[str] = Counter()
    for item in summary_ranks:
        if not isinstance(item, dict):
            raise ValueError(f"DL summary contains an invalid rank result: {path}")
        rank = _strict_json_int(item.get("rank"), "DL summary rank")
        if rank in seen_summary_ranks or rank not in by_rank:
            raise ValueError(f"DL summary contains duplicate/unexpected rank {rank}")
        seen_summary_ranks.add(rank)
        rank_file = by_rank[rank]
        task_counts = _rank_task_counts(rank_file.payload)
        status_counts = _rank_status_counts(rank_file.payload)
        summary_task_counts = _strict_count_map(
            item.get("task_counts"), f"DL summary rank {rank} task_counts"
        )
        summary_status_counts = _strict_count_map(
            item.get("status_counts"), f"DL summary rank {rank} status_counts"
        )
        aggregate_task_counts.update(task_counts)
        aggregate_status_counts.update(status_counts)
        item_file = item.get("file")
        if (
            item.get("run_id") != rank_file.dltest_run_id
            or item.get("test_plan") != expected_test_plan
            or item.get("tasks_valid") is not True
            or not isinstance(item_file, str)
            or item_file != str(rank_file.path)
            or summary_task_counts != task_counts
            or summary_status_counts != status_counts
        ):
            raise ValueError(f"DL summary rank {rank} does not match its source evidence")
    if _strict_count_map(summary.get("task_counts"), "DL summary task_counts") != dict(
        sorted(aggregate_task_counts.items())
    ):
        raise ValueError("DL summary aggregate task_counts do not match rank evidence")
    if _strict_count_map(
        summary.get("status_counts"), "DL summary status_counts"
    ) != dict(sorted(aggregate_status_counts.items())):
        raise ValueError("DL summary aggregate status_counts do not match rank evidence")
    numerical, compute, collective, overlap = classify_rank_files(rank_files)
    component_counts = {
        "numerical_correctness": len(numerical),
        "compute_performance": len(compute),
        "collective_performance": len(collective),
        "overlap_performance": len(overlap),
    }
    missing_components = sorted(
        component for component, count in component_counts.items() if count == 0
    )
    if missing_components:
        raise ValueError(
            "DL run is missing declared metric component(s): "
            f"{', '.join(missing_components)}"
        )
    return DlRunMetricBundle(
        run_id=expected_run_id,
        node=expected_node,
        cval_timestamp=expected_timestamp,
        rank_file_count=len(rank_files),
        numerical_rows=tuple(numerical),
        compute_rows=tuple(compute),
        collective_rows=tuple(collective),
        overlap_rows=tuple(overlap),
    )


def _strict_json_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _strict_nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _strict_json_int_list(value: object, field_name: str) -> list[int]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be an array")
    return [_strict_json_int(item, f"{field_name}[]") for item in value]


def _strict_count_map(value: object, field_name: str) -> dict[str, int]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field_name} must be an object")
    result: dict[str, int] = {}
    for key, count in value.items():
        parsed = _strict_json_int(count, f"{field_name}.{key}")
        if parsed < 0:
            raise ValueError(f"{field_name}.{key} must be non-negative")
        result[key] = parsed
    return result


def _rank_task_counts(payload: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for group in TASK_GROUPS:
        tasks = payload.get(group)
        if not isinstance(tasks, list):
            raise ValueError(f"DL rank task group {group!r} must be an array")
        counts[group] = len(tasks)
    return counts


def _rank_status_counts(payload: dict[str, Any]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for group in TASK_GROUPS:
        tasks = payload.get(group)
        if not isinstance(tasks, list):
            raise ValueError(f"DL rank task group {group!r} must be an array")
        for task in tasks:
            if not isinstance(task, dict):
                raise ValueError(f"DL rank task in {group!r} must be an object")
            counts[str(task.get("status", "missing"))] += 1
    return dict(counts)


def connect(db_path: Path) -> sqlite3.Connection:
    from cval.storage.sqlite_uri import connect_sqlite_file

    db_path = safe_writable_file_path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path = safe_writable_file_path(db_path)
    connection = connect_sqlite_file(db_path, mode="rwc", timeout=30)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def ensure_iterations_column(
    connection: sqlite3.Connection,
    table_name: str,
    historical_iterations: int = HISTORICAL_DL_ITERATIONS,
) -> None:
    """Add/backfill the DL iteration count on an existing metric table."""

    columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table_name})")}
    if "iterations" not in columns:
        # SQLite stores the constant DEFAULT in table metadata, so this is fast
        # even for multi-million-row metric DBs and existing rows read as 20.
        connection.execute(
            f"ALTER TABLE {table_name} ADD COLUMN iterations INTEGER "
            f"NOT NULL DEFAULT {int(historical_iterations)}"
        )


def prepare_canonical_dl_metric_tables(connection: sqlite3.Connection) -> None:
    """Create all four existing DL component tables in one canonical result DB."""

    for table_name in (
        "numerical_correctness",
        "compute_performance",
        "collective_performance",
    ):
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                run_key TEXT NOT NULL,
                node TEXT,
                cval_timestamp INTEGER,
                iterations INTEGER NOT NULL DEFAULT {HISTORICAL_DL_ITERATIONS},
                sample_dir TEXT NOT NULL,
                test_plan TEXT NOT NULL,
                dltest_run_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                task_group TEXT NOT NULL,
                task_name TEXT NOT NULL,
                status TEXT,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                source_file TEXT NOT NULL,
                PRIMARY KEY (run_key, rank, task_group, task_name, metric_name)
            )
            """
        )
        ensure_iterations_column(connection, table_name)
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_node_ts "
            f"ON {table_name}(node, cval_timestamp)"
        )
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_metric "
            f"ON {table_name}(task_name, metric_name)"
        )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS overlap_performance (
            run_key TEXT NOT NULL,
            node TEXT,
            cval_timestamp INTEGER,
            iterations INTEGER NOT NULL DEFAULT {HISTORICAL_DL_ITERATIONS},
            sample_dir TEXT NOT NULL,
            test_plan TEXT NOT NULL,
            dltest_run_id TEXT NOT NULL,
            rank INTEGER NOT NULL,
            task_group TEXT NOT NULL,
            task_name TEXT NOT NULL,
            status TEXT,
            coll_name TEXT NOT NULL,
            layer_name TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL,
            source_file TEXT NOT NULL,
            PRIMARY KEY (run_key, rank, task_group, task_name, metric_name)
        )
        """
    )
    ensure_iterations_column(connection, "overlap_performance")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_overlap_performance_node_ts "
        "ON overlap_performance(node, cval_timestamp)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_overlap_performance_pair "
        "ON overlap_performance(coll_name, layer_name)"
    )


def insert_canonical_dl_metric_bundle(
    connection: sqlite3.Connection,
    bundle: DlRunMetricBundle,
) -> int:
    """Insert one new DL bundle after rejecting ambiguous pre-existing rows."""

    component_rows: tuple[tuple[str, tuple[StandardMetricRow, ...]], ...] = (
        ("numerical_correctness", bundle.numerical_rows),
        ("compute_performance", bundle.compute_rows),
        ("collective_performance", bundle.collective_rows),
    )
    for table_name, _rows in component_rows:
        if connection.execute(
            f"SELECT 1 FROM {table_name} WHERE run_key=? LIMIT 1",
            (bundle.run_id,),
        ).fetchone():
            raise ValueError(
                f"DL metric table {table_name} already contains run {bundle.run_id!r}"
            )
    if connection.execute(
        "SELECT 1 FROM overlap_performance WHERE run_key=? LIMIT 1",
        (bundle.run_id,),
    ).fetchone():
        raise ValueError(
            f"DL metric table overlap_performance already contains run {bundle.run_id!r}"
        )

    for table_name, rows in component_rows:
        connection.executemany(
            f"""
            INSERT INTO {table_name} (
                run_key, node, cval_timestamp, iterations, sample_dir, test_plan,
                dltest_run_id, rank, task_group, task_name, status, metric_name,
                metric_value, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [astuple(row) for row in rows],
        )
    connection.executemany(
        """
        INSERT INTO overlap_performance (
            run_key, node, cval_timestamp, iterations, sample_dir, test_plan,
            dltest_run_id, rank, task_group, task_name, status, coll_name,
            layer_name, metric_name, metric_value, source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [astuple(row) for row in bundle.overlap_rows],
    )
    return sum(
        len(rows)
        for rows in (
            bundle.numerical_rows,
            bundle.compute_rows,
            bundle.collective_rows,
            bundle.overlap_rows,
        )
    )


def write_standard_db(
    db_path: Path,
    table_name: str,
    rows: list[StandardMetricRow],
    *,
    replace_run_keys: set[str] | None = None,
    reconcile_root: Path | None = None,
    generation_id: str | None = None,
) -> None:
    """Write standard metric rows to a SQLite DB."""

    with closing(connect(db_path)) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                run_key TEXT NOT NULL,
                node TEXT,
                cval_timestamp INTEGER,
                iterations INTEGER NOT NULL DEFAULT {HISTORICAL_DL_ITERATIONS},
                sample_dir TEXT NOT NULL,
                test_plan TEXT NOT NULL,
                dltest_run_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                task_group TEXT NOT NULL,
                task_name TEXT NOT NULL,
                status TEXT,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                source_file TEXT NOT NULL,
                PRIMARY KEY (run_key, rank, task_group, task_name, metric_name)
            )
            """
        )
        ensure_iterations_column(connection, table_name)
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_node_ts ON {table_name}(node, cval_timestamp)"
        )
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_metric ON {table_name}(task_name, metric_name)"
        )
        run_keys = set(replace_run_keys or set())
        if reconcile_root is not None:
            run_keys.update(_run_keys_in_scope(connection, table_name, reconcile_root))
        _delete_run_keys(connection, table_name, run_keys)
        connection.executemany(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                run_key, node, cval_timestamp, iterations, sample_dir, test_plan, dltest_run_id,
                rank, task_group, task_name, status, metric_name, metric_value, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row.__dict__.values()) for row in rows],
        )
        if generation_id:
            _write_ingest_generation(connection, generation_id)
        connection.commit()


def write_overlap_db(
    db_path: Path,
    table_name: str,
    rows: list[OverlapMetricRow],
    *,
    replace_run_keys: set[str] | None = None,
    reconcile_root: Path | None = None,
    generation_id: str | None = None,
) -> None:
    """Write overlap metric rows to a SQLite DB."""

    with closing(connect(db_path)) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                run_key TEXT NOT NULL,
                node TEXT,
                cval_timestamp INTEGER,
                iterations INTEGER NOT NULL DEFAULT {HISTORICAL_DL_ITERATIONS},
                sample_dir TEXT NOT NULL,
                test_plan TEXT NOT NULL,
                dltest_run_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                task_group TEXT NOT NULL,
                task_name TEXT NOT NULL,
                status TEXT,
                coll_name TEXT NOT NULL,
                layer_name TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                source_file TEXT NOT NULL,
                PRIMARY KEY (run_key, rank, task_group, task_name, metric_name)
            )
            """
        )
        ensure_iterations_column(connection, table_name)
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_node_ts ON {table_name}(node, cval_timestamp)"
        )
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_pair ON {table_name}(coll_name, layer_name)"
        )
        run_keys = set(replace_run_keys or set())
        if reconcile_root is not None:
            run_keys.update(_run_keys_in_scope(connection, table_name, reconcile_root))
        _delete_run_keys(connection, table_name, run_keys)
        connection.executemany(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                run_key, node, cval_timestamp, iterations, sample_dir, test_plan, dltest_run_id,
                rank, task_group, task_name, status, coll_name, layer_name,
                metric_name, metric_value, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row.__dict__.values()) for row in rows],
        )
        if generation_id:
            _write_ingest_generation(connection, generation_id)
        connection.commit()


def _delete_run_keys(
    connection: sqlite3.Connection,
    table_name: str,
    run_keys: set[str],
) -> None:
    """Delete prior rows for runs that will be replaced in this transaction."""

    keys = sorted(run_keys)
    for start in range(0, len(keys), 500):
        chunk = keys[start : start + 500]
        placeholders = ",".join("?" for _ in chunk)
        connection.execute(
            f"DELETE FROM {table_name} WHERE run_key IN ({placeholders})",
            chunk,
        )


def _run_keys_in_scope(
    connection: sqlite3.Connection,
    table_name: str,
    root: Path,
) -> set[str]:
    """Return stored run keys whose source directory lies below a scanned root."""

    lexical_root = root.expanduser()
    if lexical_root.is_symlink():
        raise ValueError(f"DL results root must not be a symlink: {lexical_root}")
    resolved_root = lexical_root.resolve()
    rows = connection.execute(
        f"SELECT DISTINCT run_key, sample_dir FROM {table_name}"
    ).fetchall()
    scoped: set[str] = set()
    for run_key, sample_dir in rows:
        try:
            Path(str(sample_dir)).expanduser().resolve().relative_to(resolved_root)
        except (OSError, ValueError):
            continue
        scoped.add(str(run_key))
    return scoped


def _write_ingest_generation(
    connection: sqlite3.Connection,
    generation_id: str,
) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cval_ingest_metadata (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            generation_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        "INSERT INTO cval_ingest_metadata(id, generation_id, updated_at) "
        "VALUES (1, ?, ?) ON CONFLICT(id) DO UPDATE SET "
        "generation_id=excluded.generation_id, updated_at=excluded.updated_at",
        (generation_id, int(time.time())),
    )


def validate_dl_metric_generation(db_paths: dict[str, str | Path]) -> str | None:
    """Require all generation-aware DL DBs to expose one completed generation."""

    from cval.storage.sqlite_uri import connect_sqlite_file

    generations: dict[str, str | None] = {}
    for component, raw_path in db_paths.items():
        path = Path(raw_path)
        if not path.is_file():
            generations[component] = None
            continue
        with closing(connect_sqlite_file(path, mode="ro", timeout=30)) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='cval_ingest_metadata'"
            ).fetchone()
            if table is None:
                generations[component] = None
                continue
            row = connection.execute(
                "SELECT generation_id FROM cval_ingest_metadata WHERE id=1"
            ).fetchone()
            generations[component] = str(row[0]) if row else None
    present = {value for value in generations.values() if value is not None}
    if not present:
        return None  # Legacy DB set without generation metadata.
    if len(present) != 1 or any(value is None for value in generations.values()):
        raise RuntimeError(
            "DL metric DB refresh generation is incomplete or inconsistent: "
            f"{generations}"
        )
    return next(iter(present))


def ingest_dltest_results(
    results_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    *,
    config: CvalConfig | None = None,
    _authorization: DlRebuildAuthorization | None = None,
) -> dict[str, Any]:
    """Ingest DL rank JSONs and return a summary."""

    root = Path(results_root) if results_root is not None else default_dl_results_root()
    if output_dir is None:
        paths = default_dl_metric_db_paths(config)
        output_label = "configured paths"
    else:
        output = Path(output_dir)
        paths = {
            "numerical_correctness": output / "dltest_numerical_correctness.db",
            "compute_performance": output / "dltest_compute_performance.db",
            "collective_performance": output / "dltest_collective_performance.db",
            "overlap_performance": output / "dltest_overlap_performance.db",
        }
        output_label = str(output)
    lexical_root = root.expanduser()
    if not lexical_root.is_absolute():
        lexical_root = Path.cwd() / lexical_root
    lexical_root = Path(*lexical_root.parts)
    authorization = require_dl_rebuild_authorization(
        _authorization,
        results_root=lexical_root,
        db_paths={key: Path(path) for key, path in paths.items()},
    )
    if not lexical_root.exists():
        raise FileNotFoundError(f"DL test results root not found: {root}")
    validated_root = safe_existing_evidence_path(
        lexical_root,
        expected_path=authorization.results_root,
        allowed_root=authorization.config.runtime.dl_results_root_path,
        expect_directory=True,
        description="DL rebuild results root",
    )
    _validate_dl_evidence_tree(validated_root)
    rank_files = list(load_rank_files(validated_root))
    if not rank_files and not any(Path(path).is_file() for path in paths.values()):
        raise ValueError(
            "DL rebuild found no valid rank evidence and no existing metric DBs to reconcile"
        )

    numerical_rows, compute_rows, collective_rows, overlap_rows = classify_rank_files(rank_files)
    run_keys = {rank_file.run_key for rank_file in rank_files}
    generation_id = f"{int(time.time() * 1_000_000)}-{uuid.uuid4().hex}"
    paths = {
        component: safe_writable_file_path(path)
        for component, path in paths.items()
    }
    write_standard_db(
        paths["numerical_correctness"],
        "numerical_correctness",
        numerical_rows,
        replace_run_keys=run_keys,
        reconcile_root=validated_root,
        generation_id=generation_id,
    )
    write_standard_db(
        paths["compute_performance"],
        "compute_performance",
        compute_rows,
        replace_run_keys=run_keys,
        reconcile_root=validated_root,
        generation_id=generation_id,
    )
    write_standard_db(
        paths["collective_performance"],
        "collective_performance",
        collective_rows,
        replace_run_keys=run_keys,
        reconcile_root=validated_root,
        generation_id=generation_id,
    )
    write_overlap_db(
        paths["overlap_performance"],
        "overlap_performance",
        overlap_rows,
        replace_run_keys=run_keys,
        reconcile_root=validated_root,
        generation_id=generation_id,
    )

    run_counts = Counter(rank_file.run_key for rank_file in rank_files)
    return {
        "results_root": str(root),
        "output_dir": output_label,
        "db_paths": {name: str(path) for name, path in paths.items()},
        "generation_id": generation_id,
        "rank_files": len(rank_files),
        "runs": len(run_counts),
        "numerical_correctness_rows": len(numerical_rows),
        "compute_performance_rows": len(compute_rows),
        "collective_performance_rows": len(collective_rows),
        "overlap_performance_rows": len(overlap_rows),
    }


def _validate_dl_evidence_tree(root: Path) -> None:
    """Reject symlinked rank/summary evidence before opening any metric DB."""

    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"DL results root must be a non-symlink directory: {root}")
    resolved_root = root.resolve()
    for candidate in root.rglob("*"):
        if candidate.is_symlink():
            raise ValueError(f"DL results tree contains a symlink: {candidate}")
        try:
            candidate.resolve().relative_to(resolved_root)
        except ValueError as exc:
            raise ValueError(
                f"DL evidence escapes its configured results root: {candidate}"
            ) from exc
