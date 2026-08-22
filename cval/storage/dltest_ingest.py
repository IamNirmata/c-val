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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from cval.config import CvalConfig, load_config
from cval.storage.paths import safe_existing_evidence_path, safe_writable_file_path
from cval.storage.write_provenance import (
    DlRebuildAuthorization,
    ResultWriteAuthorization,
    require_dl_rebuild_authorization,
    validate_current_dl_write,
)

COMPUTE_METRICS = frozenset(("fp_cpu_time", "fp_gpu_time", "bp_cpu_time", "bp_gpu_time"))
COLLECTIVE_METRICS = frozenset(("cpu_time", "gpu_time"))
OVERLAP_METRICS = frozenset(("coll_mean", "coll_stdev", "layer_mean", "layer_stdev"))
METADATA_FIELDS = frozenset(("task_name", "status", "error_msg", "coll_name", "layer_name"))
TASK_GROUPS = ("nn_tasks", "f_tasks", "coll_tasks", "overlap_tasks")
RANK_PATTERN = re.compile(r"(?:^|_)rank(?P<rank>\d+)(?:_|$)", re.IGNORECASE)
FLAT_OVERLAP_METRIC_PATTERN = re.compile(r"^(?P<stat>mean|stdev)_(?P<collective>.+)$")
FLAT_TEST_PLAN = "80gb-b200"
FLAT_TASK_COUNTS = {
    "nn_tasks": 57,
    "f_tasks": 38,
    "coll_tasks": 24,
    "overlap_layers": 14,
}
FLAT_NN_METRIC_SETS = frozenset(
    {
        frozenset(
            {
                "bias",
                "bp_cpu_time",
                "bp_gpu_time",
                "fp_cpu_time",
                "fp_gpu_time",
                "norm_output",
                "weight",
            }
        ),
        frozenset(
            {
                "bp_cpu_time",
                "bp_gpu_time",
                "fp_cpu_time",
                "fp_gpu_time",
                "norm_output",
                "weight",
            }
        ),
        frozenset(
            {
                "bias_hh_l0",
                "bias_ih_l0",
                "bp_cpu_time",
                "bp_gpu_time",
                "fp_cpu_time",
                "fp_gpu_time",
                "norm_output",
                "weight_hh_l0",
                "weight_ih_l0",
            }
        ),
        frozenset(
            {
                "bp_cpu_time",
                "bp_gpu_time",
                "fp_cpu_time",
                "fp_gpu_time",
                "norm_output",
            }
        ),
    }
)
FLAT_F_METRICS = frozenset(
    {
        "bp_cpu_time",
        "bp_gpu_time",
        "fp_cpu_time",
        "fp_gpu_time",
        "norm_output",
        "weight",
    }
)
FLAT_COLLECTIVE_METRICS = frozenset({"coll_cpu", "coll_gpu", "norm_output"})
FLAT_OVERLAP_COLLECTIVE_COUNTS = frozenset({6, 8})
FLAT_EXPECTED_ROW_COUNTS = (2168, 3040, 384, 1536)
FLAT_SCHEMA_MARKER = "_cval_flat_schema"
RUN_DIR_PATTERN = re.compile(r"^dltest-(?P<node>.+)-(?P<timestamp>\d+)$")
CANONICAL_RUN_DIR_PATTERN = re.compile(r"^(?P<node>.+)-(?P<timestamp>\d+)$")
HISTORICAL_DL_ITERATIONS = 20
DL_INGEST_BATCH_RUNS = 25
DL_INGEST_STATE_IN_PROGRESS = "in_progress"
DL_INGEST_STATE_COMPLETE = "complete"
logger = logging.getLogger(__name__)

_STANDARD_REQUIRED_COLUMNS = frozenset(
    {
        "run_key",
        "node",
        "cval_timestamp",
        "sample_dir",
        "test_plan",
        "dltest_run_id",
        "rank",
        "task_group",
        "task_name",
        "status",
        "metric_name",
        "metric_value",
        "source_file",
    }
)
_OVERLAP_REQUIRED_COLUMNS = _STANDARD_REQUIRED_COLUMNS | {"coll_name", "layer_name"}


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
                if not isinstance(payload, dict):
                    logger.info("Skipping incomplete DL task payload: %s", rank_path)
                    malformed = True
                    break
                if not _payload_tasks_completed(payload):
                    payload = _normalize_flat_rank_payload(payload, rank_path.stem)
                if payload is None or not _payload_tasks_completed(payload):
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
        flat_flags = [
            rank_file.payload.get(FLAT_SCHEMA_MARKER) is True
            for rank_file in current_run
        ]
        if any(flat_flags) and (
            not all(flat_flags)
            or len(current_run) != 8
            or {rank_file.rank for rank_file in current_run} != set(range(8))
        ):
            logger.warning(
                "Skipping flat DL run with incomplete rank coverage: %s", run_dir
            )
            continue
        if any(flat_flags) and tuple(
            len(rows) for rows in classify_rank_files(current_run)
        ) != FLAT_EXPECTED_ROW_COUNTS:
            logger.warning(
                "Skipping flat DL run with unexpected metric cardinality: %s",
                run_dir,
            )
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


def _normalize_flat_rank_payload(
    payload: dict[str, Any],
    run_id: str,
) -> dict[str, Any] | None:
    """Normalize the pre-registry flat 80gb-b200 rank JSON shape."""

    test_plan = payload.get("test_plan")
    overlap = payload.get("overlap_tasks")
    if test_plan != FLAT_TEST_PLAN or not isinstance(overlap, dict):
        return None
    groups: dict[str, list[dict[str, Any]]] = {
        "nn_tasks": [],
        "f_tasks": [],
        "coll_tasks": [],
        "overlap_tasks": [],
    }
    reserved = {"PartitionKey", "test_plan", "overlap_tasks"}
    for task_name, raw_metrics in payload.items():
        if task_name in reserved:
            continue
        if not isinstance(task_name, str) or not isinstance(raw_metrics, dict):
            return None
        metrics = dict(raw_metrics)
        if task_name.startswith("f."):
            group = "f_tasks"
            if frozenset(metrics) != FLAT_F_METRICS:
                return None
        elif task_name.startswith(
            ("allgather_", "allreduce_", "alltoall_", "reducescatter_")
        ):
            group = "coll_tasks"
            if frozenset(metrics) != FLAT_COLLECTIVE_METRICS:
                return None
            if ("coll_cpu" in metrics and "cpu_time" in metrics) or (
                "coll_gpu" in metrics and "gpu_time" in metrics
            ):
                return None
            if "coll_cpu" in metrics:
                metrics["cpu_time"] = metrics.pop("coll_cpu")
            if "coll_gpu" in metrics:
                metrics["gpu_time"] = metrics.pop("coll_gpu")
        else:
            group = "nn_tasks"
            if frozenset(metrics) not in FLAT_NN_METRIC_SETS:
                return None
        if not metrics or not all(is_metric_value(value) for value in metrics.values()):
            return None
        groups[group].append(
            {
                "task_name": task_name,
                "status": "completed",
                **metrics,
            }
        )

    if any(
        len(groups[group]) != FLAT_TASK_COUNTS[group]
        for group in ("nn_tasks", "f_tasks", "coll_tasks")
    ) or len(overlap) != FLAT_TASK_COUNTS["overlap_layers"]:
        return None
    nn_names = {task["task_name"] for task in groups["nn_tasks"]}
    collective_names = {task["task_name"] for task in groups["coll_tasks"]}
    for layer_name, raw_metrics in overlap.items():
        if not isinstance(layer_name, str) or not isinstance(raw_metrics, dict):
            return None
        if layer_name not in nn_names:
            return None
        pairs: dict[str, dict[str, float]] = {}
        for metric_name, raw_value in raw_metrics.items():
            match = FLAT_OVERLAP_METRIC_PATTERN.fullmatch(str(metric_name))
            if match is None or not is_metric_value(raw_value):
                return None
            field = "layer_mean" if match.group("stat") == "mean" else "layer_stdev"
            pairs.setdefault(match.group("collective"), {})[field] = float(raw_value)
        if (
            len(pairs) not in FLAT_OVERLAP_COLLECTIVE_COUNTS
            or not set(pairs).issubset(collective_names)
        ):
            return None
        for collective, metrics in sorted(pairs.items()):
            if set(metrics) != {"layer_mean", "layer_stdev"}:
                return None
            groups["overlap_tasks"].append(
                {
                    "task_name": f"{collective}+{layer_name}",
                    "status": "completed",
                    "coll_name": collective,
                    "layer_name": layer_name,
                    **metrics,
                }
            )
    if not all(groups[group] for group in TASK_GROUPS):
        return None
    return {
        "runID": run_id,
        "test_plan": test_plan,
        FLAT_SCHEMA_MARKER: True,
        **groups,
    }


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


def _strict_nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


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


def write_standard_db(
    db_path: Path,
    table_name: str,
    rows: list[StandardMetricRow],
    *,
    replace_run_keys: set[str] | None = None,
    reconcile_root: Path | None = None,
    generation_id: str | None = None,
    generation_state: str = DL_INGEST_STATE_COMPLETE,
    run_receipts: dict[str, str] | None = None,
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
        _ensure_ingested_runs_table(connection, table_name)
        run_keys = set(replace_run_keys or set())
        if reconcile_root is not None:
            run_keys.update(_run_keys_in_scope(connection, table_name, reconcile_root))
        _delete_run_keys(connection, table_name, run_keys)
        _delete_ingested_run_keys(connection, run_keys)
        connection.executemany(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                run_key, node, cval_timestamp, iterations, sample_dir, test_plan, dltest_run_id,
                rank, task_group, task_name, status, metric_name, metric_value, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row.__dict__.values()) for row in rows],
        )
        receipts = dict(run_receipts or {})
        for row in rows:
            receipts.setdefault(row.run_key, row.sample_dir)
        _write_ingested_run_receipts(connection, receipts)
        if generation_id:
            _write_ingest_generation(connection, generation_id, generation_state)
        connection.commit()


def write_overlap_db(
    db_path: Path,
    table_name: str,
    rows: list[OverlapMetricRow],
    *,
    replace_run_keys: set[str] | None = None,
    reconcile_root: Path | None = None,
    generation_id: str | None = None,
    generation_state: str = DL_INGEST_STATE_COMPLETE,
    run_receipts: dict[str, str] | None = None,
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
        _ensure_ingested_runs_table(connection, table_name)
        run_keys = set(replace_run_keys or set())
        if reconcile_root is not None:
            run_keys.update(_run_keys_in_scope(connection, table_name, reconcile_root))
        _delete_run_keys(connection, table_name, run_keys)
        _delete_ingested_run_keys(connection, run_keys)
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
        receipts = dict(run_receipts or {})
        for row in rows:
            receipts.setdefault(row.run_key, row.sample_dir)
        _write_ingested_run_receipts(connection, receipts)
        if generation_id:
            _write_ingest_generation(connection, generation_id, generation_state)
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


def _ensure_ingested_runs_table(
    connection: sqlite3.Connection,
    table_name: str,
) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cval_ingested_runs (
            run_key TEXT PRIMARY KEY,
            sample_dir TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        "INSERT OR IGNORE INTO cval_ingested_runs(run_key, sample_dir, updated_at) "
        f"SELECT run_key, MIN(sample_dir), ? FROM {table_name} GROUP BY run_key",
        (int(time.time()),),
    )


def _delete_ingested_run_keys(
    connection: sqlite3.Connection,
    run_keys: set[str],
) -> None:
    keys = sorted(run_keys)
    for start in range(0, len(keys), 500):
        chunk = keys[start : start + 500]
        placeholders = ",".join("?" for _ in chunk)
        connection.execute(
            f"DELETE FROM cval_ingested_runs WHERE run_key IN ({placeholders})",
            chunk,
        )


def _write_ingested_run_receipts(
    connection: sqlite3.Connection,
    receipts: dict[str, str],
) -> None:
    now = int(time.time())
    values: list[tuple[str, str, int]] = []
    for run_key, sample_dir in sorted(receipts.items()):
        if not run_key or not sample_dir:
            raise ValueError("DL ingestion receipt fields must be non-empty")
        values.append((run_key, sample_dir, now))
    connection.executemany(
        "INSERT INTO cval_ingested_runs(run_key, sample_dir, updated_at) "
        "VALUES (?, ?, ?) ON CONFLICT(run_key) DO UPDATE SET "
        "sample_dir=excluded.sample_dir, updated_at=excluded.updated_at",
        values,
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
    receipts_table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='cval_ingested_runs'"
    ).fetchone()
    if receipts_table is not None:
        rows.extend(
            connection.execute(
                "SELECT run_key, sample_dir FROM cval_ingested_runs"
            ).fetchall()
        )
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
    state: str = DL_INGEST_STATE_COMPLETE,
) -> None:
    if state not in {DL_INGEST_STATE_IN_PROGRESS, DL_INGEST_STATE_COMPLETE}:
        raise ValueError(f"Invalid DL ingest generation state: {state}")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cval_ingest_metadata (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            generation_id TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(cval_ingest_metadata)")
    }
    if "state" not in columns:
        connection.execute(
            "ALTER TABLE cval_ingest_metadata ADD COLUMN state TEXT "
            "NOT NULL DEFAULT 'complete'"
        )
    connection.execute(
        "INSERT INTO cval_ingest_metadata(id, generation_id, updated_at, state) "
        "VALUES (1, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
        "generation_id=excluded.generation_id, updated_at=excluded.updated_at, "
        "state=excluded.state",
        (generation_id, int(time.time()), state),
    )


def validate_dl_metric_generation(db_paths: dict[str, str | Path]) -> str | None:
    """Require all generation-aware DL DBs to expose one completed generation."""

    from cval.storage.sqlite_uri import connect_sqlite_file

    generations: dict[str, tuple[str, str] | None] = {}
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
            columns = {
                str(item[1])
                for item in connection.execute(
                    "PRAGMA table_info(cval_ingest_metadata)"
                )
            }
            select = (
                "SELECT generation_id, state FROM cval_ingest_metadata WHERE id=1"
                if "state" in columns
                else "SELECT generation_id, 'complete' FROM cval_ingest_metadata WHERE id=1"
            )
            row = connection.execute(select).fetchone()
            generations[component] = (
                (str(row[0]), str(row[1])) if row else None
            )
    present = {value for value in generations.values() if value is not None}
    if not present:
        return None  # Legacy DB set without generation metadata.
    if any(
        value is not None and value[1] != DL_INGEST_STATE_COMPLETE
        for value in generations.values()
    ):
        raise RuntimeError(
            f"DL metric DB refresh is not complete: {generations}"
        )
    generation_ids = {value[0] for value in present}
    if len(generation_ids) != 1 or any(value is None for value in generations.values()):
        raise RuntimeError(
            "DL metric DB refresh generation is incomplete or inconsistent: "
            f"{generations}"
        )
    return next(iter(generation_ids))


def ingest_dltest_run(
    results_root: str | Path,
    *,
    node: str,
    timestamp: object,
    config: CvalConfig,
    _authorization: ResultWriteAuthorization | None = None,
) -> dict[str, Any]:
    """Ingest exactly one complete passing canonical DL run."""

    root = Path(results_root).expanduser()
    if not root.is_absolute():
        root = Path.cwd() / root
    root = Path(*root.parts)
    paths = default_dl_metric_db_paths(config)
    validate_current_dl_write(
        _authorization,
        node=node,
        timestamp=timestamp,
        results_root=root,
        db_paths=paths,
    )
    validated_root = safe_existing_evidence_path(
        root,
        expected_path=root,
        allowed_root=config.runtime.dl_results_root_path,
        expect_directory=True,
        description="current DL run evidence",
    )
    _validate_dl_evidence_tree(validated_root)
    rank_files = list(load_rank_files(validated_root))
    if not rank_files:
        raise ValueError("Current DL run has no complete rank evidence")
    run_keys = {rank_file.run_key for rank_file in rank_files}
    identities = {
        (rank_file.node, rank_file.cval_timestamp) for rank_file in rank_files
    }
    from cval.storage.write_provenance import _timestamp_epoch

    expected_identity = (node, _timestamp_epoch(timestamp))
    if len(run_keys) != 1 or identities != {expected_identity}:
        raise ValueError("Current DL evidence does not contain exactly one requested run")

    numerical_rows, compute_rows, collective_rows, overlap_rows = classify_rank_files(
        rank_files
    )
    component_rows = {
        "numerical_correctness": numerical_rows,
        "compute_performance": compute_rows,
        "collective_performance": collective_rows,
        "overlap_performance": overlap_rows,
    }
    empty = [component for component, rows in component_rows.items() if not rows]
    if empty:
        raise ValueError(
            "Current DL run produced no rows for required components: "
            + ", ".join(empty)
        )

    generation_id = f"{int(time.time() * 1_000_000)}-{uuid.uuid4().hex}"
    safe_paths = {
        component: safe_writable_file_path(path)
        for component, path in paths.items()
    }
    run_receipts = {
        rank_file.run_key: rank_file.sample_dir for rank_file in rank_files
    }
    _write_dl_rows(
        safe_paths,
        (numerical_rows, compute_rows, collective_rows, overlap_rows),
        run_keys,
        generation_id,
        run_receipts,
        generation_state=DL_INGEST_STATE_IN_PROGRESS,
    )
    _assert_equal_dl_receipts(safe_paths)
    _finalize_dl_generation(safe_paths, generation_id)
    return {
        "results_root": str(root),
        "db_paths": {name: str(path) for name, path in safe_paths.items()},
        "generation_id": generation_id,
        "rank_files": len(rank_files),
        "runs": 1,
        "numerical_correctness_rows": len(numerical_rows),
        "compute_performance_rows": len(compute_rows),
        "collective_performance_rows": len(collective_rows),
        "overlap_performance_rows": len(overlap_rows),
    }


def ingest_dltest_results(
    results_root: str | Path | None = None,
    output_dir: str | Path | None = None,
    *,
    config: CvalConfig | None = None,
    only_missing: bool = False,
    _authorization: DlRebuildAuthorization | None = None,
) -> dict[str, Any]:
    """Reconcile DL rank JSONs in bounded batches and return a summary."""

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
        allowed_root=authorization.evidence_root,
        expect_directory=True,
        description="DL rebuild results root",
    )
    _validate_dl_evidence_tree(validated_root)
    paths = {
        component: safe_writable_file_path(path)
        for component, path in paths.items()
    }
    existing_files = {component: path.is_file() for component, path in paths.items()}
    generation_id = f"{int(time.time() * 1_000_000)}-{uuid.uuid4().hex}"
    if only_missing and any(existing_files.values()):
        if not all(existing_files.values()):
            raise ValueError("DL only-missing requires all four metric DBs or none")
        _initialize_existing_dl_state(paths, generation_id)
    complete_run_keys = _complete_stored_run_keys(paths) if only_missing else set()
    valid_run_keys: set[str] = set()
    processed_run_keys: set[str] = set()
    batch: list[RankFile] = []
    batch_run_keys: set[str] = set()
    counts: Counter[str] = Counter()
    processed_rank_files = 0

    def flush_batch() -> None:
        nonlocal batch, batch_run_keys, processed_rank_files
        if not batch:
            return
        rows = _deduplicate_component_rows(classify_rank_files(batch))
        run_receipts = {
            rank_file.run_key: rank_file.sample_dir for rank_file in batch
        }
        _write_dl_rows(
            paths,
            rows,
            batch_run_keys,
            generation_id,
            run_receipts,
            generation_state=DL_INGEST_STATE_IN_PROGRESS,
        )
        processed_rank_files += len(batch)
        processed_run_keys.update(batch_run_keys)
        for component, values in zip(
            (
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            ),
            rows,
            strict=True,
        ):
            counts[component] += len(values)
        batch = []
        batch_run_keys = set()

    for run_dir in find_dl_run_dirs(validated_root):
        current = list(load_rank_files(run_dir))
        if not current:
            continue
        current_keys = {rank_file.run_key for rank_file in current}
        duplicate_keys = current_keys & valid_run_keys
        if duplicate_keys:
            raise ValueError(
                "Duplicate DL run key appears in multiple evidence directories: "
                + ", ".join(sorted(duplicate_keys))
            )
        valid_run_keys.update(current_keys)
        if only_missing and current_keys <= complete_run_keys:
            continue
        batch.extend(current)
        batch_run_keys.update(current_keys)
        if len(batch_run_keys) >= DL_INGEST_BATCH_RUNS:
            flush_batch()
    flush_batch()

    if not valid_run_keys and not all(existing_files.values()):
        raise ValueError(
            "DL rebuild found no valid rank evidence and not all four metric DBs exist"
        )
    if not only_missing:
        _finish_dl_reconciliation(
            paths,
            validated_root,
            valid_run_keys,
            generation_id,
        )
    _assert_equal_dl_receipts(paths)
    _finalize_dl_generation(paths, generation_id)

    return {
        "results_root": str(root),
        "output_dir": output_label,
        "db_paths": {name: str(path) for name, path in paths.items()},
        "generation_id": generation_id,
        "rank_files": processed_rank_files,
        "runs": len(processed_run_keys),
        "discovered_runs": len(valid_run_keys),
        "skipped_existing_runs": len(valid_run_keys & complete_run_keys),
        "only_missing": only_missing,
        "numerical_correctness_rows": counts["numerical_correctness"],
        "compute_performance_rows": counts["compute_performance"],
        "collective_performance_rows": counts["collective_performance"],
        "overlap_performance_rows": counts["overlap_performance"],
    }


def _stored_run_keys(db_path: Path, table_name: str) -> set[str]:
    if not db_path.is_file():
        return set()
    from cval.storage.sqlite_uri import connect_sqlite_file

    with closing(connect_sqlite_file(db_path, mode="ro", timeout=30)) as connection:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if table is None:
            return set()
        receipts = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='cval_ingested_runs'"
        ).fetchone()
        source = (
            "SELECT run_key FROM cval_ingested_runs"
            if receipts is not None
            else f'SELECT DISTINCT run_key FROM "{table_name}"'
        )
        return {str(row[0]) for row in connection.execute(source)}


def _complete_stored_run_keys(paths: dict[str, Path]) -> set[str]:
    stored = [
        _stored_run_keys(paths[component], component)
        for component in (
            "numerical_correctness",
            "compute_performance",
            "collective_performance",
            "overlap_performance",
        )
    ]
    return set.intersection(*stored) if stored else set()


def _initialize_existing_dl_state(
    paths: dict[str, Path],
    generation_id: str,
) -> None:
    """Validate/migrate all existing DL DBs and establish one generation."""

    from cval.storage.sqlite_uri import connect_sqlite_file

    for component, path in paths.items():
        with closing(connect_sqlite_file(path, mode="ro", timeout=30)) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (component,),
            ).fetchone()
            if table is None:
                raise ValueError(
                    f"DL metric DB is missing required table {component}: {path}"
                )
            columns = {
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{component}")')
            }
            required = (
                _OVERLAP_REQUIRED_COLUMNS
                if component == "overlap_performance"
                else _STANDARD_REQUIRED_COLUMNS
            )
            missing = sorted(required - columns)
            if missing:
                raise ValueError(
                    f"DL metric DB table {component} is missing required columns: "
                    + ", ".join(missing)
                )

    for component, path in paths.items():
        with closing(connect(path)) as connection:
            ensure_iterations_column(connection, component)
            _ensure_ingested_runs_table(connection, component)
            _write_ingest_generation(
                connection,
                generation_id,
                DL_INGEST_STATE_IN_PROGRESS,
            )
            connection.commit()


def _deduplicate_component_rows(
    rows: tuple[
        list[StandardMetricRow],
        list[StandardMetricRow],
        list[StandardMetricRow],
        list[OverlapMetricRow],
    ],
) -> tuple[
    list[StandardMetricRow],
    list[StandardMetricRow],
    list[StandardMetricRow],
    list[OverlapMetricRow],
]:
    deduplicated: list[list[Any]] = []
    for component_rows in rows:
        unique: dict[tuple[object, ...], Any] = {}
        for row in component_rows:
            key = (
                row.run_key,
                row.rank,
                row.task_group,
                row.task_name,
                row.metric_name,
            )
            prior = unique.get(key)
            if prior is not None:
                prior_values = prior.__dict__ | {"source_file": ""}
                row_values = row.__dict__ | {"source_file": ""}
                if prior_values != row_values:
                    raise ValueError(
                        f"Conflicting duplicate DL metric evidence: {key}"
                    )
                continue
            unique[key] = row
        deduplicated.append(list(unique.values()))
    return tuple(deduplicated)  # type: ignore[return-value]


def _write_dl_rows(
    paths: dict[str, Path],
    rows: tuple[
        list[StandardMetricRow],
        list[StandardMetricRow],
        list[StandardMetricRow],
        list[OverlapMetricRow],
    ],
    run_keys: set[str],
    generation_id: str,
    run_receipts: dict[str, str],
    generation_state: str,
) -> None:
    numerical_rows, compute_rows, collective_rows, overlap_rows = rows
    write_standard_db(
        paths["numerical_correctness"],
        "numerical_correctness",
        numerical_rows,
        replace_run_keys=run_keys,
        generation_id=generation_id,
        generation_state=generation_state,
        run_receipts=run_receipts,
    )
    write_standard_db(
        paths["compute_performance"],
        "compute_performance",
        compute_rows,
        replace_run_keys=run_keys,
        generation_id=generation_id,
        generation_state=generation_state,
        run_receipts=run_receipts,
    )
    write_standard_db(
        paths["collective_performance"],
        "collective_performance",
        collective_rows,
        replace_run_keys=run_keys,
        generation_id=generation_id,
        generation_state=generation_state,
        run_receipts=run_receipts,
    )
    write_overlap_db(
        paths["overlap_performance"],
        "overlap_performance",
        overlap_rows,
        replace_run_keys=run_keys,
        generation_id=generation_id,
        generation_state=generation_state,
        run_receipts=run_receipts,
    )


def _finish_dl_reconciliation(
    paths: dict[str, Path],
    root: Path,
    valid_run_keys: set[str],
    generation_id: str,
) -> None:
    from cval.storage.sqlite_uri import connect_sqlite_file

    stale: dict[str, set[str]] = {}
    for component, path in paths.items():
        if not path.is_file():
            stale[component] = set()
            continue
        with closing(connect_sqlite_file(path, mode="ro", timeout=30)) as connection:
            stale[component] = _run_keys_in_scope(connection, component, root) - valid_run_keys
    write_standard_db(
        paths["numerical_correctness"],
        "numerical_correctness",
        [],
        replace_run_keys=stale["numerical_correctness"],
        generation_id=generation_id,
        generation_state=DL_INGEST_STATE_IN_PROGRESS,
    )
    write_standard_db(
        paths["compute_performance"],
        "compute_performance",
        [],
        replace_run_keys=stale["compute_performance"],
        generation_id=generation_id,
        generation_state=DL_INGEST_STATE_IN_PROGRESS,
    )
    write_standard_db(
        paths["collective_performance"],
        "collective_performance",
        [],
        replace_run_keys=stale["collective_performance"],
        generation_id=generation_id,
        generation_state=DL_INGEST_STATE_IN_PROGRESS,
    )
    write_overlap_db(
        paths["overlap_performance"],
        "overlap_performance",
        [],
        replace_run_keys=stale["overlap_performance"],
        generation_id=generation_id,
        generation_state=DL_INGEST_STATE_IN_PROGRESS,
    )


def _receipt_sets(paths: dict[str, Path]) -> dict[str, set[str]]:
    return {
        component: _stored_run_keys(path, component)
        for component, path in paths.items()
    }


def _assert_equal_dl_receipts(paths: dict[str, Path]) -> None:
    receipts = _receipt_sets(paths)
    unique = {frozenset(values) for values in receipts.values()}
    if len(unique) != 1:
        counts = {component: len(values) for component, values in receipts.items()}
        raise RuntimeError(
            f"DL metric DB run receipts are incomplete or inconsistent: {counts}"
        )


def _finalize_dl_generation(paths: dict[str, Path], generation_id: str) -> None:
    for path in paths.values():
        with closing(connect(path)) as connection:
            _write_ingest_generation(
                connection,
                generation_id,
                DL_INGEST_STATE_COMPLETE,
            )
            connection.commit()


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
