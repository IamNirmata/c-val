"""Ingest DL unit-test rank JSON outputs into c-val SQLite metric DBs.

The DL artifact layout has changed over time. The scanner is intentionally
recursive and accepts both of these shapes:

  /data/continuous_validation/dltest/<node>/dltest-<node>-<timestamp>/workdir/test_plans/<plan>/runs/*.json

The four output DBs mirror the DL metric categories used by baseline building:
``numerical_correctness``, ``compute_performance``, ``collective_performance``,
and ``overlap_performance``.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from cval.config import load_config

COMPUTE_METRICS = frozenset(("fp_cpu_time", "fp_gpu_time", "bp_cpu_time", "bp_gpu_time"))
COLLECTIVE_METRICS = frozenset(("cpu_time", "gpu_time"))
OVERLAP_METRICS = frozenset(("coll_mean", "coll_stdev", "layer_mean", "layer_stdev"))
METADATA_FIELDS = frozenset(("task_name", "status", "error_msg", "coll_name", "layer_name"))
TASK_GROUPS = ("nn_tasks", "f_tasks", "coll_tasks", "overlap_tasks")
RANK_PATTERN = re.compile(r"(?:^|_)rank(?P<rank>\d+)(?:_|$)", re.IGNORECASE)
RUN_DIR_PATTERN = re.compile(r"^dltest-(?P<node>.+)-(?P<timestamp>\d+)$")
HISTORICAL_DL_ITERATIONS = 20


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


def default_dl_metric_db_paths() -> dict[str, Path]:
    """Return configured output paths for the four DL metric DBs."""

    config = load_config()
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
    if not match:
        return run_dir.name, "", None
    return run_dir.name, match.group("node"), int(match.group("timestamp"))


def parse_rank(run_id: str) -> int:
    """Extract rank from a run id or filename stem."""

    match = RANK_PATTERN.search(run_id)
    return int(match.group("rank")) if match else -1


def find_dl_run_dirs(results_root: Path) -> list[Path]:
    """Return DL run directories below ``results_root`` that contain rank JSONs."""

    if not results_root.exists():
        return []
    candidates = [path for path in results_root.rglob("dltest-*") if path.is_dir()]
    run_dirs = []
    for candidate in sorted(candidates):
        if any(candidate.glob("workdir/test_plans/*/runs/*.json")):
            run_dirs.append(candidate)
    return run_dirs


def dl_run_iterations(run_dir: Path) -> int:
    """Read the run's summary iteration count, falling back for old artifacts."""

    for summary_path in sorted(run_dir.glob("dltest-summary-*.json")):
        try:
            value = json.loads(summary_path.read_text(encoding="utf-8")).get("iterations")
            if value is not None and int(value) > 0:
                return int(value)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    return HISTORICAL_DL_ITERATIONS


def load_rank_files(results_root: Path) -> Iterable[RankFile]:
    """Yield rank JSON payloads from all c-val DL run directories."""

    for run_dir in find_dl_run_dirs(results_root):
        run_key, node, cval_timestamp = parse_run_dir(run_dir)
        iterations = dl_run_iterations(run_dir)
        for runs_dir in sorted(run_dir.glob("workdir/test_plans/*/runs")):
            for rank_path in sorted(runs_dir.glob("*.json")):
                payload = json.loads(rank_path.read_text(encoding="utf-8"))
                dltest_run_id = str(payload.get("runID", rank_path.stem))
                yield RankFile(
                    run_key=run_key,
                    node=node,
                    cval_timestamp=cval_timestamp,
                    iterations=iterations,
                    sample_dir=str(run_dir),
                    test_plan=str(payload.get("test_plan", runs_dir.parent.name)),
                    dltest_run_id=dltest_run_id,
                    rank=parse_rank(dltest_run_id),
                    path=rank_path,
                    payload=payload,
                )


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

    task_name = str(task.get("task_name", ""))
    status = str(task.get("status", ""))
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
    for metric_name in OVERLAP_METRICS:
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
                task_name=str(task.get("task_name", "")),
                status=str(task.get("status", "")),
                coll_name=str(task.get("coll_name", "")),
                layer_name=str(task.get("layer_name", "")),
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
            overlap_rows.extend(overlap_metric_rows(rank_file, task))

    return numerical_rows, compute_rows, collective_rows, overlap_rows


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
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
    else:
        connection.execute(
            f"UPDATE {table_name} SET iterations = ? WHERE iterations IS NULL",
            (int(historical_iterations),),
        )


def write_standard_db(db_path: Path, table_name: str, rows: list[StandardMetricRow]) -> None:
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
        connection.executemany(
            f"""
            INSERT OR REPLACE INTO {table_name} (
                run_key, node, cval_timestamp, iterations, sample_dir, test_plan, dltest_run_id,
                rank, task_group, task_name, status, metric_name, metric_value, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row.__dict__.values()) for row in rows],
        )
        connection.commit()


def write_overlap_db(db_path: Path, table_name: str, rows: list[OverlapMetricRow]) -> None:
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
        connection.commit()


def migrate_dltest_iterations(
    output_dir: str | Path | None = None,
    *,
    historical_iterations: int = HISTORICAL_DL_ITERATIONS,
) -> dict[str, int]:
    """Add/backfill ``iterations`` on every existing DL metric DB table."""

    if historical_iterations <= 0:
        raise ValueError("historical_iterations must be positive")
    output = (
        Path(output_dir)
        if output_dir is not None
        else default_dl_metric_db_paths()["numerical_correctness"].parent
    )
    specs = {
        "numerical_correctness": output / "dltest_numerical_correctness.db",
        "compute_performance": output / "dltest_compute_performance.db",
        "collective_performance": output / "dltest_collective_performance.db",
        "overlap_performance": output / "dltest_overlap_performance.db",
    }
    summary: dict[str, int] = {}
    for table_name, db_path in specs.items():
        if not db_path.exists():
            summary[table_name] = 0
            continue
        with closing(connect(db_path)) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            if table_name not in tables:
                summary[table_name] = 0
                continue
            ensure_iterations_column(connection, table_name, historical_iterations)
            connection.commit()
            summary[table_name] = int(
                connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            )
    return summary


def ingest_dltest_results(
    results_root: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, int | str]:
    """Ingest DL rank JSONs and return a summary."""

    root = Path(results_root) if results_root is not None else default_dl_results_root()
    output = Path(output_dir) if output_dir is not None else default_dl_metric_db_paths()["numerical_correctness"].parent
    rank_files = list(load_rank_files(root.expanduser().resolve()))
    if not rank_files:
        raise FileNotFoundError(f"No DL test rank JSON files found under {root}")

    numerical_rows, compute_rows, collective_rows, overlap_rows = classify_rank_files(rank_files)
    paths = {
        "numerical_correctness": output / "dltest_numerical_correctness.db",
        "compute_performance": output / "dltest_compute_performance.db",
        "collective_performance": output / "dltest_collective_performance.db",
        "overlap_performance": output / "dltest_overlap_performance.db",
    }
    write_standard_db(paths["numerical_correctness"], "numerical_correctness", numerical_rows)
    write_standard_db(paths["compute_performance"], "compute_performance", compute_rows)
    write_standard_db(paths["collective_performance"], "collective_performance", collective_rows)
    write_overlap_db(paths["overlap_performance"], "overlap_performance", overlap_rows)

    run_counts = Counter(rank_file.run_key for rank_file in rank_files)
    return {
        "results_root": str(root),
        "output_dir": str(output),
        "rank_files": len(rank_files),
        "runs": len(run_counts),
        "numerical_correctness_rows": len(numerical_rows),
        "compute_performance_rows": len(compute_rows),
        "collective_performance_rows": len(collective_rows),
        "overlap_performance_rows": len(overlap_rows),
    }
