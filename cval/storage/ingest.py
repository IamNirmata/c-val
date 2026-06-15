"""SQLite ingestion helpers used inside validation pods.

The read side of c-val opens SQLite in `mode=ro`; this module is the explicit
write side used after a validation pod has finished running deterministic tests.
It replaces the legacy `utils/functions.py` DB commands with package-native,
testable functions.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from contextlib import closing
from pathlib import Path

from cval.config import load_config

VALID_STATUSES = {"pass", "fail", "incomplete"}
_CONFIG = load_config()
DEFAULT_VALIDATION_DB_PATH = _CONFIG.storage.validation_db_path
DEFAULT_STORAGE_DB_PATH = _CONFIG.storage.storage_db_path
DEFAULT_NCCL_DB_PATH = _CONFIG.storage.nccl_db_path

STORAGE_FILE_PREFIXES = {
    "iodepth_read_1file.json": "iodepth_read_1file",
    "iodepth_write_1file.json": "iodepth_write_1file",
    "numjobs_read_nfiles.json": "numjobs_read_nfiles",
    "numjobs_write_nfiles.json": "numjobs_write_nfiles",
    "randread.json": "randread",
    "randwrite.json": "randwrite",
}

STORAGE_METRIC_COLUMNS = tuple(
    f"{prefix}_{metric}"
    for prefix in STORAGE_FILE_PREFIXES.values()
    for metric in ("iops", "bw")
)


def parse_timestamp(value: object | None) -> int:
    """Parse c-val timestamps from epoch, ISO-8601, or `YYYYMMDD_HHMMSS` strings."""

    if value is None:
        return int(dt.datetime.now(dt.timezone.utc).timestamp())
    if isinstance(value, int | float):
        return int(value)

    timestamp = str(value).strip()
    if timestamp.isdigit():
        return int(timestamp)

    try:
        iso_timestamp = timestamp[:-1] + "+00:00" if timestamp.endswith("Z") else timestamp
        parsed = dt.datetime.fromisoformat(iso_timestamp)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        pass

    try:
        parsed = dt.datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
        return int(parsed.replace(tzinfo=dt.timezone.utc).timestamp())
    except ValueError:
        pass

    raise ValueError(f"Could not parse timestamp: {value!r}")


def add_validation_result(
    node: str,
    test: str,
    result: str,
    timestamp: object | None,
    image_name: str = "",
    db_path: str | Path = DEFAULT_VALIDATION_DB_PATH,
) -> int:
    """Append one validation result row and return the parsed timestamp."""

    parsed_timestamp = parse_timestamp(timestamp)
    if result not in VALID_STATUSES:
        raise ValueError(f"result must be one of {sorted(VALID_STATUSES)}")

    with closing(_connect_writable(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
              node TEXT NOT NULL,
              test TEXT NOT NULL,
              timestamp INTEGER NOT NULL,
              result TEXT NOT NULL CHECK (result IN ('pass','fail','incomplete')),
              image_name TEXT NOT NULL DEFAULT ''
            )
            """
        )
        _ensure_column(connection, "runs", "image_name", "TEXT NOT NULL DEFAULT ''")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_runs_node_test_ts ON runs(node, test, timestamp)"
        )
        connection.execute(
            "INSERT INTO runs(node, test, timestamp, result, image_name) VALUES (?,?,?,?,?)",
            (node, test, parsed_timestamp, result, image_name),
        )
        connection.commit()
    return parsed_timestamp


def add_storage_result(
    node: str,
    timestamp: object,
    results_dir: str | Path,
    image_name: str = "",
    db_path: str | Path = DEFAULT_STORAGE_DB_PATH,
) -> int:
    """Parse fio JSON artifacts and upsert one row into the storage metrics DB."""

    parsed_timestamp = parse_timestamp(timestamp)
    metrics = parse_storage_metrics(results_dir)
    columns = ", ".join(STORAGE_METRIC_COLUMNS)
    placeholders = ", ".join("?" for _ in STORAGE_METRIC_COLUMNS)

    with closing(_connect_writable(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS storage_performance (
                node TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                image_name TEXT NOT NULL DEFAULT '',
                iodepth_read_1file_iops REAL, iodepth_read_1file_bw REAL,
                iodepth_write_1file_iops REAL, iodepth_write_1file_bw REAL,
                numjobs_read_nfiles_iops REAL, numjobs_read_nfiles_bw REAL,
                numjobs_write_nfiles_iops REAL, numjobs_write_nfiles_bw REAL,
                randread_iops REAL, randread_bw REAL,
                randwrite_iops REAL, randwrite_bw REAL,
                PRIMARY KEY (node, timestamp)
            )
            """
        )
        _ensure_column(
            connection,
            "storage_performance",
            "image_name",
            "TEXT NOT NULL DEFAULT ''",
        )
        connection.execute(
            f"""
            INSERT OR REPLACE INTO storage_performance (node, timestamp, image_name, {columns})
            VALUES (?, ?, ?, {placeholders})
            """,
            (
                node,
                parsed_timestamp,
                image_name,
                *(metrics[column] for column in STORAGE_METRIC_COLUMNS),
            ),
        )
        connection.commit()
    return parsed_timestamp


def parse_storage_metrics(results_dir: str | Path) -> dict[str, float]:
    """Return fio IOPS and bandwidth metrics from a storage result directory."""

    directory = Path(results_dir)
    if not directory.exists():
        raise FileNotFoundError(f"Storage results directory not found: {directory}")

    metrics = {column: 0.0 for column in STORAGE_METRIC_COLUMNS}
    for filename, prefix in STORAGE_FILE_PREFIXES.items():
        result_file = directory / filename
        if not result_file.exists():
            continue
        data = json.loads(result_file.read_text(encoding="utf-8"))
        job = data.get("jobs", [{}])[0]
        read = job.get("read", {})
        write = job.get("write", {})
        metrics[f"{prefix}_iops"] = float(read.get("iops", 0.0)) + float(
            write.get("iops", 0.0)
        )
        metrics[f"{prefix}_bw"] = float(read.get("bw", 0.0)) + float(write.get("bw", 0.0))
    return metrics


def add_nccl_result(
    node: str,
    timestamp: object,
    busbw: float | str,
    latency: float | str,
    image_name: str = "",
    db_path: str | Path = DEFAULT_NCCL_DB_PATH,
) -> int:
    """Upsert one NCCL metric row and return the parsed timestamp."""

    parsed_timestamp = parse_timestamp(timestamp)
    with closing(_connect_writable(db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nccl_performance (
                node TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                image_name TEXT NOT NULL DEFAULT '',
                busbw REAL,
                latency REAL,
                PRIMARY KEY (node, timestamp)
            )
            """
        )
        _ensure_column(
            connection,
            "nccl_performance",
            "image_name",
            "TEXT NOT NULL DEFAULT ''",
        )
        connection.execute(
            "INSERT OR REPLACE INTO nccl_performance "
            "(node, timestamp, image_name, busbw, latency) VALUES (?, ?, ?, ?, ?)",
            (node, parsed_timestamp, image_name, float(busbw), float(latency)),
        )
        connection.commit()
    return parsed_timestamp


def _connect_writable(db_path: str | Path) -> sqlite3.Connection:
    """Open SQLite in create-if-needed mode and prepare parent directories."""

    path = Path(db_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(f"file:{path}?mode=rwc", uri=True, timeout=30)
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


def _ensure_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    """Add a column to an existing SQLite table when older DBs lack it."""

    existing_columns = {
        row[1] for row in connection.execute(f"PRAGMA table_info({table_name})")
    }
    if column_name not in existing_columns:
        connection.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
        )
