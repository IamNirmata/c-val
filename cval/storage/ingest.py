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
from zoneinfo import ZoneInfo

from cval.config import load_config

VALID_STATUSES = {"pass", "fail", "incomplete"}
_CONFIG = load_config()
DEFAULT_VALIDATION_DB_PATH = _CONFIG.storage.validation_db_path
DEFAULT_STORAGE_DB_PATH = _CONFIG.storage.storage_db_path
DEFAULT_NCCL_DB_PATH = _CONFIG.storage.nccl_db_path

NCCL_HEALTH_TABLE = "IB_HEALTH"
NCCL_IB_PORT_COLUMNS = tuple(f"mlx5_{index}" for index in range(14))
NCCL_LATEST_STATUS_VIEW = "LATEST_NODE_STATUS"
NCCL_RANKING_VIEW = "NODE_RANKING"
LOS_ANGELES = ZoneInfo("America/Los_Angeles")

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


def timestamp_to_los_angeles(timestamp: int) -> str:
    """Return an epoch timestamp as an ISO-8601 America/Los_Angeles value."""

    return dt.datetime.fromtimestamp(int(timestamp), tz=LOS_ANGELES).isoformat()


def _create_nccl_health_table(connection: sqlite3.Connection) -> None:
    """Create the consolidated one-row-per-NCCL-run health table."""

    port_columns = ",\n                ".join(
        f"{column} REAL" for column in NCCL_IB_PORT_COLUMNS
    )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {NCCL_HEALTH_TABLE} (
            Node TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            la_timestamp TEXT NOT NULL,
            iterations INTEGER,
            image_name TEXT NOT NULL DEFAULT '',
            cuda TEXT NOT NULL DEFAULT '',
            pytorch TEXT NOT NULL DEFAULT '',
            samples INTEGER,
            BUS_BW REAL,
            LATENCY REAL,
            {port_columns},
            PRIMARY KEY (Node, timestamp)
        )
        """
    )


def _create_nccl_health_views(
    connection: sqlite3.Connection,
    *,
    replace: bool = False,
) -> None:
    """Create the latest-node and rolling five-run ranking views."""

    if replace:
        connection.execute(f"DROP VIEW IF EXISTS {NCCL_LATEST_STATUS_VIEW}")
        connection.execute(f"DROP VIEW IF EXISTS {NCCL_RANKING_VIEW}")

    connection.execute(
        f"""
        CREATE VIEW IF NOT EXISTS {NCCL_LATEST_STATUS_VIEW} AS
        SELECT health.*
        FROM {NCCL_HEALTH_TABLE} AS health
        INNER JOIN (
            SELECT Node, MAX(timestamp) AS latest_timestamp
            FROM {NCCL_HEALTH_TABLE}
            GROUP BY Node
        ) AS latest
          ON latest.Node = health.Node
         AND latest.latest_timestamp = health.timestamp
        ORDER BY health.Node
        """
    )

    port_averages = ",\n                ".join(
        f"AVG({column}) AS {column}" for column in NCCL_IB_PORT_COLUMNS
    )
    port_select = ",\n            ".join(NCCL_IB_PORT_COLUMNS)
    connection.execute(
        f"""
        CREATE VIEW IF NOT EXISTS {NCCL_RANKING_VIEW} AS
        WITH recent AS (
            SELECT
                health.*,
                ROW_NUMBER() OVER (
                    PARTITION BY Node
                    ORDER BY timestamp DESC
                ) AS recency
            FROM {NCCL_HEALTH_TABLE} AS health
        ),
        averages AS (
            SELECT
                Node AS node,
                AVG(BUS_BW) AS bus_bw,
                AVG(LATENCY) AS latency,
                {port_averages}
            FROM recent
            WHERE recency <= 5
            GROUP BY Node
        ),
        ranked AS (
            SELECT
                node,
                bus_bw,
                ROUND(100.0 * PERCENT_RANK() OVER (ORDER BY bus_bw), 2) AS bus_bw_pctl,
                latency,
                ROUND(100.0 * PERCENT_RANK() OVER (ORDER BY latency), 2) AS latency_pctl,
                {port_select}
            FROM averages
            WHERE bus_bw IS NOT NULL AND latency IS NOT NULL
        )
        SELECT *
        FROM ranked
        ORDER BY bus_bw ASC, node ASC
        """
    )


def _optional_float(value: object) -> float | None:
    """Convert a nullable numeric value to float without raising."""

    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: object) -> int | None:
    """Convert a nullable numeric value to int without raising."""

    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def add_nccl_health_result(
    node: str,
    timestamp: object,
    *,
    iterations: int | str | None,
    image_name: str = "",
    cuda_version: str = "",
    pytorch_version: str = "",
    samples: int | str | None = None,
    bus_bw: float | str | None = None,
    latency: float | str | None = None,
    port_max_gbps: dict[str, object] | None = None,
    db_path: str | Path = DEFAULT_NCCL_DB_PATH,
) -> int:
    """Upsert one consolidated ``IB_HEALTH`` row for an NCCL validation run."""

    parsed_timestamp = parse_timestamp(timestamp)
    ports = port_max_gbps or {}
    columns = (
        "Node",
        "timestamp",
        "la_timestamp",
        "iterations",
        "image_name",
        "cuda",
        "pytorch",
        "samples",
        "BUS_BW",
        "LATENCY",
        *NCCL_IB_PORT_COLUMNS,
    )
    values = (
        node,
        parsed_timestamp,
        timestamp_to_los_angeles(parsed_timestamp),
        _optional_int(iterations),
        image_name,
        cuda_version,
        pytorch_version,
        _optional_int(samples),
        _optional_float(bus_bw),
        _optional_float(latency),
        *(_optional_float(ports.get(column)) for column in NCCL_IB_PORT_COLUMNS),
    )
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(
        f"{column}=excluded.{column}" for column in columns if column not in {"Node", "timestamp"}
    )

    with closing(_connect_writable(db_path)) as connection:
        _create_nccl_health_table(connection)
        connection.execute(
            f"INSERT INTO {NCCL_HEALTH_TABLE} ({', '.join(columns)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT(Node, timestamp) DO UPDATE SET {updates}",
            values,
        )
        _create_nccl_health_views(connection)
        connection.commit()
    return parsed_timestamp


def add_nccl_health_from_summary(
    node: str,
    timestamp: object,
    summary_json_path: str | Path,
    *,
    iterations: int | str | None = None,
    image_name: str = "",
    cuda_version: str = "",
    pytorch_version: str = "",
    db_path: str | Path = DEFAULT_NCCL_DB_PATH,
) -> int:
    """Parse an NCCL summary JSON and upsert one consolidated health row."""

    payload = json.loads(Path(summary_json_path).read_text(encoding="utf-8"))
    ports = payload.get("GCR_IB_PORT_BW_GBPS", {})
    ports = ports if isinstance(ports, dict) else {}
    port_maxima: dict[str, object] = {}
    sample_counts: list[int] = []
    for column in NCCL_IB_PORT_COLUMNS:
        entry = ports.get(column)
        if not isinstance(entry, dict):
            continue
        port_maxima[column] = entry.get("max_gbps")
        count = _optional_int(entry.get("samples"))
        if count is not None:
            sample_counts.append(count)

    resolved_iterations = iterations
    if resolved_iterations is None:
        resolved_iterations = payload.get("GCR_ITERATIONS")

    return add_nccl_health_result(
        node,
        timestamp,
        iterations=resolved_iterations,
        image_name=image_name,
        cuda_version=cuda_version,
        pytorch_version=pytorch_version,
        samples=max(sample_counts) if sample_counts else None,
        bus_bw=payload.get("GCR_BUSBW"),
        latency=payload.get("GCR_LATENCY"),
        port_max_gbps=port_maxima,
        db_path=db_path,
    )


def add_validation_result(
    node: str,
    test: str,
    result: str,
    timestamp: object | None,
    image_name: str = "",
    pytorch_version: str = "",
    cuda_version: str = "",
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
              image_name TEXT NOT NULL DEFAULT '',
              pytorch_version TEXT NOT NULL DEFAULT '',
              cuda_version TEXT NOT NULL DEFAULT ''
            )
            """
        )
        _ensure_column(connection, "runs", "image_name", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(connection, "runs", "pytorch_version", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(connection, "runs", "cuda_version", "TEXT NOT NULL DEFAULT ''")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_runs_node_test_ts ON runs(node, test, timestamp)"
        )
        connection.execute(
            "INSERT INTO runs(node, test, timestamp, result, image_name, pytorch_version, cuda_version) "
            "VALUES (?,?,?,?,?,?,?)",
            (node, test, parsed_timestamp, result, image_name, pytorch_version, cuda_version),
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
