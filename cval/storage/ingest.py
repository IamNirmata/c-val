"""SQLite ingestion helpers used inside validation pods.

The read side of c-val opens SQLite in `mode=ro`; this module is the explicit
write side used after a validation pod has finished running deterministic tests.
It replaces the legacy `utils/functions.py` DB commands with package-native,
testable functions.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
import sqlite3
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from cval.config import StorageConfig
from cval.storage.paths import safe_existing_evidence_path, safe_writable_file_path
from cval.storage.write_provenance import (
    ResultWriteAuthorization,
    validate_current_write,
)

VALID_STATUSES = {"pass", "fail", "incomplete"}
_STORAGE_DEFAULTS = StorageConfig()
DEFAULT_VALIDATION_DB_PATH = _STORAGE_DEFAULTS.validation_db_path
DEFAULT_STORAGE_DB_PATH = _STORAGE_DEFAULTS.storage_db_path
DEFAULT_NCCL_DB_PATH = _STORAGE_DEFAULTS.nccl_db_path

NCCL_HEALTH_TABLE = "IB_HEALTH"
NCCL_IB_PORT_COLUMNS = tuple(f"mlx5_{index}" for index in range(14))
NCCL_LATEST_STATUS_VIEW = "LATEST_NODE_STATUS"
NCCL_RANKING_VIEW = "NODE_RANKING"
NCCL_PORT_LABEL_PATTERN = re.compile(
    r"^mlx5_(?:0|[1-9][0-9]*)(?:\.(?:[2-9]|[1-9][0-9]+))?$"
)
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


@dataclass(frozen=True)
class NcclHealthMetrics:
    """Validated values extracted from one NCCL summary artifact."""

    iterations: int
    data_size_gb: int | None
    samples: int | None
    bus_bw: float
    alg_bw: float
    latency: float
    port_max_gbps: dict[str, object]


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
            data_size_gb INTEGER,
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

    for sql in nccl_health_view_sql().values():
        connection.execute(sql)


def nccl_health_view_sql() -> dict[str, str]:
    """Return the exact stable SQL definitions of both current views."""

    latest = f"""
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

    port_averages = ",\n                ".join(
        f"AVG({column}) AS {column}" for column in NCCL_IB_PORT_COLUMNS
    )
    port_select = ",\n            ".join(NCCL_IB_PORT_COLUMNS)
    ranking = f"""
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
    return {
        NCCL_LATEST_STATUS_VIEW: latest,
        NCCL_RANKING_VIEW: ranking,
    }


def prepare_nccl_health_schema(
    connection: sqlite3.Connection,
    *,
    include_run_id: bool = False,
) -> None:
    """Prepare the stable wide NCCL table/views and optional canonical run ID."""

    _create_nccl_health_table(connection)
    _ensure_column(connection, NCCL_HEALTH_TABLE, "data_size_gb", "INTEGER")
    if include_run_id:
        _ensure_column(connection, NCCL_HEALTH_TABLE, "run_id", "TEXT")
        connection.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{NCCL_HEALTH_TABLE}_run_id "
            f"ON {NCCL_HEALTH_TABLE}(run_id) WHERE run_id IS NOT NULL"
        )
    _create_nccl_health_views(connection)


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


def _finite_non_negative_number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return parsed


def _non_negative_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


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
    run_id: str = "",
    immutable: bool = False,
    summary_json_path: str | Path | None = None,
    ibbw_log_path: str | Path | None = None,
    require_hca_samples: bool = False,
    db_path: str | Path = DEFAULT_NCCL_DB_PATH,
    _authorization: ResultWriteAuthorization | None = None,
) -> int:
    """Upsert one consolidated ``IB_HEALTH`` row for an NCCL validation run."""

    authorization = validate_current_write(
        _authorization,
        operation="nccl",
        node=node,
        timestamp=timestamp,
        db_path=db_path,
        image_name=image_name,
        pytorch_version=pytorch_version,
        cuda_version=cuda_version,
        run_id=run_id,
        evidence_path=summary_json_path,
    )
    if summary_json_path is None:
        raise PermissionError("NCCL metrics require validated summary evidence")
    summary_metrics = _parse_nccl_health_evidence(
        summary_json_path,
        iterations=iterations,
        ibbw_log_path=ibbw_log_path,
        authorization=authorization,
        require_hca_samples=require_hca_samples,
    )
    expected_ports = {
        column: _optional_float(summary_metrics.port_max_gbps.get(column))
        for column in NCCL_IB_PORT_COLUMNS
    }
    supplied_ports = {
        column: _optional_float((port_max_gbps or {}).get(column))
        for column in NCCL_IB_PORT_COLUMNS
    }
    if (
        _optional_int(iterations) != summary_metrics.iterations
        or _optional_int(samples) != summary_metrics.samples
        or _optional_float(bus_bw) != summary_metrics.bus_bw
        or _optional_float(latency) != summary_metrics.latency
        or supplied_ports != expected_ports
    ):
        raise ValueError("NCCL metric arguments do not match validated summary evidence")
    parsed_timestamp = parse_timestamp(timestamp)
    ports = port_max_gbps or {}
    columns: tuple[str, ...] = (
        "Node",
        "timestamp",
        "la_timestamp",
        "iterations",
        "data_size_gb",
        "image_name",
        "cuda",
        "pytorch",
        "samples",
        "BUS_BW",
        "LATENCY",
        *NCCL_IB_PORT_COLUMNS,
    )
    values: tuple[object, ...] = (
        node,
        parsed_timestamp,
        timestamp_to_los_angeles(parsed_timestamp),
        _optional_int(iterations),
        summary_metrics.data_size_gb,
        image_name,
        cuda_version,
        pytorch_version,
        _optional_int(samples),
        _optional_float(bus_bw),
        _optional_float(latency),
        *(_optional_float(ports.get(column)) for column in NCCL_IB_PORT_COLUMNS),
    )
    if run_id:
        columns = (*columns, "run_id")
        values = (*values, run_id)
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(
        f"{column}=excluded.{column}" for column in columns if column not in {"Node", "timestamp"}
    )

    with closing(_connect_writable(db_path)) as connection:
        prepare_nccl_health_schema(connection, include_run_id=bool(run_id))
        if run_id or immutable:
            select_columns = (
                columns
                if run_id
                else tuple(column for column in columns if column != "run_id")
            )
            select_values = values
            where = (
                "run_id=? OR (Node=? AND timestamp=?)"
                if run_id
                else "Node=? AND timestamp=?"
            )
            params = (
                (run_id, node, parsed_timestamp)
                if run_id
                else (node, parsed_timestamp)
            )
            existing = connection.execute(
                f"SELECT {', '.join(select_columns)} FROM {NCCL_HEALTH_TABLE} "
                f"WHERE {where}",
                params,
            ).fetchall()
            if existing:
                if len(existing) == 1 and tuple(existing[0]) == tuple(select_values):
                    connection.commit()
                    return parsed_timestamp
                raise ValueError(
                    "NCCL metrics already exist with different run evidence"
                )
            connection.execute(
                f"INSERT INTO {NCCL_HEALTH_TABLE} ({', '.join(columns)}) "
                f"VALUES ({placeholders})",
                values,
            )
        else:
            connection.execute(
                f"INSERT INTO {NCCL_HEALTH_TABLE} ({', '.join(columns)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT(Node, timestamp) DO UPDATE SET {updates}",
                values,
            )
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
    run_id: str = "",
    immutable: bool = False,
    ibbw_log_path: str | Path | None = None,
    require_hca_samples: bool = False,
    db_path: str | Path = DEFAULT_NCCL_DB_PATH,
    _authorization: ResultWriteAuthorization | None = None,
) -> int:
    """Parse an NCCL summary JSON and upsert one consolidated health row."""

    authorization = validate_current_write(
        _authorization,
        operation="nccl",
        node=node,
        timestamp=timestamp,
        db_path=db_path,
        evidence_path=summary_json_path,
        image_name=image_name,
        pytorch_version=pytorch_version,
        cuda_version=cuda_version,
        run_id=run_id,
    )
    metrics = _parse_nccl_health_evidence(
        summary_json_path,
        iterations=iterations,
        ibbw_log_path=ibbw_log_path,
        authorization=authorization,
        require_hca_samples=require_hca_samples,
    )
    return add_nccl_health_result(
        node,
        timestamp,
        iterations=metrics.iterations,
        image_name=image_name,
        cuda_version=cuda_version,
        pytorch_version=pytorch_version,
        samples=metrics.samples,
        bus_bw=metrics.bus_bw,
        latency=metrics.latency,
        port_max_gbps=metrics.port_max_gbps,
        run_id=run_id,
        immutable=immutable,
        summary_json_path=summary_json_path,
        ibbw_log_path=ibbw_log_path,
        require_hca_samples=require_hca_samples,
        db_path=db_path,
        _authorization=authorization,
    )


def _parse_nccl_health_evidence(
    summary_json_path: str | Path,
    *,
    iterations: int | str | None,
    ibbw_log_path: str | Path | None,
    authorization: ResultWriteAuthorization,
    require_hca_samples: bool,
) -> NcclHealthMetrics:
    metrics = parse_nccl_health_summary(
        summary_json_path,
        iterations=iterations,
        require_hca_samples=require_hca_samples and ibbw_log_path is None,
    )
    if ibbw_log_path is None:
        return metrics
    if metrics.port_max_gbps:
        raise ValueError("NCCL IBBW recovery requires an empty summary HCA map")

    result = authorization.result
    expected_run_id = getattr(
        result, "run_id", f"{result.node}-{result.timestamp}"
    )
    evidence_root = (
        Path(authorization.config.runtime.validation_root)
        / "validation_tests/nccl/runs"
    )
    expected_log = (
        evidence_root
        / result.node
        / expected_run_id
        / "artifacts/ibbw.log"
    )
    safe_log = safe_existing_evidence_path(
        ibbw_log_path,
        expected_path=expected_log,
        allowed_root=evidence_root,
        expect_directory=False,
        description="current NCCL IBBW evidence",
    )
    from cval.validation.nccl_summary import summarize_ibbw_file

    ports = summarize_ibbw_file(safe_log)
    canonical_ports = {
        label: float(values["max_gbps"])
        for label, values in ports.items()
        if label in NCCL_IB_PORT_COLUMNS
    }
    sample_counts = [
        int(values["samples"])
        for label, values in ports.items()
        if label in NCCL_IB_PORT_COLUMNS
    ]
    if not canonical_ports:
        raise ValueError("NCCL IBBW log has no sampled mlx5_0..mlx5_13 HCA port")
    return NcclHealthMetrics(
        iterations=metrics.iterations,
        data_size_gb=metrics.data_size_gb,
        samples=max(sample_counts) if sample_counts else None,
        bus_bw=metrics.bus_bw,
        alg_bw=metrics.alg_bw,
        latency=metrics.latency,
        port_max_gbps=canonical_ports,
    )


def parse_nccl_health_summary(
    summary_json_path: str | Path,
    *,
    iterations: int | str | None = None,
    data_size_gb: int | None = None,
    require_hca_samples: bool = False,
) -> NcclHealthMetrics:
    """Validate one NCCL summary without opening or mutating a database."""

    summary_path = Path(summary_json_path).expanduser()
    if summary_path.is_symlink() or not summary_path.is_file():
        raise ValueError(f"NCCL summary must be a non-symlink regular file: {summary_path}")
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("NCCL summary must be a JSON object")
    numeric_metrics: dict[str, float] = {}
    for key in ("GCR_BUSBW", "GCR_ALGBW", "GCR_LATENCY"):
        raw_value = payload.get(key)
        value = _finite_non_negative_number(raw_value, f"NCCL summary {key}")
        if value <= 0.0:
            raise ValueError(f"NCCL summary {key} must be finite and positive")
        numeric_metrics[key] = value
    ports = payload.get("GCR_IB_PORT_BW_GBPS", {})
    if not isinstance(ports, dict):
        raise ValueError("NCCL summary GCR_IB_PORT_BW_GBPS must be an object")
    port_maxima: dict[str, object] = {}
    sample_counts: list[int] = []
    for port_label, entry in ports.items():
        if not isinstance(port_label, str) or not NCCL_PORT_LABEL_PATTERN.fullmatch(
            port_label
        ):
            raise ValueError(f"NCCL summary contains invalid HCA port label: {port_label!r}")
        if not isinstance(entry, dict):
            raise ValueError(f"NCCL summary port {port_label} must be an object")
        required_port_fields = {"avg_gbps", "max_gbps", "last_gbps", "samples"}
        if set(entry) != required_port_fields:
            raise ValueError(
                f"NCCL summary port {port_label} fields must be exactly "
                f"{sorted(required_port_fields)}"
            )
        for field in ("avg_gbps", "max_gbps", "last_gbps"):
            _finite_non_negative_number(
                entry[field],
                f"NCCL summary port {port_label}.{field}",
            )
        samples = _non_negative_integer(
            entry["samples"],
            f"NCCL summary port {port_label}.samples",
        )
        if samples <= 0:
            raise ValueError(f"NCCL summary port {port_label}.samples must be positive")
        if port_label in NCCL_IB_PORT_COLUMNS:
            port_maxima[port_label] = float(entry["max_gbps"])
            sample_counts.append(samples)
    if require_hca_samples and (
        not port_maxima or not sample_counts or max(sample_counts) <= 0
    ):
        raise ValueError(
            "NCCL summary requires at least one sampled mlx5_0..mlx5_13 HCA port"
        )

    summary_iterations_raw = payload.get("GCR_ITERATIONS")
    if (
        isinstance(summary_iterations_raw, bool)
        or not isinstance(summary_iterations_raw, int)
        or summary_iterations_raw <= 0
    ):
        raise ValueError("NCCL summary iterations must be positive")
    if iterations is None:
        parsed_iterations = summary_iterations_raw
    elif isinstance(iterations, bool) or not isinstance(iterations, int) or iterations <= 0:
        raise ValueError("Requested NCCL iterations must be a positive integer")
    else:
        parsed_iterations = iterations
    if summary_iterations_raw != parsed_iterations:
        raise ValueError("NCCL summary iterations do not match requested iterations")

    summary_data_size = payload.get("GCR_DATA_SIZE_GB")
    if summary_data_size is not None and (
        isinstance(summary_data_size, bool)
        or not isinstance(summary_data_size, int)
        or summary_data_size <= 0
    ):
        raise ValueError("NCCL summary data size must be a positive integer")
    if data_size_gb is not None and (
        isinstance(data_size_gb, bool)
        or not isinstance(data_size_gb, int)
        or data_size_gb <= 0
    ):
        raise ValueError("Requested NCCL data size must be a positive integer")
    if data_size_gb is not None and summary_data_size != data_size_gb:
        raise ValueError("NCCL summary data size does not match requested data size")

    return NcclHealthMetrics(
        iterations=parsed_iterations,
        data_size_gb=summary_data_size,
        samples=max(sample_counts) if sample_counts else None,
        bus_bw=numeric_metrics["GCR_BUSBW"],
        alg_bw=numeric_metrics["GCR_ALGBW"],
        latency=numeric_metrics["GCR_LATENCY"],
        port_max_gbps=port_maxima,
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
    _authorization: ResultWriteAuthorization | None = None,
) -> int:
    """Append one validation result row and return the parsed timestamp."""

    validate_current_write(
        _authorization,
        operation="validation-result",
        node=node,
        timestamp=timestamp,
        db_path=db_path,
        test=test,
        status=result,
        image_name=image_name,
        pytorch_version=pytorch_version,
        cuda_version=cuda_version,
    )
    parsed_timestamp = parse_timestamp(timestamp)
    if result not in VALID_STATUSES:
        raise ValueError(f"result must be one of {sorted(VALID_STATUSES)}")

    with closing(_connect_writable(db_path)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _prepare_validation_runs_schema(connection)
        expected = (
            node,
            test,
            parsed_timestamp,
            result,
            image_name,
            pytorch_version,
            cuda_version,
        )
        existing = connection.execute(
            "SELECT node, test, timestamp, result, image_name, pytorch_version, "
            "cuda_version FROM runs WHERE node=? AND test=? AND timestamp=?",
            (node, test, parsed_timestamp),
        ).fetchall()
        if existing:
            if len(existing) == 1 and tuple(existing[0]) == expected:
                connection.commit()
                return parsed_timestamp
            connection.rollback()
            raise ValueError(
                "validation result already exists with different or duplicate evidence"
            )
        connection.execute(
            "INSERT INTO runs(node, test, timestamp, result, image_name, pytorch_version, cuda_version) "
            "VALUES (?,?,?,?,?,?,?)",
            (node, test, parsed_timestamp, result, image_name, pytorch_version, cuda_version),
        )
        connection.commit()
    return parsed_timestamp


def add_validation_run_results(
    node: str,
    timestamp: object | None,
    results: dict[str, str],
    image_name: str = "",
    pytorch_version: str = "",
    cuda_version: str = "",
    db_path: str | Path = DEFAULT_VALIDATION_DB_PATH,
    _authorization: ResultWriteAuthorization | None = None,
) -> int:
    """Atomically append the current fixed current rows for one run."""

    from cval.validation.builtins import BUILTIN_STATUS_TEST_IDS

    required_tests = BUILTIN_STATUS_TEST_IDS
    if set(results) != set(required_tests):
        raise ValueError(
            "results must contain exactly storage, nccl, dltest, and all"
        )
    validate_current_write(
        _authorization,
        operation="validation-run",
        node=node,
        timestamp=timestamp,
        db_path=db_path,
        results=results,
        image_name=image_name,
        pytorch_version=pytorch_version,
        cuda_version=cuda_version,
    )
    for test_name, result in results.items():
        if result not in VALID_STATUSES:
            raise ValueError(
                f"result for {test_name!r} must be one of {sorted(VALID_STATUSES)}"
            )
    parsed_timestamp = parse_timestamp(timestamp)
    rows = [
        (
            node,
            test_name,
            parsed_timestamp,
            results[test_name],
            image_name,
            pytorch_version,
            cuda_version,
        )
        for test_name in required_tests
    ]
    with closing(_connect_writable(db_path)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _prepare_validation_runs_schema(connection)
        existing = connection.execute(
            "SELECT test, result, image_name, pytorch_version, cuda_version "
            "FROM runs WHERE node=? AND timestamp=?",
            (node, parsed_timestamp),
        ).fetchall()
        if existing:
            expected = {
                (
                    test_name,
                    results[test_name],
                    image_name,
                    pytorch_version,
                    cuda_version,
                )
                for test_name in required_tests
            }
            if Counter(existing) != Counter(expected):
                connection.rollback()
                raise ValueError(
                    "validation status rows already exist with different run evidence"
                )
            connection.commit()
            return parsed_timestamp
        connection.executemany(
            "INSERT INTO runs(node, test, timestamp, result, image_name, "
            "pytorch_version, cuda_version) VALUES (?,?,?,?,?,?,?)",
            rows,
        )
        connection.commit()
    return parsed_timestamp


def _prepare_validation_runs_schema(connection: sqlite3.Connection) -> None:
    """Create/additively update validation status tables and read-only view."""

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
        "CREATE INDEX IF NOT EXISTS idx_runs_node_test_ts "
        "ON runs(node, test, timestamp)"
    )
    connection.execute(
        """
        CREATE VIEW IF NOT EXISTS latest_status AS
        SELECT current.node, current.test,
               current.timestamp AS latest_timestamp, current.result
        FROM runs AS current
        INNER JOIN (
            SELECT node, test, MAX(timestamp) AS latest_timestamp
            FROM runs
            GROUP BY node, test
        ) AS latest
          ON latest.node = current.node
         AND latest.test = current.test
         AND latest.latest_timestamp = current.timestamp
        """
    )


def add_storage_result(
    node: str,
    timestamp: object,
    results_dir: str | Path,
    image_name: str = "",
    run_id: str = "",
    immutable: bool = False,
    db_path: str | Path = DEFAULT_STORAGE_DB_PATH,
    _authorization: ResultWriteAuthorization | None = None,
) -> int:
    """Parse fio JSON artifacts and upsert one row into the storage metrics DB."""

    validate_current_write(
        _authorization,
        operation="storage",
        node=node,
        timestamp=timestamp,
        db_path=db_path,
        evidence_path=results_dir,
        image_name=image_name,
        run_id=run_id,
    )
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
        base_values = (
            node,
            parsed_timestamp,
            image_name,
            *(metrics[column] for column in STORAGE_METRIC_COLUMNS),
        )
        if run_id:
            _ensure_column(connection, "storage_performance", "run_id", "TEXT")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_performance_run_id "
                "ON storage_performance(run_id) WHERE run_id IS NOT NULL"
            )
            values = (*base_values, run_id)
            existing = connection.execute(
                f"SELECT node, timestamp, image_name, {columns}, run_id "
                "FROM storage_performance WHERE run_id=? OR (node=? AND timestamp=?)",
                (run_id, node, parsed_timestamp),
            ).fetchall()
            if existing:
                if len(existing) == 1 and tuple(existing[0]) == values:
                    connection.commit()
                    return parsed_timestamp
                raise ValueError(
                    "Storage metrics already exist with different run evidence"
                )
            connection.execute(
                f"""
                INSERT INTO storage_performance (
                    node, timestamp, image_name, {columns}, run_id
                ) VALUES (?, ?, ?, {placeholders}, ?)
                """,
                values,
            )
        elif immutable:
            existing = connection.execute(
                f"SELECT node, timestamp, image_name, {columns} "
                "FROM storage_performance WHERE node=? AND timestamp=?",
                (node, parsed_timestamp),
            ).fetchall()
            if existing:
                if len(existing) == 1 and tuple(existing[0]) == base_values:
                    connection.commit()
                    return parsed_timestamp
                raise ValueError(
                    "Storage metrics already exist with different run evidence"
                )
            connection.execute(
                f"""
                INSERT INTO storage_performance (node, timestamp, image_name, {columns})
                VALUES (?, ?, ?, {placeholders})
                """,
                base_values,
            )
        else:
            connection.execute(
                f"""
                INSERT OR REPLACE INTO storage_performance (node, timestamp, image_name, {columns})
                VALUES (?, ?, ?, {placeholders})
                """,
                base_values,
            )
        connection.commit()
    return parsed_timestamp


def parse_storage_metrics(results_dir: str | Path) -> dict[str, float]:
    """Return fio IOPS and bandwidth metrics from a storage result directory."""

    directory = Path(results_dir)
    if not directory.is_dir() or directory.is_symlink():
        raise FileNotFoundError(f"Storage results directory not found: {directory}")
    resolved_directory = directory.resolve()
    actual_json_files = {
        child.name for child in directory.iterdir() if child.name.endswith(".json")
    }
    unexpected_files = sorted(actual_json_files - set(STORAGE_FILE_PREFIXES))
    if unexpected_files:
        raise ValueError(
            "Passing storage result contains unexpected JSON artifact(s): "
            f"{', '.join(unexpected_files)}"
        )

    metrics = {column: 0.0 for column in STORAGE_METRIC_COLUMNS}
    present_files: set[str] = set()
    for filename, prefix in STORAGE_FILE_PREFIXES.items():
        result_file = directory / filename
        if not result_file.exists():
            continue
        present_files.add(filename)
        if result_file.is_symlink() or not result_file.is_file():
            raise ValueError(
                f"Storage result must be a non-symlink regular file: {result_file}"
            )
        try:
            result_file.resolve().relative_to(resolved_directory)
        except ValueError as exc:
            raise ValueError(
                f"Storage result escapes its assigned directory: {result_file}"
            ) from exc
        data = json.loads(result_file.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"Storage result {result_file} must be an object")
        jobs = data.get("jobs")
        if not isinstance(jobs, list) or not jobs or not isinstance(jobs[0], dict):
            raise ValueError(f"Storage result {result_file} must contain jobs[0]")
        job = jobs[0]
        read = job.get("read")
        write = job.get("write")
        if not isinstance(read, dict) or not isinstance(write, dict):
            raise ValueError(f"Storage result {result_file} read/write must be objects")
        for direction, metrics_object in (("read", read), ("write", write)):
            missing_keys = sorted({"iops", "bw"} - set(metrics_object))
            if missing_keys:
                raise ValueError(
                    f"Storage result {result_file} {direction} is missing: "
                    f"{', '.join(missing_keys)}"
                )
        metrics[f"{prefix}_iops"] = _finite_non_negative_number(
            read["iops"], f"{result_file} read.iops"
        ) + _finite_non_negative_number(
            write["iops"], f"{result_file} write.iops"
        )
        metrics[f"{prefix}_bw"] = _finite_non_negative_number(
            read["bw"], f"{result_file} read.bw"
        ) + _finite_non_negative_number(
            write["bw"], f"{result_file} write.bw"
        )
    missing_files = sorted(set(STORAGE_FILE_PREFIXES) - present_files)
    if missing_files:
        raise ValueError(
            "Passing storage result is missing required FIO artifact(s): "
            f"{', '.join(missing_files)}"
        )
    return metrics


def _connect_writable(db_path: str | Path) -> sqlite3.Connection:
    """Open SQLite in create-if-needed mode and prepare parent directories."""

    from cval.storage.sqlite_uri import connect_sqlite_file

    path = safe_writable_file_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path = safe_writable_file_path(path)
    connection = connect_sqlite_file(path, mode="rwc", timeout=30)
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
