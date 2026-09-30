"""NCCL-owned summary validation and authorized raw SQLite writes."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sqlite3

from cval.storage.ingest import (
    _connect_writable,
    _ensure_column,
    _finite_non_negative_number,
    _non_negative_integer,
    _optional_float,
    _optional_int,
    parse_timestamp,
    timestamp_to_los_angeles,
)
from cval.storage.paths import safe_existing_evidence_path
from cval.storage.write_provenance import ResultWriteAuthorization, validate_current_write

def ingest(context) -> dict[str, dict]:
    from cval.storage.retry import retry_sqlite
    from cval.validation.results import ValidationResultV2

    authorization = context.authorization
    result, config = authorization.result, authorization.config
    run_id = getattr(result, "run_id", f"{result.node}-{result.timestamp}")
    run_dir = Path(config.runtime.validation_root) / "logs/validation_tests/nccl/runs" / result.node / run_id
    summary = (
        result.tests["nccl"].summary if isinstance(result, ValidationResultV2)
        else context.environment.get("CVAL_CANONICAL_NCCL_SUMMARY_FILE")
        or context.environment.get("NCCL_SUMMARY_FILE") or str(run_dir / "summary.json")
    )
    if not Path(summary).is_file():
        raise ValueError(f"Passing NCCL result is missing required summary: {summary}")
    settings = config.tests.registry.require("nccl").definition.settings
    artifacts = Path(result.tests["nccl"].artifacts) if isinstance(result, ValidationResultV2) else run_dir / "artifacts"
    ibbw = None
    if settings["ibbw_enabled"]:
        ibbw = (context.environment.get("CVAL_CANONICAL_NCCL_IBBW_LOG_FILE")
                or context.environment.get("NCCL_IBBW_LOG_FILE")
                or str(artifacts / f"ibbw-{result.node}-{result.timestamp}.log"))
    retry_sqlite(lambda: add_nccl_health_from_summary(
        result.node, result.timestamp, summary, iterations=settings["iterations"],
        image_name=result.image_name, cuda_version=result.cuda_version, pytorch_version=result.pytorch_version,
        immutable=True, ibbw_log_path=ibbw, require_hca_samples=bool(settings["ibbw_enabled"]),
        db_path=config.storage.nccl_db_path, _authorization=authorization,
    ), config.sqlite_retry)
    return {"nccl": {"rows": 1}}


NCCL_HEALTH_TABLE = "IB_HEALTH"

NCCL_IB_PORT_COLUMNS = tuple(f"mlx5_{index}" for index in range(14))

NCCL_LATEST_STATUS_VIEW = "LATEST_NODE_STATUS"

NCCL_PORT_LABEL_PATTERN = re.compile(
    r"^mlx5_(?:0|[1-9][0-9]*)(?:\.(?:[2-9]|[1-9][0-9]+))?$"
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
) -> None:
    """Create the raw latest-node view."""

    for sql in nccl_health_view_sql().values():
        connection.execute(sql)

def nccl_health_view_sql() -> dict[str, str]:
    """Return the stable raw latest-row SQL definition."""

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

    return {NCCL_LATEST_STATUS_VIEW: latest}

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
    recover_descriptor_ibbw_log: bool = False,
    require_hca_samples: bool = False,
    db_path: str | Path,
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
        recover_descriptor_ibbw_log=recover_descriptor_ibbw_log,
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
    recover_descriptor_ibbw_log: bool = False,
    require_hca_samples: bool = False,
    db_path: str | Path,
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
        recover_descriptor_ibbw_log=recover_descriptor_ibbw_log,
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
        recover_descriptor_ibbw_log=recover_descriptor_ibbw_log,
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
    recover_descriptor_ibbw_log: bool,
    require_hca_samples: bool,
) -> NcclHealthMetrics:
    metrics = parse_nccl_health_summary(
        summary_json_path,
        iterations=iterations,
        require_hca_samples=require_hca_samples and ibbw_log_path is None,
    )
    if ibbw_log_path is None:
        return metrics

    result = authorization.result
    expected_run_id = getattr(
        result, "run_id", f"{result.node}-{result.timestamp}"
    )
    evidence_root = (
        Path(authorization.config.runtime.validation_root)
        / "logs/validation_tests/nccl/runs"
    )
    legacy_root = Path(authorization.config.runtime.validation_root) / "validation_tests/nccl/runs"
    legacy_artifacts = legacy_root / result.node / expected_run_id / "artifacts"
    if Path(ibbw_log_path).parent == legacy_artifacts:
        evidence_root = legacy_root
    artifacts = evidence_root / result.node / expected_run_id / "artifacts"
    if result.schema_version == "cval.results.v3":
        evidence_root = Path(authorization.config.runtime.validation_root) / "logs/nccl"
        artifacts = evidence_root / result.node / expected_run_id
    expected_logs = {
        artifacts / "ibbw.log",
        artifacts / f"ibbw-{result.node}-{result.timestamp}.log",
    }
    requested_log = Path(ibbw_log_path)
    if requested_log not in expected_logs:
        raise ValueError(f"current NCCL IBBW evidence path is not canonical: {requested_log}")
    safe_log = safe_existing_evidence_path(
        ibbw_log_path,
        expected_path=requested_log,
        allowed_root=evidence_root,
        expect_directory=False,
        description="current NCCL IBBW evidence",
    )
    summary_payload = json.loads(Path(summary_json_path).read_text(encoding="utf-8"))
    summary_reference = summary_payload.get("GCR_IBBW_LOG_FILE")
    historical_descriptor_reference = (
        recover_descriptor_ibbw_log
        and isinstance(summary_reference, str)
        and re.fullmatch(r"/proc/self/fd/[0-9]+/ibbw\.log", summary_reference)
        and requested_log == artifacts / "ibbw.log"
    )
    if summary_reference != str(requested_log) and not historical_descriptor_reference:
        raise ValueError("NCCL IBBW evidence does not match the summary reference")
    if metrics.port_max_gbps:
        return metrics
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
