"""Storage-owned FIO evidence parsing and authorized raw SQLite writes."""

from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path

from cval.storage.ingest import (
    _connect_writable,
    _ensure_column,
    _finite_non_negative_number,
    parse_timestamp,
)
from cval.storage.write_provenance import ResultWriteAuthorization, validate_current_write

STORAGE_FILE_PREFIXES = {
    "iodepth_read_1file.json": "iodepth_read_1file",
    "iodepth_write_1file.json": "iodepth_write_1file",
    "numjobs_read_nfiles.json": "numjobs_read_nfiles",
    "numjobs_write_nfiles.json": "numjobs_write_nfiles",
    "randread.json": "randread",
    "randwrite.json": "randwrite",
}
STORAGE_METRIC_COLUMNS = tuple(
    f"{prefix}_{metric}" for prefix in STORAGE_FILE_PREFIXES.values() for metric in ("iops", "bw")
)


def ingest(context) -> dict[str, dict]:
    from cval.storage.retry import retry_sqlite
    from cval.validation.results import ValidationResultV2

    authorization = context.authorization
    result, config = authorization.result, authorization.config
    run_id = getattr(result, "run_id", f"{result.node}-{result.timestamp}")
    artifacts = (
        result.tests["storage"].artifacts if isinstance(result, ValidationResultV2)
        else context.environment.get("CVAL_CANONICAL_STORAGE_OUTPUT_DIR")
        or context.environment.get("STORAGE_OUTPUT_DIR")
        or str(Path(config.runtime.validation_root) / "logs/validation_tests/storage/runs" / result.node / run_id / "artifacts")
    )
    retry_sqlite(lambda: add_storage_result(
        result.node, result.timestamp, artifacts, image_name=result.image_name,
        immutable=True, db_path=config.storage.storage_db_path, _authorization=authorization,
    ), config.sqlite_retry)
    return {"storage": {"rows": 1}}


def add_storage_result(
    node: str,
    timestamp: object,
    results_dir: str | Path,
    image_name: str = "",
    run_id: str = "",
    immutable: bool = False,
    *,
    db_path: str | Path,
    _authorization: ResultWriteAuthorization | None = None,
) -> int:
    """Parse FIO JSON artifacts and write one authorized storage metrics row."""

    authorization = validate_current_write(
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
    summary_name = authorization.config.tests.registry.require("storage").definition.artifacts.summary_filename if authorization.result.schema_version == "cval.results.v3" else None
    metrics = parse_storage_metrics(results_dir, summary_filename=summary_name)
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
        _ensure_column(connection, "storage_performance", "image_name", "TEXT NOT NULL DEFAULT ''")
        base_values = (node, parsed_timestamp, image_name, *(metrics[column] for column in STORAGE_METRIC_COLUMNS))
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
                raise ValueError("Storage metrics already exist with different run evidence")
            connection.execute(
                f"INSERT INTO storage_performance (node, timestamp, image_name, {columns}, run_id) "
                f"VALUES (?, ?, ?, {placeholders}, ?)",
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
                raise ValueError("Storage metrics already exist with different run evidence")
            connection.execute(
                f"INSERT INTO storage_performance (node, timestamp, image_name, {columns}) "
                f"VALUES (?, ?, ?, {placeholders})",
                base_values,
            )
        else:
            connection.execute(
                f"INSERT OR REPLACE INTO storage_performance (node, timestamp, image_name, {columns}) "
                f"VALUES (?, ?, ?, {placeholders})",
                base_values,
            )
        connection.commit()
    return parsed_timestamp


def parse_storage_metrics(results_dir: str | Path, *, summary_filename: str | None = None) -> dict[str, float]:
    """Return the twelve raw FIO values, rejecting incomplete or altered evidence."""

    directory = Path(results_dir)
    if not directory.is_dir() or directory.is_symlink():
        raise FileNotFoundError(f"Storage results directory not found: {directory}")
    resolved_directory = directory.resolve()
    actual_json_files = {child.name for child in directory.iterdir() if child.name.endswith(".json")}
    if summary_filename is not None and (Path(summary_filename).name != summary_filename or summary_filename in STORAGE_FILE_PREFIXES):
        raise ValueError("Storage summary must be a distinct basename")
    unexpected_files = sorted(actual_json_files - set(STORAGE_FILE_PREFIXES) - {summary_filename})
    if unexpected_files:
        raise ValueError("Passing storage result contains unexpected JSON artifact(s): " + ", ".join(unexpected_files))

    metrics = {column: 0.0 for column in STORAGE_METRIC_COLUMNS}
    present_files: set[str] = set()
    for filename, prefix in STORAGE_FILE_PREFIXES.items():
        result_file = directory / filename
        if not result_file.exists():
            continue
        present_files.add(filename)
        if result_file.is_symlink() or not result_file.is_file():
            raise ValueError(f"Storage result must be a non-symlink regular file: {result_file}")
        try:
            result_file.resolve().relative_to(resolved_directory)
        except ValueError as error:
            raise ValueError(f"Storage result escapes its assigned directory: {result_file}") from error
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
                raise ValueError(f"Storage result {result_file} {direction} is missing: " + ", ".join(missing_keys))
        metrics[f"{prefix}_iops"] = _finite_non_negative_number(
            read["iops"], f"{result_file} read.iops"
        ) + _finite_non_negative_number(write["iops"], f"{result_file} write.iops")
        metrics[f"{prefix}_bw"] = _finite_non_negative_number(
            read["bw"], f"{result_file} read.bw"
        ) + _finite_non_negative_number(write["bw"], f"{result_file} write.bw")
    missing_files = sorted(set(STORAGE_FILE_PREFIXES) - present_files)
    if missing_files:
        raise ValueError("Passing storage result is missing required FIO artifact(s): " + ", ".join(missing_files))
    if summary_filename is None:
        return metrics
    summary_path = directory / summary_filename
    if summary_path.exists() or summary_path.is_symlink():
        if summary_path.is_symlink() or not summary_path.is_file():
            raise ValueError("Storage summary must be a non-symlink regular file")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if not isinstance(summary, dict) or summary.get("schema_version") != "cval.storage.summary.v1" or summary.get("metrics") != metrics:
            raise ValueError("Storage summary does not match retained FIO metrics")
    return metrics