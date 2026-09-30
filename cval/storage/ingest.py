"""SQLite ingestion helpers used inside validation pods.

The read side of c-val opens SQLite in `mode=ro`; this module is the explicit
write side used after a validation pod has finished running deterministic tests.
It replaces the legacy `utils/functions.py` DB commands with package-native,
testable functions.
"""

from __future__ import annotations

import datetime as dt
import math
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path
from zoneinfo import ZoneInfo

from cval.storage.paths import safe_writable_file_path
from cval.storage.write_provenance import (
    ResultWriteAuthorization,
    validate_current_write,
)

VALID_STATUSES = {"pass", "fail", "incomplete"}

_NCCL_COMPATIBILITY_EXPORTS = frozenset({
    "NCCL_HEALTH_TABLE", "NCCL_IB_PORT_COLUMNS", "NCCL_LATEST_STATUS_VIEW",
    "NCCL_PORT_LABEL_PATTERN", "NcclHealthMetrics", "_create_nccl_health_table",
    "_create_nccl_health_views", "nccl_health_view_sql", "prepare_nccl_health_schema",
    "add_nccl_health_result", "add_nccl_health_from_summary",
    "_parse_nccl_health_evidence", "parse_nccl_health_summary",
})


def __getattr__(name: str):
    if name in _NCCL_COMPATIBILITY_EXPORTS:
        owner = "nccl"
    elif name in {"STORAGE_FILE_PREFIXES", "STORAGE_METRIC_COLUMNS"}:
        owner = "storage"
    else:
        raise AttributeError(name)
    from cval.validation.plugins import load_test_support

    module = load_test_support(Path(__file__).resolve().parents[2] / f"validation-tests/{owner}/ingest.py")
    return getattr(module, name)


LOS_ANGELES = ZoneInfo("America/Los_Angeles")

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










def add_validation_result(
    node: str,
    test: str,
    result: str,
    timestamp: object | None,
    image_name: str = "",
    pytorch_version: str = "",
    cuda_version: str = "",
    *,
    db_path: str | Path,
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
    *,
    db_path: str | Path,
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
    *,
    db_path: str | Path,
    _authorization: ResultWriteAuthorization | None = None,
) -> int:
    """Compatibility entrypoint for the test-owned raw writer."""

    from cval.validation.plugins import load_test_support

    writer = load_test_support(Path(__file__).resolve().parents[2] / "validation-tests/storage/ingest.py").add_storage_result
    return writer(node, timestamp, results_dir, image_name, run_id, immutable,
                  db_path=db_path, _authorization=_authorization)


def parse_storage_metrics(results_dir: str | Path, *, summary_filename: str | None = None) -> dict[str, float]:
    """Compatibility entrypoint for the test-owned FIO parser."""

    from cval.validation.plugins import load_test_support

    parser = load_test_support(Path(__file__).resolve().parents[2] / "validation-tests/storage/ingest.py").parse_storage_metrics
    return parser(results_dir, summary_filename=summary_filename)


def _connect_writable(db_path: str | Path) -> sqlite3.Connection:
    """Open SQLite in create-if-needed mode and prepare parent directories."""

    from cval.storage.sqlite_uri import connect_sqlite_file

    path = safe_writable_file_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path = safe_writable_file_path(path)
    connection = connect_sqlite_file(path, mode="rwc")
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
