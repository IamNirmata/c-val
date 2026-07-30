"""Additive node run-history storage and read-only reporting.

The run-history database records one row per ``cval.results.v2`` run and one
normalized row per registered test. Raw execution state is copied from the
validated result envelope; derived health classes never belong here.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from cval.config import CvalConfig, encode_config_snapshot, load_config
from cval.k8s.client import KubectlClient
from cval.storage.paths import safe_writable_file_path
from cval.storage.per_test_results import (
    require_database_tables,
    require_database_views,
    require_schema_objects,
    require_exact_table_sql,
    validate_table_manifest,
)
from cval.storage.status import resolve_status_pod
from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    validation_result_v2_digest,
    validation_timestamp_to_epoch,
)
from cval.validation.registry import validation_test_config_digest
from cval.validation.runtime import effective_config_digest


SCHEMA_VERSION = 1
VALID_STATUSES = {"pass", "fail", "incomplete"}
_RUN_HISTORY_WRITE_AUTHORIZATION = object()
RUN_HISTORY_VIEW_SQL = """
CREATE VIEW IF NOT EXISTS node_run_history AS
SELECT
    r.*,
    COALESCE((
        SELECT GROUP_CONCAT(test_id, ',')
        FROM (
            SELECT test_id
            FROM run_tests rt
            WHERE rt.run_id = r.run_id AND rt.selected = 1
            ORDER BY execution_order, test_id
        )
    ), '') AS tests_ran
FROM runs r
"""


@dataclass(frozen=True)
class RunHistoryRow:
    """One operator-facing node run-history row."""

    run_id: str
    node: str
    started_timestamp: int
    started_timestamp_la: str
    completed_timestamp: int | None
    overall_status: str
    tests_ran: str
    image_name: str
    pytorch_version: str
    cuda_version: str
    git_ref: str
    global_config_digest: str
    result_path: str
    created_at: int
    updated_at: int
    result_digest: str = ""


def ingest_run_history_file(
    result_path: str | Path,
    *,
    db_path: str | Path | None = None,
    config: CvalConfig | None = None,
    result_digest: str,
    config_snapshot_b64: str,
) -> str:
    """Validate a v2 result file, persist it idempotently, and return run ID."""

    path = Path(result_path)
    result = load_validation_result(path)
    if not isinstance(result, ValidationResultV2):
        raise ValueError("Node run history accepts only cval.results.v2 artifacts")
    active_config = config or load_config()
    if not active_config.storage.run_history_enabled:
        raise ValueError("Node run-history writes are disabled by storage.run_history_enabled")
    if not config_snapshot_b64 or config_snapshot_b64 != encode_config_snapshot(active_config):
        raise ValueError("Run-history configuration does not match its immutable snapshot")
    if result.global_config_digest != effective_config_digest(active_config):
        raise ValueError("Run-history result global config digest does not match snapshot")
    registry = active_config.tests.registry
    if set(result.tests) != {test.id for test in registry.tests}:
        raise ValueError("Run-history result test set does not match snapshot registry")
    for registered_test in registry.tests:
        test = result.tests[registered_test.id]
        expected = (
            registered_test.definition.metadata.display_name,
            registered_test.enabled,
            registered_test.enabled,
            registered_test.definition.metadata.order,
            registered_test.config_path,
            validation_test_config_digest(registered_test),
        )
        actual = (
            test.display_name,
            test.enabled,
            test.selected,
            test.order,
            test.config_path,
            test.config_digest,
        )
        if actual != expected:
            raise ValueError(
                f"Run-history result metadata for {registered_test.id!r} "
                "does not match snapshot registry"
            )
    actual_digest = validation_result_v2_digest(result)
    if result_digest != actual_digest:
        raise ValueError("Run-history result does not match its immutable digest")
    expected_path = (
        Path(active_config.runtime.validation_root)
        / "logs/job_logs"
        / result.node
        / result.run_id
        / "result.json"
    )
    if path != expected_path or path.is_symlink() or path.resolve() != expected_path.resolve():
        raise ValueError("Run-history result path is not the canonical run result path")
    from cval.validation.ingestion import preflight_test_results_file

    preflight_test_results_file(
        path,
        config=active_config,
        result_digest=result_digest,
        config_snapshot_b64=config_snapshot_b64,
    )
    target = db_path or active_config.storage.run_history_db_path
    ingest_run_history_result(
        result,
        db_path=target,
        result_path=str(path),
        result_digest=result_digest,
        _authorization=_RUN_HISTORY_WRITE_AUTHORIZATION,
    )
    return result.run_id


def ingest_run_history_result(
    result: ValidationResultV2,
    *,
    db_path: str | Path,
    result_path: str = "",
    result_digest: str,
    _authorization: object | None = None,
    now: int | None = None,
) -> None:
    """Upsert one validated run and its tests in one SQLite transaction."""

    if _authorization is not _RUN_HISTORY_WRITE_AUTHORIZATION:
        raise ValueError("Node run-history writes require an authorized file boundary")
    expected_digest = validation_result_v2_digest(result)
    if result_digest != expected_digest:
        raise ValueError("Run-history result digest does not match validated evidence")
    now = int(time.time()) if now is None else int(now)
    completed_timestamp = validation_timestamp_to_epoch(result.completed_at)
    tests_in_order = sorted(result.tests.items(), key=lambda item: (item[1].order, item[0]))
    tests_requested = [test_id for test_id, test in tests_in_order if test.selected]
    path = safe_writable_file_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path = safe_writable_file_path(path)

    with closing(sqlite3.connect(f"file:{path}?mode=rwc", uri=True, timeout=30)) as connection:
        connection.execute("PRAGMA busy_timeout=30000")
        if not _database_is_empty(connection):
            _assert_supported_schema(connection, allow_empty=False)
            _validate_exact_schema_manifest(connection)
            _validate_schema_shape(connection)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        _prepare_schema(connection)
        _validate_exact_schema_manifest(connection)
        if _run_exists(connection, result.run_id):
            _validate_exact_existing_run(
                connection,
                result,
                tests_requested_json=json.dumps(tests_requested, separators=(",", ":")),
                result_path=result_path,
                result_digest=result_digest,
            )
            connection.commit()
            return

        connection.execute(
            """
            INSERT INTO runs (
                run_id, node, started_timestamp, started_timestamp_la,
                completed_timestamp, overall_status, tests_requested_json,
                image_name, pytorch_version, cuda_version, git_ref,
                global_config_digest, result_path, result_digest,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.run_id,
                result.node,
                result.timestamp,
                result.timestamp_la,
                completed_timestamp,
                result.overall,
                json.dumps(tests_requested, separators=(",", ":")),
                result.image_name,
                result.pytorch_version,
                result.cuda_version,
                result.git_ref,
                result.global_config_digest,
                result_path,
                result_digest,
                now,
                now,
            ),
        )

        for test_id, test in tests_in_order:
            connection.execute(
                """
                INSERT INTO run_tests (
                    run_id, test_id, enabled, selected, execution_order, phase,
                    status, started_timestamp, completed_timestamp, duration_ms,
                    exit_code, result_path, stdout_path, stderr_path, log_path,
                    summary_path, test_config_digest, error_message,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.run_id,
                    test_id,
                    int(test.enabled),
                    int(test.selected),
                    test.order,
                    test.phase,
                    test.status,
                    validation_timestamp_to_epoch(test.started_at),
                    validation_timestamp_to_epoch(test.completed_at),
                    test.duration_ms,
                    test.exit_code,
                    test.result,
                    test.stdout,
                    test.stderr,
                    test.log,
                    test.summary,
                    test.config_digest,
                    test.message,
                    now,
                    now,
                ),
            )
        connection.commit()


def run_history_rows_from_db(
    db_path: str | Path,
    *,
    run_id: str | None = None,
    node: str | None = None,
    test_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[RunHistoryRow]:
    """Read filtered history from a local DB in SQLite read-only mode."""

    query, params = _history_query(
        run_id=run_id,
        node=node,
        test_id=test_id,
        status=status,
        limit=limit,
    )
    path = Path(db_path)
    if not path.is_file():
        return []
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)) as connection:
        connection.row_factory = sqlite3.Row
        _assert_supported_schema(connection, allow_empty=False)
        _validate_exact_schema_manifest(connection)
        _validate_schema_shape(connection)
        rows = connection.execute(query, params).fetchall()
    return [_row_from_mapping(dict(row)) for row in rows]


def get_run_history_rows(
    *,
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
    run_id: str | None = None,
    node: str | None = None,
    test_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    config: CvalConfig | None = None,
) -> list[RunHistoryRow]:
    """Read filtered history through the PVC access pod without DB mutation."""

    active_config = config or load_config()
    pod = pod or active_config.cluster.pvc_access_pod
    namespace = namespace or active_config.cluster.namespace
    db_path = db_path or active_config.storage.run_history_db_path
    kubectl = client or KubectlClient()
    status_pod = resolve_status_pod(kubectl, namespace, pod)
    query, params = _history_query(
        run_id=run_id,
        node=node,
        test_id=test_id,
        status=status,
        limit=limit,
    )
    expected_manifest = _run_history_sql_manifest()
    script = r'''
import json
import sqlite3
import sys
from pathlib import Path

db_path = Path(sys.argv[1])
query = sys.argv[2]
params = json.loads(sys.argv[3])
expected_manifest = json.loads(sys.argv[4])
if not db_path.is_file():
    print("[]")
    raise SystemExit(0)
try:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
        )
    }
    if "schema_migrations" not in tables:
        raise RuntimeError("run-history database lacks schema_migrations")
    versions = [
        list(row)
        for row in connection.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        )
    ]
    if versions != [[1, "initial-run-history"]]:
        raise RuntimeError(f"unsupported run-history migration manifest: {versions}")
    actual_manifest = {
        f"{row[0]}:{row[1]}": str(row[2] or "")
        for row in connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE type IN ('table','index','view','trigger')"
        )
    }
    if actual_manifest != expected_manifest:
        raise RuntimeError("run-history schema manifest does not match this reader")
    rows = [dict(row) for row in connection.execute(query, params).fetchall()]
finally:
    try:
        connection.close()
    except NameError:
        pass
print(json.dumps(rows))
'''
    result = kubectl.run(
        [
            "exec",
            "-i",
            "-n",
            namespace,
            status_pod,
            "--",
            "python3",
            "-",
            db_path,
            query,
            json.dumps(params),
            json.dumps(expected_manifest, sort_keys=True),
        ],
        input_text=script,
    )
    payload = json.loads(result.stdout or "[]")
    if not isinstance(payload, list):
        raise ValueError("run history output must be a JSON array")
    return [_row_from_mapping(item) for item in payload if isinstance(item, dict)]


@lru_cache(maxsize=1)
def _run_history_sql_manifest() -> dict[str, str]:
    """Build the exact SQL manifest understood by local/remote readers."""

    with closing(sqlite3.connect(":memory:")) as connection:
        _prepare_schema(connection, validate=False)
        return {
            f"{row[0]}:{row[1]}": str(row[2] or "")
            for row in connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE type IN ('table','index','view','trigger')"
            )
        }


def _validate_exact_schema_manifest(connection: sqlite3.Connection) -> None:
    """Require every SQLite schema object and exact DDL understood by this version."""

    actual = {
        f"{row[0]}:{row[1]}": str(row[2] or "")
        for row in connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE type IN ('table','index','view','trigger')"
        )
    }
    if actual != _run_history_sql_manifest():
        raise RuntimeError("run-history schema manifest does not match this reader")


def run_history_rows_to_dicts(rows: list[RunHistoryRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]


def _prepare_schema(
    connection: sqlite3.Connection,
    *,
    validate: bool = True,
) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            node TEXT NOT NULL,
            started_timestamp INTEGER NOT NULL,
            started_timestamp_la TEXT NOT NULL,
            completed_timestamp INTEGER,
            overall_status TEXT NOT NULL
                CHECK (overall_status IN ('pass','fail','incomplete')),
            tests_requested_json TEXT NOT NULL DEFAULT '[]',
            image_name TEXT NOT NULL DEFAULT '',
            pytorch_version TEXT NOT NULL DEFAULT '',
            cuda_version TEXT NOT NULL DEFAULT '',
            git_ref TEXT NOT NULL DEFAULT '',
            global_config_digest TEXT NOT NULL DEFAULT '',
            result_path TEXT NOT NULL DEFAULT '',
            result_digest TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            CHECK (completed_timestamp IS NULL
                   OR completed_timestamp >= started_timestamp)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS run_tests (
            run_id TEXT NOT NULL,
            test_id TEXT NOT NULL,
            enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),
            selected INTEGER NOT NULL CHECK (selected IN (0,1)),
            execution_order INTEGER NOT NULL,
            phase TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pass','fail','incomplete')),
            started_timestamp INTEGER,
            completed_timestamp INTEGER,
            duration_ms INTEGER,
            exit_code INTEGER,
            result_path TEXT NOT NULL DEFAULT '',
            stdout_path TEXT NOT NULL DEFAULT '',
            stderr_path TEXT NOT NULL DEFAULT '',
            log_path TEXT NOT NULL DEFAULT '',
            summary_path TEXT NOT NULL DEFAULT '',
            test_config_digest TEXT NOT NULL DEFAULT '',
            error_message TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (run_id, test_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE RESTRICT,
            CHECK (selected = 0 OR enabled = 1),
            CHECK (duration_ms IS NULL OR duration_ms >= 0),
            CHECK (completed_timestamp IS NULL OR started_timestamp IS NULL
                   OR completed_timestamp >= started_timestamp)
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_runs_node_started "
        "ON runs(node, started_timestamp DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_runs_status_started "
        "ON runs(overall_status, started_timestamp DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_run_tests_test_status "
        "ON run_tests(test_id, status, completed_timestamp DESC)"
    )
    connection.execute(RUN_HISTORY_VIEW_SQL)
    connection.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, name, applied_at) "
        "VALUES (?, ?, ?)",
        (SCHEMA_VERSION, "initial-run-history", int(time.time())),
    )
    if validate:
        _validate_schema_shape(connection)


def _assert_supported_schema(
    connection: sqlite3.Connection,
    *,
    allow_empty: bool,
) -> None:
    """Reject missing/unknown/newer run-history schemas without mutation."""

    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    if "schema_migrations" not in tables:
        if allow_empty and not tables:
            return
        raise RuntimeError("run-history database lacks schema_migrations")
    versions = [
        tuple(row)
        for row in connection.execute(
        "SELECT version, name FROM schema_migrations ORDER BY version"
        )
    ]
    expected = [(SCHEMA_VERSION, "initial-run-history")]
    if versions != expected:
        raise RuntimeError(
            f"Unsupported run-history schema migration manifest: {versions}"
        )


def _database_is_empty(connection: sqlite3.Connection) -> bool:
    return not any(
        True
        for _row in connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type IN ('table','index','view','trigger') LIMIT 1"
        )
    )


def _validate_schema_shape(connection: sqlite3.Connection) -> None:
    require_database_tables(
        connection,
        {"schema_migrations", "runs", "run_tests"},
    )
    require_database_views(connection, {"node_run_history"})
    validate_table_manifest(
        connection,
        "schema_migrations",
        required_columns={"version", "name", "applied_at"},
        column_specs={
            "version": ("INTEGER", False, None, 1),
            "name": ("TEXT", True, None, 0),
            "applied_at": ("INTEGER", True, None, 0),
        },
        primary_key=("version",),
        allowed_indexes=set(),
        implicit_indexes=set(),
        constraint_counts=(0, 0),
    )
    require_exact_table_sql(
        connection,
        "schema_migrations",
        _run_history_sql_manifest()["table:schema_migrations"],
    )
    validate_table_manifest(
        connection,
        "runs",
        required_columns={
            "run_id",
            "node",
            "started_timestamp",
            "started_timestamp_la",
            "completed_timestamp",
            "overall_status",
            "tests_requested_json",
            "image_name",
            "pytorch_version",
            "cuda_version",
            "git_ref",
            "global_config_digest",
            "result_path",
            "result_digest",
            "created_at",
            "updated_at",
        },
        column_specs={
            "run_id": ("TEXT", False, None, 1),
            "node": ("TEXT", True, None, 0),
            "started_timestamp": ("INTEGER", True, None, 0),
            "started_timestamp_la": ("TEXT", True, None, 0),
            "completed_timestamp": ("INTEGER", False, None, 0),
            "overall_status": ("TEXT", True, None, 0),
            "tests_requested_json": ("TEXT", True, "'[]'", 0),
            "image_name": ("TEXT", True, "''", 0),
            "pytorch_version": ("TEXT", True, "''", 0),
            "cuda_version": ("TEXT", True, "''", 0),
            "git_ref": ("TEXT", True, "''", 0),
            "global_config_digest": ("TEXT", True, "''", 0),
            "result_path": ("TEXT", True, "''", 0),
            "result_digest": ("TEXT", True, "''", 0),
            "created_at": ("INTEGER", True, None, 0),
            "updated_at": ("INTEGER", True, None, 0),
        },
        primary_key=("run_id",),
        required_sql_fragments=(
            "CHECK (OVERALL_STATUS IN ('pass','fail','incomplete'))",
            "CHECK (COMPLETED_TIMESTAMP IS NULL OR COMPLETED_TIMESTAMP >= STARTED_TIMESTAMP)",
        ),
        allowed_indexes={"idx_runs_node_started", "idx_runs_status_started"},
        implicit_indexes={("pk", ("run_id",), ("BINARY",))},
        constraint_counts=(2, 0),
    )
    require_exact_table_sql(
        connection,
        "runs",
        _run_history_sql_manifest()["table:runs"],
    )
    validate_table_manifest(
        connection,
        "run_tests",
        required_columns={
            "run_id",
            "test_id",
            "enabled",
            "selected",
            "execution_order",
            "phase",
            "status",
            "started_timestamp",
            "completed_timestamp",
            "duration_ms",
            "exit_code",
            "result_path",
            "stdout_path",
            "stderr_path",
            "log_path",
            "summary_path",
            "test_config_digest",
            "error_message",
            "created_at",
            "updated_at",
        },
        column_specs={
            "run_id": ("TEXT", True, None, 1),
            "test_id": ("TEXT", True, None, 2),
            "enabled": ("INTEGER", True, None, 0),
            "selected": ("INTEGER", True, None, 0),
            "execution_order": ("INTEGER", True, None, 0),
            "phase": ("TEXT", True, None, 0),
            "status": ("TEXT", True, None, 0),
            "started_timestamp": ("INTEGER", False, None, 0),
            "completed_timestamp": ("INTEGER", False, None, 0),
            "duration_ms": ("INTEGER", False, None, 0),
            "exit_code": ("INTEGER", False, None, 0),
            "result_path": ("TEXT", True, "''", 0),
            "stdout_path": ("TEXT", True, "''", 0),
            "stderr_path": ("TEXT", True, "''", 0),
            "log_path": ("TEXT", True, "''", 0),
            "summary_path": ("TEXT", True, "''", 0),
            "test_config_digest": ("TEXT", True, "''", 0),
            "error_message": ("TEXT", True, "''", 0),
            "created_at": ("INTEGER", True, None, 0),
            "updated_at": ("INTEGER", True, None, 0),
        },
        primary_key=("run_id", "test_id"),
        required_sql_fragments=(
            "CHECK (ENABLED IN (0,1))",
            "CHECK (SELECTED IN (0,1))",
            "CHECK (STATUS IN ('pass','fail','incomplete'))",
            "CHECK (SELECTED = 0 OR ENABLED = 1)",
            "CHECK (DURATION_MS IS NULL OR DURATION_MS >= 0)",
            "CHECK (COMPLETED_TIMESTAMP IS NULL OR STARTED_TIMESTAMP IS NULL OR COMPLETED_TIMESTAMP >= STARTED_TIMESTAMP)",
            "FOREIGN KEY (RUN_ID) REFERENCES RUNS(RUN_ID) ON DELETE RESTRICT",
        ),
        allowed_indexes={"idx_run_tests_test_status"},
        implicit_indexes={
            ("pk", ("run_id", "test_id"), ("BINARY", "BINARY"))
        },
        constraint_counts=(6, 1),
    )
    require_exact_table_sql(
        connection,
        "run_tests",
        _run_history_sql_manifest()["table:run_tests"],
    )
    require_schema_objects(
        connection,
        indexes={
            "idx_runs_node_started": (
                "runs",
                ("node", "started_timestamp"),
                (False, True),
                ("BINARY", "BINARY"),
                False,
                "",
            ),
            "idx_runs_status_started": (
                "runs",
                ("overall_status", "started_timestamp"),
                (False, True),
                ("BINARY", "BINARY"),
                False,
                "",
            ),
            "idx_run_tests_test_status": (
                "run_tests",
                ("test_id", "status", "completed_timestamp"),
                (False, False, True),
                ("BINARY", "BINARY", "BINARY"),
                False,
                "",
            ),
        },
        view_sql={"node_run_history": RUN_HISTORY_VIEW_SQL},
    )
    foreign_keys = {
        (
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]).upper(),
            str(row[6]).upper(),
            str(row[7]).upper(),
        )
        for row in connection.execute("PRAGMA foreign_key_list(run_tests)")
    }
    if foreign_keys != {
        ("runs", "run_id", "run_id", "NO ACTION", "RESTRICT", "NONE")
    }:
        raise RuntimeError("run-history foreign key manifest is invalid")


def _run_exists(connection: sqlite3.Connection, run_id: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM runs WHERE run_id=?",
        (run_id,),
    ).fetchone() is not None


def _validate_exact_existing_run(
    connection: sqlite3.Connection,
    result: ValidationResultV2,
    *,
    tests_requested_json: str,
    result_path: str,
    result_digest: str,
) -> None:
    row = connection.execute(
        "SELECT node, started_timestamp, started_timestamp_la, completed_timestamp, "
        "overall_status, tests_requested_json, image_name, pytorch_version, "
        "cuda_version, git_ref, global_config_digest, result_path, result_digest "
        "FROM runs WHERE run_id=?",
        (result.run_id,),
    ).fetchone()
    expected_run = (
        result.node,
        result.timestamp,
        result.timestamp_la,
        validation_timestamp_to_epoch(result.completed_at),
        result.overall,
        tests_requested_json,
        result.image_name,
        result.pytorch_version,
        result.cuda_version,
        result.git_ref,
        result.global_config_digest,
        result_path,
        result_digest,
    )
    if row is None or tuple(row) != expected_run:
        raise ValueError("run_id already exists with different immutable run evidence")

    actual_tests = connection.execute(
        "SELECT test_id, enabled, selected, execution_order, phase, status, "
        "started_timestamp, completed_timestamp, duration_ms, exit_code, "
        "result_path, stdout_path, stderr_path, log_path, summary_path, "
        "test_config_digest, error_message FROM run_tests WHERE run_id=? "
        "ORDER BY execution_order, test_id",
        (result.run_id,),
    ).fetchall()
    expected_tests = []
    for test_id, test in sorted(
        result.tests.items(),
        key=lambda item: (item[1].order, item[0]),
    ):
        expected_tests.append(
            (
                test_id,
                int(test.enabled),
                int(test.selected),
                test.order,
                test.phase,
                test.status,
                validation_timestamp_to_epoch(test.started_at),
                validation_timestamp_to_epoch(test.completed_at),
                test.duration_ms,
                test.exit_code,
                test.result,
                test.stdout,
                test.stderr,
                test.log,
                test.summary,
                test.config_digest,
                test.message,
            )
        )
    if [tuple(item) for item in actual_tests] != expected_tests:
        raise ValueError("run_id already exists with different immutable test evidence")


def _history_query(
    *,
    run_id: str | None,
    node: str | None,
    test_id: str | None,
    status: str | None,
    limit: int,
) -> tuple[str, list[Any]]:
    if limit <= 0 or limit > 10_000:
        raise ValueError("history limit must be between 1 and 10000")
    if status is not None and status not in VALID_STATUSES:
        raise ValueError(f"history status must be one of {sorted(VALID_STATUSES)}")
    where: list[str] = []
    params: list[Any] = []
    if run_id:
        where.append("r.run_id = ?")
        params.append(run_id)
    if node:
        where.append("r.node = ?")
        params.append(node)
    if status and not test_id:
        where.append("r.overall_status = ?")
        params.append(status)
    if test_id:
        test_condition = (
            "EXISTS (SELECT 1 FROM run_tests rt "
            "WHERE rt.run_id=r.run_id AND rt.test_id=? AND rt.selected=1"
        )
        params.append(test_id)
        if status:
            test_condition += " AND rt.status=?"
            params.append(status)
        where.append(test_condition + ")")
    clause = " WHERE " + " AND ".join(where) if where else ""
    query = (
        "SELECT r.run_id, r.node, r.started_timestamp, r.started_timestamp_la, "
        "r.completed_timestamp, r.overall_status, "
        "COALESCE((SELECT GROUP_CONCAT(test_id, ',') FROM ("
        "SELECT test_id FROM run_tests rt WHERE rt.run_id=r.run_id "
        "AND rt.selected=1 ORDER BY execution_order, test_id)), '') AS tests_ran, "
        "r.image_name, r.pytorch_version, r.cuda_version, r.git_ref, "
        "r.global_config_digest, r.result_path, r.result_digest, "
        "r.created_at, r.updated_at "
        f"FROM runs r{clause} ORDER BY r.started_timestamp DESC, r.run_id DESC LIMIT ?"
    )
    params.append(limit)
    return query, params


def _row_from_mapping(item: dict[str, Any]) -> RunHistoryRow:
    completed = item.get("completed_timestamp")
    return RunHistoryRow(
        run_id=str(item.get("run_id", "")),
        node=str(item.get("node", "")),
        started_timestamp=int(item.get("started_timestamp") or 0),
        started_timestamp_la=str(item.get("started_timestamp_la", "")),
        completed_timestamp=int(completed) if completed is not None else None,
        overall_status=str(item.get("overall_status", "")),
        tests_ran=str(item.get("tests_ran", "")),
        image_name=str(item.get("image_name", "")),
        pytorch_version=str(item.get("pytorch_version", "")),
        cuda_version=str(item.get("cuda_version", "")),
        git_ref=str(item.get("git_ref", "")),
        global_config_digest=str(item.get("global_config_digest", "")),
        result_path=str(item.get("result_path", "")),
        result_digest=str(item.get("result_digest", "")),
        created_at=int(item.get("created_at") or 0),
        updated_at=int(item.get("updated_at") or 0),
    )


