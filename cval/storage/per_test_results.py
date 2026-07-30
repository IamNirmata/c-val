"""Common, additive storage for modular per-test raw results and receipts."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from contextlib import closing, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterator

from cval.validation.plugins import IngestionConflictError, IngestionReceipt
from cval.validation.registry import (
    RegisteredValidationTest,
    validation_test_config_digest,
)
from cval.storage.paths import safe_writable_file_path
from cval.health.combination import (
    resolve_environment_combination,
    valid_combination_key,
    validate_combination_for_definition,
)


SCHEMA_VERSION = 1
PLUGIN_API_VERSION = "cval.plugin.v1"
VALID_STATUSES = frozenset({"pass", "fail", "incomplete"})
COMMON_RESULT_TABLES = frozenset(
    {
        "schema_migrations",
        "test_results",
        "metric_ingestion_receipts",
        "adapter_schema_versions",
    }
)
COMMON_IMMUTABLE_KEY_GROUPS: dict[str, tuple[tuple[str, ...], ...]] = {
    "schema_migrations": (("version",),),
    "test_results": (("result_id",), ("run_id",)),
    "metric_ingestion_receipts": (("run_id",),),
    "adapter_schema_versions": (("test_id",),),
}
_ACTIVE_METRIC_SESSION: ContextVar[
    tuple[Path, "AdapterSQLiteConnection"] | None
] = (
    ContextVar("cval_active_metric_session", default=None)
)
_ADAPTER_CONNECTIONS: dict[int, sqlite3.Connection] = {}


class AdapterSQLiteConnection:
    """Restricted SQLite facade supplied to ingestion adapters."""

    __slots__ = ()

    @staticmethod
    def _reject_framework_operation(sql: str) -> None:
        statement = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        if statement in {
            "ATTACH",
            "DETACH",
            "BEGIN",
            "COMMIT",
            "END",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
            "VACUUM",
        }:
            raise sqlite3.DatabaseError(
                f"Adapter SQL operation is framework-owned: {statement}"
            )

    def execute(
        self,
        sql: str,
        parameters: tuple[Any, ...] | list[Any] = (),
    ) -> "AdapterSQLiteCursor":
        self._reject_framework_operation(sql)
        return AdapterSQLiteCursor(_adapter_connection(self).execute(sql, parameters))

    def executemany(self, sql: str, parameters: Any) -> "AdapterSQLiteCursor":
        self._reject_framework_operation(sql)
        return AdapterSQLiteCursor(_adapter_connection(self).executemany(sql, parameters))

    def commit(self) -> None:
        raise sqlite3.DatabaseError("Adapter commit is framework-owned")

    def rollback(self) -> None:
        raise sqlite3.DatabaseError("Adapter rollback is framework-owned")

    def set_authorizer(self, _callback: Any) -> None:
        raise sqlite3.DatabaseError("Adapter authorizer is framework-owned")

    def executescript(self, _script: str) -> None:
        raise sqlite3.DatabaseError("Adapter scripts are not permitted")


class AdapterSQLiteCursor:
    """Read-only cursor result facade without a recoverable connection property."""

    __slots__ = ("__rows", "__index", "rowcount", "lastrowid")

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self.__rows = cursor.fetchall()
        self.__index = 0
        self.rowcount = cursor.rowcount
        self.lastrowid = cursor.lastrowid

    def fetchone(self) -> Any:
        if self.__index >= len(self.__rows):
            return None
        row = self.__rows[self.__index]
        self.__index += 1
        return row

    def fetchall(self) -> list[Any]:
        rows = self.__rows[self.__index :]
        self.__index = len(self.__rows)
        return rows

    def __iter__(self) -> "AdapterSQLiteCursor":
        return self

    def __next__(self) -> Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


def _adapter_connection(facade: AdapterSQLiteConnection) -> sqlite3.Connection:
    try:
        return _ADAPTER_CONNECTIONS[id(facade)]
    except KeyError as exc:
        raise sqlite3.DatabaseError("Adapter SQLite session is not active") from exc


def _deny_adapter_transactions(
    action: int,
    arg1: str | None,
    _arg2: str | None,
    _db_name: str | None,
    _trigger_name: str | None,
) -> int:
    denied = {sqlite3.SQLITE_TRANSACTION}
    if hasattr(sqlite3, "SQLITE_SAVEPOINT"):
        denied.add(sqlite3.SQLITE_SAVEPOINT)
    if hasattr(sqlite3, "SQLITE_ATTACH"):
        denied.add(sqlite3.SQLITE_ATTACH)
    if hasattr(sqlite3, "SQLITE_DETACH"):
        denied.add(sqlite3.SQLITE_DETACH)
    if action == sqlite3.SQLITE_PRAGMA and str(arg1 or "").lower() not in {
        "table_info",
        "index_list",
        "index_info",
        "index_xinfo",
        "foreign_key_list",
    }:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_DENY if action in denied else sqlite3.SQLITE_OK


@dataclass(frozen=True)
class PerTestResultRecord:
    """Immutable raw columns owned by the framework in every result DB."""

    run_id: str
    test_id: str
    node: str
    run_timestamp: int
    started_timestamp: int | None
    completed_timestamp: int | None
    status: str
    exit_code: int | None
    image_name: str
    pytorch_version: str
    cuda_version: str
    test_config_digest: str
    result_path: str
    summary_path: str
    artifacts_path: str
    raw_result_json: str
    result_digest: str
    combination_key: str = ""


_RAW_RESULT_COLUMNS = (
    "test_id",
    "node",
    "run_timestamp",
    "started_timestamp",
    "completed_timestamp",
    "status",
    "exit_code",
    "image_name",
    "pytorch_version",
    "cuda_version",
    "test_config_digest",
    "combination_key",
    "result_path",
    "summary_path",
    "artifacts_path",
    "raw_result_json",
    "result_digest",
)


def resolve_test_results_db_path(
    validation_root: str | Path,
    registered_test: RegisteredValidationTest,
) -> Path:
    """Resolve a declared DB path beneath its test-owned validation-root directory."""

    root = Path(validation_root).expanduser()
    if not root.is_absolute():
        raise ValueError("runtime.validation_root must be an absolute path")
    relative = Path(registered_test.definition.artifacts.results_db_path)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ValueError("artifacts.results_db_path must be a confined relative path")
    expected_relative = Path("validation_tests") / registered_test.id
    try:
        relative.relative_to(expected_relative)
    except ValueError as exc:
        raise ValueError(
            f"Result DB for {registered_test.id!r} must stay under {expected_relative}"
        ) from exc

    lexical_target = root / relative
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"Result DB path contains a symlink: {current}")

    resolved_root = root.resolve()
    resolved_owner = (resolved_root / expected_relative).resolve()
    resolved_target = lexical_target.resolve()
    try:
        resolved_target.relative_to(resolved_owner)
    except ValueError as exc:
        raise ValueError(
            f"Result DB for {registered_test.id!r} escapes {resolved_owner}"
        ) from exc
    if resolved_target.exists() and not resolved_target.is_file():
        raise ValueError(f"Result DB path is not a regular file: {resolved_target}")
    return resolved_target


def canonical_payload_digest(payload: Any) -> str:
    """Return a deterministic digest for parsed adapter evidence."""

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def write_per_test_result(
    record: PerTestResultRecord,
    *,
    db_path: str | Path,
    now: int | None = None,
) -> bool:
    """Insert one immutable common raw row, returning false for an exact retry."""

    _validate_record(record)
    path = safe_writable_file_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path = safe_writable_file_path(path)
    now = int(time.time()) if now is None else int(now)
    with closing(
        sqlite3.connect(f"file:{path}?mode=rwc", uri=True, timeout=30)
    ) as connection:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA recursive_triggers=ON")
        if not _database_is_empty(connection):
            _assert_supported_schema(connection, allow_empty=False)
            _validate_schema_shape(connection)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        try:
            _prepare_schema(connection)
            existing = connection.execute(
                f"SELECT {', '.join(_RAW_RESULT_COLUMNS)} "
                "FROM test_results WHERE run_id=?",
                (record.run_id,),
            ).fetchone()
            expected = _record_values(record)
            if existing is not None:
                if tuple(existing) != expected:
                    raise IngestionConflictError(
                        f"Per-test run {record.run_id!r} already has different raw evidence"
                    )
                connection.commit()
                return False
            connection.execute(
                """
                INSERT INTO test_results (
                    run_id, test_id, node, run_timestamp,
                    started_timestamp, completed_timestamp,
                    status, exit_code, image_name, pytorch_version, cuda_version,
                    test_config_digest, combination_key, result_path, summary_path,
                    artifacts_path, raw_result_json, result_digest,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (record.run_id, *_record_values(record), now, now),
            )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise


@contextmanager
def framework_metric_ingestion_session(
    db_path: str | Path,
) -> Iterator[AdapterSQLiteConnection]:
    """Own one adapter transaction until the dispatcher validates its receipt."""

    path = safe_writable_file_path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Per-test result DB is not initialized: {path}")
    with closing(
        sqlite3.connect(f"file:{path}?mode=rw", uri=True, timeout=30)
    ) as connection:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA recursive_triggers=ON")
        _assert_supported_schema(connection, allow_empty=False)
        _validate_schema_shape(connection)
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        adapter_connection = AdapterSQLiteConnection()
        _ADAPTER_CONNECTIONS[id(adapter_connection)] = connection
        token = _ACTIVE_METRIC_SESSION.set((path, adapter_connection))
        try:
            connection.set_authorizer(_deny_adapter_transactions)
            yield adapter_connection
            connection.set_authorizer(None)
            connection.commit()
        except Exception:
            connection.set_authorizer(None)
            connection.rollback()
            raise
        finally:
            connection.set_authorizer(None)
            _ACTIVE_METRIC_SESSION.reset(token)
            _ADAPTER_CONNECTIONS.pop(id(adapter_connection), None)


@contextmanager
def metric_ingestion_transaction(
    db_path: str | Path,
    *,
    test_id: str,
    adapter_schema_version: int,
    validate_adapter_schema: Callable[[Any, bool], bool],
) -> Iterator[AdapterSQLiteConnection]:
    """Open a bounded transaction in an already initialized per-test DB."""

    path = safe_writable_file_path(db_path)
    active = _ACTIVE_METRIC_SESSION.get()
    if active is not None:
        active_path, adapter_connection = active
        if active_path != path:
            raise RuntimeError("Adapter attempted to write outside its framework transaction")
        _assert_supported_schema(adapter_connection, allow_empty=False)
        _validate_schema_shape(adapter_connection)
        _validate_adapter_version_and_tables(
            adapter_connection,
            test_id=test_id,
            adapter_schema_version=adapter_schema_version,
            validate_adapter_schema=validate_adapter_schema,
        )
        yield adapter_connection
        return
    if not path.is_file():
        raise FileNotFoundError(f"Per-test result DB is not initialized: {path}")
    with closing(
        sqlite3.connect(f"file:{path}?mode=rw", uri=True, timeout=30)
    ) as connection:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA recursive_triggers=ON")
        _assert_supported_schema(connection, allow_empty=False)
        _validate_schema_shape(connection)
        _validate_adapter_version_and_tables(
            connection,
            test_id=test_id,
            adapter_schema_version=adapter_schema_version,
            validate_adapter_schema=validate_adapter_schema,
        )
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        adapter_connection = AdapterSQLiteConnection()
        _ADAPTER_CONNECTIONS[id(adapter_connection)] = connection
        try:
            connection.set_authorizer(_deny_adapter_transactions)
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS adapter_schema_versions (
                    test_id TEXT PRIMARY KEY,
                    version INTEGER NOT NULL CHECK (version > 0),
                    applied_at INTEGER NOT NULL
                )
                """
            )
            yield adapter_connection
            connection.set_authorizer(None)
            connection.commit()
        except Exception:
            connection.set_authorizer(None)
            connection.rollback()
            raise
        finally:
            connection.set_authorizer(None)
            _ADAPTER_CONNECTIONS.pop(id(adapter_connection), None)


def _validate_adapter_version_and_tables(
    connection: Any,
    *,
    test_id: str,
    adapter_schema_version: int,
    validate_adapter_schema: Callable[[Any, bool], bool],
) -> None:
    adapter_tables_present = validate_adapter_schema(connection, True)
    version_rows = connection.execute(
        "SELECT test_id, version FROM adapter_schema_versions ORDER BY test_id"
    ).fetchall()
    if version_rows not in ([], [(test_id, adapter_schema_version)]):
        raise RuntimeError(
            f"Unsupported adapter schema version manifest: {version_rows}"
        )
    if not version_rows:
        if adapter_tables_present:
            raise RuntimeError(f"Adapter schema for {test_id!r} lacks version metadata")
    elif not adapter_tables_present:
        raise RuntimeError(
            f"Adapter schema for {test_id!r} is missing its metric tables"
        )


def record_adapter_schema_version(
    connection: sqlite3.Connection,
    *,
    test_id: str,
    version: int,
) -> None:
    """Record one adapter metric schema version inside its first write transaction."""

    existing = connection.execute(
        "SELECT version FROM adapter_schema_versions WHERE test_id=?",
        (test_id,),
    ).fetchone()
    if existing is not None:
        if int(existing[0]) != version:
            raise RuntimeError(
                f"Unsupported adapter schema version for {test_id!r}: {existing[0]}"
            )
        return
    connection.execute(
        "INSERT INTO adapter_schema_versions(test_id, version, applied_at) "
        "VALUES (?, ?, ?)",
        (test_id, version, int(time.time())),
    )


def validate_table_manifest(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    required_columns: set[str],
    column_specs: dict[str, tuple[str, bool, str | None, int]] | None = None,
    primary_key: tuple[str, ...] = (),
    required_sql_fragments: tuple[str, ...] = (),
    constraint_counts: tuple[int, int] | None = None,
    allowed_indexes: set[str] | None = None,
    implicit_indexes: set[
        tuple[str, tuple[str, ...], tuple[str, ...]]
    ]
    | None = None,
    allowed_triggers: set[str] | frozenset[str] | None = None,
    immutable_key_groups: tuple[tuple[str, ...], ...] = (),
    allow_missing: bool = False,
) -> bool:
    """Validate one adapter table's minimum additive manifest without mutation."""

    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    if not rows:
        if allow_missing:
            return False
        raise RuntimeError(f"Adapter schema is missing table: {table_name}")
    columns = {str(row[1]) for row in rows}
    if columns != required_columns:
        raise RuntimeError(
            f"Adapter table {table_name} columns {sorted(columns)!r} do not match "
            f"manifest {sorted(required_columns)!r}"
        )
    actual_pk = tuple(
        str(row[1]) for row in sorted(rows, key=lambda row: int(row[5])) if int(row[5])
    )
    if primary_key and actual_pk != primary_key:
        raise RuntimeError(
            f"Adapter table {table_name} has primary key {actual_pk!r}; "
            f"expected {primary_key!r}"
        )
    for column_name, expected in (column_specs or {}).items():
        row = next((item for item in rows if str(item[1]) == column_name), None)
        if row is None:
            raise RuntimeError(
                f"Adapter table {table_name} is missing column: {column_name}"
            )
        actual = (
            str(row[2]).upper(),
            bool(row[3]),
            None if row[4] is None else str(row[4]),
            int(row[5]),
        )
        if actual != expected:
            raise RuntimeError(
                f"Table {table_name} column {column_name} definition {actual!r} "
                f"does not match {expected!r}"
            )
    if required_sql_fragments or constraint_counts is not None:
        sql_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        raw_sql = "" if sql_row is None or sql_row[0] is None else str(sql_row[0])
        normalized_sql = _canonical_constraint_sql(raw_sql)
        for fragment in required_sql_fragments:
            if _canonical_constraint_sql(fragment) not in normalized_sql:
                raise RuntimeError(
                    f"Table {table_name} is missing required constraint: {fragment}"
                )
        if constraint_counts is not None:
            count_sql = re.sub(r"'(?:''|[^'])*'", "''", normalized_sql)
            actual_counts = (
                len(re.findall(r"\bCHECK\s*\(", count_sql)),
                len(re.findall(r"\bFOREIGN\s+KEY\s*\(", count_sql)),
            )
            if actual_counts != constraint_counts:
                raise RuntimeError(
                    f"Table {table_name} constraint counts {actual_counts!r} do not "
                    f"match manifest {constraint_counts!r}"
                )
    triggers = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
            (table_name,),
        )
    }
    expected_triggers = set(
        immutable_table_trigger_names(table_name)
        if immutable_key_groups
        else (allowed_triggers or ())
    )
    if triggers != expected_triggers:
        raise RuntimeError(
            f"Table {table_name} triggers {sorted(triggers)!r} do not match "
            f"manifest {sorted(expected_triggers)!r}"
        )
    if immutable_key_groups:
        expected_sql = _immutable_trigger_sql(table_name, immutable_key_groups)
        actual_sql = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type='trigger' AND tbl_name=? ORDER BY name",
                (table_name,),
            )
        }
        if any(
            _normalize_ddl(actual_sql[name]) != _normalize_ddl(sql)
            for name, sql in expected_sql.items()
        ):
            raise RuntimeError(
                f"Table {table_name} immutable trigger DDL does not match"
            )
    if allowed_indexes is not None:
        actual_indexes = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? "
                "AND name NOT LIKE 'sqlite_autoindex_%'",
                (table_name,),
            )
        }
        if actual_indexes != allowed_indexes:
            raise RuntimeError(
                f"Table {table_name} indexes {sorted(actual_indexes)!r} do not match "
                f"manifest {sorted(allowed_indexes)!r}"
            )
    if implicit_indexes is not None:
        actual_implicit: set[
            tuple[str, tuple[str, ...], tuple[str, ...]]
        ] = set()
        for index_row in connection.execute(f"PRAGMA index_list({table_name})"):
            origin = str(index_row[3])
            if origin not in {"u", "pk"}:
                continue
            name = str(index_row[1])
            columns = tuple(
                str(item[2])
                for item in connection.execute(f"PRAGMA index_info({name})")
            )
            collations = tuple(
                str(item[4]).upper()
                for item in connection.execute(f"PRAGMA index_xinfo({name})")
                if int(item[5]) == 1
            )
            actual_implicit.add((origin, columns, collations))
        if actual_implicit != implicit_indexes:
            raise RuntimeError(
                f"Table {table_name} implicit indexes {sorted(actual_implicit)!r} "
                f"do not match manifest {sorted(implicit_indexes)!r}"
            )
    return True


def require_exact_table_sql(
    connection: Any,
    table_name: str,
    expected_sql: str,
) -> None:
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    actual = "" if row is None or row[0] is None else str(row[0])
    if _normalize_ddl(actual) != _normalize_ddl(expected_sql):
        raise RuntimeError(
            f"Table {table_name} DDL does not match its exact schema manifest"
        )


def require_database_tables(
    connection: sqlite3.Connection,
    allowed_tables: set[str] | frozenset[str],
) -> None:
    """Reject every unversioned/unowned table in a per-test result database."""

    actual = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name != 'sqlite_sequence'"
        )
    }
    if not actual.issubset(set(allowed_tables)):
        raise RuntimeError(
            f"Database contains unmanifested table(s): {sorted(actual - set(allowed_tables))}"
        )


def require_database_views(
    connection: sqlite3.Connection,
    allowed_views: set[str] | frozenset[str],
    *,
    immutable_tables: dict[
        str,
        tuple[tuple[str, ...], ...],
    ] | None = None,
) -> None:
    actual = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        )
    }
    if actual != set(allowed_views):
        raise RuntimeError(
            f"Database views {sorted(actual)!r} do not match manifest "
            f"{sorted(allowed_views)!r}"
        )
    _require_exact_immutable_triggers(connection, immutable_tables or {})


def immutable_table_trigger_names(table_name: str) -> frozenset[str]:
    _validate_sql_identifier(table_name)
    return frozenset(
        {
            f"trg_{table_name}_immutable_insert",
            f"trg_{table_name}_immutable_update",
            f"trg_{table_name}_immutable_delete",
        }
    )


def prepare_immutable_table_triggers(
    connection: Any,
    table_name: str,
    key_groups: tuple[tuple[str, ...], ...],
) -> None:
    """Install exact append-only guards for one framework/adapter table."""

    _validate_sql_identifier(table_name)
    if not key_groups or any(not group for group in key_groups):
        raise ValueError("Immutable table key groups must be non-empty")
    conflict_groups: list[str] = []
    conflict_groups.append(
        f"(NEW.rowid != -1 AND EXISTS (SELECT 1 FROM {table_name} existing "
        "WHERE existing.rowid IS NEW.rowid))"
    )
    conflict_groups.append("(NEW.rowid != -1 AND NEW.rowid <= 0)")
    for group in key_groups:
        for column in group:
            _validate_sql_identifier(column)
        nonnull = " AND ".join(f"NEW.{column} IS NOT NULL" for column in group)
        equality = " AND ".join(
            f"existing.{column} IS NEW.{column}" for column in group
        )
        conflict_groups.append(
            f"(({nonnull}) AND EXISTS (SELECT 1 FROM {table_name} existing "
            f"WHERE {equality}))"
        )
    conflict_when = " OR ".join(conflict_groups)
    connection.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS trg_{table_name}_immutable_insert
        BEFORE INSERT ON {table_name}
        WHEN {conflict_when}
        BEGIN
            SELECT RAISE(ABORT, '{table_name} replacement inserts are forbidden');
        END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS trg_{table_name}_immutable_update
        BEFORE UPDATE ON {table_name}
        BEGIN
            SELECT RAISE(ABORT, '{table_name} rows are immutable');
        END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER IF NOT EXISTS trg_{table_name}_immutable_delete
        BEFORE DELETE ON {table_name}
        BEGIN
            SELECT RAISE(ABORT, '{table_name} rows are immutable');
        END
        """
    )


@lru_cache(maxsize=None)
def _immutable_trigger_sql(
    table_name: str,
    key_groups: tuple[tuple[str, ...], ...],
) -> dict[str, str]:
    _validate_sql_identifier(table_name)
    with closing(sqlite3.connect(":memory:")) as connection:
        columns = sorted({column for group in key_groups for column in group})
        connection.execute(
            f"CREATE TABLE {table_name}("
            + ", ".join(f"{column} BLOB" for column in columns)
            + ")"
        )
        prepare_immutable_table_triggers(connection, table_name, key_groups)
        return {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type='trigger' AND tbl_name=? ORDER BY name",
                (table_name,),
            )
        }


def _require_exact_immutable_triggers(
    connection: Any,
    immutable_tables: dict[str, tuple[tuple[str, ...], ...]],
) -> None:
    expected: dict[str, str] = {}
    for table_name, key_groups in sorted(immutable_tables.items()):
        expected.update(_immutable_trigger_sql(table_name, key_groups))
    actual = {
        str(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' ORDER BY name"
        )
    }
    if set(actual) != set(expected) or any(
        _normalize_ddl(actual[name]) != _normalize_ddl(sql)
        for name, sql in expected.items()
    ):
        raise RuntimeError("Database immutable trigger manifest does not match")
    for table_name in immutable_tables:
        if connection.execute(
            f"SELECT 1 FROM {table_name} WHERE rowid <= 0 LIMIT 1"
        ).fetchone() is not None:
            raise RuntimeError(
                f"Immutable table {table_name} contains an invalid hidden rowid"
            )


def _validate_sql_identifier(value: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*",
        value,
    ):
        raise ValueError(f"Unsafe SQLite identifier: {value!r}")


def validate_common_only_result_database(db_path: str | Path) -> None:
    path = safe_writable_file_path(db_path)
    with closing(
        sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    ) as connection:
        _assert_supported_schema(connection, allow_empty=False)
        _validate_schema_shape(connection)
        require_database_tables(connection, COMMON_RESULT_TABLES)
        require_database_views(
            connection,
            set(),
            immutable_tables=COMMON_IMMUTABLE_KEY_GROUPS,
        )


def validate_common_result_connection(connection: Any) -> None:
    """Validate the complete common schema on an already-open read-only connection."""

    if isinstance(connection, sqlite3.Connection) and not connection.in_transaction:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")

    _assert_supported_schema(connection, allow_empty=False)
    _validate_schema_shape(connection)


@dataclass(frozen=True)
class HealthReadIdentity:
    result_id: int
    run_id: str
    node: str
    run_timestamp: int
    started_timestamp: int
    completed_timestamp: int
    image_name: str
    pytorch_version: str
    cuda_version: str


@dataclass(frozen=True)
class HealthReadReceipt:
    run_id: str
    evidence_digest: str
    inserted_count: int
    updated_count: int
    metric_names: tuple[str, ...]


@dataclass(frozen=True)
class HealthReadMetadata:
    identities: dict[int, HealthReadIdentity]
    receipts: dict[str, HealthReadReceipt]


def validate_health_read_metadata(
    connection: Any,
    *,
    test_id: str,
    adapter_schema_version: int,
    result_ids: tuple[int, ...],
    source_snapshot: Any,
    definition: Any,
    combination: Any,
) -> HealthReadMetadata:
    """Require exact owner/version/receipt data before health adapters read metrics."""

    if isinstance(connection, sqlite3.Connection) and not connection.in_transaction:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
    validate_combination_for_definition(combination, definition)
    test_config_digest = validation_test_config_digest(definition)
    combination_key = combination.key
    if not result_ids or any(
        isinstance(result_id, bool)
        or not isinstance(result_id, int)
        or result_id <= 0
        for result_id in result_ids
    ) or len(set(result_ids)) != len(result_ids):
        raise ValueError("Health reader result IDs must be unique positive integers")
    versions = connection.execute(
        "SELECT test_id, version, applied_at FROM adapter_schema_versions ORDER BY test_id"
    ).fetchall()
    if (
        len(versions) != 1
        or versions[0][:2] != (test_id, adapter_schema_version)
        or isinstance(versions[0][2], bool)
        or not isinstance(versions[0][2], int)
        or versions[0][2] < 0
    ):
        raise RuntimeError("Health reader adapter schema version manifest is invalid")
    owners = connection.execute(
        "SELECT DISTINCT test_id FROM test_results ORDER BY test_id"
    ).fetchall()
    if owners not in ([], [(test_id,)]):
        raise RuntimeError("Health reader raw result owner manifest is invalid")
    placeholders = ", ".join("?" for _ in result_ids)
    raw_rows = connection.execute(
        "SELECT result_id, run_id, test_id, node, run_timestamp, started_timestamp, "
        "completed_timestamp, status, image_name, pytorch_version, cuda_version, "
        "test_config_digest, combination_key, raw_result_json, result_digest "
        f"FROM test_results WHERE result_id IN ({placeholders}) ORDER BY result_id",
        result_ids,
    ).fetchall()
    if len(raw_rows) != len(result_ids):
        raise RuntimeError("Health reader source snapshot contains missing raw results")
    identities: dict[int, HealthReadIdentity] = {}
    selected_run_ids: list[str] = []
    for row in raw_rows:
        if (
            isinstance(row[0], bool)
            or not isinstance(row[0], int)
            or row[0] <= 0
            or not isinstance(row[1], str)
            or not row[1]
            or row[2] != test_id
            or not isinstance(row[3], str)
            or not row[3]
            or isinstance(row[4], bool)
            or not isinstance(row[4], int)
            or row[4] < 0
            or isinstance(row[5], bool)
            or not isinstance(row[5], int)
            or row[5] < 0
            or isinstance(row[6], bool)
            or not isinstance(row[6], int)
            or row[6] < row[5]
            or row[7] != "pass"
            or not all(isinstance(value, str) for value in row[8:11])
            or row[11] != test_config_digest
            or row[12] != combination_key
            or not isinstance(row[13], str)
            or not isinstance(row[14], str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", row[14])
        ):
            raise RuntimeError("Health reader raw result provenance is invalid")
        expected_source = next(
            (
                source
                for source in source_snapshot.results
                if source.result_id == row[0]
            ),
            None,
        )
        raw_digest = "sha256:" + hashlib.sha256(row[13].encode("utf-8")).hexdigest()
        if (
            expected_source is None
            or expected_source.run_id != row[1]
            or expected_source.completed_timestamp != row[6]
            or expected_source.result_digest != row[14]
            or expected_source.raw_result_digest != raw_digest
            or expected_source.test_config_digest != row[11]
            or expected_source.combination_key != row[12]
        ):
            raise RuntimeError("Health reader source snapshot provenance is invalid")
        try:
            payload = json.loads(row[13])
        except json.JSONDecodeError as exc:
            raise RuntimeError("Health reader raw result JSON is invalid") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != "cval.test-result.v1"
            or payload.get("test_id") != test_id
            or payload.get("status") != "pass"
            or json.dumps(payload, sort_keys=True, separators=(",", ":")) != row[13]
        ):
            raise RuntimeError("Health reader raw result JSON provenance is invalid")
        identity = HealthReadIdentity(
            result_id=row[0],
            run_id=row[1],
            node=row[3],
            run_timestamp=row[4],
            started_timestamp=row[5],
            completed_timestamp=row[6],
            image_name=row[8],
            pytorch_version=row[9],
            cuda_version=row[10],
        )
        identities[row[0]] = identity
        selected_run_ids.append(row[1])
        recomputed_combination = resolve_environment_combination(
            definition,
            {
                "image_name": identity.image_name,
                "pytorch_version": identity.pytorch_version,
                "cuda_version": identity.cuda_version,
            },
        )
        if recomputed_combination != combination:
            raise RuntimeError(
                "Health reader raw environment does not match the combination"
            )

    receipts = connection.execute(
        """
        SELECT mr.run_id, mr.test_id, mr.adapter_api_version,
               mr.evidence_digest, mr.inserted_count, mr.updated_count,
               mr.metric_names_json, mr.created_at, tr.test_id
        FROM metric_ingestion_receipts mr
        LEFT JOIN test_results tr ON tr.run_id=mr.run_id
        ORDER BY mr.run_id
        """
    ).fetchall()
    receipt_map: dict[str, HealthReadReceipt] = {}
    for row in receipts:
        try:
            metric_names = json.loads(row[6])
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Health reader receipt metric_names_json is invalid") from exc
        if (
            not isinstance(row[0], str)
            or not row[0]
            or row[1] != test_id
            or row[2] != PLUGIN_API_VERSION
            or not isinstance(row[3], str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", row[3])
            or isinstance(row[4], bool)
            or not isinstance(row[4], int)
            or row[4] <= 0
            or isinstance(row[5], bool)
            or not isinstance(row[5], int)
            or row[5] != 0
            or not isinstance(metric_names, list)
            or not metric_names
            or metric_names != sorted(set(metric_names))
            or not all(isinstance(name, str) and name for name in metric_names)
            or json.dumps(metric_names, separators=(",", ":")) != row[6]
            or isinstance(row[7], bool)
            or not isinstance(row[7], int)
            or row[7] < 0
            or row[8] != test_id
        ):
            raise RuntimeError("Health reader durable receipt manifest is invalid")
        receipt_map[row[0]] = HealthReadReceipt(
            run_id=row[0],
            evidence_digest=row[3],
            inserted_count=row[4],
            updated_count=row[5],
            metric_names=tuple(metric_names),
        )
    if not set(selected_run_ids).issubset(receipt_map):
        raise RuntimeError("Health reader source snapshot lacks durable receipts")
    for source in source_snapshot.results:
        receipt = receipt_map.get(source.run_id)
        if (
            source.adapter_schema_version != adapter_schema_version
            or receipt is None
            or source.receipt_evidence_digest != receipt.evidence_digest
        ):
            raise RuntimeError("Health reader source adapter/receipt provenance is invalid")
    return HealthReadMetadata(identities, receipt_map)


def _strip_sql_comments(sql: str) -> str:
    without_blocks = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\r\n]*", " ", without_blocks)


def _canonical_constraint_sql(sql: str) -> str:
    without_comments = _strip_sql_comments(sql)
    quoted_identifiers = re.sub(
        r'"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]',
        '"?"',
        without_comments,
    )
    parts = re.split(r"('(?:''|[^'])*')", quoted_identifiers)
    canonical = "".join(
        part if index % 2 else part.upper()
        for index, part in enumerate(parts)
    )
    return " ".join(canonical.split())


def _normalize_ddl(sql: str) -> str:
    return _canonical_constraint_sql(sql).replace(" IF NOT EXISTS ", " ")


def require_schema_objects(
    connection: sqlite3.Connection,
    *,
    indexes: dict[
        str,
        tuple[
            str,
            tuple[str, ...],
            tuple[bool, ...],
            tuple[str, ...],
            bool,
            str,
        ],
    ]
    | None = None,
    views: tuple[str, ...] = (),
    view_sql: dict[str, str] | None = None,
) -> None:
    """Require named adapter indexes/views before accepting a known schema version."""

    objects = {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT type, name FROM sqlite_master WHERE type IN ('index','view')"
        )
    }
    index_specs = indexes or {}
    missing_indexes = [name for name in index_specs if ("index", name) not in objects]
    view_specs = view_sql or {}
    required_views = (*views, *view_specs.keys())
    missing_views = [name for name in required_views if ("view", name) not in objects]
    if missing_indexes or missing_views:
        raise RuntimeError(
            "Adapter schema is missing object(s): "
            f"indexes={missing_indexes}, views={missing_views}"
        )
    for name, (
        expected_owner,
        expected_columns,
        expected_descending,
        expected_collations,
        expected_unique,
        expected_where,
    ) in index_specs.items():
        row = connection.execute(
            "SELECT tbl_name, sql FROM sqlite_master WHERE type='index' AND name=?",
            (name,),
        ).fetchone()
        key_rows = [
            item
            for item in connection.execute(f"PRAGMA index_xinfo({name})")
            if int(item[5]) == 1
        ]
        actual_columns = tuple(str(item[2]) for item in key_rows)
        actual_descending = tuple(bool(item[3]) for item in key_rows)
        actual_collations = tuple(str(item[4]).upper() for item in key_rows)
        sql = "" if row is None or row[1] is None else str(row[1])
        owner_indexes = {
            str(item[1]): bool(item[2])
            for item in connection.execute(f"PRAGMA index_list({expected_owner})")
        }
        actual_unique = owner_indexes.get(name)
        normalized_where = " ".join(sql.upper().split())
        actual_predicate = (
            normalized_where.split(" WHERE ", 1)[1]
            if " WHERE " in normalized_where
            else ""
        )
        expected_predicate = " ".join(expected_where.upper().split())
        if expected_predicate.startswith("WHERE "):
            expected_predicate = expected_predicate[6:]
        if (
            row is None
            or str(row[0]) != expected_owner
            or actual_columns != expected_columns
            or actual_descending != expected_descending
            or actual_collations != expected_collations
            or actual_unique != expected_unique
            or actual_predicate != expected_predicate
        ):
            raise RuntimeError(
                f"Index {name} definition does not match its schema manifest"
            )
    for name, expected_sql in view_specs.items():
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='view' AND name=?",
            (name,),
        ).fetchone()
        actual = " ".join(
            ("" if row is None or row[0] is None else str(row[0])).upper().split()
        ).replace("CREATE VIEW IF NOT EXISTS ", "CREATE VIEW ", 1)
        expected = " ".join(expected_sql.upper().split()).replace(
            "CREATE VIEW IF NOT EXISTS ", "CREATE VIEW ", 1
        )
        if actual != expected:
            raise RuntimeError(
                f"View {name} definition does not match its schema manifest"
            )


def existing_metric_ingestion_receipt(
    connection: sqlite3.Connection,
    *,
    test_id: str,
    run_id: str,
    evidence_digest: str,
    expected_inserted_count: int,
    expected_updated_count: int,
    expected_metric_names: tuple[str, ...],
) -> IngestionReceipt | None:
    """Return an exact-retry receipt or reject changed metric evidence."""

    row = connection.execute(
        "SELECT test_id, adapter_api_version, evidence_digest, inserted_count, "
        "updated_count, metric_names_json, created_at "
        "FROM metric_ingestion_receipts WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    if (str(row[0]), str(row[1]), str(row[2])) != (
        test_id,
        PLUGIN_API_VERSION,
        evidence_digest,
    ):
        raise IngestionConflictError(
            f"Metric run {run_id!r} already has different adapter evidence"
        )
    try:
        metric_names_raw = json.loads(str(row[5]))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid metric receipt for run {run_id!r}") from exc
    if not isinstance(metric_names_raw, list) or not all(
        isinstance(name, str) for name in metric_names_raw
    ):
        raise RuntimeError(f"Invalid metric names in receipt for run {run_id!r}")
    expected_names = tuple(sorted(expected_metric_names))
    if (
        int(row[3]) != expected_inserted_count
        or int(row[4]) != expected_updated_count
        or tuple(metric_names_raw) != expected_names
    ):
        raise IngestionConflictError(
            f"Metric run {run_id!r} has a changed durable ingestion receipt"
        )
    return IngestionReceipt(
        test_id=test_id,
        run_id=run_id,
        inserted_count=expected_inserted_count,
        updated_count=expected_updated_count,
        metric_names=expected_names,
        evidence_digest=evidence_digest,
        created_at=int(row[6]),
        message="idempotent retry",
    )


def record_metric_ingestion_receipt(
    connection: sqlite3.Connection,
    receipt: IngestionReceipt,
    *,
    evidence_digest: str,
    now: int | None = None,
) -> IngestionReceipt:
    """Persist an adapter receipt in the same transaction as its metric rows."""

    if receipt.inserted_count < 0 or receipt.updated_count < 0:
        raise ValueError("Ingestion receipt counts must be non-negative")
    owner = connection.execute(
        "SELECT test_id FROM test_results WHERE run_id=?",
        (receipt.run_id,),
    ).fetchone()
    if owner is None or str(owner[0]) != receipt.test_id:
        raise IngestionConflictError(
            "Metric receipt run/test identity does not match its common raw row"
        )
    names = tuple(sorted(set(receipt.metric_names)))
    created_at = int(time.time()) if now is None else int(now)
    connection.execute(
        """
        INSERT INTO metric_ingestion_receipts (
            run_id, test_id, adapter_api_version, evidence_digest,
            inserted_count, updated_count, metric_names_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            receipt.run_id,
            receipt.test_id,
            PLUGIN_API_VERSION,
            evidence_digest,
            receipt.inserted_count,
            receipt.updated_count,
            json.dumps(names, separators=(",", ":")),
            created_at,
        ),
    )
    return IngestionReceipt(
        test_id=receipt.test_id,
        run_id=receipt.run_id,
        inserted_count=receipt.inserted_count,
        updated_count=receipt.updated_count,
        metric_names=names,
        evidence_digest=evidence_digest,
        created_at=created_at,
        message=receipt.message,
    )


def _validate_record(record: PerTestResultRecord) -> None:
    if record.status not in VALID_STATUSES:
        raise ValueError(f"Invalid per-test raw status: {record.status!r}")
    if (
        record.started_timestamp is not None
        and record.completed_timestamp is not None
        and record.completed_timestamp < record.started_timestamp
    ):
        raise ValueError("Per-test completion precedes start")
    if (
        isinstance(record.run_timestamp, bool)
        or not isinstance(record.run_timestamp, int)
        or record.run_timestamp < 0
    ):
        raise ValueError("Per-test run_timestamp must be a non-negative integer")
    try:
        payload = json.loads(record.raw_result_json)
    except json.JSONDecodeError as exc:
        raise ValueError("raw_result_json must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("raw_result_json must contain an object")
    if payload.get("schema_version") != "cval.test-result.v1":
        raise ValueError("raw_result_json must use cval.test-result.v1")
    if payload.get("test_id") != record.test_id:
        raise ValueError("raw_result_json test_id does not match result owner")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", record.result_digest):
        raise ValueError("result_digest must be a SHA-256 digest")
    if not valid_combination_key(record.combination_key):
        raise ValueError("combination_key must be empty or a SHA-256 digest")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if canonical != record.raw_result_json:
        raise ValueError("raw_result_json must use canonical JSON serialization")


def _record_values(record: PerTestResultRecord) -> tuple[Any, ...]:
    return (
        record.test_id,
        record.node,
        record.run_timestamp,
        record.started_timestamp,
        record.completed_timestamp,
        record.status,
        record.exit_code,
        record.image_name,
        record.pytorch_version,
        record.cuda_version,
        record.test_config_digest,
        record.combination_key,
        record.result_path,
        record.summary_path,
        record.artifacts_path,
        record.raw_result_json,
        record.result_digest,
    )


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
        CREATE TABLE IF NOT EXISTS test_results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            test_id TEXT NOT NULL,
            node TEXT NOT NULL,
            run_timestamp INTEGER NOT NULL CHECK (run_timestamp >= 0),
            started_timestamp INTEGER,
            completed_timestamp INTEGER,
            status TEXT NOT NULL
                CHECK (status IN ('pass', 'fail', 'incomplete')),
            exit_code INTEGER,
            image_name TEXT NOT NULL DEFAULT '',
            pytorch_version TEXT NOT NULL DEFAULT '',
            cuda_version TEXT NOT NULL DEFAULT '',
            test_config_digest TEXT NOT NULL DEFAULT '',
            combination_key TEXT NOT NULL DEFAULT '',
            result_path TEXT NOT NULL DEFAULT '',
            summary_path TEXT NOT NULL DEFAULT '',
            artifacts_path TEXT NOT NULL DEFAULT '',
            raw_result_json TEXT NOT NULL,
            result_digest TEXT NOT NULL,
            health_class_name TEXT,
            health_class_numerical INTEGER
                CHECK (health_class_numerical IS NULL
                       OR health_class_numerical BETWEEN 0 AND 5),
            health_baseline_id TEXT,
            evaluated_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            CHECK (completed_timestamp IS NULL OR started_timestamp IS NULL
                   OR completed_timestamp >= started_timestamp),
            CHECK ((health_class_name IS NULL) = (health_class_numerical IS NULL)),
            CHECK (health_baseline_id IS NULL OR health_class_numerical IS NOT NULL),
            CHECK (evaluated_at IS NULL OR health_class_numerical IS NOT NULL)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS metric_ingestion_receipts (
            run_id TEXT PRIMARY KEY,
            test_id TEXT NOT NULL,
            adapter_api_version TEXT NOT NULL,
            evidence_digest TEXT NOT NULL,
            inserted_count INTEGER NOT NULL CHECK (inserted_count >= 0),
            updated_count INTEGER NOT NULL CHECK (updated_count >= 0),
            metric_names_json TEXT NOT NULL DEFAULT '[]',
            created_at INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES test_results(run_id) ON DELETE RESTRICT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS adapter_schema_versions (
            test_id TEXT PRIMARY KEY,
            version INTEGER NOT NULL CHECK (version > 0),
            applied_at INTEGER NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_test_results_node_completed "
        "ON test_results(node, completed_timestamp DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_test_results_combination_completed "
        "ON test_results(combination_key, completed_timestamp DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_test_results_unevaluated "
        "ON test_results(combination_key, completed_timestamp) "
        "WHERE health_class_numerical IS NULL"
    )
    migration = connection.execute(
        "SELECT version FROM schema_migrations WHERE version=?",
        (SCHEMA_VERSION,),
    ).fetchone()
    if migration is None:
        connection.execute(
            "INSERT INTO schema_migrations(version, name, applied_at) "
            "VALUES (?, ?, ?)",
            (SCHEMA_VERSION, "initial-per-test-results", int(time.time())),
        )
    for table_name, key_groups in sorted(COMMON_IMMUTABLE_KEY_GROUPS.items()):
        prepare_immutable_table_triggers(connection, table_name, key_groups)
    if validate:
        _validate_schema_shape(connection)


@lru_cache(maxsize=1)
def _common_table_sql_manifest() -> dict[str, str]:
    with closing(sqlite3.connect(":memory:")) as connection:
        _prepare_schema(connection, validate=False)
        return {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='table' "
                "AND name IN ('schema_migrations','test_results',"
                "'metric_ingestion_receipts','adapter_schema_versions')"
            )
        }


def _assert_supported_schema(
    connection: sqlite3.Connection,
    *,
    allow_empty: bool,
) -> None:
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        if str(row[0]) != "sqlite_sequence"
    }
    if "schema_migrations" not in tables:
        if allow_empty and not tables:
            return
        raise RuntimeError("per-test result database lacks schema_migrations")
    versions = connection.execute(
        "SELECT version, name FROM schema_migrations ORDER BY version"
    ).fetchall()
    expected = [(SCHEMA_VERSION, "initial-per-test-results")]
    if versions != expected:
        raise RuntimeError(
            f"Unsupported per-test result schema migration manifest: {versions}"
        )


def _database_is_empty(connection: sqlite3.Connection) -> bool:
    return not any(
        str(row[0]) != "sqlite_sequence"
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    )


def _validate_schema_shape(connection: sqlite3.Connection) -> None:
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    required_tables = {
        "schema_migrations",
        "test_results",
        "metric_ingestion_receipts",
        "adapter_schema_versions",
    }
    missing_tables = sorted(required_tables - tables)
    if missing_tables:
        raise RuntimeError(
            f"Per-test result schema is missing table(s): {', '.join(missing_tables)}"
        )
    manifests = {
        "schema_migrations": {"version", "name", "applied_at"},
        "test_results": {
            "result_id",
            "run_id",
            *_RAW_RESULT_COLUMNS,
            "health_class_name",
            "health_class_numerical",
            "health_baseline_id",
            "evaluated_at",
            "created_at",
            "updated_at",
        },
        "metric_ingestion_receipts": {
            "run_id",
            "test_id",
            "adapter_api_version",
            "evidence_digest",
            "inserted_count",
            "updated_count",
            "metric_names_json",
            "created_at",
        },
        "adapter_schema_versions": {"test_id", "version", "applied_at"},
    }
    primary_keys = {
        "schema_migrations": ("version",),
        "test_results": ("result_id",),
        "metric_ingestion_receipts": ("run_id",),
        "adapter_schema_versions": ("test_id",),
    }
    column_specs = {
        "schema_migrations": {
            "version": ("INTEGER", False, None, 1),
            "name": ("TEXT", True, None, 0),
            "applied_at": ("INTEGER", True, None, 0),
        },
        "test_results": {
            "result_id": ("INTEGER", False, None, 1),
            "run_id": ("TEXT", True, None, 0),
            "test_id": ("TEXT", True, None, 0),
            "node": ("TEXT", True, None, 0),
                        "run_timestamp": ("INTEGER", True, None, 0),
            "started_timestamp": ("INTEGER", False, None, 0),
            "completed_timestamp": ("INTEGER", False, None, 0),
            "status": ("TEXT", True, None, 0),
            "exit_code": ("INTEGER", False, None, 0),
            "image_name": ("TEXT", True, "''", 0),
            "pytorch_version": ("TEXT", True, "''", 0),
            "cuda_version": ("TEXT", True, "''", 0),
            "test_config_digest": ("TEXT", True, "''", 0),
            "combination_key": ("TEXT", True, "''", 0),
            "result_path": ("TEXT", True, "''", 0),
            "summary_path": ("TEXT", True, "''", 0),
            "artifacts_path": ("TEXT", True, "''", 0),
            "raw_result_json": ("TEXT", True, None, 0),
            "result_digest": ("TEXT", True, None, 0),
            "health_class_name": ("TEXT", False, None, 0),
            "health_class_numerical": ("INTEGER", False, None, 0),
            "health_baseline_id": ("TEXT", False, None, 0),
            "evaluated_at": ("INTEGER", False, None, 0),
            "created_at": ("INTEGER", True, None, 0),
            "updated_at": ("INTEGER", True, None, 0),
        },
        "metric_ingestion_receipts": {
            "run_id": ("TEXT", False, None, 1),
            "test_id": ("TEXT", True, None, 0),
            "adapter_api_version": ("TEXT", True, None, 0),
            "evidence_digest": ("TEXT", True, None, 0),
            "inserted_count": ("INTEGER", True, None, 0),
            "updated_count": ("INTEGER", True, None, 0),
            "metric_names_json": ("TEXT", True, "'[]'", 0),
            "created_at": ("INTEGER", True, None, 0),
        },
        "adapter_schema_versions": {
            "test_id": ("TEXT", False, None, 1),
            "version": ("INTEGER", True, None, 0),
            "applied_at": ("INTEGER", True, None, 0),
        },
    }
    constraint_fragments = {
        "schema_migrations": (),
        "test_results": (
            "RUN_ID TEXT NOT NULL UNIQUE",
            "CHECK (RUN_TIMESTAMP >= 0)",
            "CHECK (STATUS IN ('pass', 'fail', 'incomplete'))",
            "CHECK (HEALTH_CLASS_NUMERICAL IS NULL OR HEALTH_CLASS_NUMERICAL BETWEEN 0 AND 5)",
            "CHECK (COMPLETED_TIMESTAMP IS NULL OR STARTED_TIMESTAMP IS NULL OR COMPLETED_TIMESTAMP >= STARTED_TIMESTAMP)",
            "CHECK ((HEALTH_CLASS_NAME IS NULL) = (HEALTH_CLASS_NUMERICAL IS NULL))",
            "CHECK (HEALTH_BASELINE_ID IS NULL OR HEALTH_CLASS_NUMERICAL IS NOT NULL)",
            "CHECK (EVALUATED_AT IS NULL OR HEALTH_CLASS_NUMERICAL IS NOT NULL)",
        ),
        "metric_ingestion_receipts": (
            "CHECK (INSERTED_COUNT >= 0)",
            "CHECK (UPDATED_COUNT >= 0)",
            "FOREIGN KEY (RUN_ID) REFERENCES TEST_RESULTS(RUN_ID) ON DELETE RESTRICT",
        ),
        "adapter_schema_versions": ("CHECK (VERSION > 0)",),
    }
    table_indexes = {
        "schema_migrations": set(),
        "test_results": {
            "idx_test_results_node_completed",
            "idx_test_results_combination_completed",
            "idx_test_results_unevaluated",
        },
        "metric_ingestion_receipts": set(),
        "adapter_schema_versions": set(),
    }
    implicit_indexes = {
        "schema_migrations": set(),
        "test_results": {("u", ("run_id",), ("BINARY",))},
        "metric_ingestion_receipts": {("pk", ("run_id",), ("BINARY",))},
        "adapter_schema_versions": {("pk", ("test_id",), ("BINARY",))},
    }
    constraint_counts = {
        "schema_migrations": (0, 0),
        "test_results": (7, 0),
        "metric_ingestion_receipts": (2, 1),
        "adapter_schema_versions": (1, 0),
    }
    for table_name, required_columns in manifests.items():
        validate_table_manifest(
            connection,
            table_name,
            required_columns=required_columns,
            column_specs=column_specs[table_name],
            primary_key=primary_keys[table_name],
            required_sql_fragments=constraint_fragments[table_name],
            constraint_counts=constraint_counts[table_name],
            allowed_indexes=table_indexes[table_name],
            implicit_indexes=implicit_indexes[table_name],
            immutable_key_groups=COMMON_IMMUTABLE_KEY_GROUPS[table_name],
        )
        require_exact_table_sql(
            connection,
            table_name,
            _common_table_sql_manifest()[table_name],
        )
    require_schema_objects(
        connection,
        indexes={
            "idx_test_results_node_completed": (
                "test_results",
                ("node", "completed_timestamp"),
                (False, True),
                ("BINARY", "BINARY"),
                False,
                "",
            ),
            "idx_test_results_combination_completed": (
                "test_results",
                ("combination_key", "completed_timestamp"),
                (False, True),
                ("BINARY", "BINARY"),
                False,
                "",
            ),
            "idx_test_results_unevaluated": (
                "test_results",
                ("combination_key", "completed_timestamp"),
                (False, False),
                ("BINARY", "BINARY"),
                False,
                "WHERE HEALTH_CLASS_NUMERICAL IS NULL",
            ),
        },
    )
    if not _has_unique_index(connection, "test_results", ("run_id",)):
        raise RuntimeError("Per-test test_results.run_id lacks a unique constraint")
    foreign_keys = {
        (
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]).upper(),
            str(row[6]).upper(),
            str(row[7]).upper(),
        )
        for row in connection.execute(
            "PRAGMA foreign_key_list(metric_ingestion_receipts)"
        )
    }
    if foreign_keys != {
        ("test_results", "run_id", "run_id", "NO ACTION", "RESTRICT", "NONE")
    }:
        raise RuntimeError("Metric receipt foreign key manifest is invalid")


def _has_unique_index(
    connection: sqlite3.Connection,
    table_name: str,
    columns: tuple[str, ...],
) -> bool:
    for row in connection.execute(f"PRAGMA index_list({table_name})"):
        if not int(row[2]):
            continue
        index_columns = tuple(
            str(item[2])
            for item in connection.execute(f"PRAGMA index_info({row[1]})")
        )
        if index_columns == columns:
            return True
    return False
