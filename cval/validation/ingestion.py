"""Registry-driven persistence of one finalized ``cval.results.v2`` run."""

from __future__ import annotations

import json
import multiprocessing
import re
import sqlite3
import traceback
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from cval.config import CvalConfig, encode_config_snapshot, load_config
from cval.health.combination import resolve_environment_combination
from cval.storage.per_test_results import (
    PerTestResultRecord,
    framework_metric_ingestion_session,
    resolve_test_results_db_path,
    write_per_test_result,
    validate_common_only_result_database,
    validate_common_result_connection,
)
from cval.storage.paths import safe_writable_file_path
from cval.validation.plugins import (
    IngestionContext,
    IngestionDisabledError,
    IngestionReceipt,
    RunContext,
    TestExecutionResult,
    load_registered_plugin,
    validate_ingestion_artifact_tree,
    PluginLoadError,
)
from cval.validation.registry import (
    RegisteredValidationTest,
    validation_test_config_digest,
)
from cval.validation.results import (
    TERMINAL_V2_PHASES,
    TestResultV2,
    ValidationResultV2,
    load_validation_result,
    validation_result_v2_digest,
    validation_timestamp_to_epoch,
)
from cval.validation.runtime import effective_config_digest


@dataclass(frozen=True)
class TestIngestionOutcome:
    """One test's common-row and optional adapter result."""

    test_id: str
    status: str
    raw_result_inserted: bool
    adapter_called: bool
    receipt: IngestionReceipt | None = None
    error_type: str = ""
    error: str = ""


@dataclass(frozen=True)
class IngestionReport:
    """Complete isolated outcome set for one modular ingestion attempt."""

    run_id: str
    ok: bool
    outcomes: tuple[TestIngestionOutcome, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "ok": self.ok,
            "outcomes": [asdict(outcome) for outcome in self.outcomes],
        }


def ingest_test_results_file(
    result_path: str | Path,
    *,
    config: CvalConfig | None = None,
    result_digest: str,
    config_snapshot_b64: str,
) -> IngestionReport:
    """Validate and ingest every selected terminal test in one v2 result file."""

    active_config, result, prepared = _prepare_ingestion_contexts(
        result_path,
        config=config,
        result_digest=result_digest,
        config_snapshot_b64=config_snapshot_b64,
        require_write_enabled=True,
    )
    _preflight_write_targets(active_config, prepared)

    outcomes: list[TestIngestionOutcome] = []
    for registered_test, context in prepared:
        raw_inserted = False
        adapter_called = False
        try:
            combination = resolve_environment_combination(
                registered_test.definition,
                {
                    "image_name": context.run.image_name,
                    "cuda_version": context.run.cuda_version,
                    "pytorch_version": context.run.pytorch_version,
                },
            )
            raw_inserted = write_per_test_result(
                PerTestResultRecord(
                    run_id=context.run.run_id,
                    test_id=registered_test.id,
                    node=context.run.node,
                    run_timestamp=context.run.started_timestamp,
                    started_timestamp=context.execution.started_timestamp,
                    completed_timestamp=context.execution.completed_timestamp,
                    status=context.execution.status,
                    exit_code=context.execution.exit_code,
                    image_name=context.run.image_name,
                    pytorch_version=context.run.pytorch_version,
                    cuda_version=context.run.cuda_version,
                    test_config_digest=context.execution.config_digest,
                    combination_key=combination.key if combination is not None else "",
                    result_path=str(context.execution.result_path),
                    summary_path=str(context.execution.summary_path),
                    artifacts_path=str(context.execution.artifacts_path),
                    raw_result_json=context.execution.raw_result_json,
                    result_digest=context.run.result_digest,
                ),
                db_path=context.result_db_path,
            )
            declaration = registered_test.definition.plugin
            should_call_adapter = bool(
                context.execution.status == "pass"
                and declaration is not None
                and "ingest" in declaration.capabilities
            )
            if not should_call_adapter:
                if declaration is not None and "ingest" in declaration.capabilities:
                    _run_adapter_schema_preflight(
                        registered_test,
                        context.result_db_path,
                    )
                else:
                    validate_common_only_result_database(context.result_db_path)
                outcomes.append(
                    TestIngestionOutcome(
                        test_id=registered_test.id,
                        status=context.execution.status,
                        raw_result_inserted=raw_inserted,
                        adapter_called=False,
                    )
                )
                continue
            plugin = load_registered_plugin(registered_test)
            if plugin is None:
                raise RuntimeError(
                    f"Test {registered_test.id!r} declares ingest without an adapter"
                )
            adapter_called = True
            with framework_metric_ingestion_session(
                context.result_db_path
            ) as connection:
                receipt = _run_adapter_ingest_subprocess(
                    registered_test,
                    context,
                    connection,
                )
                _validate_adapter_receipt(
                    receipt,
                    registered_test=registered_test,
                    run_id=result.run_id,
                    db_path=context.result_db_path,
                    connection=connection,
                )
            outcomes.append(
                TestIngestionOutcome(
                    test_id=registered_test.id,
                    status=context.execution.status,
                    raw_result_inserted=raw_inserted,
                    adapter_called=True,
                    receipt=receipt,
                )
            )
        except Exception as exc:  # noqa: BLE001 - isolate one adapter/database
            outcomes.append(
                TestIngestionOutcome(
                    test_id=registered_test.id,
                    status=context.execution.status,
                    raw_result_inserted=raw_inserted,
                    adapter_called=adapter_called,
                    error_type=exc.__class__.__name__,
                    error=_first_line(str(exc)) or exc.__class__.__name__,
                )
            )
    return IngestionReport(
        run_id=result.run_id,
        ok=all(not outcome.error for outcome in outcomes),
        outcomes=tuple(outcomes),
    )


class _AdapterRpcConnection:
    """Child-process SQLite facade backed by parent-owned RPC commands."""

    def __init__(self, pipe) -> None:
        self._pipe = pipe

    @staticmethod
    def _reject(sql: str) -> None:
        statement = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
        if statement in {
            "ATTACH", "DETACH", "BEGIN", "COMMIT", "END", "ROLLBACK",
            "SAVEPOINT", "RELEASE", "VACUUM",
        }:
            raise sqlite3.DatabaseError(
                f"Adapter SQL operation is framework-owned: {statement}"
            )

    def execute(self, sql: str, parameters=()):
        self._reject(sql)
        self._pipe.send(("execute", sql, tuple(parameters)))
        response = self._pipe.recv()
        if response[0] == "error":
            raise sqlite3.DatabaseError(response[1])
        return _AdapterRpcCursor(response[1], response[2], response[3])

    def executemany(self, sql: str, parameters):
        self._reject(sql)
        self._pipe.send(("executemany", sql, list(parameters)))
        response = self._pipe.recv()
        if response[0] == "error":
            raise sqlite3.DatabaseError(response[1])
        return _AdapterRpcCursor(response[1], response[2], response[3])

    def commit(self):
        raise sqlite3.DatabaseError("Adapter commit is framework-owned")

    def rollback(self):
        raise sqlite3.DatabaseError("Adapter rollback is framework-owned")

    def set_authorizer(self, _callback):
        raise sqlite3.DatabaseError("Adapter authorizer is framework-owned")

    def executescript(self, _script):
        raise sqlite3.DatabaseError("Adapter scripts are not permitted")


class _AdapterRpcCursor:
    def __init__(self, rows, rowcount, lastrowid) -> None:
        self._rows = list(rows)
        self._index = 0
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def fetchone(self):
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self):
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


def _adapter_child(pipe, registered_test, context) -> None:
    import cval.storage.per_test_results as storage_module

    original_transaction = storage_module.metric_ingestion_transaction
    try:
        @contextmanager
        def rpc_transaction(db_path, **kwargs):
            path = storage_module.safe_writable_file_path(db_path)
            if path != context.result_db_path:
                raise RuntimeError(
                    "Adapter attempted to write outside its framework transaction"
                )
            connection = _AdapterRpcConnection(pipe)
            storage_module._assert_supported_schema(connection, allow_empty=False)
            storage_module._validate_schema_shape(connection)
            storage_module._validate_adapter_version_and_tables(
                connection,
                test_id=kwargs["test_id"],
                adapter_schema_version=kwargs["adapter_schema_version"],
                validate_adapter_schema=kwargs["validate_adapter_schema"],
            )
            yield connection

        storage_module.metric_ingestion_transaction = rpc_transaction
        plugin = load_registered_plugin(registered_test)
        if plugin is None:
            raise PluginLoadError(f"No adapter for {registered_test.id!r}")
        receipt = plugin.ingest(context)
        pipe.send(("receipt", receipt))
    except BaseException as exc:  # noqa: BLE001 - child isolation boundary
        pipe.send(("failure", exc.__class__.__name__, str(exc), traceback.format_exc()))
    finally:
        storage_module.metric_ingestion_transaction = original_transaction
        pipe.close()


def _adapter_schema_child(pipe, registered_test) -> None:
    """Validate an existing adapter schema without a parent connection object."""

    try:
        plugin = load_registered_plugin(registered_test)
        if plugin is None:
            raise PluginLoadError(f"No adapter for {registered_test.id!r}")
        result = plugin.validate_schema(_AdapterRpcConnection(pipe), True)
        pipe.send(("schema_result", result))
    except BaseException as exc:  # noqa: BLE001 - child isolation boundary
        pipe.send(("failure", exc.__class__.__name__, str(exc), traceback.format_exc()))
    finally:
        pipe.close()


def _run_adapter_ingest_subprocess(
    registered_test: RegisteredValidationTest,
    context: IngestionContext,
    connection,
) -> IngestionReceipt:
    process_context = multiprocessing.get_context("spawn")
    child_pipe, parent_pipe = process_context.Pipe(duplex=True)
    process = process_context.Process(
        target=_adapter_child,
        args=(child_pipe, registered_test, context),
    )
    process.start()
    child_pipe.close()
    try:
        return _serve_adapter_rpc(
            process,
            parent_pipe,
            connection,
            registered_test=registered_test,
            result_kind="receipt",
        )
    except EOFError as exc:
        process.join()
        raise RuntimeError(
            f"Adapter process exited without a receipt: {process.exitcode}"
        ) from exc
    finally:
        parent_pipe.close()
        if process.is_alive():
            process.terminate()
            process.join()


def _run_adapter_schema_preflight(
    registered_test: RegisteredValidationTest,
    db_path: Path,
) -> bool:
    """Run one adapter's existing-schema validator behind read-only SQL RPC."""

    process_context = multiprocessing.get_context("spawn")
    child_pipe, parent_pipe = process_context.Pipe(duplex=True)
    process = process_context.Process(
        target=_adapter_schema_child,
        args=(child_pipe, registered_test),
    )
    process.start()
    child_pipe.close()
    try:
        with closing(
            sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
        ) as connection:
            result = _serve_adapter_rpc(
                process,
                parent_pipe,
                connection,
                registered_test=registered_test,
                result_kind="schema_result",
            )
        if not isinstance(result, bool):
            raise TypeError(
                f"Adapter {registered_test.id!r} validate_schema must return bool"
            )
        return result
    except EOFError as exc:
        process.join()
        raise RuntimeError(
            f"Adapter schema process exited without a result: {process.exitcode}"
        ) from exc
    finally:
        parent_pipe.close()
        if process.is_alive():
            process.terminate()
            process.join()


def _serve_adapter_rpc(
    process,
    parent_pipe,
    connection,
    *,
    registered_test: RegisteredValidationTest,
    result_kind: str,
):
    """Serve SQL requests until an isolated adapter returns its final value."""

    while True:
        message = parent_pipe.recv()
        kind = message[0]
        if kind in {"execute", "executemany"}:
            try:
                cursor = (
                    connection.execute(message[1], message[2])
                    if kind == "execute"
                    else connection.executemany(message[1], message[2])
                )
                parent_pipe.send(
                    ("ok", cursor.fetchall(), cursor.rowcount, cursor.lastrowid)
                )
            except Exception as exc:  # noqa: BLE001 - RPC serialization
                parent_pipe.send(("error", str(exc)))
        elif kind == result_kind:
            result = message[1]
            process.join()
            if process.exitcode != 0:
                raise RuntimeError(
                    f"Adapter process exited with code {process.exitcode}"
                )
            return result
        elif kind == "failure":
            process.join()
            raise RuntimeError(
                f"Adapter {registered_test.id!r} failed ({message[1]}): {message[2]}"
            )
        else:
            process.join()
            raise RuntimeError(
                f"Adapter {registered_test.id!r} sent unknown RPC message {kind!r}"
            )


def preflight_test_results_file(
    result_path: str | Path,
    *,
    config: CvalConfig | None = None,
    result_digest: str,
    config_snapshot_b64: str,
) -> str:
    """Validate all paths and write targets without creating or mutating anything."""

    active_config, result, prepared = _prepare_ingestion_contexts(
        result_path,
        config=config,
        result_digest=result_digest,
        config_snapshot_b64=config_snapshot_b64,
        require_write_enabled=False,
    )
    _preflight_write_targets(active_config, prepared)
    return result.run_id


def _prepare_ingestion_contexts(
    result_path: str | Path,
    *,
    config: CvalConfig | None,
    result_digest: str,
    config_snapshot_b64: str,
    require_write_enabled: bool,
) -> tuple[
    CvalConfig,
    ValidationResultV2,
    list[tuple[RegisteredValidationTest, IngestionContext]],
]:
    active_config = config or load_config()
    if not config_snapshot_b64:
        raise ValueError("An immutable effective configuration snapshot is required")
    if config_snapshot_b64 != encode_config_snapshot(active_config):
        raise ValueError("Effective configuration does not match its immutable snapshot")
    if require_write_enabled and not active_config.storage.per_test_ingestion_enabled:
        raise IngestionDisabledError(
            "Modular per-test ingestion is disabled by storage.per_test_ingestion_enabled"
        )
    path = Path(result_path).expanduser()
    result = load_validation_result(path)
    if not isinstance(result, ValidationResultV2):
        raise ValueError("Modular per-test ingestion accepts only cval.results.v2")
    if result.completed_at is None:
        raise ValueError("Modular per-test ingestion requires a completed v2 run")
    actual_result_digest = validation_result_v2_digest(result)
    if not result_digest or result_digest != actual_result_digest:
        raise ValueError("Validated v2 result does not match its immutable digest")

    validation_root = Path(active_config.runtime.validation_root).expanduser()
    if not validation_root.is_absolute():
        raise ValueError("runtime.validation_root must be an absolute path")
    expected_result_path = (
        validation_root
        / "logs"
        / "job_logs"
        / result.node
        / result.run_id
        / "result.json"
    )
    global_result_path = _require_exact_evidence_path(
        path,
        expected_result_path,
        validation_root=validation_root,
        description="global result",
        require_file=True,
    )
    if result.global_config_digest != effective_config_digest(active_config):
        raise ValueError("Result global_config_digest does not match effective config")

    registry = active_config.tests.registry
    if set(result.tests) != {test.id for test in registry.tests}:
        raise ValueError("Result registered test set does not match effective registry")

    prepared: list[tuple[RegisteredValidationTest, IngestionContext]] = []
    run_context = RunContext(
        run_id=result.run_id,
        node=result.node,
        started_timestamp=result.timestamp,
        started_timestamp_la=result.timestamp_la,
        completed_timestamp=validation_timestamp_to_epoch(result.completed_at),
        image_name=result.image_name,
        pytorch_version=result.pytorch_version,
        cuda_version=result.cuda_version,
        git_ref=result.git_ref,
        global_config_digest=result.global_config_digest,
        result_digest=result_digest,
        validation_root=validation_root,
        result_path=global_result_path,
    )
    for registered_test in registry.tests:
        test = result.tests[registered_test.id]
        _validate_result_registration(registered_test, test)
        if not test.selected:
            continue
        if test.phase not in TERMINAL_V2_PHASES:
            raise ValueError(
                f"Selected test {registered_test.id!r} is not in a terminal phase"
            )
        execution = _prepare_execution_context(
            result,
            registered_test,
            test,
            validation_root=validation_root,
        )
        result_db_path = resolve_test_results_db_path(
            validation_root,
            registered_test,
        )
        prepared.append(
            (
                registered_test,
                IngestionContext(
                    definition=registered_test.definition,
                    run=run_context,
                    execution=execution,
                    result_db_path=result_db_path,
                ),
            )
        )

    prepared = sorted(
        prepared,
        key=lambda item: (item[0].definition.metadata.order, item[0].id),
    )
    return active_config, result, prepared


def _validate_result_registration(
    registered_test: RegisteredValidationTest,
    test: TestResultV2,
) -> None:
    expected_digest = validation_test_config_digest(registered_test)
    expected = (
        registered_test.definition.metadata.display_name,
        registered_test.enabled,
        registered_test.enabled,
        registered_test.definition.metadata.order,
        registered_test.config_path,
        expected_digest,
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
            f"Result metadata for test {registered_test.id!r} does not match registry"
        )


def _preflight_write_targets(
    config: CvalConfig,
    prepared: list[tuple[RegisteredValidationTest, IngestionContext]],
) -> None:
    """Reject unsafe evidence trees and DB targets without creating anything."""

    targets = [
        config.storage.validation_db_path,
        config.storage.storage_db_path,
        config.storage.nccl_db_path,
        config.storage.dl_numerical_db_path,
        config.storage.dl_compute_db_path,
        config.storage.dl_collective_db_path,
        config.storage.dl_overlap_db_path,
        config.storage.run_history_db_path,
    ]
    for target in targets:
        safe_writable_file_path(target)
    validation_root = Path(config.runtime.validation_root)
    for registered_test in config.tests.registry.tests:
        target = resolve_test_results_db_path(validation_root, registered_test)
        safe_writable_file_path(
            target,
            allowed_root=(
                validation_root / "validation_tests" / registered_test.id
            ),
            description=f"{registered_test.id} result database",
        )
    for registered_test, context in prepared:
        if context.result_db_path.is_file():
            with closing(
                sqlite3.connect(
                    f"file:{context.result_db_path}?mode=ro",
                    uri=True,
                    timeout=30,
                )
            ) as connection:
                validate_common_result_connection(connection)
            declaration = registered_test.definition.plugin
            if declaration is not None and "ingest" in declaration.capabilities:
                _run_adapter_schema_preflight(
                    registered_test,
                    context.result_db_path,
                )
            else:
                validate_common_only_result_database(context.result_db_path)
        validate_ingestion_artifact_tree(context.execution.artifacts_path)


def _validate_adapter_receipt(
    receipt: object,
    *,
    registered_test: RegisteredValidationTest,
    run_id: str,
    db_path: Path,
    connection: sqlite3.Connection | None = None,
) -> None:
    if not isinstance(receipt, IngestionReceipt):
        raise TypeError(
            f"Adapter {registered_test.id!r} must return IngestionReceipt"
        )
    if (receipt.test_id, receipt.run_id) != (registered_test.id, run_id):
        raise ValueError(
            f"Adapter {registered_test.id!r} returned a mismatched receipt"
        )
    for field_name, value in (
        ("inserted_count", receipt.inserted_count),
        ("updated_count", receipt.updated_count),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"Adapter receipt {field_name} must be non-negative integer")
    if not isinstance(receipt.metric_names, tuple) or not all(
        isinstance(name, str) and name.strip() for name in receipt.metric_names
    ):
        raise ValueError("Adapter receipt metric_names must be non-empty strings")
    if len(set(receipt.metric_names)) != len(receipt.metric_names):
        raise ValueError("Adapter receipt metric_names must be unique")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", receipt.evidence_digest):
        raise ValueError("Adapter receipt evidence_digest must be a SHA-256 digest")
    if isinstance(receipt.created_at, bool) or not isinstance(receipt.created_at, int):
        raise ValueError("Adapter receipt created_at must be an integer")

    path = safe_writable_file_path(db_path)
    if not path.is_file():
        raise RuntimeError("Adapter returned success without a result database")
    def load_row(active_connection: sqlite3.Connection):
        return active_connection.execute(
            "SELECT test_id, adapter_api_version, evidence_digest, inserted_count, "
            "updated_count, metric_names_json, created_at "
            "FROM metric_ingestion_receipts WHERE run_id=?",
            (run_id,),
        ).fetchone()
    if connection is None:
        with closing(
            sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
        ) as read_connection:
            row = load_row(read_connection)
    else:
        row = load_row(connection)
    if row is None:
        raise RuntimeError("Adapter returned success without a durable metric receipt")
    try:
        durable_names = json.loads(str(row[5]))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Durable metric receipt contains invalid JSON") from exc
    if (
        str(row[0]) != receipt.test_id
        or str(row[1]) != "cval.plugin.v1"
        or str(row[2]) != receipt.evidence_digest
        or not isinstance(durable_names, list)
        or tuple(durable_names) != tuple(sorted(receipt.metric_names))
        or int(row[6]) != receipt.created_at
    ):
        raise RuntimeError("Durable metric receipt does not match adapter response")
    if (int(row[3]), int(row[4])) != (
        receipt.inserted_count,
        receipt.updated_count,
    ):
        raise RuntimeError("Durable metric receipt counts do not match adapter response")


def _prepare_execution_context(
    result: ValidationResultV2,
    registered_test: RegisteredValidationTest,
    test: TestResultV2,
    *,
    validation_root: Path,
) -> TestExecutionResult:
    test_id = registered_test.id
    run_dir = (
        validation_root
        / "validation_tests"
        / test_id
        / "runs"
        / result.node
        / result.run_id
    )
    log_dir = validation_root / "logs" / test_id / result.node / result.run_id
    result_path = _require_exact_evidence_path(
        test.result,
        run_dir / "result.json",
        validation_root=validation_root,
        description=f"{test_id} result",
        require_file=True,
    )
    summary_path = _require_exact_evidence_path(
        test.summary,
        run_dir / registered_test.definition.artifacts.summary_filename,
        validation_root=validation_root,
        description=f"{test_id} summary",
    )
    artifacts_path = _require_exact_evidence_path(
        test.artifacts,
        run_dir / "artifacts",
        validation_root=validation_root,
        description=f"{test_id} artifacts",
        require_directory=True,
    )
    stdout_path = _require_exact_evidence_path(
        test.stdout,
        log_dir / "stdout.log",
        validation_root=validation_root,
        description=f"{test_id} stdout",
        require_file=True,
    )
    stderr_path = _require_exact_evidence_path(
        test.stderr,
        log_dir / "stderr.log",
        validation_root=validation_root,
        description=f"{test_id} stderr",
        require_file=True,
    )
    log_path = _require_exact_evidence_path(
        test.log,
        log_dir / "events.jsonl",
        validation_root=validation_root,
        description=f"{test_id} events",
        require_file=True,
    )
    raw_payload = _load_test_result_payload(result_path)
    expected_payload = {
        "schema_version": "cval.test-result.v1",
        "test_id": test_id,
        "status": test.status,
        "phase": test.phase,
        "started_at": test.started_at,
        "completed_at": test.completed_at,
        "duration_ms": test.duration_ms,
        "exit_code": test.exit_code,
        "summary": test.summary,
        "artifacts": test.artifacts,
        "message": test.message,
    }
    if raw_payload != expected_payload:
        raise ValueError(
            f"Per-test result for {test_id!r} does not match the v2 envelope"
        )
    return TestExecutionResult(
        test_id=test_id,
        status=test.status,
        phase=test.phase,
        started_timestamp=validation_timestamp_to_epoch(test.started_at),
        completed_timestamp=validation_timestamp_to_epoch(test.completed_at),
        duration_ms=test.duration_ms,
        exit_code=test.exit_code,
        result_path=result_path,
        summary_path=summary_path,
        artifacts_path=artifacts_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        log_path=log_path,
        message=test.message,
        config_digest=test.config_digest,
        raw_result_json=json.dumps(
            raw_payload,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def _load_test_result_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid per-test result JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Per-test result must be an object: {path}")
    return payload


def _require_exact_evidence_path(
    value: str | Path,
    expected: Path,
    *,
    validation_root: Path,
    description: str,
    require_file: bool = False,
    require_directory: bool = False,
) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        raise ValueError(f"{description} path must be absolute")
    if any(part in {"", ".", ".."} for part in candidate.parts[1:]):
        raise ValueError(f"{description} path contains an invalid segment")
    if candidate != expected:
        raise ValueError(f"{description} path is not the canonical run path")
    resolved_root = validation_root.expanduser().resolve()
    resolved_candidate = candidate.resolve()
    resolved_expected = expected.resolve()
    if resolved_candidate != resolved_expected:
        raise ValueError(f"{description} path resolves away from the canonical run path")
    try:
        resolved_candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{description} path escapes validation root") from exc
    current = validation_root
    for part in candidate.relative_to(validation_root).parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{description} path contains a symlink: {current}")
    if require_file and not candidate.is_file():
        raise FileNotFoundError(f"{description} file not found: {candidate}")
    if require_directory and not candidate.is_dir():
        raise FileNotFoundError(f"{description} directory not found: {candidate}")
    return resolved_candidate


def _first_line(value: str) -> str:
    return value.strip().splitlines()[0] if value.strip() else ""
