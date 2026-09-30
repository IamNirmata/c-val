"""Coordinate one validation run's authorized raw ingestion."""

from __future__ import annotations

from collections.abc import Mapping
import os
from pathlib import Path
import signal
import sys
import time
from types import MappingProxyType

from cval.config import load_config_snapshot
from cval.storage.ingest import add_validation_run_results
from cval.storage.retry import retry_sqlite
from cval.storage.write_provenance import (
    ResultWriteAuthorization,
    authorize_result_write,
)
from cval.validation.builtins import project_builtin_statuses
from cval.validation.execution import RunLogger, RunPaths
from cval.validation.plugins import IngestContext, load_registered_plugin
from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    validation_result_digest,
    validation_result_to_env,
)
from cval.validation.runtime import effective_config_digest


def authorize_run(environment: Mapping[str, str]) -> ResultWriteAuthorization:
    """Bind runtime identity and destinations to the immutable result snapshot."""

    result_path = environment.get("CVAL_RESULT_JSON_FILE")
    if not result_path:
        raise ValueError("CVAL_RESULT_JSON_FILE is required for raw ingestion")
    try:
        result = load_validation_result(Path(result_path))
    except (OSError, ValueError) as error:
        raise ValueError(f"Structured result validation failed: {error}") from error
    run_id = getattr(result, "run_id", f"{result.node}-{result.timestamp}")
    for label, actual, expected in (
        ("node", environment.get("GCRNODE"), result.node),
        ("timestamp", environment.get("GCRTIME"), str(result.timestamp)),
        ("run_id", environment.get("CVAL_RUN_ID"), run_id),
    ):
        if actual != expected:
            raise ValueError(f"Result {label} mismatch: expected {actual}, got {expected}")

    snapshot = environment.get("CVAL_CONFIG_SNAPSHOT_B64", "")
    if not snapshot:
        raise ValueError("CVAL_CONFIG_SNAPSHOT_B64 is required for raw ingestion")
    repo_root = environment.get("CVAL_TEST_REPO_ROOT") or environment.get("CVAL_REPO_DIR")
    config = load_config_snapshot(snapshot, repo_root=Path(repo_root) if repo_root else None)
    expected = {
        "CVAL_VALIDATION_ROOT": config.runtime.validation_root,
        "CVAL_VALIDATION_DB_PATH": config.storage.validation_db_path,
        "CVAL_CONFIG_DIGEST": effective_config_digest(config),
    }
    for registered in config.tests.registry.tests:
        declaration = registered.definition.plugin
        if declaration is not None and "runtime" in declaration.capabilities:
            expected.update(load_registered_plugin(registered).runtime_environment(registered, config))
    for name, value in expected.items():
        if environment.get(name) != str(value):
            raise ValueError(f"Runtime value {name} does not match the effective configuration snapshot")
    lock_path = f"{config.runtime.validation_root.rstrip('/')}/metadata/.dl-metric-ingest.lock"
    if environment.get("CVAL_DL_METRIC_LOCK_FILE", lock_path) != lock_path:
        raise ValueError("DL metric lock path does not match the effective configuration snapshot")
    authorization = authorize_result_write(
        result_path,
        result_digest=validation_result_digest(result),
        config_snapshot_b64=snapshot,
        config=config,
    )
    canonical_run = Path(config.runtime.validation_root) / "logs/job_logs" / result.node / run_id
    dl_root = "logs/dltest" if result.schema_version == "cval.results.v3" else "logs/validation_tests/dltest/runs"
    runtime_paths = {
        "CVAL_JOB_LOG_DIR": str(canonical_run),
        "CVAL_RESULT_JSON_FILE": str(canonical_run / "result.json"),
        "DLTEST_RUN_DIR": str(
            Path(config.runtime.validation_root) / dl_root
            / result.node / run_id
        ),
    }
    if isinstance(result, ValidationResultV2):
        if "storage" in result.tests:
            runtime_paths["STORAGE_OUTPUT_DIR"] = result.tests["storage"].artifacts
        if "nccl" in result.tests:
            runtime_paths["NCCL_SUMMARY_FILE"] = result.tests["nccl"].summary
    for name, path in runtime_paths.items():
        canonical_name = "CVAL_CANONICAL_" + name.removeprefix("CVAL_")
        actual = environment.get(canonical_name, environment.get(name, path))
        if actual != path:
            raise ValueError(f"{name} does not match the canonical run evidence")
    return authorization


def ingest_run(environment: Mapping[str, str] | None = None) -> None:
    """Write test-owned metrics, then final status, for one authorized result."""

    environment = os.environ if environment is None else environment
    authorization = authorize_run(environment)
    result = authorization.result
    config = authorization.config
    run_id = getattr(result, "run_id", f"{result.node}-{result.timestamp}")
    log_dir = Path(environment.get("CVAL_JOB_LOG_DIR", str(authorization.result_path.parent)))
    paths = RunPaths(
        log_dir=log_dir, job_log=log_dir / "job.log",
        result=authorization.result_path,
        marker=log_dir / ".run-active",
    )
    policy = config.sqlite_retry
    logger = RunLogger(paths, run_id, stdout=sys.stdout, stderr=sys.stderr, write_global_files=False)
    statuses = project_builtin_statuses(validation_result_to_env(result))
    identity = {"producer": "ingestion", "node_name": result.node,
                "result_digest": authorization.result_digest, "config_digest": effective_config_digest(config)}
    dl_paths = {
        "numerical_correctness": config.storage.dl_numerical_db_path,
        "compute_performance": config.storage.dl_compute_db_path,
        "collective_performance": config.storage.dl_collective_db_path,
        "overlap_performance": config.storage.dl_overlap_db_path,
    }
    destinations = {"storage": config.storage.storage_db_path, "nccl": config.storage.nccl_db_path,
                    **dl_paths, "validation": config.storage.validation_db_path}
    stores = {name: "pending" if name == "validation" or statuses["dltest" if name in dl_paths else name] == "pass"
              else "not_required" for name in destinations}
    context = IngestContext(authorization, MappingProxyType(dict(environment)))
    writers = []
    claimed = set()
    for registered in config.tests.registry.tests:
        declaration = registered.definition.plugin
        if declaration is None or "ingest" not in declaration.capabilities:
            continue
        plugin = load_registered_plugin(registered)
        owned = plugin.raw_stores(config)
        if not isinstance(owned, dict) or any(
            name not in destinations or name == "validation" or name in claimed or destinations[name] != path
            for name, path in owned.items()
        ):
            raise ValueError(f"Invalid raw store ownership for {registered.id}")
        claimed.update(owned)
        test = result.tests.get(registered.id)
        if test is not None and test.enabled and getattr(test, "selected", True) and test.status == "pass":
            if any(stores[name] != "pending" for name in owned):
                raise ValueError(f"Raw stores do not match passing test {registered.id}")
            writers.append((registered.id, plugin, tuple(owned)))
    required_owners = {name for _test, _plugin, owned in writers for name in owned}
    if set(name for name, state in stores.items() if state == "pending") - required_owners - {"validation"}:
        raise ValueError("Passing result has raw stores without a registered ingestion owner")
    started = time.monotonic()
    current_stores: tuple[str, ...] = ()

    def store_finished(name: str, **details) -> None:
        logger.emit("ingestion_store_finished", status="pass" if stores[name] == "committed" else "incomplete",
                    store=name, outcome=stores[name], db_path=destinations[name], **identity, **details)

    logger.emit("ingestion_started", status="incomplete", stores=stores, **identity)
    try:
        for name, outcome in stores.items():
            if outcome == "not_required":
                store_finished(name)
        for test_id, plugin, current_stores in writers:
            receipts = plugin.ingest(context)
            if not isinstance(receipts, dict) or set(receipts) != set(current_stores):
                raise ValueError(f"Incomplete raw ingestion receipts for {test_id}")
            for name, receipt in receipts.items():
                if (not isinstance(receipt, dict) or set(receipt) - {"rows", "ranks", "generation_id"}
                        or type(receipt.get("rows")) is not int or receipt["rows"] < 0
                        or ("ranks" in receipt and (type(receipt["ranks"]) is not int or receipt["ranks"] <= 0))
                        or ("generation_id" in receipt and (not isinstance(receipt["generation_id"], str) or not receipt["generation_id"]))):
                    raise ValueError(f"Invalid raw ingestion receipt for {test_id}/{name}")
            for name in current_stores:
                stores[name] = "committed"
                store_finished(name, **receipts[name])

        current_stores = ("validation",)
        retry_sqlite(lambda: add_validation_run_results(
            result.node, result.timestamp, statuses,
            image_name=result.image_name, pytorch_version=result.pytorch_version,
            cuda_version=result.cuda_version, db_path=config.storage.validation_db_path,
            _authorization=authorization,
        ), policy)
        stores["validation"] = "committed"
        store_finished("validation", rows=len(statuses))
        print("Main DB update completed.", flush=True)
    except BaseException as error:
        for name in current_stores:
            if stores[name] == "pending":
                stores[name] = "unconfirmed"
        try:
            logger.emit("ingestion_finished", status="fail", message=f"Raw ingestion failed: {error}",
                        stores=stores, current_stores=list(current_stores),
                        duration_ms=max(0, int((time.monotonic() - started) * 1000)), **identity)
        except Exception as event_error:
            print(f"Could not record ingestion failure: {event_error}", file=sys.stderr)
        raise
    logger.emit("ingestion_finished", status="pass", stores=stores,
                duration_ms=max(0, int((time.monotonic() - started) * 1000)), **identity)


def main() -> int:
    def terminate(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    previous = {
        signum: signal.signal(signum, terminate)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        ingest_run()
        return 0
    except Exception as error:
        print(f"Raw ingestion failed: {error}", file=sys.stderr)
        return 1
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())