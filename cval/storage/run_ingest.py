"""Coordinate one validation run's authorized raw ingestion."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
from pathlib import Path
import runpy
import signal
import sys

from cval.config import load_config_snapshot
from cval.storage.dltest_ingest import ingest_dltest_run
from cval.storage.ingest import (
    add_nccl_health_from_summary,
    add_storage_result,
    add_validation_run_results,
)
from cval.storage.retry import load_retry_policy, retry_sqlite
from cval.storage.write_provenance import (
    ResultWriteAuthorization,
    authorize_result_write,
    bind_ingestion_digest,
)
from cval.validation.builtins import project_builtin_statuses
from cval.validation.execution import RunLogger, RunPaths
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
        "CVAL_STORAGE_DB_PATH": config.storage.storage_db_path,
        "CVAL_NCCL_DB_PATH": config.storage.nccl_db_path,
        "CVAL_DL_NUMERICAL_DB_PATH": config.storage.dl_numerical_db_path,
        "CVAL_DL_COMPUTE_DB_PATH": config.storage.dl_compute_db_path,
        "CVAL_DL_COLLECTIVE_DB_PATH": config.storage.dl_collective_db_path,
        "CVAL_DL_OVERLAP_DB_PATH": config.storage.dl_overlap_db_path,
        "CVAL_CONFIG_DIGEST": effective_config_digest(config),
    }
    nccl = config.tests.registry.get("nccl")
    if nccl is not None:
        expected["CVAL_NCCL_ITERATIONS"] = str(nccl.definition.settings["iterations"])
        expected["CVAL_IBBW_ENABLED"] = str(nccl.definition.settings["ibbw_enabled"]).lower()
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
    runtime_paths = {
        "CVAL_JOB_LOG_DIR": str(canonical_run),
        "CVAL_RESULT_JSON_FILE": str(canonical_run / "result.json"),
        "DLTEST_RUN_DIR": str(
            Path(config.runtime.validation_root) / "logs/validation_tests/dltest/runs"
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
    root = Path(config.runtime.validation_root)
    log_dir = authorization.result_path.parent
    paths = RunPaths(
        log_dir=log_dir, stdout=log_dir / "stdout.log", stderr=log_dir / "stderr.log",
        job_log=log_dir / "job.log", events=log_dir / "events.jsonl",
        result=log_dir / "result.json", legacy_env=log_dir / "result.env",
        marker=log_dir / ".run-active",
    )
    bind_ingestion_digest(authorization)
    policy = load_retry_policy()
    logger = RunLogger(paths, run_id, stdout=sys.stdout, stderr=sys.stderr, write_global_files=False)
    statuses = project_builtin_statuses(validation_result_to_env(result))
    logger.emit("ingestion_started", status="incomplete")
    try:
        if statuses["storage"] == "pass":
            test = result.tests["storage"]
            artifacts = (
                test.artifacts if isinstance(result, ValidationResultV2)
                else environment.get("CVAL_CANONICAL_STORAGE_OUTPUT_DIR")
                or environment.get("STORAGE_OUTPUT_DIR")
                or str(root / "logs/validation_tests/storage/runs" / result.node / run_id / "artifacts")
            )
            retry_sqlite(lambda: add_storage_result(
                result.node, result.timestamp, artifacts,
                image_name=result.image_name, immutable=True,
                db_path=config.storage.storage_db_path, _authorization=authorization,
            ), policy)
            print("Storage DB update completed.", flush=True)

        if statuses["nccl"] == "pass":
            test = result.tests["nccl"]
            run_dir = root / "logs/validation_tests/nccl/runs" / result.node / run_id
            summary = (
                test.summary if isinstance(result, ValidationResultV2)
                else environment.get("CVAL_CANONICAL_NCCL_SUMMARY_FILE")
                or environment.get("NCCL_SUMMARY_FILE") or str(run_dir / "summary.json")
            )
            if not Path(summary).is_file():
                raise ValueError(f"Passing NCCL result is missing required summary: {summary}")
            settings = config.tests.registry.require("nccl").definition.settings
            ibbw = None
            if settings["ibbw_enabled"]:
                ibbw = (
                    environment.get("CVAL_CANONICAL_NCCL_IBBW_LOG_FILE")
                    or environment.get("NCCL_IBBW_LOG_FILE")
                    or str(run_dir / "artifacts" / f"ibbw-{result.node}-{result.timestamp}.log")
                )
            retry_sqlite(lambda: add_nccl_health_from_summary(
                result.node, result.timestamp, summary,
                iterations=settings["iterations"], image_name=result.image_name,
                cuda_version=result.cuda_version, pytorch_version=result.pytorch_version,
                immutable=True, ibbw_log_path=ibbw, require_hca_samples=bool(settings["ibbw_enabled"]),
                db_path=config.storage.nccl_db_path, _authorization=authorization,
            ), policy)
            print("NCCL IB_HEALTH DB update completed.", flush=True)

        if statuses["dltest"] == "pass":
            run_dir = root / "logs/validation_tests/dltest/runs" / result.node / run_id
            helper = Path(__file__).resolve().parents[2] / "scripts/dl-metric-lock.py"
            lock = runpy.run_path(str(helper))["metric_lock"]
            lock_path = root / "metadata/.dl-metric-ingest.lock"
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with lock(lock_path) as verify_lock:
                def write_dl():
                    verify_lock()
                    summary = ingest_dltest_run(
                        run_dir, node=result.node, timestamp=result.timestamp,
                        config=config, _authorization=authorization,
                    )
                    verify_lock()
                    return summary

                summary = retry_sqlite(write_dl, policy)
            print(json.dumps(summary, indent=2), flush=True)
            print("DL metric DB update completed.", flush=True)

        retry_sqlite(lambda: add_validation_run_results(
            result.node, result.timestamp, statuses,
            image_name=result.image_name, pytorch_version=result.pytorch_version,
            cuda_version=result.cuda_version, db_path=config.storage.validation_db_path,
            _authorization=authorization,
        ), policy)
        print("Main DB update completed.", flush=True)
    except BaseException as error:
        try:
            logger.emit("ingestion_finished", status="fail", message=f"Raw ingestion failed: {error}")
        except Exception as event_error:
            print(f"Could not record ingestion failure: {event_error}", file=sys.stderr)
        raise
    logger.emit("ingestion_finished", status="pass")


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