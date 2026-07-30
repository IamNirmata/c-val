"""Generic in-pod runner for registered repository-local validation tests."""

from __future__ import annotations

import importlib
import json
import os
import re
import signal
import sys
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, TextIO
from zoneinfo import ZoneInfo

from cval.config import CvalConfig, REPO_ROOT, load_config, load_config_snapshot
from cval.validation.execution import RunLogger, RunPaths, TestPaths, utc_now
from cval.validation.registry import (
    RegisteredValidationTest,
    ValidationTestRegistry,
    load_test_registry,
    validation_test_config_digest,
)
from cval.validation.results import parse_validation_result_v2
from cval.validation.runtime import effective_config_digest


LOS_ANGELES = ZoneInfo("America/Los_Angeles")
TEST_RESULT_SCHEMA_VERSION = "cval.test-result.v1"
DIGEST_PREFIX = "sha256:"
PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
LEGACY_ENABLE_ENV = {
    "storage": "RUN_STORAGE",
    "nccl": "RUN_NCCL",
    "dltest": "RUN_DLTEST",
}
LEGACY_RESULT_ENV = {
    "storage": "GCRRESULT1",
    "nccl": "GCRRESULT2",
    "dltest": "GCRRESULT3",
}


def run_validation_tests(
    *,
    config: CvalConfig | None = None,
    registry: ValidationTestRegistry | None = None,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> dict[str, Any]:
    """Run every enabled test and return validated ``cval.results.v2`` state."""

    runtime_env = dict(os.environ if environ is None else environ)
    config = config or _load_runtime_config(runtime_env)
    registry = registry or _load_runtime_registry(config, runtime_env)
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr

    node = _safe_segment(
        runtime_env.get("CVAL_NODE") or runtime_env.get("GCRNODE") or "unknown",
        "node",
    )
    timestamp = _runtime_timestamp(
        runtime_env.get("CVAL_TIMESTAMP") or runtime_env.get("GCRTIME")
    )
    run_id = _safe_segment(
        runtime_env.get("CVAL_RUN_ID") or f"{node}-{timestamp}",
        "run_id",
    )
    validation_root = Path(
        runtime_env.get("CVAL_VALIDATION_ROOT", config.runtime.validation_root)
    ).expanduser()
    if not validation_root.is_absolute():
        raise ValueError("CVAL_VALIDATION_ROOT must be an absolute path")

    external_logging = _is_enabled(
        runtime_env.get("CVAL_EXTERNAL_GLOBAL_LOGGING", "false")
    )
    marker_preacquired = _is_enabled(
        runtime_env.get("CVAL_RUN_MARKER_PREACQUIRED", "false")
    )
    run_paths = _run_paths(validation_root, node, run_id)
    _preflight_registry_paths(
        validation_root,
        node,
        run_id,
        registry,
    )
    runtime_env.update(
        {
            "CVAL_JOB_LOG_DIR": str(run_paths.log_dir),
            "CVAL_RESULT_JSON_FILE": str(run_paths.result),
            "CVAL_RESULT_ENV_FILE": str(run_paths.legacy_env),
        }
    )
    _prepare_run_paths(
        run_paths,
        allow_external_logs=external_logging,
        marker_preacquired=marker_preacquired,
    )
    logger = RunLogger(
        run_paths,
        run_id,
        stdout=stdout,
        stderr=stderr,
        write_global_files=not external_logging,
    )
    result = _initial_result(
        config=config,
        registry=registry,
        runtime_env=runtime_env,
        node=node,
        timestamp=timestamp,
        run_id=run_id,
        validation_root=validation_root,
    )
    _write_state(result, run_paths)
    logger.emit(
        "run_started",
        status="incomplete",
        tests=[test.id for test in registry.enabled],
    )

    previous_handlers = _install_signal_handlers(logger)
    try:
        _execute_registry(
            registry,
            validation_root=validation_root,
            node=node,
            timestamp=timestamp,
            run_id=run_id,
            result=result,
            run_paths=run_paths,
            logger=logger,
            runtime_env=runtime_env,
        )
    except KeyboardInterrupt:
        logger.emit(
            "run_interrupted",
            status="incomplete",
            message="received termination signal",
        )
        _write_state(result, run_paths)
        raise
    finally:
        _restore_signal_handlers(previous_handlers)

    if result["completed_at"] is None:
        result["completed_at"] = utc_now()
    _write_state(result, run_paths)
    logger.emit(
        "run_finished",
        status=result["overall"],
        overall=result["overall"],
    )
    logger.message(_legacy_final_line(result))
    _write_state(result, run_paths)
    if not marker_preacquired:
        run_paths.marker.unlink()
    return result


def main() -> int:
    """Run the in-pod workflow and return an infrastructure exit code."""

    try:
        run_validation_tests()
    except KeyboardInterrupt:
        print("c-val generic runner interrupted", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 - top-level in-pod error boundary
        print(f"c-val generic runner failed: {exc}", file=sys.stderr)
        return 1
    return 0


def _execute_registry(
    registry: ValidationTestRegistry,
    *,
    validation_root: Path,
    node: str,
    timestamp: int,
    run_id: str,
    result: dict[str, Any],
    run_paths: RunPaths,
    logger: RunLogger,
    runtime_env: dict[str, str],
) -> None:
    for registered_test in registry.tests:
        if not registered_test.enabled:
            logger.emit(
                "test_skipped",
                test_id=registered_test.id,
                status="incomplete",
                message="disabled by config",
            )
            compatibility_skip = {
                "storage": "Storage test SKIPPED (disabled by config).",
                "nccl": "NCCL test SKIPPED (disabled by config).",
                "dltest": "DL Test SKIPPED (disabled by config).",
            }.get(registered_test.id)
            if compatibility_skip:
                logger.message(compatibility_skip)
            continue
        test_paths = _test_paths(validation_root, node, run_id, registered_test)
        _prepare_test_paths(test_paths)
        _run_one_test(
            registered_test,
            test_paths=test_paths,
            result=result,
            run_paths=run_paths,
            logger=logger,
            runtime_env=runtime_env,
            validation_root=validation_root,
            node=node,
            timestamp=timestamp,
            run_id=run_id,
        )


def _install_signal_handlers(
    logger: RunLogger,
) -> dict[signal.Signals, Any]:
    """Install main-thread handlers that persist interrupted test state."""

    if threading.current_thread() is not threading.main_thread():
        return {}
    previous: dict[signal.Signals, Any] = {}

    def handle_signal(_signum: int, _frame: Any) -> None:
        logger.terminate_active()
        raise KeyboardInterrupt

    for signal_name in (signal.SIGTERM, signal.SIGINT):
        previous[signal_name] = signal.getsignal(signal_name)
        signal.signal(signal_name, handle_signal)
    return previous


def _restore_signal_handlers(previous: dict[signal.Signals, Any]) -> None:
    for signal_name, handler in previous.items():
        signal.signal(signal_name, handler)


def _run_one_test(
    registered_test: RegisteredValidationTest,
    *,
    test_paths: TestPaths,
    result: dict[str, Any],
    run_paths: RunPaths,
    logger: RunLogger,
    runtime_env: dict[str, str],
    validation_root: Path,
    node: str,
    timestamp: int,
    run_id: str,
) -> None:
    test_id = registered_test.id
    state = result["tests"][test_id]
    started_clock = time.monotonic()
    state.update(
        {
            "phase": "setup",
            "started_at": utc_now(),
            "stdout": str(test_paths.stdout),
            "stderr": str(test_paths.stderr),
            "log": str(test_paths.events),
            "summary": str(test_paths.summary),
            "result": str(test_paths.result),
            "artifacts": str(test_paths.artifacts),
        }
    )
    _write_state(result, run_paths)
    logger.emit(
        "test_setup_started",
        test_id=test_id,
        test_paths=test_paths,
        status="incomplete",
    )

    environment = _test_environment(
        runtime_env,
        registered_test=registered_test,
        test_paths=test_paths,
        validation_root=validation_root,
        node=node,
        timestamp=timestamp,
        run_id=run_id,
    )
    timeout_seconds = float(registered_test.definition.metadata.timeout_seconds)
    deadline = time.monotonic() + timeout_seconds
    try:
        setup_path = registered_test.test_dir / registered_test.definition.metadata.setup
        setup = logger.stream_process(
            ["bash", str(setup_path)],
            cwd=registered_test.test_dir,
            environment=environment,
            timeout_seconds=max(0.001, deadline - time.monotonic()),
            test_paths=test_paths,
            label=f"{test_id}:setup",
        )
        if setup.timed_out:
            _finish_test(
                registered_test,
                test_paths=test_paths,
                result=result,
                run_paths=run_paths,
                logger=logger,
                phase="timed_out",
                status="fail",
                exit_code=setup.exit_code,
                started_clock=started_clock,
                message=setup.message,
                event="test_timed_out",
            )
            return
        if setup.exit_code != 0:
            _finish_test(
                registered_test,
                test_paths=test_paths,
                result=result,
                run_paths=run_paths,
                logger=logger,
                phase="setup_failed",
                status="fail",
                exit_code=setup.exit_code,
                started_clock=started_clock,
                message=f"setup exited with code {setup.exit_code}",
                event="test_finished",
            )
            return

        state["phase"] = "running"
        _write_state(result, run_paths)
        logger.emit(
            "test_started",
            test_id=test_id,
            test_paths=test_paths,
            status="incomplete",
        )
        if test_id == "dltest":
            logger.message("Running DL Test...", test_paths=test_paths)
        entrypoint = (
            registered_test.test_dir / registered_test.definition.metadata.entrypoint
        )
        workload = logger.stream_process(
            ["bash", str(entrypoint)],
            cwd=registered_test.test_dir,
            environment=environment,
            timeout_seconds=max(0.001, deadline - time.monotonic()),
            test_paths=test_paths,
            label=f"{test_id}:run",
        )
        if workload.timed_out:
            phase = "timed_out"
            status = "fail"
            event = "test_timed_out"
            message = workload.message
        else:
            phase = "finished"
            status = "pass" if workload.exit_code == 0 else "fail"
            event = "test_finished"
            message = (
                ""
                if workload.exit_code == 0
                else f"workload exited with code {workload.exit_code}"
            )
        _finish_test(
            registered_test,
            test_paths=test_paths,
            result=result,
            run_paths=run_paths,
            logger=logger,
            phase=phase,
            status=status,
            exit_code=workload.exit_code,
            started_clock=started_clock,
            message=message,
            event=event,
        )
    except KeyboardInterrupt:
        _finish_test(
            registered_test,
            test_paths=test_paths,
            result=result,
            run_paths=run_paths,
            logger=logger,
            phase="interrupted",
            status="incomplete",
            exit_code=None,
            started_clock=started_clock,
            message="interrupted",
            event="test_finished",
        )
        raise
    except Exception as exc:  # noqa: BLE001 - isolate one broken test
        message = _first_line(str(exc)) or exc.__class__.__name__
        result["errors"].append(
            {
                "code": "test_framework_error",
                "message": message,
                "test_id": test_id,
                "timestamp": utc_now(),
                "detail_path": str(test_paths.stderr),
            }
        )
        _finish_test(
            registered_test,
            test_paths=test_paths,
            result=result,
            run_paths=run_paths,
            logger=logger,
            phase="framework_error",
            status="fail",
            exit_code=None,
            started_clock=started_clock,
            message=message,
            event="test_finished",
        )


def _finish_test(
    registered_test: RegisteredValidationTest,
    *,
    test_paths: TestPaths,
    result: dict[str, Any],
    run_paths: RunPaths,
    logger: RunLogger,
    phase: str,
    status: str,
    exit_code: int | None,
    started_clock: float,
    message: str,
    event: str,
) -> None:
    test_id = registered_test.id
    state = result["tests"][test_id]
    state.update(
        {
            "phase": phase,
            "status": status,
            "completed_at": utc_now(),
            "duration_ms": max(
                0, int((time.monotonic() - started_clock) * 1000)
            ),
            "exit_code": exit_code,
            "message": message,
        }
    )
    _write_test_result(test_id, state, test_paths.result)
    _write_state(result, run_paths)
    logger.emit(
        event,
        test_id=test_id,
        test_paths=test_paths,
        status=status,
        message=message,
        phase=phase,
        exit_code=exit_code,
        duration_ms=state["duration_ms"],
    )
    _legacy_completion_message(logger, test_id, status, test_paths, state)


def _initial_result(
    *,
    config: CvalConfig,
    registry: ValidationTestRegistry,
    runtime_env: Mapping[str, str],
    node: str,
    timestamp: int,
    run_id: str,
    validation_root: Path,
) -> dict[str, Any]:
    tests: dict[str, Any] = {}
    for registered_test in registry.tests:
        selected = registered_test.enabled
        paths = _test_paths(validation_root, node, run_id, registered_test)
        tests[registered_test.id] = {
            "display_name": registered_test.definition.metadata.display_name,
            "enabled": registered_test.enabled,
            "selected": selected,
            "order": registered_test.definition.metadata.order,
            "status": "incomplete",
            "phase": "pending" if selected else "not_selected",
            "started_at": None,
            "completed_at": None,
            "duration_ms": None,
            "exit_code": None,
            "config_path": registered_test.config_path,
            "config_digest": validation_test_config_digest(registered_test),
            "stdout": str(paths.stdout) if selected else "",
            "stderr": str(paths.stderr) if selected else "",
            "log": str(paths.events) if selected else "",
            "summary": str(paths.summary) if selected else "",
            "result": str(paths.result) if selected else "",
            "artifacts": str(paths.artifacts) if selected else "",
            "message": "" if selected else "disabled by config",
        }
    selected_tests = [test for test in tests.values() if test["selected"]]
    pytorch_version, cuda_version = _framework_versions(runtime_env)
    return {
        "schema_version": "cval.results.v2",
        "run_id": run_id,
        "node": node,
        "timestamp": timestamp,
        "timestamp_la": datetime.fromtimestamp(timestamp, LOS_ANGELES).isoformat(),
        "generated_at": utc_now(),
        "completed_at": None if selected_tests else utc_now(),
        "overall": "incomplete",
        "image_name": runtime_env.get("CVAL_IMAGE_NAME", config.job.image_name),
        "pytorch_version": pytorch_version,
        "cuda_version": cuda_version,
        "git_ref": runtime_env.get("CVAL_GIT_REF", config.job.git_ref),
        "global_config_digest": _runtime_config_digest(config, runtime_env),
        "tests": tests,
        "errors": [],
    }


def _load_runtime_config(environment: Mapping[str, str]) -> CvalConfig:
    snapshot = environment.get("CVAL_CONFIG_SNAPSHOT_B64")
    if snapshot:
        runtime_repo_root = environment.get("CVAL_TEST_REPO_ROOT") or environment.get(
            "CVAL_REPO_DIR"
        )
        return load_config_snapshot(
            snapshot,
            repo_root=Path(runtime_repo_root) if runtime_repo_root else None,
        )
    configured_path = environment.get("CVAL_CONFIG_PATH")
    if configured_path and Path(configured_path).is_file():
        return load_config(Path(configured_path))
    return load_config()


def _framework_versions(environment: Mapping[str, str]) -> tuple[str, str]:
    """Return explicit or best-effort PyTorch/CUDA runtime versions."""

    pytorch = environment.get("CVAL_PYTORCH_VERSION", "")
    cuda = environment.get("CVAL_CUDA_VERSION", "")
    if pytorch and cuda:
        return pytorch, cuda
    try:
        torch = importlib.import_module("torch")
    except (ImportError, OSError):
        return pytorch, cuda
    if not pytorch:
        pytorch = str(getattr(torch, "__version__", ""))
    if not cuda:
        version = getattr(torch, "version", None)
        cuda = str(getattr(version, "cuda", "") or "")
    return pytorch, cuda


def _load_runtime_registry(
    config: CvalConfig,
    environment: Mapping[str, str],
) -> ValidationTestRegistry:
    raw_registry = environment.get("CVAL_TEST_REGISTRY_JSON")
    if raw_registry:
        try:
            payload = json.loads(raw_registry)
        except json.JSONDecodeError as exc:
            raise ValueError("CVAL_TEST_REGISTRY_JSON is invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("CVAL_TEST_REGISTRY_JSON must be an object")
        registrations: dict[str, dict[str, object]] = {}
        declared_orders: dict[str, int] = {}
        for test_id, raw in payload.items():
            if not isinstance(raw, dict):
                raise ValueError(
                    f"Runtime registry test {test_id!r} must be an object"
                )
            unknown = sorted(set(raw) - {"enabled", "config_path", "order"})
            if unknown:
                raise ValueError(
                    f"Unknown runtime registry fields for {test_id}: "
                    f"{', '.join(unknown)}"
                )
            registrations[test_id] = {
                "enabled": raw.get("enabled"),
                "config_path": raw.get("config_path"),
            }
            order = raw.get("order")
            if isinstance(order, bool) or not isinstance(order, int):
                raise ValueError(
                    f"Runtime registry order for {test_id!r} must be integer"
                )
            declared_orders[test_id] = order
        if environment.get("CVAL_TEST_REPO_ROOT"):
            repo_root = Path(environment["CVAL_TEST_REPO_ROOT"])
        elif environment.get("CVAL_VALIDATION_TESTS_DIR"):
            repo_root = Path(environment["CVAL_VALIDATION_TESTS_DIR"]).parent
        else:
            repo_root = Path(environment.get("CVAL_REPO_DIR", str(REPO_ROOT)))
        if not repo_root.is_dir():
            repo_root = REPO_ROOT
        registry = load_test_registry(
            registrations,
            repo_root=repo_root,
            include_defaults=False,
            require_enabled=False,
        )
        for test in registry.tests:
            if declared_orders[test.id] != test.definition.metadata.order:
                raise ValueError(
                    f"Runtime registry order for {test.id!r} "
                    "does not match test config"
                )
        return registry

    tests: list[RegisteredValidationTest] = []
    for test in config.tests.registry.tests:
        env_name = LEGACY_ENABLE_ENV.get(test.id)
        enabled = test.enabled
        if env_name and env_name in environment:
            enabled = _is_enabled(environment[env_name])
        tests.append(replace(test, enabled=enabled))
    return ValidationTestRegistry(tuple(tests))


def _test_environment(
    runtime_env: Mapping[str, str],
    *,
    registered_test: RegisteredValidationTest,
    test_paths: TestPaths,
    validation_root: Path,
    node: str,
    timestamp: int,
    run_id: str,
) -> dict[str, str]:
    environment = dict(runtime_env)
    environment.update(
        {
            "CVAL_RUN_ID": run_id,
            "CVAL_TEST_ID": registered_test.id,
            "CVAL_NODE": node,
            "CVAL_TIMESTAMP": str(timestamp),
            "GCRNODE": node,
            "GCRTIME": str(timestamp),
            "CVAL_TEST_DIR": str(registered_test.test_dir),
            "CVAL_TEST_CONFIG": str(registered_test.resolved_config_path),
            "CVAL_VALIDATION_ROOT": str(validation_root),
            "CVAL_TEST_OUTPUT_DIR": str(test_paths.artifacts),
            "CVAL_TEST_LOG_DIR": str(test_paths.log_dir),
            "CVAL_TEST_SUMMARY_FILE": str(test_paths.summary),
        }
    )
    if registered_test.id == "storage":
        environment.update(
            {
                "STORAGE_OUTPUT_DIR": str(test_paths.artifacts),
                "STORAGE_LOG_FILE": str(test_paths.stdout),
                "STORAGE_SUMMARY_FILE": str(test_paths.summary),
            }
        )
    elif registered_test.id == "nccl":
        environment.update(
            {
                "NCCL_OUTPUT_DIR": str(test_paths.artifacts),
                "NCCL_LOG_FILE": str(test_paths.workload_log),
                "NCCL_SUMMARY_FILE": str(test_paths.summary),
                "NCCL_IBBW_LOG_FILE": str(test_paths.artifacts / "ibbw.log"),
            }
        )
    elif registered_test.id == "dltest":
        environment.update(
            {
                "DLTEST_OUTPUT_DIR": str(test_paths.artifacts),
                "DLTEST_LOG_FILE": str(test_paths.workload_log),
                "DLTEST_SUMMARY_FILE": str(test_paths.summary),
            }
        )
    return environment


def _run_paths(
    validation_root: Path,
    node: str,
    run_id: str,
) -> RunPaths:
    log_dir = validation_root / "logs" / "job_logs" / node / run_id
    _require_below(validation_root, log_dir, "global run log directory")
    return RunPaths(
        log_dir=log_dir,
        stdout=log_dir / "stdout.log",
        stderr=log_dir / "stderr.log",
        job_log=log_dir / "job.log",
        events=log_dir / "events.jsonl",
        result=log_dir / "result.json",
        legacy_env=log_dir / "result.env",
        marker=log_dir / ".run-active",
    )


def _preflight_registry_paths(
    validation_root: Path,
    node: str,
    run_id: str,
    registry: ValidationTestRegistry,
) -> None:
    """Reject all global/dynamic test path hazards before the first evidence write."""

    _require_below(
        validation_root,
        validation_root / "logs/job_logs" / node / run_id,
        "global run log directory",
    )
    run_paths = _run_paths(validation_root, node, run_id)
    for field_name, path in (
        ("global stdout", run_paths.stdout),
        ("global stderr", run_paths.stderr),
        ("global job log", run_paths.job_log),
        ("global events", run_paths.events),
        ("global result", run_paths.result),
        ("global legacy result", run_paths.legacy_env),
        ("global marker", run_paths.marker),
        (
            "global ingestion digest",
            run_paths.log_dir / ".ingestion-result-digest",
        ),
    ):
        _require_below(validation_root, path, field_name)
    for registered_test in registry.tests:
        _test_paths(validation_root, node, run_id, registered_test)


def _test_paths(
    validation_root: Path,
    node: str,
    run_id: str,
    registered_test: RegisteredValidationTest,
) -> TestPaths:
    test_id = registered_test.id
    log_dir = validation_root / "logs" / test_id / node / run_id
    run_dir = (
        validation_root / "validation_tests" / test_id / "runs" / node / run_id
    )
    _require_below(validation_root, log_dir, "test log directory")
    _require_below(validation_root, run_dir, "test run directory")
    summary_name = registered_test.definition.artifacts.summary_filename
    return TestPaths(
        log_dir=log_dir,
        stdout=log_dir / "stdout.log",
        stderr=log_dir / "stderr.log",
        events=log_dir / "events.jsonl",
        run_dir=run_dir,
        result=run_dir / "result.json",
        summary=run_dir / summary_name,
        artifacts=run_dir / "artifacts",
        workload_log=log_dir / "workload.log",
    )


def _prepare_run_paths(
    paths: RunPaths,
    *,
    allow_external_logs: bool = False,
    marker_preacquired: bool = False,
) -> None:
    _require_below(paths.log_dir.parents[3], paths.log_dir, "global run log directory")
    for path in (
        paths.stdout,
        paths.stderr,
        paths.job_log,
        paths.events,
        paths.result,
        paths.legacy_env,
        paths.marker,
        paths.log_dir / ".ingestion-result-digest",
    ):
        _require_below(paths.log_dir.parents[3], path, "global evidence file")
    allowed_existing: set[Path] = set()
    if allow_external_logs:
        allowed_existing.update((paths.stdout, paths.stderr, paths.job_log))
    if marker_preacquired:
        allowed_existing.add(paths.marker)
    existing = set(paths.log_dir.iterdir()) if paths.log_dir.exists() else set()
    unexpected = existing - allowed_existing
    if unexpected:
        raise FileExistsError(
            "Run evidence already exists; refusing run_id reuse: "
            f"{', '.join(str(path) for path in sorted(unexpected))}"
        )
    paths.log_dir.mkdir(parents=True, exist_ok=True)
    if marker_preacquired:
        if not paths.marker.is_file():
            raise FileNotFoundError(
                f"Pre-acquired run marker is missing: {paths.marker}"
            )
    else:
        _create_run_marker(paths)
    for path in allowed_existing:
        if path.exists() and (path.is_symlink() or not path.is_file()):
            raise ValueError(f"External global evidence is not a regular file: {path}")
    paths.result.parent.mkdir(parents=True, exist_ok=True)
    paths.legacy_env.parent.mkdir(parents=True, exist_ok=True)
    for path in (paths.stdout, paths.stderr, paths.job_log, paths.events):
        path.touch(exist_ok=True)


def _create_run_marker(paths: RunPaths) -> None:
    """Atomically acquire one run ID for the current process."""

    try:
        descriptor = os.open(
            paths.marker,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
    except FileExistsError as exc:
        raise FileExistsError(
            f"Run is already active; refusing duplicate run_id: {paths.log_dir.name}"
        ) from exc
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        os.fsync(handle.fileno())


def _prepare_test_paths(paths: TestPaths) -> None:
    validation_root = paths.log_dir.parents[3]
    _require_below(validation_root, paths.log_dir, "test log directory")
    _require_below(validation_root, paths.run_dir, "test run directory")
    if paths.log_dir.exists() or paths.run_dir.exists():
        raise FileExistsError(
            "Per-test run evidence already exists; refusing reuse: "
            f"{paths.log_dir} or {paths.run_dir}"
        )
    paths.log_dir.parent.mkdir(parents=True, exist_ok=True)
    paths.run_dir.parent.mkdir(parents=True, exist_ok=True)
    paths.log_dir.mkdir()
    paths.run_dir.mkdir()
    paths.artifacts.mkdir()
    for path in (paths.stdout, paths.stderr, paths.events, paths.workload_log):
        path.touch(exist_ok=True)


def _write_state(result: dict[str, Any], paths: RunPaths) -> None:
    result["generated_at"] = utc_now()
    result["overall"] = _overall(result)
    selected = [
        state
        for state in result["tests"].values()
        if state["enabled"] and state["selected"]
    ]
    terminal_phases = {
        "not_selected",
        "finished",
        "setup_failed",
        "timed_out",
        "interrupted",
        "framework_error",
    }
    if all(state["phase"] in terminal_phases for state in selected):
        if result["completed_at"] is None:
            result["completed_at"] = utc_now()
    else:
        result["completed_at"] = None
    parse_validation_result_v2(result)
    _atomic_write_json(paths.result, result)
    _write_legacy_env(result, paths.legacy_env)


def _write_test_result(test_id: str, state: dict[str, Any], path: Path) -> None:
    payload = {
        "schema_version": TEST_RESULT_SCHEMA_VERSION,
        "test_id": test_id,
        "status": state["status"],
        "phase": state["phase"],
        "started_at": state["started_at"],
        "completed_at": state["completed_at"],
        "duration_ms": state["duration_ms"],
        "exit_code": state["exit_code"],
        "summary": state["summary"],
        "artifacts": state["artifacts"],
        "message": state["message"],
    }
    _atomic_write_json(path, payload)


def _write_legacy_env(result: dict[str, Any], path: Path) -> None:
    lines: list[str] = []
    for test_id, result_name in LEGACY_RESULT_ENV.items():
        state = result["tests"].get(test_id)
        status = state["status"] if state else "incomplete"
        lines.append(f"{result_name}={status}")
    for test_id, enabled_name in LEGACY_ENABLE_ENV.items():
        state = result["tests"].get(test_id)
        enabled = bool(state and state["enabled"])
        lines.append(f"{enabled_name}={str(enabled).lower()}")
    lines.append(f"overall_result={result['overall']}")
    lines.append(f"result_node={result['node']}")
    lines.append(f"result_timestamp={result['timestamp']}")
    lines.append(f"result_run_id={result['run_id']}")
    _atomic_write_text(path, "\n".join(lines) + "\n")


def _overall(result: dict[str, Any]) -> str:
    tests = [
        state
        for state in result["tests"].values()
        if state["enabled"] and state["selected"]
    ]
    if not tests:
        return "incomplete"
    if any(state["status"] == "fail" for state in tests):
        return "fail"
    if all(state["status"] == "pass" for state in tests):
        return "pass"
    return "incomplete"


def _runtime_config_digest(
    config: CvalConfig,
    environment: Mapping[str, str],
) -> str:
    value = environment.get("CVAL_CONFIG_DIGEST")
    expected = effective_config_digest(config)
    if value:
        if not value.startswith(DIGEST_PREFIX) or len(value) != 71:
            raise ValueError("CVAL_CONFIG_DIGEST must be a sha256 digest")
        if value != expected:
            raise ValueError(
                "CVAL_CONFIG_DIGEST does not match the effective runtime snapshot"
            )
        return value
    return expected


def _runtime_timestamp(value: str | None) -> int:
    if value is None or not value.strip():
        return int(time.time())
    text = value.strip()
    if text.isdigit():
        return int(text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        pass
    try:
        return int(
            datetime.strptime(text, "%Y%m%d_%H%M%S")
            .replace(tzinfo=LOS_ANGELES)
            .timestamp()
        )
    except ValueError as exc:
        raise ValueError(f"Invalid c-val runtime timestamp: {value!r}") from exc


def _legacy_completion_message(
    logger: RunLogger,
    test_id: str,
    status: str,
    paths: TestPaths,
    state: Mapping[str, Any],
) -> None:
    if test_id == "storage":
        prefix = (
            "Storage test is complete." if status == "pass" else "Storage test FAILED."
        )
    elif test_id == "nccl":
        prefix = "NCCL test is complete." if status == "pass" else "NCCL test FAILED."
    else:
        return
    logger.message(
        f"{prefix} Log file: {paths.stdout} Summary file: {state['summary']}",
        test_paths=paths,
    )


def _legacy_final_line(result: Mapping[str, Any]) -> str:
    values = []
    for test_id in ("storage", "nccl", "dltest"):
        state = result["tests"].get(test_id)
        values.append(f"{test_id}={state['status'] if state else 'incomplete'}")
    return "Final c-val test results: " + " ".join(values)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{threading.get_ident()}"
    )
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _safe_segment(value: str, field_name: str) -> str:
    if value in {".", ".."} or not PATH_SEGMENT_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {field_name} path segment: {value!r}")
    return value


def _require_below(root: Path, path: Path, field_name: str) -> None:
    """Reject lexical escape and every existing symlink before evidence writes."""

    if not root.is_absolute() or not path.is_absolute():
        raise ValueError(f"{field_name} requires absolute paths")
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field_name} escapes validation root: {path}") from exc
    current = root
    if current.is_symlink():
        raise ValueError(f"validation root is a symlink: {current}")
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{field_name} contains a symlink: {current}")
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"{field_name} resolves outside validation root: {path}") from exc


def _is_enabled(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _first_line(value: str) -> str:
    return value.splitlines()[0] if value else ""


if __name__ == "__main__":
    raise SystemExit(main())
