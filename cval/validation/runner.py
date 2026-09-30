"""Generic in-pod runner for registered repository-local validation tests."""

from __future__ import annotations

import importlib
import json
import os
import re
import signal
import stat
import sys
import threading
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, TextIO
from zoneinfo import ZoneInfo

from cval.config import CvalConfig, REPO_ROOT, load_config, load_config_snapshot
from cval.validation.execution import RunLogger, RunPaths, TestPaths, utc_now
from cval.validation.registry import (
    RegisteredValidationTest,
    ValidationTestRegistry,
    validation_test_config_digest,
)
from cval.validation.results import COMPACT_RESULT_SCHEMA, parse_validation_result_v2
from cval.validation.plugins import load_registered_plugin
from cval.validation.runtime import build_runtime_environment, effective_config_digest, test_evidence_environment
from cval.validation.supervisor import _add_builtin_compatibility_paths, reserve_secure_run_layout


LOS_ANGELES = ZoneInfo("America/Los_Angeles")
DIGEST_PREFIX = "sha256:"
PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")


def run_validation_tests(
    *,
    config: CvalConfig | None = None,
    registry: ValidationTestRegistry | None = None,
    environ: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> dict[str, Any]:
    """Run every enabled test and return validated ``cval.results`` state."""

    runtime_env = dict(os.environ if environ is None else environ)
    config = config or _load_runtime_config(runtime_env)
    registry = registry or config.tests.registry
    if registry.to_dict() != config.tests.registry.to_dict():
        raise ValueError("Runtime registry disagrees with the configuration")
    expected_environment = build_runtime_environment(config)
    for name, expected in expected_environment.items():
        if name in runtime_env and runtime_env[name] != expected:
            raise ValueError(f"{name} disagrees with the immutable config snapshot")
    runtime_env.update(expected_environment)
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr

    node = _safe_segment(
        runtime_env.get("CVAL_NODE") or runtime_env["GCRNODE"],
        "node",
    )
    timestamp = _runtime_timestamp(
        runtime_env.get("CVAL_TIMESTAMP") or runtime_env.get("GCRTIME")
    )
    run_id = _safe_segment(
        runtime_env.get("CVAL_RUN_ID") or f"{node}-{timestamp}",
        "run_id",
    )
    if runtime_env.get("GCRNODE", node) != node or _runtime_timestamp(runtime_env.get("GCRTIME", str(timestamp))) != timestamp:
        raise ValueError("Runtime node/timestamp aliases disagree")
    if runtime_env.get("CVAL_VALIDATION_ROOT", config.runtime.validation_root) != config.runtime.validation_root:
        raise ValueError("CVAL_VALIDATION_ROOT disagrees with the config snapshot")
    validation_root = Path(config.runtime.validation_root)
    if not validation_root.is_absolute():
        raise ValueError("CVAL_VALIDATION_ROOT must be an absolute path")

    external_logging = _is_enabled(
        runtime_env.get("CVAL_EXTERNAL_GLOBAL_LOGGING", "false")
    )
    marker_preacquired = _is_enabled(
        runtime_env.get("CVAL_RUN_MARKER_PREACQUIRED", "false")
    )
    with ExitStack() as resources:
        owned_layout = None
        secure_layout = _load_secure_layout(runtime_env, validation_root, registry)
        if secure_layout is None:
            if external_logging or marker_preacquired or runtime_env.get("CVAL_SECURE_RUN_FDS"):
                raise ValueError("Pre-acquired evidence requires a verified secure run layout")
            owned_layout = reserve_secure_run_layout(
                validation_root, node, run_id, registry_json=runtime_env["CVAL_TEST_REGISTRY_JSON"],
            )
            resources.callback(owned_layout.close)
            runtime_env.update(CVAL_NODE=node, CVAL_RUN_ID=run_id, GCRNODE=node, GCRTIME=str(timestamp))
            _add_builtin_compatibility_paths(runtime_env, owned_layout, config)
            runtime_env.update(owned_layout.environment())
            runtime_env["CVAL_EXTERNAL_GLOBAL_LOGGING"] = "false"
            secure_layout = _load_secure_layout(runtime_env, validation_root, registry)
        elif not external_logging or not marker_preacquired:
            raise ValueError("Inherited secure layout requires supervisor-owned marker and logging")
        secure_pass_fds = _secure_pass_fds(runtime_env)
        _validate_secure_pass_fds(secure_layout, secure_pass_fds)
        run_paths = _secure_run_paths(secure_layout)
        for name, path in (("CVAL_JOB_LOG_DIR", run_paths.log_dir), ("CVAL_RESULT_DIR", run_paths.log_dir),
                   ("CVAL_RESULT_JSON_FILE", run_paths.result)):
            if runtime_env.get(name) != str(path):
                raise ValueError(f"{name} disagrees with the secure run layout")
        _prepare_run_paths(run_paths)
        logger = RunLogger(
            run_paths,
            run_id,
            stdout=stdout,
            stderr=stderr,
            write_global_files=not external_logging,
            pass_fds=secure_pass_fds,
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
        started_clock = time.monotonic()
        logger.emit(
            "run_started",
            status="incomplete",
            tests=[test.id for test in registry.enabled],
            producer="runner",
            node_name=node,
            git_ref=result["git_ref"],
            config_digest=result["global_config_digest"],
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
                secure_layout=secure_layout,
            )
        except KeyboardInterrupt:
            logger.emit(
                "run_interrupted",
                status="incomplete",
                message="received termination signal",
                producer="runner",
                node_name=node,
                duration_ms=max(0, int((time.monotonic() - started_clock) * 1000)),
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
            producer="runner",
            node_name=node,
            duration_ms=max(0, int((time.monotonic() - started_clock) * 1000)),
        )
        _write_state(result, run_paths)
        if owned_layout is not None:
            owned_layout.release_marker()
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
    secure_layout: Mapping[str, Any],
) -> None:
    for registered_test in registry.tests:
        if not registered_test.enabled:
            logger.emit(
                "test_skipped",
                test_id=registered_test.id,
                status="incomplete",
                message="disabled by config",
            )
            continue
        test_paths = _secure_test_paths(secure_layout, registered_test)
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
                "detail_path": state["log"],
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
        producer="runner",
        node_name=result["node"],
    )


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
            "log": str(paths.job_log) if selected else "",
            "summary": str(paths.summary) if selected else "",
            "artifacts": str(paths.artifacts) if selected else "",
            "message": "" if selected else "disabled by config",
        }
    selected_tests = [test for test in tests.values() if test["selected"]]
    pytorch_version, cuda_version = _framework_versions(runtime_env)
    return {
        "schema_version": COMPACT_RESULT_SCHEMA,
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
    if configured_path:
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
    values = {
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
    aliases = test_evidence_environment(
        registered_test, log_dir=test_paths.log_dir, run_dir=test_paths.run_dir,
        artifacts=test_paths.artifacts, node=node, timestamp=timestamp,
        validation_root=validation_root, run_id=run_id,
    )
    values.update(aliases)
    for name, expected in values.items():
        if name in runtime_env and runtime_env[name] != expected:
            raise ValueError(f"{name} disagrees with the reserved test evidence")
    return dict(runtime_env) | values


def _test_paths(
    validation_root: Path,
    node: str,
    run_id: str,
    registered_test: RegisteredValidationTest,
) -> TestPaths:
    test_id = registered_test.id
    log_dir = validation_root / "logs" / test_id / node / run_id
    run_dir = log_dir
    _require_below(validation_root, log_dir, "test log directory")
    _require_below(validation_root, run_dir, "test run directory")
    summary_name = registered_test.definition.artifacts.summary_filename
    return TestPaths(
        log_dir=log_dir,
        run_dir=run_dir,
        summary=run_dir / summary_name,
        artifacts=run_dir,
        job_log=log_dir / "job.log",
    )


def _prepare_run_paths(paths: RunPaths) -> None:
    expected_names = {"job.log", ".run-active"}
    if {path.name for path in paths.log_dir.iterdir()} != expected_names:
        raise FileExistsError("Secure run evidence reservation is incomplete or reused")
    for path in paths.log_dir.iterdir():
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Reserved global evidence is not regular: {path.name}")


def _prepare_test_paths(paths: TestPaths) -> None:
    expected_logs = {"job.log"}
    if {path.name for path in paths.log_dir.iterdir()} != expected_logs:
        raise FileExistsError("Secure per-test log reservation is incomplete or reused")
    if any(path.is_symlink() or not path.is_file() for path in paths.log_dir.iterdir()):
        raise ValueError("Secure per-test log reservation contains a non-regular file")
    if not paths.artifacts.is_dir():
        raise ValueError("Secure per-test artifacts reservation is not a directory")


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
    with paths.result.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, sort_keys=True, allow_nan=False)
        handle.flush()


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


def _load_secure_layout(
    environment: Mapping[str, str],
    validation_root: Path,
    registry: ValidationTestRegistry,
) -> Mapping[str, Any] | None:
    raw = environment.get("CVAL_SECURE_RUN_LAYOUT_JSON")
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("CVAL_SECURE_RUN_LAYOUT_JSON is invalid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "schema_version",
        "canonical_root",
        "node",
        "run_id",
        "root_fd",
        "run_dir_fd",
        "result_file_fd",
        "result_device",
        "result_inode",
        "result_size",
        "result_mtime_ns",
        "result_ctime_ns",
        "result_digest",
        "config_digest",
        "tests",
    }:
        raise ValueError("Secure run layout has unexpected fields")
    if payload["schema_version"] != "cval.secure-run-layout.v2":
        raise ValueError("Unsupported secure run layout schema")
    if payload["canonical_root"] != str(validation_root):
        raise ValueError("Secure run layout canonical root mismatch")
    expected_node = environment.get("CVAL_NODE") or environment.get("GCRNODE")
    expected_run_id = environment.get("CVAL_RUN_ID")
    if payload["node"] != expected_node or payload["run_id"] != expected_run_id:
        raise ValueError("Secure run layout identity mismatch")
    for name in (
        "result_device",
        "result_inode",
        "result_size",
        "result_mtime_ns",
        "result_ctime_ns",
    ):
        if payload[name] is not None:
            raise ValueError("Runner received an already-bound secure result")
    if payload["result_digest"] is not None or payload["config_digest"] is not None:
        raise ValueError("Runner received precomputed secure result digests")
    tests = payload["tests"]
    if not isinstance(tests, dict):
        raise ValueError("Secure run layout tests must be an object")
    expected = {test.id for test in registry.enabled}
    if set(tests) != expected:
        raise ValueError("Secure run layout does not match enabled registry tests")
    for name in ("root_fd", "run_dir_fd"):
        _require_directory_fd(payload[name], name)
    result_descriptor = payload["result_file_fd"]
    if type(result_descriptor) is not int or not stat.S_ISREG(os.fstat(result_descriptor).st_mode) or os.fstat(result_descriptor).st_nlink != 0:
        raise ValueError("Runner result must be an anonymous inherited buffer")
    for test_id, values in tests.items():
        if not isinstance(values, dict) or set(values) != {
            "log_dir_fd",
            "run_dir_fd",
            "artifacts_dir_fd",
        }:
            raise ValueError(f"Secure run layout for {test_id!r} is malformed")
        for name, descriptor in values.items():
            _require_directory_fd(descriptor, f"{test_id}.{name}")
    return payload


def _require_directory_fd(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"Secure run layout descriptor {name} must be an integer")
    try:
        descriptor_stat = os.fstat(value)
    except OSError as exc:
        raise ValueError(f"Secure run layout descriptor {name} is not open") from exc
    if not stat.S_ISDIR(descriptor_stat.st_mode):
        raise ValueError(f"Secure run layout descriptor {name} is not a directory")


def _secure_run_paths(layout: Mapping[str, Any]) -> RunPaths:
    log_dir = Path(f"/proc/self/fd/{layout['run_dir_fd']}")
    return RunPaths(
        log_dir=log_dir,
        job_log=log_dir / "job.log",
        result=Path(f"/proc/self/fd/{layout['result_file_fd']}"),
        marker=log_dir / ".run-active",
    )


def _secure_test_paths(
    layout: Mapping[str, Any], registered_test: RegisteredValidationTest
) -> TestPaths:
    values = layout["tests"][registered_test.id]
    log_dir = Path(f"/proc/self/fd/{values['log_dir_fd']}")
    run_dir = Path(f"/proc/self/fd/{values['run_dir_fd']}")
    artifacts = Path(f"/proc/self/fd/{values['artifacts_dir_fd']}")
    plugin = load_registered_plugin(registered_test)
    writer = plugin.write_log if plugin and "log" in plugin.capabilities else None
    return TestPaths(
        log_dir=log_dir,
        run_dir=run_dir,
        summary=run_dir / registered_test.definition.artifacts.summary_filename,
        artifacts=artifacts,
        job_log=log_dir / "job.log",
        write_log=writer,
        format_output=getattr(plugin, "format_output", None) if writer else None,
    )


def _secure_pass_fds(environment: Mapping[str, str]) -> tuple[int, ...]:
    raw = environment.get("CVAL_SECURE_RUN_FDS", "")
    if not raw:
        return ()
    try:
        descriptors = tuple(int(value) for value in raw.split(","))
    except ValueError as exc:
        raise ValueError("CVAL_SECURE_RUN_FDS must contain integers") from exc
    if len(descriptors) != len(set(descriptors)):
        raise ValueError("CVAL_SECURE_RUN_FDS contains duplicate descriptors")
    for descriptor in descriptors:
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode) and not (stat.S_ISREG(info.st_mode) and info.st_nlink == 0):
            raise ValueError("Unexpected inherited descriptor type")
    return descriptors


def _validate_secure_pass_fds(
    layout: Mapping[str, Any], descriptors: tuple[int, ...]
) -> None:
    expected = {layout["root_fd"], layout["run_dir_fd"], layout["result_file_fd"]}
    for values in layout["tests"].values():
        expected.update(values.values())
    if set(descriptors) != expected:
        raise ValueError("CVAL_SECURE_RUN_FDS does not match the secure run layout")


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
