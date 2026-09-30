"""Process execution and log streaming for the modular validation runner."""

from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, TextIO


EVENT_SCHEMA_VERSION = "cval.event.v1"


def event_payload(run_id: str, event: str, *, test_id: str | None = None,
          status: str | None = None, message: str = "", **fields: Any) -> dict[str, Any]:
    """Build the common record for runner, ingestion and supervisor milestones."""

    return {"schema_version": EVENT_SCHEMA_VERSION, "event": event, "run_id": run_id,
        "test": test_id, "timestamp": utc_now(), "status": status, "message": message, **fields}


@dataclass(frozen=True)
class RunPaths:
    """Canonical framework-owned files for one node validation run."""

    log_dir: Path
    job_log: Path
    result: Path
    marker: Path


@dataclass(frozen=True)
class TestPaths:
    """Canonical framework-owned files for one test in one run."""

    log_dir: Path
    run_dir: Path
    summary: Path
    artifacts: Path
    job_log: Path
    write_log: Callable[[Path, str], None] | None = None
    format_output: Callable[..., str] | None = None


@dataclass(frozen=True)
class ProcessOutcome:
    """Outcome of one setup or workload subprocess."""

    exit_code: int
    timed_out: bool
    duration_ms: int
    message: str = ""


class RunLogger:
    """Write events and streamed subprocess output to canonical logs."""

    def __init__(
        self,
        paths: RunPaths,
        run_id: str,
        *,
        stdout: TextIO,
        stderr: TextIO,
        write_global_files: bool = True,
        pass_fds: tuple[int, ...] = (),
    ) -> None:
        self.paths = paths
        self.run_id = run_id
        self.stdout = stdout
        self.stderr = stderr
        self.write_global_files = write_global_files
        self.pass_fds = pass_fds
        self._lock = threading.Lock()
        self._process_lock = threading.RLock()
        self._active_process: subprocess.Popen[str] | None = None
        self._active_process_group: int | None = None

    def emit(
        self,
        event: str,
        *,
        test_id: str | None = None,
        test_paths: TestPaths | None = None,
        status: str | None = None,
        message: str = "",
        **fields: Any,
    ) -> dict[str, Any]:
        """Persist and print one ``cval.event.v1`` object."""

        payload = event_payload(self.run_id, event, test_id=test_id, status=status, message=message, **fields)
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        line = f"\nCVAL_EVENT {serialized}\n"
        with self._lock:
            if self.write_global_files:
                append_text(self.paths.job_log, line)
            if test_paths is not None:
                (test_paths.write_log or append_text)(test_paths.job_log, line)
            self.stdout.write(line)
            self.stdout.flush()
        return payload

    def message(
        self,
        text: str,
        *,
        test_paths: TestPaths | None = None,
        error: bool = False,
    ) -> None:
        """Write one compatibility/operator message to logs and console."""

        line = text if text.endswith("\n") else text + "\n"
        destination = self.stderr if error else self.stdout
        with self._lock:
            if self.write_global_files:
                append_text(self.paths.job_log, line)
            if test_paths is not None:
                (test_paths.write_log or append_text)(test_paths.job_log, line)
            destination.write(line)
            destination.flush()

    def stream_process(
        self,
        argv: list[str],
        *,
        cwd: Path,
        environment: Mapping[str, str],
        timeout_seconds: float,
        test_paths: TestPaths,
        label: str,
    ) -> ProcessOutcome:
        """Run one process and tee stdout/stderr live without a shell."""

        started = time.monotonic()
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            env=dict(environment),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            start_new_session=True,
            pass_fds=self.pass_fds,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        process_group_id = process.pid
        with self._process_lock:
            self._active_process = process
            self._active_process_group = process_group_id
        pump_errors: list[BaseException] = []
        threads = [
            threading.Thread(
                target=self._pump,
                args=(process.stdout, "stdout", test_paths, label, pump_errors),
                daemon=True,
            ),
            threading.Thread(
                target=self._pump,
                args=(process.stderr, "stderr", test_paths, label, pump_errors),
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()

        timed_out = False
        message = ""
        deadline = started + max(0.001, timeout_seconds)
        try:
            exit_code = process.wait(timeout=max(0.001, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            timed_out = True
            message = f"{label} exceeded timeout ({timeout_seconds:.0f}s)"
            exit_code = -9
        except BaseException:
            terminate_process_group(process_group_id, process)
            self._clear_active(process)
            raise

        if not timed_out:
            for thread in threads:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
            descendants_remain = process_group_exists(process_group_id)
            if any(thread.is_alive() for thread in threads) or descendants_remain:
                timed_out = True
                message = f"{label} left running descendant processes"
                exit_code = -9

        if timed_out:
            terminate_process_group(process_group_id, process)
            for thread in threads:
                thread.join(timeout=2.0)
        for stream in (process.stdout, process.stderr):
            if not stream.closed:
                stream.close()
        for thread in threads:
            thread.join(timeout=1.0)
        if any(thread.is_alive() for thread in threads):
            self._clear_active(process)
            raise RuntimeError(f"{label} log streams did not close after termination")
        if pump_errors and not timed_out:
            self._clear_active(process)
            raise RuntimeError(f"{label} log capture failed: {pump_errors[0]}")
        self._clear_active(process)

        duration_ms = max(0, int((time.monotonic() - started) * 1000))
        return ProcessOutcome(
            exit_code=int(exit_code),
            timed_out=timed_out,
            duration_ms=duration_ms,
            message=message,
        )

    def terminate_active(self) -> None:
        """Terminate the currently active setup/workload process group, if any."""

        with self._process_lock:
            process = self._active_process
            process_group_id = self._active_process_group
        if process is not None and process_group_id is not None:
            terminate_process_group(process_group_id, process)

    def _clear_active(self, process: subprocess.Popen[str]) -> None:
        with self._process_lock:
            if self._active_process is process:
                self._active_process = None
                self._active_process_group = None

    def _pump(
        self,
        stream: TextIO,
        stream_name: str,
        test_paths: TestPaths,
        label: str,
        errors: list[BaseException],
    ) -> None:
        console = self.stdout if stream_name == "stdout" else self.stderr
        try:
            for line in iter(stream.readline, ""):
                text = (test_paths.format_output(line, label=label, stream=stream_name)
                    if test_paths.format_output else f"[{label}:{stream_name}] {line}")
                if not isinstance(text, str):
                    raise TypeError("Test log formatter must return text")
                with self._lock:
                    (test_paths.write_log or append_text)(test_paths.job_log, text)
                    if self.write_global_files:
                        append_text(self.paths.job_log, text)
                    console.write(text)
                    console.flush()
        except BaseException as exc:  # noqa: BLE001 - propagate through caller
            errors.append(exc)


def process_group_exists(process_group_id: int) -> bool:
    """Return whether any process remains in the runner-created process group."""

    try:
        os.killpg(process_group_id, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def terminate_process_group(
    process_group_id: int,
    process: subprocess.Popen[str] | subprocess.Popen[bytes],
    grace_seconds: float = 1.0,
) -> None:
    """Terminate an entire process group even when its leader already exited."""

    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace_seconds
    while process_group_exists(process_group_id) and time.monotonic() < deadline:
        time.sleep(0.02)
    if process_group_exists(process_group_id):
        try:
            os.killpg(process_group_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process.poll() is None:
        try:
            process.wait(timeout=max(1.0, grace_seconds))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("process group did not terminate after SIGKILL") from exc


def append_text(path: Path, text: str) -> None:
    """Append UTF-8 text and flush it for live readers."""

    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid():
            raise ValueError("Log must be an owned single-link regular file")
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(text)
            handle.flush()
    finally:
        os.close(descriptor)


def utc_now() -> str:
    """Return current UTC time in RFC 3339 form."""

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
