"""One-shot U11 evaluator service wrapper with one strict stdout envelope."""

from __future__ import annotations

import io
import signal
import threading
import time
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Iterator

from cval.config import CvalConfig
from cval.evaluator.preflight import run_deployment_preflight
from cval.evaluator.release import (
    DEFAULT_BUILD_MARKER,
    effective_config_digest,
    read_verified_release_identity,
)
from cval.health.evaluator import EVALUATE_CONFIRMATION, evaluate_health_cycle


SERVICE_SCHEMA = "cval.evaluator-cycle.v1"
_DIAGNOSTIC_CAPTURE_LIMIT = 4096


class _EvaluatorInterrupted(BaseException):
    def __init__(self, signal_number: int) -> None:
        super().__init__(signal_number)
        self.signal_number = signal_number


class _DependencyFailure(RuntimeError):
    def __init__(self, stage: str, exception_name: str) -> None:
        super().__init__(f"{stage} dependency failed ({exception_name})")


class _BoundedSink(io.TextIOBase):
    def __init__(self, limit: int = _DIAGNOSTIC_CAPTURE_LIMIT) -> None:
        self.limit = limit
        self.characters = 0
        self.captured = 0

    def writable(self) -> bool:
        return True

    def write(self, value: str) -> int:
        text = str(value)
        length = len(text)
        self.characters += length
        self.captured += min(length, max(0, self.limit - self.captured))
        return length


def run_evaluator_service(
    config: CvalConfig,
    *,
    apply: bool = False,
    confirmation: str | None = None,
    write_enabled: bool = False,
    expected_commit: str | None = None,
    image_ref: str | None = None,
    marker_path: Path = DEFAULT_BUILD_MARKER,
    wall_clock: Callable[[], float] = time.time,
    monotonic_ns: Callable[[], int] = time.monotonic_ns,
    preflight_runner: Callable[..., dict[str, Any]] = run_deployment_preflight,
    evaluator: Callable[..., Any] = evaluate_health_cycle,
) -> dict[str, Any]:
    """Run startup verification, read-only preflight, then exactly one U9 cycle."""

    started_at = int(wall_clock())
    started_ns = monotonic_ns()
    release: dict[str, str] = {
        "commit": expected_commit or "",
        "image": image_ref or "",
    }
    preflight: dict[str, Any] | None = None
    u9_report: dict[str, Any] | None = None
    diagnostics: list[dict[str, Any]] = []
    interrupted_signal: int | None = None
    error = ""
    exit_code = 2
    effective = replace(
        config,
        health_evaluator=replace(
            config.health_evaluator,
            write_enabled=bool(write_enabled),
        ),
    )
    installed_handlers: dict[int, Any] = {}
    if threading.current_thread() is threading.main_thread():
        def handle_signal(received: int, _frame: Any) -> None:
            raise _EvaluatorInterrupted(received)

        for signal_number in (signal.SIGINT, signal.SIGTERM):
            installed_handlers[signal_number] = signal.getsignal(signal_number)
            signal.signal(signal_number, handle_signal)
    try:
        if apply:
            if not write_enabled:
                raise ValueError("Evaluator apply requires the independent write gate true")
            if confirmation != EVALUATE_CONFIRMATION:
                raise ValueError("Evaluator apply requires exact confirmation 'evaluate'")
        elif confirmation is not None:
            raise ValueError("Evaluator confirmation is valid only with --apply")
        elif write_enabled:
            raise ValueError("Shadow service requires the independent write gate false")
        release = read_verified_release_identity(
            expected_commit=expected_commit,
            image_ref=image_ref,
            marker_path=marker_path,
        )
        with _suppressed_dependency_output(diagnostics, "preflight"):
            preflight = preflight_runner(effective, access="rw" if apply else "ro")
        if not preflight.get("ok"):
            exit_code = 1
            error = "deployment preflight failed"
        else:
            with _suppressed_dependency_output(diagnostics, "evaluator"):
                report = evaluator(
                    effective,
                    apply=apply,
                    confirmation=confirmation,
                    now=started_at,
                )
                u9_report = report.to_dict()
            exit_code = 0 if report.ok else 1
            if exit_code:
                error = "U9 evaluator reported one or more test errors"
    except _EvaluatorInterrupted as exc:
        interrupted_signal = exc.signal_number
        error = f"interrupted by {signal.Signals(exc.signal_number).name}"
        exit_code = 128 + exc.signal_number
    except KeyboardInterrupt:
        interrupted_signal = signal.SIGINT
        error = "interrupted by SIGINT"
        exit_code = 128 + signal.SIGINT
    except SystemExit:
        error = "service dependency failed (SystemExit)"
        exit_code = 2
    except Exception as exc:  # noqa: BLE001 - service stdout contract boundary
        error = _safe_error(exc)
        exit_code = 2
    finally:
        for signal_number, previous in installed_handlers.items():
            signal.signal(signal_number, previous)
    duration_ms = max(0, (monotonic_ns() - started_ns) // 1_000_000)
    return {
        "schema_version": SERVICE_SCHEMA,
        "ok": exit_code == 0,
        "mode": "apply" if apply else "shadow",
        "release": {
            **release,
            "config_digest": effective_config_digest(effective),
        },
        "started_at": started_at,
        "duration_ms": duration_ms,
        "preflight": preflight,
        "u9_report": u9_report,
        "exit_code": exit_code,
        "error": error,
        "signal": (
            None
            if interrupted_signal is None
            else {
                "number": int(interrupted_signal),
                "name": signal.Signals(interrupted_signal).name,
            }
        ),
        "diagnostics": diagnostics,
        "log_persistence": "stdout-only",
    }


@contextmanager
def _suppressed_dependency_output(
    diagnostics: list[dict[str, Any]],
    stage: str,
) -> Iterator[None]:
    stdout = _BoundedSink()
    stderr = _BoundedSink()
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            yield
    except SystemExit:
        raise _DependencyFailure(stage, "SystemExit") from None
    except Exception as exc:
        raise _DependencyFailure(stage, exc.__class__.__name__) from None
    finally:
        diagnostics.append(
            {
                "stage": stage,
                "output_suppressed": True,
                "stdout_characters": stdout.characters,
                "stderr_characters": stderr.characters,
                "capture_limit": _DIAGNOSTIC_CAPTURE_LIMIT,
                "truncated": (
                    stdout.characters > stdout.captured
                    or stderr.characters > stderr.captured
                ),
            }
        )


def _safe_error(exc: BaseException) -> str:
    return " ".join((str(exc).strip() or exc.__class__.__name__).splitlines())


__all__ = ["SERVICE_SCHEMA", "run_evaluator_service"]
