"""Structured validation result schema helpers.

Validation pods write one canonical ``cval.results`` JSON artifact per run.
Strict historical ``cval.results.v1`` and ``cval.results.v2`` readers remain so
existing evidence is never rewritten.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from cval.validation.builtins import (
    BUILTIN_ENABLE_ENV,
    BUILTIN_RESULT_ENV,
    RESULT_PROJECTION_KEYS,
    BUILTIN_TEST_IDS,
)


VALID_STATUSES = {"pass", "fail", "incomplete"}
VALID_V2_PHASES = {
    "not_selected",
    "pending",
    "setup",
    "running",
    "finished",
    "setup_failed",
    "timed_out",
    "interrupted",
    "framework_error",
}
TERMINAL_V2_PHASES = {
    "not_selected",
    "finished",
    "setup_failed",
    "timed_out",
    "interrupted",
    "framework_error",
}
TEST_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")


@dataclass(frozen=True)
class TestResult:
    """Result for one validation layer: storage, NCCL, or DL test."""

    name: str
    status: str
    enabled: bool = True
    log: str = ""
    summary: str = ""


@dataclass(frozen=True)
class ValidationResult:
    """Parsed structured result for one node validation run."""

    schema_version: str
    node: str
    timestamp: str
    overall: str
    tests: dict[str, TestResult]
    image_name: str = ""
    pytorch_version: str = ""
    cuda_version: str = ""


@dataclass(frozen=True)
class TestResultV2:
    """Execution state and artifact paths for one registered current test."""

    display_name: str
    enabled: bool
    selected: bool
    order: int
    status: str
    phase: str
    started_at: str | None
    completed_at: str | None
    duration_ms: int | None
    exit_code: int | None
    config_path: str
    config_digest: str
    stdout: str
    stderr: str
    log: str
    summary: str
    result: str
    artifacts: str
    message: str


@dataclass(frozen=True)
class ValidationResultV2:
    """Parsed canonical dynamic result envelope for one validation run."""

    schema_version: str
    run_id: str
    node: str
    timestamp: int
    timestamp_la: str
    generated_at: str
    completed_at: str | None
    overall: str
    image_name: str
    pytorch_version: str
    cuda_version: str
    git_ref: str
    global_config_digest: str
    tests: dict[str, TestResultV2]
    errors: list[dict[str, Any]]


ValidationResultLike = ValidationResult | ValidationResultV2


RESULT_ENV_KEYS = BUILTIN_RESULT_ENV
RESULT_ENABLED_ENV_KEYS = BUILTIN_ENABLE_ENV


def load_validation_result(path: Path) -> ValidationResultLike:
    """Load and validate a structured result JSON file from disk."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("c-val result payload must be a JSON object")
    if payload.get("schema_version") in {"cval.results", "cval.results.v2"}:
        return parse_validation_result_v2(payload)
    return parse_validation_result(payload)


def parse_validation_result(payload: dict[str, Any]) -> ValidationResult:
    """Validate raw result JSON and return a typed result object."""

    if payload.get("schema_version") != "cval.results.v1":
        raise ValueError("Unsupported c-val result schema_version")
    tests_raw = payload.get("tests")
    if not isinstance(tests_raw, dict):
        raise ValueError("c-val result payload must contain a tests object")

    tests: dict[str, TestResult] = {}
    for name in BUILTIN_TEST_IDS:
        raw = tests_raw.get(name)
        # All three validation layers are required so aggregate status is meaningful.
        if not isinstance(raw, dict):
            raise ValueError(f"Missing test result for {name}")
        status = str(raw.get("status", "fail"))
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status for {name}: {status}")
        enabled_raw = raw.get("enabled", True)
        if not isinstance(enabled_raw, bool):
            raise ValueError(f"enabled for {name} must be a JSON boolean")
        enabled = enabled_raw
        if not enabled and status != "incomplete":
            raise ValueError(f"Disabled test {name} must have status 'incomplete'")
        tests[name] = TestResult(
            name=name,
            status=status,
            enabled=enabled,
            log=str(raw.get("log", "")),
            summary=str(raw.get("summary", "")),
        )

    overall = str(payload.get("overall", "fail"))
    enabled_tests = [test for test in tests.values() if test.enabled]
    if not enabled_tests:
        expected_overall = "incomplete"
    else:
        expected_overall = (
            "pass" if all(test.status == "pass" for test in enabled_tests) else "fail"
        )
    # Prevent result JSON from claiming all-pass when any component failed.
    if overall != expected_overall:
        raise ValueError(f"overall must be {expected_overall!r} for the provided tests")

    return ValidationResult(
        schema_version="cval.results.v1",
        node=str(payload.get("node", "")),
        timestamp=str(payload.get("timestamp", "")),
        image_name=str(payload.get("image_name", "")),
        pytorch_version=str(payload.get("pytorch_version", "")),
        cuda_version=str(payload.get("cuda_version", "")),
        overall=overall,
        tests=tests,
    )


def parse_validation_result_v2(payload: dict[str, Any]) -> ValidationResultV2:
    """Validate canonical or historical-v2 dynamic result envelopes."""

    allowed_top_level = {
        "schema_version",
        "run_id",
        "node",
        "timestamp",
        "timestamp_la",
        "generated_at",
        "completed_at",
        "overall",
        "image_name",
        "pytorch_version",
        "cuda_version",
        "git_ref",
        "global_config_digest",
        "tests",
        "errors",
    }
    schema_version = payload.get("schema_version")
    _reject_unknown(payload, allowed_top_level, "cval.results")
    if schema_version not in {"cval.results", "cval.results.v2"}:
        raise ValueError("Unsupported c-val result schema_version")

    run_id = _required_str(payload, "run_id")
    if not RUN_ID_PATTERN.fullmatch(run_id) or run_id in {".", ".."}:
        raise ValueError("run_id must be a safe path segment")
    node = _required_str(payload, "node")
    if not RUN_ID_PATTERN.fullmatch(node) or node in {".", ".."}:
        raise ValueError("node must be a safe path segment")
    timestamp = _required_int(payload, "timestamp")
    if timestamp < 0:
        raise ValueError("timestamp must be non-negative")
    timestamp_la = _required_str(payload, "timestamp_la")
    generated_at = _required_str(payload, "generated_at")
    completed_at = _nullable_str(payload, "completed_at")
    _validate_timestamp(timestamp_la, "timestamp_la")
    _validate_timestamp(generated_at, "generated_at")
    if completed_at is not None:
        _validate_timestamp(completed_at, "completed_at")
    if int(_parse_timestamp(timestamp_la).timestamp()) != timestamp:
        raise ValueError("timestamp_la must represent timestamp")

    overall = _required_str(payload, "overall")
    if overall not in VALID_STATUSES:
        raise ValueError(f"Invalid current overall status: {overall}")
    global_config_digest = _required_str(payload, "global_config_digest")
    if not DIGEST_PATTERN.fullmatch(global_config_digest):
        raise ValueError("global_config_digest must be a sha256 digest")

    tests_raw = payload.get("tests")
    if not isinstance(tests_raw, dict) or not tests_raw:
        raise ValueError("cval.results tests must be a non-empty object")
    tests: dict[str, TestResultV2] = {}
    selected_orders: dict[int, str] = {}
    for test_id, raw in tests_raw.items():
        if not isinstance(test_id, str) or not TEST_ID_PATTERN.fullmatch(test_id):
            raise ValueError(f"Invalid current test ID: {test_id!r}")
        if not isinstance(raw, dict):
            raise ValueError(f"current test {test_id!r} must be an object")
        test = _parse_v2_test(test_id, raw)
        if test.selected:
            if test.order in selected_orders:
                raise ValueError(
                    "Selected current tests must have unique order: "
                    f"{selected_orders[test.order]!r} and {test_id!r}"
                )
            selected_orders[test.order] = test_id
        tests[test_id] = test

    participating = [test for test in tests.values() if test.enabled and test.selected]
    if not participating:
        expected_overall = "incomplete"
    elif any(test.status == "fail" for test in participating):
        expected_overall = "fail"
    elif all(test.status == "pass" for test in participating):
        expected_overall = "pass"
    else:
        expected_overall = "incomplete"
    if overall != expected_overall:
        raise ValueError(
            f"current overall must be {expected_overall!r} for the provided tests"
        )

    run_terminal = all(test.phase in TERMINAL_V2_PHASES for test in participating)
    if run_terminal != (completed_at is not None):
        raise ValueError(
            "completed_at must be set exactly when every selected test is terminal"
        )
    if completed_at is not None:
        run_completed = _parse_timestamp(completed_at)
        for test_id, test in tests.items():
            if test.completed_at and _parse_timestamp(test.completed_at) > run_completed:
                raise ValueError(
                    f"run completed_at precedes test {test_id!r} completion"
                )

    errors_raw = payload.get("errors")
    if not isinstance(errors_raw, list):
        raise ValueError("cval.results errors must be an array")
    errors = [_parse_v2_error(raw, index) for index, raw in enumerate(errors_raw)]

    return ValidationResultV2(
        schema_version=str(schema_version),
        run_id=run_id,
        node=node,
        timestamp=timestamp,
        timestamp_la=timestamp_la,
        generated_at=generated_at,
        completed_at=completed_at,
        overall=overall,
        image_name=_string(payload, "image_name"),
        pytorch_version=_string(payload, "pytorch_version"),
        cuda_version=_string(payload, "cuda_version"),
        git_ref=_string(payload, "git_ref"),
        global_config_digest=global_config_digest,
        tests=tests,
        errors=errors,
    )


def validation_result_to_env(result: ValidationResultLike) -> dict[str, str]:
    """Convert structured result statuses to legacy GCRRESULT variables."""

    values = {
        env_key: (
            result.tests[test_name].status
            if test_name in result.tests
            else "incomplete"
        )
        for test_name, env_key in RESULT_ENV_KEYS.items()
    }
    values.update(
        {
            env_key: str(
                result.tests[test_name].enabled
                if test_name in result.tests
                else False
            ).lower()
            for test_name, env_key in RESULT_ENABLED_ENV_KEYS.items()
        }
    )
    keys = RESULT_PROJECTION_KEYS
    values[keys["overall"]] = result.overall
    values[keys["image_name"]] = result.image_name
    values[keys["pytorch_version"]] = result.pytorch_version
    values[keys["cuda_version"]] = result.cuda_version
    values[keys["node"]] = result.node
    values[keys["timestamp"]] = str(result.timestamp)
    values[keys["run_id"]] = (
        result.run_id
        if isinstance(result, ValidationResultV2)
        else f"{result.node}-{result.timestamp}"
    )
    values[keys["schema_version"]] = result.schema_version
    values[keys["global_config_digest"]] = (
        result.global_config_digest
        if isinstance(result, ValidationResultV2)
        else ""
    )
    values[keys["digest"]] = validation_result_digest(result)
    values[keys["storage_artifacts"]] = (
        result.tests["storage"].artifacts
        if isinstance(result, ValidationResultV2) and "storage" in result.tests
        else ""
    )
    values[keys["nccl_summary"]] = (
        result.tests["nccl"].summary
        if isinstance(result, ValidationResultV2) and "nccl" in result.tests
        else ""
    )
    return values


def validation_result_v2_digest(result: ValidationResultV2) -> str:
    """Return a canonical SHA-256 digest over the complete validated v2 result."""

    payload = json.dumps(
        asdict(result),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def validation_result_digest(result: ValidationResultLike) -> str:
    """Return the canonical immutable digest for either supported result schema."""

    if isinstance(result, ValidationResultV2):
        return validation_result_v2_digest(result)
    payload = json.dumps(
        asdict(result),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def validation_result_to_env_lines(result: ValidationResultLike) -> list[str]:
    """Render env-style lines consumed by shell process substitution."""

    values = validation_result_to_env(result)
    for key, value in values.items():
        if any(character in value for character in ("\x00", "\r", "\n")):
            raise ValueError(f"Result projection value contains a control character: {key}")
    return [f"{key}={value}" for key, value in values.items()]


def _parse_v2_test(test_id: str, raw: dict[str, Any]) -> TestResultV2:
    allowed = {
        "display_name",
        "enabled",
        "selected",
        "order",
        "status",
        "phase",
        "started_at",
        "completed_at",
        "duration_ms",
        "exit_code",
        "config_path",
        "config_digest",
        "stdout",
        "stderr",
        "log",
        "summary",
        "result",
        "artifacts",
        "message",
    }
    _reject_unknown(raw, allowed, f"current test {test_id!r}")
    enabled = _required_bool(raw, "enabled", test_id)
    selected = _required_bool(raw, "selected", test_id)
    if selected and not enabled:
        raise ValueError(f"Disabled current test {test_id!r} cannot be selected")
    order = _required_int(raw, "order")
    if order < 0:
        raise ValueError(f"current test {test_id!r} order must be non-negative")
    status = _required_str(raw, "status")
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid v2 status for {test_id}: {status}")
    phase = _required_str(raw, "phase")
    if phase not in VALID_V2_PHASES:
        raise ValueError(f"Invalid v2 phase for {test_id}: {phase}")
    if not selected and (status != "incomplete" or phase != "not_selected"):
        raise ValueError(
            f"Non-selected current test {test_id!r} must be incomplete/not_selected"
        )

    started_at = _nullable_str(raw, "started_at")
    completed_at = _nullable_str(raw, "completed_at")
    if started_at is not None:
        _validate_timestamp(started_at, f"{test_id}.started_at")
    if completed_at is not None:
        _validate_timestamp(completed_at, f"{test_id}.completed_at")
    if completed_at is not None and started_at is None:
        raise ValueError(f"current test {test_id!r} completed without starting")
    if started_at and completed_at:
        if _parse_timestamp(completed_at) < _parse_timestamp(started_at):
            raise ValueError(f"current test {test_id!r} completed before it started")

    duration_ms = _nullable_int(raw, "duration_ms")
    if duration_ms is not None and duration_ms < 0:
        raise ValueError(f"current test {test_id!r} duration_ms must be non-negative")
    exit_code = _nullable_int(raw, "exit_code")

    if phase == "not_selected":
        if selected or status != "incomplete":
            raise ValueError(
                f"not_selected current test {test_id!r} must be unselected/incomplete"
            )
        if any(
            value is not None
            for value in (started_at, completed_at, duration_ms, exit_code)
        ):
            raise ValueError(
                f"Non-selected current test {test_id!r} cannot have execution state"
            )
    elif phase == "pending":
        if any(
            value is not None
            for value in (started_at, completed_at, duration_ms, exit_code)
        ):
            raise ValueError(f"Pending current test {test_id!r} has impossible state")
        if status != "incomplete":
            raise ValueError(f"Pending current test {test_id!r} must be incomplete")
    elif phase in {"setup", "running"}:
        if started_at is None or completed_at is not None:
            raise ValueError(f"Active current test {test_id!r} needs only started_at")
        if status != "incomplete" or duration_ms is not None or exit_code is not None:
            raise ValueError(f"Active current test {test_id!r} has terminal values")
    elif phase == "finished":
        if status not in {"pass", "fail"} or completed_at is None:
            raise ValueError(f"Finished current test {test_id!r} needs terminal status/time")
        if status == "pass" and exit_code != 0:
            raise ValueError(f"Passing current test {test_id!r} must have exit_code 0")
        if status == "fail" and (exit_code is None or exit_code == 0):
            raise ValueError(f"Failed current test {test_id!r} needs non-zero exit_code")
    elif phase in {"setup_failed", "timed_out", "framework_error"}:
        if status != "fail" or completed_at is None:
            raise ValueError(f"current test {test_id!r} {phase} must be failed/terminal")
        if phase == "setup_failed" and (exit_code is None or exit_code == 0):
            raise ValueError(
                f"setup_failed current test {test_id!r} needs non-zero exit_code"
            )
        if phase == "timed_out" and exit_code == 0:
            raise ValueError(f"timed_out current test {test_id!r} cannot have exit_code 0")
        if phase == "framework_error" and exit_code == 0:
            raise ValueError(
                f"framework_error current test {test_id!r} cannot have exit_code 0"
            )
    elif phase == "interrupted":
        if status != "incomplete" or completed_at is None:
            raise ValueError(f"Interrupted current test {test_id!r} must be incomplete")
    if phase in TERMINAL_V2_PHASES - {"not_selected"} and duration_ms is None:
        raise ValueError(f"Terminal current test {test_id!r} requires duration_ms")

    config_digest = _required_str(raw, "config_digest")
    if not DIGEST_PATTERN.fullmatch(config_digest):
        raise ValueError(f"current test {test_id!r} config_digest must be sha256")
    path_values = {
        "config_path": _required_str(raw, "config_path"),
        **{
            key: _string(raw, key)
            for key in (
                "stdout",
                "stderr",
                "log",
                "summary",
                "result",
                "artifacts",
            )
        },
    }
    for key, value in path_values.items():
        if "\x00" in value:
            raise ValueError(f"current test {test_id!r} {key} contains NUL")
    if selected:
        missing_paths = [key for key, value in path_values.items() if not value]
        if missing_paths:
            raise ValueError(
                f"Selected current test {test_id!r} has empty paths: "
                f"{', '.join(missing_paths)}"
            )

    return TestResultV2(
        display_name=_required_str(raw, "display_name"),
        enabled=enabled,
        selected=selected,
        order=order,
        status=status,
        phase=phase,
        started_at=started_at,
        completed_at=completed_at,
        duration_ms=duration_ms,
        exit_code=exit_code,
        config_path=path_values["config_path"],
        config_digest=config_digest,
        stdout=path_values["stdout"],
        stderr=path_values["stderr"],
        log=path_values["log"],
        summary=path_values["summary"],
        result=path_values["result"],
        artifacts=path_values["artifacts"],
        message=_string(raw, "message"),
    )


def _parse_v2_error(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"v2 errors[{index}] must be an object")
    allowed = {"code", "message", "test_id", "timestamp", "detail_path"}
    _reject_unknown(raw, allowed, f"v2 errors[{index}]")
    test_id = raw.get("test_id")
    if test_id is not None and (
        not isinstance(test_id, str) or not TEST_ID_PATTERN.fullmatch(test_id)
    ):
        raise ValueError(f"v2 errors[{index}].test_id is invalid")
    timestamp = _required_str(raw, "timestamp")
    _validate_timestamp(timestamp, f"errors[{index}].timestamp")
    return {
        "code": _required_str(raw, "code"),
        "message": _required_str(raw, "message"),
        "test_id": test_id,
        "timestamp": timestamp,
        "detail_path": _string(raw, "detail_path"),
    }


def _reject_unknown(data: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ValueError(f"Unknown field(s) in {where}: {', '.join(unknown)}")


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _string(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def _nullable_str(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string or null")
    return value


def _required_bool(data: dict[str, Any], key: str, test_id: str) -> bool:
    value = data.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"current test {test_id!r} {key} must be a boolean")
    return value


def _required_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _nullable_int(data: dict[str, Any], key: str) -> int | None:
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer or null")
    return value


def _validate_timestamp(value: str, field_name: str) -> None:
    try:
        parsed = _parse_timestamp(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")


def _parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(normalized)