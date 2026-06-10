from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


VALID_STATUSES = {"pass", "fail", "incomplete"}


@dataclass(frozen=True)
class TestResult:
    name: str
    status: str
    log: str = ""
    summary: str = ""


@dataclass(frozen=True)
class ValidationResult:
    schema_version: str
    node: str
    timestamp: str
    overall: str
    tests: dict[str, TestResult]


RESULT_ENV_KEYS = {
    "storage": "GCRRESULT1",
    "nccl": "GCRRESULT2",
    "dltest": "GCRRESULT3",
}


def load_validation_result(path: Path) -> ValidationResult:
    return parse_validation_result(json.loads(path.read_text(encoding="utf-8")))


def parse_validation_result(payload: dict[str, Any]) -> ValidationResult:
    if payload.get("schema_version") != "cval.results.v1":
        raise ValueError("Unsupported c-val result schema_version")
    tests_raw = payload.get("tests")
    if not isinstance(tests_raw, dict):
        raise ValueError("c-val result payload must contain a tests object")

    tests: dict[str, TestResult] = {}
    for name in ("storage", "nccl", "dltest"):
        raw = tests_raw.get(name)
        if not isinstance(raw, dict):
            raise ValueError(f"Missing test result for {name}")
        status = str(raw.get("status", "fail"))
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status for {name}: {status}")
        tests[name] = TestResult(
            name=name,
            status=status,
            log=str(raw.get("log", "")),
            summary=str(raw.get("summary", "")),
        )

    overall = str(payload.get("overall", "fail"))
    expected_overall = "pass" if all(test.status == "pass" for test in tests.values()) else "fail"
    if overall != expected_overall:
        raise ValueError(f"overall must be {expected_overall!r} for the provided tests")

    return ValidationResult(
        schema_version="cval.results.v1",
        node=str(payload.get("node", "")),
        timestamp=str(payload.get("timestamp", "")),
        overall=overall,
        tests=tests,
    )


def validation_result_to_env(result: ValidationResult) -> dict[str, str]:
    values = {
        env_key: result.tests[test_name].status
        for test_name, env_key in RESULT_ENV_KEYS.items()
    }
    values["overall_result"] = result.overall
    return values


def validation_result_to_env_lines(result: ValidationResult) -> list[str]:
    return [f"{key}={value}" for key, value in validation_result_to_env(result).items()]