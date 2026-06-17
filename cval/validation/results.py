"""Structured validation result schema helpers.

Validation pods write one `cval.results.v1` JSON artifact per run. This module
validates that artifact and converts it to legacy env-style status variables
used by `db-update.sh`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


VALID_STATUSES = {"pass", "fail", "incomplete"}


@dataclass(frozen=True)
class TestResult:
    """Result for one validation layer: storage, NCCL, or DL test."""

    name: str
    status: str
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


RESULT_ENV_KEYS = {
    "storage": "GCRRESULT1",
    "nccl": "GCRRESULT2",
    "dltest": "GCRRESULT3",
}


def load_validation_result(path: Path) -> ValidationResult:
    """Load and validate a structured result JSON file from disk."""

    return parse_validation_result(json.loads(path.read_text(encoding="utf-8")))


def parse_validation_result(payload: dict[str, Any]) -> ValidationResult:
    """Validate raw result JSON and return a typed result object."""

    if payload.get("schema_version") != "cval.results.v1":
        raise ValueError("Unsupported c-val result schema_version")
    tests_raw = payload.get("tests")
    if not isinstance(tests_raw, dict):
        raise ValueError("c-val result payload must contain a tests object")

    tests: dict[str, TestResult] = {}
    for name in ("storage", "nccl", "dltest"):
        raw = tests_raw.get(name)
        # All three validation layers are required so aggregate status is meaningful.
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
    # Prevent result JSON from claiming all-pass when any component failed.
    if overall != expected_overall:
        raise ValueError(f"overall must be {expected_overall!r} for the provided tests")

    return ValidationResult(
        schema_version="cval.results.v1",
        node=str(payload.get("node", "")),
        timestamp=str(payload.get("timestamp", "")),
        image_name=str(payload.get("image_name", "")),
        overall=overall,
        tests=tests,
    )


def validation_result_to_env(result: ValidationResult) -> dict[str, str]:
    """Convert structured result statuses to legacy GCRRESULT variables."""

    values = {
        env_key: result.tests[test_name].status
        for test_name, env_key in RESULT_ENV_KEYS.items()
    }
    values["overall_result"] = result.overall
    values["image_name"] = result.image_name
    return values


def validation_result_to_env_lines(result: ValidationResult) -> list[str]:
    """Render env-style lines consumed by shell process substitution."""

    return [f"{key}={value}" for key, value in validation_result_to_env(result).items()]