"""Current built-in validation names and runtime projections.

The generic runner is registry-driven.  These constants exist only for the
three built-in tests whose established environment variables, log markers,
and raw SQLite rows are still consumed by current jobs and historical result
readers.  This module intentionally contains no inventory, audit, or removal
policy machinery.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class BuiltinTestProjection:
    """One built-in test's established environment and marker projection."""

    test_id: str
    result_env: str
    enabled_env: str
    completion_marker: str = ""
    failure_marker: str = ""
    running_marker: str = ""
    skipped_marker: str = ""


BUILTIN_TEST_PROJECTIONS = (
    BuiltinTestProjection(
        "storage",
        "GCRRESULT1",
        "RUN_STORAGE",
        "Storage test is complete.",
        "Storage test FAILED.",
        skipped_marker="Storage test SKIPPED (disabled by config).",
    ),
    BuiltinTestProjection(
        "nccl",
        "GCRRESULT2",
        "RUN_NCCL",
        "NCCL test is complete.",
        "NCCL test FAILED.",
        skipped_marker="NCCL test SKIPPED (disabled by config).",
    ),
    BuiltinTestProjection(
        "dltest",
        "GCRRESULT3",
        "RUN_DLTEST",
        running_marker="Running DL Test...",
        skipped_marker="DL Test SKIPPED (disabled by config).",
    ),
)
BUILTIN_TEST_IDS = tuple(item.test_id for item in BUILTIN_TEST_PROJECTIONS)
BUILTIN_AGGREGATE_TEST_ID = "all"
BUILTIN_STATUS_TEST_IDS = BUILTIN_TEST_IDS + (BUILTIN_AGGREGATE_TEST_ID,)
BUILTIN_RESULT_ENV = MappingProxyType(
    {item.test_id: item.result_env for item in BUILTIN_TEST_PROJECTIONS}
)
BUILTIN_ENABLE_ENV = MappingProxyType(
    {item.test_id: item.enabled_env for item in BUILTIN_TEST_PROJECTIONS}
)
BUILTIN_DONE_MARKERS = MappingProxyType(
    {
        item.test_id: (item.completion_marker, item.failure_marker)
        for item in BUILTIN_TEST_PROJECTIONS
        if item.completion_marker
    }
)
BUILTIN_RUNNING_MARKERS = MappingProxyType(
    {
        item.test_id: item.running_marker
        for item in BUILTIN_TEST_PROJECTIONS
        if item.running_marker
    }
)
BUILTIN_SKIPPED_MARKERS = MappingProxyType(
    {
        item.test_id: item.skipped_marker
        for item in BUILTIN_TEST_PROJECTIONS
        if item.skipped_marker
    }
)
BUILTIN_FINAL_RESULT_PREFIX = "Final c-val test results:"
BUILTIN_DB_UPDATE_DONE_MARKER = "Main DB update completed."

BUILTIN_TEST_EVIDENCE_ENV = MappingProxyType(
    {
        "storage": MappingProxyType(
            {
                "run_dir": "STORAGE_RUN_DIR",
                "artifacts": "STORAGE_OUTPUT_DIR",
                "log": "STORAGE_LOG_FILE",
                "summary": "STORAGE_SUMMARY_FILE",
            }
        ),
        "nccl": MappingProxyType(
            {
                "run_dir": "NCCL_RUN_DIR",
                "artifacts": "NCCL_OUTPUT_DIR",
                "log": "NCCL_LOG_FILE",
                "summary": "NCCL_SUMMARY_FILE",
                "ibbw_log": "NCCL_IBBW_LOG_FILE",
            }
        ),
        "dltest": MappingProxyType(
            {
                "run_dir": "DLTEST_RUN_DIR",
                "artifacts": "DLTEST_OUTPUT_DIR",
                "log": "DLTEST_LOG_FILE",
                "summary": "DLTEST_SUMMARY_FILE",
            }
        ),
    }
)

RESULT_PROJECTION_KEYS = MappingProxyType(
    {
        "overall": "overall_result",
        "image_name": "image_name",
        "pytorch_version": "pytorch_version",
        "cuda_version": "cuda_version",
        "node": "result_node",
        "timestamp": "result_timestamp",
        "run_id": "result_run_id",
        "schema_version": "result_schema_version",
        "global_config_digest": "result_global_config_digest",
        "digest": "result_digest",
        "storage_artifacts": "result_storage_artifacts",
        "nccl_summary": "result_nccl_summary",
    }
)

DEFAULT_TEST_REGISTRATIONS = MappingProxyType(
    {
        item.test_id: MappingProxyType(
            {
                "enabled": True,
                "config_path": f"validation-tests/{item.test_id}/test_config.toml",
            }
        )
        for item in BUILTIN_TEST_PROJECTIONS
    }
)

BUILTIN_RUNTIME_SETTING_DEFAULTS = MappingProxyType(
    {
        "storage": MappingProxyType({"install_fio": True}),
        "nccl": MappingProxyType(
            {
                "gpu_count": 8,
                "iterations": 20,
                "data_size_gb": 8,
                "ibbw_enabled": True,
                "ibbw_start_device": None,
                "ibbw_end_device": None,
                "net": "IB",
                "p2p_disable": True,
                "shm_disable": True,
                "debug": "INFO",
            }
        ),
        "dltest": MappingProxyType(
            {"gpu_count": 8, "test_plan": "80gb-example", "iterations": 100}
        ),
    }
)

def project_builtin_statuses(projected_environment: Mapping[str, str]) -> dict[str, str]:
    """Project current built-in status rows from a validated result."""

    projected = {
        item.test_id: projected_environment[item.result_env]
        for item in BUILTIN_TEST_PROJECTIONS
    }
    projected[BUILTIN_AGGREGATE_TEST_ID] = projected_environment["overall_result"]
    return projected


__all__ = [
    "BUILTIN_AGGREGATE_TEST_ID",
    "BUILTIN_DB_UPDATE_DONE_MARKER",
    "BUILTIN_DONE_MARKERS",
    "BUILTIN_ENABLE_ENV",
    "BUILTIN_FINAL_RESULT_PREFIX",
    "BUILTIN_RESULT_ENV",
    "BUILTIN_RUNNING_MARKERS",
    "BUILTIN_RUNTIME_SETTING_DEFAULTS",
    "BUILTIN_SKIPPED_MARKERS",
    "BUILTIN_STATUS_TEST_IDS",
    "BUILTIN_TEST_EVIDENCE_ENV",
    "BUILTIN_TEST_IDS",
    "BUILTIN_TEST_PROJECTIONS",
    "DEFAULT_TEST_REGISTRATIONS",
    "RESULT_PROJECTION_KEYS",
    "project_builtin_statuses",
]