"""Immutable catalog of compatibility surfaces retained during U12.

This module is the single fixed-name allowlist.  It documents, projects, and
audits compatibility behavior without deciding that any surface may be
removed.  Historical readers are permanent; all other removals remain blocked
until U11 live acceptance and an explicitly completed compatibility period.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping

from cval.validation.secure_fs import (
    assert_lexical_directory_identity,
    lexical_absolute,
    open_directory_no_symlinks,
    read_regular_file_at,
)


COMPATIBILITY_CATALOG_VERSION = "cval.compatibility-catalog.v1"
COMPATIBILITY_AUDIT_VERSION = "cval.compatibility-audit.v1"
MAX_AUDIT_INPUTS = 64
MAX_AUDIT_FILE_BYTES = 8 * 1024 * 1024
MAX_AUDIT_TOTAL_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class LegacyTestProjection:
    """One built-in's exact legacy environment and marker projection."""

    test_id: str
    result_env: str
    enabled_env: str
    completion_marker: str = ""
    failure_marker: str = ""
    running_marker: str = ""
    skipped_marker: str = ""


LEGACY_TEST_PROJECTIONS = (
    LegacyTestProjection(
        "storage",
        "GCRRESULT1",
        "RUN_STORAGE",
        "Storage test is complete.",
        "Storage test FAILED.",
        skipped_marker="Storage test SKIPPED (disabled by config).",
    ),
    LegacyTestProjection(
        "nccl",
        "GCRRESULT2",
        "RUN_NCCL",
        "NCCL test is complete.",
        "NCCL test FAILED.",
        skipped_marker="NCCL test SKIPPED (disabled by config).",
    ),
    LegacyTestProjection(
        "dltest",
        "GCRRESULT3",
        "RUN_DLTEST",
        running_marker="Running DL Test...",
        skipped_marker="DL Test SKIPPED (disabled by config).",
    ),
)
LEGACY_TEST_IDS = tuple(item.test_id for item in LEGACY_TEST_PROJECTIONS)
LEGACY_AGGREGATE_TEST_ID = "all"
LEGACY_STATUS_TEST_IDS = LEGACY_TEST_IDS + (LEGACY_AGGREGATE_TEST_ID,)
LEGACY_RESULT_ENV = MappingProxyType(
    {item.test_id: item.result_env for item in LEGACY_TEST_PROJECTIONS}
)
LEGACY_ENABLE_ENV = MappingProxyType(
    {item.test_id: item.enabled_env for item in LEGACY_TEST_PROJECTIONS}
)
LEGACY_DONE_MARKERS = MappingProxyType(
    {
        item.test_id: (item.completion_marker, item.failure_marker)
        for item in LEGACY_TEST_PROJECTIONS
        if item.completion_marker
    }
)
LEGACY_RUNNING_MARKERS = MappingProxyType(
    {
        item.test_id: item.running_marker
        for item in LEGACY_TEST_PROJECTIONS
        if item.running_marker
    }
)
LEGACY_SKIPPED_MARKERS = MappingProxyType(
    {
        item.test_id: item.skipped_marker
        for item in LEGACY_TEST_PROJECTIONS
        if item.skipped_marker
    }
)
LEGACY_FINAL_RESULT_PREFIX = "Final c-val test results:"
LEGACY_DB_UPDATE_DONE_MARKER = "Main DB update completed."

# Exact retained environment/result aliases.  Keep this source-only allowlist
# synchronized with 0-env.sh, db-update.sh, the runner projection, and pinned
# readers.  Prefix-only placeholders are intentionally forbidden here: the
# compatibility audit must identify complete historical names.
LEGACY_TEST_EVIDENCE_ENV = MappingProxyType(
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
LEGACY_RESULT_FILE_ENV = (
    "CVAL_RESULT_DIR",
    "CVAL_RESULT_ENV_FILE",
    "CVAL_RESULT_JSON_FILE",
)
LEGACY_RESULT_PROJECTION_KEYS = MappingProxyType(
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
LEGACY_RUNTIME_ENV_NAMES = tuple(
    dict.fromkeys(
        (
            "GCRNODE",
            "GCRTIME",
            "CVAL_RUN_ID",
            "CVAL_GIT_REPO",
            "CVAL_GIT_REF",
            "CVAL_RUNTIME_ENV_B64",
            "CVAL_IMAGE_NAME",
            "CVAL_PYTORCH_VERSION",
            "CVAL_CUDA_VERSION",
            "CVAL_VALIDATION_ROOT",
            "CVAL_REPO_DIR",
            "CVAL_VALIDATION_TESTS_DIR",
            "CVAL_DL_UNIT_TEST_DIR",
            "CVAL_CONFIG_PATH",
            "CVAL_CONFIG_DIGEST",
            "CVAL_CONFIG_SNAPSHOT_B64",
            "CVAL_ENABLED_TESTS",
            "CVAL_TEST_REGISTRY_JSON",
            "CVAL_VALIDATION_DB_PATH",
            "CVAL_RUN_HISTORY_ENABLED",
            "CVAL_PER_TEST_INGESTION_ENABLED",
            "CVAL_RUN_HISTORY_DB_PATH",
            "CVAL_STORAGE_DB_PATH",
            "CVAL_NCCL_DB_PATH",
            "CVAL_DL_NUMERICAL_DB_PATH",
            "CVAL_DL_COMPUTE_DB_PATH",
            "CVAL_DL_COLLECTIVE_DB_PATH",
            "CVAL_DL_OVERLAP_DB_PATH",
            "CVAL_STORAGE_INSTALL_FIO",
            "CVAL_NCCL_GPU_COUNT",
            "CVAL_NCCL_ITERATIONS",
            "CVAL_NCCL_DATA_SIZE_GB",
            "CVAL_IBBW_ENABLED",
            "CVAL_IBBW_START_DEVICE",
            "CVAL_IBBW_END_DEVICE",
            "CVAL_NCCL_NET",
            "CVAL_NCCL_P2P_DISABLE",
            "CVAL_NCCL_SHM_DISABLE",
            "CVAL_NCCL_DEBUG",
            "CVAL_DL_GPU_COUNT",
            "CVAL_DL_TEST_PLAN",
            "CVAL_DL_ITERATIONS",
            "CVAL_JOB_LOG_DIR",
            "CVAL_TEST_OUTPUT_DIR",
            "CVAL_TEST_LOG_DIR",
            "CVAL_TEST_SUMMARY_FILE",
            "CVAL_ALLOW_LEGACY_RESULT_ENV",
        )
        + tuple(LEGACY_ENABLE_ENV.values())
        + tuple(LEGACY_RESULT_ENV.values())
        + tuple(
            name
            for aliases in LEGACY_TEST_EVIDENCE_ENV.values()
            for name in aliases.values()
        )
        + LEGACY_RESULT_FILE_ENV
        + tuple(LEGACY_RESULT_PROJECTION_KEYS.values())
    )
)

# Current descriptor-anchored supervisor/ingestion protocol.  These exact
# built-in names are cataloged so fixed-name inventory tests cannot silently
# omit them, but they are not legacy compatibility surfaces and are never
# candidates for compatibility cleanup.  Test-ID-derived names for future
# registrations remain dynamic rather than becoming fixed catalog entries.
INTERNAL_RUNTIME_PROTOCOL_NAMES = (
    "CVAL_SECURE_RUN_LAYOUT_JSON",
    "CVAL_SECURE_RUN_FDS",
    "CVAL_EXTERNAL_GLOBAL_LOGGING",
    "CVAL_RUN_MARKER_PREACQUIRED",
    "CVAL_SECURE_STORAGE_RUN_DIR",
    "CVAL_SECURE_STORAGE_ARTIFACTS_DIR",
    "CVAL_SECURE_NCCL_RUN_DIR",
    "CVAL_SECURE_NCCL_ARTIFACTS_DIR",
    "CVAL_SECURE_DLTEST_RUN_DIR",
    "CVAL_SECURE_DLTEST_ARTIFACTS_DIR",
    "CVAL_CANONICAL_JOB_LOG_DIR",
    "CVAL_CANONICAL_RESULT_JSON_FILE",
    "CVAL_CANONICAL_STORAGE_RUN_DIR",
    "CVAL_CANONICAL_STORAGE_OUTPUT_DIR",
    "CVAL_CANONICAL_NCCL_RUN_DIR",
    "CVAL_CANONICAL_NCCL_OUTPUT_DIR",
    "CVAL_CANONICAL_NCCL_SUMMARY_FILE",
    "CVAL_CANONICAL_DLTEST_RUN_DIR",
    "CVAL_CANONICAL_DLTEST_OUTPUT_DIR",
)

DEFAULT_TEST_REGISTRATIONS = MappingProxyType(
    {
        item.test_id: MappingProxyType(
            {
                "enabled": True,
                "config_path": f"validation-tests/{item.test_id}/test_config.toml",
            }
        )
        for item in LEGACY_TEST_PROJECTIONS
    }
)

LEGACY_RUNTIME_SETTING_DEFAULTS = MappingProxyType(
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

# owner, alias, component, refresh group.  This tuple remains byte-for-byte the
# U10 overlay while moving every fixed compatibility name into one module.
COMPATIBILITY_ALIAS_ROWS = (
    ("dltest", "dltest-numerical", "numerical_correctness", "dltest"),
    ("dltest", "dltest-compute", "compute_performance", "dltest"),
    ("dltest", "dltest-collective", "collective_performance", "dltest"),
    ("dltest", "dltest-overlap", "overlap_performance", "dltest"),
)


def project_legacy_statuses(
    projected_environment: Mapping[str, str],
) -> dict[str, str]:
    """Project the fixed compatibility status set from validated result env."""

    projected = {
        item.test_id: projected_environment[item.result_env]
        for item in LEGACY_TEST_PROJECTIONS
    }
    projected[LEGACY_AGGREGATE_TEST_ID] = projected_environment["overall_result"]
    return projected


@dataclass(frozen=True)
class CompatibilitySurface:
    """One retained producer/consumer family and its removal policy."""

    surface_id: str
    category: str
    description: str
    tokens: tuple[str, ...]
    producers: tuple[str, ...]
    consumers: tuple[str, ...]
    historical_reader: bool = False

    @property
    def blockers(self) -> tuple[str, ...]:
        if self.historical_reader:
            return ("historical-reader-retained",)
        return ("u11-live-acceptance", "compatibility-period")

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["blockers"] = list(self.blockers)
        value["token_classification"] = "compatibility-legacy"
        value["legacy_removal_candidate"] = not self.historical_reader
        value["removal_eligible"] = False
        return value


COMPATIBILITY_SURFACES = (
    CompatibilitySurface(
        "default-registrations",
        "registry",
        "Implicit storage/NCCL/DL registrations retained for partial configs.",
        LEGACY_TEST_IDS,
        ("cval/validation/compatibility.py",),
        ("cval/validation/registry.py", "config/cval.toml"),
    ),
    CompatibilitySurface(
        "legacy-runtime-environment",
        "environment",
        "Fixed runtime, result, artifact, log, and summary aliases.",
        LEGACY_RUNTIME_ENV_NAMES,
        (
            "cval/validation/runtime.py",
            "cval/validation/runner.py",
            "validation-tests/0-env.sh",
        ),
        ("validation-tests/db-update.sh", "validation-tests/*/run-test.sh"),
    ),
    CompatibilitySurface(
        "legacy-log-markers",
        "logs",
        "Fixed completion/failure/final markers for pinned log readers.",
        tuple(
            marker
            for item in LEGACY_TEST_PROJECTIONS
            for marker in (
                item.completion_marker,
                item.failure_marker,
                item.running_marker,
                item.skipped_marker,
            )
            if marker
        )
        + (LEGACY_FINAL_RESULT_PREFIX, LEGACY_DB_UPDATE_DONE_MARKER),
        ("cval/validation/runner.py", "validation-tests/db-update.sh"),
        ("cval/orchestrator/validate.py",),
    ),
    CompatibilitySurface(
        "legacy-wrapper-entrypoints",
        "wrappers",
        "Established shell entrypoints that delegate to canonical runners.",
        ("storage.sh", "run-nccl-allreduce.sh", "dltest.sh"),
        ("validation-tests/storage/", "validation-tests/nccl/", "validation-tests/dltest/"),
        ("pinned validation jobs",),
    ),
    CompatibilitySurface(
        "compatibility-cli-and-writers",
        "cli-db",
        "Hidden fixed compatibility ingestion/maintenance commands and metadata writers.",
        (
            "db-add-run-results",
            "db-add-result",
            "db-add-storage-result",
            "db-add-nccl-health",
            "db-rebuild-dltest-metrics",
            "validation.db",
            "test-storage.db",
            "test-nccl.db",
            "dltest_numerical_correctness.db",
            "dltest_compute_performance.db",
            "dltest_collective_performance.db",
            "dltest_overlap_performance.db",
        ),
        ("cval/cli.py", "cval/storage/ingest.py", "validation-tests/db-update.sh"),
        ("status/results/baseline compatibility readers",),
    ),
    CompatibilitySurface(
        "dl-operational-aliases",
        "operator-api",
        "Four established DL component target aliases.",
        tuple(row[1] for row in COMPATIBILITY_ALIAS_ROWS),
        ("cval/validation/compatibility.py",),
        ("CLI, exports, baseline loops, models",),
    ),
    CompatibilitySurface(
        "historical-v1-result-reader",
        "historical-reader",
        "Strict cval.results.v1 parser and read-only projection.",
        ("cval.results.v1", "GCRRESULT1", "GCRRESULT2", "GCRRESULT3"),
        ("historical artifacts",),
        ("cval/validation/results.py",),
        historical_reader=True,
    ),
    CompatibilitySurface(
        "historical-dl-artifact-reader",
        "historical-reader",
        "Legacy DL artifact and summary discovery used for rebuilds.",
        (
            "dltest-summary-",
            "dltest/",
            "OLD_nccl_performance",
            "OLD_nccl_ib_port_performance",
        ),
        ("historical PVC copies",),
        ("cval/storage/dltest_ingest.py", "compatibility SQLite readers"),
        historical_reader=True,
    ),
)


def compatibility_inventory() -> dict[str, object]:
    """Return the deterministic, read-only U12 inventory."""

    return {
        "schema_version": COMPATIBILITY_CATALOG_VERSION,
        "removal_eligible": False,
        "global_blockers": ["u11-live-acceptance", "compatibility-period"],
        "surfaces": [surface.to_dict() for surface in COMPATIBILITY_SURFACES],
        "internal_runtime_protocol": {
            "token_classification": "internal-current-protocol",
            "description": (
                "Descriptor-anchored supervisor controls and canonical path guards."
            ),
            "tokens": list(INTERNAL_RUNTIME_PROTOCOL_NAMES),
            "legacy_removal_candidate": False,
            "removal_eligible": False,
            "blockers": ["current-runtime-protocol"],
        },
    }


def audit_compatibility_inputs(paths: Iterable[str | Path]) -> dict[str, object]:
    """Scan only explicitly named local regular files under fixed bounds."""

    requested = tuple(Path(path).expanduser() for path in paths)
    if not requested:
        raise ValueError("compatibility audit requires at least one explicit --input file")
    if len(requested) > MAX_AUDIT_INPUTS:
        raise ValueError(f"compatibility audit accepts at most {MAX_AUDIT_INPUTS} inputs")

    observations: dict[str, list[dict[str, object]]] = {
        surface.surface_id: [] for surface in COMPATIBILITY_SURFACES
    }
    internal_observations: list[dict[str, object]] = []
    input_rows: list[dict[str, object]] = []
    total = 0
    for requested_path in requested:
        path, payload = _read_bounded_explicit_file(requested_path)
        total += len(payload)
        if total > MAX_AUDIT_TOTAL_BYTES:
            raise ValueError(
                f"compatibility audit total input exceeds {MAX_AUDIT_TOTAL_BYTES} bytes"
            )
        scan = _scan_payload(path, payload)
        input_rows.append(
            {
                "path": str(path),
                "bytes": len(payload),
                "scan_status": scan[0],
                "classification": "unknown" if scan[0] != "scanned" else "scannable",
                "format": scan[1],
                "reason": scan[2],
            }
        )
        if scan[0] != "scanned":
            continue
        text = scan[3]
        for surface in COMPATIBILITY_SURFACES:
            matched = tuple(
                token for token in surface.tokens if _token_is_present(text, token)
            )
            if matched:
                observations[surface.surface_id].append(
                    {"path": str(path), "tokens": list(matched)}
                )
        internal_matched = tuple(
            token
            for token in INTERNAL_RUNTIME_PROTOCOL_NAMES
            if _token_is_present(text, token)
        )
        if internal_matched:
            internal_observations.append(
                {"path": str(path), "tokens": list(internal_matched)}
            )

    surface_rows = []
    for surface in COMPATIBILITY_SURFACES:
        seen = observations[surface.surface_id]
        blockers = list(surface.blockers)
        if seen:
            blockers.append("observed-explicit-input")
        surface_rows.append(
            {
                "surface_id": surface.surface_id,
                "observed": bool(seen),
                "observations": seen,
                "blockers": blockers,
                "token_classification": "compatibility-legacy",
                "legacy_removal_candidate": not surface.historical_reader,
                "removal_eligible": False,
            }
        )
    return {
        "schema_version": COMPATIBILITY_AUDIT_VERSION,
        "offline": True,
        "explicit_inputs_only": True,
        "limits": {
            "max_inputs": MAX_AUDIT_INPUTS,
            "max_file_bytes": MAX_AUDIT_FILE_BYTES,
            "max_total_bytes": MAX_AUDIT_TOTAL_BYTES,
        },
        "inputs": input_rows,
        "total_bytes": total,
        "removal_eligible": False,
        "global_blockers": ["u11-live-acceptance", "compatibility-period"],
        "surfaces": surface_rows,
        "internal_runtime_protocol": {
            "token_classification": "internal-current-protocol",
            "observed": bool(internal_observations),
            "observations": internal_observations,
            "legacy_removal_candidate": False,
            "removal_eligible": False,
            "blockers": ["current-runtime-protocol"],
        },
    }


def _read_bounded_explicit_file(requested: Path) -> tuple[Path, bytes]:
    path = lexical_absolute(requested)
    parent, parent_fd = open_directory_no_symlinks(path.parent)
    try:
        parent_metadata = os.fstat(parent_fd)
        parent_identity = (parent_metadata.st_dev, parent_metadata.st_ino)
        payload = read_regular_file_at(
            parent_fd,
            path.name,
            max_bytes=MAX_AUDIT_FILE_BYTES,
            require_current_owner=True,
            reject_group_world_write=True,
            no_atime=True,
            nonblocking=True,
        )
        retained = os.fstat(parent_fd)
        if (retained.st_dev, retained.st_ino) != parent_identity:
            raise RuntimeError(f"Retained input parent identity changed: {parent}")
        assert_lexical_directory_identity(parent, parent_identity)
        return parent / path.name, payload
    finally:
        os.close(parent_fd)


_TEXT_SUFFIXES = frozenset(
    {
        "",
        ".txt",
        ".log",
        ".md",
        ".sh",
        ".py",
        ".env",
        ".csv",
        ".yml",
        ".yaml",
    }
)
_IDENTIFIER_CHARS = "A-Za-z0-9_.-"


def _scan_payload(path: Path, payload: bytes) -> tuple[str, str, str, str]:
    suffix = path.suffix.lower()
    if suffix not in _TEXT_SUFFIXES | {".json", ".jsonl", ".toml"}:
        return "unscannable", "unsupported", "unsupported-file-type", ""
    if b"\x00" in payload:
        return "unscannable", "binary", "embedded-nul", ""
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return "unscannable", "binary", "invalid-utf8", ""
    if any(ord(character) < 32 and character not in "\n\r\t" for character in text):
        return "unscannable", "binary", "control-characters", ""
    try:
        if suffix == ".json":
            parsed = json.loads(text)
            return "scanned", "json", "", _structured_text(parsed)
        if suffix == ".jsonl":
            values = [json.loads(line) for line in text.splitlines() if line.strip()]
            return "scanned", "jsonl", "", _structured_text(values)
        if suffix == ".toml":
            return "scanned", "toml", "", _structured_text(tomllib.loads(text))
    except (json.JSONDecodeError, tomllib.TOMLDecodeError):
        return "unscannable", suffix.lstrip("."), "malformed-structured-input", ""
    return "scanned", "text", "", text


def _structured_text(value: object) -> str:
    values: list[str] = []

    def visit(item: object) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                values.append(str(key))
                visit(child)
        elif isinstance(item, list | tuple):
            for child in item:
                visit(child)
        elif isinstance(item, str):
            values.append(item)
        elif item is not None:
            values.append(str(item))

    visit(value)
    return "\n".join(values)


def _token_is_present(text: str, token: str) -> bool:
    """Match complete tokens with path separators acting as boundaries."""

    if not token:
        return False
    left_boundary = rf"(?<![{_IDENTIFIER_CHARS}])"
    if token.endswith(("/", "\\")):
        # A retained directory-root token names one complete path component.
        # The separator is part of the token; a preceding separator is also a
        # valid boundary, while an embedded identifier prefix is not.
        pattern = left_boundary + re.escape(token)
        return re.search(pattern, text) is not None
    if token.endswith(("_", "-")):
        # Prefix tokens require a non-empty identifier suffix.  Separators may
        # bound the resulting filename but cannot make an embedded identifier
        # (for example ``mydltest-summary-*``) match.
        pattern = (
            left_boundary
            + re.escape(token)
            + rf"[{_IDENTIFIER_CHARS}]+(?![{_IDENTIFIER_CHARS}])"
        )
        return re.search(pattern, text) is not None
    pattern = (
        left_boundary + re.escape(token) + rf"(?![{_IDENTIFIER_CHARS}])"
    )
    return re.search(pattern, text) is not None


__all__ = [
    "COMPATIBILITY_ALIAS_ROWS",
    "COMPATIBILITY_SURFACES",
    "DEFAULT_TEST_REGISTRATIONS",
    "INTERNAL_RUNTIME_PROTOCOL_NAMES",
    "LEGACY_DB_UPDATE_DONE_MARKER",
    "LEGACY_AGGREGATE_TEST_ID",
    "LEGACY_DONE_MARKERS",
    "LEGACY_ENABLE_ENV",
    "LEGACY_FINAL_RESULT_PREFIX",
    "LEGACY_RESULT_ENV",
    "LEGACY_RESULT_FILE_ENV",
    "LEGACY_RESULT_PROJECTION_KEYS",
    "LEGACY_RUNTIME_ENV_NAMES",
    "LEGACY_RUNTIME_SETTING_DEFAULTS",
    "LEGACY_RUNNING_MARKERS",
    "LEGACY_SKIPPED_MARKERS",
    "LEGACY_STATUS_TEST_IDS",
    "LEGACY_TEST_EVIDENCE_ENV",
    "LEGACY_TEST_PROJECTIONS",
    "LEGACY_TEST_IDS",
    "audit_compatibility_inputs",
    "compatibility_inventory",
    "project_legacy_statuses",
]