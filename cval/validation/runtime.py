"""Generic in-pod runtime context for validation jobs.

The Volcano manifest carries a small fixed environment plus one base64-encoded,
shell-quoted runtime payload. This keeps test-specific settings out of
Kubernetes YAML while built-in shell workloads consume established variables.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import shlex
from pathlib import Path
from typing import TYPE_CHECKING

from cval.config import REPO_ROOT, config_to_dict, encode_config_snapshot
from cval.validation.builtins import (
    BUILTIN_ENABLE_ENV,
    BUILTIN_TEST_EVIDENCE_ENV,
)

if TYPE_CHECKING:
    from cval.config import CvalConfig
    from cval.validation.registry import RegisteredValidationTest


ENVIRONMENT_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def build_runtime_environment(config: CvalConfig) -> dict[str, str]:
    """Return the generic registry context plus current built-in variables."""

    from cval.validation.plugins import validate_registry_plugins

    registry = config.tests.registry
    validate_registry_plugins(registry.enabled)
    storage = registry.get("storage")
    nccl = registry.get("nccl")
    dltest = registry.get("dltest")
    registrations = {
        test.id: {
            "enabled": test.enabled,
            "config_path": test.config_path,
            "order": test.definition.metadata.order,
        }
        for test in registry.tests
    }
    values = {
        "CVAL_REPO_DIR": config.runtime.repo_dir,
        "CVAL_VALIDATION_ROOT": config.runtime.validation_root,
        "CVAL_IMAGE_NAME": config.job.image_name,
        "CVAL_GIT_REPO": config.job.git_repo,
        "CVAL_GIT_REF": config.job.git_ref,
        "CVAL_CONFIG_PATH": f"{config.runtime.repo_dir.rstrip('/')}/config/cval.toml",
        "CVAL_CONFIG_DIGEST": effective_config_digest(config),
        "CVAL_CONFIG_SNAPSHOT_B64": encode_config_snapshot(config),
        "CVAL_ENABLED_TESTS": ",".join(test.id for test in registry.enabled),
        "CVAL_TEST_REGISTRY_JSON": json.dumps(
            registrations, sort_keys=True, separators=(",", ":")
        ),
        "CVAL_VALIDATION_TESTS_DIR": config.runtime.validation_tests_dir,
        "CVAL_DL_UNIT_TEST_DIR": config.runtime.dl_unit_test_dir,
        "CVAL_VALIDATION_DB_PATH": config.storage.validation_db_path,
        "CVAL_STORAGE_DB_PATH": config.storage.storage_db_path,
        "CVAL_NCCL_DB_PATH": config.storage.nccl_db_path,
        "CVAL_DL_NUMERICAL_DB_PATH": config.storage.dl_numerical_db_path,
        "CVAL_DL_COMPUTE_DB_PATH": config.storage.dl_compute_db_path,
        "CVAL_DL_COLLECTIVE_DB_PATH": config.storage.dl_collective_db_path,
        "CVAL_DL_OVERLAP_DB_PATH": config.storage.dl_overlap_db_path,
        "CVAL_DL_METRIC_LOCK_FILE": (
            f"{config.runtime.validation_root.rstrip('/')}/metadata/.dl-metric-ingest.lock"
        ),
        BUILTIN_ENABLE_ENV["storage"]: _shell_bool(bool(storage and storage.enabled)),
        BUILTIN_ENABLE_ENV["nccl"]: _shell_bool(bool(nccl and nccl.enabled)),
        BUILTIN_ENABLE_ENV["dltest"]: _shell_bool(bool(dltest and dltest.enabled)),
    }
    projections = {
        "storage": {"install_fio": "CVAL_STORAGE_INSTALL_FIO"},
        "nccl": {
            "gpu_count": "CVAL_NCCL_GPU_COUNT", "iterations": "CVAL_NCCL_ITERATIONS",
            "data_size_gb": "CVAL_NCCL_DATA_SIZE_GB", "ibbw_enabled": "CVAL_IBBW_ENABLED",
            "ibbw_start_device": "CVAL_IBBW_START_DEVICE", "ibbw_end_device": "CVAL_IBBW_END_DEVICE",
            "net": "CVAL_NCCL_NET", "p2p_disable": "CVAL_NCCL_P2P_DISABLE",
            "shm_disable": "CVAL_NCCL_SHM_DISABLE", "debug": "CVAL_NCCL_DEBUG",
            "ibbw_interval_seconds": "CVAL_IBBW_INTERVAL_SECONDS", "ib_sys_root": "CVAL_IB_SYS_ROOT",
        },
        "dltest": {"gpu_count": "CVAL_DL_GPU_COUNT", "test_plan": "CVAL_DL_TEST_PLAN", "iterations": "CVAL_DL_ITERATIONS"},
    }
    for test_id, names in projections.items():
        registered = registry.get(test_id)
        if registered is None:
            continue
        for setting, name in names.items():
            value = registered.definition.settings[setting]
            values[name] = _shell_bool(value) if isinstance(value, bool) else str(value)
    if storage is not None:
        values["CVAL_STORAGE_SETTINGS_JSON"] = json.dumps(storage.to_dict()["settings"], sort_keys=True, separators=(",", ":"))
    _validate_environment(values)
    return values


def test_evidence_environment(test: RegisteredValidationTest, *, log_dir: Path, run_dir: Path,
                              artifacts: Path, node: str, timestamp: int,
                              validation_root: Path, run_id: str) -> dict[str, str]:
    aliases = BUILTIN_TEST_EVIDENCE_ENV.get(test.id)
    if aliases is None:
        return {}
    values = {
        aliases["run_dir"]: str(run_dir),
        aliases["artifacts"]: str(artifacts),
        aliases["summary"]: str(run_dir / test.definition.artifacts.summary_filename),
        aliases["log"]: str(log_dir / "job.log"),
    }
    if "ibbw_log" in aliases:
        values[aliases["ibbw_log"]] = str(artifacts / f"ibbw-{node}-{timestamp}.log")
    canonical_run = validation_root / "logs" / test.id / node / run_id
    values[f"CVAL_CANONICAL_{aliases['run_dir']}"] = str(canonical_run)
    values[f"CVAL_CANONICAL_{aliases['artifacts']}"] = str(canonical_run)
    if test.id == "nccl":
        values[f"CVAL_CANONICAL_{aliases['summary']}"] = str(canonical_run / test.definition.artifacts.summary_filename)
        values[f"CVAL_CANONICAL_{aliases['ibbw_log']}"] = str(canonical_run / f"ibbw-{node}-{timestamp}.log")
    return values


def encode_runtime_environment(values: dict[str, str]) -> str:
    """Encode deterministic shell exports as a single YAML-safe value."""

    _validate_environment(values)
    lines = [f"export {name}={shlex.quote(values[name])}" for name in sorted(values)]
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def _decode_runtime_environment(payload: str) -> str:
    """Decode one payload for contract tests."""

    try:
        return base64.b64decode(payload, validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Invalid c-val runtime environment payload") from exc


def effective_config_digest(config: CvalConfig) -> str:
    """Return a stable SHA-256 digest of the composed effective configuration."""

    data = config_to_dict(config)
    template_path = config.job.template_path
    try:
        data["job"]["template_path"] = template_path.resolve().relative_to(
            REPO_ROOT.resolve()
        ).as_posix()
    except ValueError:
        data["job"]["template_path"] = str(template_path)
    canonical = json.dumps(
        data, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _shell_bool(value: bool) -> str:
    return "true" if value else "false"


def _validate_environment(values: dict[str, str]) -> None:
    for name, value in values.items():
        if not ENVIRONMENT_NAME_PATTERN.fullmatch(name):
            raise ValueError(f"Invalid runtime environment name: {name!r}")
        if not isinstance(value, str):
            raise ValueError(f"Runtime environment value must be a string: {name}")
        if "\x00" in value:
            raise ValueError(f"Runtime environment value contains NUL: {name}")
