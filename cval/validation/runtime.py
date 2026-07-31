"""Generic in-pod runtime context for validation jobs.

The Volcano manifest carries a small fixed environment plus one base64-encoded,
shell-quoted compatibility payload. This keeps test-specific settings out of
Kubernetes YAML while the v1 shell runner still consumes its existing variables.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import shlex
from typing import TYPE_CHECKING

from cval.config import REPO_ROOT, config_to_dict, encode_config_snapshot
from cval.validation.compatibility import (
    LEGACY_ENABLE_ENV,
    LEGACY_RUNTIME_SETTING_DEFAULTS,
)

if TYPE_CHECKING:
    from cval.config import CvalConfig


ENVIRONMENT_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def build_runtime_environment(config: CvalConfig) -> dict[str, str]:
    """Return the generic registry context plus current v1 compatibility vars."""

    from cval.validation.plugins import validate_registry_plugins

    registry = config.tests.registry
    validate_registry_plugins(registry.enabled)
    storage = registry.get("storage")
    nccl = registry.get("nccl")
    dltest = registry.get("dltest")
    storage_settings = (
        storage.definition.settings
        if storage is not None
        else LEGACY_RUNTIME_SETTING_DEFAULTS["storage"]
    )
    nccl_settings = (
        nccl.definition.settings
        if nccl is not None
        else LEGACY_RUNTIME_SETTING_DEFAULTS["nccl"]
    )
    dltest_settings = (
        dltest.definition.settings
        if dltest is not None
        else LEGACY_RUNTIME_SETTING_DEFAULTS["dltest"]
    )
    registrations = {
        test.id: {
            "enabled": test.enabled,
            "config_path": test.config_path,
            "order": test.definition.metadata.order,
        }
        for test in registry.tests
    }
    values = {
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
        "CVAL_RUN_HISTORY_ENABLED": _shell_bool(
            config.storage.run_history_enabled
        ),
        "CVAL_PER_TEST_INGESTION_ENABLED": _shell_bool(
            config.storage.per_test_ingestion_enabled
        ),
        "CVAL_RUN_HISTORY_DB_PATH": config.storage.run_history_db_path,
        "CVAL_STORAGE_DB_PATH": config.storage.storage_db_path,
        "CVAL_NCCL_DB_PATH": config.storage.nccl_db_path,
        "CVAL_DL_NUMERICAL_DB_PATH": config.storage.dl_numerical_db_path,
        "CVAL_DL_COMPUTE_DB_PATH": config.storage.dl_compute_db_path,
        "CVAL_DL_COLLECTIVE_DB_PATH": config.storage.dl_collective_db_path,
        "CVAL_DL_OVERLAP_DB_PATH": config.storage.dl_overlap_db_path,
        LEGACY_ENABLE_ENV["storage"]: _shell_bool(bool(storage and storage.enabled)),
        "CVAL_STORAGE_INSTALL_FIO": _shell_bool(bool(storage_settings["install_fio"])),
        LEGACY_ENABLE_ENV["nccl"]: _shell_bool(bool(nccl and nccl.enabled)),
        "CVAL_NCCL_GPU_COUNT": str(nccl_settings["gpu_count"]),
        "CVAL_NCCL_ITERATIONS": str(nccl_settings["iterations"]),
        "CVAL_NCCL_DATA_SIZE_GB": str(nccl_settings["data_size_gb"]),
        "CVAL_IBBW_ENABLED": _shell_bool(bool(nccl_settings["ibbw_enabled"])),
        "CVAL_IBBW_START_DEVICE": (
            "" if nccl_settings.get("ibbw_start_device") is None
            else str(nccl_settings["ibbw_start_device"])
        ),
        "CVAL_IBBW_END_DEVICE": (
            "" if nccl_settings.get("ibbw_end_device") is None
            else str(nccl_settings["ibbw_end_device"])
        ),
        "CVAL_NCCL_NET": str(nccl_settings["net"]),
        "CVAL_NCCL_P2P_DISABLE": _shell_bool(bool(nccl_settings["p2p_disable"])),
        "CVAL_NCCL_SHM_DISABLE": _shell_bool(bool(nccl_settings["shm_disable"])),
        "CVAL_NCCL_DEBUG": str(nccl_settings["debug"]),
        LEGACY_ENABLE_ENV["dltest"]: _shell_bool(bool(dltest and dltest.enabled)),
        "CVAL_DL_GPU_COUNT": str(dltest_settings["gpu_count"]),
        "CVAL_DL_TEST_PLAN": str(dltest_settings["test_plan"]),
        "CVAL_DL_ITERATIONS": str(dltest_settings["iterations"]),
    }
    _validate_environment(values)
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
