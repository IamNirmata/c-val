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
from cval.validation.plugins import TestEnvironmentContext, load_registered_plugin

if TYPE_CHECKING:
    from cval.config import CvalConfig
    from cval.validation.registry import RegisteredValidationTest


ENVIRONMENT_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def build_runtime_environment(config: CvalConfig) -> dict[str, str]:
    """Return the shared registry context plus test-owned runtime variables."""

    from cval.validation.plugins import validate_registry_plugins

    registry = config.tests.registry
    validate_registry_plugins(registry.enabled)
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
        "CVAL_VALIDATION_DB_PATH": config.storage.validation_db_path,
    }
    for test in registry.tests:
        declaration = test.definition.plugin
        if declaration is None or "runtime" not in declaration.capabilities:
            continue
        projected = load_registered_plugin(test).runtime_environment(test, config)
        if not isinstance(projected, dict) or set(projected) & set(values):
            raise ValueError(f"Test {test.id} runtime environment overrides another owner")
        values.update(projected)
    _validate_environment(values)
    return values


def test_evidence_environment(test: RegisteredValidationTest, *, log_dir: Path, run_dir: Path,
                              artifacts: Path, node: str, timestamp: int,
                              validation_root: Path, run_id: str) -> dict[str, str]:
    declaration = test.definition.plugin
    if declaration is None or "runtime" not in declaration.capabilities:
        return {}
    context = TestEnvironmentContext(test, log_dir, run_dir, artifacts, node, timestamp, validation_root, run_id)
    values = load_registered_plugin(test).evidence_environment(context)
    if not isinstance(values, dict):
        raise ValueError(f"Test {test.id} evidence environment must be a mapping")
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
