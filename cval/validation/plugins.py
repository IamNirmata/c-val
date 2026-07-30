"""Trusted repository-local validation adapter protocol and loader.

Adapters are imported only from paths declared by validated ``cval.test.v1``
descriptors.  The framework supplies immutable values and never gives adapters
Kubernetes clients or an unconstrained output path.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from cval.validation.registry import (
    PLUGIN_API_VERSION,
    RegisteredValidationTest,
    ValidationTestDefinition,
    resolve_confined_path,
)


class PluginError(RuntimeError):
    """Base error for validation adapter failures."""


class PluginLoadError(PluginError):
    """Raised when a declared adapter cannot satisfy its frozen contract."""


class IngestionError(PluginError):
    """Raised when an adapter cannot ingest valid current-run evidence."""


class IngestionConflictError(IngestionError):
    """Raised when a run ID is retried with different raw or metric evidence."""


class IngestionDisabledError(IngestionError):
    """Raised when modular per-test persistence has not been activated."""


@dataclass(frozen=True)
class ConfigIssue:
    """One deterministic adapter-owned descriptor validation issue."""

    code: str
    message: str


@dataclass(frozen=True)
class RunContext:
    """Immutable identity and environment for one completed c-val run."""

    run_id: str
    node: str
    started_timestamp: int
    started_timestamp_la: str
    completed_timestamp: int | None
    image_name: str
    pytorch_version: str
    cuda_version: str
    git_ref: str
    global_config_digest: str
    result_digest: str
    validation_root: Path
    result_path: Path


@dataclass(frozen=True)
class TestExecutionResult:
    """Immutable terminal state and confined evidence for one selected test."""

    test_id: str
    status: str
    phase: str
    started_timestamp: int | None
    completed_timestamp: int | None
    duration_ms: int | None
    exit_code: int | None
    result_path: Path
    summary_path: Path
    artifacts_path: Path
    stdout_path: Path
    stderr_path: Path
    log_path: Path
    message: str
    config_digest: str
    raw_result_json: str


@dataclass(frozen=True)
class IngestionContext:
    """Only the validated values an ingestion adapter may need."""

    definition: ValidationTestDefinition
    run: RunContext
    execution: TestExecutionResult
    result_db_path: Path


@dataclass(frozen=True)
class IngestionReceipt:
    """Deterministic result returned by one metric adapter invocation."""

    test_id: str
    run_id: str
    inserted_count: int
    updated_count: int
    metric_names: tuple[str, ...]
    evidence_digest: str
    created_at: int
    message: str = ""


def load_registered_plugin(registered_test: RegisteredValidationTest) -> Any | None:
    """Import and validate one declared adapter from its confined test directory."""

    declaration = registered_test.definition.plugin
    if declaration is None:
        return None
    adapter_path = resolve_confined_path(
        registered_test.test_dir,
        declaration.adapter,
        field_name=f"{registered_test.id} plugin.adapter",
        require_file=True,
    )
    module_token = hashlib.sha256(
        f"{adapter_path}:{adapter_path.stat().st_mtime_ns}:{adapter_path.stat().st_size}".encode(
            "utf-8"
        )
    ).hexdigest()[:20]
    module_name = f"_cval_validation_plugin_{registered_test.id}_{module_token}"
    spec = importlib.util.spec_from_file_location(module_name, adapter_path)
    if spec is None or spec.loader is None:
        raise PluginLoadError(f"Could not create import spec for adapter: {adapter_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 - adapter import boundary
        sys.modules.pop(module_name, None)
        raise PluginLoadError(
            f"Could not import adapter for {registered_test.id!r}: {exc}"
        ) from exc

    if getattr(module, "CVAL_PLUGIN_API", None) != PLUGIN_API_VERSION:
        raise PluginLoadError(
            f"Adapter {adapter_path} must export CVAL_PLUGIN_API={PLUGIN_API_VERSION!r}"
        )
    if "PLUGIN" not in module.__dict__:
        raise PluginLoadError(f"Adapter {adapter_path} must export exactly one PLUGIN object")
    plugin = module.__dict__["PLUGIN"]
    if plugin is None:
        raise PluginLoadError(f"Adapter {adapter_path} exports an empty PLUGIN object")
    if getattr(plugin, "plugin_id", None) != registered_test.id:
        raise PluginLoadError(
            f"Adapter plugin_id must equal registered test ID {registered_test.id!r}"
        )
    capabilities = getattr(plugin, "capabilities", None)
    if not isinstance(capabilities, frozenset) or not all(
        isinstance(capability, str) for capability in capabilities
    ):
        raise PluginLoadError("Adapter capabilities must be a frozenset of strings")
    declared = frozenset(declaration.capabilities)
    if capabilities != declared:
        raise PluginLoadError(
            f"Adapter capabilities {sorted(capabilities)} do not match descriptor "
            f"{sorted(declared)}"
        )

    required_methods: dict[str, tuple[str, ...]] = {
        "config": ("validate_config",),
        "ingest": ("validate_schema", "ingest"),
        "health": ("metric_specs", "load_observations"),
        "export": ("export_rows",),
    }
    for capability in sorted(declared):
        missing = [
            method
            for method in required_methods[capability]
            if not callable(getattr(plugin, method, None))
        ]
        if missing:
            raise PluginLoadError(
                f"Adapter {registered_test.id!r} capability {capability!r} is missing "
                f"method(s): {', '.join(missing)}"
            )
    health = registered_test.definition.health
    if "health" in declared:
        assert health is not None and health.enabled
        policy_version = getattr(plugin, "health_policy_version", None)
        if policy_version != health.policy_version:
            raise PluginLoadError(
                f"Adapter {registered_test.id!r} health_policy_version must equal "
                f"descriptor policy {health.policy_version!r}"
            )
        if callable(getattr(plugin, "build_candidate", None)):
            raise PluginLoadError(
                "Health candidate construction is framework-owned; custom adapters "
                "may customize only verdict aggregation"
            )
    if "health" in declared and health is not None and health.strategy == "custom":
        missing = [
            method
            for method in ("classify",)
            if not callable(getattr(plugin, method, None))
        ]
        if missing:
            raise PluginLoadError(
                f"Custom health adapter {registered_test.id!r} is missing method(s): "
                f"{', '.join(missing)}"
            )
    return plugin


def validate_registry_plugins(
    tests: Iterable[RegisteredValidationTest],
) -> tuple[str, ...]:
    """Load declarations and run deterministic config hooks for supplied tests."""

    loaded: list[str] = []
    for registered_test in tests:
        plugin = load_registered_plugin(registered_test)
        if plugin is None:
            continue
        declaration = registered_test.definition.plugin
        assert declaration is not None
        if "config" in declaration.capabilities:
            issues = plugin.validate_config(registered_test.definition)
            if not isinstance(issues, tuple) or not all(
                isinstance(issue, ConfigIssue) for issue in issues
            ):
                raise PluginLoadError(
                    f"Adapter {registered_test.id!r} validate_config must return "
                    "tuple[ConfigIssue, ...]"
                )
            if issues:
                rendered = "; ".join(f"{issue.code}: {issue.message}" for issue in issues)
                raise PluginLoadError(
                    f"Adapter {registered_test.id!r} configuration is invalid: {rendered}"
                )
        if "health" in declaration.capabilities:
            from cval.health.engine import validate_metric_specs

            specs = plugin.metric_specs(registered_test.definition)
            if not isinstance(specs, tuple):
                raise PluginLoadError(
                    f"Adapter {registered_test.id!r} metric_specs must return a tuple"
                )
            try:
                validate_metric_specs(specs, registered_test.definition)
            except (TypeError, ValueError) as exc:
                raise PluginLoadError(
                    f"Adapter {registered_test.id!r} health metric specs are invalid: {exc}"
                ) from exc
        loaded.append(registered_test.id)
    return tuple(loaded)


def validate_ingestion_artifact_tree(root: Path) -> None:
    """Reject symlinks or resolved escapes before an adapter reads descendants."""

    lexical_root = root.expanduser()
    if not lexical_root.is_dir() or lexical_root.is_symlink():
        raise IngestionError(f"Adapter artifact root is not a regular directory: {root}")
    resolved_root = lexical_root.resolve()
    for child in lexical_root.rglob("*"):
        if child.is_symlink():
            raise IngestionError(f"Adapter artifact tree contains a symlink: {child}")
        try:
            child.resolve().relative_to(resolved_root)
        except ValueError as exc:
            raise IngestionError(
                f"Adapter artifact path escapes its assigned root: {child}"
            ) from exc
