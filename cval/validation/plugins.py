"""Trusted repository-local validation adapter protocol and loader.

Adapters are imported only from paths declared by validated ``cval.test.v1``
descriptors.  The framework supplies immutable values and never gives adapters
Kubernetes clients or an unconstrained output path.
"""

from __future__ import annotations

import hashlib
import importlib.util
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from cval.models import LatestStatusRow
from cval.validation.operational_targets import OperationalTarget

from cval.validation.registry import (
    PLUGIN_API_VERSION,
    RegisteredValidationTest,
    ValidationTestDefinition,
    resolve_confined_path,
)

if TYPE_CHECKING:
    from cval.config import CvalConfig


class PluginError(RuntimeError):
    """Base error for validation adapter failures."""


class PluginLoadError(PluginError):
    """Raised when a declared adapter cannot satisfy its frozen contract."""


@dataclass(frozen=True)
class ConfigIssue:
    """One deterministic adapter-owned descriptor validation issue."""

    code: str
    message: str


@dataclass(frozen=True)
class ExportContext:
    """Strict read-only inputs supplied to one result-export hook.

    The context intentionally has no output path, writable database handle, or
    Kubernetes client.  Built-in adapters may use the source identifiers with
    the framework's read-only metric readers and must return rows to core.
    """

    target: OperationalTarget
    definition: ValidationTestDefinition
    config: "CvalConfig"
    status_rows: tuple[LatestStatusRow, ...]
    pod: str
    namespace: str
    source_db_paths: tuple[tuple[str, str], ...]
    include_metrics: bool = True

    def __post_init__(self) -> None:
        if self.target.owner_test_id != self.definition.metadata.id:
            raise ValueError("Export target owner does not match its definition")
        if self.config.tests.registry.get(self.target.owner_test_id) is None:
            raise ValueError("ExportContext.config does not contain the target owner")
        if not isinstance(self.status_rows, tuple) or not all(
            isinstance(row, LatestStatusRow) for row in self.status_rows
        ):
            raise TypeError("ExportContext.status_rows must be tuple[LatestStatusRow, ...]")
        if not isinstance(self.pod, str) or not self.pod.strip():
            raise ValueError("ExportContext.pod must be a non-empty string")
        if not isinstance(self.namespace, str) or not self.namespace.strip():
            raise ValueError("ExportContext.namespace must be a non-empty string")
        if not isinstance(self.source_db_paths, tuple):
            raise TypeError("ExportContext.source_db_paths must be a tuple")
        keys: set[str] = set()
        for item in self.source_db_paths:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not all(isinstance(value, str) and value.strip() for value in item)
            ):
                raise TypeError(
                    "ExportContext.source_db_paths must contain non-empty string pairs"
                )
            if item[0] in keys:
                raise ValueError("ExportContext.source_db_paths contains duplicate keys")
            keys.add(item[0])
        if not isinstance(self.include_metrics, bool):
            raise TypeError("ExportContext.include_metrics must be boolean")

    def source_db_path(self, key: str) -> str | None:
        """Return one declared read-only source DB path."""

        return dict(self.source_db_paths).get(key)


@dataclass(frozen=True)
class ExportRows:
    """Immutable, strictly rectangular CSV-safe rows returned by a plugin."""

    columns: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]
    row_label: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.columns, tuple) or not self.columns:
            raise TypeError("ExportRows.columns must be a non-empty tuple")
        if len(set(self.columns)) != len(self.columns) or not all(
            isinstance(column, str)
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column)
            for column in self.columns
        ):
            raise ValueError("ExportRows columns must be unique safe identifiers")
        if not isinstance(self.rows, tuple):
            raise TypeError("ExportRows.rows must be a tuple")
        for row in self.rows:
            if (
                not isinstance(row, tuple)
                or len(row) != len(self.columns)
                or not all(isinstance(value, str) for value in row)
            ):
                raise TypeError(
                    "ExportRows rows must be same-width tuples containing only strings"
                )
        if (
            not isinstance(self.row_label, str)
            or "\n" in self.row_label
            or "\r" in self.row_label
        ):
            raise ValueError("ExportRows.row_label must be a single-line string")


def export_rows_from_records(
    columns: tuple[str, ...],
    records: Iterable[Mapping[str, object]],
    *,
    row_label: str = "",
) -> ExportRows:
    """Normalize scalar record dictionaries into the strict export contract."""

    normalized_rows: list[tuple[str, ...]] = []
    for record in records:
        if not isinstance(record, Mapping):
            raise TypeError("Export records must be mappings")
        unknown = sorted(set(record) - set(columns))
        if unknown:
            raise ValueError(
                "Export record contains undeclared columns: " + ", ".join(unknown)
            )
        values: list[str] = []
        for column in columns:
            value = record.get(column, "")
            if value is None:
                values.append("")
            elif isinstance(value, bool):
                values.append(str(value).lower())
            elif isinstance(value, int) and not isinstance(value, bool):
                values.append(str(value))
            elif isinstance(value, float):
                if not math.isfinite(value):
                    raise ValueError("Export rows cannot contain non-finite numbers")
                values.append(str(value))
            elif isinstance(value, str):
                values.append(value)
            else:
                raise TypeError(
                    "Export row values must be string, integer, finite float, boolean, or null"
                )
        normalized_rows.append(tuple(values))
    return ExportRows(
        columns=columns,
        rows=tuple(normalized_rows),
        row_label=row_label,
    )


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
    previous_bytecode = sys.dont_write_bytecode
    previous_modules = frozenset(sys.modules)
    support_root = str(registered_test.test_dir)
    sys.path.insert(0, support_root)
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    except BaseException as exc:  # noqa: BLE001 - fail-closed adapter boundary
        raise PluginLoadError(
            f"Could not import adapter for {registered_test.id!r}: {exc}"
        ) from exc
    finally:
        sys.dont_write_bytecode = previous_bytecode
        try:
            sys.path.remove(support_root)
        except ValueError:
            pass
        sys.modules.pop(module_name, None)
        for imported_name in set(sys.modules) - previous_modules:
            imported = sys.modules.get(imported_name)
            imported_file = getattr(imported, "__file__", None)
            if not imported_file:
                continue
            try:
                Path(imported_file).resolve().relative_to(
                    registered_test.test_dir.resolve()
                )
            except (OSError, ValueError):
                continue
            sys.modules.pop(imported_name, None)

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
            try:
                issues = plugin.validate_config(registered_test.definition)
            except BaseException as exc:  # noqa: BLE001 - fail-closed hook boundary
                raise PluginLoadError(
                    f"Adapter {registered_test.id!r} validate_config failed: {exc}"
                ) from exc
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
        loaded.append(registered_test.id)
    return tuple(loaded)
