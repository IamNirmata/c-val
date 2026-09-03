"""Canonical raw result export dispatch."""

from __future__ import annotations

from cval.config import CvalConfig
from cval.validation.operational_targets import (
    RESULTS_EXPORT,
    OperationalTarget,
    build_operational_target_catalog,
)
from cval.validation.plugins import (
    ExportContext,
    ExportRows,
    PluginLoadError,
    load_registered_plugin,
)


def resolve_operational_target(
    config: CvalConfig,
    name: str,
    operation: str,
) -> OperationalTarget:
    """Rebuild the immutable catalog and resolve one enabled target."""

    return build_operational_target_catalog(config.tests.registry).require(name, operation)


def export_result_rows(
    config: CvalConfig,
    target_name: str,
    context: ExportContext,
) -> ExportRows:
    """Invoke one read-only export hook and enforce its rectangular contract."""

    target = resolve_operational_target(config, target_name, RESULTS_EXPORT)
    if context.target != target:
        raise ValueError("Export context target does not match the resolved catalog target")
    registered, plugin = _operational_plugin(config, target)
    if context.definition != registered.definition:
        raise ValueError("Export context definition is not the current registry definition")
    rows = plugin.export_rows(context)
    if not isinstance(rows, ExportRows):
        raise TypeError(
            f"Adapter {target.owner_test_id!r} export_rows must return ExportRows"
        )
    return ExportRows(
        tuple(rows.columns),
        tuple(tuple(row) for row in rows.rows),
        rows.row_label,
    )


def _operational_plugin(config: CvalConfig, target: OperationalTarget):
    registered = config.tests.registry.require(target.owner_test_id)
    if not registered.enabled:
        raise ValueError(f"Operational target owner is disabled: {target.owner_test_id}")
    plugin = load_registered_plugin(registered)
    if plugin is None:
        raise PluginLoadError(
            f"Operational target owner {target.owner_test_id!r} has no plugin"
        )
    return registered, plugin


__all__ = [
    "export_result_rows",
    "resolve_operational_target",
]
