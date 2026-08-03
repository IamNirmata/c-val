"""Immutable registry-derived targets for current evaluator operations.

Targets are created in registry order from enabled adapter capabilities. The
four built-in DL component aliases live here so the CLI, loops, and exports use
one deterministic catalog.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType

from cval.validation.builtins import BUILTIN_ALIAS_ROWS
from cval.validation.registry import TEST_ID_PATTERN, ValidationTestRegistry

BASELINE_BUILD = "baseline-build"
BASELINE_LIST = "baseline-list"
BASELINE_SHOW = "baseline-show"
BASELINE_ACTIVATE = "baseline-activate"
BASELINE_CLASSIFY = "baseline-classify"
RESULTS_EXPORT = "results-export"
CLASSIFICATIONS_EXPORT = "classifications-export"

OPERATION_ORDER = (
    BASELINE_BUILD,
    BASELINE_LIST,
    BASELINE_SHOW,
    BASELINE_ACTIVATE,
    BASELINE_CLASSIFY,
    RESULTS_EXPORT,
    CLASSIFICATIONS_EXPORT,
)

_BASELINE_OPERATIONS = frozenset(
    {
        BASELINE_BUILD,
        BASELINE_LIST,
        BASELINE_SHOW,
        BASELINE_ACTIVATE,
        BASELINE_CLASSIFY,
        CLASSIFICATIONS_EXPORT,
    }
)
_EXPORT_OPERATIONS = frozenset({RESULTS_EXPORT})
_ALIAS_OPERATIONS = frozenset(
    {BASELINE_CLASSIFY, RESULTS_EXPORT, CLASSIFICATIONS_EXPORT}
)
RESERVED_TARGET_NAMES = frozenset({"all", "overall"})

# owner, alias, component, refresh group. Keep this as the one built-in
# overlay; models, exporters, the CLI, and loops must not maintain copies.
BUILTIN_TARGET_ALIASES = MappingProxyType(
    {alias: owner for owner, alias, _component, _refresh in BUILTIN_ALIAS_ROWS}
)
DL_COMPONENT_TEST_TYPES = MappingProxyType(
    {
        "dltest": None,
        **{
            alias: component
            for owner, alias, component, _refresh in BUILTIN_ALIAS_ROWS
            if owner == "dltest"
        },
    }
)


@dataclass(frozen=True)
class OperationalTarget:
    """One immutable operator-facing evaluator target."""

    name: str
    owner_test_id: str
    baseline_test_type: str
    status_test: str
    operations: frozenset[str]
    alias: bool = False
    component: str = ""
    refresh_group: str = ""

    def supports(self, operation: str) -> bool:
        """Return whether this target supports one known operation."""

        _require_operation(operation)
        return operation in self.operations

    def to_dict(self) -> dict[str, object]:
        """Return a stable JSON-ready representation."""

        return {
            "format_version": "cval.operational-target.v1",
            "name": self.name,
            "owner_test_id": self.owner_test_id,
            "baseline_test_type": self.baseline_test_type,
            "status_test": self.status_test,
            "operations": [
                operation for operation in OPERATION_ORDER if operation in self.operations
            ],
            "alias": self.alias,
            "component": self.component,
            "refresh_group": self.refresh_group,
        }


@dataclass(frozen=True)
class OperationalTargetCatalog:
    """Ordered immutable catalog derived from one validated registry."""

    targets: tuple[OperationalTarget, ...]

    def get(self, name: str) -> OperationalTarget | None:
        """Return one target by its exact operator-facing name."""

        return next((target for target in self.targets if target.name == name), None)

    def require(self, name: str, operation: str) -> OperationalTarget:
        """Resolve a target again at execution time and require an operation."""

        _require_operation(operation)
        target = self.get(name)
        if target is None:
            raise ValueError(f"Operational target is not enabled or registered: {name}")
        if operation not in target.operations:
            raise ValueError(
                f"Operational target {name!r} does not support {operation!r}"
            )
        return target

    def for_operation(self, operation: str) -> tuple[OperationalTarget, ...]:
        """Return supporting targets without changing registry order."""

        _require_operation(operation)
        return tuple(
            target for target in self.targets if operation in target.operations
        )

    def names_for(self, operation: str) -> tuple[str, ...]:
        """Return supporting target names in registry/overlay order."""

        return tuple(target.name for target in self.for_operation(operation))


def build_operational_target_catalog(
    registry: ValidationTestRegistry,
) -> OperationalTargetCatalog:
    """Build and validate the evaluator catalog for a registry.

    Disabled tests produce no targets.  A ``baseline`` capability enables the
    baseline lifecycle and classification operations; the ``export`` capability
    enables result export. Alias names are
    reserved even when their owner is disabled so a future enable cannot change
    the meaning of an already-registered canonical test ID.
    """

    registered_names = [test.id for test in registry.tests]
    duplicate_names = _duplicates(registered_names)
    if duplicate_names:
        raise ValueError(
            "Operational target canonical names collide: "
            + ", ".join(duplicate_names)
        )
    reserved = sorted(set(registered_names) & RESERVED_TARGET_NAMES)
    if reserved:
        raise ValueError(
            "Validation test IDs use reserved operational target names: "
            + ", ".join(reserved)
        )

    alias_names = {alias for _owner, alias, _component, _refresh in BUILTIN_ALIAS_ROWS}
    alias_collisions = sorted(set(registered_names) & alias_names)
    if alias_collisions:
        raise ValueError(
            "Validation test IDs collide with built-in target aliases: "
            + ", ".join(alias_collisions)
        )

    targets: list[OperationalTarget] = []
    for registered in registry.tests:
        if not registered.enabled:
            continue
        declaration = registered.definition.plugin
        capabilities = frozenset(declaration.capabilities) if declaration else frozenset()
        operations = frozenset()
        if "baseline" in capabilities:
            operations |= _BASELINE_OPERATIONS
        if "export" in capabilities:
            operations |= _EXPORT_OPERATIONS
        if not operations:
            continue

        refresh_group = "dltest" if registered.id == "dltest" else ""
        canonical = OperationalTarget(
            name=registered.id,
            owner_test_id=registered.id,
            baseline_test_type=registered.id,
            status_test=registered.id,
            operations=operations,
            refresh_group=refresh_group,
        )
        targets.append(canonical)

        for owner, alias, component, alias_refresh_group in BUILTIN_ALIAS_ROWS:
            if owner != registered.id:
                continue
            alias_operations = operations & _ALIAS_OPERATIONS
            if not alias_operations:
                continue
            targets.append(
                OperationalTarget(
                    name=alias,
                    owner_test_id=owner,
                    baseline_test_type=owner,
                    status_test=owner,
                    operations=alias_operations,
                    alias=True,
                    component=component,
                    refresh_group=alias_refresh_group,
                )
            )

    names = [target.name for target in targets]
    collisions = _duplicates(names)
    if collisions:
        raise ValueError(
            "Operational target names collide: " + ", ".join(collisions)
        )
    return OperationalTargetCatalog(tuple(targets))


def normalize_operational_target(name: str) -> str:
    """Return the canonical owner for a built-in alias."""

    normalized = name.strip().lower()
    return BUILTIN_TARGET_ALIASES.get(normalized, normalized)


def operational_component(name: str) -> str | None:
    """Return the component selected by a known DL target, if any."""

    return DL_COMPONENT_TEST_TYPES.get(name.strip().lower())


def validate_operational_target_name(name: str) -> str:
    """Validate a canonical/alias-shaped target name for safe filenames."""

    normalized = name.strip().lower()
    if not TEST_ID_PATTERN.fullmatch(normalized):
        raise ValueError(f"Invalid operational target name: {name!r}")
    if normalized in RESERVED_TARGET_NAMES:
        raise ValueError(f"Reserved operational target name: {name!r}")
    return normalized


def _require_operation(operation: str) -> None:
    if operation not in OPERATION_ORDER:
        raise ValueError(f"Unknown operational target operation: {operation!r}")


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


__all__ = [
    "BASELINE_ACTIVATE",
    "BASELINE_BUILD",
    "BASELINE_CLASSIFY",
    "BASELINE_LIST",
    "BASELINE_SHOW",
    "CLASSIFICATIONS_EXPORT",
    "BUILTIN_TARGET_ALIASES",
    "DL_COMPONENT_TEST_TYPES",
    "OPERATION_ORDER",
    "OperationalTarget",
    "OperationalTargetCatalog",
    "RESERVED_TARGET_NAMES",
    "RESULTS_EXPORT",
    "build_operational_target_catalog",
    "operational_component",
    "normalize_operational_target",
    "validate_operational_target_name",
]
