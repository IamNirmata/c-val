"""Immutable registry-derived targets for raw result export."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from cval.validation.registry import RESERVED_TEST_NAMES, TEST_ID_PATTERN, ValidationTestRegistry

if TYPE_CHECKING:
    from cval.config import CvalConfig

RESULTS_EXPORT = "results-export"
OPERATION_ORDER = (RESULTS_EXPORT,)
RESERVED_TARGET_NAMES = RESERVED_TEST_NAMES


@dataclass(frozen=True)
class OperationalTarget:
    """One immutable operator-facing raw export target."""

    name: str
    owner_test_id: str
    status_test: str
    operations: frozenset[str]

    def supports(self, operation: str) -> bool:
        """Return whether this target supports one known operation."""

        _require_operation(operation)
        return operation in self.operations

    def to_dict(self) -> dict[str, object]:
        """Return a stable JSON-ready representation."""

        return {
            "format_version": "cval.operational-target.v2",
            "name": self.name,
            "owner_test_id": self.owner_test_id,
            "status_test": self.status_test,
            "operations": [
                operation for operation in OPERATION_ORDER if operation in self.operations
            ],
        }


@dataclass(frozen=True)
class OperationalTargetCatalog:
    """Ordered immutable export catalog derived from one validated registry."""

    targets: tuple[OperationalTarget, ...]

    def get(self, name: str) -> OperationalTarget | None:
        """Return one target by its exact operator-facing name."""

        return next((target for target in self.targets if target.name == name), None)

    def require(self, name: str, operation: str) -> OperationalTarget:
        """Resolve a target and require its raw operation."""

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
        """Return supporting target names in registry order."""

        return tuple(target.name for target in self.for_operation(operation))


def build_operational_target_catalog(
    registry: ValidationTestRegistry,
) -> OperationalTargetCatalog:
    """Preserve the catalog API as a view of the registry's export selection."""

    return OperationalTargetCatalog(tuple(
        OperationalTarget(test.id, test.id, test.id, frozenset({RESULTS_EXPORT}))
        for test in registry.exportable
    ))


def resolve_operational_target(config: CvalConfig, name: str, operation: str) -> OperationalTarget:
    """Resolve an enabled exporter directly without constructing another catalog."""

    _require_operation(operation)
    if not any(test.id == name for test in config.tests.registry.exportable):
        raise ValueError(f"Operational target is not enabled or registered: {name}")
    return OperationalTarget(name, name, name, frozenset({RESULTS_EXPORT}))


def validate_operational_target_name(name: str) -> str:
    """Validate a target name for safe filenames."""

    normalized = name.strip().lower()
    if not TEST_ID_PATTERN.fullmatch(normalized):
        raise ValueError(f"Invalid operational target name: {name!r}")
    if normalized in RESERVED_TARGET_NAMES:
        raise ValueError(f"Reserved operational target name: {name!r}")
    return normalized


def _require_operation(operation: str) -> None:
    if operation not in OPERATION_ORDER:
        raise ValueError(f"Unknown operational target operation: {operation!r}")


__all__ = [
    "OPERATION_ORDER",
    "OperationalTarget",
    "OperationalTargetCatalog",
    "RESERVED_TARGET_NAMES",
    "RESULTS_EXPORT",
    "build_operational_target_catalog",
    "resolve_operational_target",
    "validate_operational_target_name",
]
