"""Immutable registry-derived targets for raw result export."""

from __future__ import annotations

from dataclasses import dataclass

from cval.validation.registry import TEST_ID_PATTERN, ValidationTestRegistry

RESULTS_EXPORT = "results-export"
OPERATION_ORDER = (RESULTS_EXPORT,)
RESERVED_TARGET_NAMES = frozenset({"all", "overall"})


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
    """Build the raw export catalog from enabled plugin capabilities."""

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

    targets: list[OperationalTarget] = []
    for registered in registry.tests:
        if not registered.enabled:
            continue
        declaration = registered.definition.plugin
        capabilities = frozenset(declaration.capabilities) if declaration else frozenset()
        if "export" not in capabilities:
            continue
        targets.append(
            OperationalTarget(
                name=registered.id,
                owner_test_id=registered.id,
                status_test=registered.id,
                operations=frozenset({RESULTS_EXPORT}),
            )
        )
    return OperationalTargetCatalog(tuple(targets))


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


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


__all__ = [
    "OPERATION_ORDER",
    "OperationalTarget",
    "OperationalTargetCatalog",
    "RESERVED_TARGET_NAMES",
    "RESULTS_EXPORT",
    "build_operational_target_catalog",
    "validate_operational_target_name",
]
