"""Repository-local validation test registry and descriptor loading.

The global c-val config explicitly registers each test with an activation flag
and a repository-relative ``test_config.toml`` path.  This module resolves and
validates those descriptors without importing optional adapter code or running
test commands.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import tomllib
from collections.abc import Iterator
from dataclasses import dataclass, field, fields, is_dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from cval.validation.builtins import DEFAULT_TEST_REGISTRATIONS


TEST_SCHEMA_VERSION = "cval.test.v1"
PLUGIN_API_VERSION = "cval.plugin.v1"
TEST_ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
RESOURCE_QUANTITY_PATTERN = re.compile(
    r"^(?P<number>(?:\d+(?:\.\d*)?|\.\d+))"
    r"(?P<suffix>m|u|n|[EPTGMK]i?|[eE][+-]?\d+)?$"
)
ALLOWED_PLUGIN_CAPABILITIES = frozenset(
    {"config", "baseline", "export"}
)

class FrozenMapping(Mapping[str, Any]):
    """Pickle-safe immutable mapping used for descriptor-owned settings."""

    __slots__ = ("_items",)

    def __init__(self, values: Mapping[str, Any] | tuple[tuple[str, Any], ...]) -> None:
        items = values.items() if isinstance(values, Mapping) else values
        self._items = tuple((key, value) for key, value in items)

    def __getitem__(self, key: str) -> Any:
        for item_key, value in self._items:
            if item_key == key:
                return value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return (key for key, _value in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __reduce__(self):
        return (type(self), (self._items,))


def _freeze_json_like(value: Any) -> Any:
    """Recursively freeze TOML/JSON-like mappings and arrays."""

    if isinstance(value, Mapping):
        return FrozenMapping(
            tuple((str(key), _freeze_json_like(item)) for key, item in value.items())
        )
    if isinstance(value, list | tuple):
        return tuple(_freeze_json_like(item) for item in value)
    return value


def _json_ready(value: Any) -> Any:
    """Return mutable JSON-ready containers without exposing registry state."""

    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _json_ready(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_ready(item) for item in value]
    return value


@dataclass(frozen=True)
class TestMetadata:
    """Execution metadata from one test descriptor."""

    id: str
    display_name: str
    description: str
    order: int
    entrypoint: str
    setup: str
    timeout_seconds: int
    continue_on_failure: bool = True


@dataclass(frozen=True)
class TestRequirements:
    """Minimum shared job resources required by one validation test."""

    cpu: str = "0"
    memory: str = "0"
    gpu_count: int = 0
    rdma_count: int = 0
    shared_memory: str = "0"
    read_sysfs: bool = False


@dataclass(frozen=True)
class TestArtifacts:
    """Test artifact settings used by the generic runner."""

    summary_filename: str = "summary.json"


@dataclass(frozen=True)
class TestPlugin:
    """Optional Python adapter declaration."""

    adapter: str
    api_version: str
    capabilities: tuple[str, ...]
    support_files: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValidationTestDefinition:
    """A fully validated repository-local test descriptor."""

    schema_version: str
    metadata: TestMetadata
    requirements: TestRequirements
    artifacts: TestArtifacts
    settings: Mapping[str, Any] = field(default_factory=dict)
    plugin: TestPlugin | None = None


@dataclass(frozen=True)
class RegisteredValidationTest:
    """Global activation state joined to one validated test descriptor."""

    enabled: bool
    config_path: str
    resolved_config_path: Path
    test_dir: Path
    definition: ValidationTestDefinition

    @property
    def id(self) -> str:
        return self.definition.metadata.id

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready effective test configuration."""

        value = _json_ready(self.definition)
        value["enabled"] = self.enabled
        value["config_path"] = self.config_path
        return value


@dataclass(frozen=True)
class ValidationTestRegistry:
    """Deterministically ordered set of registered validation tests."""

    tests: tuple[RegisteredValidationTest, ...] = ()

    def get(self, test_id: str) -> RegisteredValidationTest | None:
        """Return one registered test by ID, if present."""

        return next((test for test in self.tests if test.id == test_id), None)

    def require(self, test_id: str) -> RegisteredValidationTest:
        """Return one test or raise a precise configuration error."""

        test = self.get(test_id)
        if test is None:
            raise ValueError(f"Validation test is not registered: {test_id}")
        return test

    @property
    def enabled(self) -> tuple[RegisteredValidationTest, ...]:
        """Return enabled tests in deterministic execution order."""

        return tuple(test for test in self.tests if test.enabled)

    def to_dict(self) -> dict[str, Any]:
        """Return an ID-keyed JSON-ready effective registry."""

        return {test.id: test.to_dict() for test in self.tests}


def validation_test_config_digest(
    test: RegisteredValidationTest | ValidationTestDefinition,
) -> str:
    """Return the canonical SHA-256 identity of one composed test descriptor."""

    definition = test.definition if isinstance(test, RegisteredValidationTest) else test
    canonical = json.dumps(
        _json_ready(definition),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def load_test_registry(
    tests_section: Mapping[str, Any] | None,
    *,
    repo_root: Path,
    include_defaults: bool = True,
    require_enabled: bool = True,
) -> ValidationTestRegistry:
    """Compose global registrations and load all referenced test descriptors.

    ``tests_section`` contains only activation and config path values.  Known
    built-ins are merged with defaults to preserve partial global config
    overrides during the compatibility window.
    """

    if tests_section is None:
        supplied: dict[str, Any] = {}
    elif isinstance(tests_section, Mapping):
        supplied = dict(tests_section)
    else:
        raise ValueError("Config section [tests] must be a table")

    registrations: dict[str, dict[str, object]] = {}
    if include_defaults:
        registrations = {
            test_id: dict(values)
            for test_id, values in DEFAULT_TEST_REGISTRATIONS.items()
        }

    for test_id, raw in supplied.items():
        _validate_test_id(test_id)
        if not isinstance(raw, Mapping):
            raise ValueError(f"Config section [tests.{test_id}] must be a table")
        unknown = sorted(set(raw) - {"enabled", "config_path"})
        if unknown:
            raise ValueError(
                f"Unknown value(s) under [tests.{test_id}]: {', '.join(unknown)}; "
                "move test-specific settings to test_config.toml"
            )
        current = registrations.get(test_id, {})
        if "enabled" in raw:
            current["enabled"] = _strict_bool(raw["enabled"], f"tests.{test_id}.enabled")
        elif test_id not in registrations:
            raise ValueError(f"tests.{test_id}.enabled is required for a new test")
        if "config_path" in raw:
            current["config_path"] = _strict_str(
                raw["config_path"], f"tests.{test_id}.config_path"
            )
        elif test_id not in registrations:
            raise ValueError(f"tests.{test_id}.config_path is required for a new test")
        registrations[test_id] = current

    loaded: list[RegisteredValidationTest] = []
    paths: dict[Path, str] = {}
    for test_id, registration in registrations.items():
        _validate_test_id(test_id)
        enabled = _strict_bool(registration.get("enabled"), f"tests.{test_id}.enabled")
        config_path = _strict_str(
            registration.get("config_path"), f"tests.{test_id}.config_path"
        )
        resolved = resolve_confined_path(
            repo_root,
            config_path,
            field_name=f"tests.{test_id}.config_path",
            require_file=True,
        )
        if resolved in paths:
            raise ValueError(
                f"Validation tests {paths[resolved]!r} and {test_id!r} use the same "
                f"config_path: {config_path}"
            )
        paths[resolved] = test_id
        definition = load_test_definition(resolved, expected_id=test_id)
        loaded.append(
            RegisteredValidationTest(
                enabled=enabled,
                config_path=config_path,
                resolved_config_path=resolved,
                test_dir=resolved.parent,
                definition=definition,
            )
        )

    loaded.sort(key=lambda test: (test.definition.metadata.order, test.id))
    enabled_tests = [test for test in loaded if test.enabled]
    if require_enabled and not enabled_tests:
        raise ValueError("At least one test must be enabled under [tests.*]")

    enabled_orders: dict[int, str] = {}
    for test in enabled_tests:
        order = test.definition.metadata.order
        if order in enabled_orders:
            raise ValueError(
                "Enabled validation tests must have unique execution order: "
                f"{enabled_orders[order]!r} and {test.id!r} both use {order}"
            )
        enabled_orders[order] = test.id

    return ValidationTestRegistry(tuple(loaded))


def load_test_definition(path: Path, *, expected_id: str) -> ValidationTestDefinition:
    """Parse and validate one ``cval.test.v1`` descriptor."""

    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"Invalid test TOML {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Validation test config must be a TOML table: {path}")

    allowed_root = {
        "schema_version",
        "test",
        "requirements",
        "settings",
        "artifacts",
        "plugin",
    }
    _reject_unknown(data, allowed_root, str(path))
    schema_version = _strict_str(data.get("schema_version"), "schema_version")
    if schema_version != TEST_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported test schema_version in {path}: {schema_version!r}; "
            f"expected {TEST_SCHEMA_VERSION!r}"
        )

    test = _required_table(data, "test", path)
    _reject_unknown(
        test,
        {
            "id",
            "display_name",
            "description",
            "order",
            "entrypoint",
            "setup",
            "timeout_seconds",
            "continue_on_failure",
        },
        f"{path} [test]",
    )
    test_id = _strict_str(test.get("id"), "test.id")
    _validate_test_id(test_id)
    if test_id != expected_id:
        raise ValueError(
            f"Registered test ID {expected_id!r} does not match {path} [test].id "
            f"{test_id!r}"
        )
    display_name = _strict_str(test.get("display_name"), "test.display_name").strip()
    if not 1 <= len(display_name) <= 100:
        raise ValueError("test.display_name must contain 1 to 100 characters")
    description = _optional_str(test, "description", "")
    if len(description) > 500:
        raise ValueError("test.description must not exceed 500 characters")
    order = _strict_int(test.get("order"), "test.order")
    if order < 0:
        raise ValueError("test.order must be non-negative")
    timeout_seconds = _strict_int(test.get("timeout_seconds"), "test.timeout_seconds")
    if timeout_seconds <= 0:
        raise ValueError("test.timeout_seconds must be positive")
    continue_on_failure = _optional_bool(test, "continue_on_failure", True)
    if not continue_on_failure:
        raise ValueError("cval.test.v1 requires test.continue_on_failure=true")

    entrypoint = _strict_str(test.get("entrypoint"), "test.entrypoint")
    setup = _strict_str(test.get("setup"), "test.setup")
    resolve_confined_path(
        path.parent,
        entrypoint,
        field_name="test.entrypoint",
        require_file=True,
    )
    resolve_confined_path(
        path.parent,
        setup,
        field_name="test.setup",
        require_file=True,
    )

    requirements_raw = _optional_table(data, "requirements", path)
    _reject_unknown(
        requirements_raw,
        {"cpu", "memory", "gpu_count", "rdma_count", "shared_memory", "read_sysfs"},
        f"{path} [requirements]",
    )
    requirements = TestRequirements(
        cpu=_resource_quantity(requirements_raw, "cpu", "0"),
        memory=_resource_quantity(requirements_raw, "memory", "0"),
        gpu_count=_non_negative_int(requirements_raw, "gpu_count", 0),
        rdma_count=_non_negative_int(requirements_raw, "rdma_count", 0),
        shared_memory=_resource_quantity(requirements_raw, "shared_memory", "0"),
        read_sysfs=_optional_bool(requirements_raw, "read_sysfs", False),
    )

    settings = _optional_table(data, "settings", path)
    artifacts_raw = _optional_table(data, "artifacts", path)
    _reject_unknown(
        artifacts_raw,
        {"summary_filename"},
        f"{path} [artifacts]",
    )
    summary_filename = _optional_str(artifacts_raw, "summary_filename", "summary.json")
    if Path(summary_filename).name != summary_filename or summary_filename in {"", ".", ".."}:
        raise ValueError("artifacts.summary_filename must be a basename")
    artifacts = TestArtifacts(summary_filename)

    plugin = _parse_plugin(data, path)

    return ValidationTestDefinition(
        schema_version=schema_version,
        metadata=TestMetadata(
            id=test_id,
            display_name=display_name,
            description=description,
            order=order,
            entrypoint=entrypoint,
            setup=setup,
            timeout_seconds=timeout_seconds,
            continue_on_failure=continue_on_failure,
        ),
        requirements=requirements,
        artifacts=artifacts,
        settings=_freeze_json_like(settings),
        plugin=plugin,
    )


def resolve_confined_path(
    root: Path,
    relative_path: str,
    *,
    field_name: str,
    require_file: bool = False,
) -> Path:
    """Resolve a relative path and require it to remain under ``root``."""

    path = Path(relative_path).expanduser()
    if path.is_absolute():
        raise ValueError(f"{field_name} must be relative to {root}: {relative_path!r}")
    resolved_root = root.expanduser().resolve()
    resolved = (resolved_root / path).resolve()
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(
            f"{field_name} escapes its allowed root {resolved_root}: {relative_path!r}"
        ) from exc
    if require_file and not resolved.is_file():
        raise FileNotFoundError(f"{field_name} file not found: {resolved}")
    return resolved


def parse_resource_quantity(value: str) -> Decimal:
    """Parse the Kubernetes quantity subset used by c-val resource checks."""

    match = RESOURCE_QUANTITY_PATTERN.fullmatch(value)
    if not match:
        raise ValueError(f"Invalid Kubernetes resource quantity: {value!r}")
    try:
        number = Decimal(match.group("number"))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid Kubernetes resource quantity: {value!r}") from exc
    suffix = match.group("suffix") or ""
    factors = {
        "": Decimal(1),
        "m": Decimal("0.001"),
        "u": Decimal("0.000001"),
        "n": Decimal("0.000000001"),
        "K": Decimal(1000),
        "M": Decimal(1000) ** 2,
        "G": Decimal(1000) ** 3,
        "T": Decimal(1000) ** 4,
        "P": Decimal(1000) ** 5,
        "E": Decimal(1000) ** 6,
        "Ki": Decimal(1024),
        "Mi": Decimal(1024) ** 2,
        "Gi": Decimal(1024) ** 3,
        "Ti": Decimal(1024) ** 4,
        "Pi": Decimal(1024) ** 5,
        "Ei": Decimal(1024) ** 6,
    }
    if suffix.startswith(("e", "E")) and len(suffix) > 1:
        return number * (Decimal(10) ** int(suffix[1:]))
    return number * factors[suffix]


def _parse_plugin(
    data: Mapping[str, Any], path: Path
) -> TestPlugin | None:
    if "plugin" not in data:
        return None
    raw = _required_table(data, "plugin", path)
    _reject_unknown(
        raw,
        {"adapter", "api_version", "capabilities", "support_files"},
        f"{path} [plugin]",
    )
    adapter = _strict_str(raw.get("adapter"), "plugin.adapter")
    resolve_confined_path(
        path.parent,
        adapter,
        field_name="plugin.adapter",
        require_file=True,
    )
    api_version = _strict_str(raw.get("api_version"), "plugin.api_version")
    if api_version != PLUGIN_API_VERSION:
        raise ValueError(
            f"Unsupported plugin.api_version {api_version!r}; expected {PLUGIN_API_VERSION!r}"
        )
    capabilities = _strict_str_tuple(raw.get("capabilities"), "plugin.capabilities")
    if len(set(capabilities)) != len(capabilities):
        raise ValueError("plugin.capabilities must not contain duplicates")
    unknown = sorted(set(capabilities) - ALLOWED_PLUGIN_CAPABILITIES)
    if unknown:
        raise ValueError(f"Unknown plugin capabilities: {', '.join(unknown)}")
    support_files = _strict_str_tuple(
        raw.get("support_files", ()), "plugin.support_files"
    )
    if len(set(support_files)) != len(support_files):
        raise ValueError("plugin.support_files must not contain duplicates")
    if adapter in support_files:
        raise ValueError("plugin.support_files must not repeat plugin.adapter")
    for index, support_file in enumerate(support_files):
        resolve_confined_path(
            path.parent,
            support_file,
            field_name=f"plugin.support_files[{index}]",
            require_file=True,
        )
    return TestPlugin(adapter, api_version, capabilities, support_files)


def _validation_root_test_path(value: Any, test_id: str, field_name: str) -> str:
    text = _strict_str(value, field_name)
    path = Path(text)
    if path.is_absolute():
        raise ValueError(f"{field_name} must be relative to runtime.validation_root")
    normalized = Path(*path.parts)
    expected = Path("validation_tests") / test_id
    try:
        normalized.relative_to(expected)
    except ValueError as exc:
        raise ValueError(f"{field_name} must stay under {expected}") from exc
    if any(part in {"", ".", ".."} for part in normalized.parts):
        raise ValueError(f"{field_name} contains an invalid path segment")
    return normalized.as_posix()


def _validate_test_id(test_id: str) -> None:
    if not isinstance(test_id, str) or not TEST_ID_PATTERN.fullmatch(test_id):
        raise ValueError(
            "Validation test ID must match ^[a-z][a-z0-9-]{0,62}$: "
            f"{test_id!r}"
        )


def _required_table(data: Mapping[str, Any], name: str, path: Path) -> Mapping[str, Any]:
    if name not in data:
        raise ValueError(f"Missing [{name}] table in {path}")
    value = data[name]
    if not isinstance(value, Mapping):
        raise ValueError(f"[{name}] in {path} must be a table")
    return value


def _optional_table(data: Mapping[str, Any], name: str, path: Path) -> Mapping[str, Any]:
    value = data.get(name, {})
    if not isinstance(value, Mapping):
        raise ValueError(f"[{name}] in {path} must be a table")
    return value


def _reject_unknown(data: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ValueError(f"Unknown value(s) in {where}: {', '.join(unknown)}")


def _strict_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_str(data: Mapping[str, Any], key: str, default: str) -> str:
    if key not in data:
        return default
    value = data[key]
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value.strip()


def _strict_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a TOML boolean")
    return value


def _optional_bool(data: Mapping[str, Any], key: str, default: bool) -> bool:
    return default if key not in data else _strict_bool(data[key], key)


def _strict_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _optional_int(data: Mapping[str, Any], key: str, default: int) -> int:
    return default if key not in data else _strict_int(data[key], key)


def _non_negative_int(data: Mapping[str, Any], key: str, default: int) -> int:
    value = _optional_int(data, key, default)
    if value < 0:
        raise ValueError(f"{key} must be non-negative")
    return value


def _strict_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _optional_float(data: Mapping[str, Any], key: str, default: float) -> float:
    return default if key not in data else _strict_float(data[key], key)


def _strict_str_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        raise ValueError(f"{field_name} must be an array of strings")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field_name} must contain only non-empty strings")
        result.append(item.strip())
    return tuple(result)


def _resource_quantity(data: Mapping[str, Any], key: str, default: str) -> str:
    value = default if key not in data else _strict_str(data[key], key)
    parse_resource_quantity(value)
    return value