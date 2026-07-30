"""Canonical environment-combination identities for comparable health results."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from typing import Any

from cval.health.models import EnvironmentCombination
from cval.validation.registry import ValidationTestDefinition


COMBINATION_KEY_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMON_FACTORS = frozenset({"image_name", "cuda_version", "pytorch_version"})


def canonicalize_factors(factors: Mapping[str, Any]) -> EnvironmentCombination:
    """Return a typed canonical JSON/SHA-256 identity for non-empty scalar factors."""

    if not factors:
        raise ValueError("Environment combination must contain at least one factor")
    normalized: dict[str, str | int | float | bool] = {}
    for raw_name, value in factors.items():
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ValueError("Environment combination factor names must be non-empty strings")
        name = raw_name.strip()
        if name in normalized:
            raise ValueError(f"Duplicate environment combination factor: {name}")
        if isinstance(value, str):
            if not value.strip():
                raise ValueError(f"Environment combination factor {name!r} is empty")
            normalized[name] = value
        elif isinstance(value, bool):
            normalized[name] = value
        elif isinstance(value, int):
            normalized[name] = value
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(
                    f"Environment combination factor {name!r} must be finite"
                )
            normalized[name] = 0.0 if value == 0.0 else value
        else:
            raise ValueError(
                f"Environment combination factor {name!r} must be a JSON scalar"
            )
    factors_json = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    digest = hashlib.sha256(factors_json.encode("utf-8")).hexdigest()
    return EnvironmentCombination(f"sha256:{digest}", factors_json)


def resolve_environment_combination(
    definition: ValidationTestDefinition,
    common_values: Mapping[str, Any],
) -> EnvironmentCombination | None:
    """Resolve configured factors; return ``None`` when a required value is absent."""

    health = definition.health
    if health is None or not health.enabled:
        return None
    factors: dict[str, Any] = {}
    for factor in health.combination_factors:
        if factor in _COMMON_FACTORS:
            value = common_values.get(factor)
            if value is not None and not isinstance(value, str):
                raise ValueError(
                    f"Environment combination common factor {factor!r} "
                    "must be a non-empty string"
                )
        else:
            value = definition.settings.get(factor)
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        factors[factor] = value
    return canonicalize_factors(factors)


def validate_environment_combination(combination: EnvironmentCombination) -> None:
    """Require canonical JSON and a matching combination digest."""

    if not isinstance(combination, EnvironmentCombination):
        raise TypeError("Expected EnvironmentCombination")
    if not COMBINATION_KEY_PATTERN.fullmatch(combination.key):
        raise ValueError("Environment combination key must be a SHA-256 digest")
    try:
        value = json.loads(combination.factors_json)
    except json.JSONDecodeError as exc:
        raise ValueError("Environment combination factors_json is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("Environment combination factors_json must contain an object")
    expected = canonicalize_factors(value)
    if expected != combination:
        raise ValueError("Environment combination is not canonical or digest-bound")


def validate_combination_for_definition(
    combination: EnvironmentCombination,
    definition: ValidationTestDefinition,
) -> None:
    """Require exact declared factor names and current immutable setting values."""

    validate_environment_combination(combination)
    health = definition.health
    if health is None or not health.enabled:
        raise ValueError("Environment combination definition is not health-enabled")
    factors = json.loads(combination.factors_json)
    if set(factors) != set(health.combination_factors):
        raise ValueError("Environment combination factor set does not match definition")
    for factor in health.combination_factors:
        if factor in _COMMON_FACTORS:
            if not isinstance(factors[factor], str) or not factors[factor].strip():
                raise ValueError(
                    f"Environment combination common factor {factor!r} must be a non-empty string"
                )
            continue
        if not _same_json_scalar(factors[factor], definition.settings.get(factor)):
            raise ValueError(
                f"Environment combination setting {factor!r} does not match definition"
            )


def valid_combination_key(value: str) -> bool:
    return value == "" or bool(COMBINATION_KEY_PATTERN.fullmatch(value))


def _same_json_scalar(left: Any, right: Any) -> bool:
    return type(left) is type(right) and left == right
