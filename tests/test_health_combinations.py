from __future__ import annotations

import json
import math
import unittest

from cval.config import load_config
from cval.health.combination import (
    canonicalize_factors,
    resolve_environment_combination,
    validate_environment_combination,
    validate_combination_for_definition,
)


class HealthCombinationTests(unittest.TestCase):
    def test_factor_order_is_canonical_and_digest_bound(self) -> None:
        first = canonicalize_factors({"image_name": "img", "iterations": 20})
        second = canonicalize_factors({"iterations": 20, "image_name": "img"})

        self.assertEqual(first, second)
        self.assertEqual(
            json.loads(first.factors_json),
            {"image_name": "img", "iterations": 20},
        )
        validate_environment_combination(first)

    def test_scalar_types_are_distinct(self) -> None:
        self.assertNotEqual(
            canonicalize_factors({"value": 1}).key,
            canonicalize_factors({"value": "1"}).key,
        )
        self.assertNotEqual(
            canonicalize_factors({"value": True}).key,
            canonicalize_factors({"value": 1}).key,
        )

    def test_missing_common_factor_makes_result_health_ineligible(self) -> None:
        definition = load_config().tests.registry.require("storage").definition

        combination = resolve_environment_combination(
            definition,
            {
                "image_name": "img",
                "cuda_version": "",
                "pytorch_version": "2.8",
            },
        )

        self.assertIsNone(combination)

    def test_settings_and_common_values_compose_exactly(self) -> None:
        definition = load_config().tests.registry.require("nccl").definition

        combination = resolve_environment_combination(
            definition,
            {
                "image_name": "img",
                "cuda_version": "12.9",
                "pytorch_version": "2.8",
            },
        )

        self.assertIsNotNone(combination)
        self.assertEqual(
            json.loads(combination.factors_json),
            {
                "cuda_version": "12.9",
                "data_size_gb": 8,
                "image_name": "img",
                "iterations": 20,
                "pytorch_version": "2.8",
            },
        )

    def test_rejects_non_scalar_empty_and_nonfinite_values(self) -> None:
        for value in ({"nested": 1}, [1], "", math.inf, math.nan, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                canonicalize_factors({"value": value})

    def test_rejects_forged_digest_or_noncanonical_json(self) -> None:
        combination = canonicalize_factors({"b": 2, "a": 1})
        from cval.health.models import EnvironmentCombination

        with self.assertRaisesRegex(ValueError, "digest-bound"):
            validate_environment_combination(
                EnvironmentCombination(combination.key, '{"b":2,"a":1}')
            )
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            validate_environment_combination(
                EnvironmentCombination("sha256:bad", combination.factors_json)
            )

    def test_candidate_combination_requires_exact_declared_factor_set(self) -> None:
        definition = load_config().tests.registry.require("storage").definition
        wrong = canonicalize_factors({"wrong-factor": "img"})
        with self.assertRaisesRegex(ValueError, "factor set"):
            validate_combination_for_definition(wrong, definition)

    def test_candidate_combination_setting_values_match_descriptor(self) -> None:
        definition = load_config().tests.registry.require("nccl").definition
        wrong = canonicalize_factors(
            {
                "image_name": "img",
                "cuda_version": "12.9",
                "pytorch_version": "2.8",
                "iterations": 999,
                "data_size_gb": 8,
            }
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            validate_combination_for_definition(wrong, definition)
        wrong_type = canonicalize_factors(
            {
                "image_name": "img",
                "cuda_version": "12.9",
                "pytorch_version": "2.8",
                "iterations": 20.0,
                "data_size_gb": 8,
            }
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            validate_combination_for_definition(wrong_type, definition)

    def test_setting_factor_integer_and_float_are_not_equivalent(self) -> None:
        definition = load_config().tests.registry.require("nccl").definition
        wrong = canonicalize_factors(
            {
                "image_name": "img",
                "cuda_version": "12.9",
                "pytorch_version": "2.8",
                "iterations": 20.0,
                "data_size_gb": 8,
            }
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            validate_combination_for_definition(wrong, definition)


if __name__ == "__main__":
    unittest.main()
