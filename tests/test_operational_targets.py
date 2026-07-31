"""U10 registry-derived compatibility target catalog tests."""

from __future__ import annotations

import argparse
import io
import json
import unittest
from contextlib import redirect_stdout
from dataclasses import FrozenInstanceError, replace

from cval.cli import build_parser, main
from cval.config import CvalConfig, TestsConfig, load_config
from cval.validation.operational_targets import (
    BASELINE_BUILD,
    BASELINE_CLASSIFY,
    BASELINE_LIST,
    CLASSIFICATIONS_EXPORT,
    RESULTS_EXPORT,
    OperationalTargetCatalog,
    build_operational_target_catalog,
)
from cval.validation.registry import ValidationTestRegistry


class OperationalTargetCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config()
        self.registry = self.config.tests.registry

    def _test(
        self,
        source_id: str,
        *,
        test_id: str,
        order: int,
        enabled: bool = True,
        capabilities: tuple[str, ...] = ("baseline", "export"),
    ):
        source = self.registry.require(source_id)
        metadata = replace(source.definition.metadata, id=test_id, order=order)
        plugin = replace(source.definition.plugin, capabilities=capabilities)
        definition = replace(source.definition, metadata=metadata, plugin=plugin)
        return replace(
            source,
            enabled=enabled,
            config_path=f"validation-tests/{test_id}/test_config.toml",
            definition=definition,
        )

    def test_default_catalog_preserves_registry_then_overlay_order(self) -> None:
        catalog = build_operational_target_catalog(self.registry)

        self.assertEqual(
            catalog.names_for(BASELINE_CLASSIFY),
            (
                "storage",
                "nccl",
                "dltest",
                "dltest-numerical",
                "dltest-compute",
                "dltest-collective",
                "dltest-overlap",
            ),
        )
        self.assertEqual(
            catalog.names_for(BASELINE_BUILD), ("storage", "nccl", "dltest")
        )
        self.assertEqual(catalog.names_for(BASELINE_LIST), ("storage", "nccl", "dltest"))
        self.assertEqual(
            catalog.names_for(RESULTS_EXPORT),
            (
                "storage",
                "nccl",
                "dltest",
                "dltest-numerical",
                "dltest-compute",
                "dltest-collective",
                "dltest-overlap",
            ),
        )
        alias = catalog.require("dltest-compute", BASELINE_CLASSIFY)
        self.assertTrue(alias.alias)
        self.assertEqual(alias.owner_test_id, "dltest")
        self.assertEqual(alias.component, "compute_performance")
        self.assertEqual(alias.refresh_group, "dltest")

    def test_capabilities_select_operations_and_keep_registry_order(self) -> None:
        export_only = self._test(
            "storage",
            test_id="exporter",
            order=15,
            capabilities=("export",),
        )
        baseline_only = self._test(
            "storage",
            test_id="metric",
            order=25,
            capabilities=("baseline",),
        )
        registry = ValidationTestRegistry(
            (
                self.registry.require("storage"),
                export_only,
                self.registry.require("nccl"),
                baseline_only,
                self.registry.require("dltest"),
            )
        )
        catalog = build_operational_target_catalog(registry)

        self.assertEqual(
            catalog.names_for(RESULTS_EXPORT)[:4],
            ("storage", "exporter", "nccl", "dltest"),
        )
        self.assertNotIn("exporter", catalog.names_for(BASELINE_BUILD))
        self.assertIn("metric", catalog.names_for(BASELINE_BUILD))
        self.assertNotIn("metric", catalog.names_for(RESULTS_EXPORT))
        self.assertIn("metric", catalog.names_for(CLASSIFICATIONS_EXPORT))

    def test_disabled_owner_and_its_aliases_are_absent(self) -> None:
        disabled_dl = replace(self.registry.require("dltest"), enabled=False)
        registry = ValidationTestRegistry(
            (
                self.registry.require("storage"),
                self.registry.require("nccl"),
                disabled_dl,
            )
        )
        catalog = build_operational_target_catalog(registry)

        self.assertNotIn("dltest", catalog.names_for(BASELINE_BUILD))
        self.assertFalse(
            any(name.startswith("dltest") for name in catalog.names_for(RESULTS_EXPORT))
        )
        with self.assertRaisesRegex(ValueError, "not enabled"):
            catalog.require("dltest-compute", BASELINE_CLASSIFY)

    def test_reserved_and_dl_alias_collisions_are_rejected_even_when_disabled(self) -> None:
        for test_id, message in (
            ("overall", "reserved"),
            ("all", "reserved"),
            ("dltest-compute", "compatibility target aliases"),
        ):
            with self.subTest(test_id=test_id):
                colliding = self._test(
                    "storage",
                    test_id=test_id,
                    order=99,
                    enabled=False,
                )
                registry = ValidationTestRegistry((*self.registry.tests, colliding))
                with self.assertRaisesRegex(ValueError, message):
                    build_operational_target_catalog(registry)

    def test_catalog_and_targets_are_immutable(self) -> None:
        catalog = build_operational_target_catalog(self.registry)
        self.assertIsInstance(catalog, OperationalTargetCatalog)
        with self.assertRaises(FrozenInstanceError):
            catalog.targets = ()  # type: ignore[misc]
        with self.assertRaises(FrozenInstanceError):
            catalog.targets[0].name = "changed"  # type: ignore[misc]

    def test_parser_retains_all_old_aliases(self) -> None:
        parser = build_parser(self.config)
        for command in (
            ("baseline", "classify", "--test-type"),
            ("results", "--test"),
            ("classifications", "--test"),
        ):
            for target in (
                "storage",
                "nccl",
                "dltest",
                "dltest-numerical",
                "dltest-compute",
                "dltest-collective",
                "dltest-overlap",
            ):
                with self.subTest(command=command, target=target):
                    args = [*command, target]
                    if command[0] == "baseline":
                        args.append("--output")
                        args.append("json")
                    parsed = parser.parse_args(args)
                    self.assertIsInstance(parsed, argparse.Namespace)

    def test_parser_discovers_synthetic_target_without_cli_constant_edit(self) -> None:
        synthetic = self._test("storage", test_id="synthetic", order=15)
        registry = ValidationTestRegistry(
            (
                self.registry.require("storage"),
                synthetic,
                self.registry.require("nccl"),
                self.registry.require("dltest"),
            )
        )
        config = replace(self.config, tests=replace(self.config.tests, registry=registry))
        parser = build_parser(config)

        self.assertEqual(
            parser.parse_args(
                ["baseline", "build", "--test-type", "synthetic"]
            ).test_type,
            "synthetic",
        )
        self.assertEqual(
            parser.parse_args(
                ["baseline", "classify", "--test-type", "synthetic"]
            ).test_type,
            "synthetic",
        )
        self.assertEqual(
            parser.parse_args(["results", "--test", "synthetic"]).test,
            "synthetic",
        )
        self.assertEqual(
            parser.parse_args(["classifications", "--test", "synthetic"]).test,
            "synthetic",
        )

    def test_hidden_enumeration_is_plain_tsv_or_json_not_shell_code(self) -> None:
        tsv = io.StringIO()
        with redirect_stdout(tsv):
            self.assertEqual(
                main(
                    [
                        "operational-targets",
                        "--operation",
                        "baseline-build",
                        "--output",
                        "tsv",
                    ]
                ),
                0,
            )
        lines = tsv.getvalue().splitlines()
        fields = [line.split("\t") for line in lines]
        self.assertTrue(all(len(row) == 7 for row in fields))
        self.assertTrue(
            all(row[0] == "cval.operational-target.v1" for row in fields)
        )
        self.assertEqual([row[1] for row in fields], ["storage", "nccl", "dltest"])
        self.assertFalse(any("=" in line or "eval" in line for line in lines))

        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(
                main(
                    [
                        "operational-targets",
                        "--operation",
                        "results-export",
                        "--output",
                        "json",
                    ]
                ),
                0,
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(payload[0]["name"], "storage")
        self.assertIn("results-export", payload[0]["operations"])


if __name__ == "__main__":
    unittest.main()
