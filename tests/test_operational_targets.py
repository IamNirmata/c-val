"""Registry-derived raw export target catalog tests."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr
from dataclasses import FrozenInstanceError, replace

from cval.cli import build_parser
from cval.config import load_config
from cval.validation.operational_targets import (
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
        capabilities: tuple[str, ...] = ("export",),
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

    def test_default_catalog_preserves_registry_order(self) -> None:
        catalog = build_operational_target_catalog(self.registry)

        self.assertEqual(
            catalog.names_for(RESULTS_EXPORT),
            ("storage", "nccl", "dltest"),
        )
        target = catalog.require("dltest", RESULTS_EXPORT)
        self.assertEqual(target.owner_test_id, "dltest")
        self.assertEqual(target.status_test, "dltest")
        self.assertEqual(target.to_dict()["format_version"], "cval.operational-target.v2")

    def test_export_capability_selects_targets_and_keeps_registry_order(self) -> None:
        export_only = self._test(
            "storage",
            test_id="exporter",
            order=15,
            capabilities=("export",),
        )
        config_only = self._test(
            "storage",
            test_id="validator",
            order=25,
            capabilities=("config",),
        )
        registry = ValidationTestRegistry(
            (
                self.registry.require("storage"),
                export_only,
                self.registry.require("nccl"),
                config_only,
                self.registry.require("dltest"),
            )
        )
        catalog = build_operational_target_catalog(registry)

        self.assertEqual(
            catalog.names_for(RESULTS_EXPORT)[:4],
            ("storage", "exporter", "nccl", "dltest"),
        )
        self.assertNotIn("validator", catalog.names_for(RESULTS_EXPORT))

    def test_disabled_target_is_absent(self) -> None:
        disabled_dl = replace(self.registry.require("dltest"), enabled=False)
        registry = ValidationTestRegistry(
            (
                self.registry.require("storage"),
                self.registry.require("nccl"),
                disabled_dl,
            )
        )
        catalog = build_operational_target_catalog(registry)

        self.assertNotIn("dltest", catalog.names_for(RESULTS_EXPORT))
        with self.assertRaisesRegex(ValueError, "not enabled"):
            catalog.require("dltest", RESULTS_EXPORT)

    def test_reserved_names_are_rejected_even_when_disabled(self) -> None:
        for test_id in ("overall", "all"):
            with self.subTest(test_id=test_id):
                colliding = self._test(
                    "storage",
                    test_id=test_id,
                    order=99,
                    enabled=False,
                )
                registry = ValidationTestRegistry((*self.registry.tests, colliding))
                with self.assertRaisesRegex(ValueError, "reserved"):
                    build_operational_target_catalog(registry)

    def test_catalog_and_targets_are_immutable(self) -> None:
        catalog = build_operational_target_catalog(self.registry)
        self.assertIsInstance(catalog, OperationalTargetCatalog)
        with self.assertRaises(FrozenInstanceError):
            catalog.targets = ()  # type: ignore[misc]
        with self.assertRaises(FrozenInstanceError):
            catalog.targets[0].name = "changed"  # type: ignore[misc]

    def test_parser_exposes_only_raw_result_targets_and_flags(self) -> None:
        parser = build_parser(self.config)
        for target in ("overall", "all", "storage", "nccl", "dltest"):
            with self.subTest(command="results", target=target):
                self.assertEqual(
                    parser.parse_args(["results", "--test", target]).test,
                    target,
                )
        for args in (
            ["baseline", "build", "--test-type", "storage"],
            ["nccl-eval", "status"],
            ["results", "--test", "storage", "--classifications-only"],
            ["results", "--test", "storage", "--no-classification"],
            ["results", "--test", "dltest-compute"],
        ):
            with self.subTest(args=args), redirect_stderr(io.StringIO()), self.assertRaises(
                SystemExit
            ):
                parser.parse_args(args)

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
            parser.parse_args(["results", "--test", "synthetic"]).test,
            "synthetic",
        )

    def test_removed_hidden_enumeration_is_unparseable(self) -> None:
        parser = build_parser(self.config)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["operational-targets", "--operation", "results-export"])


if __name__ == "__main__":
    unittest.main()
