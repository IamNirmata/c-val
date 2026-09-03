from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cval.cli import main
from cval.k8s.discovery import NodeStatus
from cval.models import LatestStatusRow
from cval.validation.plugins import ExportRows


class CliTests(unittest.TestCase):
    def test_nodes_inventory_only_lists_gpu_names_without_full_discovery(self) -> None:
        output = io.StringIO()
        with (
            patch(
                "cval.cli.discover_gpu_node_names",
                return_value=["slc01-cl02-hgx-0001", "slc01-cl02-hgx-0002"],
            ) as inventory,
            patch("cval.cli.discover_free_nodes") as full_discovery,
            redirect_stdout(output),
        ):
            exit_code = main(["nodes", "--inventory-only", "--output", "json"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            json.loads(output.getvalue()),
            {
                "nodes": ["slc01-cl02-hgx-0001", "slc01-cl02-hgx-0002"],
                "node_count": 2,
            },
        )
        inventory.assert_called_once()
        full_discovery.assert_not_called()

    def test_nodes_check_node_reports_targeted_eligibility(self) -> None:
        output = io.StringIO()
        status = NodeStatus(
            name="slc01-cl02-hgx-0001",
            found=True,
            is_gpu_node=True,
            schedulable=True,
            resource_ready=True,
            capacity=8,
            allocatable=8,
            used=0,
            free=8,
            fully_free=True,
            reason="free and schedulable",
            ready=True,
            status_label="ready",
        )
        with patch("cval.cli.describe_node", return_value=status) as describe, redirect_stdout(output):
            exit_code = main(
                [
                    "nodes",
                    "--check-node",
                    status.name,
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        payload = json.loads(output.getvalue())
        self.assertTrue(payload["eligible"])
        self.assertEqual(payload["name"], status.name)
        describe.assert_called_once()

    def test_nodes_check_node_rejects_cordoned_node_with_blocking_taint(self) -> None:
        output = io.StringIO()
        status = NodeStatus(
            name="slc01-cl02-hgx-0001",
            found=True,
            is_gpu_node=True,
            schedulable=False,
            resource_ready=True,
            capacity=8,
            allocatable=8,
            used=0,
            free=8,
            fully_free=True,
            reason="node carries a blocking NoSchedule taint",
            cordoned=True,
            ready=True,
            status_label="unschedulable",
        )
        with patch("cval.cli.describe_node", return_value=status), redirect_stdout(output):
            exit_code = main(
                ["nodes", "--check-node", status.name, "--output", "json"]
            )

        self.assertEqual(exit_code, 0)
        self.assertFalse(json.loads(output.getvalue())["eligible"])

    def test_help_lists_only_preferred_commands(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            with self.assertRaises(SystemExit) as exc:
                main(["--help"])

        self.assertEqual(exc.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn(
            "{config,tests,nodes,validate,status,plan,run,jobs,results}",
            help_text,
        )
        self.assertNotIn("prioritize", help_text)
        self.assertNotIn("run-batch", help_text)
        self.assertNotIn("db-add-result", help_text)
        for removed in ("history", "compatibility", "health", "evaluator-preflight"):
            self.assertNotIn(f"    {removed}", help_text)

    def test_results_command_writes_csv(self) -> None:
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "cval.cli.get_latest_status_rows",
                return_value=[
                    LatestStatusRow("node-b", "all", 200, "pass"),
                    LatestStatusRow("node-a", "all", 100, "fail"),
                    LatestStatusRow("node-a", "storage", 100, "pass"),
                ],
            ):
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "results",
                            "--test",
                            "overall",
                            "--type",
                            "csv",
                            "--output-dir",
                            tmpdir,
                            "--no-metrics",
                        ]
                    )

            files = list(Path(tmpdir).glob("cval_overall_*.csv"))
            text = files[0].read_text(encoding="utf-8")

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(files), 1)
        self.assertIn("Wrote 2 overall latest result row(s)", output.getvalue())
        self.assertNotIn("classification", text)

    def test_plugin_results_command_reports_export_row_count(self) -> None:
        output = io.StringIO()
        exported = ExportRows(("node",), (("node-a",), ("node-b",)))

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "cval.cli.get_latest_status_rows",
                return_value=[LatestStatusRow("node-a", "storage", 100, "pass")],
            ), patch(
                "cval.validation.operations.export_result_rows",
                return_value=exported,
            ):
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "results",
                            "--test",
                            "storage",
                            "--no-metrics",
                            "--output-dir",
                            tmpdir,
                        ]
                    )

        self.assertEqual(exit_code, 0)
        self.assertIn("Wrote 2 storage latest result row(s)", output.getvalue())

    def test_nccl_raw_export_uses_plugin_rows(self) -> None:
        output = io.StringIO()
        exported = ExportRows(("node", "BUS_BW"), (("node-a", "44.0000"),))

        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "cval.cli.get_latest_status_rows",
            return_value=[LatestStatusRow("node-a", "nccl", 100, "pass")],
        ), patch(
            "cval.validation.operations.export_result_rows",
            return_value=exported,
        ), redirect_stdout(output):
            exit_code = main(
                [
                    "results",
                    "--test",
                    "nccl",
                    "--output-dir",
                    tmpdir,
                ]
            )

        self.assertEqual(exit_code, 0)

    def test_removed_results_classification_flags_are_rejected(self) -> None:
        for flag in (
            "--classifications-only",
            "--classification-db-path=/tmp/removed.db",
            "--no-classification",
        ):
            with self.subTest(flag=flag), redirect_stderr(io.StringIO()), self.assertRaises(
                SystemExit
            ) as exc:
                main(["results", "--test", "storage", flag])
            self.assertEqual(exc.exception.code, 2)

    def test_config_command_prints_effective_config(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(["config"])

        self.assertEqual(exit_code, 0)
        config = json.loads(output.getvalue())
        self.assertEqual(config["job"]["job_prefix"], "cval")
        self.assertEqual(config["cluster"]["namespace"], "gcr-admin")
        self.assertEqual(
            config["tests"]["nccl"]["config_path"],
            "validation-tests/nccl/test_config.toml",
        )

    def test_tests_list_command_prints_registry_json(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(["tests", "list", "--output", "json"])

        self.assertEqual(exit_code, 0)
        rows = json.loads(output.getvalue())
        self.assertEqual([row["id"] for row in rows], ["storage", "nccl", "dltest"])
        self.assertTrue(all(row["schema_version"] == "cval.test.v1" for row in rows))

    def test_tests_describe_command_prints_effective_settings(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(["tests", "describe", "nccl"])

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["metadata"]["id"], "nccl")
        self.assertEqual(result["settings"]["iterations"], 20)
        self.assertEqual(result["requirements"]["gpu_count"], 8)

    def test_tests_validate_command_reports_counts(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(["tests", "validate", "--output", "json"])

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertTrue(result["valid"])
        self.assertEqual(result["registered_count"], 3)
        self.assertEqual(result["enabled_count"], 3)


    def test_invalid_config_is_reported_without_traceback(self) -> None:
        stderr = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[tests.nccl]
enabled = true
iterations = 99
""",
                encoding="utf-8",
            )
            with redirect_stderr(stderr):
                exit_code = main(["--config", str(config_path), "tests", "validate"])

        self.assertEqual(exit_code, 2)
        self.assertIn("Configuration error:", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_dl_rebuild_uses_exact_paths_from_cli_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_root = root / "results"
            rank_path = (
                results_root
                / "dltest-node-a-123/workdir/test_plans/plan/runs/example_RANK0.json"
            )
            rank_path.parent.mkdir(parents=True)
            rank_path.write_text(
                json.dumps(
                    {
                        "runID": "example_RANK0",
                        "test_plan": "plan",
                        "nn_tasks": [
                            {
                                "task_name": "linear",
                                "status": "completed",
                                "norm_output": 1.0,
                                "fp_gpu_time": 2.0,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            custom_paths = {
                "dl_numerical_db_path": root / "one/numerical.custom.db",
                "dl_compute_db_path": root / "two/compute.custom.db",
                "dl_collective_db_path": root / "three/collective.custom.db",
                "dl_overlap_db_path": root / "four/overlap.custom.db",
            }
            config_path = root / "cval.toml"
            config_path.write_text(
                "[storage]\n"
                + "\n".join(
                    f'{key} = "{path}"' for key, path in custom_paths.items()
                )
                + f'\n[runtime]\ndl_results_root_path = "{results_root}"\n'
                + "\n",
                encoding="utf-8",
            )
            output = io.StringIO()

            with redirect_stdout(output):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "db-rebuild-dltest-metrics",
                        "--results-root",
                        str(results_root),
                        "--output",
                        "json",
                    ]
                )

            receipt = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            set(receipt["db_paths"].values()),
            {str(path) for path in custom_paths.values()},
        )

    def test_config_file_overrides_cli_defaults(self) -> None:
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[scheduling]
batch_size = 1

[job]
job_prefix = "custom"
""",
                encoding="utf-8",
            )
            with redirect_stdout(output):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "plan",
                        "--free-nodes",
                        "slc01-cl02-hgx-0001,slc01-cl02-hgx-0002",
                        "--timestamp",
                        "12345",
                        "--git-ref",
                        "a" * 40,
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(len(result["planned_jobs"]), 1)
        self.assertEqual(
            result["planned_jobs"][0]["job_name"],
            "custom-slc01-cl02-hgx-0001-pytorch-26-05-py3-12345",
        )

    def test_removed_commands_are_not_parseable(self) -> None:
        for command in (
            "history",
            "db-upsert-run-history",
            "db-ingest-test-results",
            "db-preflight-test-results",
            "health",
            "compatibility",
            "baseline",
            "nccl-eval",
        ):
            with self.subTest(command=command), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as exc:
                    main([command])
                self.assertEqual(exc.exception.code, 2)

    def test_plan_command_uses_provided_free_nodes(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "plan",
                    "--free-nodes",
                    "slc01-cl02-hgx-0001,slc01-cl02-hgx-0002",
                    "--batch-size",
                    "1",
                    "--timestamp",
                    "12345",
                    "--git-ref",
                    "a" * 40,
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        plan = json.loads(output.getvalue())
        self.assertEqual(
            set(plan),
            {
                "batch_size",
                "days_threshold",
                "free_nodes_count",
                "queue_count",
                "planned_jobs",
            },
        )
        self.assertEqual(plan["free_nodes_count"], 2)
        self.assertEqual(plan["queue_count"], 2)
        self.assertEqual(len(plan["planned_jobs"]), 1)

    def test_plan_command_treats_explicit_empty_free_nodes_as_an_empty_snapshot(self) -> None:
        output = io.StringIO()

        with patch("cval.cli.discover_free_nodes") as discover:
            with redirect_stdout(output):
                exit_code = main(
                    [
                        "plan",
                        "--free-nodes",
                        "",
                        "--batch-size",
                        "1",
                        "--timestamp",
                        "12345",
                        "--git-ref",
                        "a" * 40,
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 0)
        discover.assert_not_called()
        plan = json.loads(output.getvalue())
        self.assertEqual(plan["free_nodes_count"], 0)
        self.assertEqual(plan["queue_count"], 0)
        self.assertEqual(plan["planned_jobs"], [])

    def test_plan_command_can_use_live_status(self) -> None:
        output = io.StringIO()

        with patch(
            "cval.cli.get_latest_status_rows",
            return_value=[LatestStatusRow("slc01-cl02-hgx-0001", "all", 200, "pass")],
        ):
            with redirect_stdout(output):
                exit_code = main(
                    [
                        "plan",
                        "--free-nodes",
                        "slc01-cl02-hgx-0001,slc01-cl02-hgx-0002",
                        "--live-status",
                        "--batch-size",
                        "2",
                        "--timestamp",
                        "12345",
                        "--git-ref",
                        "a" * 40,
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 0)
        plan = json.loads(output.getvalue())
        self.assertEqual(plan["queue_count"], 2)
        self.assertEqual(plan["planned_jobs"][0]["node"], "slc01-cl02-hgx-0002")
        self.assertEqual(plan["planned_jobs"][0]["reason"], "never-tested")

    def test_plan_rejects_fail_closed_default_commit(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = main(["plan", "--free-nodes", ""])

        self.assertEqual(exit_code, 2)
        self.assertIn("exact lowercase 40-hex commit", stderr.getvalue())

    def test_run_without_submit_is_rejected(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = main(
                [
                    "run",
                    "--free-nodes",
                    "slc01-cl02-hgx-0001",
                ]
            )

        self.assertEqual(exit_code, 2)
        self.assertIn("use plan for read-only queue inspection", stderr.getvalue())

    def test_submit_plan_submit_requires_confirmation(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = main(
                [
                    "run",
                    "--free-nodes",
                    "slc01-cl02-hgx-0001",
                    "--git-ref",
                    "a" * 40,
                    "--submit",
                ]
            )

        self.assertEqual(exit_code, 2)
        self.assertIn("Policy violation", stderr.getvalue())

    def test_run_submit_rejects_moving_ref(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = main(
                [
                    "run",
                    "--free-nodes",
                    "slc01-cl02-hgx-0001",
                    "--git-ref",
                    "main",
                    "--submit",
                    "--confirm",
                    "submit",
                ]
            )

        self.assertEqual(exit_code, 2)
        self.assertIn("exact lowercase 40-hex commit", stderr.getvalue())

    def test_validate_requires_git_ref_at_parse_time(self) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
            main(["validate", "--node", "node-x", "--submit", "--confirm", "submit"])

        self.assertEqual(exc.exception.code, 2)

    def test_validate_without_submit_never_enters_orchestration(self) -> None:
        stderr = io.StringIO()

        with patch(
            "cval.orchestrator.validate.run_node_validation"
        ) as run_validation, redirect_stderr(stderr):
            exit_code = main(
                [
                    "validate",
                    "--node",
                    "node-x",
                    "--git-ref",
                    "a" * 40,
                ]
            )

        self.assertEqual(exit_code, 2)
        run_validation.assert_not_called()
        self.assertIn("explicit --submit", stderr.getvalue())

if __name__ == "__main__":
    unittest.main()