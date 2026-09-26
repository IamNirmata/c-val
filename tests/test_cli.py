from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cval.cli import build_parser, main
from cval.config import encode_config_snapshot, load_config
from cval.jobs.manager import JobSubmissionResult
from cval.jobs.renderer import render_validation_job_from_file
from cval.k8s.client import CommandResult
from cval.k8s.discovery import NodeStatus
from cval.models import LatestStatusRow
from cval.validation.plugins import ExportRows


class CliTests(unittest.TestCase):
    def test_c1_redundant_toml_setting_flags_are_removed(self) -> None:
        parser = build_parser(load_config())
        cases = (
            (["nodes"], ("--node-filter=hgx",)),
            (["plan"], ("--node-filter=hgx", "--threshold-days=7")),
            (["run"], ("--node-filter=hgx", "--threshold-days=7", "--batch-size=3")),
            (["jobs", "--jobs", "job-a"], ("--timeout-seconds=6000", "--poll-interval-seconds=60")),
            (["validate", "--node", "node-a", "--git-ref", "a" * 40],
             ("--poll-interval=3", "--timeout-seconds=6000", "--pending-timeout=600")),
        )
        for command, flags in cases:
            for flag in flags:
                with self.subTest(command=command, flag=flag), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    parser.parse_args([*command, flag])
                self.assertEqual(error.exception.code, 2)
        self.assertEqual(parser.parse_args(["plan", "--batch-size", "1"]).batch_size, 1)

    def test_c1_commands_use_pinned_settings_without_duplicate_arguments(self) -> None:
        base = load_config()
        config = replace(base, cluster=replace(base.cluster, node_filter="reviewed-hgx"),
                         scheduling=replace(base.scheduling, days_threshold=9, batch_size=2),
                         monitoring=replace(base.monitoring, timeout_seconds=7000, poll_interval_seconds=30,
                                            validation_poll_seconds=5, validation_pending_seconds=900))
        with patch.dict(os.environ, {"CVAL_CONFIG_SNAPSHOT_B64": encode_config_snapshot(config)}, clear=True):
            with patch("cval.cli.discover_gpu_node_names", return_value=[]) as inventory, redirect_stdout(io.StringIO()):
                self.assertEqual(main(["nodes", "--inventory-only", "--output", "json"]), 0)
                inventory.assert_called_once_with(config=config)
            with patch("cval.cli.monitor_jobs_until_terminal", return_value=[]) as watch, redirect_stdout(io.StringIO()):
                self.assertEqual(main(["jobs", "--jobs", "job-a", "--watch", "--output", "json"]), 0)
                watch.assert_called_once_with(["job-a"], config=config, namespace=config.cluster.namespace)
            with patch("cval.orchestrator.validate.run_node_validation", return_value={"ok": True}) as validate, redirect_stdout(io.StringIO()):
                self.assertEqual(main(["validate", "--node", "node-a", "--git-ref", "a" * 40,
                                       "--submit", "--confirm", "submit", "--output", "json"]), 0)
                self.assertEqual(validate.call_args.kwargs["config"], config)
                self.assertTrue(validate.call_args.kwargs["submit"])
                self.assertFalse({"poll_interval", "overall_timeout", "pending_timeout"} & validate.call_args.kwargs.keys())
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(main(["plan", "--free-nodes", "node-a,node-b,node-c", "--batch-size", "1",
                                       "--git-ref", "a" * 40, "--output", "json"]), 0)
            plan = json.loads(output.getvalue())
            self.assertEqual((plan["days_threshold"], plan["batch_size"], len(plan["candidates"])), (9, 1, 1))
            with patch("cval.cli.submit_workflow_plan", return_value=JobSubmissionResult(config.cluster.namespace, [])) as submit, redirect_stdout(io.StringIO()):
                self.assertEqual(main(["run", "--free-nodes", "node-a,node-b,node-c", "--git-ref", "a" * 40,
                                       "--timestamp", "123", "--submit", "--confirm", "submit", "--output", "json"]), 0)
                self.assertEqual(len(submit.call_args.args[0].planned_jobs), 2)
                self.assertEqual(submit.call_args.kwargs["config"], config)

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
        self.assertNotIn("    prioritize ", help_text)
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

    def test_single_choice_format_flags_are_removed(self) -> None:
        for arguments in (
            ["config", "--output", "json"],
            ["results", "--test", "overall", "--type", "csv"],
        ):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    main(arguments)
                self.assertEqual(error.exception.code, 2)

    def test_removed_operator_overrides_and_tsv_are_rejected(self) -> None:
        cases = (
            (["status"], ("--pod=other", "--namespace=other", "--db-path=/other.db", "--output=tsv")),
            (["plan"], ("--db-status-tsv=old.tsv", "--status-pod=other", "--status-namespace=other", "--status-db-path=/other.db")),
            (["run"], ("--db-status-tsv=old.tsv", "--template=other.yml", "--job-prefix=other", "--git-repo=other", "--namespace=other", "--allowed-namespace=other", "--max-batch-size=99")),
            (["results", "--test", "overall"], ("--pod=other", "--namespace=other", "--db-path=/other.db", "--nccl-db-path=/other.db", "--storage-db-path=/other.db")),
            (["jobs", "--jobs", "job-a"], ("--namespace=other",)),
            (["validate", "--node", "node-a", "--git-ref", "a" * 40], ("--namespace=other", "--pvc-pod=other", "--download", "--download-dir=/tmp/unused")),
        )
        for command, flags in cases:
            for flag in flags:
                with self.subTest(command=command, flag=flag), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as error:
                        main([*command, flag])
                    self.assertEqual(error.exception.code, 2)

    def test_reviewed_config_owns_submission_and_status_targets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "cval.toml"
            config_path.write_text(
                '[cluster]\nnamespace = "reviewed"\npvc_access_pod = "reviewed-reader"\n'
                '[storage]\nvalidation_db_path = "/reviewed/status.db"\n'
                '[job]\njob_prefix = "reviewed"\ngit_repo = "https://example.invalid/c-val.git"\n'
                '[job_template]\nnamespace = "reviewed"\ncpu = "101"\n'
                '[policy]\nnamespace_allowlist = ["reviewed"]\nmax_batch_size = 1\n'
            )
            prefix = ["--config", str(config_path)]
            with patch("cval.cli.get_latest_status_rows", return_value=[]) as status, redirect_stdout(io.StringIO()):
                self.assertEqual(main([*prefix, "status", "--output", "json"]), 0)
                config = status.call_args.kwargs["config"]
                self.assertEqual(config.cluster.namespace, "reviewed")
                self.assertEqual(config.cluster.pvc_access_pod, "reviewed-reader")
                self.assertEqual(config.storage.validation_db_path, "/reviewed/status.db")
                self.assertEqual(main([*prefix, "plan", "--free-nodes", "node-a", "--live-status", "--git-ref", "a" * 40]), 0)
                self.assertEqual(status.call_args.kwargs["config"].cluster.namespace, "reviewed")
            with (
                patch("cval.orchestrator.workflow.render_validation_job_from_file", wraps=render_validation_job_from_file) as render,
                patch("cval.k8s.client.KubectlClient.run", return_value=CommandResult([], "created", "", 0)) as kubectl,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main([*prefix, "run", "--free-nodes", "node-a", "--batch-size", "1", "--git-ref", "a" * 40, "--submit", "--confirm", "submit"]), 0)
                self.assertEqual(kubectl.call_args.args[0], ["create", "-n", "reviewed", "-f", "-"])
                self.assertEqual(render.call_args.kwargs["job_prefix"], "reviewed")
                self.assertEqual(render.call_args.kwargs["git_repo"], "https://example.invalid/c-val.git")
                self.assertEqual(render.call_args.kwargs["job_template_config"].cpu, "101")
                kubectl.reset_mock()
                with redirect_stderr(io.StringIO()):
                    self.assertEqual(main([*prefix, "run", "--free-nodes", "node-a,node-b", "--batch-size", "2", "--git-ref", "a" * 40, "--submit", "--confirm", "submit"]), 2)
                kubectl.assert_not_called()

    def test_reviewed_config_owns_validation_monitoring_and_export_targets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "cval.toml"
            config_path.write_text(
                '[cluster]\nnamespace = "reviewed"\npvc_access_pod = "reviewed-reader"\n'
                '[storage]\nvalidation_db_path = "/reviewed/status.db"\n'
                'nccl_db_path = "/reviewed/nccl.db"\n'
                'storage_db_path = "/reviewed/storage.db"\n'
            )
            prefix = ["--config", str(config_path)]
            with (
                patch("cval.orchestrator.validate.run_node_validation", return_value={"ok": True}) as validate,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main([
                    *prefix, "validate", "--node", "node-a", "--git-ref", "a" * 40,
                    "--submit", "--confirm", "submit", "--output", "json",
                ]), 0)
                self.assertEqual(validate.call_args.args, ("node-a",))
                settings = validate.call_args.kwargs["config"]
                self.assertEqual(settings.cluster.namespace, "reviewed")
                self.assertEqual(settings.cluster.pvc_access_pod, "reviewed-reader")
                self.assertEqual(settings.storage.validation_db_path, "/reviewed/status.db")
                self.assertTrue(validate.call_args.kwargs["submit"])
                self.assertEqual(validate.call_args.kwargs["confirmation"], "submit")
            with (
                patch("cval.cli.get_job_phases", return_value=[]) as snapshot,
                patch("cval.cli.monitor_jobs_until_terminal", return_value=[]) as watch,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main([*prefix, "jobs", "--jobs", "job-a"]), 0)
                self.assertEqual(main([*prefix, "jobs", "--jobs", "job-a", "--watch"]), 0)
                self.assertEqual(snapshot.call_args.kwargs["namespace"], "reviewed")
                self.assertEqual(watch.call_args.kwargs["namespace"], "reviewed")
            with (
                patch("cval.cli.get_latest_status_rows", return_value=[]) as status,
                patch("cval.storage.metrics.get_latest_nccl_metrics", return_value={}) as nccl,
                patch("cval.storage.metrics.get_latest_storage_metrics", return_value={}) as storage,
                patch("cval.validation.operations.export_result_rows", return_value=ExportRows(("node",), ())) as export,
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main([
                    *prefix, "results", "--test", "overall", "--output-dir", directory,
                ]), 0)
                self.assertEqual(status.call_args.kwargs["config"].storage.validation_db_path, "/reviewed/status.db")
                self.assertEqual(nccl.call_args.kwargs["config"].storage.nccl_db_path, "/reviewed/nccl.db")
                self.assertEqual(storage.call_args.kwargs["config"].storage.storage_db_path, "/reviewed/storage.db")
                self.assertEqual(main([
                    *prefix, "results", "--test", "nccl", "--output-dir", directory,
                ]), 0)
                context = export.call_args.args[2]
                self.assertEqual(context.namespace, "reviewed")
                self.assertEqual(context.pod, "reviewed-reader")
                self.assertEqual(dict(context.source_db_paths), {
                    "nccl": "/reviewed/nccl.db", "storage": "/reviewed/storage.db",
                })

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
                        "--git-ref",
                        "a" * 40,
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["batch_size"], 1)
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0]["node"], "slc01-cl02-hgx-0001")

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

    def test_plan_preserves_priority_and_limit_without_rendering(self) -> None:
        output = io.StringIO()
        with (
            patch("cval.orchestrator.workflow.render_validation_job_from_file") as render,
            patch("cval.cli.submit_workflow_plan") as submit,
            patch("cval.cli.discover_free_nodes") as discover,
            patch(
                "cval.cli.get_latest_status_rows",
                return_value=[LatestStatusRow("node-a", "all", 100, "pass")],
            ),
            redirect_stdout(output),
        ):
            exit_code = main([
                "plan", "--free-nodes", "node-c,node-a,node-b",
                "--live-status", "--batch-size", "1", "--git-ref", "a" * 40,
                "--output", "json",
            ])

        self.assertEqual(exit_code, 0)
        render.assert_not_called()
        submit.assert_not_called()
        discover.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertEqual(set(payload), {
            "batch_size", "days_threshold", "free_nodes_count", "queue_count",
            "git_ref", "candidates",
        })
        self.assertEqual(payload["git_ref"], "a" * 40)
        self.assertEqual(payload["batch_size"], 1)
        self.assertEqual(payload["free_nodes_count"], 3)
        self.assertEqual(payload["queue_count"], 3)
        self.assertEqual(payload["candidates"], [{
            "node": "node-b", "priority": 1, "last_tested_timestamp": 0,
            "age_days": None, "reason": "never-tested",
        }])

    def test_candidate_inspection_then_submission_renders_only_selected_node(self) -> None:
        output = io.StringIO()
        with (
            patch(
                "cval.orchestrator.workflow.render_validation_job_from_file",
                wraps=render_validation_job_from_file,
            ) as render,
            patch(
                "cval.k8s.client.KubectlClient.run",
                return_value=CommandResult([], "created", "", 0),
            ) as kubectl,
        ):
            with redirect_stdout(output):
                exit_code = main([
                    "plan", "--free-nodes", "node-a,node-b",
                    "--batch-size", "2", "--git-ref", "a" * 40, "--output", "json",
                ])
            self.assertEqual(exit_code, 0)
            candidates = json.loads(output.getvalue())["candidates"]
            render.assert_not_called()
            kubectl.assert_not_called()

            selected_node = candidates[1]["node"]
            with redirect_stdout(io.StringIO()):
                exit_code = main([
                    "run", "--free-nodes", selected_node, "--batch-size", "1",
                    "--timestamp", "12345", "--git-ref", "a" * 40,
                    "--submit", "--confirm", "submit", "--output", "json",
                ])

        self.assertEqual(exit_code, 0)
        render.assert_called_once()
        self.assertEqual(render.call_args.kwargs["node_name"], "node-b")
        kubectl.assert_called_once()
        self.assertEqual(kubectl.call_args.args[0], ["create", "-n", "gcr-admin", "-f", "-"])
        self.assertIn('kubernetes.io/hostname: "node-b"', kubectl.call_args.kwargs["input_text"])

    def test_candidate_inspection_does_not_bypass_submission_template_validation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing_template = str(Path(directory) / "missing-job.yml")
            config_path = Path(directory) / "cval.toml"
            config_path.write_text(f'[job]\ntemplate_path = "{missing_template}"\n')
            prefix = ["--config", str(config_path)]
            shared = [
                "--free-nodes", "node-a", "--git-ref", "a" * 40,
            ]
            with patch("cval.cli.submit_workflow_plan") as submit, redirect_stdout(io.StringIO()):
                self.assertEqual(main([*prefix, "plan", *shared]), 0)
                with self.assertRaises(FileNotFoundError):
                    main([*prefix, "run", *shared, "--submit", "--confirm", "submit"])
            submit.assert_not_called()

    def test_plan_empty_snapshot_and_table(self) -> None:
        for nodes in ("", "node-a"):
            for output_format in ("json", "table"):
                with self.subTest(nodes=nodes, output=output_format):
                    output = io.StringIO()
                    with (
                        patch("cval.cli.discover_free_nodes") as discover,
                        patch("cval.orchestrator.workflow.render_validation_job_from_file") as render,
                        redirect_stdout(output),
                    ):
                        exit_code = main([
                            "plan", "--free-nodes", nodes,
                            "--git-ref", "a" * 40, "--output", output_format,
                        ])
                    self.assertEqual(exit_code, 0)
                    discover.assert_not_called()
                    render.assert_not_called()
                    if output_format == "json":
                        payload = json.loads(output.getvalue())
                        self.assertEqual(payload["queue_count"], int(bool(nodes)))
                        self.assertEqual(len(payload["candidates"]), int(bool(nodes)))
                    else:
                        self.assertIn("jobs not rendered", output.getvalue())
                        if nodes:
                            self.assertIn(nodes, output.getvalue())

    def test_plan_requires_exact_ref_before_inputs(self) -> None:
        for revision in ("main", "0" * 40):
            with (
                self.subTest(revision=revision),
                patch("cval.cli._load_plan_inputs") as inputs,
                redirect_stderr(io.StringIO()) as errors,
            ):
                exit_code = main(["plan", "--git-ref", revision])
                self.assertEqual(exit_code, 2)
                self.assertIn("exact lowercase 40-hex commit", errors.getvalue())
                inputs.assert_not_called()

    def test_plan_rejects_retired_rendering_modes(self) -> None:
        for flag in (
            "--candidates-only", "--include-yaml", "--timestamp=12345",
            "--template=unused.yml", "--job-prefix=unused", "--git-repo=unused",
        ):
            with self.subTest(flag=flag), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main(["plan", flag])
            self.assertEqual(error.exception.code, 2)

    def test_plan_rejects_invalid_limit_before_loading_inputs(self) -> None:
        with patch("cval.cli._load_plan_inputs") as inputs:
            with self.assertRaisesRegex(ValueError, "batch_size must be >= 1"):
                main(["plan", "--batch-size", "0", "--git-ref", "a" * 40])
        inputs.assert_not_called()

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
                "git_ref",
                "candidates",
            },
        )
        self.assertEqual(plan["free_nodes_count"], 2)
        self.assertEqual(plan["queue_count"], 2)
        self.assertEqual(len(plan["candidates"]), 1)

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
        self.assertEqual(plan["candidates"], [])

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
                        "--git-ref",
                        "a" * 40,
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 0)
        plan = json.loads(output.getvalue())
        self.assertEqual(plan["queue_count"], 2)
        self.assertEqual(plan["candidates"][0]["node"], "slc01-cl02-hgx-0002")
        self.assertEqual(plan["candidates"][0]["reason"], "never-tested")

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