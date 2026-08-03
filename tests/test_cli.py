from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cval.cli import main
from cval.baselines import stats
from cval.baselines.storage import (
    activate_baseline,
    get_active_baseline,
    store_dynamic_baseline,
)
from cval.config import load_config
from cval.models import ClassificationResultRow, LatestStatusRow
from cval.validation.plugins import ExportRows


class CliTests(unittest.TestCase):
    def test_help_lists_only_preferred_commands(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            with self.assertRaises(SystemExit) as exc:
                main(["--help"])

        self.assertEqual(exc.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn(
            "{config,tests,nodes,validate,status,plan,run,jobs,result,results,"
            "classifications,baseline,nccl-eval,overview}",
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
            ), patch(
                "cval.storage.classification_status.get_latest_classification_rows",
                return_value=[],
            ) as classification_reader:
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
                        ]
                    )

            files = list(Path(tmpdir).glob("cval_overall_*.csv"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(files), 1)
        self.assertIn("Wrote 2 overall latest result row(s)", output.getvalue())
        classification_reader.assert_called_once()

    def test_plugin_results_command_reports_export_row_count(self) -> None:
        output = io.StringIO()
        exported = ExportRows(("node",), (("node-a",), ("node-b",)))

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "cval.cli.get_latest_status_rows",
                return_value=[LatestStatusRow("node-a", "storage", 100, "pass")],
            ), patch(
                "cval.validation.operations.export_evaluator_rows",
                return_value=exported,
            ):
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "results",
                            "--test",
                            "storage",
                            "--no-classification",
                            "--no-metrics",
                            "--output-dir",
                            tmpdir,
                        ]
                    )

        self.assertEqual(exit_code, 0)
        self.assertIn("Wrote 2 storage latest result row(s)", output.getvalue())

    def test_classifications_command_writes_csv(self) -> None:
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "cval.storage.classification_status.get_latest_classification_rows",
                return_value=[
                    ClassificationResultRow(
                        200,
                        "node-a",
                        "storage",
                        "storage-1",
                        "degraded",
                        False,
                        12,
                        2,
                        0,
                        2,
                        0.1667,
                        22.0,
                    )
                ],
            ):
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "classifications",
                            "--test",
                            "storage",
                            "--type",
                            "csv",
                            "--output-dir",
                            tmpdir,
                        ]
                    )

            files = list(Path(tmpdir).glob("cval_classifications_storage_*.csv"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(files), 1)
        self.assertIn("Wrote 1 storage classification row(s)", output.getvalue())

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

    def test_empty_baseline_build_fails_nonzero_and_preserves_active(self) -> None:
        from cval.storage.ingest import STORAGE_METRIC_COLUMNS

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "storage.db"
            columns = STORAGE_METRIC_COLUMNS
            with closing(sqlite3.connect(source)) as connection:
                connection.execute(
                    "CREATE TABLE storage_performance (node TEXT, timestamp INTEGER, "
                    "image_name TEXT, "
                    + ", ".join(f"{name} REAL" for name in columns)
                    + ")"
                )
                connection.execute(
                    "INSERT INTO storage_performance VALUES (?,?,?,"
                    + ",".join("?" for _ in columns)
                    + ")",
                    ("node-a", 2_000_000_000, "img", *([100.0] * len(columns))),
                )
                connection.commit()
            baseline_root = root / "baselines"
            config_path = root / "cval.toml"
            config_path.write_text(
                f'[baseline]\nbaseline_root_path = "{baseline_root}"\nmin_samples = 5\n',
                encoding="utf-8",
            )
            config = load_config(config_path)
            metric = stats.summarize_metric(
                columns[0],
                [100.0] * 8,
                direction="low_bad",
                tolerance_pct=10.0,
                bootstrap=False,
            ).to_dict()
            metric["source_table"] = "storage_performance"
            active = {
                "schema_version": "cval.baseline.v2", "baseline_id": "active-1",
                "test_type": "storage", "stratum_key": "", "window_days": 30,
                "created_at": 1, "timestamp": 1, "n_samples": 8,
                "method": "robust_mad", "metrics": {columns[0]: metric},
            }
            store_dynamic_baseline(active, config=config)
            activate_baseline("active-1", "storage", config=config)
            stderr = io.StringIO()

            with redirect_stderr(stderr):
                exit_code = main(
                    [
                        "--config", str(config_path), "baseline", "build",
                        "--test-type", "storage", "--source-db", str(source),
                        "--window-days", "365", "--min-samples", "5",
                        "--activate", "--output", "json",
                    ]
                )

            self.assertEqual(exit_code, 1)
            self.assertIn("metrics must be non-empty", stderr.getvalue())
            self.assertEqual(
                get_active_baseline("storage", config=config)["baseline_id"],
                "active-1",
            )

    def test_classify_cli_rejects_retained_global_store_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "cval.toml"
            config_path.write_text(
                f'[baseline]\nbaseline_root_path = "{root / "baselines"}"\n',
                encoding="utf-8",
            )
            stderr = io.StringIO()
            with patch(
                "cval.validation.operations.classify_evaluator_target",
                return_value=[],
            ), patch(
                "cval.baselines.storage.get_active_baseline",
                return_value={"baseline_id": "active", "metrics": {"x": {}}},
            ), redirect_stderr(stderr):
                exit_code = main(
                    [
                        "--config", str(config_path), "baseline", "classify",
                        "--test-type", "storage", "--store-results",
                        "--classification-db-path",
                        str(root / "baselines/classification-results.db"),
                    ]
                )

            self.assertEqual(exit_code, 1)
            self.assertIn("global classification DB is read-only", stderr.getvalue())

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
            "evaluator-preflight",
            "compatibility",
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

    def test_result_command_prints_status_assignments(self) -> None:
        payload = {
            "schema_version": "cval.results.v1",
            "node": "slc01-cl02-hgx-0001",
            "timestamp": "12345",
            "image_name": "pytorch:26.05-py3",
            "overall": "fail",
            "tests": {
                "storage": {"status": "pass"},
                "nccl": {"status": "fail"},
                "dltest": {"status": "pass"},
            },
        }
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            result_path = Path(tmpdir) / "result.json"
            result_path.write_text(json.dumps(payload), encoding="utf-8")
            with redirect_stdout(output):
                exit_code = main(["result", "--result-json", str(result_path)])

        self.assertEqual(exit_code, 0)
        self.assertIn("GCRRESULT1=pass", output.getvalue())
        self.assertIn("GCRRESULT2=fail", output.getvalue())
        self.assertIn("overall_result=fail", output.getvalue())
        self.assertIn("image_name=pytorch:26.05-py3", output.getvalue())

    def test_result_command_reads_v2_for_legacy_db_update(self) -> None:
        from tests.test_results_v2 import payload

        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            result_path = Path(tmpdir) / "result.json"
            result_path.write_text(json.dumps(payload()), encoding="utf-8")
            with redirect_stdout(output):
                exit_code = main(["result", "--result-json", str(result_path)])

        self.assertEqual(exit_code, 0)
        self.assertIn("GCRRESULT1=pass", output.getvalue())
        self.assertIn("GCRRESULT2=pass", output.getvalue())
        self.assertIn("GCRRESULT3=pass", output.getvalue())
        self.assertIn("overall_result=pass", output.getvalue())

    def test_db_add_result_command_writes_sqlite_row(self) -> None:
        from cval.config import encode_config_snapshot, load_config
        from cval.validation.results import (
            load_validation_result,
            validation_result_digest,
        )

        output = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "validation.db"
            config_path = root / "cval.toml"
            config_path.write_text(
                f'''[storage]
validation_db_path = "{db_path}"
[runtime]
validation_root = "{root}"
''',
                encoding="utf-8",
            )
            config = load_config(config_path)
            result_path = (
                root
                / "logs/job_logs/slc01-cl02-hgx-0001/"
                "slc01-cl02-hgx-0001-12345/result.json"
            )
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "schema_version": "cval.results.v1",
                        "node": "slc01-cl02-hgx-0001",
                        "timestamp": "12345",
                        "overall": "pass",
                        "image_name": "pytorch:26.05-py3",
                        "tests": {
                            "storage": {"status": "pass"},
                            "nccl": {"status": "pass"},
                            "dltest": {"status": "pass"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            result = load_validation_result(result_path)
            with patch.dict(
                os.environ,
                {"CVAL_CONFIG_SNAPSHOT_B64": encode_config_snapshot(config)},
                clear=False,
            ):
                with redirect_stdout(output):
                    exit_code = main(
                        [
                        "--config",
                        str(config_path),
                        "db-add-result",
                        "slc01-cl02-hgx-0001",
                        "all",
                        "pass",
                        "12345",
                        "--image-name",
                        "pytorch:26.05-py3",
                        "--result-json",
                        str(result_path),
                        "--result-digest",
                        validation_result_digest(result),
                        "--db-path",
                        str(db_path),
                        ]
                    )

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, test, timestamp, result, image_name FROM runs"
                ).fetchone()

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            row,
            ("slc01-cl02-hgx-0001", "all", 12345, "pass", "pytorch:26.05-py3"),
        )
        self.assertIn("Added validation result", output.getvalue())


if __name__ == "__main__":
    unittest.main()