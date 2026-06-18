from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cval.cli import main
from cval.models import LatestStatusRow


class CliTests(unittest.TestCase):
    def test_help_lists_only_preferred_commands(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            with self.assertRaises(SystemExit) as exc:
                main(["--help"])

        self.assertEqual(exc.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn("{config,status,nodes,overview,run,jobs,result,results}", help_text)
        self.assertNotIn("submit-plan", help_text)
        self.assertNotIn("run-batch", help_text)
        self.assertNotIn("db-add-result", help_text)

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
                        ]
                    )

            files = list(Path(tmpdir).glob("cval_overall_*.csv"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(files), 1)
        self.assertIn("Wrote 2 overall latest result row(s)", output.getvalue())

    def test_prioritize_json_command(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "prioritize",
                    "--free-nodes",
                    "slc01-cl02-hgx-0002,slc01-cl02-hgx-0001",
                    "--threshold-days",
                    "4",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        queue = json.loads(output.getvalue())
        self.assertEqual(
            [candidate["node"] for candidate in queue],
            ["slc01-cl02-hgx-0001", "slc01-cl02-hgx-0002"],
        )
        self.assertTrue(all(candidate["reason"] == "never-tested" for candidate in queue))

    def test_run_command_defaults_to_dry_run(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "run",
                    "--free-nodes",
                    "slc01-cl02-hgx-0001,slc01-cl02-hgx-0002",
                    "--batch-size",
                    "1",
                    "--timestamp",
                    "12345",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["submitted_count"], 0)
        self.assertEqual(
            result["jobs"][0]["job_name"],
            "cval-slc01-cl02-hgx-0001-pytorch-26-05-py3-12345",
        )

    def test_legacy_run_batch_command_still_works(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "run-batch",
                    "--nodes",
                    "slc01-cl02-hgx-0001",
                    "--batch-size",
                    "1",
                    "--timestamp",
                    "12345",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertIn("Dry run: 1 job(s) would be submitted", output.getvalue())

    def test_config_command_prints_effective_config(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(["config"])

        self.assertEqual(exit_code, 0)
        config = json.loads(output.getvalue())
        self.assertEqual(config["job"]["job_prefix"], "cval")
        self.assertEqual(config["cluster"]["namespace"], "gcr-admin")

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
                        "run",
                        "--free-nodes",
                        "slc01-cl02-hgx-0001,slc01-cl02-hgx-0002",
                        "--timestamp",
                        "12345",
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(len(result["jobs"]), 1)
        self.assertEqual(
            result["jobs"][0]["job_name"],
            "custom-slc01-cl02-hgx-0001-pytorch-26-05-py3-12345",
        )

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
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        plan = json.loads(output.getvalue())
        self.assertTrue(plan["dry_run"])
        self.assertEqual(plan["free_nodes_count"], 2)
        self.assertEqual(plan["queue_count"], 2)
        self.assertEqual(len(plan["planned_jobs"]), 1)

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
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(exit_code, 0)
        plan = json.loads(output.getvalue())
        self.assertEqual(plan["queue_count"], 2)
        self.assertEqual(plan["planned_jobs"][0]["node"], "slc01-cl02-hgx-0002")
        self.assertEqual(plan["planned_jobs"][0]["reason"], "never-tested")

    def test_submit_plan_defaults_to_dry_run(self) -> None:
        output = io.StringIO()

        with redirect_stdout(output):
            exit_code = main(
                [
                    "run",
                    "--free-nodes",
                    "slc01-cl02-hgx-0001,slc01-cl02-hgx-0002",
                    "--batch-size",
                    "1",
                    "--timestamp",
                    "12345",
                    "--output",
                    "json",
                ]
            )

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["submitted_count"], 0)
        self.assertFalse(result["jobs"][0]["submitted"])

    def test_submit_plan_submit_requires_confirmation(self) -> None:
        stderr = io.StringIO()

        with redirect_stderr(stderr):
            exit_code = main(
                [
                    "run",
                    "--free-nodes",
                    "slc01-cl02-hgx-0001",
                    "--submit",
                ]
            )

        self.assertEqual(exit_code, 2)
        self.assertIn("Policy violation", stderr.getvalue())

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

    def test_db_add_result_command_writes_sqlite_row(self) -> None:
        output = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            with redirect_stdout(output):
                exit_code = main(
                    [
                        "db-add-result",
                        "slc01-cl02-hgx-0001",
                        "all",
                        "pass",
                        "12345",
                        "--image-name",
                        "pytorch:26.05-py3",
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