from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts/cval-node-cooldown.py"


class NodeCooldownTests(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def test_missing_state_excludes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report = root / "report.json"
            completed = self._run(
                "filter",
                "--state-file",
                str(root / "missing.csv"),
                "--nodes",
                "node-a,node-b",
                "--now",
                "20000",
                "--cooldown-seconds",
                "14400",
                "--report",
                str(report),
            )
            payload = json.loads(report.read_text(encoding="utf-8"))

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "node-a,node-b")
        self.assertEqual(payload["cooldown_excluded"], [])
        self.assertEqual(payload["priority_eligible_nodes"], ["node-a", "node-b"])

    def test_active_cooldown_is_excluded_until_exact_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            state = root / "node_cool_down.csv"
            state.write_text(
                "node_name,latest_job_submission_timestamp\nnode-a,1000\n",
                encoding="utf-8",
            )
            before_report = root / "before.json"
            before = self._run(
                "filter",
                "--state-file",
                str(state),
                "--nodes",
                "node-a,node-b",
                "--now",
                "15399",
                "--cooldown-seconds",
                "14400",
                "--report",
                str(before_report),
            )
            at_expiry = self._run(
                "filter",
                "--state-file",
                str(state),
                "--nodes",
                "node-a,node-b",
                "--now",
                "15400",
                "--cooldown-seconds",
                "14400",
                "--report",
                str(root / "expiry.json"),
            )
            payload = json.loads(before_report.read_text(encoding="utf-8"))

        self.assertEqual(before.returncode, 0, before.stderr)
        self.assertEqual(before.stdout.strip(), "node-b")
        self.assertEqual(payload["cooldown_excluded"][0]["node"], "node-a")
        self.assertEqual(payload["cooldown_excluded"][0]["cooldown_until"], 15400)
        self.assertEqual(at_expiry.returncode, 0, at_expiry.stderr)
        self.assertEqual(at_expiry.stdout.strip(), "node-a,node-b")

    def test_record_retains_one_latest_row_per_node(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = Path(tmpdir) / "node_cool_down.csv"
            for node, timestamp in (
                ("node-b", "200"),
                ("node-a", "100"),
                ("node-a", "300"),
                ("node-a", "250"),
            ):
                completed = self._run(
                    "record",
                    "--state-file",
                    str(state),
                    "--node",
                    node,
                    "--timestamp",
                    timestamp,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
            with state.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(
            rows,
            [
                {
                    "node_name": "node-a",
                    "latest_job_submission_timestamp": "300",
                    "latest_job_submission_timestamp_la": "1969-12-31T16:05:00-08:00",
                },
                {
                    "node_name": "node-b",
                    "latest_job_submission_timestamp": "200",
                    "latest_job_submission_timestamp_la": "1969-12-31T16:03:20-08:00",
                },
            ],
        )

    def test_migrate_adds_la_timestamp_to_legacy_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = Path(tmpdir) / "node_cool_down.csv"
            state.write_text(
                "node_name,latest_job_submission_timestamp\nnode-a,1000\n",
                encoding="utf-8",
            )
            completed = self._run(
                "migrate",
                "--state-file",
                str(state),
            )
            with state.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            rows,
            [
                {
                    "node_name": "node-a",
                    "latest_job_submission_timestamp": "1000",
                    "latest_job_submission_timestamp_la": "1969-12-31T16:16:40-08:00",
                }
            ],
        )

    def test_malformed_or_duplicate_state_fails_closed(self) -> None:
        for content in (
            "wrong,header\nnode-a,100\n",
            "node_name,latest_job_submission_timestamp\nnode-a,100\nnode-a,200\n",
            "node_name,latest_job_submission_timestamp\nnode-a,nope\n",
            (
                "node_name,latest_job_submission_timestamp,"
                "latest_job_submission_timestamp_la\n"
                "node-a,1000,not-the-derived-time\n"
            ),
        ):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                state = root / "node_cool_down.csv"
                state.write_text(content, encoding="utf-8")
                completed = self._run(
                    "filter",
                    "--state-file",
                    str(state),
                    "--nodes",
                    "node-a",
                    "--now",
                    "1000",
                    "--cooldown-seconds",
                    "100",
                    "--report",
                    str(root / "report.json"),
                )

            self.assertNotEqual(completed.returncode, 0)


if __name__ == "__main__":
    unittest.main()
