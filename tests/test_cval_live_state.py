from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts/cval-live-state.py"
GIT_REF = "a" * 40


class CvalLiveStateTests(unittest.TestCase):
    def _run(self, *arguments: str) -> None:
        subprocess.run([sys.executable, str(SCRIPT), *arguments], check=True)

    def test_updates_compact_session_files_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            session = Path(tmpdir) / "20260904_104430_PDT"
            self._run(
                "init",
                str(session),
                "--started-at",
                "2026-09-04T10:44:30-07:00",
                "--git-ref",
                GIT_REF,
                "--branch",
                "raw-only-framework",
                "--batch-size",
                "3",
                "--plan-limit",
                "all",
            )
            node = session / "check.json"
            node.write_text(
                json.dumps({"name": "slc01-cl02-hgx-0001", "eligible": True}),
                encoding="utf-8",
            )
            self._run("node", str(session), "--source", str(node))
            receipt = session / "submit-123.json"
            receipt.write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "job_name": "cval-slc01-cl02-hgx-0001-123",
                                "node": "slc01-cl02-hgx-0001",
                                "git_ref": GIT_REF,
                                "submitted": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            self._run(
                "receipt",
                str(session),
                "--source",
                str(receipt),
                "--observed-at",
                "2026-09-04T10:45:00-07:00",
            )
            self._run(
                "job",
                str(session),
                "--job-name",
                "cval-slc01-cl02-hgx-0001-123",
                "--phase",
                "Running",
                "--observed-at",
                "2026-09-04T10:46:00-07:00",
            )
            self._run(
                "update",
                str(session),
                "--updated-at",
                "2026-09-04T10:46:00-07:00",
                "--state",
                "watching",
                "--message",
                "tracking one job",
                "--cycle-id",
                "20260904_104500_PDT",
            )

            state = json.loads((session / "state.json").read_text(encoding="utf-8"))
            with (session / "jobs.csv").open(newline="", encoding="utf-8") as handle:
                jobs = list(csv.DictReader(handle))
            summary = (session / "SUMMARY.md").read_text(encoding="utf-8")

            self.assertEqual(state["state"], "watching")
            self.assertEqual(state["job_counts"], {"Running": 1, "total": 1})
            self.assertEqual(state["node_snapshots"], 1)
            self.assertEqual(jobs[0]["phase"], "Running")
            self.assertIn("running=1", summary)
            self.assertIn("Nodes checked: 1", summary)
            self.assertEqual(
                (session.parent / "current-session").read_text(encoding="utf-8"),
                session.name + "\n",
            )
            self.assertEqual(list(session.rglob("*.tmp-*")), [])


if __name__ == "__main__":
    unittest.main()