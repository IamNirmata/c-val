from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
import os
import runpy
import socket
import hashlib
import time
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, call, patch
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts/cval-live-state.py"
GIT_REF = "a" * 40


class StatusCsvTests(unittest.TestCase):
    def test_admission_lock_remains_fail_fast_and_bounds_opt_in_wait(self) -> None:
        from cval.config import load_config
        from cval.jobs.manager import admission_lock
        from cval.policy import PolicyViolation

        with tempfile.TemporaryDirectory() as directory:
            config = load_config()
            config = replace(config, live=replace(config.live, log_dir=directory, control_host=socket.gethostname()))
            for timeout, clock, attempts in ((0, [100, 100], 1), (5, [100, 100, 106], 2)):
                with self.subTest(timeout=timeout), \
                     patch("cval.jobs.manager.fcntl.flock", side_effect=BlockingIOError()) as lock, \
                     patch("cval.jobs.manager.time.monotonic", side_effect=clock), \
                     patch("cval.jobs.manager.time.sleep") as wait:
                    with self.assertRaisesRegex(PolicyViolation, "shared lock"), admission_lock(config, timeout_seconds=timeout):
                        self.fail("A busy lock must not be acquired")
                    self.assertEqual(lock.call_count, attempts)
                    self.assertEqual(wait.call_count, attempts - 1)
            for timeout in (-1, float("inf"), float("nan")):
                with self.subTest(invalid_timeout=timeout), self.assertRaises(ValueError), admission_lock(config, timeout_seconds=timeout):
                    self.fail("An invalid wait bound must be rejected")

    def test_refresh_retains_collected_snapshot_through_brief_lock_contention(self) -> None:
        from cval.config import encode_config_snapshot, load_config
        from cval.storage.status import refresh_status_csvs

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = root / "session"
            session.mkdir()
            log_dir = root / "logs"
            log_dir.mkdir()
            base = load_config()
            config = replace(base, live=replace(base.live, log_dir=str(log_dir), status_dir=str(root / "reports"),
                                                control_host=socket.gethostname()))
            state = {"config_snapshot_b64": encode_config_snapshot(config), "intents": {}}
            now = int(time.time())
            snapshot = {"observed_at_epoch": now - 170, "collected_at_epoch": now, "inventory_state": "queried", "nodes": [],
                        "coverage": {"schema_version": "cval.coverage.v1", "state": "empty", "policy": "successful_complete_run",
                                     "requested_tests": sorted(test.id for test in config.tests.registry.enabled), "nodes": [], "timestamps": {}}}
            for attempts in ([None, BlockingIOError(), None], [BlockingIOError(), None, None]):
                with self.subTest(attempts=attempts), patch("cval.jobs.manager.check_admission_cluster"), \
                     patch("cval.jobs.manager.read_session_state", return_value=state), \
                     patch("cval.jobs.manager.read_admission_session", return_value=(session, state)), \
                     patch("cval.storage.status.collect_status_observation", return_value=snapshot) as collect, \
                     patch("cval.jobs.manager.fcntl.flock", side_effect=attempts) as lock:
                    refreshed = refresh_status_csvs(session, client=Mock())
                self.assertEqual(refreshed["state"], "queried")
                self.assertEqual(refreshed["coverage_state"], "empty")
                self.assertEqual(refreshed["collected_at_epoch"], now)
                collect.assert_called_once()
                self.assertEqual(lock.call_count, 3)
                self.assertEqual(json.loads((session / "status-observation.json").read_text()), snapshot)

    def test_plan_coverage_summary_does_not_write_a_duplicate_snapshot(self) -> None:
        script = (REPO_ROOT / "scripts/cval-live.sh").read_text()
        function = "json_coverage_summary_tsv() {" + script.split("json_coverage_summary_tsv() {", 1)[1].split("\ncollect_plan_inputs()", 1)[0]
        self.assertNotIn("latest-status-map.json", script)
        self.assertNotIn("repair_cycle_cooldowns", script)
        self.assertNotIn("json_submitted_jobs_csv_from_dir", script)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "coverage.json"
            source.write_text(json.dumps({"schema_version": "cval.coverage.v1", "state": "empty",
                "policy": "successful_complete_run", "nodes": [{"node": "node-a", "state": "empty", "coverage_timestamp": None}], "timestamps": {}}))
            before = source.read_bytes()
            result = subprocess.run(["bash", "-s", "--", str(source)],
                input=function + '\njson_coverage_summary_tsv "$1"\n', cwd=REPO_ROOT,
                env=os.environ | {"PYTHONDONTWRITEBYTECODE": "1"}, text=True, capture_output=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "1\t0")
            self.assertEqual(source.read_bytes(), before)
            self.assertEqual(list(Path(directory).iterdir()), [source])

    def test_watch_refresh_uses_pinned_interval_and_stops_with_its_owner(self) -> None:
        from cval.config import load_config

        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            (session / "status-observation.json").write_text(json.dumps({"observed_at_epoch": 1000}))
            stopping = Mock()
            stopping.is_set.return_value = False
            stopping.wait.side_effect = [False, True]
            helper = runpy.run_path(str(SCRIPT))
            with patch("threading.Event", return_value=stopping), patch("signal.signal"), patch("time.time", return_value=1000), \
                 patch("cval.storage.status.refresh_status_csvs", return_value={"state": "queried"}) as refresh:
                helper["_status"](SimpleNamespace(cval_config=load_config(), session_dir=session, watch=True))
            refresh.assert_called_once_with(session)
            self.assertEqual(stopping.wait.call_args_list, [call(300), call(300)])

    def test_watch_retries_contention_without_skipping_a_refresh_interval(self) -> None:
        from cval.config import load_config
        from cval.policy import PolicyViolation

        with tempfile.TemporaryDirectory() as directory:
            session = Path(directory)
            (session / "status-observation.json").write_text(json.dumps({"observed_at_epoch": 1000, "collected_at_epoch": 1170}))
            stopping = Mock()
            stopping.is_set.return_value = False
            stopping.wait.side_effect = [False, True]
            helper = runpy.run_path(str(SCRIPT))
            with patch("threading.Event", return_value=stopping), patch("signal.signal"), patch("time.time", return_value=1170), \
                 patch("cval.storage.status.refresh_status_csvs", side_effect=PolicyViolation("Another admission or session update holds the shared lock")):
                helper["_status"](SimpleNamespace(cval_config=load_config(), session_dir=session, watch=True))
            self.assertEqual(stopping.wait.call_args_list, [call(130), call(5)])

    def test_loop_refresh_starts_before_submission_and_has_exit_cleanup(self) -> None:
        script = (REPO_ROOT / "scripts/cval-live.sh").read_text()
        run_loop = script.split("\nrun_loop() {", 1)[1].split("\nstart_session()", 1)[0]
        self.assertLess(run_loop.index("start_status_refresh"), run_loop.index("run_submit_cycle"))
        start = script.split("\nstart_status_refresh() {", 1)[1].split("\nrun_once()", 1)[0]
        self.assertIn('python3 "$STATE_HELPER" status "$SESSION_DIR" || return "$?"', start)
        self.assertIn("trap 'stop_status_refresh' EXIT", start)
        subprocess.run(["bash", "-n", str(REPO_ROOT / "scripts/cval-live.sh")], check=True)

    def test_intent_is_projected_before_create_and_unresolved_csv_is_not_adopted(self) -> None:
        from cval.config import encode_config_snapshot, load_config
        from cval.jobs.manager import admission_lock, retain_submission_event
        from cval.storage.status import check_status_reconciliation, read_status_csvs, refresh_status_csvs
        from cval.validation.runtime import effective_config_digest

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = load_config()
            config = replace(config, job=replace(config.job, git_ref=GIT_REF),
                live=replace(config.live, log_dir=str(root / "private"), status_dir=str(root / "run-logs"), control_host=socket.gethostname()))
            session = root / "private" / "test-session"
            session.parent.mkdir()
            approval = root / "approval.md"
            approval.write_text("Local fixture only; no cluster changes.\n")
            helper = runpy.run_path(str(SCRIPT))
            with patch.dict(os.environ, {"CVAL_CONFIG_SNAPSHOT_B64": encode_config_snapshot(config)}), admission_lock(config):
                helper["_init"](SimpleNamespace(session_dir=session, git_ref=GIT_REF, approval_record=approval,
                    started_at="2026-09-25T21:00:00Z", branch="main", batch_size=3, plan_limit="all", pruning_enabled=False))
                state = json.loads((session / "state.json").read_text())
                state["intents"]["new-job"] = {"job_name": "new-job", "node_name": "hgx-a", "timestamp": 1790370000,
                    "run_id": "hgx-a-1790370000", "git_ref": GIT_REF, "config_digest": effective_config_digest(config), "events": [],
                    "manifest": "fixture", "manifest_sha256": hashlib.sha256(b"fixture").hexdigest(),
                    "confirmation": "submit", "allow_cordoned": False, "approval_sha256": state["approval"]["sha256"]}
                state = retain_submission_event(session, state, "new-job", {"state": "prepared"})
                rows, cluster = read_status_csvs(root / "run-logs")
                self.assertEqual(rows[0]["job_name"], "new-job")
                self.assertEqual(rows[0]["job_phase"], "prepared")
                self.assertEqual(rows[0]["recording_state"], "no_results")
                self.assertEqual(rows[0]["snapshot_id"], cluster["snapshot_id"])
                self.assertEqual(sorted(path.name for path in (root / "run-logs").iterdir()), ["cluster-status.csv", "node-status.csv"])
                with self.assertRaisesRegex(ValueError, "Unresolved status attempt"):
                    check_status_reconciliation(rows, {"intents": {}})
                check_status_reconciliation(rows, state)
                state = retain_submission_event(session, state, "new-job", {"state": "submitted", "uid": "job-uid",
                    "phase": "Running", "created_at": "2026-09-25T21:01:00Z"})
                rows, _cluster = read_status_csvs(root / "run-logs")
                self.assertEqual(rows[0]["job_uid"], "job-uid")
                self.assertEqual(rows[0]["job_phase"], "Running")
                self.assertEqual(len(state["intents"]["new-job"]["events"]), 2)
            def collect(*_args):
                with admission_lock(config):
                    retain_submission_event(session, state, "new-job", {"state": "observed", "uid": "job-uid",
                        "phase": "Running", "runtime": {"tests_outcome": "fail"}})
                now = int(time.time())
                return {"observed_at_epoch": now, "inventory_state": "queried", "nodes": [
                    {"node_name": "hgx-a", "node_uid": "node-uid", "observed_at_epoch": now}],
                    "coverage": {"schema_version": "cval.coverage.v1", "state": "empty", "policy": "successful_complete_run",
                        "requested_tests": sorted(test.id for test in config.tests.registry.enabled), "timestamps": {},
                        "nodes": [{"node": "hgx-a", "state": "empty", "coverage_timestamp": None}]}}
            with patch("cval.jobs.manager.check_admission_cluster"), patch("cval.storage.status.collect_status_observation", side_effect=collect):
                refreshed = refresh_status_csvs(session, client=Mock())
            self.assertEqual(refreshed["node_count"], 1)
            rows, _cluster = read_status_csvs(root / "run-logs")
            self.assertEqual(rows[0]["test_outcome"], "fail")
            self.assertEqual(rows[0]["job_uid"], "job-uid")

    def test_partial_csv_pair_is_rejected(self) -> None:
        from cval.storage.status import read_status_csvs

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "node-status.csv").write_text("node_name\nhgx-a\n")
            with self.assertRaisesRegex(ValueError, "pair is missing"):
                read_status_csvs(root)


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