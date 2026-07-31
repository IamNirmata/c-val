"""Tests for the operational overview and job listing."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cval.config import load_config
from cval.jobs.monitor import JobPhase, list_job_phases
from cval.k8s.client import CommandResult
from cval.models import ClassificationResultRow
from cval.orchestrator.overview import (
    _freshness_counts,
    _summarize_classifications,
    _summarize_jobs,
    build_overview,
    render_overview,
)


class FakeClient:
    """Minimal KubectlClient stand-in returning a canned result."""

    def __init__(self, stdout: str, returncode: int = 0) -> None:
        self._stdout = stdout
        self._returncode = returncode

    def run(self, args, check: bool = True, input_text: str | None = None) -> CommandResult:
        return CommandResult(
            args=list(args), stdout=self._stdout, stderr="", returncode=self._returncode
        )


class TestFreshnessCounts(unittest.TestCase):
    def test_valid_outdated_split(self):
        now = 1_000_000
        day = 86400
        status = {"a": now - day, "b": now - 10 * day, "c": 0}
        valid, outdated = _freshness_counts(status, days_threshold=7, now=now)
        self.assertEqual(valid, 1)     # 'a' is 1 day old
        self.assertEqual(outdated, 2)  # 'b' is 10 days old, 'c' never tested


class TestSummarizeJobs(unittest.TestCase):
    def test_counts_by_phase(self):
        jobs = [
            JobPhase("j1", "Running"),
            JobPhase("j2", "Running"),
            JobPhase("j3", "Completed"),
        ]
        self.assertEqual(_summarize_jobs(jobs), {"Running": 2, "Completed": 1})


class TestSummarizeClassifications(unittest.TestCase):
    def test_counts_by_test_and_status(self):
        rows = [
            ClassificationResultRow(100, "n1", "storage", "s1", "normal", True, 1, 0, 0, 0, 0.0, 0.0),
            ClassificationResultRow(100, "n2", "storage", "s1", "degraded", False, 1, 1, 0, 1, 1.0, 20.0),
            ClassificationResultRow(100, "n3", "nccl", "n1", "normal", True, 1, 0, 0, 0, 0.0, 0.0),
        ]
        self.assertEqual(
            _summarize_classifications(rows),
            {"storage": {"normal": 1, "degraded": 1}, "nccl": {"normal": 1}},
        )

    def test_overview_excludes_historical_rows_for_disabled_targets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                "[tests.nccl]\nenabled = false\n",
                encoding="utf-8",
            )
            config = load_config(config_path)
            rows = [
                ClassificationResultRow(
                    100, "n1", "storage", "s1", "normal", True,
                    1, 0, 0, 0, 0.0, 0.0,
                ),
                ClassificationResultRow(
                    100, "n2", "nccl", "n1", "degraded", False,
                    1, 1, 0, 1, 1.0, 20.0,
                ),
            ]
            with (
                patch(
                    "cval.orchestrator.overview.discover_free_nodes",
                    return_value=(
                        [],
                        {"capacity": 0, "allocatable": 0, "used": 0, "free": 0},
                    ),
                ),
                patch(
                    "cval.orchestrator.overview.get_latest_status_rows",
                    return_value=[],
                ),
                patch(
                    "cval.orchestrator.overview.get_latest_classification_rows",
                    return_value=rows,
                ),
            ):
                overview = build_overview(
                    config=config,
                    include_jobs=False,
                    now=1_000_000,
                )

        self.assertEqual(overview["classifications"]["total"], 1)
        self.assertEqual(
            overview["classifications"]["by_test"],
            {"storage": {"normal": 1}},
        )


class TestListJobPhases(unittest.TestCase):
    def test_filters_by_prefix_and_defaults_unknown(self):
        payload = {
            "items": [
                {"metadata": {"name": "cval-node-1"}, "status": {"state": {"phase": "Running"}}},
                {"metadata": {"name": "other-job"}, "status": {"state": {"phase": "Pending"}}},
                {"metadata": {"name": "cval-node-2"}, "status": {"state": {}}},
            ]
        }
        client = FakeClient(json.dumps(payload))
        phases = list_job_phases(namespace="ns", prefix="cval", client=client)
        mapping = {phase.job_name: phase.phase for phase in phases}
        self.assertEqual(mapping, {"cval-node-1": "Running", "cval-node-2": "Unknown"})

    def test_returns_empty_on_failure(self):
        client = FakeClient("", returncode=1)
        self.assertEqual(list_job_phases(namespace="ns", prefix="cval", client=client), [])

    def test_returns_empty_on_bad_json(self):
        client = FakeClient("not json")
        self.assertEqual(list_job_phases(namespace="ns", prefix="cval", client=client), [])


class TestRenderOverview(unittest.TestCase):
    def test_render_contains_all_sections(self):
        overview = {
            "generated_at": 1700000000,
            "namespace": "gcr-admin",
            "days_threshold": 7,
            "nodes": {
                "fully_free": 2,
                "free_gpus": 16,
                "total_gpus": 100,
                "fully_free_names": ["n1", "n2"],
            },
            "freshness": {"nodes_with_results": 50, "valid": 40, "outdated": 10},
            "queue": {
                "needing_validation": 1,
                "candidates": [
                    {
                        "priority": 1,
                        "node": "n1",
                        "reason": "never-tested",
                        "age_days": None,
                        "last_tested_timestamp": 0,
                    }
                ],
            },
            "jobs": {
                "total": 1,
                "by_phase": {"Running": 1},
                "active": [{"job_name": "cval-n1", "phase": "Running"}],
                "items": [{"job_name": "cval-n1", "phase": "Running"}],
            },
            "classifications": {
                "total": 2,
                "by_test": {"storage": {"normal": 1, "degraded": 1}},
            },
            "errors": {},
        }
        text = render_overview(overview)
        for token in ("NODES", "RESULTS", "CLASSIFY", "QUEUE", "JOBS", "never-tested", "cval-n1"):
            self.assertIn(token, text)

    def test_render_shows_errors(self):
        overview = {
            "generated_at": 1700000000,
            "namespace": "gcr-admin",
            "days_threshold": 7,
            "nodes": {"fully_free": 0, "free_gpus": 0, "total_gpus": 0, "fully_free_names": []},
            "freshness": {"nodes_with_results": 0, "valid": 0, "outdated": 0},
            "queue": {"needing_validation": 0, "candidates": []},
            "jobs": {"total": 0, "by_phase": {}, "active": [], "items": []},
            "errors": {"status": "pod not found\nsecond line"},
        }
        text = render_overview(overview)
        self.assertIn("! status: pod not found", text)
        self.assertNotIn("second line", text)


if __name__ == "__main__":
    unittest.main()
