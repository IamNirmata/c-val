from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cval.jobs.manager import submit_workflow_plan
from cval.jobs.monitor import get_job_phase, monitor_jobs_until_terminal
from cval.k8s.client import CommandResult
from cval.orchestrator.workflow import build_workflow_plan
from cval.policy import ExecutionPolicy, PolicyViolation


TEMPLATE = (
        Path(__file__).resolve().parents[1] / "ymls/specific-node-job.yml"
).read_text(encoding="utf-8")


class FakeKubectlClient:
    def __init__(self, stdout: str = "created") -> None:
        self.stdout = stdout
        self.calls: list[tuple[list[str], str | None]] = []

    def run(self, args, check=True, input_text=None):
        self.calls.append((list(args), input_text))
        return CommandResult(args=list(args), stdout=self.stdout, stderr="", returncode=0)


class JobManagerTests(unittest.TestCase):
    def test_submit_workflow_plan_defaults_to_dry_run(self) -> None:
        plan = _plan(batch_size=2)
        client = FakeKubectlClient()

        result = submit_workflow_plan(plan, client=client)

        self.assertTrue(result.dry_run)
        self.assertEqual(result.submitted_count, 0)
        self.assertEqual(len(result.records), 2)
        self.assertEqual(client.calls, [])

    def test_real_submit_requires_confirmation(self) -> None:
        plan = _plan(batch_size=1)

        with self.assertRaises(PolicyViolation):
            submit_workflow_plan(plan, submit=True, confirmation=None)

    def test_real_submit_uses_stdin_manifest_when_confirmed(self) -> None:
        plan = _plan(batch_size=1)
        client = FakeKubectlClient(stdout="job.batch.volcano.sh/test created")
        policy = ExecutionPolicy(namespace_allowlist=("gcr-admin",), max_batch_size=1)

        result = submit_workflow_plan(
            plan,
            client=client,
            policy=policy,
            submit=True,
            confirmation="submit",
        )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.submitted_count, 1)
        self.assertEqual(client.calls[0][0], ["create", "-n", "gcr-admin", "-f", "-"])
        self.assertIn("slc01-cl02-hgx-0001", client.calls[0][1])

    def test_get_job_phase_is_read_only(self) -> None:
        client = FakeKubectlClient(stdout="Running")

        phase = get_job_phase("hari-gcr-ceval-node-123", client=client)

        self.assertEqual(phase.phase, "Running")
        self.assertEqual(client.calls[0][0][:3], ["get", "vcjob", "-n"])

    def test_monitor_jobs_marks_nonterminal_timeout(self) -> None:
        client = FakeKubectlClient(stdout="Running")

        jobs = monitor_jobs_until_terminal(
            ["hari-gcr-ceval-node-123"],
            client=client,
            timeout_seconds=0,
            poll_interval_seconds=0,
        )

        self.assertEqual(jobs[0].phase, "Running")
        self.assertFalse(jobs[0].terminal)
        self.assertTrue(jobs[0].timed_out)

    def test_monitor_jobs_stops_on_terminal_phase(self) -> None:
        client = FakeKubectlClient(stdout="Completed")

        jobs = monitor_jobs_until_terminal(
            ["hari-gcr-ceval-node-123"],
            client=client,
            timeout_seconds=30,
            poll_interval_seconds=0,
        )

        self.assertEqual(jobs[0].phase, "Completed")
        self.assertTrue(jobs[0].terminal)
        self.assertFalse(jobs[0].timed_out)


def _plan(batch_size: int):
    with tempfile.TemporaryDirectory() as tmpdir:
        template_path = Path(tmpdir) / "job.yml"
        template_path.write_text(TEMPLATE, encoding="utf-8")
        return build_workflow_plan(
            ["slc01-cl02-hgx-0001", "slc01-cl02-hgx-0002"],
            {},
            batch_size=batch_size,
            template_path=template_path,
            timestamp=12345,
        )


if __name__ == "__main__":
    unittest.main()