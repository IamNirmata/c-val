from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from cval.orchestrator.workflow import build_workflow_plan, workflow_plan_to_dict


TEMPLATE = """
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  generateName: jobname-placeholder
spec:
  tasks:
    - template:
        spec:
          nodeSelector:
            kubernetes.io/hostname: "nodename-placeholder"
          containers:
            - env:
                - name: GCRTIME
                  value: "time-placeholder"
"""


class WorkflowTests(unittest.TestCase):
    def test_builds_dry_run_workflow_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            template_path = Path(tmpdir) / "job.yml"
            template_path.write_text(TEMPLATE, encoding="utf-8")

            plan = build_workflow_plan(
                ["slc01-cl02-hgx-0002", "slc01-cl02-hgx-0001"],
                {"slc01-cl02-hgx-0002": 100},
                days_threshold=4,
                batch_size=1,
                template_path=template_path,
                timestamp=12345,
                now=datetime.fromtimestamp(1_000_000, tz=timezone.utc),
            )

        self.assertTrue(plan.dry_run)
        self.assertEqual(len(plan.queue), 2)
        self.assertEqual(len(plan.planned_jobs), 1)
        self.assertEqual(plan.planned_jobs[0].candidate.node, "slc01-cl02-hgx-0001")
        self.assertEqual(
            plan.planned_jobs[0].rendered_job.job_name,
          "gcr-cval-slc01-cl02-hgx-0001-pytorch-26-05-py3-12345",
        )

    def test_workflow_plan_to_dict_excludes_yaml_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            template_path = Path(tmpdir) / "job.yml"
            template_path.write_text(TEMPLATE, encoding="utf-8")
            plan = build_workflow_plan(
                ["slc01-cl02-hgx-0001"],
                {},
                template_path=template_path,
                timestamp=12345,
            )

        data = workflow_plan_to_dict(plan)

        self.assertEqual(data["queue_count"], 1)
        self.assertNotIn("yaml_text", data["planned_jobs"][0])


if __name__ == "__main__":
    unittest.main()