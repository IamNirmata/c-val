from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from cval.jobs.renderer import render_validation_job_from_file
from cval.orchestrator.workflow import build_workflow_plan
from cval.scheduler.priority import build_priority_queue


TEMPLATE = (
    Path(__file__).resolve().parents[1] / "ymls/specific-node-job.yml"
).read_text(encoding="utf-8")


class WorkflowTests(unittest.TestCase):
    def test_submission_preserves_candidate_order_and_renders_only_its_batch(self) -> None:
        nodes = ["node-b", "node-a", "node-fresh"]
        history = {"node-b": 100, "node-fresh": 999_999}
        options = {
            "days_threshold": 4,
            "batch_size": 1,
            "timestamp": 12345,
            "now": datetime.fromtimestamp(1_000_000, tz=timezone.utc),
        }
        with patch(
            "cval.orchestrator.workflow.render_validation_job_from_file",
            wraps=render_validation_job_from_file,
        ) as renderer:
            candidates = build_priority_queue(
                nodes, history, days_threshold=4, now=options["now"]
            )
            renderer.assert_not_called()
            rendered = build_workflow_plan(nodes, history, **options)
            renderer.assert_called_once()
            self.assertEqual(renderer.call_args.kwargs["node_name"], "node-a")

        self.assertEqual(candidates, rendered.queue)
        self.assertEqual([candidate.node for candidate in candidates], ["node-a", "node-b"])
        self.assertEqual(len(rendered.planned_jobs), 1)

    def test_empty_submission_batch_does_not_render(self) -> None:
        with patch("cval.orchestrator.workflow.render_validation_job_from_file") as renderer:
            empty = build_workflow_plan([], {})
        renderer.assert_not_called()
        self.assertEqual(empty.queue, [])
        self.assertEqual(empty.planned_jobs, [])

    def test_submission_batch_rejects_invalid_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "batch_size must be >= 1"):
            build_workflow_plan(["node-a"], {}, batch_size=0)

    def test_builds_read_only_workflow_plan(self) -> None:
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

        self.assertEqual(len(plan.queue), 2)
        self.assertEqual(len(plan.planned_jobs), 1)
        self.assertEqual(plan.planned_jobs[0].candidate.node, "slc01-cl02-hgx-0001")
        self.assertEqual(
            plan.planned_jobs[0].rendered_job.job_name,
          "cval-slc01-cl02-hgx-0001-pytorch-26-05-py3-12345",
        )

if __name__ == "__main__":
    unittest.main()