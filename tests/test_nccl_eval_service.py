from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from cval.nccl_eval.config import NcclEvaluationConfig
from pathlib import Path

from cval.nccl_eval.service import resident, worker


class NcclEvalServiceTests(unittest.TestCase):
    def test_worker_stops_after_current_batch_and_claims_no_new_work(self) -> None:
        stopped = threading.Event()
        calls = []

        def evaluate_once(config, *, worker_id, batch_size=None):
            calls.append(worker_id)
            stopped.set()
            return {
                "claimed_count": 1,
                "completed_count": 1,
                "waiting_count": 0,
                "failed_count": 0,
                "retry_count": 0,
            }

        with patch(
            "cval.nccl_eval.service.evaluate_once", side_effect=evaluate_once
        ), patch("cval.nccl_eval.service.recover") as recover:
            receipt = worker(
                NcclEvaluationConfig(evaluator_poll_interval_seconds=0.1),
                worker_id="worker-1",
                recover_every_cycles=1,
                stop_event=stopped,
                install_signal_handlers=False,
            )

        self.assertEqual(calls, ["worker-1"])
        recover.assert_not_called()
        self.assertTrue(receipt["stop_requested"])
        self.assertEqual(receipt["cycles_completed"], 1)
        self.assertEqual(receipt["claimed_count"], 1)
        self.assertEqual(receipt["completed_count"], 1)
        self.assertEqual(receipt["event"], "nccl_evaluator_worker_stopped")

    def test_resident_runs_ingest_baseline_evaluate_then_stops(self) -> None:
        stopped = threading.Event()
        events = []

        class Scan:
            root_exists = True
            discovered_json_count = 1

        def evaluate_once(config, *, worker_id, batch_size=None):
            stopped.set()
            return {
                "event": "nccl_evaluation_batch_completed",
                "claimed_count": 1,
                "completed_count": 1,
                "waiting_count": 0,
                "failed_count": 0,
                "retry_count": 0,
            }

        with patch("cval.nccl_eval.outbox.scan_outbox", return_value=Scan()), patch(
            "cval.nccl_eval.outbox.ingest_outbox_progression",
            return_value={
                "mode": "apply",
                "outbox_root": "/data/outbox",
                "processed_count": 1,
                "ingested_count": 1,
                "rejected_count": 0,
                "error_count": 0,
                "skipped_terminal_count": 0,
                "ingested": [],
                "rejected": [],
                "errors": [],
            },
        ), patch(
            "cval.nccl_eval.service.build_baselines",
            return_value={"event": "nccl_baseline_build_completed", "built_count": 1},
        ), patch(
            "cval.nccl_eval.service.evaluate_once", side_effect=evaluate_once
        ), patch("cval.nccl_eval.service.recover") as recover:
            receipt = resident(
                NcclEvaluationConfig(
                    evaluator_poll_interval_seconds=0.1,
                    baseline_builder_interval_seconds=1.0,
                ),
                worker_id="resident-1",
                outbox_root=Path("/data/outbox"),
                stop_event=stopped,
                install_signal_handlers=False,
                event_sink=events.append,
            )

        recover.assert_not_called()
        self.assertEqual(
            [event["event"] for event in events],
            [
                "nccl_outbox_cycle_completed",
                "nccl_baseline_build_completed",
                "nccl_evaluation_batch_completed",
            ],
        )
        self.assertEqual(receipt["ingested_count"], 1)
        self.assertEqual(receipt["baselines_built_count"], 1)
        self.assertEqual(receipt["completed_count"], 1)
        self.assertTrue(receipt["stop_requested"])


if __name__ == "__main__":
    unittest.main()
