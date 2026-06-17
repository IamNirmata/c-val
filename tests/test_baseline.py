"""Tests for baseline ingestion and classification."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.baselines.models import BaselineConfig, BaselineMetrics
from cval.baselines.ingest import (
    classify_result_vs_baseline,
    compute_peer_stats,
    load_baseline_summary,
)
from cval.baselines.storage import (
    load_baseline_from_db,
    list_baselines,
    store_baseline,
)


class TestBaselineLoading(unittest.TestCase):
    """Test baseline summary loading from directories."""

    def test_load_storage_baseline(self):
        """Load a storage baseline from a summary.json file."""
        with TemporaryDirectory() as tmpdir:
            baseline_dir = Path(tmpdir)
            summary_data = {
                "test_plan": "80gb-example",
                "timestamp": 1700000000,
                "node": "test-node-1",
                "iodepth_read_1file_iops": 50000.0,
                "iodepth_read_1file_bw": 400.0,
                "iodepth_write_1file_iops": 45000.0,
                "iodepth_write_1file_bw": 350.0,
            }
            (baseline_dir / "summary.json").write_text(json.dumps(summary_data))

            baseline = load_baseline_summary(baseline_dir, "storage")
            self.assertIsNotNone(baseline)
            self.assertEqual(baseline.test_type, "storage")
            self.assertEqual(baseline.test_plan, "80gb-example")
            self.assertEqual(baseline.iodepth_read_1file_iops, 50000.0)

    def test_load_nccl_baseline(self):
        """Load an NCCL baseline."""
        with TemporaryDirectory() as tmpdir:
            baseline_dir = Path(tmpdir)
            summary_data = {
                "test_plan": "all-reduce-8gpu",
                "timestamp": 1700000000,
                "busbw": 500.0,
                "latency": 25.5,
            }
            (baseline_dir / "summary.json").write_text(json.dumps(summary_data))

            baseline = load_baseline_summary(baseline_dir, "nccl")
            self.assertIsNotNone(baseline)
            self.assertEqual(baseline.busbw, 500.0)
            self.assertEqual(baseline.latency, 25.5)

    def test_load_dltest_baseline(self):
        """Load a DL test baseline."""
        with TemporaryDirectory() as tmpdir:
            baseline_dir = Path(tmpdir)
            summary_data = {
                "test_plan": "80gb-example",
                "timestamp": 1700000000,
                "task_counts": {"nn_tasks": 456, "f_tasks": 304, "coll_tasks": 192, "overlap_tasks": 384},
                "status_counts": {"completed": 1336},
                "numerical_metrics": {
                    "task_1": {"norm_output": 0.5, "weight": 0.1, "bias": 0.05}
                },
                "collective_metrics": {
                    "task_1": {"allreduce_time": 1.5}
                },
            }
            (baseline_dir / "summary.json").write_text(json.dumps(summary_data))

            baseline = load_baseline_summary(baseline_dir, "dltest")
            self.assertIsNotNone(baseline)
            self.assertEqual(baseline.task_counts["nn_tasks"], 456)

    def test_load_missing_baseline(self):
        """Return None when summary.json is missing."""
        with TemporaryDirectory() as tmpdir:
            baseline_dir = Path(tmpdir)
            baseline = load_baseline_summary(baseline_dir, "storage")
            self.assertIsNone(baseline)


class TestBaselineStorage(unittest.TestCase):
    """Test baseline storage and retrieval in SQLite."""

    def test_store_and_load_baseline(self):
        """Store and retrieve a baseline from the DB."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            baseline = BaselineMetrics(
                test_type="nccl",
                baseline_id="b200-pt2.8.0",
                test_plan="all-reduce-8gpu",
                timestamp=1700000000,
                node="test-node-1",
                busbw=500.0,
                latency=25.5,
            )

            store_baseline(baseline, db_path=db_path)
            loaded = load_baseline_from_db("b200-pt2.8.0", "nccl", db_path=db_path)

            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.baseline_id, "b200-pt2.8.0")
            self.assertEqual(loaded.busbw, 500.0)
            self.assertEqual(loaded.latency, 25.5)

    def test_list_baselines(self):
        """List all baselines in the DB."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            for i in range(3):
                baseline = BaselineMetrics(
                    test_type="nccl",
                    baseline_id=f"baseline-{i}",
                    timestamp=1700000000 + i,
                )
                store_baseline(baseline, db_path=db_path)

            baselines = list_baselines(db_path=db_path)
            self.assertEqual(len(baselines), 3)

    def test_list_baselines_by_type(self):
        """Filter baselines by test type."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            baselines_to_create = [
                ("baseline-nccl", "nccl"),
                ("baseline-storage-1", "storage"),
                ("baseline-storage-2", "storage"),
            ]
            for baseline_id, test_type in baselines_to_create:
                baseline = BaselineMetrics(
                    test_type=test_type,
                    baseline_id=baseline_id,
                    timestamp=1700000000,
                )
                store_baseline(baseline, db_path=db_path, test_type=test_type)

            storage_baselines = list_baselines(test_type="storage", db_path=db_path)
            self.assertEqual(len(storage_baselines), 2)


class TestBaselineClassification(unittest.TestCase):
    """Test result classification against baselines."""

    def test_classify_nccl_normal(self):
        """Classify an NCCL result as normal (within tolerance)."""
        baseline = BaselineMetrics(
            test_type="nccl",
            baseline_id="b200-pt2.8.0",
            busbw=500.0,
            latency=25.5,
        )
        result = {"busbw": 502.0, "latency": 25.6}
        config = BaselineConfig(nccl_peer_tolerance_pct=5.0)

        classification = classify_result_vs_baseline(result, baseline, config=config)
        self.assertEqual(classification["status"], "normal")
        self.assertEqual(len(classification["violations"]), 0)

    def test_classify_nccl_degraded(self):
        """Classify an NCCL result as degraded (outside tolerance)."""
        baseline = BaselineMetrics(
            test_type="nccl",
            baseline_id="b200-pt2.8.0",
            busbw=500.0,
            latency=25.5,
        )
        result = {"busbw": 450.0, "latency": 30.0}
        config = BaselineConfig(nccl_peer_tolerance_pct=5.0)

        classification = classify_result_vs_baseline(result, baseline, config=config)
        self.assertEqual(classification["status"], "degraded")
        self.assertGreater(len(classification["violations"]), 0)

    def test_classify_storage_normal(self):
        """Classify storage result as normal."""
        baseline = BaselineMetrics(
            test_type="storage",
            baseline_id="storage-baseline-1",
            iodepth_read_1file_iops=50000.0,
            iodepth_read_1file_bw=400.0,
        )
        result = {
            "iodepth_read_1file_iops": 50500.0,
            "iodepth_read_1file_bw": 402.0,
        }
        config = BaselineConfig(storage_peer_tolerance_pct=10.0)

        classification = classify_result_vs_baseline(result, baseline, config=config)
        self.assertEqual(classification["status"], "normal")

    def test_classify_dl_compute_tight(self):
        """Classify DL compute tasks with tight tolerance."""
        baseline = BaselineMetrics(
            test_type="dltest",
            baseline_id="dl-baseline-1",
            task_counts={"nn_tasks": 456, "f_tasks": 304, "coll_tasks": 192},
            numerical_metrics={"task_1": {"norm_output": 0.5, "weight": 0.1, "bias": 0.05}},
        )
        result = {
            "task_counts": {"nn_tasks": 460, "f_tasks": 304, "coll_tasks": 192},
            "numerical_metrics": {"task_1": {"norm_output": 0.5, "weight": 0.1, "bias": 0.05}},
        }
        config = BaselineConfig(dl_compute_tolerance_pct=3.0)

        classification = classify_result_vs_baseline(result, baseline, config=config)
        # nn_tasks changed by ~0.88%, within 3% tolerance
        self.assertEqual(classification["status"], "normal")

    def test_classify_dl_numerical_exact(self):
        """Classify DL numerical metrics with almost-exact tolerance."""
        baseline = BaselineMetrics(
            test_type="dltest",
            baseline_id="dl-baseline-1",
            numerical_metrics={"task_1": {"norm_output": 0.5, "weight": 0.1, "bias": 0.05}},
        )
        result = {
            "numerical_metrics": {"task_1": {"norm_output": 0.501, "weight": 0.1, "bias": 0.05}},
        }
        config = BaselineConfig(dl_numerical_tolerance_pct=0.1)

        classification = classify_result_vs_baseline(result, baseline, config=config)
        # norm_output changed by 0.2%, outside 0.1% tolerance
        # Because 0.501 > 0.5, pct_diff = (0.501 - 0.5) / 0.5 = 0.002 = 0.2%
        # which is > 0.1%, so it's a violation. Since all improvements are positive,
        # and 0.2% > 0, it's treated as "improved" instead of "degraded"
        self.assertGreater(len(classification["violations"]), 0)

    def test_classify_dl_overlap_lenient(self):
        """Classify DL overlap tasks with lenient tolerance."""
        baseline = BaselineMetrics(
            test_type="dltest",
            baseline_id="dl-baseline-1",
            task_counts={"overlap_tasks": 384},
        )
        result = {
            "task_counts": {"overlap_tasks": 410},  # ~6.8% increase
        }
        config = BaselineConfig(dl_overlap_tolerance_pct=20.0)

        classification = classify_result_vs_baseline(result, baseline, config=config)
        # Within 20% tolerance, should be normal
        self.assertEqual(classification["status"], "normal")


if __name__ == "__main__":
    unittest.main()
