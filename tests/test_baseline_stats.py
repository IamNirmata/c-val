"""Tests for robust baseline statistics kernels."""

import math
import unittest

from cval.baselines import stats


class TestRobustEstimators(unittest.TestCase):
    def test_median_and_percentiles(self):
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.assertEqual(stats.median(data), 5.5)
        # Type-7 linear interpolation, matching numpy defaults.
        self.assertAlmostEqual(stats.percentile(data, 25), 3.25)
        self.assertAlmostEqual(stats.percentile(data, 75), 7.75)
        self.assertAlmostEqual(stats.iqr(data), 4.5)

    def test_percentile_single_value(self):
        self.assertEqual(stats.percentile([42.0], 99), 42.0)

    def test_mad_and_sigma(self):
        data = [1, 1, 2, 2, 4, 6, 9]
        # median = 2; abs deviations = [1,1,0,0,2,4,7] -> median 1.
        self.assertEqual(stats.mad(data), 1.0)
        self.assertAlmostEqual(stats.mad_sigma(data), 1.4826)

    def test_modified_zscore_mad_zero_fallback(self):
        # >half identical -> MAD == 0; fallback uses MeanAD, no ZeroDivision.
        data = [10, 10, 10, 10, 10, 13]
        zscores = stats.modified_zscores(data)
        self.assertEqual(len(zscores), len(data))
        self.assertTrue(all(math.isfinite(z) for z in zscores))
        self.assertGreater(abs(zscores[-1]), 0.0)

    def test_modified_zscore_all_identical(self):
        self.assertEqual(stats.modified_zscores([5, 5, 5, 5]), [0.0, 0.0, 0.0, 0.0])

    def test_is_deterministic(self):
        self.assertTrue(stats.is_deterministic([1.0, 1.0, 1.0, 1.0, 1.0]))
        self.assertFalse(stats.is_deterministic([1.0, 2.0, 3.0, 4.0, 5.0]))

    def test_trim_outliers_removes_spike(self):
        data = [100.0] * 20 + [100000.0]
        kept, removed = stats.trim_outliers(data)
        self.assertEqual(removed, 1)
        self.assertNotIn(100000.0, kept)

    def test_tukey_fences(self):
        data = [10, 12, 14, 16, 18, 20, 22, 24]
        low, high = stats.tukey_fences(data, k=1.5)
        self.assertLess(low, min(data))
        self.assertGreater(high, max(data))

    def test_bootstrap_ci_reproducible_and_ordered(self):
        data = [float(x) for x in range(1, 51)]
        ci_a = stats.bootstrap_median_ci(data, seed=7)
        ci_b = stats.bootstrap_median_ci(data, seed=7)
        self.assertEqual(ci_a, ci_b)  # deterministic for a fixed seed
        self.assertLessEqual(ci_a[0], ci_a[1])


class TestSummarizeMetric(unittest.TestCase):
    def test_low_bad_band_is_one_sided(self):
        data = [500.0 + (i % 5 - 2) for i in range(40)]
        stat = stats.summarize_metric(
            "busbw", data, direction=stats.DIRECTION_LOW_BAD, tolerance_pct=5.0
        )
        # Higher-is-better: no upper penalty, finite lower bound.
        self.assertTrue(math.isinf(stat.upper_bound))
        self.assertFalse(math.isinf(stat.lower_bound))
        self.assertLess(stat.lower_bound, stat.median)

    def test_high_bad_band_is_one_sided(self):
        data = [25.0 + (i % 5 - 2) * 0.1 for i in range(40)]
        stat = stats.summarize_metric(
            "latency", data, direction=stats.DIRECTION_HIGH_BAD, tolerance_pct=5.0
        )
        self.assertTrue(math.isinf(stat.lower_bound))
        self.assertFalse(math.isinf(stat.upper_bound))
        self.assertGreater(stat.upper_bound, stat.median)

    def test_tolerance_floor_widens_tight_mad(self):
        # Near-constant data -> tiny MAD; the 10% floor must dominate the band.
        data = [1000.0 + (i % 2) * 0.01 for i in range(40)]
        stat = stats.summarize_metric(
            "iops", data, direction=stats.DIRECTION_TWO_SIDED, tolerance_pct=10.0
        )
        # Band half-width should be ~10% of the median, not ~0.
        self.assertAlmostEqual(stat.median, 1000.0, delta=1.0)
        self.assertLessEqual(stat.lower_bound, 905.0)
        self.assertGreaterEqual(stat.upper_bound, 1095.0)

    def test_deterministic_metric_uses_relative_floor(self):
        data = [0.5] * 30
        stat = stats.summarize_metric(
            "norm_output", data, direction=stats.DIRECTION_TWO_SIDED, tolerance_pct=0.1
        )
        self.assertTrue(stat.deterministic)
        self.assertEqual(stat.method, "deterministic")
        # 0.1% of 0.5 = 0.0005 band half-width.
        self.assertAlmostEqual(stat.lower_bound, 0.5 - 0.0005, places=6)
        self.assertAlmostEqual(stat.upper_bound, 0.5 + 0.0005, places=6)

    def test_to_dict_serializes_infinity_as_none(self):
        data = [500.0 + (i % 5 - 2) for i in range(40)]
        stat = stats.summarize_metric(
            "busbw", data, direction=stats.DIRECTION_LOW_BAD, tolerance_pct=5.0
        )
        as_dict = stat.to_dict()
        self.assertIsNone(as_dict["upper_bound"])
        self.assertIsNotNone(as_dict["lower_bound"])


class TestClassifyValue(unittest.TestCase):
    def _busbw_stat(self):
        data = [500.0 + (i % 5 - 2) for i in range(40)]
        return stats.summarize_metric(
            "busbw", data, direction=stats.DIRECTION_LOW_BAD, tolerance_pct=5.0
        )

    def test_normal_within_band(self):
        status, _ = stats.classify_value(500.0, self._busbw_stat())
        self.assertEqual(status, "normal")

    def test_degraded_below_lower_bound(self):
        status, pct = stats.classify_value(400.0, self._busbw_stat())
        self.assertEqual(status, "degraded")
        self.assertLess(pct, 0.0)

    def test_improved_above_p95_for_low_bad(self):
        status, pct = stats.classify_value(600.0, self._busbw_stat())
        self.assertEqual(status, "improved")
        self.assertGreater(pct, 0.0)

    def test_classify_from_stored_dict(self):
        stored = self._busbw_stat().to_dict()
        status, _ = stats.classify_value(500.0, stored)
        self.assertEqual(status, "normal")


if __name__ == "__main__":
    unittest.main()
