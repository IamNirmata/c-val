import csv
import io
import unittest

from evaluation_engine.report import render_report


def sample(**overrides):
    return {
        "node": "node-a", "test": "nccl", "raw_status": "pass",
        "raw_timestamp": "1788475963", "evaluated_at": "1788924307",
        "exported_at_utc": "2026-09-09T03:26:07+00:00", "status": "unclassified",
        "health_class": "", "reason": "missing_threshold_or_metric_coverage",
        "baseline_id": "", "baseline_expired": "", "classified": "0",
        "expected": "10", "bad": "0", "evaluation_id": "evaluation-a",
    } | overrides


class EvaluationReportTests(unittest.TestCase):
    def test_readable_report_preserves_classes_and_explains_missing_evidence(self):
        rows = [
            sample(reason="evaluation evidence path does not exist: /raw/result.json"),
            sample(node="node-b"),
            sample(node="node-c", status="complete", health_class="bad", raw_status="fail", reason="raw_test_failed"),
            sample(node="node-d", status="complete", health_class="good", reason="full_coverage", classified="10", baseline_id="b1", baseline_expired="1"),
        ]
        before = [dict(row) for row in rows]
        report, summary = render_report(rows)
        parsed = list(csv.DictReader(io.StringIO(report)))
        self.assertEqual(rows, before)
        self.assertEqual(len(parsed), 4)
        self.assertEqual([row["recorded_class"] for row in parsed], ["not_classified", "not_classified", "bad", "good"])
        self.assertEqual(parsed[0]["reason_code"], "missing_result_provenance")
        self.assertIn("missing", parsed[0]["explanation"])
        self.assertIn("not a test failure", parsed[1]["explanation"])
        self.assertIn("raw NCCL test failed", parsed[2]["explanation"])
        self.assertEqual(parsed[3]["baseline_state"], "expired")
        self.assertIn("expired", parsed[3]["explanation"])
        self.assertGreater(float(parsed[0]["raw_age_days"]), 0)
        self.assertIn("not hardware diagnoses", summary)

    def test_invalid_report_identity_is_rejected(self):
        for rows in ([], [sample(test="dltest")], [sample(), sample()], [sample(), sample(node="node-b", exported_at_utc="2026-09-10T00:00:00+00:00")]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                render_report(rows)

    def test_unknown_reason_and_evaluator_error_stay_visible(self):
        report, _summary = render_report([sample(status="error", reason="database is locked")])
        row = next(csv.DictReader(io.StringIO(report)))
        self.assertEqual(row["evaluation_state"], "error")
        self.assertEqual(row["recorded_class"], "not_classified")
        self.assertIn("Evaluator error", row["explanation"])
        self.assertIn("database is locked", row["explanation"])


if __name__ == "__main__":
    unittest.main()