"""Render a readable NCCL report from an existing read-only evaluation export."""

from __future__ import annotations

import argparse
import csv
import io
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


FIELDS = (
    "node", "raw_test_result", "raw_run_at_la", "raw_age_days",
    "evaluation_state", "recorded_class", "explanation", "metrics_compared",
    "metrics_expected", "bad_metrics", "baseline_state", "evaluated_at_la",
    "reason_code", "evaluation_id", "exported_at_utc",
)
EXPLANATIONS = {
    "full_coverage": "All required metrics compared against the matched baseline.",
    "raw_test_failed": "The raw NCCL test failed; this is not a baseline-only failure.",
    "required_metric_failed": "At least one required metric is outside its baseline threshold.",
    "missing_threshold_or_metric_coverage": "No usable matched baseline or incomplete metric coverage; not a test failure.",
    "hardware_or_rank_coverage_mismatch": "Recorded hardware or rank coverage does not match evaluator requirements.",
    "incomplete_or_conflicting_status_set": "The raw run lacks a complete consistent status set.",
    "raw_test_incomplete": "The raw test is incomplete; no class can be assigned.",
    "test_disabled": "NCCL was disabled for this run.",
}
LOS_ANGELES = ZoneInfo("America/Los_Angeles")


def render_report(rows: list[dict[str, str]]) -> tuple[str, str]:
    if not rows or any(row["test"] != "nccl" for row in rows):
        raise ValueError("report requires a nonempty NCCL-only export")
    if len({row["node"] for row in rows}) != len(rows):
        raise ValueError("report requires one latest evaluation per node")
    exported = {row["exported_at_utc"] for row in rows}
    if len(exported) != 1:
        raise ValueError("report must use one export snapshot")
    exported_at = exported.pop()
    snapshot_time = datetime.fromisoformat(exported_at)
    if snapshot_time.tzinfo is None:
        raise ValueError("export timestamp must include its timezone")
    report_rows = []
    reasons: Counter[str] = Counter()
    for row in sorted(rows, key=lambda item: item["node"]):
        reason = row["reason"]
        if reason.startswith("evaluation evidence path does not exist:"):
            reason = "missing_result_provenance"
            explanation = "Canonical result evidence is missing; the raw status alone cannot be classified."
        else:
            explanation = EXPLANATIONS.get(reason, row["reason"])
        if row["status"] == "error":
            explanation = "Evaluator error; no reliable classification. " + explanation
        baseline = "not_selected"
        if row["baseline_id"]:
            baseline = {"0": "valid_at_export", "1": "expired"}.get(row["baseline_expired"], "expiry_unknown")
        if baseline == "expired":
            explanation = "Stored class uses an expired baseline. " + explanation
        run_time = datetime.fromtimestamp(int(row["raw_timestamp"]), timezone.utc)
        evaluated_time = datetime.fromtimestamp(int(row["evaluated_at"]), timezone.utc)
        reasons[reason] += 1
        report_rows.append({
            "node": row["node"],
            "raw_test_result": row["raw_status"],
            "raw_run_at_la": run_time.astimezone(LOS_ANGELES).isoformat(timespec="seconds"),
            "raw_age_days": f"{(snapshot_time - run_time).total_seconds() / 86400:.1f}",
            "evaluation_state": row["status"],
            "recorded_class": row["health_class"] or "not_classified",
            "explanation": explanation,
            "metrics_compared": row["classified"],
            "metrics_expected": row["expected"],
            "bad_metrics": row["bad"],
            "baseline_state": baseline,
            "evaluated_at_la": evaluated_time.astimezone(LOS_ANGELES).isoformat(timespec="seconds"),
            "reason_code": reason,
            "evaluation_id": row["evaluation_id"],
            "exported_at_utc": exported_at,
        })
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(report_rows)
    classes = Counter(row["recorded_class"] for row in report_rows)
    raw = Counter(row["raw_test_result"] for row in report_rows)
    summary = [
        "# NCCL Classification Report", "", f"Snapshot: {exported_at}. Nodes: {len(rows)}.", "",
        "Raw test result and evaluator class are different: `pass` can be unclassified",
        "or outside a relative performance threshold. `not_classified` is not `bad`.",
        "These are stored test comparisons, not hardware diagnoses. No classes were recomputed.", "",
        "Raw results: " + ", ".join(f"{name}={count}" for name, count in sorted(raw.items())) + ".",
        "Recorded classes: " + ", ".join(f"{name}={count}" for name, count in sorted(classes.items())) + ".", "",
        "| Explanation | Nodes |", "|---|---:|",
        *(f"| {reason} | {count} |" for reason, count in reasons.most_common()), "",
        "`raw_age_days` measures test age at export. A recent `evaluated_at_la` can",
        "be a replay of old evidence, not a new validation run. Latest means latest",
        "stored evaluation per node; newly committed raw runs may still await evaluation.", "",
        "A baseline can be unselected because of cohort, cutoff, expiry or coverage rules;",
        "the exported reason does not distinguish these. An expired stored class is retained",
        "but flagged. Keep the original detailed CSV for source digests and full error paths.",
    ]
    return buffer.getvalue(), "\n".join(summary) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    with args.input.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    report, summary = render_report(rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs = (("_readable.csv", report), ("_summary.md", summary))
    paths = [args.output_dir / (args.input.stem + suffix) for suffix, _ in outputs]
    if any(path.exists() for path in paths):
        raise FileExistsError("report output already exists; refusing overwrite")
    for path, (_, content) in zip(paths, outputs, strict=True):
        with path.open("x", encoding="utf-8", newline="") as handle:
            handle.write(content)
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())