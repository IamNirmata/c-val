"""Metric-policy math shared by test-owned baseline/classification scripts."""

from collections import Counter
from statistics import median
import math


def build_rule(values, direction, policy):
    if len(values) < policy["min_nodes"] or not all(math.isfinite(value) for value in values):
        return None
    center = median(values)
    rule = {"direction": direction, "baseline": center, "nodes": len(values)}
    if direction == "reference":
        reference, count = Counter(values).most_common(1)[0]
        if count / len(values) < policy["min_mode_fraction"]:
            return None
        tolerance = policy["atol"] + policy["rtol"] * abs(reference)
        return rule | {"baseline": reference, "lower": reference - tolerance, "upper": reference + tolerance, "method": "independent_node_consensus"}
    if direction not in {"lower", "higher"}:
        raise ValueError("unknown metric direction")
    ordered = sorted(values)
    probability = policy["quantile"] if direction == "lower" else 1 - policy["quantile"]
    position = probability * (len(ordered) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    boundary = ordered[lower_index] + (position - lower_index) * (ordered[upper_index] - ordered[lower_index])
    gap = max(abs(center) * policy["min_relative_gap"], policy["atol"])
    boundary = max(boundary, center + gap) if direction == "lower" else min(boundary, center - gap)
    return rule | {"bad_at": boundary, "method": "quantile_with_minimum_gap"}


def classify(value, rule):
    if value is None or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None, "invalid_metric"
    if rule is None:
        return None, "missing_threshold"
    direction = rule["direction"]
    if direction == "reference":
        return ("good", "within_reference_interval") if rule["lower"] <= value <= rule["upper"] else ("bad", "outside_reference_interval")
    bad = value >= rule["bad_at"] if direction == "lower" else value <= rule["bad_at"]
    excellent = value < rule["baseline"] if direction == "lower" else value > rule["baseline"]
    return ("bad", "beyond_threshold") if bad else (("excellent", "better_than_median") if excellent else ("good", "within_threshold"))