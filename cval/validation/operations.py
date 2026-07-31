"""Generic compatibility baseline, classification, and export dispatch.

These extension points deliberately remain separate from U7/U8/U9.  Built-in
adapters continue reading the established metadata metric databases and writing
the established baseline/classification databases; registry capabilities only
select and dispatch operator-facing targets.
"""

from __future__ import annotations

import json
import math
from typing import Any

from cval.baselines import stats
from cval.config import CvalConfig
from cval.validation.operational_targets import (
	BASELINE_BUILD,
	BASELINE_CLASSIFY,
	RESULTS_EXPORT,
	OperationalTarget,
	build_operational_target_catalog,
)
from cval.validation.plugins import (
	BaselineBuildContext,
	BaselineClassificationContext,
	ExportContext,
	ExportRows,
	PluginLoadError,
	load_registered_plugin,
)


def resolve_operational_target(
	config: CvalConfig,
	name: str,
	operation: str,
) -> OperationalTarget:
	"""Rebuild the immutable catalog and resolve one enabled target."""

	return build_operational_target_catalog(config.tests.registry).require(name, operation)


def build_compatibility_baseline(
	config: CvalConfig,
	target_name: str,
	*,
	window_days: int,
	min_samples: int,
	source_db: str | None = None,
	image_name: str | None = None,
	node: str | None = None,
	test_plan: str | None = None,
	baseline_id: str | None = None,
) -> dict[str, Any]:
	"""Invoke one target owner's compatibility baseline builder."""

	target = resolve_operational_target(config, target_name, BASELINE_BUILD)
	registered, plugin = _operational_plugin(config, target)
	context = BaselineBuildContext(
		target=target,
		definition=registered.definition,
		config=config,
		window_days=window_days,
		min_samples=min_samples,
		source_db=source_db,
		image_name=image_name,
		node=node,
		test_plan=test_plan,
		baseline_id=baseline_id,
	)
	record = plugin.build_compatibility_baseline(context)
	validate_compatibility_baseline_record(
		record,
		expected_test_type=target.baseline_test_type,
	)
	return record


def classify_compatibility_target(
	config: CvalConfig,
	target_name: str,
	baseline: dict[str, Any],
	*,
	window_days: int,
	source_db: str | None = None,
	node: str | None = None,
) -> list[dict[str, Any]]:
	"""Invoke one target owner's compatibility classifier."""

	target = resolve_operational_target(config, target_name, BASELINE_CLASSIFY)
	validation_baseline = dict(baseline) if isinstance(baseline, dict) else baseline
	if isinstance(validation_baseline, dict):
		validation_baseline.pop("component", None)
		validation_baseline.pop("components", None)
	validate_compatibility_baseline_record(
		validation_baseline,
		expected_test_type=target.baseline_test_type,
	)
	registered, plugin = _operational_plugin(config, target)
	context = BaselineClassificationContext(
		target=target,
		definition=registered.definition,
		config=config,
		window_days=window_days,
		source_db=source_db,
		node=node,
	)
	verdicts = plugin.classify_compatibility(context, baseline)
	if not isinstance(verdicts, tuple):
		raise TypeError(
			f"Adapter {target.owner_test_id!r} classify_compatibility must return a tuple"
		)
	normalized = list(verdicts)
	baseline_id = baseline.get("baseline_id") if isinstance(baseline, dict) else None
	if not isinstance(baseline_id, str) or not baseline_id:
		raise ValueError("Compatibility classification baseline_id is invalid")
	validate_compatibility_classification_verdicts(
		normalized,
		target=target,
		expected_baseline_id=baseline_id,
	)
	return normalized


def export_compatibility_rows(
	config: CvalConfig,
	target_name: str,
	context: ExportContext,
) -> ExportRows:
	"""Invoke one read-only export hook and enforce its rectangular contract."""

	target = resolve_operational_target(config, target_name, RESULTS_EXPORT)
	if context.target != target:
		raise ValueError("Export context target does not match the resolved catalog target")
	registered, plugin = _operational_plugin(config, target)
	if context.definition != registered.definition:
		raise ValueError("Export context definition is not the current registry definition")
	rows = plugin.export_rows(context)
	if not isinstance(rows, ExportRows):
		raise TypeError(
			f"Adapter {target.owner_test_id!r} export_rows must return ExportRows"
		)
	return ExportRows(
		tuple(rows.columns),
		tuple(tuple(row) for row in rows.rows),
		rows.row_label,
	)


def _operational_plugin(config: CvalConfig, target: OperationalTarget):
	registered = config.tests.registry.require(target.owner_test_id)
	if not registered.enabled:
		raise ValueError(f"Operational target owner is disabled: {target.owner_test_id}")
	plugin = load_registered_plugin(registered)
	if plugin is None:
		raise PluginLoadError(
			f"Operational target owner {target.owner_test_id!r} has no plugin"
		)
	return registered, plugin


_BASELINE_FIELDS = frozenset(
	{
		"schema_version", "baseline_id", "test_type", "stratum_key",
		"window_days", "created_at", "timestamp", "n_samples", "method", "metrics",
	}
)
_METRIC_STAT_FIELDS = frozenset(
	{
		"metric", "direction", "n", "n_excluded", "median", "mad", "mad_sigma",
		"iqr", "p01", "p05", "p25", "p50", "p75", "p95", "p99", "minimum",
		"maximum", "skewness", "kurtosis", "ci_low", "ci_high", "deterministic",
		"lower_bound", "upper_bound", "method", "source_table",
	}
)
_VERDICT_FIELDS = frozenset(
	{
		"node", "test_type", "baseline_test_type", "dl_component", "baseline_id",
		"status", "n_metrics", "n_compared", "n_degraded", "n_band_degraded",
		"n_improved", "degraded_metric_fraction", "degraded_metric_percent",
		"worst_pct_diff", "metrics",
	}
)
_DL_VERDICT_FIELDS = frozenset(
	{
		"components", "dl_degraded_severity_pct", "dl_min_degraded_metrics",
		"dl_degraded_metric_fraction_threshold",
	}
)
_METRIC_REPORT_FIELDS = frozenset(
	{
		"metric", "component", "value", "median", "status", "pct_diff",
		"abs_pct_diff", "counts_for_degraded_status", "direction", "lower_bound",
		"upper_bound",
	}
)
_DL_COMPONENT_FIELDS = frozenset(
	{
		"component", "status", "n_compared", "n_degraded", "n_band_degraded",
		"n_improved", "degraded_metric_fraction", "degraded_metric_percent",
		"worst_pct_diff", "dl_degraded_severity_pct", "dl_min_degraded_metrics",
		"dl_degraded_metric_fraction_threshold",
	}
)
_DL_COMPONENTS = frozenset(
	{
		"numerical_correctness", "compute_performance", "collective_performance",
		"overlap_performance",
	}
)


def validate_compatibility_baseline_record(
	record: object,
	*,
	expected_test_type: str,
) -> None:
	"""Validate the exact persisted ``cval.baseline.v2`` compatibility shape."""

	if not isinstance(record, dict):
		raise TypeError("Compatibility baseline hooks must return a dictionary")
	_require_exact_fields(record, _BASELINE_FIELDS, "Compatibility baseline record")
	if record["schema_version"] != "cval.baseline.v2":
		raise ValueError("Compatibility baseline schema_version is invalid")
	if record["test_type"] != expected_test_type:
		raise ValueError("Compatibility baseline record has the wrong test_type")
	_nonempty_string(record["baseline_id"], "Compatibility baseline_id")
	_string(record["test_type"], "Compatibility baseline test_type")
	_string(record["stratum_key"], "Compatibility baseline stratum_key")
	_nonempty_string(record["method"], "Compatibility baseline method")
	if not isinstance(record["metrics"], dict):
		raise TypeError("Compatibility baseline metrics must be a dictionary")
	for field in ("window_days", "created_at", "timestamp", "n_samples"):
		_non_negative_integer(record[field], f"Compatibility baseline {field}")
	if record["window_days"] <= 0:
		raise ValueError("Compatibility baseline window_days must be positive")
	if record["timestamp"] != record["created_at"]:
		raise ValueError("Compatibility baseline timestamp must equal created_at")
	for metric_name, metric in record["metrics"].items():
		_nonempty_string(metric_name, "Compatibility baseline metric key")
		_validate_metric_stat(metric_name, metric)
	_require_json_safe(record, "Compatibility baseline record")


def validate_compatibility_classification_verdicts(
	verdicts: list[dict[str, Any]],
	*,
	target: OperationalTarget,
	expected_baseline_id: str | None = None,
) -> None:
	"""Validate exact plugin verdict schemas, identities, and score invariants."""

	seen_nodes: set[str] = set()
	for verdict in verdicts:
		if not isinstance(verdict, dict):
			raise TypeError("Compatibility classification verdicts must be dictionaries")
		expected_fields = _VERDICT_FIELDS
		if target.baseline_test_type == "dltest":
			expected_fields |= _DL_VERDICT_FIELDS
		_require_exact_fields(
			verdict,
			expected_fields,
			"Compatibility classification verdict",
		)
		node = verdict["node"]
		if not isinstance(node, str) or not node or node in seen_nodes:
			raise ValueError("Classification verdict nodes must be non-empty and unique")
		seen_nodes.add(node)
		if verdict["test_type"] != target.name:
			raise ValueError("Classification verdict has the wrong operational target")
		if verdict["baseline_test_type"] != target.baseline_test_type:
			raise ValueError("Classification verdict has the wrong baseline test type")
		expected_component = target.component if target.baseline_test_type == "dltest" else ""
		if verdict["dl_component"] != expected_component:
			raise ValueError("Classification verdict has the wrong component identity")
		_nonempty_string(verdict["baseline_id"], "Classification verdict baseline_id")
		if expected_baseline_id is not None and verdict["baseline_id"] != expected_baseline_id:
			raise ValueError("Classification verdict baseline identity does not match")
		if verdict["status"] not in {"normal", "degraded", "improved"}:
			raise ValueError("Classification verdict has an invalid status")
		for field in (
			"n_metrics", "n_compared", "n_degraded", "n_band_degraded", "n_improved",
		):
			_non_negative_integer(verdict[field], f"Classification verdict {field}")
		if not isinstance(verdict["metrics"], list):
			raise TypeError("Classification verdict metrics must be a list")
		if verdict["n_metrics"] != len(verdict["metrics"]):
			raise ValueError("Classification verdict n_metrics does not match metrics")
		if verdict["n_compared"] != verdict["n_metrics"]:
			raise ValueError("Classification verdict n_compared does not match metrics")
		if not (
			0 <= verdict["n_degraded"] <= verdict["n_band_degraded"] <= verdict["n_compared"]
		):
			raise ValueError("Classification degraded counts are inconsistent")
		if not (0 <= verdict["n_improved"] <= verdict["n_compared"]):
			raise ValueError("Classification improved count is inconsistent")
		_validate_fraction_scores(verdict, "Classification verdict")
		metric_counts = _validate_metric_reports(verdict["metrics"])
		if target.component and any(
			metric["component"] != target.component for metric in verdict["metrics"]
		):
			raise ValueError("Classification metric component does not match target")
		if target.baseline_test_type == "dltest" and any(
			metric["component"] not in _DL_COMPONENTS for metric in verdict["metrics"]
		):
			raise ValueError("DL classification metric component is invalid")
		if verdict["n_band_degraded"] != metric_counts["degraded"]:
			raise ValueError("Classification n_band_degraded does not match metrics")
		if verdict["n_improved"] != metric_counts["improved"]:
			raise ValueError("Classification n_improved does not match metrics")
		if target.baseline_test_type == "dltest":
			if verdict["n_degraded"] != metric_counts["counted_degraded"]:
				raise ValueError("DL classification n_degraded does not match metrics")
			expected_dl_status = _validate_dl_components(verdict)
			if target.component and verdict["status"] != verdict["components"][target.component]["status"]:
				raise ValueError("DL target status does not match its component summary")
			if verdict["status"] != expected_dl_status:
				raise ValueError("DL classification status does not match metric aggregation")
		elif verdict["n_degraded"] != verdict["n_band_degraded"]:
			raise ValueError("Non-DL degraded counts must be equal")
		else:
			expected_status = (
				"degraded"
				if verdict["n_degraded"]
				else "improved"
				if verdict["n_improved"]
				else "normal"
			)
			if verdict["status"] != expected_status:
				raise ValueError("Classification status is inconsistent with metric counts")
			if metric_counts["counted_degraded"]:
				raise ValueError("Non-DL metrics cannot set the DL degraded-count flag")
		expected_worst = metric_counts["worst"]
		if not math.isclose(
			_number(verdict["worst_pct_diff"], "Classification worst_pct_diff"),
			expected_worst,
			rel_tol=1e-12,
			abs_tol=1e-12,
		):
			raise ValueError("Classification worst_pct_diff does not match metrics")
		_require_json_safe(verdict, "Compatibility classification verdict")


def _validate_metric_stat(metric_name: str, metric: object) -> None:
	if not isinstance(metric, dict):
		raise TypeError("Compatibility baseline metric values must be dictionaries")
	_require_exact_fields(metric, _METRIC_STAT_FIELDS, f"Baseline metric {metric_name!r}")
	if metric["metric"] != metric_name:
		raise ValueError("Compatibility baseline metric identity does not match its key")
	if metric["direction"] not in stats.VALID_DIRECTIONS:
		raise ValueError("Compatibility baseline metric direction is invalid")
	_nonempty_string(metric["source_table"], "Compatibility baseline metric source_table")
	_nonempty_string(metric["method"], "Compatibility baseline metric method")
	_non_negative_integer(metric["n"], "Compatibility baseline metric n")
	if metric["n"] <= 0:
		raise ValueError("Compatibility baseline metric n must be positive")
	_non_negative_integer(metric["n_excluded"], "Compatibility baseline metric n_excluded")
	if not isinstance(metric["deterministic"], bool):
		raise TypeError("Compatibility baseline metric deterministic must be boolean")
	for field in (
		"median", "mad", "mad_sigma", "iqr", "p01", "p05", "p25", "p50",
		"p75", "p95", "p99", "minimum", "maximum", "skewness", "kurtosis",
		"ci_low", "ci_high",
	):
		_number(metric[field], f"Compatibility baseline metric {field}")
	for field in ("lower_bound", "upper_bound"):
		_optional_number(metric[field], f"Compatibility baseline metric {field}")
	for field in ("mad", "mad_sigma", "iqr"):
		if float(metric[field]) < 0.0:
			raise ValueError(f"Compatibility baseline metric {field} must be non-negative")
	ordered = [
		float(metric[field])
		for field in ("minimum", "p01", "p05", "p25", "p50", "p75", "p95", "p99", "maximum")
	]
	if any(
		left > right and not math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)
		for left, right in zip(ordered, ordered[1:])
	):
		raise ValueError("Compatibility baseline metric percentiles are unordered")
	if not math.isclose(
		float(metric["median"]),
		float(metric["p50"]),
		rel_tol=1e-12,
		abs_tol=1e-12,
	):
		raise ValueError("Compatibility baseline metric median does not equal p50")
	if float(metric["ci_low"]) > float(metric["ci_high"]):
		raise ValueError("Compatibility baseline metric confidence interval is inverted")
	direction = metric["direction"]
	if direction == stats.DIRECTION_LOW_BAD and metric["upper_bound"] is not None:
		raise ValueError("low_bad compatibility metric must have an unbounded upper side")
	if direction == stats.DIRECTION_HIGH_BAD and metric["lower_bound"] is not None:
		raise ValueError("high_bad compatibility metric must have an unbounded lower side")
	if direction == stats.DIRECTION_TWO_SIDED and (
		metric["lower_bound"] is None or metric["upper_bound"] is None
	):
		raise ValueError("two_sided compatibility metric must have two finite bounds")
	if metric["lower_bound"] is not None and float(metric["lower_bound"]) > float(metric["median"]):
		raise ValueError("Compatibility baseline lower bound exceeds its median")
	if metric["upper_bound"] is not None and float(metric["upper_bound"]) < float(metric["median"]):
		raise ValueError("Compatibility baseline upper bound is below its median")


def _validate_metric_reports(metrics: list[object]) -> dict[str, int | float]:
	seen: set[tuple[str, str]] = set()
	degraded = 0
	counted_degraded = 0
	improved = 0
	worst = 0.0
	for metric in metrics:
		if not isinstance(metric, dict):
			raise TypeError("Classification metric reports must be dictionaries")
		_require_exact_fields(metric, _METRIC_REPORT_FIELDS, "Classification metric report")
		name = _nonempty_string(metric["metric"], "Classification metric name")
		component = _string(metric["component"], "Classification metric component")
		identity = (component, name)
		if identity in seen:
			raise ValueError("Classification metric identities must be unique")
		seen.add(identity)
		status = metric["status"]
		if status not in {"normal", "degraded", "improved"}:
			raise ValueError("Classification metric status is invalid")
		value = _number(metric["value"], "Classification metric value")
		median = _number(metric["median"], "Classification metric median")
		pct_diff = _number(metric["pct_diff"], "Classification metric pct_diff")
		abs_pct_diff = _number(metric["abs_pct_diff"], "Classification metric abs_pct_diff")
		expected_pct_diff = ((value - median) / median * 100.0) if median else 0.0
		if not math.isclose(
			pct_diff,
			expected_pct_diff,
			rel_tol=1e-12,
			abs_tol=1e-12,
		):
			raise ValueError("Classification metric pct_diff is inconsistent")
		if abs_pct_diff < 0.0 or not math.isclose(
			abs_pct_diff, abs(pct_diff), rel_tol=1e-12, abs_tol=1e-12
		):
			raise ValueError("Classification metric abs_pct_diff is inconsistent")
		if not isinstance(metric["counts_for_degraded_status"], bool):
			raise TypeError("Classification metric degraded-count flag must be boolean")
		if metric["counts_for_degraded_status"] and status != "degraded":
			raise ValueError("Only degraded metrics can count toward degraded status")
		if metric["direction"] not in stats.VALID_DIRECTIONS:
			raise ValueError("Classification metric direction is invalid")
		_optional_number(metric["lower_bound"], "Classification metric lower_bound")
		_optional_number(metric["upper_bound"], "Classification metric upper_bound")
		if status == "degraded":
			degraded += 1
			worst = max(worst, abs_pct_diff)
		if metric["counts_for_degraded_status"]:
			counted_degraded += 1
		if status == "improved":
			improved += 1
	return {
		"degraded": degraded,
		"counted_degraded": counted_degraded,
		"improved": improved,
		"worst": worst,
	}


def _validate_dl_components(verdict: dict[str, Any]) -> str:
	"""Validate and derive every DL component plus the top-level status."""

	components = verdict["components"]
	if not isinstance(components, dict) or set(components) != _DL_COMPONENTS:
		raise ValueError("DL classification components are incomplete or unknown")
	severity = _number(verdict["dl_degraded_severity_pct"], "DL degraded severity")
	minimum = _non_negative_integer(
		verdict["dl_min_degraded_metrics"], "DL minimum degraded metrics"
	)
	fraction_threshold = _number(
		verdict["dl_degraded_metric_fraction_threshold"],
		"DL degraded fraction threshold",
	)
	if severity < 0.0:
		raise ValueError("DL degraded severity must be non-negative")
	if minimum <= 0:
		raise ValueError("DL minimum degraded metrics must be positive")
	if not 0.0 <= fraction_threshold <= 1.0:
		raise ValueError("DL degraded fraction threshold must be between zero and one")

	metric_groups: dict[str, list[dict[str, Any]]] = {
		component: [] for component in _DL_COMPONENTS
	}
	for metric in verdict["metrics"]:
		component = metric["component"]
		metric_groups[component].append(metric)
		expected_flag = (
			metric["status"] == "degraded"
			and float(metric["abs_pct_diff"]) >= severity
		)
		if metric["counts_for_degraded_status"] is not expected_flag:
			raise ValueError(
				"DL metric degraded-count flag contradicts the severity threshold"
			)

	for component_name, summary in components.items():
		if not isinstance(summary, dict):
			raise TypeError("DL component summaries must be dictionaries")
		_require_exact_fields(summary, _DL_COMPONENT_FIELDS, "DL component summary")
		if summary["component"] != component_name:
			raise ValueError("DL component summary identity does not match its key")
		if summary["status"] not in {"normal", "degraded", "improved"}:
			raise ValueError("DL component summary status is invalid")
		for field in ("n_compared", "n_degraded", "n_band_degraded", "n_improved"):
			_non_negative_integer(summary[field], f"DL component {field}")
		if not (0 <= summary["n_degraded"] <= summary["n_band_degraded"] <= summary["n_compared"]):
			raise ValueError("DL component degraded counts are inconsistent")
		if not (0 <= summary["n_improved"] <= summary["n_compared"]):
			raise ValueError("DL component improved count is inconsistent")
		_validate_fraction_scores(summary, "DL component summary")
		component_worst = _number(
			summary["worst_pct_diff"], "DL component worst_pct_diff"
		)
		component_severity = _number(
			summary["dl_degraded_severity_pct"], "DL component degraded severity"
		)
		component_minimum = _non_negative_integer(
			summary["dl_min_degraded_metrics"], "DL component minimum metrics"
		)
		component_fraction_threshold = _number(
			summary["dl_degraded_metric_fraction_threshold"],
			"DL component fraction threshold",
		)
		if (
			not math.isclose(component_severity, severity, rel_tol=1e-12, abs_tol=1e-12)
			or component_minimum != minimum
			or not math.isclose(
				component_fraction_threshold,
				fraction_threshold,
				rel_tol=1e-12,
				abs_tol=1e-12,
			)
		):
			raise ValueError("DL component aggregation thresholds do not match the verdict")

		derived = _derive_dl_summary(
			metric_groups[component_name],
			minimum=minimum,
			fraction_threshold=fraction_threshold,
		)
		for field in ("n_compared", "n_degraded", "n_band_degraded", "n_improved"):
			if summary[field] != derived[field]:
				raise ValueError(f"DL component {field} does not match metric reports")
		if not math.isclose(
			component_worst,
			derived["worst_pct_diff"],
			rel_tol=1e-12,
			abs_tol=1e-12,
		):
			raise ValueError("DL component worst_pct_diff does not match metric reports")
		if summary["status"] != derived["status"]:
			raise ValueError("DL component status does not match metric aggregation")

	top_level = _derive_dl_summary(
		verdict["metrics"],
		minimum=minimum,
		fraction_threshold=fraction_threshold,
	)
	if any(summary["status"] == "degraded" for summary in components.values()):
		return "degraded"
	if (
		any(summary["status"] == "improved" for summary in components.values())
		and top_level["status"] != "degraded"
	):
		return "improved"
	return str(top_level["status"])


def _derive_dl_summary(
	metrics: list[dict[str, Any]],
	*,
	minimum: int,
	fraction_threshold: float,
) -> dict[str, int | float | str]:
	"""Derive compatibility DL aggregation from validated metric reports."""

	n_compared = len(metrics)
	n_degraded = sum(
		1 for metric in metrics if metric["counts_for_degraded_status"]
	)
	n_band_degraded = sum(
		1 for metric in metrics if metric["status"] == "degraded"
	)
	n_improved = sum(1 for metric in metrics if metric["status"] == "improved")
	degraded_fraction = n_degraded / n_compared if n_compared else 0.0
	worst = max(
		(
			float(metric["abs_pct_diff"])
			for metric in metrics
			if metric["status"] == "degraded"
		),
		default=0.0,
	)
	if n_degraded and (
		n_degraded >= minimum or degraded_fraction >= fraction_threshold
	):
		status = "degraded"
	elif n_improved and not n_degraded:
		status = "improved"
	else:
		status = "normal"
	return {
		"status": status,
		"n_compared": n_compared,
		"n_degraded": n_degraded,
		"n_band_degraded": n_band_degraded,
		"n_improved": n_improved,
		"worst_pct_diff": worst,
	}


def _validate_fraction_scores(value: dict[str, Any], label: str) -> None:
	fraction = _number(value["degraded_metric_fraction"], f"{label} degraded fraction")
	percent = _number(value["degraded_metric_percent"], f"{label} degraded percent")
	if not 0.0 <= fraction <= 1.0:
		raise ValueError(f"{label} degraded fraction must be between zero and one")
	expected = value["n_degraded"] / value["n_compared"] if value["n_compared"] else 0.0
	if not math.isclose(fraction, expected, rel_tol=1e-12, abs_tol=1e-12):
		raise ValueError(f"{label} degraded fraction does not match counts")
	if not math.isclose(percent, fraction * 100.0, rel_tol=1e-12, abs_tol=1e-12):
		raise ValueError(f"{label} degraded percent does not match fraction")


def _require_exact_fields(value: dict[str, Any], expected: frozenset[str], label: str) -> None:
	missing = sorted(expected - set(value))
	unknown = sorted(set(value) - expected)
	if missing or unknown:
		parts = []
		if missing:
			parts.append("missing: " + ", ".join(missing))
		if unknown:
			parts.append("unknown: " + ", ".join(unknown))
		raise ValueError(f"{label} fields are not exact ({'; '.join(parts)})")


def _string(value: object, label: str) -> str:
	if not isinstance(value, str) or "\x00" in value or "\n" in value or "\r" in value:
		raise TypeError(f"{label} must be a single-line string")
	return value


def _nonempty_string(value: object, label: str) -> str:
	parsed = _string(value, label)
	if not parsed.strip():
		raise ValueError(f"{label} must be non-empty")
	return parsed


def _non_negative_integer(value: object, label: str) -> int:
	if isinstance(value, bool) or not isinstance(value, int):
		raise TypeError(f"{label} must be an integer")
	if value < 0:
		raise ValueError(f"{label} must be non-negative")
	return value


def _number(value: object, label: str) -> float:
	if isinstance(value, bool) or not isinstance(value, int | float):
		raise TypeError(f"{label} must be numeric")
	parsed = float(value)
	if not math.isfinite(parsed):
		raise ValueError(f"{label} must be finite")
	return parsed


def _optional_number(value: object, label: str) -> float | None:
	return None if value is None else _number(value, label)


def _require_json_safe(value: object, label: str) -> None:
	try:
		json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
	except (TypeError, ValueError) as exc:
		raise ValueError(f"{label} is not strict JSON-safe: {exc}") from exc


__all__ = [
	"build_compatibility_baseline",
	"classify_compatibility_target",
	"export_compatibility_rows",
	"resolve_operational_target",
	"validate_compatibility_baseline_record",
	"validate_compatibility_classification_verdicts",
]
