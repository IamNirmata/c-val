"""DL component hooks for the canonical c-val evaluator."""

from __future__ import annotations

import math
from collections.abc import Mapping

from cval.validation.plugins import (
    ConfigIssue,
    ExportContext,
    ExportRows,
    export_rows_from_records,
)

CVAL_PLUGIN_API = "cval.plugin.v1"


class DltestPlugin:
    plugin_id = "dltest"
    capabilities = frozenset({"config", "baseline", "export"})

    def validate_config(self, definition) -> tuple[ConfigIssue, ...]:
        settings = definition.settings
        allowed = {"gpu_count", "test_plan", "iterations", "health_aggregation"}
        issues = []
        unknown = sorted(set(settings) - allowed)
        if unknown:
            issues.append(ConfigIssue("unknown_setting", ", ".join(unknown)))
        for key in ("gpu_count", "iterations"):
            value = settings.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be positive integer"))
        plan = settings.get("test_plan")
        if not isinstance(plan, str) or not plan.strip():
            issues.append(ConfigIssue("invalid_test_plan", "test_plan must be non-empty string"))
        aggregation = settings.get("health_aggregation")
        expected = {"degraded_metric_fraction", "min_degraded_metrics", "degraded_severity_pct"}
        if not isinstance(aggregation, Mapping) or set(aggregation) != expected:
            issues.append(ConfigIssue("invalid_health_aggregation", "health_aggregation keys are invalid"))
        else:
            fraction = aggregation["degraded_metric_fraction"]
            minimum = aggregation["min_degraded_metrics"]
            severity = aggregation["degraded_severity_pct"]
            if isinstance(fraction, bool) or not isinstance(fraction, int | float) or not 0 <= float(fraction) <= 1:
                issues.append(ConfigIssue("invalid_degraded_metric_fraction", "degraded_metric_fraction must be in [0,1]"))
            if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum <= 0:
                issues.append(ConfigIssue("invalid_min_degraded_metrics", "min_degraded_metrics must be positive"))
            if isinstance(severity, bool) or not isinstance(severity, int | float) or not math.isfinite(float(severity)) or float(severity) < 0:
                issues.append(ConfigIssue("invalid_degraded_severity_pct", "degraded_severity_pct must be finite and non-negative"))
        return tuple(issues)

    def build_baseline(self, context):
        from cval.baselines.build import build_dl_baseline

        return build_dl_baseline(
            config=context.config,
            window_days=context.window_days,
            min_samples=context.min_samples,
            test_plan=context.test_plan,
            baseline_id=context.baseline_id,
        )

    def classify(self, context, baseline) -> tuple[dict, ...]:
        from cval.baselines.classify import classify_node, classify_nodes

        if context.node:
            verdicts = [
                classify_node(
                    context.target.name,
                    context.node,
                    baseline,
                    config=context.config,
                    window_days=context.window_days,
                )
            ]
        else:
            verdicts = classify_nodes(
                context.target.name,
                baseline,
                config=context.config,
                window_days=context.window_days,
            )
        return tuple(verdicts)

    def export_rows(self, context: ExportContext) -> ExportRows:
        from cval.storage.results_export import (
            get_csv_columns,
            latest_result_rows,
            rows_to_csv_records,
        )

        selected = latest_result_rows(list(context.status_rows), context.target.name)
        records = rows_to_csv_records(
            selected, context.target.name, list(context.classification_rows)
        )
        columns = get_csv_columns(context.target.name)
        projected = ({column: record.get(column, "") for column in columns} for record in records)
        return export_rows_from_records(columns, projected)


PLUGIN = DltestPlugin()
