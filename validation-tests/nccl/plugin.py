"""NCCL hooks for the canonical c-val evaluator."""

from __future__ import annotations

from cval.validation.plugins import (
    ConfigIssue,
    ExportContext,
    ExportRows,
    export_rows_from_records,
)

CVAL_PLUGIN_API = "cval.plugin.v1"


class NcclPlugin:
    plugin_id = "nccl"
    capabilities = frozenset({"config", "export"})

    def validate_config(self, definition) -> tuple[ConfigIssue, ...]:
        settings = definition.settings
        allowed = {
            "gpu_count", "iterations", "data_size_gb", "ibbw_enabled",
            "ibbw_start_device", "ibbw_end_device", "net", "p2p_disable",
            "shm_disable", "debug",
        }
        issues = []
        unknown = sorted(set(settings) - allowed)
        if unknown:
            issues.append(ConfigIssue("unknown_setting", ", ".join(unknown)))
        for key in ("gpu_count", "iterations", "data_size_gb"):
            value = settings.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be positive integer"))
        for key in ("ibbw_enabled", "p2p_disable", "shm_disable"):
            if not isinstance(settings.get(key), bool):
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be boolean"))
        for key in ("net", "debug"):
            if not isinstance(settings.get(key), str) or not settings.get(key).strip():
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be non-empty string"))
        start = settings.get("ibbw_start_device")
        end = settings.get("ibbw_end_device")
        if (start is None) != (end is None):
            issues.append(ConfigIssue("invalid_ibbw_range", "both IBBW bounds are required"))
        elif start is not None and (
            isinstance(start, bool) or isinstance(end, bool)
            or not isinstance(start, int) or not isinstance(end, int)
            or start < 0 or end < start
        ):
            issues.append(ConfigIssue("invalid_ibbw_range", "IBBW bounds are invalid"))
        return tuple(issues)

    def export_rows(self, context: ExportContext) -> ExportRows:
        from cval.storage.metrics import get_latest_nccl_health_metrics
        from cval.storage.results_export import (
            NCCL_HEALTH_CSV_COLUMNS,
            latest_result_rows,
            nccl_health_rows_to_csv_records,
        )

        metrics = None
        if context.include_metrics:
            metrics = get_latest_nccl_health_metrics(
                pod=context.pod,
                namespace=context.namespace,
                db_path=context.source_db_path("nccl"),
                config=context.config,
            )
        selected = latest_result_rows(list(context.status_rows), context.target.name)
        records = nccl_health_rows_to_csv_records(selected, metrics)
        return export_rows_from_records(
            NCCL_HEALTH_CSV_COLUMNS, records, row_label="nccl IB_HEALTH"
        )


PLUGIN = NcclPlugin()
