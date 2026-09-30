"""NCCL validation config and raw IB health export hooks."""

from __future__ import annotations

from pathlib import Path
import runpy

write_log = runpy.run_path(str(Path(__file__).with_name("logger.py")))["write_log"]

from cval.validation.plugins import (
    ConfigIssue,
    ExportContext,
    ExportRows,
    export_rows_from_records,
)

CVAL_PLUGIN_API = "cval.plugin.v1"


class NcclPlugin:
    plugin_id = "nccl"
    capabilities = frozenset({"config", "export", "log"})
    write_log = staticmethod(write_log)

    def validate_config(self, definition) -> tuple[ConfigIssue, ...]:
        settings = definition.settings
        allowed = {
            "gpu_count", "iterations", "data_size_gb", "ibbw_enabled",
            "ibbw_start_device", "ibbw_end_device", "net", "p2p_disable",
            "shm_disable", "debug", "ibbw_interval_seconds", "ib_sys_root",
        }
        issues = []
        missing = sorted(allowed - set(settings))
        if missing:
            issues.append(ConfigIssue("missing_setting", ", ".join(missing)))
        unknown = sorted(set(settings) - allowed)
        if unknown:
            issues.append(ConfigIssue("unknown_setting", ", ".join(unknown)))
        for key in ("gpu_count", "iterations", "data_size_gb", "ibbw_interval_seconds"):
            value = settings.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be positive integer"))
        for key in ("ibbw_enabled", "p2p_disable", "shm_disable"):
            if not isinstance(settings.get(key), bool):
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be boolean"))
        for key in ("net", "debug", "ib_sys_root"):
            if not isinstance(settings.get(key), str) or not settings.get(key).strip():
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be non-empty string"))
        start = settings.get("ibbw_start_device")
        end = settings.get("ibbw_end_device")
        if (start, end) != ("", "") and (
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

        selected = latest_result_rows(list(context.status_rows), context.target.name)
        metrics = None
        if context.include_metrics:
            metrics = get_latest_nccl_health_metrics(
                pod=context.pod,
                namespace=context.namespace,
                db_path=context.source_db_path("nccl"),
                config=context.config,
                timestamps={row.node: row.latest_timestamp for row in selected},
            )
        records = nccl_health_rows_to_csv_records(selected, metrics)
        return export_rows_from_records(
            NCCL_HEALTH_CSV_COLUMNS, records, row_label="nccl IB_HEALTH"
        )


PLUGIN = NcclPlugin()
