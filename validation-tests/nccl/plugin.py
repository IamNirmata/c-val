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
            "shm_disable", "debug", "evaluation_enabled",
            "evaluation_test_name", "evaluation_test_definition_version",
            "evaluation_collective",
            "evaluation_datatype", "evaluation_reduction",
            "evaluation_message_size_bytes", "evaluation_warmup_iterations",
            "evaluation_samples_per_result", "evaluation_iteration_semantics",
            "evaluation_sample_semantics",
            "evaluation_latency_unit", "evaluation_latency_source_unit",
            "evaluation_latency_conversion", "evaluation_driver_group_source",
            "evaluation_topology_class_source",
        }
        issues = []
        unknown = sorted(set(settings) - allowed)
        if unknown:
            issues.append(ConfigIssue("unknown_setting", ", ".join(unknown)))
        for key in ("gpu_count", "iterations", "data_size_gb"):
            value = settings.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be positive integer"))
        for key in ("ibbw_enabled", "p2p_disable", "shm_disable", "evaluation_enabled"):
            if not isinstance(settings.get(key), bool):
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be boolean"))
        for key in (
            "net", "debug", "evaluation_test_name",
            "evaluation_test_definition_version",
            "evaluation_collective", "evaluation_datatype", "evaluation_reduction",
            "evaluation_iteration_semantics", "evaluation_sample_semantics",
            "evaluation_latency_unit", "evaluation_latency_source_unit",
            "evaluation_latency_conversion",
            "evaluation_driver_group_source", "evaluation_topology_class_source",
        ):
            if not isinstance(settings.get(key), str) or not settings.get(key).strip():
                issues.append(ConfigIssue(f"invalid_{key}", f"{key} must be non-empty string"))
        message_size = settings.get("evaluation_message_size_bytes")
        if (
            isinstance(message_size, bool)
            or not isinstance(message_size, int)
            or message_size <= 0
        ):
            issues.append(
                ConfigIssue(
                    "invalid_evaluation_message_size_bytes",
                    "evaluation_message_size_bytes must be a positive integer",
                )
            )
        elif message_size != settings.get("data_size_gb", 0) * 1024 * 1024 * 1024 * 2:
            issues.append(
                ConfigIssue(
                    "mismatched_evaluation_message_size_bytes",
                    "evaluation message bytes must match BF16 data_size_gb elements",
                )
            )
        warmup = settings.get("evaluation_warmup_iterations")
        if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup < 0:
            issues.append(
                ConfigIssue(
                    "invalid_evaluation_warmup_iterations",
                    "evaluation_warmup_iterations must be a non-negative integer",
                )
            )
        samples = settings.get("evaluation_samples_per_result")
        if isinstance(samples, bool) or not isinstance(samples, int) or samples <= 0:
            issues.append(
                ConfigIssue(
                    "invalid_evaluation_samples_per_result",
                    "evaluation_samples_per_result must be a positive integer",
                )
            )
        for key, expected in (
            ("evaluation_latency_unit", "us"),
            ("evaluation_latency_source_unit", "ms"),
            ("evaluation_latency_conversion", "ms_to_us_x1000"),
            ("evaluation_driver_group_source", "runtime_evidence"),
            ("evaluation_topology_class_source", "runtime_evidence"),
            ("evaluation_iteration_semantics", "timed_collective_repetitions"),
            ("evaluation_sample_semantics", "one_aggregate_mean_per_node"),
        ):
            if settings.get(key) != expected:
                issues.append(
                    ConfigIssue(f"invalid_{key}", f"{key} must be {expected!r}")
                )
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
