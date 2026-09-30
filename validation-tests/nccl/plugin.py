"""NCCL validation config and raw IB health export hooks."""

from __future__ import annotations

from pathlib import Path
import runpy

logger = runpy.run_path(str(Path(__file__).with_name("logger.py")))
write_log = logger["write_log"]

from cval.validation.plugins import (
    ConfigIssue,
    ExportContext,
    ExportRows,
    export_rows_from_records,
    load_test_support,
)

CVAL_PLUGIN_API = "cval.plugin.v1"


class NcclPlugin:
    plugin_id = "nccl"
    capabilities = frozenset({"config", "export", "log", "ingest", "runtime"})
    write_log = staticmethod(write_log)
    format_output = staticmethod(logger["format_output"])

    def runtime_environment(self, test, config) -> dict[str, str]:
        names = {
            "gpu_count": "CVAL_NCCL_GPU_COUNT", "iterations": "CVAL_NCCL_ITERATIONS",
            "data_size_gb": "CVAL_NCCL_DATA_SIZE_GB", "ibbw_enabled": "CVAL_IBBW_ENABLED",
            "ibbw_start_device": "CVAL_IBBW_START_DEVICE", "ibbw_end_device": "CVAL_IBBW_END_DEVICE",
            "net": "CVAL_NCCL_NET", "p2p_disable": "CVAL_NCCL_P2P_DISABLE",
            "shm_disable": "CVAL_NCCL_SHM_DISABLE", "debug": "CVAL_NCCL_DEBUG",
            "ibbw_interval_seconds": "CVAL_IBBW_INTERVAL_SECONDS", "ib_sys_root": "CVAL_IB_SYS_ROOT",
        }
        values = {"RUN_NCCL": str(test.enabled).lower(), "CVAL_NCCL_DB_PATH": config.storage.nccl_db_path}
        for setting, name in names.items():
            value = test.definition.settings[setting]
            values[name] = str(value).lower() if isinstance(value, bool) else str(value)
        return values

    def evidence_environment(self, context) -> dict[str, str]:
        values = context.evidence_aliases(run="NCCL_RUN_DIR", artifacts="NCCL_OUTPUT_DIR",
                                          summary="NCCL_SUMMARY_FILE", log="NCCL_LOG_FILE")
        canonical = context.validation_root / "logs/nccl" / context.node / context.run_id
        sample_name = f"ibbw-{context.node}-{context.timestamp}.log"
        values.update(NCCL_IBBW_LOG_FILE=str(context.artifacts / sample_name),
                      CVAL_CANONICAL_NCCL_SUMMARY_FILE=str(canonical / context.test.definition.artifacts.summary_filename),
                      CVAL_CANONICAL_NCCL_IBBW_LOG_FILE=str(canonical / sample_name))
        return values

    def raw_stores(self, config) -> dict[str, str]:
        return {"nccl": config.storage.nccl_db_path}

    def ingest(self, context) -> dict[str, dict]:
        return load_test_support(Path(__file__).with_name("ingest.py")).ingest(context)

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
