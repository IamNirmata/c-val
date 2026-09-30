"""DL validation config and raw result export hooks."""

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


class DltestPlugin:
    plugin_id = "dltest"
    capabilities = frozenset({"config", "export", "log", "ingest", "runtime"})
    write_log = staticmethod(write_log)
    format_output = staticmethod(logger["format_output"])

    def runtime_environment(self, test, config) -> dict[str, str]:
        settings = test.definition.settings
        return {
            "RUN_DLTEST": str(test.enabled).lower(), "CVAL_DL_UNIT_TEST_DIR": config.runtime.dl_unit_test_dir,
            "CVAL_DL_GPU_COUNT": str(settings["gpu_count"]), "CVAL_DL_TEST_PLAN": str(settings["test_plan"]),
            "CVAL_DL_ITERATIONS": str(settings["iterations"]),
            "CVAL_DL_NUMERICAL_DB_PATH": config.storage.dl_numerical_db_path,
            "CVAL_DL_COMPUTE_DB_PATH": config.storage.dl_compute_db_path,
            "CVAL_DL_COLLECTIVE_DB_PATH": config.storage.dl_collective_db_path,
            "CVAL_DL_OVERLAP_DB_PATH": config.storage.dl_overlap_db_path,
            "CVAL_DL_METRIC_LOCK_FILE": f"{config.runtime.validation_root.rstrip('/')}/metadata/.dl-metric-ingest.lock",
        }

    def evidence_environment(self, context) -> dict[str, str]:
        return context.evidence_aliases(run="DLTEST_RUN_DIR", artifacts="DLTEST_OUTPUT_DIR",
                                        summary="DLTEST_SUMMARY_FILE", log="DLTEST_LOG_FILE")

    def raw_stores(self, config) -> dict[str, str]:
        return {
            "numerical_correctness": config.storage.dl_numerical_db_path,
            "compute_performance": config.storage.dl_compute_db_path,
            "collective_performance": config.storage.dl_collective_db_path,
            "overlap_performance": config.storage.dl_overlap_db_path,
        }

    def ingest(self, context) -> dict[str, dict]:
        return load_test_support(Path(__file__).with_name("ingest.py")).ingest(context)

    def validate_config(self, definition) -> tuple[ConfigIssue, ...]:
        settings = definition.settings
        allowed = {"gpu_count", "test_plan", "iterations"}
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
        return tuple(issues)

    def export_rows(self, context: ExportContext) -> ExportRows:
        from cval.storage.results_export import (
            get_csv_columns,
            latest_result_rows,
            rows_to_csv_records,
        )

        selected = latest_result_rows(list(context.status_rows), context.target.name)
        records = rows_to_csv_records(
            selected, context.target.name
        )
        columns = get_csv_columns(context.target.name)
        projected = ({column: record.get(column, "") for column in columns} for record in records)
        return export_rows_from_records(columns, projected)


PLUGIN = DltestPlugin()
