"""Storage validation config and raw result export hooks."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from cval.validation.plugins import (
    ConfigIssue,
    ExportContext,
    ExportRows,
    export_rows_from_records,
)

CVAL_PLUGIN_API = "cval.plugin.v1"


class StoragePlugin:
    plugin_id = "storage"
    capabilities = frozenset({"config", "export"})

    def validate_config(self, definition) -> tuple[ConfigIssue, ...]:
        settings = definition.settings
        issues = []
        unknown = sorted(set(settings) - {"install_fio", "fio", "workloads"})
        if unknown:
            issues.append(ConfigIssue("unknown_setting", ", ".join(unknown)))
        if not isinstance(settings.get("install_fio"), bool):
            issues.append(ConfigIssue("invalid_install_fio", "install_fio must be boolean"))
        common = settings.get("fio")
        if not isinstance(common, Mapping) or set(common) != {"ioengine", "direct", "gtod_reduce", "size", "time_based", "runtime", "group_reporting"}:
            issues.append(ConfigIssue("invalid_fio", "fio must explicitly define all common workload settings"))
        else:
            for key in ("ioengine", "size"):
                if not isinstance(common[key], str) or not common[key]:
                    issues.append(ConfigIssue("invalid_fio", f"fio.{key} must be a nonempty string"))
            for key in ("direct", "gtod_reduce"):
                if type(common[key]) is not int or common[key] not in (0, 1):
                    issues.append(ConfigIssue("invalid_fio", f"fio.{key} must be 0 or 1"))
            if type(common["runtime"]) is not int or common["runtime"] <= 0:
                issues.append(ConfigIssue("invalid_fio", "fio.runtime must be positive"))
            for key in ("time_based", "group_reporting"):
                if type(common[key]) is not bool:
                    issues.append(ConfigIssue("invalid_fio", f"fio.{key} must be boolean"))
        workloads = settings.get("workloads")
        expected = {"randwrite", "randread", "iodepth_write_1file", "iodepth_read_1file", "numjobs_write_nfiles", "numjobs_read_nfiles"}
        if not isinstance(workloads, Mapping) or set(workloads) != expected:
            issues.append(ConfigIssue("invalid_workloads", "workloads must define the six raw storage measurements"))
        else:
            for name, workload in workloads.items():
                filename_key = "filename_format" if name.startswith("numjobs_") else "filename"
                if not isinstance(workload, Mapping) or set(workload) != {"name", "rw", "bs", "iodepth", "numjobs", filename_key}:
                    issues.append(ConfigIssue("invalid_workload", f"{name} must explicitly define its FIO settings"))
                    continue
                for key in ("name", "rw", "bs", filename_key):
                    if not isinstance(workload[key], str) or not workload[key]:
                        issues.append(ConfigIssue("invalid_workload", f"{name}.{key} must be nonempty"))
                filename = workload[filename_key]
                if isinstance(filename, str) and (Path(filename).name != filename or filename in {".", ".."}):
                    issues.append(ConfigIssue("invalid_workload", f"{name} filename must stay in the run directory"))
                for key in ("iodepth", "numjobs"):
                    if type(workload[key]) is not int or workload[key] <= 0:
                        issues.append(ConfigIssue("invalid_workload", f"{name}.{key} must be positive"))
        return tuple(issues)

    def export_rows(self, context: ExportContext) -> ExportRows:
        from cval.storage.metrics import get_latest_storage_metrics
        from cval.storage.results_export import (
            get_csv_columns,
            latest_result_rows,
            rows_to_csv_records,
        )

        metrics = None
        if context.include_metrics:
            metrics = get_latest_storage_metrics(
                pod=context.pod,
                namespace=context.namespace,
                db_path=context.source_db_path("storage"),
                config=context.config,
            )
        selected = latest_result_rows(list(context.status_rows), context.target.name)
        records = rows_to_csv_records(
            selected,
            context.target.name,
            storage_metrics=metrics,
        )
        columns = get_csv_columns(context.target.name)
        projected = ({column: record.get(column, "") for column in columns} for record in records)
        return export_rows_from_records(columns, projected)


PLUGIN = StoragePlugin()
