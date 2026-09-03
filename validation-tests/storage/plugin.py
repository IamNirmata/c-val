"""Storage validation config and raw result export hooks."""

from __future__ import annotations

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
        unknown = sorted(set(settings) - {"install_fio"})
        if unknown:
            issues.append(ConfigIssue("unknown_setting", ", ".join(unknown)))
        if not isinstance(settings.get("install_fio"), bool):
            issues.append(ConfigIssue("invalid_install_fio", "install_fio must be boolean"))
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
