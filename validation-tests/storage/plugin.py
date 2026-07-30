"""Storage metric ingestion adapter for ``cval.plugin.v1``."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from functools import lru_cache

from cval.health.engine import metric_specs_from_definition, validate_source_snapshot
from cval.health.models import HealthContext, MetricObservation, MetricSpec
from cval.health.sqlite_values import sqlite_integer, sqlite_number, sqlite_text
from cval.storage.ingest import STORAGE_METRIC_COLUMNS, parse_storage_metrics
from cval.storage.per_test_results import (
    COMMON_IMMUTABLE_KEY_GROUPS,
    COMMON_RESULT_TABLES,
    canonical_payload_digest,
    existing_metric_ingestion_receipt,
    metric_ingestion_transaction,
    record_adapter_schema_version,
    record_metric_ingestion_receipt,
    prepare_immutable_table_triggers,
    require_schema_objects,
    require_database_tables,
    require_database_views,
    require_exact_table_sql,
    validate_table_manifest,
    validate_common_result_connection,
    validate_health_read_metadata,
)
from cval.validation.plugins import (
    ConfigIssue,
    IngestionConflictError,
    IngestionContext,
    IngestionReceipt,
    validate_ingestion_artifact_tree,
)


CVAL_PLUGIN_API = "cval.plugin.v1"
ADAPTER_SCHEMA_VERSION = 1
STORAGE_IMMUTABLE_KEY_GROUPS = (("node", "timestamp"), ("run_id",))


def _prepare_storage_metric_schema(connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS storage_performance (
            node TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            image_name TEXT NOT NULL DEFAULT '',
            iodepth_read_1file_iops REAL, iodepth_read_1file_bw REAL,
            iodepth_write_1file_iops REAL, iodepth_write_1file_bw REAL,
            numjobs_read_nfiles_iops REAL, numjobs_read_nfiles_bw REAL,
            numjobs_write_nfiles_iops REAL, numjobs_write_nfiles_bw REAL,
            randread_iops REAL, randread_bw REAL,
            randwrite_iops REAL, randwrite_bw REAL,
            run_id TEXT,
            PRIMARY KEY (node, timestamp)
        )
        """
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_performance_run_id "
        "ON storage_performance(run_id) WHERE run_id IS NOT NULL"
    )
    prepare_immutable_table_triggers(
        connection,
        "storage_performance",
        STORAGE_IMMUTABLE_KEY_GROUPS,
    )


@lru_cache(maxsize=1)
def _storage_table_sql() -> str:
    with closing(sqlite3.connect(":memory:")) as connection:
        _prepare_storage_metric_schema(connection)
        return str(
            connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' "
                "AND name='storage_performance'"
            ).fetchone()[0]
        )


def _validate_storage_schema(connection, allow_missing: bool) -> bool:
    require_database_tables(
        connection,
        set(COMMON_RESULT_TABLES) | {"storage_performance"},
    )
    immutable_tables = dict(COMMON_IMMUTABLE_KEY_GROUPS)
    if "storage_performance" in {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }:
        immutable_tables["storage_performance"] = STORAGE_IMMUTABLE_KEY_GROUPS
    require_database_views(
        connection,
        set(),
        immutable_tables=immutable_tables,
    )
    metric_specs = {
        column: ("REAL", False, None, 0) for column in STORAGE_METRIC_COLUMNS
    }
    present = validate_table_manifest(
        connection,
        "storage_performance",
        required_columns={
            "node",
            "timestamp",
            "image_name",
            *STORAGE_METRIC_COLUMNS,
            "run_id",
        },
        column_specs={
            "node": ("TEXT", True, None, 1),
            "timestamp": ("INTEGER", True, None, 2),
            "image_name": ("TEXT", True, "''", 0),
            **metric_specs,
            "run_id": ("TEXT", False, None, 0),
        },
        primary_key=("node", "timestamp"),
        allowed_indexes={"idx_storage_performance_run_id"},
        implicit_indexes={
            ("pk", ("node", "timestamp"), ("BINARY", "BINARY"))
        },
        immutable_key_groups=STORAGE_IMMUTABLE_KEY_GROUPS,
        constraint_counts=(0, 0),
        allow_missing=allow_missing,
    )
    if present:
        require_exact_table_sql(
            connection,
            "storage_performance",
            _storage_table_sql(),
        )
        require_schema_objects(
            connection,
            indexes={
                "idx_storage_performance_run_id": (
                    "storage_performance",
                    ("run_id",),
                    (False,),
                    ("BINARY",),
                    True,
                    "WHERE RUN_ID IS NOT NULL",
                )
            },
        )
    return present


class StorageIngestionPlugin:
    plugin_id = "storage"
    health_policy_version = "storage.health.v1"
    capabilities = frozenset({"config", "ingest", "health"})

    def validate_schema(self, connection, allow_missing: bool) -> bool:
        return _validate_storage_schema(connection, allow_missing)

    def validate_config(self, definition) -> tuple[ConfigIssue, ...]:
        settings = definition.settings
        issues = []
        unknown = sorted(set(settings) - {"install_fio"})
        if unknown:
            issues.append(ConfigIssue("unknown_setting", ", ".join(unknown)))
        if not isinstance(settings.get("install_fio"), bool):
            issues.append(ConfigIssue("invalid_install_fio", "install_fio must be boolean"))
        return tuple(issues)

    def metric_specs(self, definition) -> tuple[MetricSpec, ...]:
        return metric_specs_from_definition(definition)

    def load_observations(
        self,
        context: HealthContext,
    ) -> tuple[MetricObservation, ...]:
        if context.combination is None or not context.source_snapshot.results:
            return ()
        validate_source_snapshot(context.source_snapshot)
        result_ids = context.source_snapshot.result_ids
        placeholders = ", ".join("?" for _ in result_ids)
        with closing(
            sqlite3.connect(
                f"file:{context.result_db_path}?mode=ro",
                uri=True,
                timeout=30,
            )
        ) as connection:
            validate_common_result_connection(connection)
            _validate_storage_schema(connection, False)
            metadata = validate_health_read_metadata(
                connection,
                test_id=self.plugin_id,
                adapter_schema_version=ADAPTER_SCHEMA_VERSION,
                result_ids=result_ids,
                source_snapshot=context.source_snapshot,
                definition=context.definition,
                combination=context.combination,
            )
            identities = metadata.identities
            for source_result in context.source_snapshot.results:
                identity = identities[source_result.result_id]
                if (source_result.run_id, source_result.completed_timestamp) != (
                    identity.run_id,
                    identity.completed_timestamp,
                ):
                    raise RuntimeError("Storage health source snapshot identity is invalid")
            rows = connection.execute(
                "SELECT tr.result_id, tr.run_id, "
                "COALESCE(tr.completed_timestamp, tr.started_timestamp), "
                "sp.node, sp.timestamp, sp.image_name, "
                f"{', '.join(f'sp.{column}' for column in STORAGE_METRIC_COLUMNS)} "
                "FROM test_results tr "
                "JOIN metric_ingestion_receipts mr ON mr.run_id=tr.run_id "
                "AND mr.test_id='storage' "
                "JOIN storage_performance sp ON sp.run_id=tr.run_id "
                "WHERE tr.status='pass' AND tr.combination_key=? "
                f"AND tr.result_id IN ({placeholders}) ORDER BY tr.result_id",
                (context.combination.key, *result_ids),
            ).fetchall()
            orphan = connection.execute(
                """
                SELECT 1 FROM storage_performance sp
                LEFT JOIN test_results tr ON tr.run_id=sp.run_id
                WHERE sp.run_id IS NULL OR tr.run_id IS NULL LIMIT 1
                """
            ).fetchone()
            if orphan is not None:
                raise RuntimeError("Storage health database contains orphan metric rows")
        if len(rows) != len(result_ids):
            raise RuntimeError("Storage health source rows are incomplete or duplicated")
        observations = []
        for row in rows:
            result_id = sqlite_integer(row[0], "storage result_id", positive=True)
            identity = identities[result_id]
            run_id = sqlite_text(row[1], "storage run_id")
            completed_timestamp = sqlite_integer(
                row[2], "storage completed_timestamp", non_negative=True
            )
            metric_timestamp = sqlite_integer(
                row[4], "storage metric timestamp", non_negative=True
            )
            metric_owner = (
                sqlite_text(row[3], "storage metric node"),
                sqlite_text(row[5], "storage metric image", nonempty=False),
            )
            if (
                (run_id, completed_timestamp)
                != (identity.run_id, identity.completed_timestamp)
                or metric_timestamp != identity.run_timestamp
                or metric_owner
                != (identity.node, identity.image_name)
            ):
                raise RuntimeError("Storage health metric ownership is invalid")
            for column, value in zip(STORAGE_METRIC_COLUMNS, row[6:]):
                if value is None:
                    continue
                observations.append(
                    MetricObservation(
                        result_id=result_id,
                        run_id=run_id,
                        completed_timestamp=completed_timestamp,
                        source="storage_performance",
                        metric_name=column,
                        sample_key=column,
                        value=sqlite_number(value, f"storage {column}"),
                    )
                )
            persisted_metrics = {
                column: sqlite_number(value, f"storage {column}")
                for column, value in zip(STORAGE_METRIC_COLUMNS, row[6:])
            }
            receipt = metadata.receipts[run_id]
            if (
                receipt.inserted_count != 1
                or receipt.updated_count != 0
                or receipt.metric_names != tuple(sorted(STORAGE_METRIC_COLUMNS))
                or receipt.evidence_digest
                != canonical_payload_digest(
                    {
                        "run_id": run_id,
                        "node": metric_owner[0],
                        "timestamp": sqlite_integer(
                            row[4], "storage metric timestamp", non_negative=True
                        ),
                        "image_name": metric_owner[1],
                        "metrics": persisted_metrics,
                    }
                )
            ):
                raise RuntimeError("Storage health receipt does not match metric content")
        return tuple(observations)

    def ingest(self, context: IngestionContext) -> IngestionReceipt:
        validate_ingestion_artifact_tree(context.execution.artifacts_path)
        metrics = parse_storage_metrics(context.execution.artifacts_path)
        evidence_digest = canonical_payload_digest(
            {
                "run_id": context.run.run_id,
                "node": context.run.node,
                "timestamp": context.run.started_timestamp,
                "image_name": context.run.image_name,
                "metrics": metrics,
            }
        )
        with metric_ingestion_transaction(
            context.result_db_path,
            test_id=self.plugin_id,
            adapter_schema_version=ADAPTER_SCHEMA_VERSION,
            validate_adapter_schema=_validate_storage_schema,
        ) as connection:
            _prepare_storage_metric_schema(connection)
            record_adapter_schema_version(
                connection,
                test_id=self.plugin_id,
                version=ADAPTER_SCHEMA_VERSION,
            )
            _validate_storage_schema(connection, False)

            existing = existing_metric_ingestion_receipt(
                connection,
                test_id=self.plugin_id,
                run_id=context.run.run_id,
                evidence_digest=evidence_digest,
                expected_inserted_count=1,
                expected_updated_count=0,
                expected_metric_names=tuple(STORAGE_METRIC_COLUMNS),
            )
            if existing is not None:
                row = connection.execute(
                    "SELECT node, timestamp, image_name, "
                    f"{', '.join(STORAGE_METRIC_COLUMNS)} "
                    "FROM storage_performance WHERE run_id=?",
                    (context.run.run_id,),
                ).fetchone()
                expected_row = (
                    context.run.node,
                    context.run.started_timestamp,
                    context.run.image_name,
                    *(metrics[column] for column in STORAGE_METRIC_COLUMNS),
                )
                if row is None or tuple(row) != expected_row:
                    raise IngestionConflictError(
                        f"Storage receipt for {context.run.run_id!r} does not match "
                        "the persisted metric row"
                    )
                return existing

            collision = connection.execute(
                "SELECT run_id FROM storage_performance WHERE node=? AND timestamp=?",
                (context.run.node, context.run.started_timestamp),
            ).fetchone()
            if collision is not None:
                raise IngestionConflictError(
                    "Storage node/timestamp already exists without an exact run receipt"
                )
            metric_columns = ", ".join(STORAGE_METRIC_COLUMNS)
            placeholders = ", ".join("?" for _ in STORAGE_METRIC_COLUMNS)
            connection.execute(
                f"""
                INSERT INTO storage_performance (
                    node, timestamp, image_name, {metric_columns}, run_id
                ) VALUES (?, ?, ?, {placeholders}, ?)
                """,
                (
                    context.run.node,
                    context.run.started_timestamp,
                    context.run.image_name,
                    *(metrics[column] for column in STORAGE_METRIC_COLUMNS),
                    context.run.run_id,
                ),
            )
            receipt = IngestionReceipt(
                test_id=self.plugin_id,
                run_id=context.run.run_id,
                inserted_count=1,
                updated_count=0,
                metric_names=tuple(STORAGE_METRIC_COLUMNS),
                evidence_digest=evidence_digest,
                created_at=0,
            )
            receipt = record_metric_ingestion_receipt(
                connection,
                receipt,
                evidence_digest=evidence_digest,
            )
            return receipt


PLUGIN = StorageIngestionPlugin()
