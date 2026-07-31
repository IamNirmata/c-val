"""NCCL/HCA metric ingestion adapter for ``cval.plugin.v1``."""

from __future__ import annotations

import math
import sqlite3
from contextlib import closing
from functools import lru_cache
from typing import Any

from cval.health.engine import metric_specs_from_definition, validate_source_snapshot
from cval.health.models import HealthContext, MetricObservation, MetricSpec
from cval.health.sqlite_values import sqlite_integer, sqlite_number, sqlite_text
from cval.storage.ingest import (
    NCCL_HEALTH_TABLE,
    NCCL_IB_PORT_COLUMNS,
    parse_nccl_health_summary,
    nccl_health_view_sql,
    prepare_nccl_health_schema,
    timestamp_to_los_angeles,
)
from cval.storage.sqlite_snapshot import health_read_connection
from cval.storage.per_test_results import (
    COMMON_RESULT_TABLES,
    common_immutable_key_groups,
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
    BaselineBuildContext,
    BaselineClassificationContext,
    ConfigIssue,
    ExportContext,
    ExportRows,
    IngestionConflictError,
    IngestionContext,
    IngestionReceipt,
    export_rows_from_records,
)


CVAL_PLUGIN_API = "cval.plugin.v1"
ADAPTER_SCHEMA_VERSION = 1
NCCL_IMMUTABLE_KEY_GROUPS = (("Node", "timestamp"), ("run_id",))


@lru_cache(maxsize=1)
def _nccl_table_sql() -> str:
    with closing(sqlite3.connect(":memory:")) as connection:
        prepare_nccl_health_schema(connection, include_run_id=True)
        prepare_immutable_table_triggers(
            connection,
            NCCL_HEALTH_TABLE,
            NCCL_IMMUTABLE_KEY_GROUPS,
        )
        return str(
            connection.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' "
                f"AND name='{NCCL_HEALTH_TABLE}'"
            ).fetchone()[0]
        )


def _validate_nccl_schema(connection, allow_missing: bool) -> bool:
    require_database_tables(
        connection,
        set(COMMON_RESULT_TABLES) | {NCCL_HEALTH_TABLE},
    )
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (NCCL_HEALTH_TABLE,),
    ).fetchone()
    require_database_views(
        connection,
        {"LATEST_NODE_STATUS", "NODE_RANKING"} if table_exists else set(),
        immutable_tables={
            **common_immutable_key_groups(connection),
            **(
                {NCCL_HEALTH_TABLE: NCCL_IMMUTABLE_KEY_GROUPS}
                if table_exists
                else {}
            ),
        },
    )
    port_specs = {
        column: ("REAL", False, None, 0) for column in NCCL_IB_PORT_COLUMNS
    }
    present = validate_table_manifest(
        connection,
        NCCL_HEALTH_TABLE,
        required_columns={
            "Node",
            "timestamp",
            "la_timestamp",
            "iterations",
            "data_size_gb",
            "image_name",
            "cuda",
            "pytorch",
            "samples",
            "BUS_BW",
            "LATENCY",
            *NCCL_IB_PORT_COLUMNS,
            "run_id",
        },
        column_specs={
            "Node": ("TEXT", True, None, 1),
            "timestamp": ("INTEGER", True, None, 2),
            "la_timestamp": ("TEXT", True, None, 0),
            "iterations": ("INTEGER", False, None, 0),
            "data_size_gb": ("INTEGER", False, None, 0),
            "image_name": ("TEXT", True, "''", 0),
            "cuda": ("TEXT", True, "''", 0),
            "pytorch": ("TEXT", True, "''", 0),
            "samples": ("INTEGER", False, None, 0),
            "BUS_BW": ("REAL", False, None, 0),
            "LATENCY": ("REAL", False, None, 0),
            **port_specs,
            "run_id": ("TEXT", False, None, 0),
        },
        primary_key=("Node", "timestamp"),
        allowed_indexes={f"idx_{NCCL_HEALTH_TABLE}_run_id"},
        implicit_indexes={
            ("pk", ("Node", "timestamp"), ("BINARY", "BINARY"))
        },
        immutable_key_groups=NCCL_IMMUTABLE_KEY_GROUPS,
        constraint_counts=(0, 0),
        allow_missing=allow_missing,
    )
    if present:
        require_exact_table_sql(
            connection,
            NCCL_HEALTH_TABLE,
            _nccl_table_sql(),
        )
        require_schema_objects(
            connection,
            indexes={
                f"idx_{NCCL_HEALTH_TABLE}_run_id": (
                    NCCL_HEALTH_TABLE,
                    ("run_id",),
                    (False,),
                    ("BINARY",),
                    True,
                    "WHERE RUN_ID IS NOT NULL",
                )
            },
            view_sql=nccl_health_view_sql(),
        )
    return present


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


class NcclIngestionPlugin:
    plugin_id = "nccl"
    health_policy_version = "nccl.health.v1"
    capabilities = frozenset({"config", "ingest", "health", "baseline", "export"})

    def validate_schema(self, connection, allow_missing: bool) -> bool:
        return _validate_nccl_schema(connection, allow_missing)

    def validate_config(self, definition) -> tuple[ConfigIssue, ...]:
        settings = definition.settings
        allowed = {
            "gpu_count",
            "iterations",
            "data_size_gb",
            "ibbw_enabled",
            "ibbw_start_device",
            "ibbw_end_device",
            "net",
            "p2p_disable",
            "shm_disable",
            "debug",
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
            isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or start < 0
            or end < start
        ):
            issues.append(ConfigIssue("invalid_ibbw_range", "IBBW bounds are invalid"))
        return tuple(issues)

    def metric_specs(self, definition) -> tuple[MetricSpec, ...]:
        return metric_specs_from_definition(definition)

    def build_compatibility_baseline(self, context: BaselineBuildContext):
        """Build from the established metadata/test-nccl.db surface."""

        from cval.baselines.build import build_nccl_baseline

        return build_nccl_baseline(
            config=context.config,
            db_path=context.source_db,
            window_days=context.window_days,
            min_samples=context.min_samples,
            image_name=context.image_name,
            node=context.node,
            baseline_id=context.baseline_id,
        )

    def classify_compatibility(
        self,
        context: BaselineClassificationContext,
        baseline,
    ) -> tuple[dict, ...]:
        """Classify from the established metadata/test-nccl.db surface."""

        from cval.baselines.classify import classify_node, classify_nodes

        if context.node:
            verdicts = [
                classify_node(
                    context.target.name,
                    context.node,
                    baseline,
                    config=context.config,
                    db_path=context.source_db,
                    window_days=context.window_days,
                )
            ]
        else:
            verdicts = classify_nodes(
                context.target.name,
                baseline,
                config=context.config,
                db_path=context.source_db,
                window_days=context.window_days,
            )
        return tuple(verdicts)

    def export_rows(self, context: ExportContext) -> ExportRows:
        """Return the established wide IB_HEALTH CSV projection."""

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
        records = nccl_health_rows_to_csv_records(
            selected,
            metrics,
            list(context.classification_rows),
        )
        return export_rows_from_records(
            NCCL_HEALTH_CSV_COLUMNS,
            records,
            row_label="nccl IB_HEALTH",
        )

    def load_observations(
        self,
        context: HealthContext,
    ) -> tuple[MetricObservation, ...]:
        if context.combination is None or not context.source_snapshot.results:
            return ()
        validate_source_snapshot(context.source_snapshot)
        result_ids = context.source_snapshot.result_ids
        placeholders = ", ".join("?" for _ in result_ids)
        with health_read_connection(context.result_db_path) as connection:
            validate_common_result_connection(connection)
            _validate_nccl_schema(connection, False)
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
                    raise RuntimeError("NCCL health source snapshot identity is invalid")
            rows = connection.execute(
                "SELECT tr.result_id, tr.run_id, "
                "COALESCE(tr.completed_timestamp, tr.started_timestamp), "
                "ib.Node, ib.timestamp, ib.image_name, ib.cuda, ib.pytorch, "
                "ib.iterations, ib.data_size_gb, ib.samples, ib.BUS_BW, ib.LATENCY, "
                "ib.la_timestamp, "
                f"{', '.join(f'ib.{column}' for column in NCCL_IB_PORT_COLUMNS)} "
                "FROM test_results tr "
                "JOIN metric_ingestion_receipts mr ON mr.run_id=tr.run_id "
                "AND mr.test_id='nccl' "
                f"JOIN {NCCL_HEALTH_TABLE} ib ON ib.run_id=tr.run_id "
                "WHERE tr.status='pass' AND tr.combination_key=? "
                f"AND tr.result_id IN ({placeholders}) ORDER BY tr.result_id",
                (context.combination.key, *result_ids),
            ).fetchall()
            orphan = connection.execute(
                f"""
                SELECT 1 FROM {NCCL_HEALTH_TABLE} ib
                LEFT JOIN test_results tr ON tr.run_id=ib.run_id
                WHERE ib.run_id IS NULL OR tr.run_id IS NULL LIMIT 1
                """
            ).fetchone()
            if orphan is not None:
                raise RuntimeError("NCCL health database contains orphan metric rows")
        if len(rows) != len(result_ids):
            raise RuntimeError("NCCL health source rows are incomplete or duplicated")
        observations = []
        for row in rows:
            result_id = sqlite_integer(row[0], "NCCL result_id", positive=True)
            identity = identities[result_id]
            run_id = sqlite_text(row[1], "NCCL run_id")
            completed_timestamp = sqlite_integer(
                row[2], "NCCL completed_timestamp", non_negative=True
            )
            metric_timestamp = sqlite_integer(
                row[4], "NCCL metric timestamp", non_negative=True
            )
            owner = (
                sqlite_text(row[3], "NCCL metric node"),
                sqlite_text(row[5], "NCCL metric image", nonempty=False),
                sqlite_text(row[6], "NCCL metric CUDA", nonempty=False),
                sqlite_text(row[7], "NCCL metric PyTorch", nonempty=False),
            )
            if (
                (run_id, completed_timestamp)
                != (identity.run_id, identity.completed_timestamp)
                or metric_timestamp != identity.run_timestamp
                or owner
                != (
                    identity.node,
                    identity.image_name,
                    identity.cuda_version,
                    identity.pytorch_version,
                )
            ):
                raise RuntimeError("NCCL health metric ownership is invalid")
            iterations = sqlite_integer(row[8], "NCCL iterations", positive=True)
            if iterations != context.definition.settings.get("iterations"):
                raise RuntimeError("NCCL metric iterations do not match the descriptor")
            data_size_gb = sqlite_integer(
                row[9], "NCCL data_size_gb", positive=True
            )
            if data_size_gb != context.definition.settings.get("data_size_gb"):
                raise RuntimeError("NCCL metric data size does not match the descriptor")
            samples = (
                sqlite_integer(row[10], "NCCL samples", positive=True)
                if row[10] is not None
                else None
            )
            bus_bw = sqlite_number(row[11], "NCCL BUS_BW")
            latency = sqlite_number(row[12], "NCCL LATENCY")
            la_timestamp = sqlite_text(row[13], "NCCL la_timestamp")
            if la_timestamp != timestamp_to_los_angeles(metric_timestamp):
                raise RuntimeError("NCCL LA timestamp does not match metric timestamp")
            normalized_ports = {
                column: (
                    sqlite_number(value, f"NCCL {column}")
                    if value is not None
                    else None
                )
                for column, value in zip(NCCL_IB_PORT_COLUMNS, row[14:])
            }
            if context.definition.settings.get("ibbw_enabled") and (
                samples is None or not any(value is not None for value in normalized_ports.values())
            ):
                raise RuntimeError("NCCL HCA evidence does not satisfy the descriptor")
            for source, value in (("busbw", bus_bw), ("latency", latency)):
                if value is None:
                    continue
                observations.append(
                    MetricObservation(
                        result_id=result_id,
                        run_id=run_id,
                        completed_timestamp=completed_timestamp,
                        source=source,
                        metric_name=source,
                        sample_key=source,
                        value=sqlite_number(value, f"NCCL {source}"),
                    )
                )
            receipt = metadata.receipts[run_id]
            metric_names = (
                "BUS_BW",
                "LATENCY",
                *(
                    column
                    for column in NCCL_IB_PORT_COLUMNS
                    if normalized_ports[column] is not None
                ),
            )
            persisted_evidence = {
                "run_id": run_id,
                "node": owner[0],
                "timestamp": sqlite_integer(
                    row[4], "NCCL metric timestamp", non_negative=True
                ),
                "la_timestamp": la_timestamp,
                "image_name": owner[1],
                "cuda_version": owner[2],
                "pytorch_version": owner[3],
                "iterations": iterations,
                "data_size_gb": data_size_gb,
                "samples": samples,
                "bus_bw": bus_bw,
                "latency": latency,
                "ports": normalized_ports,
            }
            if (
                receipt.inserted_count != 1
                or receipt.updated_count != 0
                or receipt.metric_names != tuple(sorted(metric_names))
                or receipt.evidence_digest
                != canonical_payload_digest(persisted_evidence)
            ):
                raise RuntimeError("NCCL health receipt does not match metric content")
        return tuple(observations)

    def ingest(self, context: IngestionContext) -> IngestionReceipt:
        requested_iterations: Any = context.definition.settings.get("iterations")
        metrics = parse_nccl_health_summary(
            context.execution.summary_path,
            iterations=requested_iterations,
            data_size_gb=int(context.definition.settings["data_size_gb"]),
            require_hca_samples=bool(
                context.definition.settings.get("ibbw_enabled")
            ),
        )
        normalized_ports = {
            column: _optional_float(metrics.port_max_gbps.get(column))
            for column in NCCL_IB_PORT_COLUMNS
        }
        evidence_digest = canonical_payload_digest(
            {
                "run_id": context.run.run_id,
                "node": context.run.node,
                "timestamp": context.run.started_timestamp,
                "la_timestamp": timestamp_to_los_angeles(
                    context.run.started_timestamp
                ),
                "image_name": context.run.image_name,
                "cuda_version": context.run.cuda_version,
                "pytorch_version": context.run.pytorch_version,
                "iterations": metrics.iterations,
                "data_size_gb": metrics.data_size_gb,
                "samples": metrics.samples,
                "bus_bw": metrics.bus_bw,
                "latency": metrics.latency,
                "ports": normalized_ports,
            }
        )
        metric_names = (
            "BUS_BW",
            "LATENCY",
            *(
                column
                for column in NCCL_IB_PORT_COLUMNS
                if normalized_ports[column] is not None
            ),
        )
        with metric_ingestion_transaction(
            context.result_db_path,
            test_id=self.plugin_id,
            adapter_schema_version=ADAPTER_SCHEMA_VERSION,
            validate_adapter_schema=_validate_nccl_schema,
        ) as connection:
            prepare_nccl_health_schema(connection, include_run_id=True)
            prepare_immutable_table_triggers(
                connection,
                NCCL_HEALTH_TABLE,
                NCCL_IMMUTABLE_KEY_GROUPS,
            )
            record_adapter_schema_version(
                connection,
                test_id=self.plugin_id,
                version=ADAPTER_SCHEMA_VERSION,
            )
            _validate_nccl_schema(connection, False)
            existing = existing_metric_ingestion_receipt(
                connection,
                test_id=self.plugin_id,
                run_id=context.run.run_id,
                evidence_digest=evidence_digest,
                expected_inserted_count=1,
                expected_updated_count=0,
                expected_metric_names=metric_names,
            )
            if existing is not None:
                row = connection.execute(
                    f"SELECT Node, timestamp, la_timestamp, iterations, image_name, "
                    f"data_size_gb, cuda, pytorch, samples, BUS_BW, LATENCY, "
                    f"{', '.join(NCCL_IB_PORT_COLUMNS)} "
                    f"FROM {NCCL_HEALTH_TABLE} WHERE run_id=?",
                    (context.run.run_id,),
                ).fetchone()
                expected_row = (
                    context.run.node,
                    context.run.started_timestamp,
                    timestamp_to_los_angeles(context.run.started_timestamp),
                    metrics.iterations,
                    context.run.image_name,
                    metrics.data_size_gb,
                    context.run.cuda_version,
                    context.run.pytorch_version,
                    metrics.samples,
                    metrics.bus_bw,
                    metrics.latency,
                    *(normalized_ports[column] for column in NCCL_IB_PORT_COLUMNS),
                )
                if row is None or tuple(row) != expected_row:
                    raise IngestionConflictError(
                        f"NCCL receipt for {context.run.run_id!r} does not match "
                        "the persisted metric row"
                    )
                return existing

            collision = connection.execute(
                f"SELECT run_id FROM {NCCL_HEALTH_TABLE} WHERE Node=? AND timestamp=?",
                (context.run.node, context.run.started_timestamp),
            ).fetchone()
            if collision is not None:
                raise IngestionConflictError(
                    "NCCL node/timestamp already exists without an exact run receipt"
                )
            columns = (
                "Node",
                "timestamp",
                "la_timestamp",
                "iterations",
                "data_size_gb",
                "image_name",
                "cuda",
                "pytorch",
                "samples",
                "BUS_BW",
                "LATENCY",
                *NCCL_IB_PORT_COLUMNS,
                "run_id",
            )
            values = (
                context.run.node,
                context.run.started_timestamp,
                timestamp_to_los_angeles(context.run.started_timestamp),
                metrics.iterations,
                metrics.data_size_gb,
                context.run.image_name,
                context.run.cuda_version,
                context.run.pytorch_version,
                metrics.samples,
                metrics.bus_bw,
                metrics.latency,
                *(normalized_ports[column] for column in NCCL_IB_PORT_COLUMNS),
                context.run.run_id,
            )
            placeholders = ", ".join("?" for _ in columns)
            connection.execute(
                f"INSERT INTO {NCCL_HEALTH_TABLE} ({', '.join(columns)}) "
                f"VALUES ({placeholders})",
                values,
            )
            receipt = IngestionReceipt(
                test_id=self.plugin_id,
                run_id=context.run.run_id,
                inserted_count=1,
                updated_count=0,
                metric_names=metric_names,
                evidence_digest=evidence_digest,
                created_at=0,
            )
            receipt = record_metric_ingestion_receipt(
                connection,
                receipt,
                evidence_digest=evidence_digest,
            )
            return receipt


PLUGIN = NcclIngestionPlugin()
