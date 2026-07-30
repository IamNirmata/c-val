"""DL unit-test metric ingestion adapter for ``cval.plugin.v1``."""

from __future__ import annotations

from dataclasses import asdict, astuple
import json
import math
import sqlite3
from contextlib import closing
from functools import lru_cache

from cval.health.engine import (
    metric_specs_from_definition,
    validate_health_verdict,
    validate_source_snapshot,
)
from cval.health.models import (
    HealthCandidate,
    HealthClassCode,
    HealthContext,
    HealthVerdict,
    MetricObservation,
    MetricSpec,
)
from cval.health.sqlite_values import sqlite_integer, sqlite_number, sqlite_text
from cval.storage.dltest_ingest import (
    DlRunMetricBundle,
    insert_canonical_dl_metric_bundle,
    load_canonical_dl_run_metrics,
    prepare_canonical_dl_metric_tables,
)
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
DL_IMMUTABLE_KEY_GROUPS = (
    ("run_key", "rank", "task_group", "task_name", "metric_name"),
)

STANDARD_COLUMNS = {
    "run_key",
    "node",
    "cval_timestamp",
    "iterations",
    "sample_dir",
    "test_plan",
    "dltest_run_id",
    "rank",
    "task_group",
    "task_name",
    "status",
    "metric_name",
    "metric_value",
    "source_file",
}


@lru_cache(maxsize=1)
def _dl_table_sql() -> dict[str, str]:
    with closing(sqlite3.connect(":memory:")) as connection:
        prepare_canonical_dl_metric_tables(connection)
        for table_name in (
            "numerical_correctness",
            "compute_performance",
            "collective_performance",
            "overlap_performance",
        ):
            prepare_immutable_table_triggers(
                connection,
                table_name,
                DL_IMMUTABLE_KEY_GROUPS,
            )
        return {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type='table' "
                "AND name IN ('numerical_correctness','compute_performance',"
                "'collective_performance','overlap_performance')"
            )
        }


def _validate_dl_schema(connection, allow_missing: bool) -> bool:
    tables = (
        "numerical_correctness",
        "compute_performance",
        "collective_performance",
        "overlap_performance",
    )
    require_database_tables(connection, set(COMMON_RESULT_TABLES) | set(tables))
    existing_tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    require_database_views(
        connection,
        set(),
        immutable_tables={
            **COMMON_IMMUTABLE_KEY_GROUPS,
            **(
                {
                    table_name: DL_IMMUTABLE_KEY_GROUPS
                    for table_name in tables
                }
                if set(tables).issubset(existing_tables)
                else {}
            ),
        },
    )
    present = []
    for table_name in tables:
        columns = set(STANDARD_COLUMNS)
        specs = {
            "run_key": ("TEXT", True, None, 1),
            "node": ("TEXT", False, None, 0),
            "cval_timestamp": ("INTEGER", False, None, 0),
            "iterations": ("INTEGER", True, "20", 0),
            "sample_dir": ("TEXT", True, None, 0),
            "test_plan": ("TEXT", True, None, 0),
            "dltest_run_id": ("TEXT", True, None, 0),
            "rank": ("INTEGER", True, None, 2),
            "task_group": ("TEXT", True, None, 3),
            "task_name": ("TEXT", True, None, 4),
            "status": ("TEXT", False, None, 0),
            "metric_name": ("TEXT", True, None, 5),
            "metric_value": ("REAL", False, None, 0),
            "source_file": ("TEXT", True, None, 0),
        }
        if table_name == "overlap_performance":
            columns.update({"coll_name", "layer_name"})
            specs.update(
                {
                    "coll_name": ("TEXT", True, None, 0),
                    "layer_name": ("TEXT", True, None, 0),
                }
            )
        present.append(
            validate_table_manifest(
                connection,
                table_name,
                required_columns=columns,
                column_specs=specs,
                primary_key=(
                    "run_key",
                    "rank",
                    "task_group",
                    "task_name",
                    "metric_name",
                ),
                allowed_indexes=(
                    {
                        f"idx_{table_name}_node_ts",
                        f"idx_{table_name}_metric",
                    }
                    if table_name != "overlap_performance"
                    else {
                        "idx_overlap_performance_node_ts",
                        "idx_overlap_performance_pair",
                    }
                ),
                implicit_indexes={
                    (
                        "pk",
                        (
                            "run_key",
                            "rank",
                            "task_group",
                            "task_name",
                            "metric_name",
                        ),
                        ("BINARY", "BINARY", "BINARY", "BINARY", "BINARY"),
                    )
                },
                immutable_key_groups=DL_IMMUTABLE_KEY_GROUPS,
                constraint_counts=(0, 0),
                allow_missing=allow_missing,
            )
        )
        if present[-1]:
            require_exact_table_sql(
                connection,
                table_name,
                _dl_table_sql()[table_name],
            )
    if any(present) and not all(present):
        raise RuntimeError("DL adapter schema is only partially present")
    if all(present):
        require_schema_objects(
            connection,
            indexes={
                "idx_numerical_correctness_node_ts": (
                    "numerical_correctness", ("node", "cval_timestamp"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
                "idx_numerical_correctness_metric": (
                    "numerical_correctness", ("task_name", "metric_name"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
                "idx_compute_performance_node_ts": (
                    "compute_performance", ("node", "cval_timestamp"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
                "idx_compute_performance_metric": (
                    "compute_performance", ("task_name", "metric_name"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
                "idx_collective_performance_node_ts": (
                    "collective_performance", ("node", "cval_timestamp"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
                "idx_collective_performance_metric": (
                    "collective_performance", ("task_name", "metric_name"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
                "idx_overlap_performance_node_ts": (
                    "overlap_performance", ("node", "cval_timestamp"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
                "idx_overlap_performance_pair": (
                    "overlap_performance", ("coll_name", "layer_name"),
                    (False, False), ("BINARY", "BINARY"), False, ""
                ),
            },
        )
    return all(present)


def _bundle_payload(bundle: DlRunMetricBundle) -> dict[str, object]:
    def rows_payload(rows):
        values = [asdict(row) for row in rows]
        return sorted(
            values,
            key=lambda value: json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
        )

    return {
        "run_id": bundle.run_id,
        "node": bundle.node,
        "cval_timestamp": bundle.cval_timestamp,
        "numerical_rows": rows_payload(bundle.numerical_rows),
        "compute_rows": rows_payload(bundle.compute_rows),
        "collective_rows": rows_payload(bundle.collective_rows),
        "overlap_rows": rows_payload(bundle.overlap_rows),
    }


def _persisted_bundle_payload(
    connection,
    run_id: str,
    *,
    expected_node: str,
    expected_timestamp: int,
    expected_test_plan: str,
    expected_iterations: int,
    expected_gpu_count: int,
) -> tuple[dict[str, object], int, tuple[str, ...]]:
    component_rows: dict[str, list[dict[str, object]]] = {}
    metric_names: set[str] = set()
    cval_timestamps: set[int] = set()
    total = 0
    for source in (
        "numerical_correctness",
        "compute_performance",
        "collective_performance",
        "overlap_performance",
    ):
        columns = [
            "run_key", "node", "cval_timestamp", "iterations", "sample_dir",
            "test_plan", "dltest_run_id", "rank", "task_group", "task_name",
            "status",
        ]
        if source == "overlap_performance":
            columns.extend(("coll_name", "layer_name"))
        columns.extend(("metric_name", "metric_value", "source_file"))
        rows = connection.execute(
            f"SELECT {', '.join(columns)} FROM {source} WHERE run_key=? "
            "ORDER BY rank, task_group, task_name, metric_name",
            (run_id,),
        ).fetchall()
        values: list[dict[str, object]] = []
        observed_ranks: set[int] = set()
        for row in rows:
            decoded = dict(zip(columns, row))
            decoded["run_key"] = sqlite_text(decoded["run_key"], "DL run_key")
            decoded["node"] = sqlite_text(decoded["node"], "DL node")
            decoded["cval_timestamp"] = sqlite_integer(
                decoded["cval_timestamp"], "DL cval_timestamp", non_negative=True
            )
            decoded["iterations"] = sqlite_integer(
                decoded["iterations"], "DL iterations", positive=True
            )
            for field in (
                "sample_dir", "test_plan", "dltest_run_id", "task_group",
                "task_name", "status", "metric_name", "source_file",
            ):
                decoded[field] = sqlite_text(decoded[field], f"DL {field}")
            if source == "overlap_performance":
                decoded["coll_name"] = sqlite_text(
                    decoded["coll_name"], "DL coll_name"
                )
                decoded["layer_name"] = sqlite_text(
                    decoded["layer_name"], "DL layer_name"
                )
            decoded["rank"] = sqlite_integer(
                decoded["rank"], "DL rank", non_negative=True
            )
            decoded["metric_value"] = sqlite_number(
                decoded["metric_value"], "DL metric_value"
            )
            if (
                decoded["run_key"] != run_id
                or decoded["node"] != expected_node
                or decoded["cval_timestamp"] != expected_timestamp
                or decoded["test_plan"] != expected_test_plan
                or decoded["iterations"] != expected_iterations
                or decoded["status"] != "completed"
            ):
                raise RuntimeError("DL persisted metric identity/settings are invalid")
            cval_timestamps.add(decoded["cval_timestamp"])
            observed_ranks.add(decoded["rank"])
            metric_names.add(f"{source}.{decoded['metric_name']}")
            values.append(decoded)
        component_rows[source] = sorted(
            values,
            key=lambda value: json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
        )
        if not values or observed_ranks != set(range(expected_gpu_count)):
            raise RuntimeError(
                f"DL persisted {source} rank/component coverage is incomplete"
            )
        total += len(values)
    if total == 0 or len(cval_timestamps) != 1:
        raise RuntimeError("DL persisted metric bundle is empty or mixed")
    return (
        {
            "run_id": run_id,
            "node": expected_node,
            "cval_timestamp": next(iter(cval_timestamps)),
            "numerical_rows": component_rows["numerical_correctness"],
            "compute_rows": component_rows["compute_performance"],
            "collective_rows": component_rows["collective_performance"],
            "overlap_rows": component_rows["overlap_performance"],
        },
        total,
        tuple(sorted(metric_names)),
    )


class DltestIngestionPlugin:
    plugin_id = "dltest"
    health_policy_version = "dltest.health.v1"
    capabilities = frozenset({"config", "ingest", "health"})

    def validate_schema(self, connection, allow_missing: bool) -> bool:
        return _validate_dl_schema(connection, allow_missing)

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
        if not isinstance(aggregation, dict):
            issues.append(
                ConfigIssue("invalid_health_aggregation", "health_aggregation must be a table")
            )
        else:
            expected = {
                "degraded_metric_fraction",
                "min_degraded_metrics",
                "degraded_severity_pct",
            }
            extra = sorted(set(aggregation) - expected)
            missing = sorted(expected - set(aggregation))
            if extra or missing:
                issues.append(
                    ConfigIssue(
                        "invalid_health_aggregation_keys",
                        f"missing={missing}, unknown={extra}",
                    )
                )
            fraction = aggregation.get("degraded_metric_fraction")
            if (
                isinstance(fraction, bool)
                or not isinstance(fraction, int | float)
                or not 0.0 <= float(fraction) <= 1.0
            ):
                issues.append(
                    ConfigIssue(
                        "invalid_degraded_metric_fraction",
                        "degraded_metric_fraction must be finite number in [0,1]",
                    )
                )
            minimum = aggregation.get("min_degraded_metrics")
            if (
                isinstance(minimum, bool)
                or not isinstance(minimum, int)
                or minimum <= 0
            ):
                issues.append(
                    ConfigIssue(
                        "invalid_min_degraded_metrics",
                        "min_degraded_metrics must be a positive integer",
                    )
                )
            severity = aggregation.get("degraded_severity_pct")
            if (
                isinstance(severity, bool)
                or not isinstance(severity, int | float)
                or not math.isfinite(float(severity))
                or float(severity) < 0
            ):
                issues.append(
                    ConfigIssue(
                        "invalid_degraded_severity_pct",
                        "degraded_severity_pct must be finite and non-negative",
                    )
                )
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
        observations = []
        with closing(
            sqlite3.connect(
                f"file:{context.result_db_path}?mode=ro",
                uri=True,
                timeout=30,
            )
        ) as connection:
            validate_common_result_connection(connection)
            _validate_dl_schema(connection, False)
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
                    raise RuntimeError("DL health source snapshot identity is invalid")
            for source in (
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            ):
                orphan = connection.execute(
                    f"""
                    SELECT 1 FROM {source} metric
                    LEFT JOIN test_results tr ON tr.run_id=metric.run_key
                    WHERE tr.run_id IS NULL LIMIT 1
                    """
                ).fetchone()
                if orphan is not None:
                    raise RuntimeError("DL health database contains orphan metric rows")
                rows = connection.execute(
                    "SELECT tr.result_id, tr.run_id, "
                    "COALESCE(tr.completed_timestamp, tr.started_timestamp), "
                    "metric.node, metric.cval_timestamp, metric.run_key, "
                    "metric.rank, metric.task_group, metric.task_name, "
                    "metric.metric_name, metric.metric_value "
                    "FROM test_results tr "
                    "JOIN metric_ingestion_receipts mr ON mr.run_id=tr.run_id "
                    "AND mr.test_id='dltest' "
                    f"JOIN {source} metric ON metric.run_key=tr.run_id "
                    "WHERE tr.status='pass' AND tr.combination_key=? "
                    f"AND tr.result_id IN ({placeholders}) "
                    "ORDER BY tr.result_id, metric.rank, metric.task_group, "
                    "metric.task_name, metric.metric_name",
                    (context.combination.key, *result_ids),
                ).fetchall()
                for row in rows:
                    if row[10] is None:
                        continue
                    result_id = sqlite_integer(
                        row[0], "DL result_id", positive=True
                    )
                    identity = identities[result_id]
                    run_id = sqlite_text(row[1], "DL run_id")
                    completed_timestamp = sqlite_integer(
                        row[2], "DL completed_timestamp", non_negative=True
                    )
                    metric_timestamp = sqlite_integer(
                        row[4], "DL metric timestamp", non_negative=True
                    )
                    metric_owner = (
                        sqlite_text(row[3], "DL metric node"),
                        sqlite_text(row[5], "DL metric run_key"),
                    )
                    if (
                        (run_id, completed_timestamp)
                        != (identity.run_id, identity.completed_timestamp)
                        or metric_timestamp != identity.run_timestamp
                        or metric_owner
                        != (identity.node, identity.run_id)
                    ):
                        raise RuntimeError("DL health metric ownership is invalid")
                    rank = sqlite_integer(row[6], "DL rank", non_negative=True)
                    task_group = sqlite_text(row[7], "DL task_group")
                    task_name = sqlite_text(row[8], "DL task_name")
                    metric_name = sqlite_text(row[9], "DL metric_name")
                    expanded_name = (
                        f"{task_group}/{task_name}/rank{rank}/{metric_name}"
                        if source == "numerical_correctness"
                        else f"{task_group}/{task_name}/{metric_name}"
                    )
                    observations.append(
                        MetricObservation(
                            result_id=result_id,
                            run_id=run_id,
                            completed_timestamp=completed_timestamp,
                            source=source,
                            metric_name=expanded_name,
                            sample_key=(
                                f"rank{rank}:{task_group}:{task_name}:{metric_name}"
                            ),
                            value=sqlite_number(row[10], "DL metric_value"),
                        )
                    )
            for identity in identities.values():
                payload, inserted_count, metric_names = _persisted_bundle_payload(
                    connection,
                    identity.run_id,
                    expected_node=identity.node,
                    expected_timestamp=identity.run_timestamp,
                    expected_test_plan=str(context.definition.settings["test_plan"]),
                    expected_iterations=int(context.definition.settings["iterations"]),
                    expected_gpu_count=int(context.definition.settings["gpu_count"]),
                )
                receipt = metadata.receipts[identity.run_id]
                if (
                    receipt.inserted_count != inserted_count
                    or receipt.updated_count != 0
                    or receipt.metric_names != metric_names
                    or receipt.evidence_digest != canonical_payload_digest(payload)
                ):
                    raise RuntimeError("DL health receipt does not match metric content")
        return tuple(observations)

    def classify(
        self,
        context: HealthContext,
        baseline: HealthCandidate | None,
        observations: tuple[MetricObservation, ...],
        base: HealthVerdict,
    ) -> HealthVerdict:
        if base.class_code is HealthClassCode.DNR:
            return base
        aggregation = context.definition.settings["health_aggregation"]
        severity = float(aggregation["degraded_severity_pct"])
        minimum = int(aggregation["min_degraded_metrics"])
        fraction_threshold = float(aggregation["degraded_metric_fraction"])
        qualifying = [
            metric
            for metric in base.metrics
            if metric.class_code
            in {
                HealthClassCode.UNDERPERFORMING,
                HealthClassCode.VERY_BAD,
                HealthClassCode.TERRIBLE,
            }
            and metric.severity_pct >= severity
        ]
        fraction = len(qualifying) / len(base.metrics) if base.metrics else 0.0
        if qualifying and (len(qualifying) >= minimum or fraction >= fraction_threshold):
            code = max(metric.class_code for metric in qualifying)
        elif not qualifying and not any(
            metric.class_code
            in {
                HealthClassCode.UNDERPERFORMING,
                HealthClassCode.VERY_BAD,
                HealthClassCode.TERRIBLE,
            }
            for metric in base.metrics
        ) and any(
            metric.class_code is HealthClassCode.EXCELLENT for metric in base.metrics
        ):
            code = HealthClassCode.EXCELLENT
        else:
            code = HealthClassCode.NOMINAL
        details = {
            "aggregation": "dl_severity_count_fraction.v1",
            "degraded_severity_pct": severity,
            "min_degraded_metrics": minimum,
            "degraded_metric_fraction_threshold": fraction_threshold,
            "n_compared": len(base.metrics),
            "n_qualifying_degraded": len(qualifying),
            "qualifying_degraded_fraction": fraction,
        }
        verdict = HealthVerdict(
            test_id=base.test_id,
            combination_key=base.combination_key,
            baseline_id=base.baseline_id,
            class_code=code,
            class_name=code.class_name,
            dnr_reason=None,
            metrics=base.metrics,
            details_json=json.dumps(
                details,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
        )
        validate_health_verdict(
            verdict,
            test_id=self.plugin_id,
            combination=context.combination,
            baseline=baseline,
        )
        return verdict

    def ingest(self, context: IngestionContext) -> IngestionReceipt:
        settings = context.definition.settings
        validate_ingestion_artifact_tree(context.execution.artifacts_path)
        bundle = load_canonical_dl_run_metrics(
            context.execution.result_path.parent,
            summary_path=context.execution.summary_path,
            expected_run_id=context.run.run_id,
            expected_node=context.run.node,
            expected_timestamp=context.run.started_timestamp,
            expected_test_plan=str(settings["test_plan"]),
            expected_iterations=int(settings["iterations"]),
            expected_gpu_count=int(settings["gpu_count"]),
        )
        evidence_digest = canonical_payload_digest(_bundle_payload(bundle))
        component_rows = {
            "numerical_correctness": bundle.numerical_rows,
            "compute_performance": bundle.compute_rows,
            "collective_performance": bundle.collective_rows,
            "overlap_performance": bundle.overlap_rows,
        }
        inserted_count = sum(len(rows) for rows in component_rows.values())
        metric_names = tuple(
            sorted(
                {
                    f"{component}.{row.metric_name}"
                    for component, rows in component_rows.items()
                    for row in rows
                }
            )
        )
        with metric_ingestion_transaction(
            context.result_db_path,
            test_id=self.plugin_id,
            adapter_schema_version=ADAPTER_SCHEMA_VERSION,
            validate_adapter_schema=_validate_dl_schema,
        ) as connection:
            prepare_canonical_dl_metric_tables(connection)
            for table_name in (
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            ):
                prepare_immutable_table_triggers(
                    connection,
                    table_name,
                    DL_IMMUTABLE_KEY_GROUPS,
                )
            record_adapter_schema_version(
                connection,
                test_id=self.plugin_id,
                version=ADAPTER_SCHEMA_VERSION,
            )
            _validate_dl_schema(connection, False)
            existing = existing_metric_ingestion_receipt(
                connection,
                test_id=self.plugin_id,
                run_id=context.run.run_id,
                evidence_digest=evidence_digest,
                expected_inserted_count=inserted_count,
                expected_updated_count=0,
                expected_metric_names=metric_names,
            )
            if existing is not None:
                for table_name, rows in component_rows.items():
                    if table_name == "overlap_performance":
                        columns = (
                            "run_key, node, cval_timestamp, iterations, sample_dir, "
                            "test_plan, dltest_run_id, rank, task_group, task_name, "
                            "status, coll_name, layer_name, metric_name, metric_value, "
                            "source_file"
                        )
                    else:
                        columns = (
                            "run_key, node, cval_timestamp, iterations, sample_dir, "
                            "test_plan, dltest_run_id, rank, task_group, task_name, "
                            "status, metric_name, metric_value, source_file"
                        )
                    persisted = connection.execute(
                        f"SELECT {columns} FROM {table_name} WHERE run_key=? "
                        "ORDER BY rank, task_group, task_name, metric_name",
                        (context.run.run_id,),
                    ).fetchall()
                    expected_rows = sorted(
                        (astuple(row) for row in rows),
                        key=lambda row: (
                            row[7],
                            row[8],
                            row[9],
                            row[13] if table_name == "overlap_performance" else row[11],
                        ),
                    )
                    if [tuple(row) for row in persisted] != expected_rows:
                        raise IngestionConflictError(
                            f"DL persisted rows differ for {table_name} run "
                            f"{context.run.run_id!r}"
                        )
                return existing

            actual_inserted_count = insert_canonical_dl_metric_bundle(connection, bundle)
            if actual_inserted_count != inserted_count:
                raise RuntimeError("DL adapter inserted an unexpected metric row count")
            receipt = IngestionReceipt(
                test_id=self.plugin_id,
                run_id=context.run.run_id,
                inserted_count=inserted_count,
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


PLUGIN = DltestIngestionPlugin()
