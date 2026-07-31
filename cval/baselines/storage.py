"""Store and retrieve baselines in SQLite databases."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from cval.config import load_config
from cval.models import dl_component_for_test_type, normalize_baseline_test_type
from cval.storage.ingest import _connect_writable, _ensure_column
from cval.storage.sqlite_uri import connect_sqlite_file
from cval.validation.operational_targets import (
    BASELINE_LIST,
    build_operational_target_catalog,
    validate_operational_target_name,
)


BASELINE_DB_FILENAMES = {
    "storage": "test-storage-baselines.db",
    "nccl": "test-nccl-baselines.db",
}

DL_BASELINE_DB_FILENAMES = {
    "numerical_correctness": "dltest_numerical_correctness-baselines.db",
    "compute_performance": "dltest_compute_performance-baselines.db",
    "collective_performance": "dltest_collective_performance-baselines.db",
    "overlap_performance": "dltest_overlap_performance-baselines.db",
}

CLASSIFICATION_DB_FILENAME = "classification-results.db"


def baseline_root_path(config=None) -> Path:
    """Return the operator-owned baseline root directory."""

    active_config = config or load_config()
    return Path(active_config.baseline.baseline_root_path).expanduser()


def default_dynamic_baseline_db_path(
    test_type: str,
    source_table: str | None = None,
    config=None,
) -> Path:
    """Return the default baseline DB for a test/component."""

    root = baseline_root_path(config)
    baseline_test_type = normalize_baseline_test_type(test_type)
    component = source_table or dl_component_for_test_type(test_type)
    if baseline_test_type == "dltest":
        if not component:
            return root / "dltest-baselines.db"
        filename = DL_BASELINE_DB_FILENAMES.get(component)
        if filename is None:
            raise ValueError(f"unknown DL baseline component: {component!r}")
        return root / filename
    filename = BASELINE_DB_FILENAMES.get(baseline_test_type)
    if filename is None:
        safe_target = validate_operational_target_name(baseline_test_type)
        filename = f"plugin-{safe_target}-baselines.db"
    return root / filename


def default_dynamic_baseline_db_paths(test_type: str, config=None) -> list[Path]:
    """Return all default baseline DB paths for a test type."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    component = dl_component_for_test_type(test_type)
    if baseline_test_type == "dltest":
        if component:
            return [default_dynamic_baseline_db_path(test_type, config=config)]
        return [
            default_dynamic_baseline_db_path(baseline_test_type, table, config=config)
            for table in DL_BASELINE_DB_FILENAMES
        ]
    return [default_dynamic_baseline_db_path(baseline_test_type, config=config)]


def default_classification_db_path(config=None) -> Path:
    """Return the default classification-result DB path."""

    return baseline_root_path(config) / CLASSIFICATION_DB_FILENAME


def validate_default_baseline_db_paths(config=None) -> None:
    """Reject default path collisions across enabled canonical baseline targets."""

    active_config = config or load_config()
    catalog = build_operational_target_catalog(active_config.tests.registry)
    owners: dict[Path, str] = {}
    classification_path = default_classification_db_path(active_config).absolute()
    for target in catalog.for_operation(BASELINE_LIST):
        for path in default_dynamic_baseline_db_paths(
            target.name,
            config=active_config,
        ):
            lexical = path.expanduser().absolute()
            if lexical == classification_path:
                raise ValueError(
                    f"Baseline target {target.name!r} collides with classification DB"
                )
            previous = owners.setdefault(lexical, target.name)
            if previous != target.name:
                raise ValueError(
                    f"Baseline targets {previous!r} and {target.name!r} collide at {lexical}"
                )


# --- Dynamic (versioned, statistically built) baselines ---------------------
#
# Dynamic baselines reuse the ``baselines`` table but add provenance columns so
# every record is immutable and auditable. ``status`` moves candidate -> active
# -> superseded; new results are classified against the single ``active`` row
# for their ``(test_type, stratum_key)``.

_DYNAMIC_COLUMNS = (
    ("status", "TEXT NOT NULL DEFAULT 'candidate'"),
    ("stratum_key", "TEXT NOT NULL DEFAULT ''"),
    ("n_samples", "INTEGER NOT NULL DEFAULT 0"),
    ("window_days", "INTEGER NOT NULL DEFAULT 0"),
    ("method", "TEXT NOT NULL DEFAULT ''"),
    ("schema_version", "TEXT NOT NULL DEFAULT ''"),
    ("created_at", "INTEGER NOT NULL DEFAULT 0"),
    ("supersedes", "TEXT NOT NULL DEFAULT ''"),
)


def _ensure_baselines_schema(connection: sqlite3.Connection) -> None:
    """Create the baselines table and add dynamic columns to older DBs."""

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS baselines (
          baseline_id TEXT NOT NULL,
          test_type TEXT NOT NULL,
          metrics_json TEXT,
          timestamp INTEGER,
          PRIMARY KEY (baseline_id, test_type)
        )
        """
    )
    for column_name, column_definition in _DYNAMIC_COLUMNS:
        _ensure_column(connection, "baselines", column_name, column_definition)
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_baselines_type_status "
        "ON baselines(test_type, status, stratum_key)"
    )


def _store_dynamic_baseline_in_db(record: dict, db_path: str | Path, status: str) -> str:
    """Store one dynamic baseline record in one SQLite DB."""

    from cval.validation.operations import validate_compatibility_baseline_record

    baseline_id = record["baseline_id"]
    test_type = record["test_type"]
    if status != "candidate":
        raise ValueError("New compatibility baselines must be stored as candidates")
    validation_record = dict(record)
    component = validation_record.pop("component", None)
    validate_compatibility_baseline_record(
        validation_record,
        expected_test_type=test_type,
    )
    if component is not None:
        if test_type != "dltest" or component not in DL_BASELINE_DB_FILENAMES:
            raise ValueError("Compatibility baseline component projection is invalid")
        if any(
            metric.get("source_table") != component
            for metric in record["metrics"].values()
        ):
            raise ValueError("Compatibility baseline component contains foreign metrics")
    record_json = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    with closing(_connect_writable(db_path)) as connection:
        values = (
            baseline_id,
            test_type,
            record_json,
            int(record.get("timestamp", record.get("created_at", 0))),
            status,
            record.get("stratum_key", ""),
            int(record.get("n_samples", 0)),
            int(record.get("window_days", 0)),
            record.get("method", ""),
            record.get("schema_version", ""),
            int(record.get("created_at", 0)),
            record.get("supersedes", ""),
        )
        try:
            # Serialize first-use schema creation/migration as well as write
            # admission. Otherwise concurrent writers can both observe a
            # missing additive column and race the same ALTER TABLE.
            connection.execute("BEGIN IMMEDIATE")
            _ensure_baselines_schema(connection)
            # Attempt the immutable insert, then validate the row that won the
            # primary-key conflict. DO NOTHING is essential: an exact retry
            # must never reset active/superseded lifecycle state.
            connection.execute(
                """
                INSERT INTO baselines (
                  baseline_id, test_type, metrics_json, timestamp,
                  status, stratum_key, n_samples, window_days, method,
                  schema_version, created_at, supersedes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(baseline_id, test_type) DO NOTHING
                """,
                values,
            )
            existing = connection.execute(
                "SELECT metrics_json, timestamp, stratum_key, n_samples, window_days, "
                "method, schema_version, created_at, supersedes FROM baselines "
                "WHERE baseline_id=? AND test_type=?",
                (baseline_id, test_type),
            ).fetchone()
            if existing is None:
                raise RuntimeError("Compatibility baseline insert produced no durable row")
            try:
                existing_record = json.loads(existing[0])
                existing_json = json.dumps(
                    existing_record,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "Stored compatibility baseline content is invalid"
                ) from exc
            if existing_json != record_json:
                raise ValueError(
                    f"Compatibility baseline ID {baseline_id!r} already has different content"
                )
            expected_columns = (
                int(record.get("timestamp", record.get("created_at", 0))),
                record.get("stratum_key", ""),
                int(record.get("n_samples", 0)),
                int(record.get("window_days", 0)),
                record.get("method", ""),
                record.get("schema_version", ""),
                int(record.get("created_at", 0)),
                record.get("supersedes", ""),
            )
            if tuple(existing[1:]) != expected_columns:
                raise ValueError(
                    f"Compatibility baseline ID {baseline_id!r} has inconsistent stored identity"
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    return baseline_id


def _split_dl_record_for_config(record: dict, config=None) -> list[tuple[Path, dict]]:
    """Split a DL record using paths from the supplied config."""

    targets: list[tuple[Path, dict]] = []
    metrics = record.get("metrics", {})
    for source_table in DL_BASELINE_DB_FILENAMES:
        component_metrics = {
            metric_name: metric_stat
            for metric_name, metric_stat in metrics.items()
            if metric_stat.get("source_table") == source_table
        }
        component_record = dict(record)
        component_record["metrics"] = component_metrics
        component_record["component"] = source_table
        targets.append(
            (default_dynamic_baseline_db_path("dltest", source_table, config=config), component_record)
        )
    return targets


def _merge_records(records: list[dict]) -> dict | None:
    """Merge component baseline records into one logical record."""

    if not records:
        return None
    merged = dict(records[0])
    merged_metrics = {}
    baseline_ids = []
    components = []
    for record in records:
        merged_metrics.update(record.get("metrics", {}))
        baseline_id = record.get("baseline_id")
        if baseline_id and baseline_id not in baseline_ids:
            baseline_ids.append(baseline_id)
        component = record.get("component")
        if component and component not in components:
            components.append(component)
    merged["metrics"] = merged_metrics
    merged["components"] = components
    if len(baseline_ids) > 1:
        merged["baseline_id"] = ",".join(baseline_ids)
    return merged


def store_dynamic_baseline(
    record: dict,
    db_path: str | Path | None = None,
    status: str = "candidate",
    config=None,
) -> str:
    """Persist a built baseline record and return its baseline_id.

    The full record (including per-metric robust stats) is stored as JSON; key
    provenance fields are also written to first-class columns for querying.
    New baselines default to ``candidate`` so a degraded fleet cannot silently
    re-baseline itself; call :func:`activate_baseline` to promote.
    """

    from cval.validation.operations import validate_compatibility_baseline_record

    validate_compatibility_baseline_record(
        record,
        expected_test_type=record.get("test_type") if isinstance(record, dict) else "",
    )

    if db_path is not None:
        return _store_dynamic_baseline_in_db(record, db_path, status)

    if record["test_type"] == "dltest":
        for target_path, component_record in _split_dl_record_for_config(record, config=config):
            _store_dynamic_baseline_in_db(component_record, target_path, status)
        return record["baseline_id"]

    target_path = default_dynamic_baseline_db_path(record["test_type"], config=config)
    return _store_dynamic_baseline_in_db(record, target_path, status)


def _activate_baseline_in_db(
    baseline_id: str,
    test_type: str,
    db_path: str | Path,
) -> bool:
    """Promote one baseline in one DB."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    with closing(_connect_writable(db_path)) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")
            _ensure_baselines_schema(connection)
            row = connection.execute(
                "SELECT stratum_key FROM baselines WHERE baseline_id = ? AND test_type = ?",
                (baseline_id, baseline_test_type),
            ).fetchone()
            if row is None:
                connection.commit()
                return False
            stratum_key = row[0]
            connection.execute(
                """
                UPDATE baselines SET status = 'superseded'
                WHERE test_type = ? AND stratum_key = ? AND status = 'active'
                  AND baseline_id != ?
                """,
                (baseline_test_type, stratum_key, baseline_id),
            )
            connection.execute(
                "UPDATE baselines SET status = 'active' WHERE baseline_id = ? AND test_type = ?",
                (baseline_id, baseline_test_type),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    return True


def activate_baseline(
    baseline_id: str,
    test_type: str,
    db_path: str | Path | None = None,
    config=None,
) -> bool:
    """Promote one baseline to ``active`` and supersede the previous active one.

    Only the prior active baseline for the *same* ``(test_type, stratum_key)``
    is superseded, so different strata keep independent active baselines.
    Returns False when the baseline_id does not exist.
    """

    if db_path is not None:
        return _activate_baseline_in_db(baseline_id, test_type, db_path)

    activated_any = False
    for path in default_dynamic_baseline_db_paths(test_type, config=config):
        activated_any = _activate_baseline_in_db(baseline_id, test_type, path) or activated_any
    return activated_any


def _load_dynamic_baseline_from_db(
    baseline_id: str,
    test_type: str,
    db_path: str | Path,
) -> dict | None:
    """Load a full baseline record from one DB."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    path = Path(db_path)
    if not path.exists():
        return None
    with closing(connect_sqlite_file(path, mode="ro")) as connection:
        row = connection.execute(
            "SELECT metrics_json FROM baselines WHERE baseline_id = ? AND test_type = ?",
            (baseline_id, baseline_test_type),
        ).fetchone()
        if not row or not row[0]:
            return None
        record = json.loads(row[0])
        _validate_loaded_baseline_record(record, baseline_id, baseline_test_type)
        return record


def load_dynamic_baseline(
    baseline_id: str,
    test_type: str,
    db_path: str | Path | None = None,
    config=None,
) -> dict | None:
    """Load a full baseline record (with per-metric stats) by id."""

    if db_path is not None:
        return _load_dynamic_baseline_from_db(baseline_id, test_type, db_path)

    records = [
        record
        for path in default_dynamic_baseline_db_paths(test_type, config=config)
        if (record := _load_dynamic_baseline_from_db(baseline_id, test_type, path))
    ]
    return _merge_records(records)


def _get_active_baseline_from_db(
    test_type: str,
    stratum_key: str | None,
    db_path: str | Path,
) -> dict | None:
    """Return the active baseline record from one DB."""

    baseline_test_type = normalize_baseline_test_type(test_type)
    path = Path(db_path)
    if not path.exists():
        return None
    with closing(connect_sqlite_file(path, mode="ro")) as connection:
        query = (
            "SELECT baseline_id, metrics_json FROM baselines "
            "WHERE test_type = ? AND status = 'active'"
        )
        params: list = [baseline_test_type]
        if stratum_key is not None:
            query += " AND stratum_key = ?"
            params.append(stratum_key)
        query += " ORDER BY created_at DESC LIMIT 1"
        row = connection.execute(query, params).fetchone()
        if not row or not row[1]:
            return None
        record = json.loads(row[1])
        _validate_loaded_baseline_record(record, row[0], baseline_test_type)
        return record


def get_active_baseline(
    test_type: str,
    stratum_key: str | None = None,
    db_path: str | Path | None = None,
    config=None,
) -> dict | None:
    """Return the active baseline record for a test type / stratum, if any."""

    if db_path is not None:
        return _get_active_baseline_from_db(test_type, stratum_key, db_path)

    records = [
        record
        for path in default_dynamic_baseline_db_paths(test_type, config=config)
        if (record := _get_active_baseline_from_db(test_type, stratum_key, path))
    ]
    return _merge_records(records)


def _list_dynamic_baselines_from_db(
    test_type: str | None,
    db_path: str | Path,
) -> list[tuple]:
    """List dynamic baselines from one DB."""

    path = Path(db_path)
    if not path.exists():
        return []
    with closing(connect_sqlite_file(path, mode="ro")) as connection:
        base_query = (
            "SELECT baseline_id, test_type, status, stratum_key, n_samples, created_at "
            "FROM baselines"
        )
        if test_type:
            return connection.execute(
                base_query + " WHERE test_type = ? ORDER BY created_at DESC",
                (normalize_baseline_test_type(test_type),),
            ).fetchall()
        return connection.execute(
            base_query + " ORDER BY test_type, created_at DESC"
        ).fetchall()


def _validate_loaded_baseline_record(
    record: object,
    baseline_id: str,
    test_type: str,
) -> None:
    from cval.validation.operations import validate_compatibility_baseline_record

    if not isinstance(record, dict):
        raise ValueError("Stored compatibility baseline must be a JSON object")
    validation_record = dict(record)
    component = validation_record.pop("component", None)
    validate_compatibility_baseline_record(
        validation_record,
        expected_test_type=test_type,
    )
    if validation_record["baseline_id"] != baseline_id:
        raise ValueError("Stored compatibility baseline identity does not match its row")
    if component is not None and (
        test_type != "dltest" or component not in DL_BASELINE_DB_FILENAMES
    ):
        raise ValueError("Stored compatibility baseline component is invalid")


def list_dynamic_baselines(
    test_type: str | None = None,
    db_path: str | Path | None = None,
    config=None,
) -> list[tuple]:
    """List dynamic baselines as (id, test_type, status, stratum, n, created_at)."""

    active_config = config or load_config()
    catalog = build_operational_target_catalog(active_config.tests.registry)
    enabled_test_types = {
        target.baseline_test_type for target in catalog.for_operation(BASELINE_LIST)
    }
    if db_path is not None:
        rows = _list_dynamic_baselines_from_db(test_type, db_path)
        return [row for row in rows if row[1] in enabled_test_types]

    if test_type:
        paths = default_dynamic_baseline_db_paths(test_type, config=active_config)
    else:
        paths = []
        for target in catalog.for_operation(BASELINE_LIST):
            for path in default_dynamic_baseline_db_paths(
                target.name, config=active_config
            ):
                if path not in paths:
                    paths.append(path)
    rows: list[tuple] = []
    for path in paths:
        rows.extend(_list_dynamic_baselines_from_db(test_type, path))
    return rows


def store_classification_results(
    verdicts: list[dict],
    db_path: str | Path | None = None,
    classified_at: int | None = None,
    config=None,
) -> int:
    """Persist node classification verdicts and return rows written.

    ``passed`` is true for normal/improved and false for degraded. Raw
    validation pass/fail rows stay untouched in validation.db.
    """

    from cval.validation.operations import validate_compatibility_classification_verdicts

    active_config = config or load_config()
    catalog = build_operational_target_catalog(active_config.tests.registry)
    by_name = {target.name: target for target in catalog.for_operation("baseline-classify")}
    grouped: dict[str, list[dict]] = {}
    for verdict in verdicts:
        if not isinstance(verdict, dict) or not isinstance(verdict.get("test_type"), str):
            raise TypeError("Classification persistence requires validated verdict dictionaries")
        grouped.setdefault(verdict["test_type"], []).append(verdict)
    for test_type, group in grouped.items():
        target = by_name.get(test_type)
        if target is None:
            raise ValueError(f"Classification target is not enabled: {test_type}")
        baseline_ids = {verdict.get("baseline_id") for verdict in group}
        if len(baseline_ids) != 1:
            raise ValueError("Classification persistence requires one baseline identity per target")
        validate_compatibility_classification_verdicts(
            group,
            target=target,
            expected_baseline_id=next(iter(baseline_ids)),
        )

    db_path = db_path or default_classification_db_path(active_config)
    classified_at = int(time.time()) if classified_at is None else int(classified_at)
    with closing(_connect_writable(db_path)) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS classification_results (
                  classified_at INTEGER NOT NULL,
                  node TEXT NOT NULL,
                  test_type TEXT NOT NULL,
                  baseline_id TEXT NOT NULL,
                  status TEXT NOT NULL,
                  passed INTEGER NOT NULL,
                  n_compared INTEGER NOT NULL,
                  n_degraded INTEGER NOT NULL,
                  n_improved INTEGER NOT NULL,
                  n_band_degraded INTEGER NOT NULL DEFAULT 0,
                  degraded_metric_fraction REAL NOT NULL DEFAULT 0.0,
                  worst_pct_diff REAL NOT NULL DEFAULT 0.0,
                  metrics_json TEXT NOT NULL,
                  PRIMARY KEY (classified_at, node, test_type, baseline_id)
                )
                """
            )
            _ensure_column(
                connection,
                "classification_results",
                "n_band_degraded",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                connection,
                "classification_results",
                "degraded_metric_fraction",
                "REAL NOT NULL DEFAULT 0.0",
            )
            _ensure_column(
                connection,
                "classification_results",
                "worst_pct_diff",
                "REAL NOT NULL DEFAULT 0.0",
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_classification_node_test_time "
                "ON classification_results(node, test_type, classified_at)"
            )
            connection.executemany(
                """
                INSERT OR REPLACE INTO classification_results (
                  classified_at, node, test_type, baseline_id, status, passed,
                  n_compared, n_degraded, n_improved, n_band_degraded,
                  degraded_metric_fraction, worst_pct_diff, metrics_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        classified_at,
                        verdict["node"],
                        verdict["test_type"],
                        verdict.get("baseline_id") or "",
                        verdict["status"],
                        0 if verdict["status"] == "degraded" else 1,
                        int(verdict.get("n_compared", 0)),
                        int(verdict.get("n_degraded", 0)),
                        int(verdict.get("n_improved", 0)),
                        int(verdict.get("n_band_degraded", verdict.get("n_degraded", 0))),
                        float(verdict.get("degraded_metric_fraction", 0.0)),
                        float(verdict.get("worst_pct_diff", 0.0)),
                        json.dumps(verdict.get("metrics", [])),
                    )
                    for verdict in verdicts
                ],
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    return len(verdicts)
