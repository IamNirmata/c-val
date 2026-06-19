"""Read and export latest baseline classification results.

The raw pass/fail status in ``validation.db`` answers whether a validation job
completed its deterministic checks. This module reads the derived health
verdicts in ``classification-results.db`` so operators can see degraded nodes
without parsing SQLite JSON payloads manually.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from zoneinfo import ZoneInfo

from cval.baselines.storage import default_classification_db_path
from cval.k8s.client import KubectlClient
from cval.models import ClassificationResultRow
from cval.storage.status import resolve_status_pod

LOS_ANGELES = ZoneInfo("America/Los_Angeles")
UTC = dt.timezone.utc

CLASSIFICATION_CSV_COLUMNS = (
    "node",
    "test_type",
    "classified_at",
    "classified_time_utc",
    "classified_time_los_angeles",
    "baseline_id",
    "status",
    "passed",
    "n_compared",
    "n_degraded",
    "n_band_degraded",
    "n_improved",
    "degraded_metric_fraction",
    "degraded_metric_percent",
    "worst_pct_diff",
)


def timestamp_to_utc(timestamp: int | None) -> str:
    if timestamp is None:
        return ""
    return dt.datetime.fromtimestamp(int(timestamp), tz=UTC).isoformat()


def timestamp_to_los_angeles(timestamp: int | None) -> str:
    if timestamp is None:
        return ""
    return dt.datetime.fromtimestamp(int(timestamp), tz=LOS_ANGELES).isoformat()


def parse_latest_classification_rows_json(output: str) -> list[ClassificationResultRow]:
    """Parse JSON emitted by the read-only classification helper."""

    data = json.loads(output or "[]")
    if not isinstance(data, list):
        raise ValueError("classification JSON must be a list")
    rows: list[ClassificationResultRow] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        rows.append(_row_from_dict(item))
    return rows


def _row_from_dict(item: dict) -> ClassificationResultRow:
    n_compared = int(item.get("n_compared") or 0)
    n_degraded = int(item.get("n_degraded") or 0)
    degraded_fraction_value = item.get("degraded_metric_fraction")
    degraded_fraction = (
        float(degraded_fraction_value)
        if degraded_fraction_value not in {None, ""}
        else (n_degraded / n_compared if n_compared else 0.0)
    )
    return ClassificationResultRow(
        classified_at=int(item.get("classified_at") or 0),
        node=str(item.get("node", "")),
        test_type=str(item.get("test_type", "")),
        baseline_id=str(item.get("baseline_id", "")),
        status=str(item.get("status", "")),
        passed=bool(item.get("passed", False)),
        n_compared=n_compared,
        n_degraded=n_degraded,
        n_improved=int(item.get("n_improved") or 0),
        n_band_degraded=int(item.get("n_band_degraded") or item.get("n_degraded") or 0),
        degraded_metric_fraction=degraded_fraction,
        worst_pct_diff=float(item.get("worst_pct_diff") or 0.0),
    )


def latest_classification_rows_from_db(db_path: str | Path) -> list[ClassificationResultRow]:
    """Read latest classification rows directly from a local SQLite DB."""

    path = Path(db_path)
    if not path.exists():
        return []
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        return [_row_from_dict(dict(row)) for row in _latest_rows(connection)]


def get_latest_classification_rows(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
    config=None,
) -> list[ClassificationResultRow]:
    """Read latest classification rows from the PVC access pod in read-only mode."""

    from cval.config import load_config

    active_config = config or load_config()
    pod = pod or active_config.cluster.pvc_access_pod
    namespace = namespace or active_config.cluster.namespace
    db_path = db_path or str(default_classification_db_path(active_config))
    kubectl = client or KubectlClient()
    status_pod = resolve_status_pod(kubectl, namespace, pod)
    code = r'''
import json
import sqlite3
import sys
from pathlib import Path

db_path = sys.argv[1]
path = Path(db_path)
if not path.exists():
    print("[]")
    raise SystemExit(0)

def metric_fallbacks(metrics_json):
    try:
        metrics = json.loads(metrics_json or "[]")
    except Exception:
        metrics = []
    band_degraded = 0
    worst = 0.0
    for metric in metrics if isinstance(metrics, list) else []:
        if not isinstance(metric, dict):
            continue
        if metric.get("status") == "degraded":
            band_degraded += 1
            try:
                worst = max(worst, abs(float(metric.get("pct_diff") or 0.0)))
            except (TypeError, ValueError):
                pass
    return band_degraded, worst

try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "classification_results" not in tables:
        print("[]")
        raise SystemExit(0)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(classification_results)")}
    rows = conn.execute(
        """
        SELECT cr.* FROM classification_results cr
        JOIN (
          SELECT node, test_type, MAX(classified_at) AS latest_classified_at
          FROM classification_results
          GROUP BY node, test_type
        ) latest
          ON cr.node = latest.node
         AND cr.test_type = latest.test_type
         AND cr.classified_at = latest.latest_classified_at
        ORDER BY cr.test_type, cr.node
        """
    ).fetchall()
    out = []
    for row in rows:
        n_compared = int(row["n_compared"] or 0)
        n_degraded = int(row["n_degraded"] or 0)
        metrics_json = row["metrics_json"] if "metrics_json" in columns else "[]"
        fallback_band, fallback_worst = metric_fallbacks(metrics_json)
        n_band_degraded = (
            int(row["n_band_degraded"] or 0)
            if "n_band_degraded" in columns
            else fallback_band or n_degraded
        )
        degraded_fraction = (
            float(row["degraded_metric_fraction"] or 0.0)
            if "degraded_metric_fraction" in columns
            else (n_degraded / n_compared if n_compared else 0.0)
        )
        worst_pct_diff = (
            float(row["worst_pct_diff"] or 0.0)
            if "worst_pct_diff" in columns
            else fallback_worst
        )
        out.append(
            {
                "classified_at": row["classified_at"],
                "node": row["node"],
                "test_type": row["test_type"],
                "baseline_id": row["baseline_id"],
                "status": row["status"],
                "passed": bool(row["passed"]),
                "n_compared": n_compared,
                "n_degraded": n_degraded,
                "n_improved": int(row["n_improved"] or 0),
                "n_band_degraded": n_band_degraded,
                "degraded_metric_fraction": degraded_fraction,
                "worst_pct_diff": worst_pct_diff,
            }
        )
except Exception as exc:
    print(f"Error reading classification_results from {db_path}: {exc}", file=sys.stderr)
    raise SystemExit(1)

print(json.dumps(out))
'''
    result = kubectl.run(
        ["exec", "-i", "-n", namespace, status_pod, "--", "python3", "-", db_path],
        input_text=code,
    )
    return parse_latest_classification_rows_json(result.stdout)


def _latest_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "classification_results" not in tables:
        return []
    return connection.execute(
        """
        SELECT cr.* FROM classification_results cr
        JOIN (
          SELECT node, test_type, MAX(classified_at) AS latest_classified_at
          FROM classification_results
          GROUP BY node, test_type
        ) latest
          ON cr.node = latest.node
         AND cr.test_type = latest.test_type
         AND cr.classified_at = latest.latest_classified_at
        ORDER BY cr.test_type, cr.node
        """
    ).fetchall()


def classification_rows_by_node_test(
    rows: list[ClassificationResultRow],
) -> dict[tuple[str, str], ClassificationResultRow]:
    """Index latest classification rows by ``(node, test_type)``."""

    return {(row.node, row.test_type): row for row in rows}


def filter_classification_rows(
    rows: list[ClassificationResultRow], test_type: str | None
) -> list[ClassificationResultRow]:
    """Filter and sort latest classification rows for export."""

    if not test_type or test_type == "all":
        selected = rows
    else:
        selected = [row for row in rows if row.test_type == test_type]
    return sorted(selected, key=lambda row: (row.test_type, row.node))


def classification_rows_to_csv_records(
    rows: list[ClassificationResultRow],
) -> list[dict[str, str]]:
    """Convert classification rows to CSV-ready dictionaries."""

    records: list[dict[str, str]] = []
    for row in rows:
        records.append(
            {
                "node": row.node,
                "test_type": row.test_type,
                "classified_at": str(row.classified_at),
                "classified_time_utc": timestamp_to_utc(row.classified_at),
                "classified_time_los_angeles": timestamp_to_los_angeles(row.classified_at),
                "baseline_id": row.baseline_id,
                "status": row.status,
                "passed": "true" if row.passed else "false",
                "n_compared": str(row.n_compared),
                "n_degraded": str(row.n_degraded),
                "n_band_degraded": str(row.n_band_degraded),
                "n_improved": str(row.n_improved),
                "degraded_metric_fraction": f"{row.degraded_metric_fraction:.6f}",
                "degraded_metric_percent": f"{row.degraded_metric_fraction * 100.0:.3f}",
                "worst_pct_diff": f"{row.worst_pct_diff:.3f}",
            }
        )
    return records


def default_classifications_filename(test_type: str, now: dt.datetime | None = None) -> str:
    from cval.storage.results_export import los_angeles_filename_timestamp

    safe_test = test_type.replace("-", "_") if test_type else "all"
    return f"cval_classifications_{safe_test}_{los_angeles_filename_timestamp(now)}.csv"


def write_classifications_csv(
    rows: list[ClassificationResultRow],
    test_type: str | None = None,
    output_dir: str | Path = ".",
    now: dt.datetime | None = None,
) -> Path:
    """Write latest classification rows to a local CSV and return the path."""

    selected = filter_classification_rows(rows, test_type)
    directory = Path(output_dir).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / default_classifications_filename(test_type or "all", now)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CLASSIFICATION_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(classification_rows_to_csv_records(selected))
    return output_path


def classification_row_to_dict(row: ClassificationResultRow) -> dict:
    """Return a JSON-serializable classification row dict."""

    return asdict(row)