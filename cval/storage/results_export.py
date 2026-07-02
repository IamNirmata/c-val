"""Export latest c-val status rows to local files.

The source of truth is the read-only `latest_status` view in validation.db. This
module converts the latest row for one requested test into a local CSV snapshot
that operators can share, diff, or archive.
"""

from __future__ import annotations

import csv
import datetime as dt
import re
from pathlib import Path
from zoneinfo import ZoneInfo

from cval.models import ClassificationResultRow, LatestStatusRow, NcclMetrics, StorageMetrics
from cval.storage.classification_status import classification_rows_by_node_test

LOS_ANGELES = ZoneInfo("America/Los_Angeles")
UTC = dt.timezone.utc

RESULT_TEST_ALIASES = {
    "overall": "all",
    "all": "all",
    "storage": "storage",
    "nccl": "nccl",
    "dltest": "dltest",
    "dltest-numerical": "dltest",
    "dltest-compute": "dltest",
    "dltest-collective": "dltest",
    "dltest-overlap": "dltest",
}

CSV_BASE_COLUMNS = (
    "node",
    "test",
    "db_test",
    "latest_timestamp",
    "latest_time_utc",
    "latest_time_los_angeles",
    "result",
    "classification_status",
    "classification_passed",
    "classification_baseline_id",
    "classified_timestamp",
    "classified_time_los_angeles",
    "n_compared",
    "n_degraded",
    "n_band_degraded",
    "degraded_metric_fraction",
    "degraded_metric_percent",
    "worst_pct_diff",
)

# Keep the old name as an alias so existing call-sites and tests still compile.
CSV_COLUMNS = CSV_BASE_COLUMNS

Nccl_EXTRA_COLUMNS: tuple[str, ...] = ("nccl_busbw", "nccl_latency")
STORAGE_EXTRA_COLUMNS: tuple[str, ...] = (
    "iodepth_read_1file_iops",
    "iodepth_read_1file_bw",
    "iodepth_write_1file_iops",
    "iodepth_write_1file_bw",
    "numjobs_read_nfiles_iops",
    "numjobs_read_nfiles_bw",
    "numjobs_write_nfiles_iops",
    "numjobs_write_nfiles_bw",
    "randread_iops",
    "randread_bw",
    "randwrite_iops",
    "randwrite_bw",
)


def get_csv_columns(test_name: str) -> tuple[str, ...]:
    """Return the CSV fieldnames for a given test export."""
    display = display_result_test(test_name)
    if display == "nccl":
        return CSV_BASE_COLUMNS + Nccl_EXTRA_COLUMNS
    if display == "storage":
        return CSV_BASE_COLUMNS + STORAGE_EXTRA_COLUMNS
    if display == "overall":
        return CSV_BASE_COLUMNS + Nccl_EXTRA_COLUMNS + STORAGE_EXTRA_COLUMNS
    return CSV_BASE_COLUMNS


def normalize_result_test(test_name: str) -> str:
    """Map user-facing result test names to DB latest_status test names."""

    normalized = test_name.strip().lower()
    if normalized not in RESULT_TEST_ALIASES:
        valid = ", ".join(sorted(RESULT_TEST_ALIASES))
        raise ValueError(f"test must be one of: {valid}")
    return RESULT_TEST_ALIASES[normalized]


def display_result_test(test_name: str) -> str:
    """Return the user-facing display name for a selected test."""

    normalized = test_name.strip().lower()
    return "overall" if normalized in {"overall", "all"} else normalized


def classification_result_test(test_name: str) -> str:
    """Return the classification test_type used for a requested result export."""

    display_test = display_result_test(test_name)
    return "" if display_test == "overall" else display_test


def latest_result_rows(rows: list[LatestStatusRow], test_name: str) -> list[LatestStatusRow]:
    """Return rows for the selected test, sorted by node."""

    db_test = normalize_result_test(test_name)
    selected = [row for row in rows if row.test == db_test]
    return sorted(selected, key=lambda row: row.node)


def timestamp_to_utc(timestamp: int | None) -> str:
    """Format epoch seconds as UTC ISO-8601, or empty for missing timestamps."""

    if timestamp is None:
        return ""
    return dt.datetime.fromtimestamp(int(timestamp), tz=UTC).isoformat()


def timestamp_to_los_angeles(timestamp: int | None) -> str:
    """Format epoch seconds in America/Los_Angeles, or empty for missing timestamps."""

    if timestamp is None:
        return ""
    return dt.datetime.fromtimestamp(int(timestamp), tz=LOS_ANGELES).isoformat()


def los_angeles_filename_timestamp(now: dt.datetime | None = None) -> str:
    """Return a filename-safe LA timestamp like `20260617_201500_PDT`."""

    current = now or dt.datetime.now(tz=LOS_ANGELES)
    if current.tzinfo is None:
        current = current.replace(tzinfo=LOS_ANGELES)
    current_la = current.astimezone(LOS_ANGELES)
    suffix = current_la.strftime("%Y%m%d_%H%M%S_%Z")
    return re.sub(r"[^A-Za-z0-9_]+", "_", suffix)


def default_results_filename(test_name: str, now: dt.datetime | None = None) -> str:
    """Build the requested CSV filename: cval_<test>_<LA timestamp>.csv."""

    safe_test = re.sub(r"[^A-Za-z0-9_]+", "_", display_result_test(test_name))
    return f"cval_{safe_test}_{los_angeles_filename_timestamp(now)}.csv"


def rows_to_csv_records(
    rows: list[LatestStatusRow],
    requested_test: str,
    classifications: list[ClassificationResultRow] | None = None,
    nccl_metrics: dict[str, NcclMetrics] | None = None,
    storage_metrics: dict[str, StorageMetrics] | None = None,
) -> list[dict[str, str]]:
    """Convert status rows to CSV record dictionaries.

    ``nccl_metrics`` and ``storage_metrics`` are optional node→metrics maps
    fetched from test-nccl.db / test-storage.db. When supplied the relevant
    metric columns are populated; absent nodes get empty strings.
    """

    display_test = display_result_test(requested_test)
    classification_test = classification_result_test(requested_test)
    by_node_test = classification_rows_by_node_test(classifications or [])
    nccl_by_node = nccl_metrics or {}
    storage_by_node = storage_metrics or {}
    records: list[dict[str, str]] = []
    for row in rows:
        classification = by_node_test.get((row.node, classification_test))
        classification_status = classification.status if classification else ""
        classification_passed = "" if classification is None else str(classification.passed).lower()
        classified_at = classification.classified_at if classification else None
        degraded_fraction = classification.degraded_metric_fraction if classification else 0.0

        record: dict[str, str] = {
            "node": row.node,
            "test": display_test,
            "db_test": row.test,
            "latest_timestamp": "" if row.latest_timestamp is None else str(row.latest_timestamp),
            "latest_time_utc": timestamp_to_utc(row.latest_timestamp),
            "latest_time_los_angeles": timestamp_to_los_angeles(row.latest_timestamp),
            "result": row.result,
            "classification_status": classification_status,
            "classification_passed": classification_passed,
            "classification_baseline_id": classification.baseline_id if classification else "",
            "classified_timestamp": "" if classified_at is None else str(classified_at),
            "classified_time_los_angeles": timestamp_to_los_angeles(classified_at),
            "n_compared": "" if classification is None else str(classification.n_compared),
            "n_degraded": "" if classification is None else str(classification.n_degraded),
            "n_band_degraded": "" if classification is None else str(classification.n_band_degraded),
            "degraded_metric_fraction": "" if classification is None else f"{degraded_fraction:.6f}",
            "degraded_metric_percent": "" if classification is None else f"{degraded_fraction * 100.0:.3f}",
            "worst_pct_diff": "" if classification is None else f"{classification.worst_pct_diff:.3f}",
        }

        # NCCL metric columns (busbw GB/s, latency µs)
        nccl = nccl_by_node.get(row.node)
        record["nccl_busbw"] = "" if nccl is None or nccl.busbw is None else f"{nccl.busbw:.4f}"
        record["nccl_latency"] = "" if nccl is None or nccl.latency is None else f"{nccl.latency:.4f}"

        # Storage metric columns (IOPS and bandwidth KB/s)
        storage = storage_by_node.get(row.node)
        for col in STORAGE_EXTRA_COLUMNS:
            val = getattr(storage, col, None) if storage is not None else None
            record[col] = "" if val is None else f"{val:.4f}"

        records.append(record)
    return records


def write_latest_results_csv(
    rows: list[LatestStatusRow],
    test_name: str,
    output_dir: str | Path = ".",
    now: dt.datetime | None = None,
    classifications: list[ClassificationResultRow] | None = None,
    nccl_metrics: dict[str, NcclMetrics] | None = None,
    storage_metrics: dict[str, StorageMetrics] | None = None,
) -> Path:
    """Write latest rows for one test to a local CSV and return the path."""

    selected = latest_result_rows(rows, test_name)
    directory = Path(output_dir).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / default_results_filename(test_name, now)
    columns = get_csv_columns(test_name)

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(
            rows_to_csv_records(selected, test_name, classifications, nccl_metrics, storage_metrics)
        )

    return output_path
