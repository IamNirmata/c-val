"""Deterministic, non-authoritative U8/compatibility shadow parity reports."""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, Iterable

from cval.health.models import DnrReason, HEALTH_CLASS_NAMES, HealthClassCode
from cval.storage.per_test_results import (
    audit_classification_history_connection,
    validate_common_result_connection,
    validate_test_result_owner_integrity,
)
from cval.storage.sqlite_snapshot import (
    immutable_sqlite_snapshot,
    read_regular_file_without_atime,
)


PARITY_SCHEMA = "cval.shadow-parity.v1"
SQLITE_SIGNED_INT64_MAX = (1 << 63) - 1
_U8_BUCKETS = {
    "Excellent": "improved",
    "Nominal": "normal",
    "Underperforming": "degraded",
    "Very Bad": "degraded",
    "Terrible": "degraded",
    "DNR": "dnr",
}
_COMPAT_BUCKETS = {
    "improved": "improved",
    "normal": "normal",
    "degraded": "degraded",
}
_DNR_WITHOUT_BASELINE = frozenset(
    {
        DnrReason.RAW_FAILED.value,
        DnrReason.RAW_INCOMPLETE.value,
        DnrReason.MISSING_COMBINATION.value,
        DnrReason.NO_ACTIVE_BASELINE.value,
    }
)
_DNR_WITH_BASELINE = frozenset(
    {
        DnrReason.NO_OBSERVATIONS.value,
        DnrReason.INCOMPLETE_METRIC_COVERAGE.value,
        DnrReason.INCOMPATIBLE_ADAPTER_VERSION.value,
    }
)
_BASELINE_ID_PATTERN = re.compile(r"hb1:[0-9a-f]{64}")


def build_shadow_parity_report(
    *,
    u8_json_paths: Iterable[str | Path] = (),
    u8_db_paths: Iterable[str | Path] = (),
    compatibility_json_paths: Iterable[str | Path] = (),
    compatibility_db_paths: Iterable[str | Path] = (),
    registered_test_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Compare supplied copied inputs without writing or consulting live state."""

    u8: dict[tuple[str, str], dict[str, Any]] = {}
    compatibility: dict[tuple[str, str], dict[str, Any]] = {}
    registered = frozenset(registered_test_ids)
    if any(type(test_id) is not str or not test_id for test_id in registered):
        raise ValueError("Registered parity test IDs must be exact non-empty text")
    db_paths = tuple(Path(path) for path in u8_db_paths)
    if db_paths and not registered:
        raise ValueError("Copied U8 DB parity requires registered test IDs")
    for path in u8_json_paths:
        _merge(u8, _u8_json_records(Path(path)), "U8")
    for path in db_paths:
        _merge(u8, _u8_db_records(path, registered), "U8")
    for path in compatibility_json_paths:
        _merge(compatibility, _compat_json_records(Path(path)), "compatibility")
    for path in compatibility_db_paths:
        _merge(compatibility, _compat_db_records(Path(path)), "compatibility")
    if not u8 and not compatibility:
        raise ValueError("At least one copied U8 or compatibility input is required")

    keys = sorted(set(u8) | set(compatibility))
    paired: list[dict[str, Any]] = []
    u8_only: list[dict[str, str]] = []
    compatibility_only: list[dict[str, str]] = []
    matrix: dict[tuple[str, str], int] = {}
    u8_bucket_counts: dict[str, int] = {}
    compat_bucket_counts: dict[str, int] = {}
    for key in keys:
        u8_record = u8.get(key)
        compat_record = compatibility.get(key)
        if u8_record is not None:
            bucket = u8_record["direction_bucket"]
            u8_bucket_counts[bucket] = u8_bucket_counts.get(bucket, 0) + 1
        if compat_record is not None:
            bucket = compat_record["direction_bucket"]
            compat_bucket_counts[bucket] = compat_bucket_counts.get(bucket, 0) + 1
        if u8_record is None:
            compatibility_only.append({"node": key[0], "test_id": key[1]})
            continue
        if compat_record is None:
            u8_only.append({"node": key[0], "test_id": key[1]})
            continue
        matrix_key = (u8_record["direction_bucket"], compat_record["direction_bucket"])
        matrix[matrix_key] = matrix.get(matrix_key, 0) + 1
        paired.append(
            {
                "node": key[0],
                "test_id": key[1],
                "u8": u8_record,
                "compatibility": compat_record,
                "direction_match": matrix_key[0] == matrix_key[1],
            }
        )

    divergences = [record for record in paired if not record["direction_match"]]
    return {
        "schema_version": PARITY_SCHEMA,
        "authoritative": False,
        "ok": True,
        "coverage": {
            "u8": len(u8),
            "compatibility": len(compatibility),
            "paired": len(paired),
            "u8_only": len(u8_only),
            "compatibility_only": len(compatibility_only),
            "direction_matches": len(paired) - len(divergences),
            "direction_divergences": len(divergences),
        },
        "bucket_counts": {
            "u8": _sorted_counts(u8_bucket_counts),
            "compatibility": _sorted_counts(compat_bucket_counts),
        },
        "direction_matrix": [
            {"u8": key[0], "compatibility": key[1], "count": count}
            for key, count in sorted(matrix.items())
        ],
        "comparisons": paired,
        "divergences": divergences,
        "unpaired": {
            "u8_only": u8_only,
            "compatibility_only": compatibility_only,
        },
        "limitations": [
            "Direction buckets are a reporting projection, not a class equivalence or cutover decision.",
            "U8 DNR remains distinct and never maps to compatibility normal, improved, or degraded.",
            "Only latest supplied labels per node/test are compared; input completeness is external evidence.",
            "The report performs no Kubernetes, PVC, network, or live database access and writes nothing.",
        ],
    }


def _u8_json_records(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    records = _json_records(path)
    output: dict[tuple[str, str], dict[str, Any]] = {}
    for item in records:
        node = _required_text(item, "node")
        test_id = _required_text(item, "test_id", fallback="test_type")
        run_id = _required_text(item, "run_id")
        class_name = _required_alias(
            item,
            "health_class_name",
            "class_name",
            label="U8 class_name",
        )
        class_code = _required_alias(
            item,
            "health_class_numerical",
            "class_code",
            label="U8 class_code",
        )
        if isinstance(class_code, bool):
            raise ValueError("U8 class_code must be an integer in 0..5, not Boolean")
        if type(class_code) is not int:
            raise ValueError("U8 class_code must have type exactly int in 0..5")
        if type(class_name) is not str or class_name not in _U8_BUCKETS:
            raise ValueError(f"Unknown U8 health class: {class_name!r}")
        try:
            code = HealthClassCode(class_code)
        except ValueError as exc:
            raise ValueError("U8 class_code must be in 0..5") from exc
        if class_name != code.class_name:
            raise ValueError("U8 class code/name mismatch")
        dnr_reason = item.get("dnr_reason")
        baseline_id = item.get("baseline_id")
        _validate_u8_baseline_semantics(
            code,
            dnr_reason=dnr_reason,
            baseline_id=baseline_id,
            source="U8 JSON",
        )
        _validate_optional_timestamps(item, source="U8 JSON")
        record = {
            "class_code": int(code),
            "class_name": class_name,
            "dnr_reason": dnr_reason,
            "run_id": run_id,
            "baseline_id": baseline_id,
            "direction_bucket": _U8_BUCKETS[class_name],
        }
        _insert_unique(output, (node, test_id), record, "U8")
    return output


def _compat_json_records(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    records = _json_records(path)
    output: dict[tuple[str, str], dict[str, Any]] = {}
    for item in records:
        node = _required_text(item, "node")
        test_id = _required_text(item, "test_type", fallback="test_id")
        status = _required_text(item, "status")
        if status not in _COMPAT_BUCKETS:
            raise ValueError(f"Unknown compatibility label: {status!r}")
        baseline_id = _required_text(item, "baseline_id")
        _validate_optional_timestamps(item, source="compatibility JSON")
        record = {
            "status": status,
            "baseline_id": baseline_id,
            "direction_bucket": _COMPAT_BUCKETS[status],
        }
        _insert_unique(output, (node, test_id), record, "compatibility")
    return output


def _u8_db_records(
    path: Path,
    registered_test_ids: frozenset[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    output: dict[tuple[str, str], dict[str, Any]] = {}
    with immutable_sqlite_snapshot(path) as snapshot, closing(snapshot.connect()) as connection:
        validate_common_result_connection(connection)
        owner_rows = connection.execute(
            "SELECT DISTINCT test_id, typeof(test_id) FROM test_results ORDER BY test_id"
        ).fetchall()
        if (
            len(owner_rows) != 1
            or owner_rows[0][1] != "text"
            or type(owner_rows[0][0]) is not str
            or not owner_rows[0][0]
            or owner_rows[0][0] not in registered_test_ids
        ):
            raise RuntimeError("U8 SQLite test owner is not exactly one registered test ID")
        validate_test_result_owner_integrity(
            connection,
            test_id=owner_rows[0][0],
        )
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        }
        if "classification_history" not in tables:
            return output
        audit_classification_history_connection(connection)
        rows = connection.execute(
            """
            SELECT result.node, result.test_id, history.run_id,
                   history.baseline_id, history.health_class_name,
                   history.health_class_numerical, history.dnr_reason,
                   history.classified_at, history.classification_id,
                   typeof(history.health_class_numerical), history.result_id,
                   result.result_id, result.run_id
            FROM classification_history history
            JOIN test_results result
              ON result.run_id = history.run_id
             AND result.result_id = history.result_id
            ORDER BY result.node, result.test_id,
                     history.classified_at DESC, history.classification_id DESC
            """
        )
        seen: set[tuple[str, str]] = set()
        for row in rows:
            if (
                not _is_exact_nonempty_text(row[0])
                or not _is_exact_nonempty_text(row[1])
                or not _is_exact_nonempty_text(row[2])
                or not _is_sqlite_nonnegative_integer(row[7])
                or not _is_sqlite_positive_integer(row[8])
                or not _is_sqlite_positive_integer(row[10])
                or row[10] != row[11]
                or row[2] != row[12]
            ):
                raise ValueError("U8 SQLite result/run/test owner evidence is invalid")
            key = (row[0], row[1])
            if key in seen:
                continue
            seen.add(key)
            record = _decode_u8_db_record(row)
            _insert_unique(
                output,
                key,
                record,
                "U8",
            )
    return output


def _decode_u8_db_record(row: tuple[Any, ...]) -> dict[str, Any]:
    raw_code = row[5]
    if row[9] != "integer" or type(raw_code) is not int:
        raise ValueError(
            "U8 SQLite class_code must have storage class INTEGER and type exactly int"
        )
    try:
        code = HealthClassCode(raw_code)
    except ValueError as exc:
        raise ValueError("U8 SQLite class_code must be in 0..5") from exc

    class_name = row[4]
    if type(class_name) is not str or class_name != code.class_name:
        raise ValueError("U8 SQLite class name does not match its stable code")

    dnr_reason = row[6]
    baseline_id = row[3]
    _validate_u8_baseline_semantics(
        code,
        dnr_reason=dnr_reason,
        baseline_id=baseline_id,
        source="U8 SQLite",
    )

    return {
        "class_code": int(code),
        "class_name": class_name,
        "dnr_reason": dnr_reason,
        "run_id": row[2],
        "baseline_id": baseline_id,
        "direction_bucket": _U8_BUCKETS[class_name],
    }


def _compat_db_records(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    output: dict[tuple[str, str], dict[str, Any]] = {}
    with immutable_sqlite_snapshot(path) as snapshot, closing(snapshot.connect()) as connection:
        connection.execute("PRAGMA query_only=ON")
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        }
        if "classification_results" not in tables:
            raise ValueError("Compatibility DB has no classification_results table")
        required = {"node", "test_type", "status", "baseline_id", "classified_at"}
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(classification_results)")
        }
        if not required.issubset(columns):
            raise ValueError("Compatibility DB classification_results schema is incomplete")
        rows = connection.execute(
            """
            SELECT node, test_type, status, baseline_id, classified_at, rowid,
                   typeof(node), typeof(test_type), typeof(status),
                   typeof(baseline_id), typeof(classified_at), typeof(rowid)
            FROM classification_results
            ORDER BY node, test_type, classified_at DESC, rowid DESC
            """
        )
        seen: set[tuple[str, str]] = set()
        for row in rows:
            node, test_id, status, baseline_id, classified_at, row_id = row[:6]
            if tuple(row[6:]) != (
                "text",
                "text",
                "text",
                "text",
                "integer",
                "integer",
            ):
                raise ValueError(
                    "Compatibility SQLite row uses an invalid storage class"
                )
            if (
                type(node) is not str
                or not node.strip()
                or type(test_id) is not str
                or not test_id.strip()
                or type(baseline_id) is not str
                or not baseline_id.strip()
                or type(status) is not str
                or status not in _COMPAT_BUCKETS
                or not _is_sqlite_nonnegative_integer(classified_at)
                or not _is_sqlite_positive_integer(row_id)
            ):
                raise ValueError("Compatibility SQLite row evidence is invalid")
            key = (node, test_id)
            if key in seen:
                continue
            seen.add(key)
            _insert_unique(
                output,
                key,
                {
                    "status": status,
                    "baseline_id": baseline_id,
                    "direction_bucket": _COMPAT_BUCKETS[status],
                },
                "compatibility",
            )
    return output


def _json_records(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(
        read_regular_file_without_atime(path, description="parity JSON input").decode("utf-8"),
        object_pairs_hook=_reject_duplicate_json_keys,
        parse_constant=_reject_nonstandard_json_constant,
    )
    if type(payload) is not list or any(type(item) is not dict for item in payload):
        raise ValueError("Parity JSON input must be exactly an array of record objects")
    return payload


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"Parity JSON input contains duplicate object key: {key!r}")
        output[key] = value
    return output


def _reject_nonstandard_json_constant(value: str) -> Any:
    raise ValueError(f"Parity JSON input contains non-standard numeric constant: {value}")


def _required_text(item: dict[str, Any], key: str, *, fallback: str | None = None) -> str:
    primary_present = key in item
    fallback_present = fallback is not None and fallback in item
    if primary_present and fallback_present and (
        type(item[key]) is not type(item[fallback]) or item[key] != item[fallback]
    ):
        raise ValueError(f"Parity record {key} aliases conflict")
    value = item[key] if primary_present else item.get(fallback)
    if not _is_exact_nonempty_text(value):
        raise ValueError(f"Parity record {key} must be a non-empty string")
    return value


def _required_alias(
    item: dict[str, Any],
    primary: str,
    fallback: str,
    *,
    label: str,
) -> Any:
    primary_present = primary in item
    fallback_present = fallback in item
    if not primary_present and not fallback_present:
        raise ValueError(f"{label} is required")
    if primary_present and fallback_present and (
        type(item[primary]) is not type(item[fallback])
        or item[primary] != item[fallback]
    ):
        raise ValueError(f"{label} aliases conflict")
    return item[primary] if primary_present else item[fallback]


def _is_exact_nonempty_text(value: Any) -> bool:
    return type(value) is str and bool(value) and value == value.strip()


def _is_sqlite_nonnegative_integer(value: Any) -> bool:
    return type(value) is int and 0 <= value <= SQLITE_SIGNED_INT64_MAX


def _is_sqlite_positive_integer(value: Any) -> bool:
    return type(value) is int and 0 < value <= SQLITE_SIGNED_INT64_MAX


def _validate_optional_timestamps(item: dict[str, Any], *, source: str) -> None:
    for field in (
        "classified_at",
        "run_timestamp",
        "started_timestamp",
        "completed_timestamp",
    ):
        if field not in item:
            continue
        value = item[field]
        if not _is_sqlite_nonnegative_integer(value):
            raise ValueError(
                f"{source} {field} must have type exactly non-negative int no greater "
                f"than SQLite signed 64-bit maximum {SQLITE_SIGNED_INT64_MAX}"
            )


def _validate_u8_baseline_semantics(
    code: HealthClassCode,
    *,
    dnr_reason: Any,
    baseline_id: Any,
    source: str,
) -> None:
    stable_reasons = _DNR_WITHOUT_BASELINE | _DNR_WITH_BASELINE
    if code is HealthClassCode.DNR:
        if type(dnr_reason) is not str or dnr_reason not in stable_reasons:
            raise ValueError(f"{source} DNR requires exactly one stable DnrReason")
    elif dnr_reason is not None:
        raise ValueError(f"{source} DNR reason is valid only for class 5")
    if baseline_id is not None and (
        type(baseline_id) is not str
        or _BASELINE_ID_PATTERN.fullmatch(baseline_id) is None
    ):
        raise ValueError(f"{source} baseline_id must be hb1:<64 lowercase hex> or null")
    if code is not HealthClassCode.DNR and baseline_id is None:
        raise ValueError(f"{source} evaluated classes require a baseline_id")
    if code is HealthClassCode.DNR:
        requires_baseline = dnr_reason in _DNR_WITH_BASELINE
        if requires_baseline != (baseline_id is not None):
            raise ValueError(f"{source} DNR reason/baseline semantics are invalid")


def _insert_unique(
    output: dict[tuple[str, str], dict[str, Any]],
    key: tuple[str, str],
    record: dict[str, Any],
    label: str,
) -> None:
    if key in output:
        raise ValueError(f"Duplicate {label} node/test label: {key[0]}/{key[1]}")
    output[key] = record


def _merge(
    output: dict[tuple[str, str], dict[str, Any]],
    records: dict[tuple[str, str], dict[str, Any]],
    label: str,
) -> None:
    for key, record in records.items():
        _insert_unique(output, key, record, label)


def _sorted_counts(counts: dict[str, int]) -> list[dict[str, Any]]:
    return [{"bucket": key, "count": counts[key]} for key in sorted(counts)]


__all__ = [
    "PARITY_SCHEMA",
    "SQLITE_SIGNED_INT64_MAX",
    "build_shadow_parity_report",
]
