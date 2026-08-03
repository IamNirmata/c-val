"""Read-only normalization of copied legacy SQLite ``IB_HEALTH`` rows."""

from __future__ import annotations

import json
import math
import re
import sqlite3
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid5

from cval.nccl_eval.models import (
    IngestionBatch,
    NicResult,
    NodeResult,
    ResultStatus,
    TestRun,
)


LEGACY_RUN_NAMESPACE = UUID("78bb1ad2-14d8-5542-9f08-fb765653a4c3")
_NIC_COLUMN = re.compile(r"^mlx5_\d+(?:\.\d+)?$")
_SQLITE_DUPLICATE_SUFFIX = re.compile(r"^(?P<base>.+):\d+$")
_LEGACY_LATENCY_CONVERSION = "ms_to_us_x1000"


@dataclass(frozen=True)
class LegacyProfileMetadata:
    """Facts absent from the wide SQLite schema and therefore required explicitly."""

    test_definition_version: str
    gpu_model: str
    gpus_per_node: int
    compiled_nccl_version: str
    runtime_nccl_package_version: str
    driver_version: str
    driver_version_group: str
    topology_class: str
    test_config: Mapping[str, Any]
    source_commit: str
    image_digest: str
    implementation_identity: str
    cval_result_digest: str
    runtime_evidence_sha256: str
    test_name: str = "nccl-loopback-allreduce"
    cuda_version: str | None = None
    pytorch_version: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "test_definition_version",
            "gpu_model",
            "compiled_nccl_version",
            "runtime_nccl_package_version",
            "driver_version",
            "driver_version_group",
            "topology_class",
            "test_name",
            "source_commit",
            "image_digest",
            "implementation_identity",
            "cval_result_digest",
            "runtime_evidence_sha256",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"legacy metadata {name} must be explicit and non-empty")
        if isinstance(self.gpus_per_node, bool) or not isinstance(self.gpus_per_node, int):
            raise ValueError("legacy metadata gpus_per_node must be an integer")
        if self.gpus_per_node <= 0:
            raise ValueError("legacy metadata gpus_per_node must be positive")
        if not isinstance(self.test_config, Mapping):
            raise ValueError("legacy metadata test_config must be an object")
        config = dict(self.test_config)
        missing = {
            "collective",
            "datatype",
            "reduction",
            "message_size",
            "warmup_iterations",
        } - set(config)
        if missing:
            raise ValueError(
                "legacy metadata test_config is missing: " + ", ".join(sorted(missing))
            )
        expected_evidence = {
            "latency_unit": "us",
            "latency_source_unit": "ms",
            "latency_conversion": _LEGACY_LATENCY_CONVERSION,
        }
        for key, expected in expected_evidence.items():
            if key in config and config[key] != expected:
                raise ValueError(f"legacy metadata test_config.{key} must be {expected!r}")
            config[key] = expected
        object.__setattr__(self, "test_config", config)
        for name in (
            "source_commit",
            "image_digest",
            "implementation_identity",
            "cval_result_digest",
            "runtime_evidence_sha256",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.startswith("legacy:"):
                raise ValueError(f"legacy metadata {name} must be an explicit legacy: sentinel")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "LegacyProfileMetadata":
        if not isinstance(value, Mapping):
            raise ValueError("legacy profile metadata must be an object")
        allowed = {
            "test_definition_version",
            "gpu_model",
            "gpus_per_node",
            "compiled_nccl_version",
            "runtime_nccl_package_version",
            "driver_version",
            "driver_version_group",
            "topology_class",
            "test_config",
            "test_name",
            "source_commit",
            "image_digest",
            "implementation_identity",
            "cval_result_digest",
            "runtime_evidence_sha256",
            "cuda_version",
            "pytorch_version",
        }
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValueError("unknown legacy metadata field(s): " + ", ".join(unknown))
        required = allowed - {
            "test_name",
            "cuda_version",
            "pytorch_version",
        }
        missing = sorted(key for key in required if key not in value)
        if missing:
            raise ValueError(
                "legacy profile metadata requires explicit field(s): " + ", ".join(missing)
            )
        return cls(
            test_definition_version=_required_string(value, "test_definition_version"),
            gpu_model=_required_string(value, "gpu_model"),
            gpus_per_node=_required_integer(value, "gpus_per_node"),
            compiled_nccl_version=_required_string(value, "compiled_nccl_version"),
            runtime_nccl_package_version=_required_string(
                value, "runtime_nccl_package_version"
            ),
            driver_version=_required_string(value, "driver_version"),
            driver_version_group=_required_string(value, "driver_version_group"),
            topology_class=_required_string(value, "topology_class"),
            test_config=value["test_config"],
            source_commit=_required_string(value, "source_commit"),
            image_digest=_required_string(value, "image_digest"),
            implementation_identity=_required_string(value, "implementation_identity"),
            cval_result_digest=_required_string(value, "cval_result_digest"),
            runtime_evidence_sha256=_required_string(value, "runtime_evidence_sha256"),
            test_name=str(value.get("test_name", "nccl-loopback-allreduce")),
            cuda_version=_optional_string(value.get("cuda_version"), "cuda_version"),
            pytorch_version=_optional_string(value.get("pytorch_version"), "pytorch_version"),
        )


def read_legacy_batches(
    sqlite_path: str | Path, metadata: LegacyProfileMetadata
) -> tuple[IngestionBatch, ...]:
    """Read a copied SQLite DB in mode=ro and return deterministic normalized batches."""

    path = Path(sqlite_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Legacy SQLite copy not found: {path}")
    uri = path.as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        columns = [
            row[1]
            for row in connection.execute("PRAGMA table_info('IB_HEALTH')").fetchall()
        ]
        if not columns:
            raise ValueError("Legacy SQLite copy does not contain IB_HEALTH")
        folded_columns: dict[str, str] = {}
        for column in columns:
            folded = column.casefold()
            duplicate = _SQLITE_DUPLICATE_SUFFIX.fullmatch(column)
            collision = folded in folded_columns or (
                duplicate is not None
                and duplicate.group("base").casefold() in folded_columns
            )
            if collision:
                original = (
                    folded_columns[folded]
                    if folded in folded_columns
                    else folded_columns[duplicate.group("base").casefold()]
                )
                raise ValueError(
                    "Legacy IB_HEALTH contains duplicate or case-colliding columns: "
                    f"{original!r}, {column!r}"
                )
            folded_columns[folded] = column
        lower = {column.lower(): column for column in columns}
        required = {
            "node",
            "timestamp",
            "iterations",
            "image_name",
            "cuda",
            "pytorch",
            "samples",
            "bus_bw",
            "latency",
        }
        missing = sorted(required - set(lower))
        if missing:
            raise ValueError("Legacy IB_HEALTH is missing column(s): " + ", ".join(missing))
        nic_candidates = [
            column for column in columns if column.casefold().startswith("mlx5_")
        ]
        malformed_nics = sorted(
            column for column in nic_candidates if not _NIC_COLUMN.fullmatch(column)
        )
        if malformed_nics:
            raise ValueError(
                "Legacy IB_HEALTH contains malformed NIC column(s): "
                + ", ".join(malformed_nics)
            )
        nic_columns = sorted(nic_candidates, key=_nic_sort_key)
        rows = connection.execute(
            "SELECT * FROM IB_HEALTH ORDER BY timestamp, Node"
        ).fetchall()

    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    run_facts: dict[str, dict[str, object]] = {}
    for row in rows:
        timestamp = _timestamp(row[lower["timestamp"]], "timestamp")
        cuda = _row_text_with_fallback(
            row[lower["cuda"]], metadata.cuda_version, "cuda"
        )
        pytorch = _row_text_with_fallback(
            row[lower["pytorch"]], metadata.pytorch_version, "pytorch"
        )
        iterations = _positive_row_int(row[lower["iterations"]], "iterations")
        samples = _optional_nonnegative_int(row[lower["samples"]], "samples")
        image_name = _optional_row_text(row[lower["image_name"]])
        facts = {
            "timestamp": timestamp.isoformat(),
            "iterations": iterations,
            "samples": samples,
            "image_name": image_name,
            "cuda_version": cuda,
            "pytorch_version": pytorch,
            "metadata": {
                "test_name": metadata.test_name,
                "test_definition_version": metadata.test_definition_version,
                "gpu_model": metadata.gpu_model,
                "gpus_per_node": metadata.gpus_per_node,
                "compiled_nccl_version": metadata.compiled_nccl_version,
                "runtime_nccl_package_version": metadata.runtime_nccl_package_version,
                "driver_version": metadata.driver_version,
                "driver_version_group": metadata.driver_version_group,
                "topology_class": metadata.topology_class,
                "test_config": metadata.test_config,
            },
        }
        key = json.dumps(facts, sort_keys=True, separators=(",", ":"), allow_nan=False)
        grouped[key].append(row)
        run_facts[key] = facts

    batches: list[IngestionBatch] = []
    for key in sorted(grouped):
        facts = run_facts[key]
        run_id = uuid5(LEGACY_RUN_NAMESPACE, key)
        timestamp = datetime.fromisoformat(str(facts["timestamp"]))
        run = TestRun(
            run_id=run_id,
            test_name=metadata.test_name,
            test_definition_version=metadata.test_definition_version,
            started_at=timestamp,
            completed_at=None,
            image_name=facts["image_name"],
            image_digest=metadata.image_digest,
            cuda_version=str(facts["cuda_version"]),
            pytorch_version=str(facts["pytorch_version"]),
            compiled_nccl_version=metadata.compiled_nccl_version,
            runtime_nccl_package_version=metadata.runtime_nccl_package_version,
            driver_version=metadata.driver_version,
            driver_version_group=metadata.driver_version_group,
            topology_class=metadata.topology_class,
            gpu_model=metadata.gpu_model,
            gpus_per_node=metadata.gpus_per_node,
            iterations=int(facts["iterations"]),
            samples=facts["samples"],
            test_config=metadata.test_config,
            cval_run_id=f"legacy-{run_id}",
            cval_result_digest=metadata.cval_result_digest,
            summary_sha256="legacy:sqlite-row",
            runtime_evidence_sha256=metadata.runtime_evidence_sha256,
            source_commit=metadata.source_commit,
            implementation_identity=metadata.implementation_identity,
            legacy_source=True,
        )
        node_results: list[NodeResult] = []
        seen_nodes: dict[str, tuple[object, ...]] = {}
        for row in grouped[key]:
            node_name = _required_row_text(row[lower["node"]], "Node")
            row_signature = tuple(row[column] for column in columns)
            previous = seen_nodes.get(node_name)
            if previous is not None:
                if previous == row_signature:
                    continue
                raise ValueError(
                    "Legacy IB_HEALTH has differing duplicate rows for run/node "
                    f"{run_id}/{node_name}"
                )
            seen_nodes[node_name] = row_signature
            bus_bw = _optional_nonnegative_float(row[lower["bus_bw"]], "BUS_BW")
            latency_ms = _optional_nonnegative_float(row[lower["latency"]], "LATENCY")
            latency_us = None if latency_ms is None else latency_ms * 1000.0
            status = (
                ResultStatus.SUCCESS
                if bus_bw is not None and latency_us is not None
                else ResultStatus.NO_RESULT
            )
            missing_metrics = [
                name
                for name, value in (("BUS_BW", bus_bw), ("LATENCY", latency_us))
                if value is None
            ]
            nics = tuple(
                NicResult(column, value)
                for column in nic_columns
                if (value := _optional_nonnegative_float(row[column], column)) is not None
            )
            la_value = row[lower["la_timestamp"]] if "la_timestamp" in lower else None
            node_results.append(
                NodeResult(
                    node_name=node_name,
                    test_timestamp=_timestamp(row[lower["timestamp"]], "timestamp"),
                    la_timestamp=(None if la_value in (None, "") else _timestamp(la_value, "la_timestamp")),
                    bus_bw_gbps=bus_bw,
                    latency_us=latency_us,
                    result_status=status,
                    error_code=(
                        None
                        if status is ResultStatus.SUCCESS
                        else "LEGACY_MISSING_" + "_AND_".join(missing_metrics)
                    ),
                    error_message=(
                        None
                        if status is ResultStatus.SUCCESS
                        else "Legacy row lacks " + " and ".join(missing_metrics)
                    ),
                    nics=nics,
                )
            )
        batches.append(IngestionBatch(run, tuple(node_results)))
    return tuple(batches)


def legacy_summary(batches: tuple[IngestionBatch, ...], source: str | Path) -> dict[str, object]:
    return {
        "mode": "dry-run",
        "source_copy": str(Path(source).expanduser().resolve()),
        "latency_source_unit": "ms",
        "latency_unit": "us",
        "latency_conversion": _LEGACY_LATENCY_CONVERSION,
        "batch_count": len(batches),
        "node_result_count": sum(len(batch.node_results) for batch in batches),
        "nic_result_count": sum(
            len(node.nics) for batch in batches for node in batch.node_results
        ),
        "calibration_decision_count": 0,
        "run_ids": [str(batch.test_run.run_id) for batch in batches],
    }


def _timestamp(value: object, field_name: str) -> datetime:
    if isinstance(value, bool):
        raise ValueError(f"legacy {field_name} is invalid")
    if isinstance(value, int | float):
        parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.isdigit():
            parsed = datetime.fromtimestamp(int(text), tz=timezone.utc)
        else:
            try:
                parsed = datetime.fromisoformat(
                    text[:-1] + "+00:00" if text.endswith("Z") else text
                )
            except ValueError as exc:
                raise ValueError(f"legacy {field_name} is not a timestamp") from exc
            if parsed.tzinfo is None:
                raise ValueError(f"legacy {field_name} must include a timezone")
    else:
        raise ValueError(f"legacy {field_name} is not a timestamp")
    return parsed.astimezone(timezone.utc)


def _optional_nonnegative_float(value: object, field_name: str) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ValueError(f"legacy {field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"legacy {field_name} must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"legacy {field_name} must be finite and non-negative")
    return parsed


def _positive_row_int(value: object, field_name: str) -> int:
    parsed = _optional_nonnegative_int(value, field_name)
    if parsed is None or parsed <= 0:
        raise ValueError(f"legacy {field_name} must be positive")
    return parsed


def _optional_nonnegative_int(value: object, field_name: str) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise ValueError(f"legacy {field_name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"legacy {field_name} must be an integer") from exc
    if parsed < 0:
        raise ValueError(f"legacy {field_name} must be non-negative")
    return parsed


def _required_row_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"legacy {field_name} is missing; supply an explicit metadata override")
    return value.strip()


def _optional_row_text(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("legacy text field must be a string")
    return value.strip() or None


def _row_text_with_fallback(
    value: object, fallback: str | None, field_name: str
) -> str:
    row_value = _optional_row_text(value)
    if row_value is not None:
        return row_value
    if fallback is not None:
        return fallback
    raise ValueError(
        f"legacy {field_name} is missing; supply an explicit metadata fallback"
    )


def _required_string(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"legacy metadata {key} must be explicit and non-empty")
    return item.strip()


def _optional_string(value: object, key: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"legacy metadata {key} must be non-empty when supplied")
    return value.strip()


def _required_integer(value: Mapping[str, Any], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ValueError(f"legacy metadata {key} must be an integer")
    return item


def _nic_sort_key(name: str) -> tuple[int, int, str]:
    suffix = name.lower().rsplit("_", 1)[-1]
    device, _, port = suffix.partition(".")
    return (
        int(device) if device.isdigit() else 10_000,
        int(port) if port.isdigit() else 1,
        name,
    )
