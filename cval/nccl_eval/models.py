"""Immutable validated inputs for NCCL ingestion and evaluation."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID


_NIC_DEVICE = re.compile(r"^mlx5_\d+(?:\.\d+)?$")
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_LEGACY_SENTINEL = re.compile(r"^legacy:[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")


class ResultStatus(str, Enum):
    SUCCESS = "SUCCESS"
    TIMEOUT = "TIMEOUT"
    TEST_ERROR = "TEST_ERROR"
    NO_RESULT = "NO_RESULT"


class EvaluationScope(str, Enum):
    OUT_OF_SAMPLE = "OUT_OF_SAMPLE"
    IN_SAMPLE = "IN_SAMPLE"
    REEVALUATION = "REEVALUATION"


class EvaluationJobStatus(str, Enum):
    PENDING = "PENDING"
    WAITING_FOR_BASELINE = "WAITING_FOR_BASELINE"
    PROCESSING = "PROCESSING"
    RETRY = "RETRY"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ProfileStatus(str, Enum):
    COLLECTING = "COLLECTING"
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"


class BaselineStatus(str, Enum):
    BUILDING = "BUILDING"
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class NicResult:
    """One normalized InfiniBand device observation."""

    device_name: str
    max_bus_bw_gbps: float | None

    def __post_init__(self) -> None:
        _nonempty(self.device_name, "device_name", maximum=128)
        if not _NIC_DEVICE.fullmatch(self.device_name):
            raise ValueError("device_name must use the mlx5_<device>[.<port>] form")
        _optional_nonnegative_finite(self.max_bus_bw_gbps, "max_bus_bw_gbps")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "NicResult":
        _require_mapping(value, "NIC result")
        _reject_unknown(value, {"device_name", "max_bus_bw_gbps"}, "NIC result")
        return cls(
            device_name=_required_str(value, "device_name"),
            max_bus_bw_gbps=_optional_float(value.get("max_bus_bw_gbps"), "max_bus_bw_gbps"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "device_name": self.device_name,
            "max_bus_bw_gbps": self.max_bus_bw_gbps,
        }


@dataclass(frozen=True)
class NodeResult:
    """One node's immutable result within a multi-node run."""

    node_name: str
    test_timestamp: datetime
    bus_bw_gbps: float | None
    latency_us: float | None
    result_status: ResultStatus = ResultStatus.SUCCESS
    la_timestamp: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None
    nics: tuple[NicResult, ...] = ()

    def __post_init__(self) -> None:
        _nonempty(self.node_name, "node_name", maximum=253)
        object.__setattr__(self, "test_timestamp", _utc(self.test_timestamp, "test_timestamp"))
        if self.la_timestamp is not None:
            object.__setattr__(self, "la_timestamp", _utc(self.la_timestamp, "la_timestamp"))
        _optional_nonnegative_finite(self.bus_bw_gbps, "bus_bw_gbps")
        _optional_nonnegative_finite(self.latency_us, "latency_us")
        if not isinstance(self.result_status, ResultStatus):
            raise TypeError("result_status must be a ResultStatus")
        if self.result_status is ResultStatus.SUCCESS and (
            self.bus_bw_gbps is None or self.latency_us is None
        ):
            raise ValueError("SUCCESS node results require both bus_bw_gbps and latency_us")
        _optional_text(self.error_code, "error_code", maximum=128)
        _optional_text(self.error_message, "error_message", maximum=4000)
        if not isinstance(self.nics, tuple) or not all(
            isinstance(item, NicResult) for item in self.nics
        ):
            raise TypeError("nics must be tuple[NicResult, ...]")
        devices = [item.device_name for item in self.nics]
        if len(devices) != len(set(devices)):
            raise ValueError("nics must contain unique device_name values")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "NodeResult":
        _require_mapping(value, "node result")
        _reject_unknown(
            value,
            {
                "node_name",
                "test_timestamp",
                "la_timestamp",
                "bus_bw_gbps",
                "latency_us",
                "result_status",
                "error_code",
                "error_message",
                "nics",
            },
            "node result",
        )
        raw_nics = value.get("nics", [])
        if not isinstance(raw_nics, list | tuple):
            raise ValueError("node result nics must be an array")
        try:
            status = ResultStatus(value.get("result_status", ResultStatus.SUCCESS.value))
        except (TypeError, ValueError) as exc:
            raise ValueError("node result result_status is invalid") from exc
        return cls(
            node_name=_required_str(value, "node_name"),
            test_timestamp=_parse_datetime(value.get("test_timestamp"), "test_timestamp"),
            la_timestamp=(
                None
                if value.get("la_timestamp") is None
                else _parse_datetime(value.get("la_timestamp"), "la_timestamp")
            ),
            bus_bw_gbps=_optional_float(value.get("bus_bw_gbps"), "bus_bw_gbps"),
            latency_us=_optional_float(value.get("latency_us"), "latency_us"),
            result_status=status,
            error_code=_optional_input_str(value.get("error_code"), "error_code"),
            error_message=_optional_input_str(value.get("error_message"), "error_message"),
            nics=tuple(NicResult.from_dict(item) for item in raw_nics),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "node_name": self.node_name,
            "test_timestamp": self.test_timestamp.isoformat(),
            "la_timestamp": self.la_timestamp.isoformat() if self.la_timestamp else None,
            "bus_bw_gbps": self.bus_bw_gbps,
            "latency_us": self.latency_us,
            "result_status": self.result_status.value,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "nics": [item.to_dict() for item in self.nics],
        }


@dataclass(frozen=True)
class TestRun:
    """Shared environment and configuration for one NCCL test execution."""

    run_id: UUID
    test_name: str
    test_definition_version: str
    started_at: datetime
    completed_at: datetime | None
    cuda_version: str
    pytorch_version: str
    compiled_nccl_version: str
    runtime_nccl_package_version: str
    driver_version: str
    driver_version_group: str
    topology_class: str
    gpu_model: str
    gpus_per_node: int
    iterations: int
    samples: int | None
    test_config: Mapping[str, Any]
    cval_run_id: str
    cval_result_digest: str
    summary_sha256: str | None
    runtime_evidence_sha256: str
    source_commit: str
    image_digest: str
    implementation_identity: str
    legacy_source: bool
    image_name: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, UUID):
            raise TypeError("run_id must be a UUID")
        for name in (
            "test_name",
            "test_definition_version",
            "cuda_version",
            "pytorch_version",
            "compiled_nccl_version",
            "runtime_nccl_package_version",
            "driver_version",
            "driver_version_group",
            "topology_class",
            "gpu_model",
        ):
            _nonempty(getattr(self, name), name, maximum=256)
        _optional_text(self.image_name, "image_name", maximum=1000)
        if not isinstance(self.legacy_source, bool):
            raise TypeError("legacy_source must be boolean")
        if not _RUN_ID.fullmatch(self.cval_run_id) or self.cval_run_id in {".", ".."}:
            raise ValueError("cval_run_id must be a safe path segment")
        for name in ("cval_result_digest", "runtime_evidence_sha256"):
            _provenance_digest(getattr(self, name), name, legacy=self.legacy_source)
        if self.summary_sha256 is not None:
            _provenance_digest(
                self.summary_sha256, "summary_sha256", legacy=self.legacy_source
            )
        if self.legacy_source:
            for name in ("source_commit", "image_digest", "implementation_identity"):
                value = getattr(self, name)
                if not isinstance(value, str) or not _LEGACY_SENTINEL.fullmatch(value):
                    raise ValueError(f"legacy {name} must use an explicit legacy: sentinel")
        else:
            if not _COMMIT.fullmatch(self.source_commit):
                raise ValueError("source_commit must be an exact lowercase 40-hex commit")
            if not _SHA256.fullmatch(self.image_digest):
                raise ValueError("image_digest must be an exact sha256 digest")
            if not _SHA256.fullmatch(self.implementation_identity):
                raise ValueError("implementation_identity must be an exact sha256 digest")
        started_at = _utc(self.started_at, "started_at")
        object.__setattr__(self, "started_at", started_at)
        if self.completed_at is not None:
            completed_at = _utc(self.completed_at, "completed_at")
            if completed_at < started_at:
                raise ValueError("completed_at must not precede started_at")
            object.__setattr__(self, "completed_at", completed_at)
        _positive_int(self.gpus_per_node, "gpus_per_node", maximum=32767)
        _positive_int(self.iterations, "iterations")
        if self.samples is not None:
            _nonnegative_int(self.samples, "samples")
        _validate_json_value(self.test_config, "test_config")
        config = json_ready(self.test_config)
        required_config = {
            "collective",
            "datatype",
            "reduction",
            "message_size",
            "latency_unit",
            "warmup_iterations",
        }
        missing = sorted(required_config - set(self.test_config))
        if missing:
            raise ValueError(
                "test_config is missing material profile field(s): " + ", ".join(missing)
            )
        for key in ("collective", "datatype", "reduction", "message_size"):
            _nonempty(config[key], f"test_config.{key}", maximum=256)
        if config["latency_unit"] != "us":
            raise ValueError("test_config.latency_unit must be the canonical unit 'us'")
        _nonnegative_int(config["warmup_iterations"], "test_config.warmup_iterations")
        for key, expected in (("iterations", self.iterations), ("samples", self.samples)):
            if key in config and (
                type(config[key]) is not type(expected) or config[key] != expected
            ):
                raise ValueError(f"test_config.{key} must exactly match {key}")
            config[key] = expected
        object.__setattr__(self, "test_config", _freeze_json(config))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TestRun":
        _require_mapping(value, "test run")
        allowed = {
            "run_id",
            "test_name",
            "test_definition_version",
            "started_at",
            "completed_at",
            "image_name",
            "image_digest",
            "cuda_version",
            "pytorch_version",
            "compiled_nccl_version",
            "runtime_nccl_package_version",
            "driver_version",
            "driver_version_group",
            "topology_class",
            "gpu_model",
            "gpus_per_node",
            "iterations",
            "samples",
            "test_config",
            "cval_run_id",
            "cval_result_digest",
            "summary_sha256",
            "runtime_evidence_sha256",
            "source_commit",
            "implementation_identity",
            "legacy_source",
        }
        _reject_unknown(value, allowed, "test run")
        try:
            run_id = UUID(_required_str(value, "run_id"))
        except (TypeError, ValueError) as exc:
            raise ValueError("test run run_id must be a UUID") from exc
        config = value.get("test_config")
        _require_mapping(config, "test run test_config")
        return cls(
            run_id=run_id,
            test_name=_required_str(value, "test_name"),
            test_definition_version=_required_str(value, "test_definition_version"),
            started_at=_parse_datetime(value.get("started_at"), "started_at"),
            completed_at=(
                None
                if value.get("completed_at") is None
                else _parse_datetime(value.get("completed_at"), "completed_at")
            ),
            image_name=_optional_input_str(value.get("image_name"), "image_name"),
            image_digest=_required_str(value, "image_digest"),
            cuda_version=_required_str(value, "cuda_version"),
            pytorch_version=_required_str(value, "pytorch_version"),
            compiled_nccl_version=_required_str(value, "compiled_nccl_version"),
            runtime_nccl_package_version=_required_str(
                value, "runtime_nccl_package_version"
            ),
            driver_version=_required_str(value, "driver_version"),
            driver_version_group=_required_str(value, "driver_version_group"),
            topology_class=_required_str(value, "topology_class"),
            gpu_model=_required_str(value, "gpu_model"),
            gpus_per_node=_required_int(value, "gpus_per_node"),
            iterations=_required_int(value, "iterations"),
            samples=(
                None if value.get("samples") is None else _required_int(value, "samples")
            ),
            test_config=config,
            cval_run_id=_required_str(value, "cval_run_id"),
            cval_result_digest=_required_str(value, "cval_result_digest"),
            summary_sha256=_optional_input_str(
                value.get("summary_sha256"), "summary_sha256"
            ),
            runtime_evidence_sha256=_required_str(value, "runtime_evidence_sha256"),
            source_commit=_required_str(value, "source_commit"),
            implementation_identity=_required_str(value, "implementation_identity"),
            legacy_source=_required_bool(value, "legacy_source"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": str(self.run_id),
            "test_name": self.test_name,
            "test_definition_version": self.test_definition_version,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "image_name": self.image_name,
            "image_digest": self.image_digest,
            "cuda_version": self.cuda_version,
            "pytorch_version": self.pytorch_version,
            "compiled_nccl_version": self.compiled_nccl_version,
            "runtime_nccl_package_version": self.runtime_nccl_package_version,
            "driver_version": self.driver_version,
            "driver_version_group": self.driver_version_group,
            "topology_class": self.topology_class,
            "gpu_model": self.gpu_model,
            "gpus_per_node": self.gpus_per_node,
            "iterations": self.iterations,
            "samples": self.samples,
            "test_config": json_ready(self.test_config),
            "cval_run_id": self.cval_run_id,
            "cval_result_digest": self.cval_result_digest,
            "summary_sha256": self.summary_sha256,
            "runtime_evidence_sha256": self.runtime_evidence_sha256,
            "source_commit": self.source_commit,
            "implementation_identity": self.implementation_identity,
            "legacy_source": self.legacy_source,
        }


@dataclass(frozen=True)
class IngestionBatch:
    """Atomic multi-node ingestion unit."""

    test_run: TestRun
    node_results: tuple[NodeResult, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.test_run, TestRun):
            raise TypeError("test_run must be a TestRun")
        if not isinstance(self.node_results, tuple) or not self.node_results:
            raise ValueError("node_results must be a non-empty tuple")
        if not all(isinstance(item, NodeResult) for item in self.node_results):
            raise TypeError("node_results must contain only NodeResult values")
        nodes = [item.node_name for item in self.node_results]
        if len(nodes) != len(set(nodes)):
            raise ValueError("node_results must contain unique node_name values")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "IngestionBatch":
        _require_mapping(value, "ingestion batch")
        _reject_unknown(value, {"test_run", "node_results"}, "ingestion batch")
        run = value.get("test_run")
        nodes = value.get("node_results")
        _require_mapping(run, "ingestion batch test_run")
        if not isinstance(nodes, list | tuple):
            raise ValueError("ingestion batch node_results must be an array")
        return cls(TestRun.from_dict(run), tuple(NodeResult.from_dict(item) for item in nodes))

    def to_dict(self) -> dict[str, object]:
        return {
            "test_run": self.test_run.to_dict(),
            "node_results": [item.to_dict() for item in self.node_results],
        }


def json_ready(value: Any) -> Any:
    """Return mutable JSON-native containers from frozen model values."""

    if isinstance(value, Mapping):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_json(item) for item in value)
    return value


def _validate_json_value(value: Any, field_name: str) -> None:
    if value is None or isinstance(value, str | bool | int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{field_name} contains a non-finite number")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{field_name} object keys must be non-empty strings")
            _validate_json_value(item, f"{field_name}.{key}")
        return
    if isinstance(value, list | tuple):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{field_name}[{index}]")
        return
    raise ValueError(f"{field_name} contains a non-JSON value")


def _parse_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    return _utc(parsed, field_name)


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _optional_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be numeric or null")
    parsed = float(value)
    _optional_nonnegative_finite(parsed, field_name)
    return parsed


def _optional_nonnegative_finite(value: float | None, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{field_name} must be numeric or null")
    if not math.isfinite(float(value)) or float(value) < 0.0:
        raise ValueError(f"{field_name} must be finite and non-negative")


def _positive_int(value: int, field_name: str, *, maximum: int | None = None) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field_name} must not exceed {maximum}")


def _nonnegative_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def _nonempty(value: Any, field_name: str, *, maximum: int) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    if len(value) > maximum or "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be a single-line string of at most {maximum} characters")


def _optional_text(value: Any, field_name: str, *, maximum: int) -> None:
    if value is None:
        return
    _nonempty(value, field_name, maximum=maximum)


def _require_mapping(value: Any, field_name: str) -> None:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")


def _reject_unknown(value: Mapping[str, Any], allowed: set[str], field_name: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{field_name} has unknown field(s): {', '.join(unknown)}")


def _required_str(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return item.strip()


def _optional_input_str(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string or null")
    return value.strip()


def _required_int(value: Mapping[str, Any], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ValueError(f"{key} must be an integer")
    return item


def _required_bool(value: Mapping[str, Any], key: str) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        raise ValueError(f"{key} must be boolean")
    return item


def _provenance_digest(value: Any, field_name: str, *, legacy: bool) -> None:
    if not isinstance(value, str) or not (
        _SHA256.fullmatch(value) or (legacy and _LEGACY_SENTINEL.fullmatch(value))
    ):
        suffix = " or an explicit legacy: sentinel" if legacy else ""
        raise ValueError(f"{field_name} must be an exact sha256 digest{suffix}")
