"""Immutable native NCCL outbox production and scheduled ingestion."""

from __future__ import annotations

import hashlib
import heapq
import json
import math
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping
from uuid import UUID, uuid5

from cval.nccl_eval.models import IngestionBatch, NicResult, NodeResult, ResultStatus, TestRun
from cval.nccl_eval.profile import build_profile_identity
from cval.nccl_eval.runtime_evidence import (
    RuntimeEvidence,
    atomic_write_once,
)
from cval.validation.registry import RegisteredValidationTest, validation_test_config_digest
from cval.validation.results import (
    RUN_ID_PATTERN,
    ValidationResultV2,
    parse_validation_result_v2,
    validation_result_v2_digest,
)

if TYPE_CHECKING:
    from cval.config import CvalConfig
    from cval.nccl_eval.config import NcclEvaluationConfig


OUTBOX_RUN_NAMESPACE = UUID("5973bca5-6ea2-5d95-882d-511107003da3")
OUTBOX_FILE_MODE = 0o644
OUTBOX_DIRECTORY_MODE = 0o755
MAX_OUTBOX_FILE_BYTES = 1024 * 1024
OUTBOX_COMMIT_SCHEMA = "cval.nccl-outbox-commit.v1"
_SAFE_JSON_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}\.json$")
_CREDENTIAL_URL = re.compile(r"(postgres(?:ql)?://)[^\s/@:]+(?::[^\s/@]*)?@", re.IGNORECASE)


@dataclass(frozen=True)
class ScannedOutboxFile:
    name: str
    sha256: str
    payload: bytes
    batch: IngestionBatch
    profile_id: str
    observed_fingerprint: str


@dataclass(frozen=True)
class OutboxScan:
    root: Path
    root_exists: bool
    limit: int
    discovered_json_count: int
    files: tuple[ScannedOutboxFile, ...]
    invalid: tuple[dict[str, str], ...]

    def public_dict(self) -> dict[str, object]:
        profile_ids = sorted({item.profile_id for item in self.files})
        return {
            "mode": "dry-run",
            "progression": "lexical-preview-only",
            "outbox_root": str(self.root),
            "root_exists": self.root_exists,
            "limit": self.limit,
            "discovered_json_count": self.discovered_json_count,
            "selected_count": len(self.files) + len(self.invalid),
            "valid_count": len(self.files),
            "invalid_count": len(self.invalid),
            "valid_files": [item.name for item in self.files],
            "invalid": list(self.invalid),
            "profile_ids": profile_ids,
        }


class OutboxIngestionError(RuntimeError):
    """Fail-closed ingestion error carrying completed per-file receipts."""

    def __init__(self, receipt: dict[str, object]) -> None:
        self.receipt = receipt
        errors = receipt.get("errors", [])
        detail = errors[-1] if isinstance(errors, list) and errors else "unknown error"
        super().__init__(
            "outbox ingestion stopped after durable per-file commits; "
            f"receipt={detail!r}"
        )


def build_ingestion_batch(
    *,
    result_json: Path,
    summary: Path,
    runtime_evidence: Path,
    result_digest: str,
    config: CvalConfig,
) -> IngestionBatch:
    """Build one strict native batch from canonical c-val run evidence."""

    result_payload = _read_stable_regular_file(result_json, maximum_bytes=MAX_OUTBOX_FILE_BYTES)
    try:
        result_raw = json.loads(result_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("NCCL result JSON is not valid UTF-8 JSON") from exc
    if not isinstance(result_raw, dict):
        raise ValueError("NCCL result JSON must be an object")
    result = parse_validation_result_v2(result_raw)
    computed_result_digest = validation_result_v2_digest(result)
    if result_digest != computed_result_digest:
        raise ValueError("NCCL result digest does not match the canonical ValidationResultV2")
    registered = config.tests.registry.require("nccl")
    test = result.tests.get("nccl")
    if test is None or not test.enabled or not test.selected:
        raise ValueError("NCCL outbox emission requires a selected enabled NCCL result")
    if result.completed_at is None or test.completed_at is None or test.started_at is None:
        raise ValueError("NCCL outbox emission requires terminal run and test timestamps")

    expected_result = (
        Path(config.runtime.validation_root)
        / "logs"
        / "job_logs"
        / result.node
        / result.run_id
        / "result.json"
    )
    _require_same_path(result_json, expected_result, "result JSON", must_exist=True)
    _require_same_path(summary, Path(test.summary), "NCCL summary", must_exist=test.status == "pass")
    expected_evidence = Path(test.artifacts) / "runtime-evidence.json"
    _require_same_path(
        runtime_evidence,
        expected_evidence,
        "NCCL runtime evidence",
        must_exist=True,
    )
    if test.config_path != registered.config_path:
        raise ValueError("NCCL result config_path does not match the registered descriptor")
    expected_config_digest = validation_test_config_digest(registered)
    if test.config_digest != expected_config_digest:
        raise ValueError("NCCL result config_digest does not match the registered descriptor")

    evidence_payload = _read_stable_regular_file(
        runtime_evidence, maximum_bytes=16384, expected_mode=0o600
    )
    try:
        evidence_raw = json.loads(evidence_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("NCCL runtime evidence is not valid UTF-8 JSON") from exc
    evidence = RuntimeEvidence.from_dict(evidence_raw)
    settings = registered.definition.settings
    _validate_profile_sources(settings)
    iterations = _positive_int(settings.get("iterations"), "settings.iterations")
    samples = _positive_int(
        settings.get("evaluation_samples_per_result"),
        "settings.evaluation_samples_per_result",
    )
    test_config = _profile_test_config(
        settings,
        registered=registered,
        config_digest=expected_config_digest,
        iterations=iterations,
        samples=samples,
    )
    run_uuid = uuid5(
        OUTBOX_RUN_NAMESPACE,
        f"{result.run_id}\n{expected_config_digest}",
    )
    result_status, error_code, error_message = _result_failure(result, test)
    bus_bw: float | None = None
    latency_us: float | None = None
    nics: tuple[NicResult, ...] = ()
    summary_digest: str | None = None
    if result_status is ResultStatus.SUCCESS:
        summary_payload = _read_stable_regular_file(
            summary, maximum_bytes=MAX_OUTBOX_FILE_BYTES
        )
        summary_digest = f"sha256:{hashlib.sha256(summary_payload).hexdigest()}"
        bus_bw, latency_us, nics = _load_summary(
            summary_payload,
            expected_iterations=iterations,
            expected_data_size_gb=_positive_int(
                settings.get("data_size_gb"), "settings.data_size_gb"
            ),
        )

    started_at = _timestamp(test.started_at, "NCCL test started_at")
    completed_at = _timestamp(test.completed_at, "NCCL test completed_at")
    node_timestamp = completed_at
    la_timestamp = _timestamp(result.timestamp_la, "result timestamp_la")
    image_digest = _image_digest(result.image_name)
    if image_digest is None:
        raise ValueError("native NCCL evaluation requires an immutable image @sha256 digest")
    implementation_identity = "sha256:" + hashlib.sha256(
        (
            f"{registered.config_path}\n{expected_config_digest}\n"
            f"{settings['evaluation_test_definition_version']}\n"
        ).encode("utf-8")
    ).hexdigest()
    return IngestionBatch(
        test_run=TestRun(
            run_id=run_uuid,
            test_name=_required_setting(settings, "evaluation_test_name"),
            test_definition_version=_required_setting(
                settings, "evaluation_test_definition_version"
            ),
            started_at=started_at,
            completed_at=completed_at,
            image_name=result.image_name or None,
            image_digest=image_digest,
            cuda_version=_required_fact(result.cuda_version, "result cuda_version"),
            pytorch_version=_required_fact(
                result.pytorch_version, "result pytorch_version"
            ),
            compiled_nccl_version=evidence.compiled_nccl_version,
            runtime_nccl_package_version=evidence.runtime_nccl_package_version,
            driver_version=evidence.driver_version,
            driver_version_group=evidence.driver_version_group,
            topology_class=evidence.topology_class,
            gpu_model=evidence.gpu_model,
            gpus_per_node=_positive_int(
                settings.get("gpu_count"), "settings.gpu_count"
            ),
            iterations=iterations,
            samples=samples,
            test_config=test_config,
            cval_run_id=result.run_id,
            cval_result_digest=computed_result_digest,
            summary_sha256=summary_digest,
            runtime_evidence_sha256=(
                f"sha256:{hashlib.sha256(evidence_payload).hexdigest()}"
            ),
            source_commit=result.git_ref,
            implementation_identity=implementation_identity,
            legacy_source=False,
        ),
        node_results=(
            NodeResult(
                node_name=result.node,
                test_timestamp=node_timestamp,
                la_timestamp=la_timestamp,
                bus_bw_gbps=bus_bw,
                latency_us=latency_us,
                result_status=result_status,
                error_code=error_code,
                error_message=error_message,
                nics=nics,
            ),
        ),
    )


def outbox_payload(batch: IngestionBatch) -> bytes:
    if not isinstance(batch, IngestionBatch):
        raise TypeError("batch must be an IngestionBatch")
    return (
        json.dumps(
            batch.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def emit_outbox(outbox_root: Path, cval_run_id: str, batch: IngestionBatch) -> dict[str, object]:
    """Create ``pending/<c-val-run-id>.json`` before authoritative raw DB writes."""

    if not isinstance(cval_run_id, str) or not RUN_ID_PATTERN.fullmatch(cval_run_id):
        raise ValueError("c-val run ID is not a safe outbox filename")
    root = _ensure_outbox_root(outbox_root)
    pending_root = _ensure_outbox_root(root / "pending")
    _ensure_outbox_root(root / "committed")
    path = pending_root / f"{cval_run_id}.json"
    payload = outbox_payload(batch)
    created = atomic_write_once(path, payload, mode=OUTBOX_FILE_MODE)
    profile = build_profile_identity(batch.test_run)
    return {
        "mode": "apply",
        "pending_file": str(path),
        "created": created,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
        "run_id": str(batch.test_run.run_id),
        "cval_run_id": cval_run_id,
        "profile_id": str(profile.profile_id),
    }


def emission_plan(outbox_root: Path, cval_run_id: str, batch: IngestionBatch) -> dict[str, object]:
    if not isinstance(cval_run_id, str) or not RUN_ID_PATTERN.fullmatch(cval_run_id):
        raise ValueError("c-val run ID is not a safe outbox filename")
    payload = outbox_payload(batch)
    profile = build_profile_identity(batch.test_run)
    return {
        "mode": "dry-run",
        "valid": True,
        "pending_file": str(Path(outbox_root) / "pending" / f"{cval_run_id}.json"),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
        "run_id": str(batch.test_run.run_id),
        "cval_run_id": cval_run_id,
        "profile_id": str(profile.profile_id),
        "node_count": len(batch.node_results),
    }


def commit_outbox(
    outbox_root: Path,
    *,
    pending: Path,
    result_digest: str,
) -> dict[str, object]:
    """Create the retryable final marker only after raw SQLite writes commit."""

    validated = _validate_pending_commit(outbox_root, pending, result_digest)
    committed_root = _ensure_outbox_root(Path(outbox_root) / "committed")
    marker = committed_root / Path(pending).name
    created = atomic_write_once(
        marker, validated["marker_payload"], mode=OUTBOX_FILE_MODE
    )
    return {
        "mode": "apply",
        "commit_marker": str(marker),
        "created": created,
        "pending_sha256": validated["pending_sha256"],
        "result_digest": result_digest,
        "cval_run_id": validated["cval_run_id"],
    }


def commit_outbox_plan(
    outbox_root: Path,
    *,
    pending: Path,
    result_digest: str,
) -> dict[str, object]:
    """Validate a pending payload and return the marker plan without mutation."""

    validated = _validate_pending_commit(outbox_root, pending, result_digest)
    return {
        "mode": "dry-run",
        "valid": True,
        "pending": str(Path(pending)),
        "commit_marker": str(Path(outbox_root) / "committed" / Path(pending).name),
        "pending_sha256": validated["pending_sha256"],
        "result_digest": result_digest,
        "cval_run_id": validated["cval_run_id"],
    }


def _validate_pending_commit(
    outbox_root: Path,
    pending: Path,
    result_digest: str,
) -> dict[str, object]:
    """Bind one exact pending payload to its expected immutable commit marker."""

    root = Path(outbox_root)
    pending = Path(pending)
    if not root.is_absolute() or not pending.is_absolute():
        raise ValueError("outbox and pending paths must be absolute")
    if pending.parent != root / "pending" or not _SAFE_JSON_NAME.fullmatch(pending.name):
        raise ValueError("pending path must be an immediate safe file below outbox/pending")
    payload = _read_stable_regular_file(
        pending, maximum_bytes=MAX_OUTBOX_FILE_BYTES, expected_mode=OUTBOX_FILE_MODE
    )
    try:
        batch = IngestionBatch.from_dict(json.loads(payload.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("pending NCCL outbox payload is invalid") from exc
    cval_run_id = pending.name[:-5]
    if batch.test_run.cval_run_id != cval_run_id:
        raise ValueError("pending filename does not match embedded c-val run ID")
    if batch.test_run.cval_result_digest != result_digest:
        raise ValueError("pending payload does not match the supplied result digest")
    pending_sha256 = hashlib.sha256(payload).hexdigest()
    marker_value = {
        "schema_version": OUTBOX_COMMIT_SCHEMA,
        "cval_run_id": cval_run_id,
        "pending_sha256": pending_sha256,
        "cval_result_digest": result_digest,
    }
    marker_payload = (
        json.dumps(marker_value, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    return {
        "pending_sha256": pending_sha256,
        "cval_run_id": cval_run_id,
        "marker_payload": marker_payload,
    }


def scan_outbox(outbox_root: Path, *, limit: int) -> OutboxScan:
    """Database-free lexical preview of committed marker/pending pairs."""

    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000:
        raise ValueError("outbox limit must be between 1 and 5000")
    root = Path(outbox_root)
    if not root.is_absolute():
        raise ValueError("outbox root must be absolute")
    try:
        root_fd = _open_directory_no_symlinks(root)
    except FileNotFoundError:
        return OutboxScan(root, False, limit, 0, (), ())
    files: list[ScannedOutboxFile] = []
    invalid: list[dict[str, str]] = []
    discovered = 0
    pending_fd: int | None = None
    committed_fd: int | None = None
    try:
        try:
            pending_fd = os.open(
                "pending",
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
            committed_fd = os.open(
                "committed",
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
        except FileNotFoundError:
            return OutboxScan(root, True, limit, 0, (), ())
        with os.scandir(committed_fd) as entries:
            json_names = (
                entry.name
                for entry in entries
                if entry.name.endswith(".json")
            )
            selected = heapq.nsmallest(limit, json_names)
        with os.scandir(committed_fd) as entries:
            discovered = sum(1 for entry in entries if entry.name.endswith(".json"))
        for name in selected:
            try:
                scanned = _scan_one(pending_fd, committed_fd, name)
            except Exception as exc:  # noqa: BLE001 - invalid-file reporting boundary
                invalid.append({"file": name, "error": _safe_scan_error(exc)})
            else:
                files.append(scanned)
    finally:
        if pending_fd is not None:
            os.close(pending_fd)
        if committed_fd is not None:
            os.close(committed_fd)
        os.close(root_fd)
    return OutboxScan(
        root=root,
        root_exists=True,
        limit=limit,
        discovered_json_count=discovered,
        files=tuple(files),
        invalid=tuple(invalid),
    )


def ingest_scanned_outbox(
    config: NcclEvaluationConfig,
    scan: OutboxScan,
    *,
    continue_on_error: bool = False,
) -> dict[str, object]:
    """Durably ingest each exact scanned file without mutating the outbox."""

    if scan.invalid:
        raise ValueError("outbox apply refuses a scan containing invalid files")
    from cval.nccl_eval.service import open_repository

    receipts: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    with open_repository(config) as repository:
        for item in scan.files:
            try:
                receipt = repository.ingest_outbox_batch(
                    item.batch,
                    outbox_name=item.name,
                    content_sha256=item.sha256,
                    observed_fingerprint=item.observed_fingerprint,
                )
            except Exception as exc:  # noqa: BLE001 - per-file durable boundary
                errors.append({"file": item.name, "error": _safe_db_error(exc, config)})
                result = {
                    "mode": "apply",
                    "outbox_root": str(scan.root),
                    "attempted_count": len(receipts) + 1,
                    "ingested_count": len(receipts),
                    "receipts": receipts,
                    "errors": errors,
                    "stopped_early": not continue_on_error,
                }
                if not continue_on_error:
                    raise OutboxIngestionError(result) from exc
            else:
                receipts.append({"file": item.name, **receipt})
    return {
        "mode": "apply",
        "outbox_root": str(scan.root),
        "attempted_count": len(scan.files),
        "ingested_count": len(receipts),
        "receipts": receipts,
        "errors": errors,
        "stopped_early": False,
    }


def ingest_outbox_progression(
    config: NcclEvaluationConfig,
    outbox_root: Path,
    *,
    limit: int,
    continue_on_error: bool = False,
) -> dict[str, object]:
    """Process at most ``limit`` nonterminal committed names from the durable cursor."""

    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 5000:
        raise ValueError("outbox limit must be between 1 and 5000")
    root = Path(outbox_root)
    if not root.is_absolute():
        raise ValueError("outbox root must be absolute")
    from cval.nccl_eval.service import open_repository

    root_fd = _open_directory_no_symlinks(root)
    pending_fd: int | None = None
    committed_fd: int | None = None
    try:
        pending_fd = os.open(
            "pending",
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
        committed_fd = os.open(
            "committed",
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_fd,
        )
        with os.scandir(committed_fd) as entries:
            names = sorted(entry.name for entry in entries if entry.name.endswith(".json"))
        ingested: list[dict[str, object]] = []
        rejected: list[dict[str, object]] = []
        errors: list[dict[str, str]] = []
        skipped_terminal = 0
        with open_repository(config) as repository:
            cursor = repository.get_outbox_cursor()
            split = next(
                (index for index, name in enumerate(names) if cursor is None or name > cursor),
                len(names),
            )
            ordered = names[split:] + names[:split]
            for chunk_start in range(0, len(ordered), 5000):
                chunk = ordered[chunk_start : chunk_start + 5000]
                terminal = repository.outbox_terminal_states(chunk)
                terminal_cursor: str | None = None
                for name in chunk:
                    if name in terminal:
                        skipped_terminal += 1
                        terminal_cursor = name
                        continue
                    if terminal_cursor is not None:
                        repository.set_outbox_cursor(terminal_cursor)
                        terminal_cursor = None
                    try:
                        item = _scan_one(pending_fd, committed_fd, name)
                    except Exception as exc:  # noqa: BLE001 - durable rejection boundary
                        fingerprint = _rejection_fingerprint(committed_fd, name)
                        receipt = repository.record_outbox_rejection(
                            outbox_name=name,
                            safe_error=_safe_scan_error(exc),
                            observed_fingerprint=fingerprint,
                        )
                        repository.set_outbox_cursor(name)
                        rejected.append(receipt)
                    else:
                        try:
                            receipt = repository.ingest_outbox_batch(
                                item.batch,
                                outbox_name=item.name,
                                content_sha256=item.sha256,
                                observed_fingerprint=item.observed_fingerprint,
                            )
                        except Exception as exc:  # noqa: BLE001 - per-file DB boundary
                            errors.append(
                                {"file": item.name, "error": _safe_db_error(exc, config)}
                            )
                            if not continue_on_error:
                                raise OutboxIngestionError(
                                    {
                                        "mode": "apply",
                                        "outbox_root": str(root),
                                        "processed_count": (
                                            len(ingested) + len(rejected) + len(errors)
                                        ),
                                        "ingested_count": len(ingested),
                                        "rejected_count": len(rejected),
                                        "errors": errors,
                                        "stopped_early": True,
                                    }
                                ) from exc
                        else:
                            repository.set_outbox_cursor(name)
                            ingested.append(receipt)
                    if len(ingested) + len(rejected) + len(errors) >= limit:
                        return {
                            "mode": "apply",
                            "outbox_root": str(root),
                            "processed_count": (
                                len(ingested) + len(rejected) + len(errors)
                            ),
                            "ingested_count": len(ingested),
                            "rejected_count": len(rejected),
                            "error_count": len(errors),
                            "skipped_terminal_count": skipped_terminal,
                            "ingested": ingested,
                            "rejected": rejected,
                            "errors": errors,
                        }
                if terminal_cursor is not None:
                    repository.set_outbox_cursor(terminal_cursor)
        return {
            "mode": "apply",
            "outbox_root": str(root),
            "processed_count": len(ingested) + len(rejected) + len(errors),
            "ingested_count": len(ingested),
            "rejected_count": len(rejected),
            "error_count": len(errors),
            "skipped_terminal_count": skipped_terminal,
            "ingested": ingested,
            "rejected": rejected,
            "errors": errors,
        }
    finally:
        if pending_fd is not None:
            os.close(pending_fd)
        if committed_fd is not None:
            os.close(committed_fd)
        os.close(root_fd)


def _rejection_fingerprint(directory_fd: int, name: str) -> str:
    try:
        value = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        material = (
            f"{name}\0{value.st_dev}\0{value.st_ino}\0{value.st_mode}\0"
            f"{value.st_nlink}\0{value.st_size}\0{value.st_mtime_ns}\0{value.st_ctime_ns}"
        )
    except OSError as exc:
        material = f"{name}\0unavailable\0{type(exc).__name__}"
    return "sha256:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def _profile_test_config(
    settings: Mapping[str, Any],
    *,
    registered: RegisteredValidationTest,
    config_digest: str,
    iterations: int,
    samples: int,
) -> dict[str, object]:
    message_size = _positive_int(
        settings.get("evaluation_message_size_bytes"),
        "settings.evaluation_message_size_bytes",
    )
    return {
        "collective": _required_setting(settings, "evaluation_collective"),
        "datatype": _required_setting(settings, "evaluation_datatype"),
        "reduction": _required_setting(settings, "evaluation_reduction"),
        "message_size": f"{message_size}B",
        "message_size_bytes": message_size,
        "warmup_iterations": _nonnegative_int(
            settings.get("evaluation_warmup_iterations"),
            "settings.evaluation_warmup_iterations",
        ),
        "latency_unit": _required_setting(settings, "evaluation_latency_unit"),
        "latency_source_unit": _required_setting(
            settings, "evaluation_latency_source_unit"
        ),
        "latency_conversion": _required_setting(
            settings, "evaluation_latency_conversion"
        ),
        "iterations": iterations,
        "samples": samples,
        "iteration_semantics": _required_setting(
            settings, "evaluation_iteration_semantics"
        ),
        "sample_semantics": _required_setting(settings, "evaluation_sample_semantics"),
        "descriptor_config_path": registered.config_path,
        "descriptor_config_digest": config_digest,
    }


def _validate_profile_sources(settings: Mapping[str, Any]) -> None:
    expected = {
        "evaluation_latency_unit": "us",
        "evaluation_latency_source_unit": "ms",
        "evaluation_latency_conversion": "ms_to_us_x1000",
        "evaluation_driver_group_source": "runtime_evidence",
        "evaluation_topology_class_source": "runtime_evidence",
    }
    for key, value in expected.items():
        if settings.get(key) != value:
            raise ValueError(f"NCCL descriptor {key} must be exactly {value!r}")


def _load_summary(
    payload: bytes,
    *,
    expected_iterations: int,
    expected_data_size_gb: int,
) -> tuple[float, float, tuple[NicResult, ...]]:
    try:
        raw = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("NCCL summary is not valid UTF-8 JSON") from exc
    if not isinstance(raw, dict):
        raise ValueError("NCCL summary must be an object")
    bus = _positive_metric(raw.get("GCR_BUSBW"), "GCR_BUSBW")
    latency_ms = _positive_metric(raw.get("GCR_LATENCY"), "GCR_LATENCY")
    if raw.get("GCR_ITERATIONS") != expected_iterations:
        raise ValueError("NCCL summary iterations do not match the descriptor")
    if raw.get("GCR_DATA_SIZE_GB") != expected_data_size_gb:
        raise ValueError("NCCL summary data size does not match the descriptor")
    ports = raw.get("GCR_IB_PORT_BW_GBPS", {})
    if not isinstance(ports, dict):
        raise ValueError("NCCL summary HCA metrics must be an object")
    nics: list[NicResult] = []
    for device_name in sorted(ports):
        item = ports[device_name]
        if not isinstance(item, dict):
            raise ValueError(f"NCCL HCA summary for {device_name} must be an object")
        maximum = item.get("max_gbps")
        nics.append(
            NicResult(
                device_name,
                None if maximum is None else _nonnegative_metric(maximum, f"{device_name}.max_gbps"),
            )
        )
    return bus, latency_ms * 1000.0, tuple(nics)


def _result_failure(result: ValidationResultV2, test: Any) -> tuple[ResultStatus, str | None, str | None]:
    if test.status == "pass":
        return ResultStatus.SUCCESS, None, None
    matching = [item for item in result.errors if item.get("test_id") == "nccl"]
    exact_error = matching[-1] if matching else None
    if test.phase == "timed_out":
        status = ResultStatus.TIMEOUT
        default_code = "CVAL_NCCL_TIMED_OUT"
    elif test.phase == "interrupted":
        status = ResultStatus.NO_RESULT
        default_code = "CVAL_NCCL_INTERRUPTED"
    else:
        status = ResultStatus.TEST_ERROR
        if test.phase in {"finished", "setup_failed"} and test.exit_code is not None:
            default_code = f"CVAL_NCCL_{test.phase.upper()}_EXIT_{test.exit_code}"
        else:
            default_code = f"CVAL_NCCL_{test.phase.upper()}"
    error_code = str(exact_error["code"]) if exact_error else default_code
    message = (
        str(exact_error["message"])
        if exact_error
        else (test.message or f"NCCL test ended in phase {test.phase}")
    )
    return status, error_code, message[:4000]


def _scan_one(pending_fd: int, committed_fd: int, name: str) -> ScannedOutboxFile:
    if not _SAFE_JSON_NAME.fullmatch(name) or not RUN_ID_PATTERN.fullmatch(name[:-5]):
        raise ValueError("outbox filename is not a safe c-val run ID")
    marker_payload = _read_outbox_at(committed_fd, name, maximum_bytes=16 * 1024)
    try:
        marker = json.loads(marker_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("commit marker is not valid UTF-8 JSON") from exc
    if not isinstance(marker, dict) or set(marker) != {
        "schema_version",
        "cval_run_id",
        "pending_sha256",
        "cval_result_digest",
    }:
        raise ValueError("commit marker fields are invalid")
    if marker["schema_version"] != OUTBOX_COMMIT_SCHEMA:
        raise ValueError("commit marker schema is unsupported")
    if marker["cval_run_id"] != name[:-5]:
        raise ValueError("commit marker filename does not match c-val run ID")
    if not isinstance(marker["pending_sha256"], str) or not re.fullmatch(
        r"[0-9a-f]{64}", marker["pending_sha256"]
    ):
        raise ValueError("commit marker pending digest is invalid")
    if not isinstance(marker["cval_result_digest"], str) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", marker["cval_result_digest"]
    ):
        raise ValueError("commit marker result digest is invalid")
    payload = _read_outbox_at(pending_fd, name, maximum_bytes=MAX_OUTBOX_FILE_BYTES)
    sha256 = hashlib.sha256(payload).hexdigest()
    if sha256 != marker["pending_sha256"]:
        raise ValueError("commit marker does not bind the pending payload")
    try:
        raw = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("outbox file is not valid UTF-8 JSON") from exc
    batch = IngestionBatch.from_dict(raw)
    if batch.test_run.cval_run_id != name[:-5]:
        raise ValueError("outbox filename does not match embedded c-val run ID")
    if batch.test_run.cval_result_digest != marker["cval_result_digest"]:
        raise ValueError("commit marker result digest does not match pending payload")
    profile = build_profile_identity(batch.test_run)
    fingerprint = hashlib.sha256(
        marker_payload + b"\0" + sha256.encode("ascii")
    ).hexdigest()
    return ScannedOutboxFile(
        name=name,
        sha256=sha256,
        payload=payload,
        batch=batch,
        profile_id=str(profile.profile_id),
        observed_fingerprint=f"sha256:{fingerprint}",
    )


def _read_outbox_at(directory_fd: int, name: str, *, maximum_bytes: int) -> bytes:
    descriptor = os.open(
        name,
        os.O_RDONLY
        | os.O_NONBLOCK
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=directory_fd,
    )
    try:
        value = os.fstat(descriptor)
        if (
            not stat.S_ISREG(value.st_mode)
            or stat.S_IMODE(value.st_mode) != OUTBOX_FILE_MODE
            or value.st_nlink != 1
            or value.st_size <= 0
            or value.st_size > maximum_bytes
        ):
            raise PermissionError("outbox file mode, type, link count, or size is unsafe")
        before = _stat_identity(value)
        payload = _read_fd_exact(descriptor, value.st_size)
        if (
            _stat_identity(os.fstat(descriptor)) != before
            or _stat_identity(
                os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            )
            != before
        ):
            raise OSError("outbox file changed while being read")
        return payload
    finally:
        os.close(descriptor)


def _ensure_outbox_root(path: Path) -> Path:
    root = Path(path)
    if not root.is_absolute():
        raise ValueError("outbox root must be absolute")
    current = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in root.parts[1:]:
            if part in {"", ".", ".."}:
                raise ValueError("outbox root contains an unsafe path component")
            try:
                os.mkdir(part, OUTBOX_DIRECTORY_MODE, dir_fd=current)
            except FileExistsError:
                pass
            following = os.open(
                part,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = following
        value = os.fstat(current)
        if stat.S_IMODE(value.st_mode) != OUTBOX_DIRECTORY_MODE:
            raise PermissionError("outbox root must have exact mode 0755")
        os.fsync(current)
    finally:
        os.close(current)
    return root


def _open_directory_no_symlinks(path: Path) -> int:
    current = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            following = os.open(
                part,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = following
        value = os.fstat(current)
        if stat.S_IMODE(value.st_mode) != OUTBOX_DIRECTORY_MODE:
            raise PermissionError("outbox root must have exact mode 0755")
        return current
    except BaseException:
        os.close(current)
        raise


def _open_parent_no_symlinks(path: Path) -> int:
    current = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:]:
            if part in {"", ".", ".."}:
                raise ValueError("immutable input path contains an unsafe component")
            following = os.open(
                part,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = following
        return current
    except BaseException:
        os.close(current)
        raise


def _read_stable_regular_file(
    path: Path,
    *,
    maximum_bytes: int,
    expected_mode: int | None = None,
) -> bytes:
    path = Path(path)
    if not path.is_absolute() or os.path.normpath(str(path)) != str(path):
        raise ValueError("immutable input path must be absolute and lexical-canonical")
    parent_fd = _open_parent_no_symlinks(path.parent)
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path.name,
            os.O_RDONLY
            | os.O_CLOEXEC
            | os.O_NONBLOCK
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        value = os.fstat(descriptor)
        if (
            not stat.S_ISREG(value.st_mode)
            or value.st_nlink != 1
            or value.st_size <= 0
            or value.st_size > maximum_bytes
            or (
                expected_mode is not None
                and stat.S_IMODE(value.st_mode) != expected_mode
            )
        ):
            raise PermissionError(f"immutable input file is unsafe: {path.name}")
        before = _stat_identity(value)
        payload = _read_fd_exact(descriptor, value.st_size)
        if (
            _stat_identity(os.fstat(descriptor)) != before
            or _stat_identity(
                os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            )
            != before
        ):
            raise OSError(f"immutable input file changed while reading: {path.name}")
        return payload
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent_fd)


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_fd_exact(descriptor: int, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = os.read(descriptor, min(65536, remaining))
        if not chunk:
            raise OSError("outbox file changed while being read")
        chunks.append(chunk)
        remaining -= len(chunk)
    if os.read(descriptor, 1):
        raise OSError("outbox file grew while being read")
    return b"".join(chunks)


def _require_same_path(actual: Path, expected: Path, name: str, *, must_exist: bool) -> None:
    del must_exist
    actual_path = Path(actual)
    expected_path = Path(expected)
    if (
        not actual_path.is_absolute()
        or not expected_path.is_absolute()
        or os.path.normpath(str(actual_path)) != str(actual_path)
        or os.path.normpath(str(expected_path)) != str(expected_path)
        or actual_path != expected_path
    ):
        raise ValueError(f"{name} path does not match the canonical c-val result")


def _timestamp(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise ValueError(f"{field_name} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return parsed


def _image_digest(image_name: str) -> str | None:
    if "@sha256:" not in image_name:
        return None
    digest = image_name.rsplit("@", 1)[1]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("result image digest is malformed")
    return digest


def _required_setting(settings: Mapping[str, Any], key: str) -> str:
    return _required_fact(settings.get(key), f"settings.{key}")


def _required_fact(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be a non-empty single-line string")
    return value.strip()


def _positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _positive_metric(value: object, field_name: str) -> float:
    parsed = _nonnegative_metric(value, field_name)
    if parsed <= 0.0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def _nonnegative_metric(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0.0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return parsed


def _safe_scan_error(error: BaseException) -> str:
    return (str(error).replace("\n", " ").replace("\r", " ") or type(error).__name__)[:500]


def _safe_db_error(error: BaseException, config: NcclEvaluationConfig) -> str:
    message = str(error).replace("\n", " ").replace("\r", " ")
    if config.database_url:
        message = message.replace(config.database_url, "[DATABASE_URL REDACTED]")
    message = _CREDENTIAL_URL.sub(r"\1[REDACTED]@", message)
    return (message or type(error).__name__)[:500]
