"""Immutable authorization boundary for every compatibility SQLite writer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cval.config import CvalConfig, encode_config_snapshot, load_config
from cval.validation.results import (
    ValidationResult,
    ValidationResultLike,
    ValidationResultV2,
    load_validation_result,
    validation_result_digest,
    validation_result_to_env,
)
from cval.validation.runtime import effective_config_digest
from cval.storage.paths import safe_existing_evidence_path, safe_writable_file_path


_AUTHORIZATION_NONCE = object()


@dataclass(frozen=True)
class ResultWriteAuthorization:
    """Validated immutable result/config provenance for one DB write sequence."""

    result: ValidationResultLike
    result_path: Path
    result_digest: str
    config: CvalConfig
    _nonce: object


@dataclass(frozen=True)
class DlRebuildAuthorization:
    results_root: Path
    db_paths: dict[str, Path]
    config: CvalConfig
    _nonce: object


def authorize_result_write(
    result_path: str | Path,
    *,
    result_digest: str,
    config_snapshot_b64: str,
    config: CvalConfig | None = None,
) -> ResultWriteAuthorization:
    """Validate one result and return an unforgeable in-process write capability."""

    active_config = config or load_config()
    path = Path(result_path).expanduser()
    result = load_validation_result(path)
    if not config_snapshot_b64:
        raise ValueError("Compatibility writes require an immutable config snapshot")
    if config_snapshot_b64 != encode_config_snapshot(active_config):
        raise ValueError("Compatibility config does not match its snapshot")
    if result_digest != validation_result_digest(result):
        raise ValueError("Compatibility result does not match its digest")
    if isinstance(result, ValidationResultV2):
        if result.global_config_digest != effective_config_digest(active_config):
            raise ValueError("V2 compatibility result config digest does not match snapshot")
        from cval.validation.ingestion import preflight_test_results_file

        preflight_test_results_file(
            path,
            config=active_config,
            result_digest=result_digest,
            config_snapshot_b64=config_snapshot_b64,
        )
    elif not isinstance(result, ValidationResult):  # pragma: no cover
        raise ValueError("Unsupported compatibility result schema")
    run_id = result.run_id if isinstance(result, ValidationResultV2) else f"{result.node}-{result.timestamp}"
    expected_path = (
        Path(active_config.runtime.validation_root)
        / "logs/job_logs"
        / result.node
        / run_id
        / "result.json"
    )
    safe_result_path = safe_writable_file_path(
        path,
        allowed_root=active_config.runtime.validation_root,
        description="compatibility result",
    )
    if path != expected_path or safe_result_path != expected_path or not path.is_file():
        raise ValueError("Compatibility result path is not canonical")
    for target in (
        active_config.storage.validation_db_path,
        active_config.storage.run_history_db_path,
        active_config.storage.storage_db_path,
        active_config.storage.nccl_db_path,
        active_config.storage.dl_numerical_db_path,
        active_config.storage.dl_compute_db_path,
        active_config.storage.dl_collective_db_path,
        active_config.storage.dl_overlap_db_path,
    ):
        safe_writable_file_path(target)
    return ResultWriteAuthorization(
        result=result,
        result_path=path,
        result_digest=result_digest,
        config=active_config,
        _nonce=_AUTHORIZATION_NONCE,
    )


def require_result_write_authorization(
    authorization: ResultWriteAuthorization | None,
) -> ResultWriteAuthorization:
    """Reject direct low-level writers without a validated file boundary."""

    if (
        authorization is None
        or not isinstance(authorization, ResultWriteAuthorization)
        or authorization._nonce is not _AUTHORIZATION_NONCE
    ):
        raise PermissionError(
            "Compatibility SQLite writes require validated result provenance"
        )
    return authorization


def authorize_dl_rebuild(
    results_root: str | Path,
    output_dir: str | Path | None,
    *,
    config: CvalConfig,
    config_snapshot_b64: str,
) -> DlRebuildAuthorization:
    """Authorize one configured four-DB DL rebuild and preflight every target."""

    if not config_snapshot_b64 or config_snapshot_b64 != encode_config_snapshot(config):
        raise ValueError("DL rebuild config does not match an immutable snapshot")
    raw_root = Path(results_root).expanduser()
    root = safe_existing_evidence_path(
        raw_root,
        expected_path=raw_root,
        allowed_root=config.runtime.dl_results_root_path,
        expect_directory=True,
        description="DL rebuild results root",
        allow_missing=True,
    )
    configured = {
        "numerical_correctness": Path(config.storage.dl_numerical_db_path),
        "compute_performance": Path(config.storage.dl_compute_db_path),
        "collective_performance": Path(config.storage.dl_collective_db_path),
        "overlap_performance": Path(config.storage.dl_overlap_db_path),
    }
    if output_dir is None:
        targets = configured
    else:
        output = Path(output_dir)
        targets = {
            "numerical_correctness": output / "dltest_numerical_correctness.db",
            "compute_performance": output / "dltest_compute_performance.db",
            "collective_performance": output / "dltest_collective_performance.db",
            "overlap_performance": output / "dltest_overlap_performance.db",
        }
        if {
            key: path.expanduser().resolve() for key, path in targets.items()
        } != {
            key: path.expanduser().resolve() for key, path in configured.items()
        }:
            raise ValueError("DL rebuild output directory does not match configured DB paths")
    safe_targets = {
        key: safe_writable_file_path(path) for key, path in targets.items()
    }
    return DlRebuildAuthorization(root, safe_targets, config, _AUTHORIZATION_NONCE)


def require_dl_rebuild_authorization(
    authorization: DlRebuildAuthorization | None,
    *,
    results_root: str | Path,
    db_paths: dict[str, Path],
) -> DlRebuildAuthorization:
    requested_root = Path(results_root).expanduser()
    if not requested_root.is_absolute():
        requested_root = Path.cwd() / requested_root
    requested_root = Path(*requested_root.parts)
    if (
        authorization is None
        or not isinstance(authorization, DlRebuildAuthorization)
        or authorization._nonce is not _AUTHORIZATION_NONCE
        or authorization.results_root != requested_root
        or authorization.db_paths
        != {key: path.expanduser().resolve() for key, path in db_paths.items()}
    ):
        raise PermissionError("DL rebuild requires validated configured provenance")
    return authorization


def validate_compatibility_write(
    authorization: ResultWriteAuthorization | None,
    *,
    operation: str,
    node: str,
    timestamp: object,
    db_path: str | Path,
    test: str = "",
    status: str = "",
    results: dict[str, str] | None = None,
    evidence_path: str | Path | None = None,
    image_name: str = "",
    pytorch_version: str = "",
    cuda_version: str = "",
    run_id: str = "",
) -> ResultWriteAuthorization:
    """Require a write request to match every immutable provenance field."""

    auth = require_result_write_authorization(authorization)
    result = auth.result
    if node != result.node or _timestamp_epoch(timestamp) != _timestamp_epoch(
        result.timestamp
    ):
        raise ValueError("Compatibility write identity does not match result evidence")
    expected_versions = (
        (image_name, result.image_name)
        if operation == "storage"
        else (
            image_name,
            result.image_name,
            pytorch_version,
            result.pytorch_version,
            cuda_version,
            result.cuda_version,
        )
    )
    if operation == "storage":
        versions_match = expected_versions[0] == expected_versions[1]
    else:
        versions_match = all(
            expected_versions[index] == expected_versions[index + 1]
            for index in range(0, len(expected_versions), 2)
        )
    if not versions_match:
        raise ValueError("Compatibility write versions do not match result evidence")

    projected = validation_result_to_env(result)
    if operation == "validation-result":
        expected_status = (
            projected["overall_result"]
            if test == "all"
            else result.tests[test].status
            if test in result.tests
            else None
        )
        if expected_status != status:
            raise ValueError("Compatibility status does not match result evidence")
    elif operation == "validation-run":
        from cval.validation.compatibility import project_legacy_statuses

        expected = project_legacy_statuses(projected)
        if results != expected:
            raise ValueError("Compatibility status set does not match result evidence")
    elif operation in {"storage", "nccl"}:
        test_result = result.tests.get(operation)
        if test_result is None or test_result.status != "pass":
            raise ValueError(f"Compatibility {operation} metrics require a passing result")
        if isinstance(result, ValidationResultV2) and evidence_path is not None:
            expected_evidence = (
                test_result.artifacts if operation == "storage" else test_result.summary
            )
            if Path(evidence_path) != Path(expected_evidence):
                raise ValueError(
                    f"Compatibility {operation} path does not match result evidence"
                )
    else:
        raise ValueError(f"Unknown compatibility write operation: {operation}")

    target_field = {
        "validation-result": "validation_db_path",
        "validation-run": "validation_db_path",
        "storage": "storage_db_path",
        "nccl": "nccl_db_path",
    }[operation]
    if Path(db_path).expanduser() != Path(
        getattr(auth.config.storage, target_field)
    ).expanduser():
        raise ValueError(
            f"Compatibility {operation} DB path does not match snapshot"
        )
    expected_run_id = (
        result.run_id
        if isinstance(result, ValidationResultV2)
        else f"{result.node}-{result.timestamp}"
    )
    if run_id and run_id != expected_run_id:
        raise ValueError("Compatibility run ID does not match result evidence")
    if operation in {"storage", "nccl"} and evidence_path is not None:
        evidence_root = (
            Path(auth.config.runtime.validation_root)
            / "validation_tests"
            / operation
            / "runs"
        )
        expected_evidence = (
            evidence_root
            / result.node
            / expected_run_id
            / ("artifacts" if operation == "storage" else "summary.json")
        )
        safe_existing_evidence_path(
            evidence_path,
            expected_path=expected_evidence,
            allowed_root=evidence_root,
            expect_directory=operation == "storage",
            description=f"compatibility {operation} evidence",
        )
    return auth


def _timestamp_epoch(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("Boolean timestamp is invalid")
    if isinstance(value, int | float):
        return int(value)
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        parsed = datetime.strptime(text, "%Y%m%d_%H%M%S")
        return int(parsed.replace(tzinfo=timezone.utc).timestamp())


