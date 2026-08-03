"""Immutable authorization boundary for every current raw SQLite writer."""

from __future__ import annotations

import json
import os
import stat
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


@dataclass(frozen=True)
class _SecureResultBinding:
    canonical_root: Path
    node: str
    run_id: str
    root_fd: int
    run_dir_fd: int
    result_file_fd: int
    result_identity: tuple[int, int, int, int, int]
    result_digest: str
    config_digest: str

    @property
    def read_path(self) -> Path:
        return Path(f"/proc/self/fd/{self.result_file_fd}")

    @property
    def canonical_path(self) -> Path:
        return (
            self.canonical_root
            / "logs"
            / "job_logs"
            / self.node
            / self.run_id
            / "result.json"
        )

    def verify(self) -> None:
        """Revalidate every inherited and canonical identity without writing."""

        try:
            root = os.fstat(self.root_fd)
            canonical_root = os.stat(self.canonical_root, follow_symlinks=False)
            run_dir = os.fstat(self.run_dir_fd)
            named_run_dir = os.stat(
                self.canonical_path.parent,
                follow_symlinks=False,
            )
            descriptor_result = os.fstat(self.result_file_fd)
            named_result = os.stat(
                "result.json",
                dir_fd=self.run_dir_fd,
                follow_symlinks=False,
            )
            canonical_result = os.stat(
                self.canonical_path,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise ValueError("Validation result descriptor binding is unavailable") from exc
        if not stat.S_ISDIR(root.st_mode) or not stat.S_ISDIR(canonical_root.st_mode):
            raise ValueError("Validation root descriptor is not a directory")
        if _device_inode(root) != _device_inode(canonical_root):
            raise ValueError("Validation root descriptor identity changed")
        _require_private_directory(run_dir, "validation run directory")
        _require_private_directory(named_run_dir, "canonical validation run directory")
        if _device_inode(run_dir) != _device_inode(named_run_dir):
            raise ValueError("Validation result directory identity changed")
        for value in (descriptor_result, named_result, canonical_result):
            _require_private_regular_file(value, "validation result")
            if _immutable_file_identity(value) != self.result_identity:
                raise ValueError("Validation result identity or content changed")
        safe_existing_evidence_path(
            self.canonical_path,
            expected_path=self.canonical_path,
            allowed_root=self.canonical_root,
            expect_directory=False,
            description="validation result",
        )


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
    if not config_snapshot_b64:
        raise ValueError("Raw DB writes require an immutable config snapshot")
    if config_snapshot_b64 != encode_config_snapshot(active_config):
        raise ValueError("Raw DB config does not match its snapshot")
    secure_binding = _secure_result_binding(
        path,
        config=active_config,
        result_digest=result_digest,
    )
    if secure_binding is None:
        _prevalidate_historical_result_path(path, active_config)
        result = load_validation_result(path)
    else:
        secure_binding.verify()
        result = load_validation_result(secure_binding.read_path)
        secure_binding.verify()
    if result_digest != validation_result_digest(result):
        raise ValueError("Raw DB result does not match its digest")
    if isinstance(result, ValidationResultV2):
        if result.global_config_digest != effective_config_digest(active_config):
            raise ValueError("Current result config digest does not match snapshot")
    elif not isinstance(result, ValidationResult):  # pragma: no cover
        raise ValueError("Unsupported result schema")
    run_id = result.run_id if isinstance(result, ValidationResultV2) else f"{result.node}-{result.timestamp}"
    expected_path = (
        Path(active_config.runtime.validation_root)
        / "logs/job_logs"
        / result.node
        / run_id
        / "result.json"
    )
    _validate_result_path(
        path,
        expected_path,
        active_config,
        secure_binding=secure_binding,
    )
    for target in (
        active_config.storage.validation_db_path,
        active_config.storage.storage_db_path,
        active_config.storage.nccl_db_path,
        active_config.storage.dl_numerical_db_path,
        active_config.storage.dl_compute_db_path,
        active_config.storage.dl_collective_db_path,
        active_config.storage.dl_overlap_db_path,
    ):
        safe_writable_file_path(target)
    if secure_binding is not None:
        secure_binding.verify()
    return ResultWriteAuthorization(
        result=result,
        result_path=path,
        result_digest=result_digest,
        config=active_config,
        _nonce=_AUTHORIZATION_NONCE,
    )


def _validate_result_path(
    path: Path,
    expected_path: Path,
    config: CvalConfig,
    *,
    secure_binding: _SecureResultBinding | None = None,
) -> None:
    """Bind a canonical result or the secure supervisor's inherited FD path."""

    if secure_binding is None:
        if path != expected_path:
            raise ValueError("Validation result path is not canonical")
        safe_result_path = safe_writable_file_path(
            path,
            allowed_root=config.runtime.validation_root,
            description="validation result",
        )
        if safe_result_path != expected_path or not path.is_file():
            raise ValueError("Validation result path is not canonical")
        return
    if secure_binding.canonical_path != expected_path:
        raise ValueError("Validation result descriptor identity is not canonical")
    secure_binding.verify()


def _prevalidate_historical_result_path(path: Path, config: CvalConfig) -> None:
    """Preflight the retained pathname-based reader before parsing old evidence."""

    root = Path(config.runtime.validation_root)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError("Validation result path escapes validation root") from exc
    safe_writable_file_path(
        path,
        allowed_root=root,
        description="validation result",
    )
    if not path.is_file():
        raise ValueError("Validation result path is not a regular file")


def _secure_result_binding(
    path: Path,
    *,
    config: CvalConfig,
    result_digest: str,
) -> _SecureResultBinding | None:
    raw_layout = os.environ.get("CVAL_SECURE_RUN_LAYOUT_JSON")
    if not raw_layout:
        if str(path).startswith("/proc/self/fd/"):
            raise ValueError("Descriptor result path requires a secure run layout")
        return None
    try:
        layout = json.loads(raw_layout)
    except json.JSONDecodeError as exc:
        raise ValueError("Secure run layout is invalid JSON") from exc
    expected_fields = {
        "schema_version",
        "canonical_root",
        "node",
        "run_id",
        "root_fd",
        "run_dir_fd",
        "result_file_fd",
        "result_device",
        "result_inode",
        "result_size",
        "result_mtime_ns",
        "result_ctime_ns",
        "result_digest",
        "config_digest",
        "tests",
    }
    if not isinstance(layout, dict) or set(layout) != expected_fields:
        raise ValueError("Secure run layout has unexpected fields")
    if layout["schema_version"] != "cval.secure-run-layout.v1":
        raise ValueError("Unsupported secure run layout schema")
    canonical_root = Path(config.runtime.validation_root)
    if not canonical_root.is_absolute() or str(canonical_root) != layout["canonical_root"]:
        raise ValueError("Secure run layout canonical root mismatch")
    node = os.environ.get("GCRNODE")
    run_id = os.environ.get("CVAL_RUN_ID")
    if not node or not run_id or layout["node"] != node or layout["run_id"] != run_id:
        raise ValueError("Secure run layout run identity mismatch")
    integer_fields = (
        "root_fd",
        "run_dir_fd",
        "result_file_fd",
        "result_device",
        "result_inode",
        "result_size",
        "result_mtime_ns",
        "result_ctime_ns",
    )
    if any(
        isinstance(layout[name], bool)
        or not isinstance(layout[name], int)
        or layout[name] < 0
        for name in integer_fields
    ):
        raise ValueError("Secure run layout result binding is malformed")
    listed_fds = _secure_run_fds()
    expected_fds = {
        layout["root_fd"],
        layout["run_dir_fd"],
        layout["result_file_fd"],
    }
    tests = layout["tests"]
    if not isinstance(tests, dict):
        raise ValueError("Secure run layout tests are malformed")
    for test_id, values in tests.items():
        if not isinstance(test_id, str) or not isinstance(values, dict) or set(values) != {
            "log_dir_fd",
            "run_dir_fd",
            "artifacts_dir_fd",
        }:
            raise ValueError("Secure run layout test descriptors are malformed")
        for descriptor in values.values():
            if isinstance(descriptor, bool) or not isinstance(descriptor, int) or descriptor < 0:
                raise ValueError("Secure run layout test descriptor is malformed")
            expected_fds.add(descriptor)
    if listed_fds != expected_fds:
        raise ValueError("Secure result descriptors are not inherited run descriptors")
    expected_path = Path(f"/proc/self/fd/{layout['run_dir_fd']}/result.json")
    if path != expected_path:
        raise ValueError("Validation result descriptor path is invalid")
    config_digest = effective_config_digest(config)
    if (
        layout["result_digest"] != result_digest
        or layout["config_digest"] != config_digest
    ):
        raise ValueError("Secure result/config digest binding changed")
    binding = _SecureResultBinding(
        canonical_root=canonical_root,
        node=node,
        run_id=run_id,
        root_fd=layout["root_fd"],
        run_dir_fd=layout["run_dir_fd"],
        result_file_fd=layout["result_file_fd"],
        result_identity=(
            layout["result_device"],
            layout["result_inode"],
            layout["result_size"],
            layout["result_mtime_ns"],
            layout["result_ctime_ns"],
        ),
        result_digest=result_digest,
        config_digest=config_digest,
    )
    binding.verify()
    return binding


def _secure_run_fds() -> set[int]:
    raw = os.environ.get("CVAL_SECURE_RUN_FDS", "")
    if not raw:
        raise ValueError("CVAL_SECURE_RUN_FDS is required for secure ingestion")
    parts = raw.split(",")
    try:
        values = [int(part) for part in parts]
    except ValueError as exc:
        raise ValueError("CVAL_SECURE_RUN_FDS must contain integers") from exc
    if any(value < 0 for value in values) or len(values) != len(set(values)):
        raise ValueError("CVAL_SECURE_RUN_FDS is malformed")
    return set(values)


def _device_inode(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _immutable_file_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_private_directory(value: os.stat_result, name: str) -> None:
    if (
        not stat.S_ISDIR(value.st_mode)
        or stat.S_IMODE(value.st_mode) != 0o700
        or value.st_uid != os.geteuid()
    ):
        raise ValueError(f"Secure {name} must be an owner-only directory")


def _require_private_regular_file(value: os.stat_result, name: str) -> None:
    if (
        not stat.S_ISREG(value.st_mode)
        or stat.S_IMODE(value.st_mode) != 0o600
        or value.st_uid != os.geteuid()
        or value.st_nlink != 1
    ):
        raise ValueError(
            f"Secure {name} must be an owner-only single-link regular file"
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
            "Current SQLite writes require validated result provenance"
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


def validate_current_write(
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
    """Require a current raw write to match every immutable provenance field."""

    auth = require_result_write_authorization(authorization)
    _revalidate_authorized_result(auth)
    result = auth.result
    if node != result.node or _timestamp_epoch(timestamp) != _timestamp_epoch(
        result.timestamp
    ):
        raise ValueError("Current write identity does not match result evidence")
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
        raise ValueError("Current write versions do not match result evidence")

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
            raise ValueError("Current status does not match result evidence")
    elif operation == "validation-run":
        from cval.validation.builtins import project_builtin_statuses

        expected = project_builtin_statuses(projected)
        if results != expected:
            raise ValueError("Current status set does not match result evidence")
    elif operation in {"storage", "nccl"}:
        test_result = result.tests.get(operation)
        if test_result is None or test_result.status != "pass":
            raise ValueError(f"Current {operation} metrics require a passing result")
        if isinstance(result, ValidationResultV2) and evidence_path is not None:
            expected_evidence = (
                test_result.artifacts if operation == "storage" else test_result.summary
            )
            if Path(evidence_path) != Path(expected_evidence):
                raise ValueError(
                    f"Current {operation} path does not match result evidence"
                )
    else:
        raise ValueError(f"Unknown current write operation: {operation}")

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
            f"Current {operation} DB path does not match snapshot"
        )
    expected_run_id = (
        result.run_id
        if isinstance(result, ValidationResultV2)
        else f"{result.node}-{result.timestamp}"
    )
    if run_id and run_id != expected_run_id:
        raise ValueError("Current run ID does not match result evidence")
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
            description=f"current {operation} evidence",
        )
    return auth


def _revalidate_authorized_result(auth: ResultWriteAuthorization) -> None:
    """Rebind the result immediately before a low-level current DB write."""

    secure_binding = _secure_result_binding(
        auth.result_path,
        config=auth.config,
        result_digest=auth.result_digest,
    )
    if secure_binding is None:
        _prevalidate_historical_result_path(auth.result_path, auth.config)
        current = load_validation_result(auth.result_path)
    else:
        secure_binding.verify()
        current = load_validation_result(secure_binding.read_path)
        secure_binding.verify()
    if (
        current != auth.result
        or validation_result_digest(current) != auth.result_digest
        or (
            isinstance(current, ValidationResultV2)
            and current.global_config_digest != effective_config_digest(auth.config)
        )
    ):
        raise ValueError("Authorized validation result changed before DB write")


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


