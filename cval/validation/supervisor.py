"""Descriptor-anchored startup and supervision for one in-pod validation run."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import signal
import stat
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Callable, Mapping, Sequence

from cval.config import CvalConfig, load_config_snapshot
from cval.validation.execution import EVENT_SCHEMA_VERSION, event_payload, process_group_exists, terminate_process_group, utc_now
from cval.validation.results import (
    RUN_ID_PATTERN as SAFE_SEGMENT,
    ValidationResultV2,
    load_validation_result,
    parse_validation_result_v2,
    result_payload,
    validation_result_digest,
)
from cval.validation.secure_fs import write_file_at, rename_noreplace_at, read_regular_file_at
from cval.validation.runtime import build_runtime_environment, effective_config_digest, test_evidence_environment


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(
    os, "O_NOFOLLOW", 0
)
_FILE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(
    os, "O_NOFOLLOW", 0
)
_PARENT_MODE = 0o750
_RUN_MODE = 0o700
_FILE_MODE = 0o600
_LAYOUT_VERSION = "cval.secure-run-layout.v2"
_GLOBAL_FILES = ("job.log", ".run-active")
_RESULT_SEALS = fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL


@dataclass(frozen=True)
class _DirectoryIdentity:
    relative: tuple[str, ...]
    descriptor: int
    device: int
    inode: int


@dataclass
class SecureRunLayout:
    """Open descriptors anchoring every framework-owned run directory."""

    canonical_root: Path
    node: str
    run_id: str
    root_fd: int
    run_dir_fd: int
    test_fds: dict[str, tuple[int, int, int]]
    identities: tuple[_DirectoryIdentity, ...]
    global_file_fds: dict[str, int]
    owned_fds: list[int]
    result_file_fd: int | None = None
    result_device: int | None = None
    result_inode: int | None = None
    result_size: int | None = None
    result_mtime_ns: int | None = None
    result_ctime_ns: int | None = None
    result_digest: str | None = None
    config_digest: str | None = None

    @property
    def inherited_fds(self) -> tuple[int, ...]:
        descriptors = {self.root_fd, self.run_dir_fd}
        for values in self.test_fds.values():
            descriptors.update(values)
        if self.result_file_fd is not None:
            descriptors.add(self.result_file_fd)
        return tuple(sorted(descriptors))

    def environment(self) -> dict[str, str]:
        tests = {
            test_id: {
                "log_dir_fd": values[0],
                "run_dir_fd": values[1],
                "artifacts_dir_fd": values[2],
            }
            for test_id, values in sorted(self.test_fds.items())
        }
        payload = {
            "schema_version": _LAYOUT_VERSION,
            "canonical_root": str(self.canonical_root),
            "node": self.node,
            "run_id": self.run_id,
            "root_fd": self.root_fd,
            "run_dir_fd": self.run_dir_fd,
            "result_file_fd": self.result_file_fd,
            "result_device": self.result_device,
            "result_inode": self.result_inode,
            "result_size": self.result_size,
            "result_mtime_ns": self.result_mtime_ns,
            "result_ctime_ns": self.result_ctime_ns,
            "result_digest": self.result_digest,
            "config_digest": self.config_digest,
            "tests": tests,
        }
        anchored_run = _fd_path(self.run_dir_fd)
        values = {
            "CVAL_SECURE_RUN_LAYOUT_JSON": json.dumps(
                payload, sort_keys=True, separators=(",", ":")
            ),
            "CVAL_SECURE_RUN_FDS": ",".join(
                str(descriptor) for descriptor in self.inherited_fds
            ),
            "CVAL_EXTERNAL_GLOBAL_LOGGING": "true",
            "CVAL_RUN_MARKER_PREACQUIRED": "true",
            "CVAL_JOB_LOG_DIR": anchored_run,
            "CVAL_RESULT_DIR": anchored_run,
            "CVAL_RESULT_JSON_FILE": _fd_path(self.result_file_fd),
            "CVAL_CANONICAL_JOB_LOG_DIR": str(self.canonical_root / "logs/job_logs" / self.node / self.run_id),
            "CVAL_CANONICAL_RESULT_JSON_FILE": str(self.canonical_root / "logs/job_logs" / self.node / self.run_id / "result.json"),
        }
        for test_id, (_log_fd, run_fd, artifacts_fd) in self.test_fds.items():
            prefix = test_id.upper().replace("-", "_")
            values[f"CVAL_SECURE_{prefix}_RUN_DIR"] = _fd_path(run_fd)
            values[f"CVAL_SECURE_{prefix}_ARTIFACTS_DIR"] = _fd_path(artifacts_fd)
        return values

    def verify(self) -> None:
        """Fail if any canonical directory name no longer reaches its retained fd."""

        root_check = _open_absolute_directory(self.canonical_root)
        try:
            _require_same_directory(self.root_fd, root_check, "validation root")
        finally:
            os.close(root_check)
        for identity in self.identities:
            check = _open_relative_directory(self.root_fd, identity.relative)
            try:
                current = os.fstat(check)
                retained = os.fstat(identity.descriptor)
                if (
                    current.st_dev != identity.device
                    or current.st_ino != identity.inode
                    or retained.st_dev != identity.device
                    or retained.st_ino != identity.inode
                ):
                    raise RuntimeError(
                        "Run evidence directory identity changed: "
                        + "/".join(identity.relative)
                    )
            finally:
                os.close(check)
        if self.result_digest is not None:
            self._verify_bound_result()

    def bind_result(
        self,
        *,
        config_snapshot_b64: str,
        config_digest: str,
        repo_root: Path | None,
    ) -> None:
        """Open and bind the runner's final result before starting ingestion."""

        if self.result_digest is not None:
            raise RuntimeError("Secure validation result is already bound")
        if not config_snapshot_b64:
            raise ValueError("Secure ingestion requires an immutable config snapshot")
        config = load_config_snapshot(config_snapshot_b64, repo_root=repo_root)
        actual_config_digest = effective_config_digest(config)
        if config_digest != actual_config_digest:
            raise ValueError("Secure ingestion config digest does not match snapshot")
        self.verify()
        descriptor = self.result_file_fd
        result_stat = os.fstat(descriptor)
        _require_private_regular_file(result_stat, "validation result", links=0)
        if result_stat.st_size > config.monitoring.verification_max_bytes:
            raise ValueError("Validation result exceeds the evidence byte limit")
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS, _RESULT_SEALS)
        result = load_validation_result(Path(_fd_path(descriptor)))
        if not isinstance(result, ValidationResultV2) or result.schema_version != "cval.results.v3":
            raise ValueError("Secure supervisor requires the compact result schema")
        if result.node != self.node or result.run_id != self.run_id:
            raise ValueError("Secure validation result identity is not canonical")
        if result.global_config_digest != actual_config_digest:
            raise ValueError("Secure validation result config digest changed")
        result_stat = os.fstat(descriptor)
        self.result_device = result_stat.st_dev
        self.result_inode = result_stat.st_ino
        self.result_size = result_stat.st_size
        self.result_mtime_ns = result_stat.st_mtime_ns
        self.result_ctime_ns = result_stat.st_ctime_ns
        self.result_digest = validation_result_digest(result)
        self.config_digest = actual_config_digest
        self._verify_bound_result()

    def _verify_bound_result(self) -> None:
        if self.result_file_fd is None:
            raise RuntimeError("Secure validation result is not bound")
        retained = os.fstat(self.result_file_fd)
        _require_private_regular_file(retained, "validation result", links=0)
        if fcntl.fcntl(self.result_file_fd, fcntl.F_GET_SEALS) != _RESULT_SEALS:
            raise RuntimeError("Validation result buffer is not immutable")
        expected = (
            self.result_device,
            self.result_inode,
            self.result_size,
            self.result_mtime_ns,
            self.result_ctime_ns,
        )
        actual = (retained.st_dev, retained.st_ino, retained.st_size, retained.st_mtime_ns, retained.st_ctime_ns)
        if actual != expected:
            raise RuntimeError("Secure validation result identity or content changed")

    def release_marker(self) -> None:
        self.verify()
        os.unlink(".run-active", dir_fd=self.run_dir_fd)
        os.fsync(self.run_dir_fd)

    def close(self) -> None:
        for descriptor in reversed(self.owned_fds):
            try:
                os.close(descriptor)
            except OSError:
                pass
        self.owned_fds.clear()


def reserve_secure_run_layout(
    validation_root: str | Path,
    node: str,
    run_id: str,
    *,
    registry_json: str,
    directory_observer: Callable[[tuple[str, ...], int], None] | None = None,
) -> SecureRunLayout:
    """Create one run tree exclusively through retained ``openat`` descriptors."""

    for field_name, value in (("node", node), ("run_id", run_id)):
        if value in {".", ".."} or not SAFE_SEGMENT.fullmatch(value):
            raise ValueError(f"Invalid {field_name} path segment: {value!r}")
    root = Path(validation_root).expanduser()
    if not root.is_absolute():
        raise ValueError("validation root must be absolute")
    root = Path(os.path.abspath(root))
    test_ids = _registry_test_ids(registry_json)
    enabled = _enabled_test_ids(registry_json, test_ids)
    owned: list[int] = []
    identities: list[_DirectoryIdentity] = []
    global_files: dict[str, int] = {}
    try:
        root_fd = _open_absolute_directory(root)
        owned.append(root_fd)
        run_relative = ("logs", "job_logs", node, run_id)
        run_fd = _mkdirs_open(
            root_fd,
            run_relative,
            final_exclusive=True,
            owned=owned,
            observer=directory_observer,
        )
        identities.append(_identity(run_relative, run_fd))
        for name in _GLOBAL_FILES:
            descriptor = _create_file(run_fd, name)
            owned.append(descriptor)
            global_files[name] = descriptor
        os.write(
            global_files[".run-active"],
            f"pid={os.getpid()}\n".encode("utf-8"),
        )
        os.fsync(global_files[".run-active"])
        os.fsync(run_fd)
        result_fd = os.memfd_create("cval-result", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        os.fchmod(result_fd, _FILE_MODE)
        owned.append(result_fd)

        test_fds: dict[str, tuple[int, int, int]] = {}
        for test_id in enabled:
            log_relative = ("logs", test_id, node, run_id)
            test_log_fd = _mkdirs_open(
                root_fd,
                log_relative,
                final_exclusive=True,
                owned=owned,
                observer=directory_observer,
            )
            identities.append(_identity(log_relative, test_log_fd))
            owned.append(_create_file(test_log_fd, "job.log"))
            test_fds[test_id] = (test_log_fd, test_log_fd, test_log_fd)

        layout = SecureRunLayout(
            canonical_root=root,
            node=node,
            run_id=run_id,
            root_fd=root_fd,
            run_dir_fd=run_fd,
            test_fds=test_fds,
            identities=tuple(identities),
            global_file_fds=global_files,
            owned_fds=owned,
            result_file_fd=result_fd,
        )
        layout.verify()
        return layout
    except BaseException:
        for descriptor in reversed(owned):
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _emit_supervisor_event(layout: SecureRunLayout, console: BinaryIO, event: str, **fields) -> None:
    """Append between child lifetimes through the reserved evidence descriptors."""

    layout.verify()
    payload = event_payload(layout.run_id, event, node_name=layout.node, **fields)
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    line = f"\nCVAL_EVENT {serialized}\n".encode()
    for name in ("job.log",):
        descriptor = layout.global_file_fds[name]
        _require_private_regular_file(os.fstat(descriptor), name)
        named = os.stat(name, dir_fd=layout.run_dir_fd, follow_symlinks=False)
        _require_stat_identity(named, os.fstat(descriptor), name)
        os.lseek(descriptor, 0, os.SEEK_END)
        _write_all(descriptor, line)
        os.fsync(descriptor)
    console.write(line)
    console.flush()


def _ingestion_acknowledgement(layout: SecureRunLayout, config: CvalConfig, result: ValidationResultV2) -> dict | None:
    """Read the ingestion owner's last acknowledgement, never infer it from exit zero."""

    descriptor = os.open("job.log", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=layout.run_dir_fd)
    with os.fdopen(descriptor, "rb") as handle:
        _require_private_regular_file(os.fstat(handle.fileno()), "job.log")
        _require_stat_identity(os.fstat(handle.fileno()), os.fstat(layout.global_file_fds["job.log"]), "job.log")
        max_bytes = config.monitoring.verification_max_bytes
        if os.fstat(handle.fileno()).st_size > max_bytes:
            raise ValueError("Ingestion acknowledgement exceeds configured evidence read limit")
        content = handle.read(max_bytes + 1)
    if len(content) > max_bytes:
        raise ValueError("Ingestion acknowledgement exceeds configured evidence read limit")
    acknowledgement = None
    for line in content.splitlines():
        if not line.startswith(b"CVAL_EVENT "):
            continue
        record = json.loads(line.removeprefix(b"CVAL_EVENT "))
        if record.get("schema_version") != EVENT_SCHEMA_VERSION or record.get("run_id") != layout.run_id:
            raise ValueError("Ingestion event identity mismatch")
        if record.get("event") == "ingestion_started":
            acknowledgement = None
        elif record.get("event") == "ingestion_finished":
            if record.get("producer") != "ingestion" or record.get("node_name") != layout.node or record.get("result_digest") != layout.result_digest or record.get("config_digest") != layout.config_digest:
                raise ValueError("Ingestion acknowledgement digest mismatch")
            stores = record.get("stores")
            owners = {"storage": "storage", "nccl": "nccl", "numerical_correctness": "dltest",
                      "compute_performance": "dltest", "collective_performance": "dltest", "overlap_performance": "dltest"}
            if not isinstance(stores, dict) or set(stores) != set(owners) | {"validation"} or record.get("status") not in {"pass", "fail"}:
                raise ValueError("Incomplete ingestion acknowledgement")
            if any(value not in {"pending", "not_required", "committed", "unconfirmed"} for value in stores.values()):
                raise ValueError("Unknown ingestion store outcome")
            if record["status"] == "pass":
                expected = {name: "committed" if owner in result.tests and result.tests[owner].enabled and result.tests[owner].selected and result.tests[owner].status == "pass"
                            else "not_required" for name, owner in owners.items()} | {"validation": "committed"}
                if stores != expected:
                    raise ValueError("Ingestion success lacks required store acknowledgements")
            acknowledgement = record
    return acknowledgement


def _supervised_outcome(result: ValidationResultV2 | None, exit_code: int, ingestion: str) -> str:
    if exit_code != 0 or (result is not None and result.overall == "fail") or ingestion == "fail":
        return "fail"
    if result is not None and not result.errors and result.overall == "pass" and ingestion == "pass":
        return "pass"
    return "incomplete"


def _file_states(layout: SecureRunLayout, result: ValidationResultV2, config: CvalConfig) -> dict[str, str]:
    states = {}
    for name, test in result.tests.items():
        states[name] = "not_required" if not test.selected else "incomplete"
        if not test.selected:
            continue
        descriptor = layout.test_fds[name][0]
        try:
            read_regular_file_at(descriptor, "job.log", max_bytes=config.monitoring.verification_max_bytes)
            log_descriptor = os.open("job.log", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
            try:
                os.fsync(log_descriptor)
            finally:
                os.close(log_descriptor)
            if test.status == "pass":
                summary = config.tests.registry.require(name).definition.artifacts.summary_filename
                if not read_regular_file_at(descriptor, summary, max_bytes=config.monitoring.verification_max_bytes):
                    continue
            states[name] = "complete"
        except (OSError, ValueError):
            pass
    return states


def _publish_final_result(layout: SecureRunLayout, result: ValidationResultV2, completion: dict) -> None:
    payload = result_payload(result) | {"completion": completion}
    parse_validation_result_v2(payload)
    layout.verify()
    temporary = f".result-{os.getpid()}.tmp"
    write_file_at(layout.run_dir_fd, temporary, (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode(), 0o600)
    try:
        rename_noreplace_at(layout.run_dir_fd, temporary, layout.run_dir_fd, "result.json")
    except OSError as error:
        if error.errno not in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
            raise
        layout.verify()
        os.link(temporary, "result.json", src_dir_fd=layout.run_dir_fd,
                dst_dir_fd=layout.run_dir_fd, follow_symlinks=False)
        os.unlink(temporary, dir_fd=layout.run_dir_fd)
    os.fsync(layout.run_dir_fd)


def supervise_validation_run(
    *,
    environment: Mapping[str, str],
    runner_command: Sequence[str],
    db_update_command: Sequence[str] | None,
    validation_tests_dir: str | Path,
    stdout: BinaryIO | None = None,
    stderr: BinaryIO | None = None,
) -> int:
    """Capture one run, authorize ingestion and publish its final receipt last."""

    runtime_env = dict(environment)
    sensitive_fragments = (
        "PASSWORD",
        "SECRET",
        "TOKEN",
        "CREDENTIAL",
        "PRIVATE_KEY",
        "API_KEY",
    )
    for name in tuple(runtime_env):
        upper = name.upper()
        if (
            upper == "DATABASE_URL"
            or any(fragment in upper for fragment in sensitive_fragments)
        ):
            runtime_env.pop(name, None)
    required = ("CVAL_VALIDATION_ROOT", "GCRNODE", "CVAL_RUN_ID", "CVAL_TEST_REGISTRY_JSON")
    missing = [name for name in required if not runtime_env.get(name)]
    if missing:
        raise ValueError("Missing secure supervisor environment: " + ", ".join(missing))
    config = load_config_snapshot(
        runtime_env["CVAL_CONFIG_SNAPSHOT_B64"],
        repo_root=Path(runtime_env["CVAL_REPO_DIR"]),
    )
    for name, expected in build_runtime_environment(config).items():
        if runtime_env.get(name) != expected:
            raise ValueError(f"{name} disagrees with the immutable config snapshot")
    if str(validation_tests_dir) != config.runtime.validation_tests_dir:
        raise ValueError("Validation test directory disagrees with the config snapshot")
    if runtime_env.get("CVAL_NODE", runtime_env["GCRNODE"]) != runtime_env["GCRNODE"] or runtime_env.get("CVAL_TIMESTAMP", runtime_env["GCRTIME"]) != runtime_env["GCRTIME"]:
        raise ValueError("Runtime node/timestamp aliases disagree")
    layout = reserve_secure_run_layout(
        runtime_env["CVAL_VALIDATION_ROOT"],
        runtime_env["GCRNODE"],
        runtime_env["CVAL_RUN_ID"],
        registry_json=runtime_env["CVAL_TEST_REGISTRY_JSON"],
    )
    canonical_run = (
        layout.canonical_root
        / "logs"
        / "job_logs"
        / runtime_env["GCRNODE"]
        / runtime_env["CVAL_RUN_ID"]
    )
    try:
        _add_builtin_compatibility_paths(runtime_env, layout, config)
        runtime_env.update(layout.environment())
    except BaseException:
        layout.close()
        raise
    repo_dir = runtime_env.get("CVAL_REPO_DIR")
    if repo_dir:
        existing_pythonpath = runtime_env.get("PYTHONPATH", "")
        runtime_env["PYTHONPATH"] = (
            repo_dir
            if not existing_pythonpath
            else f"{repo_dir}{os.pathsep}{existing_pythonpath}"
        )
    stdout = stdout or sys.stdout.buffer
    stderr = stderr or sys.stderr.buffer
    previous_handlers: dict[signal.Signals, object] = {}
    active: list[subprocess.Popen[bytes] | None] = [None]
    started = time.monotonic()
    stage = "startup"
    result: ValidationResultV2 | None = None
    runner_exit: int | None = None
    ingestion_exit: int | None = None
    acknowledgement: dict | None = None
    ingestion = "not_started" if db_update_command is not None else "not_requested"
    log_capture_complete = True
    exit_code = 1
    message = ""

    def handle_signal(signum: int, _frame: object) -> None:
        process = active[0]
        if process is not None:
            terminate_process_group(process.pid, process, grace_seconds=2.0)
        raise KeyboardInterrupt(signum)

    if threading.current_thread() is threading.main_thread():
        for signal_name in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signal_name] = signal.getsignal(signal_name)
            signal.signal(signal_name, handle_signal)
    try:
        _emit_supervisor_event(layout, stdout, "supervisor_started", status="incomplete", producer="supervisor",
                               git_ref=config.job.git_ref, config_digest=effective_config_digest(config),
                               tests=[test.id for test in config.tests.registry.enabled],
                               log_dir=str(canonical_run), result_path=str(canonical_run / "result.json"))
        _write_all(layout.global_file_fds["job.log"], b"test started.\n")
        for index, command in enumerate((runner_command, db_update_command)):
            if index == 1:
                stage = "result_finalization"
                repo_root_value = runtime_env.get("CVAL_TEST_REPO_ROOT") or repo_dir
                layout.bind_result(
                    config_snapshot_b64=runtime_env.get(
                        "CVAL_CONFIG_SNAPSHOT_B64", ""
                    ),
                    config_digest=runtime_env.get("CVAL_CONFIG_DIGEST", ""),
                    repo_root=(
                        None if not repo_root_value else Path(repo_root_value)
                    ),
                )
                runtime_env.update(layout.environment())
                result = load_validation_result(Path(f"/proc/self/fd/{layout.result_file_fd}"))
                _emit_supervisor_event(layout, stdout, "result_finalized", status=result.overall, producer="supervisor",
                                       result_path=str(canonical_run / "result.json"), result_digest=layout.result_digest)
            if command is None:
                continue
            layout.verify()
            stage = "runner" if index == 0 else "ingestion"
            child_started = time.monotonic()
            if index == 1:
                ingestion = "unconfirmed"
            log_capture_complete = False
            return_code = _run_child(
                command,
                cwd=Path(validation_tests_dir),
                environment=runtime_env,
                pass_fds=layout.inherited_fds,
                stdout=stdout,
                stderr=stderr,
                job_log_fd=layout.global_file_fds["job.log"],
                active=active,
                timeout_seconds=config.runtime.runner_timeout_seconds if index == 0 else config.runtime.ingestion_timeout_seconds,
            )
            log_capture_complete = True
            if index == 0:
                runner_exit = return_code
            else:
                ingestion_exit = return_code
            layout.verify()
            if index == 1:
                acknowledgement = _ingestion_acknowledgement(layout, config, result)
                if acknowledgement is not None:
                    ingestion = acknowledgement["status"]
            _emit_supervisor_event(layout, stdout, "supervisor_stage_finished", producer="supervisor", stage=stage,
                                   status="pass" if return_code == 0 else "fail", exit_code=return_code,
                                   duration_ms=max(0, int((time.monotonic() - child_started) * 1000)))
            if return_code != 0:
                exit_code = return_code
                message = f"{stage} child exited with code {return_code}"
                return exit_code
            if index == 1 and ingestion != "pass":
                raise RuntimeError("Ingestion child exited zero without a matching successful acknowledgement")
        stage = "log_finalization"
        if any(value == "incomplete" for value in _file_states(layout, result, config).values()):
            raise RuntimeError("Required test log or summary is missing")
        _write_all(layout.global_file_fds["job.log"], b"Completed.\n")
        layout.release_marker()
        stage = "finished"
        exit_code = 0
        return 0
    except KeyboardInterrupt:
        exit_code = 143
        message = "Supervisor interrupted; available evidence retained"
        return exit_code
    except Exception as error:
        message = str(error).splitlines()[0] if str(error) else type(error).__name__
        raise
    finally:
        for signal_name, handler in previous_handlers.items():
            signal.signal(signal_name, handler)
        try:
            layout.verify()
            os.fsync(layout.global_file_fds["job.log"])
            if result is None:
                try:
                    result = load_validation_result(Path(_fd_path(layout.result_file_fd)))
                except (OSError, ValueError):
                    pass
            _emit_supervisor_event(layout, stdout, "supervisor_finished", producer="supervisor",
                                   status=_supervised_outcome(result, exit_code, ingestion), message=message, stage=stage,
                                   duration_ms=max(0, int((time.monotonic() - started) * 1000)), exit_code=exit_code,
                                   runner_exit_code=runner_exit, ingestion_exit_code=ingestion_exit,
                                   tests_outcome=result.overall if result is not None else "unconfirmed",
                                   tests={name: {"status": test.status, "phase": test.phase} for name, test in result.tests.items()} if result is not None else {},
                                   result_state="finalized" if result is not None else "unconfirmed",
                                   result_path=str(canonical_run / "result.json"), result_digest=layout.result_digest,
                                   logs_state="retained" if log_capture_complete else "unconfirmed",
                                   log_dir=str(canonical_run), ingestion_outcome=ingestion,
                                   stores=acknowledgement["stores"] if acknowledgement is not None else {})
            if isinstance(result, ValidationResultV2):
                _publish_final_result(layout, result, {
                    "finished_at": utc_now(),
                    "exit_code": exit_code, "logs": "retained" if log_capture_complete else "unconfirmed",
                    "files": _file_states(layout, result, config), "ingestion": ingestion,
                    "stores": acknowledgement["stores"] if acknowledgement else {},
                    "result_digest": validation_result_digest(result),
                })
        finally:
            layout.close()


def main() -> int:
    environment = dict(os.environ)
    validation_tests_dir = environment.get("CVAL_VALIDATION_TESTS_DIR")
    if not validation_tests_dir:
        print("CVAL_VALIDATION_TESTS_DIR is required", file=sys.stderr)
        return 1
    try:
        return supervise_validation_run(
            environment=environment,
            runner_command=(sys.executable, "-m", "cval.validation.runner"),
            db_update_command=(sys.executable, "-m", "cval.storage.run_ingest"),
            validation_tests_dir=validation_tests_dir,
        )
    except Exception as exc:  # noqa: BLE001 - top-level pod boundary
        print(f"c-val secure supervisor failed: {exc}", file=sys.stderr)
        return 1


def _registry_test_ids(payload: str) -> tuple[str, ...]:
    if not payload:
        raise ValueError("runtime test registry is required")
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("runtime test registry is invalid JSON") from exc
    if not isinstance(data, dict) or not data:
        raise ValueError("runtime test registry must be a non-empty object")
    for test_id in data:
        if not isinstance(test_id, str) or not SAFE_SEGMENT.fullmatch(test_id):
            raise ValueError(f"Invalid test ID path segment: {test_id!r}")
    return tuple(sorted(data))


def _enabled_test_ids(payload: str, test_ids: tuple[str, ...]) -> tuple[str, ...]:
    data = json.loads(payload)
    enabled: list[str] = []
    for test_id in test_ids:
        registration = data[test_id]
        if not isinstance(registration, dict):
            raise ValueError(f"Runtime registry test {test_id!r} must be an object")
        value = registration.get("enabled")
        if not isinstance(value, bool):
            raise ValueError(f"Runtime registry enabled for {test_id!r} must be boolean")
        if value:
            enabled.append(test_id)
    return tuple(enabled)


def _open_absolute_directory(path: Path) -> int:
    if not path.is_absolute():
        raise ValueError("directory path must be absolute")
    current = os.open("/", _DIRECTORY_FLAGS)
    try:
        for part in path.parts[1:]:
            next_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            os.close(current)
            current = next_fd
        return current
    except BaseException:
        os.close(current)
        raise


def _open_relative_directory(root_fd: int, relative: tuple[str, ...]) -> int:
    current = os.dup(root_fd)
    try:
        for part in relative:
            next_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            os.close(current)
            current = next_fd
        return current
    except BaseException:
        os.close(current)
        raise


def _mkdirs_open(
    root_fd: int,
    relative: tuple[str, ...],
    *,
    final_exclusive: bool,
    owned: list[int],
    observer: Callable[[tuple[str, ...], int], None] | None,
) -> int:
    current = os.dup(root_fd)
    traversed: list[str] = []
    try:
        for index, part in enumerate(relative):
            if part in {"", ".", ".."} or "/" in part:
                raise ValueError(f"Unsafe run evidence path component: {part!r}")
            final = index == len(relative) - 1
            created = False
            try:
                os.mkdir(
                    part,
                    _RUN_MODE if final else _PARENT_MODE,
                    dir_fd=current,
                )
                created = True
            except FileExistsError:
                if final and final_exclusive:
                    raise FileExistsError(
                        "Run evidence already exists; refusing run_id reuse: "
                        + "/".join(relative)
                    )
            next_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=current)
            if created:
                os.fchmod(next_fd, _RUN_MODE if final else _PARENT_MODE)
                created_stat = os.fstat(next_fd)
                if created_stat.st_uid != os.geteuid():
                    raise PermissionError("Created run evidence directory owner changed")
            named_stat = os.stat(part, dir_fd=current, follow_symlinks=False)
            _require_stat_identity(named_stat, os.fstat(next_fd), part)
            os.close(current)
            current = next_fd
            traversed.append(part)
            if observer is not None:
                observer(tuple(traversed), current)
        owned.append(current)
        return current
    except BaseException:
        os.close(current)
        raise


def _create_file(directory_fd: int, name: str) -> int:
    descriptor = os.open(name, _FILE_FLAGS, _FILE_MODE, dir_fd=directory_fd)
    os.fchmod(descriptor, _FILE_MODE)
    value = os.fstat(descriptor)
    if not stat.S_ISREG(value.st_mode) or value.st_uid != os.geteuid():
        os.close(descriptor)
        raise PermissionError(f"Reserved run evidence file is unsafe: {name}")
    named = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    _require_stat_identity(named, value, name)
    return descriptor


def _identity(relative: tuple[str, ...], descriptor: int) -> _DirectoryIdentity:
    value = os.fstat(descriptor)
    return _DirectoryIdentity(relative, descriptor, value.st_dev, value.st_ino)


def _require_same_directory(left: int, right: int, name: str) -> None:
    first = os.fstat(left)
    second = os.fstat(right)
    if (first.st_dev, first.st_ino) != (second.st_dev, second.st_ino):
        raise RuntimeError(f"{name} identity changed")


def _require_stat_identity(named: os.stat_result, opened: os.stat_result, name: str) -> None:
    if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
        raise RuntimeError(f"Directory entry identity changed while opening: {name}")


def _require_private_regular_file(value: os.stat_result, name: str, *, links: int = 1) -> None:
    if (
        not stat.S_ISREG(value.st_mode)
        or stat.S_IMODE(value.st_mode) != _FILE_MODE
        or value.st_uid != os.geteuid()
        or value.st_nlink != links
    ):
        raise PermissionError(
            f"Secure {name} must be an owner-only single-link regular file"
        )


def _fd_path(descriptor: int) -> str:
    path = f"/proc/self/fd/{descriptor}"
    if not os.path.exists(path):
        raise RuntimeError("/proc/self/fd is required for secure run evidence paths")
    return path


def _add_builtin_compatibility_paths(
    environment: dict[str, str], layout: SecureRunLayout, config: CvalConfig
) -> None:
    node = environment["GCRNODE"]
    run_id = environment["CVAL_RUN_ID"]
    canonical_root = layout.canonical_root
    canonical_run = canonical_root / "logs/job_logs" / node / run_id
    values = {name: value for name, value in layout.environment().items()
              if name in {"CVAL_JOB_LOG_DIR", "CVAL_RESULT_DIR", "CVAL_RESULT_JSON_FILE",
                          "CVAL_CANONICAL_JOB_LOG_DIR", "CVAL_CANONICAL_RESULT_JSON_FILE"}}
    canonical_values = {"CVAL_JOB_LOG_DIR": str(canonical_run), "CVAL_RESULT_DIR": str(canonical_run),
                        "CVAL_RESULT_JSON_FILE": str(canonical_run / "result.json")}
    for test_id, (log_fd, run_fd, artifacts_fd) in layout.test_fds.items():
        test = config.tests.registry.require(test_id)
        canonical_run = canonical_root / "logs" / test_id / node / run_id
        values.update(test_evidence_environment(
            test, log_dir=Path(_fd_path(log_fd)), run_dir=Path(_fd_path(run_fd)),
            artifacts=Path(_fd_path(artifacts_fd)), node=node, timestamp=int(environment["GCRTIME"]),
            validation_root=canonical_root, run_id=run_id,
        ))
        canonical_values.update(test_evidence_environment(
            test, log_dir=canonical_root / "logs" / test_id / node / run_id, run_dir=canonical_run,
            artifacts=canonical_run, node=node, timestamp=int(environment["GCRTIME"]),
            validation_root=canonical_root, run_id=run_id,
        ))
    for name, value in values.items():
        if name in environment and environment[name] not in {value, canonical_values.get(name, value)}:
            raise ValueError(f"{name} disagrees with the reserved run evidence")
    environment.update(values)


def _run_child(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    pass_fds: tuple[int, ...],
    stdout: BinaryIO,
    stderr: BinaryIO,
    job_log_fd: int,
    active: list[subprocess.Popen[bytes] | None],
    timeout_seconds: float,
) -> int:
    if timeout_seconds <= 0:
        raise ValueError("Supervised child timeout must be positive")
    deadline = time.monotonic() + timeout_seconds
    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=dict(environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        pass_fds=pass_fds,
        start_new_session=True,
    )
    active[0] = process
    assert process.stdout is not None
    errors: list[BaseException] = []

    def pump() -> None:
        try:
            while True:
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    return
                _write_all(job_log_fd, chunk)
                stdout.write(chunk)
                stdout.flush()
        except BaseException as exc:  # noqa: BLE001 - propagated after join
            errors.append(exc)

    thread = threading.Thread(target=pump, daemon=True)
    thread.start()
    try:
        return_code = process.wait(timeout=max(0.001, deadline - time.monotonic()))
        if process_group_exists(process.pid):
            raise RuntimeError("supervised child left running descendant processes")
    except BaseException:
        terminate_process_group(process.pid, process, grace_seconds=2.0)
        raise
    finally:
        active[0] = None
        thread.join(timeout=5.0)
        process.stdout.close()
        if thread.is_alive():
            raise RuntimeError("supervised child log streams did not close")
        if errors:
            raise RuntimeError(f"supervised child log capture failed: {errors[0]}")
    return return_code


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("Short write to job log")
        view = view[written:]


if __name__ == "__main__":
    raise SystemExit(main())