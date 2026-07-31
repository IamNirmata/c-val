"""Descriptor-anchored startup and supervision for one in-pod validation run."""

from __future__ import annotations

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

from cval.validation.compatibility import LEGACY_TEST_IDS
from cval.validation.path_preflight import SAFE_SEGMENT, _registry_test_ids


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(
    os, "O_NOFOLLOW", 0
)
_FILE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(
    os, "O_NOFOLLOW", 0
)
_PARENT_MODE = 0o750
_RUN_MODE = 0o700
_FILE_MODE = 0o600
_LAYOUT_VERSION = "cval.secure-run-layout.v1"
_GLOBAL_FILES = (
    "stdout.log",
    "stderr.log",
    "job.log",
    "events.jsonl",
    "result.json",
    "result.env",
    ".run-active",
)
_TEST_LOG_FILES = ("stdout.log", "stderr.log", "events.jsonl", "workload.log")


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
    root_fd: int
    run_dir_fd: int
    test_fds: dict[str, tuple[int, int, int]]
    identities: tuple[_DirectoryIdentity, ...]
    global_file_fds: dict[str, int]
    owned_fds: list[int]

    @property
    def inherited_fds(self) -> tuple[int, ...]:
        descriptors = {self.root_fd, self.run_dir_fd}
        for values in self.test_fds.values():
            descriptors.update(values)
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
            "root_fd": self.root_fd,
            "run_dir_fd": self.run_dir_fd,
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
            "CVAL_RESULT_JSON_FILE": f"{anchored_run}/result.json",
            "CVAL_RESULT_ENV_FILE": f"{anchored_run}/result.env",
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
            for name in _TEST_LOG_FILES:
                owned.append(_create_file(test_log_fd, name))

            run_relative = (
                "validation_tests",
                test_id,
                "runs",
                node,
                run_id,
            )
            test_run_fd = _mkdirs_open(
                root_fd,
                run_relative,
                final_exclusive=True,
                owned=owned,
                observer=directory_observer,
            )
            identities.append(_identity(run_relative, test_run_fd))
            artifacts_fd = _mkdirs_open(
                test_run_fd,
                ("artifacts",),
                final_exclusive=True,
                owned=owned,
                observer=None,
            )
            identities.append(_identity(run_relative + ("artifacts",), artifacts_fd))
            test_fds[test_id] = (test_log_fd, test_run_fd, artifacts_fd)

        layout = SecureRunLayout(
            canonical_root=root,
            root_fd=root_fd,
            run_dir_fd=run_fd,
            test_fds=test_fds,
            identities=tuple(identities),
            global_file_fds=global_files,
            owned_fds=owned,
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


def supervise_validation_run(
    *,
    environment: Mapping[str, str],
    runner_command: Sequence[str],
    db_update_command: Sequence[str] | None,
    validation_tests_dir: str | Path,
    stdout: BinaryIO | None = None,
    stderr: BinaryIO | None = None,
) -> int:
    """Reserve evidence, run both compatibility children, and cleanly release."""

    runtime_env = dict(environment)
    required = ("CVAL_VALIDATION_ROOT", "GCRNODE", "CVAL_RUN_ID", "CVAL_TEST_REGISTRY_JSON")
    missing = [name for name in required if not runtime_env.get(name)]
    if missing:
        raise ValueError("Missing secure supervisor environment: " + ", ".join(missing))
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
    runtime_env.update(layout.environment())
    runtime_env["CVAL_CANONICAL_JOB_LOG_DIR"] = str(canonical_run)
    repo_dir = runtime_env.get("CVAL_REPO_DIR")
    if repo_dir:
        existing_pythonpath = runtime_env.get("PYTHONPATH", "")
        runtime_env["PYTHONPATH"] = (
            repo_dir
            if not existing_pythonpath
            else f"{repo_dir}{os.pathsep}{existing_pythonpath}"
        )
    _add_builtin_compatibility_paths(runtime_env, layout)
    stdout = stdout or sys.stdout.buffer
    stderr = stderr or sys.stderr.buffer
    previous_handlers: dict[signal.Signals, object] = {}
    active: list[subprocess.Popen[bytes] | None] = [None]

    def handle_signal(signum: int, _frame: object) -> None:
        process = active[0]
        if process is not None:
            _terminate_child_group(process)
        raise KeyboardInterrupt(signum)

    if threading.current_thread() is threading.main_thread():
        for signal_name in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signal_name] = signal.getsignal(signal_name)
            signal.signal(signal_name, handle_signal)
    try:
        _write_all(layout.global_file_fds["stdout.log"], b"test started.\n")
        _write_all(layout.global_file_fds["job.log"], b"test started.\n")
        for command in (runner_command, db_update_command):
            if command is None:
                continue
            layout.verify()
            return_code = _run_child(
                command,
                cwd=Path(validation_tests_dir),
                environment=runtime_env,
                pass_fds=layout.inherited_fds,
                stdout=stdout,
                stderr=stderr,
                global_stdout_fd=layout.global_file_fds["stdout.log"],
                global_stderr_fd=layout.global_file_fds["stderr.log"],
                job_log_fd=layout.global_file_fds["job.log"],
                active=active,
            )
            layout.verify()
            if return_code != 0:
                return return_code
        _write_all(layout.global_file_fds["stdout.log"], b"Completed.\n")
        _write_all(layout.global_file_fds["job.log"], b"Completed.\n")
        layout.release_marker()
        return 0
    except KeyboardInterrupt:
        return 143
    finally:
        for signal_name, handler in previous_handlers.items():
            signal.signal(signal_name, handler)
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
            db_update_command=(
                "/bin/bash",
                "-c",
                "source ./0-env.sh && exec /bin/bash ./db-update.sh",
            ),
            validation_tests_dir=validation_tests_dir,
        )
    except Exception as exc:  # noqa: BLE001 - top-level pod boundary
        print(f"c-val secure supervisor failed: {exc}", file=sys.stderr)
        return 1


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


def _fd_path(descriptor: int) -> str:
    path = f"/proc/self/fd/{descriptor}"
    if not os.path.exists(path):
        raise RuntimeError("/proc/self/fd is required for secure run evidence paths")
    return path


def _add_builtin_compatibility_paths(
    environment: dict[str, str], layout: SecureRunLayout
) -> None:
    node = environment["GCRNODE"]
    run_id = environment["CVAL_RUN_ID"]
    canonical_root = layout.canonical_root
    for test_id, (log_fd, run_fd, artifacts_fd) in layout.test_fds.items():
        if test_id not in LEGACY_TEST_IDS:
            continue
        prefix = test_id.upper()
        canonical_run = canonical_root / "validation_tests" / test_id / "runs" / node / run_id
        environment[f"{prefix}_RUN_DIR"] = _fd_path(run_fd)
        environment[f"{prefix}_OUTPUT_DIR"] = _fd_path(artifacts_fd)
        environment[f"CVAL_CANONICAL_{prefix}_RUN_DIR"] = str(canonical_run)
        environment[f"CVAL_CANONICAL_{prefix}_OUTPUT_DIR"] = str(canonical_run / "artifacts")
        if test_id == "storage":
            environment["STORAGE_LOG_FILE"] = f"{_fd_path(log_fd)}/stdout.log"
            environment["STORAGE_SUMMARY_FILE"] = f"{_fd_path(run_fd)}/summary.txt"
        elif test_id == "nccl":
            environment["NCCL_LOG_FILE"] = f"{_fd_path(log_fd)}/workload.log"
            environment["NCCL_SUMMARY_FILE"] = f"{_fd_path(run_fd)}/summary.json"
            environment["CVAL_CANONICAL_NCCL_SUMMARY_FILE"] = str(
                canonical_run / "summary.json"
            )
            timestamp = environment.get("GCRTIME", "unknown")
            environment["NCCL_IBBW_LOG_FILE"] = (
                f"{_fd_path(artifacts_fd)}/ibbw-{node}-{timestamp}.log"
            )
        else:
            environment["DLTEST_LOG_FILE"] = f"{_fd_path(log_fd)}/workload.log"
            environment["DLTEST_SUMMARY_FILE"] = f"{_fd_path(run_fd)}/summary.json"


def _run_child(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    pass_fds: tuple[int, ...],
    stdout: BinaryIO,
    stderr: BinaryIO,
    global_stdout_fd: int,
    global_stderr_fd: int,
    job_log_fd: int,
    active: list[subprocess.Popen[bytes] | None],
) -> int:
    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=dict(environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        pass_fds=pass_fds,
        start_new_session=True,
    )
    active[0] = process
    assert process.stdout is not None and process.stderr is not None
    lock = threading.Lock()
    errors: list[BaseException] = []

    def pump(source: BinaryIO, console: BinaryIO, destination_fd: int) -> None:
        try:
            while True:
                chunk = source.read(65536)
                if not chunk:
                    return
                with lock:
                    _write_all(destination_fd, chunk)
                    _write_all(job_log_fd, chunk)
                    console.write(chunk)
                    console.flush()
        except BaseException as exc:  # noqa: BLE001 - propagated after join
            errors.append(exc)

    threads = (
        threading.Thread(
            target=pump,
            args=(process.stdout, stdout, global_stdout_fd),
            daemon=True,
        ),
        threading.Thread(
            target=pump,
            args=(process.stderr, stderr, global_stderr_fd),
            daemon=True,
        ),
    )
    for thread in threads:
        thread.start()
    try:
        return_code = process.wait()
    except BaseException:
        _terminate_child_group(process)
        raise
    finally:
        active[0] = None
    for thread in threads:
        thread.join(timeout=5.0)
    process.stdout.close()
    process.stderr.close()
    if any(thread.is_alive() for thread in threads):
        raise RuntimeError("supervised child log streams did not close")
    if errors:
        raise RuntimeError(f"supervised child log capture failed: {errors[0]}")
    return return_code


def _terminate_child_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 2.0
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.02)
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=2.0)


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(descriptor, view)
        view = view[written:]


if __name__ == "__main__":
    raise SystemExit(main())