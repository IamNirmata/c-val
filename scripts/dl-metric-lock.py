#!/usr/bin/env python3
"""Safely serialize DL metric ingestion on its stable metadata directory inode."""

from __future__ import annotations

import ctypes
import fcntl
import os
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path


_POLL_SECONDS = 0.05
_TERMINATION_GRACE_SECONDS = 1.0
_PR_SET_CHILD_SUBREAPER = 36


def _directory_identity(path: Path) -> tuple[int, int]:
    metadata = os.stat(path, follow_symlinks=False)
    if not stat.S_ISDIR(metadata.st_mode):
        raise OSError("DL metric lock parent is not a directory")
    return metadata.st_dev, metadata.st_ino


def _assert_path_identity(path: Path, expected: tuple[int, int]) -> None:
    if _directory_identity(path) != expected:
        raise OSError("DL metric lock directory path/device/inode changed")


def _enable_child_subreaper() -> None:
    """Adopt orphaned command descendants so they can be reaped before unlock."""

    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))


def _signal_process_group(process_group_id: int, signal_number: int) -> bool:
    try:
        os.killpg(process_group_id, signal_number)
        return True
    except ProcessLookupError:
        return False


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _reap_process_group(
    process: subprocess.Popen[bytes], process_group_id: int
) -> None:
    """Reap the leader and any descendants adopted as the child subreaper."""

    process.poll()
    while True:
        try:
            child_pid, _status = os.waitpid(-process_group_id, os.WNOHANG)
        except ChildProcessError:
            return
        if child_pid == 0:
            return


def _wait_for_process_group(
    process: subprocess.Popen[bytes],
    process_group_id: int,
    *,
    deadline: float | None,
) -> bool:
    while True:
        _reap_process_group(process, process_group_id)
        if not _process_group_exists(process_group_id):
            process.poll()
            return True
        if deadline is not None and time.monotonic() >= deadline:
            return False
        time.sleep(_POLL_SECONDS)


def _terminate_process_group(
    process: subprocess.Popen[bytes],
    process_group_id: int,
    *,
    initial_signal: int | None = signal.SIGTERM,
) -> None:
    """Terminate, kill after a bounded grace, and reap the whole command group."""

    if initial_signal is not None:
        _signal_process_group(process_group_id, initial_signal)
    deadline = time.monotonic() + _TERMINATION_GRACE_SECONDS
    if not _wait_for_process_group(process, process_group_id, deadline=deadline):
        _signal_process_group(process_group_id, signal.SIGKILL)
        # Do not release the directory flock while a commanded process remains.
        _wait_for_process_group(process, process_group_id, deadline=None)
    try:
        process.wait(timeout=0)
    except subprocess.TimeoutExpired as exc:  # pragma: no cover - group is gone
        raise RuntimeError("command process leader was not reaped") from exc


def main(argv: list[str]) -> int:
    if len(argv) < 4 or argv[2] != "--":
        print("usage: dl-metric-lock.py LOCK_FILE -- COMMAND [ARG ...]", file=sys.stderr)
        return 2
    if not all(hasattr(os, name) for name in ("O_NOFOLLOW", "O_DIRECTORY")):
        print(
            "dl-metric-lock: O_NOFOLLOW/O_DIRECTORY unavailable; refusing work",
            file=sys.stderr,
        )
        return 1

    marker_path = Path(argv[1]).expanduser()
    if not marker_path.is_absolute():
        marker_path = Path.cwd() / marker_path
    marker_path = Path(*marker_path.parts)
    lock_directory = marker_path.parent
    command = argv[3:]
    fd = -1
    process: subprocess.Popen[bytes] | None = None
    process_group_id: int | None = None
    group_reaped = False
    received_signal: int | None = None
    forwarded_signal = False
    previous_signal_handlers: dict[int, signal.Handlers] = {}

    def forward_signal(signal_number: int, _frame: object) -> None:
        nonlocal forwarded_signal, received_signal
        if received_signal is None:
            received_signal = signal_number
        if process_group_id is not None:
            forwarded_signal = (
                _signal_process_group(process_group_id, signal_number)
                or forwarded_signal
            )

    try:
        canonical_directory = lock_directory.resolve(strict=True)
        if canonical_directory != lock_directory:
            raise OSError("DL metric lock directory path is not canonical")
        if marker_path.exists() and marker_path.is_dir():
            raise OSError("DL metric lock marker path must not be a directory")
        fd = os.open(
            canonical_directory,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        metadata = os.fstat(fd)
        identity = (metadata.st_dev, metadata.st_ino)
        if not stat.S_ISDIR(metadata.st_mode):
            raise OSError("DL metric lock descriptor is not a directory")
        if metadata.st_uid != os.geteuid():
            raise OSError("DL metric lock directory is not owned by the effective user")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise OSError("DL metric lock directory must not be group/other writable")
        _assert_path_identity(canonical_directory, identity)

        fcntl.flock(fd, fcntl.LOCK_EX)
        _assert_path_identity(canonical_directory, identity)
        print(f"acquired DL metric lock (directory): {canonical_directory}", flush=True)
        _enable_child_subreaper()
        for signal_number in (signal.SIGINT, signal.SIGTERM):
            previous_signal_handlers[signal_number] = signal.getsignal(signal_number)
            signal.signal(signal_number, forward_signal)
        if received_signal is not None:
            return 128 + received_signal
        process = subprocess.Popen(
            command,
            close_fds=True,
            start_new_session=True,
        )
        process_group_id = process.pid
        if received_signal is not None:
            forwarded_signal = (
                _signal_process_group(process_group_id, received_signal)
                or forwarded_signal
            )
        while True:
            if received_signal is not None:
                _terminate_process_group(
                    process,
                    process_group_id,
                    initial_signal=None if forwarded_signal else received_signal,
                )
                group_reaped = True
                return 128 + received_signal
            try:
                return_code = process.wait(timeout=_POLL_SECONDS)
                break
            except subprocess.TimeoutExpired:
                _assert_path_identity(canonical_directory, identity)
        if received_signal is not None:
            _terminate_process_group(
                process,
                process_group_id,
                initial_signal=None if forwarded_signal else received_signal,
            )
            group_reaped = True
            return 128 + received_signal
        _assert_path_identity(canonical_directory, identity)
        if _process_group_exists(process_group_id):
            _terminate_process_group(process, process_group_id)
            group_reaped = True
            raise OSError("command exited while descendant processes remained")
        _reap_process_group(process, process_group_id)
        group_reaped = True
        return return_code if return_code >= 0 else 128 - return_code
    except OSError as exc:
        if process is not None and process_group_id is not None and not group_reaped:
            _terminate_process_group(process, process_group_id)
            group_reaped = True
        print(f"dl-metric-lock: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        if process is not None and process_group_id is not None and not group_reaped:
            _terminate_process_group(
                process,
                process_group_id,
                initial_signal=signal.SIGINT,
            )
            group_reaped = True
        return 130
    finally:
        if process is not None and process_group_id is not None and not group_reaped:
            _terminate_process_group(process, process_group_id)
        for signal_number, handler in previous_signal_handlers.items():
            signal.signal(signal_number, handler)
        if fd >= 0:
            os.close(fd)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))