"""Small descriptor-anchored filesystem primitives for local lifecycle tools."""

from __future__ import annotations

import ctypes
import errno
import os
import stat
from pathlib import Path


_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
_DIRECTORY_FLAGS |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_DIRECTORY_FLAGS |= getattr(os, "O_NONBLOCK", 0)
_RENAME_NOREPLACE = 1


def lexical_absolute(path: str | Path) -> Path:
    """Return an absolute lexical path without resolving any symlink."""

    return Path(os.path.abspath(os.path.expanduser(os.fspath(path))))


def safe_relative_parts(path: str | Path, *, field_name: str) -> tuple[str, ...]:
    """Validate and split one root-relative path without filesystem resolution."""

    value = Path(path)
    if value.is_absolute() or not value.parts or any(
        part in {"", ".", ".."} for part in value.parts
    ):
        raise ValueError(f"{field_name} must be a confined relative path: {path!r}")
    return value.parts


def open_directory_no_symlinks(path: str | Path) -> tuple[Path, int]:
    """Open every lexical ancestor as a nonblocking, no-follow directory."""

    absolute = lexical_absolute(path)
    descriptor = os.open(os.sep, _DIRECTORY_FLAGS)
    try:
        for part in absolute.parts[1:]:
            next_descriptor = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        return absolute, descriptor
    except BaseException:
        os.close(descriptor)
        raise


def open_directory_at(root_fd: int, relative: str | Path) -> int:
    """Open a confined descendant directory from a retained root descriptor."""

    parts = safe_relative_parts(relative, field_name="directory path")
    descriptor = os.dup(root_fd)
    try:
        for part in parts:
            next_descriptor = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def open_parent_at(root_fd: int, relative: str | Path) -> tuple[int, str]:
    """Open the parent of a confined descendant and return its basename."""

    parts = safe_relative_parts(relative, field_name="file path")
    descriptor = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            next_descriptor = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor, parts[-1]
    except BaseException:
        os.close(descriptor)
        raise


def descriptor_identity(descriptor: int) -> tuple[int, int]:
    metadata = os.fstat(descriptor)
    return metadata.st_dev, metadata.st_ino


def assert_lexical_directory_identity(path: str | Path, expected: tuple[int, int]) -> None:
    """Reopen a lexical directory path and require the retained identity."""

    _absolute, descriptor = open_directory_no_symlinks(path)
    try:
        if descriptor_identity(descriptor) != expected:
            raise RuntimeError(f"Directory identity changed during operation: {path}")
    finally:
        os.close(descriptor)


def mkdir_exact_at(parent_fd: int, name: str, mode: int = 0o700) -> int:
    """Create and open one child directory with a mode independent of umask."""

    if "/" in name or name in {"", ".", ".."}:
        raise ValueError(f"Unsafe directory basename: {name!r}")
    os.mkdir(name, mode, dir_fd=parent_fd)
    try:
        # mkdir applies the process umask. Restore the exact owner-only mode
        # before opening the exclusively created child; the retained 0700
        # parent prevents an untrusted name replacement in between.
        os.chmod(name, mode, dir_fd=parent_fd, follow_symlinks=False)
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except BaseException:
        os.rmdir(name, dir_fd=parent_fd)
        raise
    try:
        os.fchmod(descriptor, mode)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def write_file_at(parent_fd: int, name: str, payload: bytes, mode: int) -> None:
    """Exclusively write and fsync one regular child without following links."""

    if "/" in name or name in {"", ".", ".."}:
        raise ValueError(f"Unsafe file basename: {name!r}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(name, flags, mode, dir_fd=parent_fd)
    try:
        os.fchmod(descriptor, mode)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write while staging file")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_regular_file_at(
    root_fd: int,
    relative: str | Path,
    *,
    max_bytes: int,
    require_current_owner: bool = False,
    reject_group_world_write: bool = False,
    no_atime: bool = False,
    nonblocking: bool = False,
) -> bytes:
    """Read one confined stable regular file through retained descriptors."""

    parent_fd, name = open_parent_at(root_fd, relative)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    if no_atime:
        no_atime_flag = getattr(os, "O_NOATIME", 0)
        if not no_atime_flag:
            os.close(parent_fd)
            raise OSError(
                errno.ENOTSUP,
                "O_NOATIME is required for metadata-side-effect-free reads but is unsupported",
            )
        flags |= no_atime_flag
    if nonblocking:
        flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        try:
            descriptor = os.open(name, flags, dir_fd=parent_fd)
        except OSError as exc:
            no_atime_errors = {
                errno.EPERM,
                errno.EACCES,
                errno.EINVAL,
                errno.ENOSYS,
                errno.ENOTSUP,
                getattr(errno, "EOPNOTSUPP", errno.ENOTSUP),
            }
            if no_atime and exc.errno in no_atime_errors:
                raise OSError(
                    exc.errno,
                    "O_NOATIME is required for metadata-side-effect-free reads "
                    "but is unsupported or not permitted",
                    os.fspath(relative),
                ) from exc
            raise
    finally:
        os.close(parent_fd)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"Input is not a regular file: {relative}")
        if require_current_owner and before.st_uid != os.geteuid():
            raise ValueError(f"Input is not owned by the current user: {relative}")
        if reject_group_world_write and before.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ValueError(f"Input is group/world writable: {relative}")
        if before.st_size < 0 or before.st_size > max_bytes:
            raise ValueError(f"Input exceeds {max_bytes} bytes: {relative}")
        payload = bytearray()
        while len(payload) <= max_bytes:
            chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) > max_bytes:
            raise ValueError(f"Input exceeds {max_bytes} bytes: {relative}")
        identity_fields = (
            "st_dev",
            "st_ino",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
            "st_uid",
            "st_gid",
            "st_mode",
        )
        if tuple(getattr(before, field) for field in identity_fields) != tuple(
            getattr(after, field) for field in identity_fields
        ):
            raise RuntimeError(f"Input changed while reading: {relative}")
        if len(payload) != before.st_size:
            raise RuntimeError(f"Input size changed while reading: {relative}")
        return bytes(payload)
    finally:
        os.close(descriptor)


def remove_tree_at(parent_fd: int, name: str) -> None:
    """Remove one staged tree recursively without following descendant links."""

    try:
        metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(metadata.st_mode):
        os.unlink(name, dir_fd=parent_fd)
        return
    descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    try:
        for child in os.listdir(descriptor):
            remove_tree_at(descriptor, child)
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=parent_fd)


def rename_noreplace_at(
    source_parent_fd: int,
    source_name: str,
    destination_parent_fd: int,
    destination_name: str,
) -> None:
    """Atomically publish a name with Linux ``RENAME_NOREPLACE`` semantics."""

    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise OSError(errno.ENOSYS, "renameat2 is required for no-overwrite publication")
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        source_parent_fd,
        os.fsencode(source_name),
        destination_parent_fd,
        os.fsencode(destination_name),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), destination_name)


__all__ = [
    "assert_lexical_directory_identity",
    "descriptor_identity",
    "lexical_absolute",
    "mkdir_exact_at",
    "open_directory_at",
    "open_directory_no_symlinks",
    "read_regular_file_at",
    "remove_tree_at",
    "rename_noreplace_at",
    "safe_relative_parts",
    "write_file_at",
]
