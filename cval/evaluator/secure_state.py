"""Linux no-overwrite publication for persistent evaluator directories."""

from __future__ import annotations

import os
import secrets
import stat
from dataclasses import dataclass
from typing import Callable

from cval.evaluator.signals import defer_creation_signals
from cval.validation.secure_fs import rename_noreplace_at

_DIRECTORY_MODE = 0o700
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
_DIRECTORY_FLAGS |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_DIRECTORY_FLAGS |= getattr(os, "O_NONBLOCK", 0)
_MAX_STAGE_ATTEMPTS = 16
_MAX_QUARANTINE_ATTEMPTS = 16


@dataclass(frozen=True)
class PublishedDirectory:
    """Retained descriptor and identity of one newly published directory."""

    descriptor: int
    identity: tuple[int, int]


def create_published_directory_at(
    parent_fd: int,
    final_name: str,
    *,
    expected_uid: int,
    expected_gid: int,
    mode: int = _DIRECTORY_MODE,
) -> PublishedDirectory:
    """Create privately, bind, and atomically publish one persistent directory.

    The private name is a same-parent 128-bit random capability. The caller's
    retained parent descriptor anchors staging, no-overwrite publication, and
    exact-identity rollback. A pre-existing or racing final name is never
    opened, repaired, adopted, or overwritten.
    """

    _validate_basename(final_name)
    if mode != _DIRECTORY_MODE:
        raise ValueError("Persistent evaluator directories must use exact mode 0700")
    descriptor = -1
    identity: tuple[int, int] | None = None
    stage_name: str | None = None
    stage_created = False
    transferred = False
    try:
        for _attempt in range(_MAX_STAGE_ATTEMPTS):
            candidate = f".cval-dir-stage-{secrets.token_hex(16)}"
            try:
                with defer_creation_signals():
                    os.mkdir(candidate, mode, dir_fd=parent_fd)
                    stage_name = candidate
                    stage_created = True
                    staged = os.stat(
                        candidate,
                        dir_fd=parent_fd,
                        follow_symlinks=False,
                    )
                    identity = (staged.st_dev, staged.st_ino)
                    descriptor = os.open(candidate, _DIRECTORY_FLAGS, dir_fd=parent_fd)
                    opened = os.fstat(descriptor)
                    if (opened.st_dev, opened.st_ino) != identity:
                        raise RuntimeError(
                            "Persistent directory staging identity changed before binding"
                        )
                break
            except FileExistsError:
                # A private-name collision does not expose or adopt that entry.
                stage_name = None
                stage_created = False
                identity = None
                continue
        else:
            raise RuntimeError("Could not reserve a private evaluator directory name")

        assert stage_name is not None and identity is not None and descriptor >= 0
        os.fchmod(descriptor, mode)
        _assert_created_directory(
            descriptor,
            identity,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            mode=mode,
        )
        os.fsync(descriptor)
        _directory_publication_checkpoint(
            "staged",
            parent_fd,
            stage_name,
            final_name,
            descriptor,
            identity,
        )
        rename_noreplace_at(parent_fd, stage_name, parent_fd, final_name)
        stage_name = None
        _directory_publication_checkpoint(
            "published",
            parent_fd,
            "",
            final_name,
            descriptor,
            identity,
        )
        _assert_named_directory(parent_fd, final_name, descriptor, identity)
        os.fsync(parent_fd)
        transferred = True
        return PublishedDirectory(descriptor=descriptor, identity=identity)
    except BaseException as primary_error:
        cleanup_identity = identity
        if cleanup_identity is None and descriptor >= 0:
            try:
                metadata = os.fstat(descriptor)
                cleanup_identity = (metadata.st_dev, metadata.st_ino)
            except BaseException as capture_error:
                _add_cleanup_note(
                    primary_error,
                    "Persistent directory identity recovery failed closed",
                    capture_error,
                )
        if cleanup_identity is None and stage_created and stage_name is not None:
            try:
                metadata = os.stat(
                    stage_name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                cleanup_identity = (metadata.st_dev, metadata.st_ino)
            except BaseException as capture_error:
                _add_cleanup_note(
                    primary_error,
                    "Persistent directory staged identity recovery failed closed",
                    capture_error,
                )
        if cleanup_identity is not None:
            try:
                # Only these two framework-controlled names are eligible. Never
                # scan for or remove a same-inode relocation.
                if stage_name is not None:
                    _remove_empty_directory_if_identity(
                        parent_fd,
                        stage_name,
                        cleanup_identity,
                    )
                _remove_empty_directory_if_identity(
                    parent_fd,
                    final_name,
                    cleanup_identity,
                )
                os.fsync(parent_fd)
            except BaseException as cleanup_error:
                _add_cleanup_note(
                    primary_error,
                    "Persistent directory exact-identity cleanup failed closed",
                    cleanup_error,
                )
        raise
    finally:
        if descriptor >= 0 and not transferred:
            os.close(descriptor)


def _validate_basename(name: str) -> None:
    if (
        not isinstance(name, str)
        or name in {"", ".", ".."}
        or os.path.sep in name
        or (os.path.altsep is not None and os.path.altsep in name)
        or len(os.fsencode(name)) > 255
    ):
        raise ValueError(f"Unsafe persistent directory basename: {name!r}")


def _assert_created_directory(
    descriptor: int,
    identity: tuple[int, int],
    *,
    expected_uid: int,
    expected_gid: int,
    mode: int,
) -> None:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or (metadata.st_dev, metadata.st_ino) != identity
        or (metadata.st_uid, metadata.st_gid) != (expected_uid, expected_gid)
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise PermissionError(
            "Persistent evaluator staging directory is not exact owner 0700"
        )


def _assert_named_directory(
    parent_fd: int,
    name: str,
    descriptor: int,
    identity: tuple[int, int],
) -> None:
    retained = os.fstat(descriptor)
    current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if (
        not stat.S_ISDIR(retained.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or (retained.st_dev, retained.st_ino) != identity
        or (current.st_dev, current.st_ino) != identity
    ):
        raise RuntimeError("Persistent directory publication identity changed")


def _remove_empty_directory_if_identity(
    parent_fd: int,
    name: str,
    identity: tuple[int, int],
) -> bool:
    return remove_entry_if_identity_at(
        parent_fd,
        name,
        identity,
        is_directory=True,
        description="Persistent directory cleanup target",
    )


def remove_entry_if_identity_at(
    parent_fd: int,
    name: str,
    expected: tuple[int, int],
    *,
    is_directory: bool,
    description: str,
    binding_guard: Callable[[], None] | None = None,
) -> bool:
    """Relocate, verify, and remove one exact same-parent entry.

    The random quarantine name is a private 128-bit capability under the
    accepted single-owner-process threat model.  A public-name replacement is
    moved out of the way only long enough to inspect it, then restored with
    no-overwrite semantics.  Cleanup never scans for or removes a same-inode
    relocation under any other name.
    """

    _validate_basename(name)
    if binding_guard is not None:
        binding_guard()
    quarantine_name: str | None = None
    for _attempt in range(_MAX_QUARANTINE_ATTEMPTS):
        candidate = f".cval-cleanup-{secrets.token_hex(16)}"
        try:
            rename_noreplace_at(parent_fd, name, parent_fd, candidate)
        except FileNotFoundError:
            return False
        except FileExistsError:
            continue
        quarantine_name = candidate
        break
    else:
        raise RuntimeError("Could not reserve a private cleanup quarantine name")

    assert quarantine_name is not None
    try:
        os.fsync(parent_fd)
        _quarantine_cleanup_checkpoint(
            "relocated",
            parent_fd,
            name,
            quarantine_name,
            expected,
            is_directory,
        )
        metadata = os.stat(
            quarantine_name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        actual_identity = (metadata.st_dev, metadata.st_ino)
        expected_type = (
            stat.S_ISDIR(metadata.st_mode)
            if is_directory
            else stat.S_ISREG(metadata.st_mode)
        )
        if actual_identity != expected or not expected_type:
            raise RuntimeError(f"{description} was replaced; replacement preserved")

        if is_directory:
            descriptor = os.open(
                quarantine_name,
                _DIRECTORY_FLAGS,
                dir_fd=parent_fd,
            )
            try:
                retained = os.fstat(descriptor)
                if (
                    not stat.S_ISDIR(retained.st_mode)
                    or (retained.st_dev, retained.st_ino) != expected
                    or os.listdir(descriptor)
                ):
                    raise RuntimeError(
                        f"{description} changed or is not empty; preserved"
                    )
            finally:
                os.close(descriptor)
            os.rmdir(quarantine_name, dir_fd=parent_fd)
        else:
            os.unlink(quarantine_name, dir_fd=parent_fd)
        quarantine_name = None
        os.fsync(parent_fd)
        return True
    except BaseException as primary_error:
        if quarantine_name is not None:
            try:
                rename_noreplace_at(
                    parent_fd,
                    quarantine_name,
                    parent_fd,
                    name,
                )
                quarantine_name = None
                os.fsync(parent_fd)
                if binding_guard is not None:
                    binding_guard()
            except BaseException as restore_error:
                _add_cleanup_note(
                    primary_error,
                    f"{description} quarantine restoration failed closed",
                    restore_error,
                )
        raise


def _quarantine_cleanup_checkpoint(
    _stage: str,
    _parent_fd: int,
    _public_name: str,
    _quarantine_name: str,
    _expected: tuple[int, int],
    _is_directory: bool,
) -> None:
    """Non-mutating production hook used by cleanup relocation-race tests."""


def _directory_publication_checkpoint(
    _stage: str,
    _parent_fd: int,
    _stage_name: str,
    _final_name: str,
    _descriptor: int,
    _identity: tuple[int, int],
) -> None:
    """Non-mutating production hook used by exact publication-race tests."""


def _add_cleanup_note(
    primary_error: BaseException,
    message: str,
    cleanup_error: BaseException,
) -> None:
    if hasattr(primary_error, "add_note"):
        primary_error.add_note(
            f"{message}: {type(cleanup_error).__name__}: {cleanup_error}"
        )


__all__ = [
    "PublishedDirectory",
    "create_published_directory_at",
    "remove_entry_if_identity_at",
]
