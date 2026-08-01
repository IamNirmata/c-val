"""Descriptor-anchored ownership and creation for evaluator-owned state."""

from __future__ import annotations

import fcntl
import os
import stat
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from cval.config import CvalConfig
from cval.evaluator.secure_state import (
    create_published_directory_at,
    remove_entry_if_identity_at,
)
from cval.evaluator.signals import defer_creation_signals
from cval.storage.sqlite_uri import SQLiteFileIdentity, assert_sqlite_file_identity

_STATE_DIRECTORY_MODE = 0o700
_STATE_FILE_MODE = 0o600
_MAX_OWNER_ID = 2_147_483_647


@dataclass(frozen=True)
class StateRootIdentity:
    """Exact identity of the configured evaluator state root."""

    path: Path
    device: int
    inode: int
    owner_uid: int
    owner_gid: int
    mode: int


@dataclass(frozen=True)
class StateDirectoryIdentity:
    """Exact identity of one evaluator-owned state directory."""

    path: Path
    device: int
    inode: int
    owner_uid: int
    owner_gid: int
    mode: int


@dataclass(frozen=True)
class StateFileIdentity:
    """Exact identity and metadata of one evaluator-owned regular file."""

    path: Path
    device: int
    inode: int
    owner_uid: int
    owner_gid: int
    mode: int
    link_count: int

    def sqlite_identity(self) -> SQLiteFileIdentity:
        return SQLiteFileIdentity(self.path, self.device, self.inode)


class StateLockError(RuntimeError):
    """Raised when a shared evaluator-state lock cannot remain exact."""


@dataclass
class StateDirectoryBinding:
    """Retained root-to-parent descriptors and exact directory identities."""

    root_path: Path
    target_path: Path
    descriptors: tuple[int, ...]
    identities: tuple[StateDirectoryIdentity, ...]
    relative_parts: tuple[str, ...]
    missing_parts: tuple[str, ...] = ()

    @property
    def missing(self) -> bool:
        return bool(self.missing_parts)

    @property
    def root_fd(self) -> int:
        return self.descriptors[0]

    @property
    def parent_fd(self) -> int:
        return self.descriptors[-1]

    @property
    def parent_path(self) -> Path:
        return self.identities[-1].path

    def assert_path_binding(self) -> None:
        """Reopen the lexical root/ancestry and compare retained descriptors."""

        reopened_root = _open_absolute_directory(self.root_path)
        try:
            _assert_directory_identity(reopened_root, self.identities[0])
        finally:
            os.close(reopened_root)
        for descriptor, identity in zip(
            self.descriptors,
            self.identities,
            strict=True,
        ):
            _assert_directory_identity(descriptor, identity)
        for index, part in enumerate(self.relative_parts):
            reopened = _open_directory_at(self.descriptors[index], part)
            try:
                _assert_directory_identity(reopened, self.identities[index + 1])
            finally:
                os.close(reopened)
        if self.missing_parts:
            first_missing = self.missing_parts[0]
            try:
                os.stat(
                    first_missing,
                    dir_fd=self.parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                pass
            else:
                raise RuntimeError(
                    "Evaluator state missing ancestry appeared while bound: "
                    f"{self.parent_path / first_missing}"
                )

    def open_file(
        self,
        name: str,
        *,
        writable: bool,
        create_exclusive: bool = False,
    ) -> int:
        _validate_leaf_name(name)
        self.assert_path_binding()
        if self.missing:
            raise FileNotFoundError(
                f"Evaluator state parent is missing: {self.parent_path / self.missing_parts[0]}"
            )
        return _open_file_at(
            self.parent_fd,
            name,
            writable=writable,
            create_exclusive=create_exclusive,
        )

    def unlink_if_identity(
        self,
        name: str,
        expected: tuple[int, int],
    ) -> bool:
        """Unlink only the exact retained-parent entry; preserve replacements."""

        _validate_leaf_name(name)
        self.assert_path_binding()
        return remove_entry_if_identity_at(
            self.parent_fd,
            name,
            expected,
            is_directory=False,
            description=f"Evaluator state cleanup target {self.parent_path / name}",
            binding_guard=self.assert_path_binding,
        )


@dataclass
class StateTargetBinding:
    """A retained directory binding plus one exact state-file descriptor."""

    directory: StateDirectoryBinding
    name: str
    descriptor: int | None
    identity: StateFileIdentity | None

    @property
    def path(self) -> Path:
        return self.directory.parent_path / self.name

    @property
    def sqlite_identity(self) -> SQLiteFileIdentity | None:
        return self.identity.sqlite_identity() if self.identity is not None else None

    def assert_path_binding(self) -> None:
        self.directory.assert_path_binding()
        if self.descriptor is None or self.identity is None:
            try:
                os.stat(
                    self.name,
                    dir_fd=self.directory.parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return
            raise RuntimeError(f"Evaluator state target appeared: {self.path}")
        _assert_state_file_metadata(self.descriptor, self.identity)
        current = _open_file_at(
            self.directory.parent_fd,
            self.name,
            writable=False,
        )
        try:
            _assert_state_file_metadata(current, self.identity)
        finally:
            os.close(current)

    def adopt_created_file(
        self,
        expected_identity: StateFileIdentity,
        *,
        writable: bool,
    ) -> None:
        """Adopt only the exact identity captured by the publisher."""

        if self.descriptor is not None or self.identity is not None:
            raise RuntimeError(f"Evaluator state target is already bound: {self.path}")
        if expected_identity.path != self.path:
            raise RuntimeError("Published evaluator state identity has the wrong path")
        self.directory.assert_path_binding()
        descriptor = _open_file_at(
            self.directory.parent_fd,
            self.name,
            writable=writable,
        )
        adopted = False
        try:
            _assert_state_file_metadata(descriptor, expected_identity)
            self.descriptor = descriptor
            self.identity = expected_identity
            self.assert_path_binding()
            adopted = True
        except BaseException:
            self.descriptor = None
            self.identity = None
            raise
        finally:
            if not adopted:
                os.close(descriptor)

    def release_adopted_file(self) -> None:
        """Return an adopted binding to its original unowned state."""

        descriptor = self.descriptor
        self.descriptor = None
        self.identity = None
        if descriptor is not None:
            os.close(descriptor)


@dataclass(frozen=True)
class StateTestLockGuard:
    """Callable exact binding for one held shared per-test lock."""

    directory: StateDirectoryBinding
    name: str
    descriptor: int
    identity: StateFileIdentity

    @property
    def path(self) -> Path:
        return self.identity.path

    def __call__(self) -> None:
        try:
            self.directory.assert_path_binding()
            _assert_state_file_metadata(self.descriptor, self.identity)
            current = _open_file_at(
                self.directory.parent_fd,
                self.name,
                writable=True,
            )
            try:
                _assert_state_file_metadata(current, self.identity)
            finally:
                os.close(current)
        except (OSError, RuntimeError, PermissionError, ValueError) as exc:
            raise StateLockError("Evaluator shared lock binding changed while held") from exc


def configured_state_root(config: CvalConfig) -> Path:
    """Return the strictly validated lexical evaluator state root."""

    state_root = _absolute_lexical_path(
        config.health_evaluator.state_root,
        description="health_evaluator.state_root",
    )
    validation_root = _absolute_lexical_path(
        config.runtime.validation_root,
        description="runtime.validation_root",
    )
    if state_root == validation_root or state_root in validation_root.parents:
        raise ValueError(
            "health_evaluator.state_root must not equal or contain "
            "runtime.validation_root"
        )
    for name, value in (
        ("state_owner_uid", config.health_evaluator.state_owner_uid),
        ("state_owner_gid", config.health_evaluator.state_owner_gid),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
            or value > _MAX_OWNER_ID
        ):
            raise ValueError(
                f"health_evaluator.{name} must be a positive non-root 32-bit ID"
            )
    if config.health_evaluator.validation_root_mode != "0700":
        raise ValueError(
            "health_evaluator.validation_root_mode must be exactly '0700' for the "
            "dedicated evaluator state root"
        )
    return state_root


def assert_evaluator_process_owner(config: CvalConfig) -> None:
    """Require the process effective identity to equal the fixed state owner."""

    expected = (
        config.health_evaluator.state_owner_uid,
        config.health_evaluator.state_owner_gid,
    )
    actual = (os.geteuid(), os.getegid())
    if actual != expected:
        raise PermissionError(
            "Evaluator process owner mismatch: "
            f"expected uid={expected[0]} gid={expected[1]}, "
            f"got uid={actual[0]} gid={actual[1]}"
        )


@contextmanager
def open_state_root(
    config: CvalConfig,
    *,
    require_writable: bool,
) -> Iterator[tuple[int, StateRootIdentity]]:
    """Open and validate the pre-provisioned state root without following links."""

    root = configured_state_root(config)
    assert_evaluator_process_owner(config)
    descriptor = _open_absolute_directory(root)
    try:
        metadata = os.fstat(descriptor)
        expected_uid = config.health_evaluator.state_owner_uid
        expected_gid = config.health_evaluator.state_owner_gid
        mode = stat.S_IMODE(metadata.st_mode)
        if not stat.S_ISDIR(metadata.st_mode):
            raise PermissionError("Evaluator state root is not a directory")
        if (metadata.st_uid, metadata.st_gid) != (expected_uid, expected_gid):
            raise PermissionError(
                "Evaluator state root owner mismatch: "
                f"expected uid={expected_uid} gid={expected_gid}, "
                f"got uid={metadata.st_uid} gid={metadata.st_gid}"
            )
        if mode != _STATE_DIRECTORY_MODE:
            raise PermissionError(
                "Evaluator state root mode mismatch: "
                f"expected 0700, got {mode:04o}"
            )
        if require_writable:
            readonly_flag = getattr(stat, "ST_RDONLY", 1)
            if os.statvfs(descriptor).f_flag & readonly_flag:
                raise PermissionError("Evaluator state root mount is read-only")
            if not mode & stat.S_IWUSR:
                raise PermissionError("Evaluator state root is not owner-writable")
        identity = StateRootIdentity(
            path=root,
            device=metadata.st_dev,
            inode=metadata.st_ino,
            owner_uid=metadata.st_uid,
            owner_gid=metadata.st_gid,
            mode=mode,
        )
        yield descriptor, identity
        _assert_open_directory_identity(descriptor, identity)
        reopened = _open_absolute_directory(root)
        try:
            _assert_open_directory_identity(reopened, identity)
        finally:
            os.close(reopened)
    finally:
        os.close(descriptor)


@contextmanager
def bind_state_directory(
    config: CvalConfig,
    target: str | Path,
    *,
    create: bool,
    allow_missing: bool,
    require_writable: bool,
) -> Iterator[StateDirectoryBinding]:
    """Retain every root-to-target-parent descriptor until the caller exits."""

    root = configured_state_root(config)
    relative = _relative_state_target(root, target)
    descriptors: list[int] = []
    identities: list[StateDirectoryIdentity] = []
    with open_state_root(config, require_writable=require_writable) as (
        root_fd,
        root_identity,
    ):
        descriptors.append(os.dup(root_fd))
        identities.append(_directory_identity(root_identity.path, descriptors[0]))
        current = root
        missing_parts: tuple[str, ...] = ()
        parent_parts = relative.parts[:-1]
        try:
            for part_index, part in enumerate(parent_parts):
                current = current / part
                created = False
                created_identity: tuple[int, int] | None = None
                try:
                    next_descriptor = _open_directory_at(descriptors[-1], part)
                except FileNotFoundError:
                    if not create:
                        if allow_missing:
                            missing_parts = parent_parts[part_index:]
                            break
                        raise
                    fcntl.flock(descriptors[-1], fcntl.LOCK_EX)
                    serialization_error: BaseException | None = None
                    next_descriptor = None
                    try:
                        try:
                            next_descriptor = _open_directory_at(
                                descriptors[-1],
                                part,
                            )
                        except FileNotFoundError:
                            published = create_published_directory_at(
                                descriptors[-1],
                                part,
                                expected_uid=config.health_evaluator.state_owner_uid,
                                expected_gid=config.health_evaluator.state_owner_gid,
                            )
                            created = True
                            next_descriptor = published.descriptor
                            created_identity = published.identity
                    except BaseException as primary_error:
                        serialization_error = primary_error
                        raise
                    finally:
                        unlock_error: BaseException | None = None
                        try:
                            fcntl.flock(descriptors[-1], fcntl.LOCK_UN)
                        except BaseException as error:
                            unlock_error = error
                        if serialization_error is not None:
                            if unlock_error is not None:
                                _add_cleanup_note(
                                    serialization_error,
                                    "Evaluator state ancestry serialization unlock failed",
                                    unlock_error,
                                )
                            if next_descriptor is not None:
                                try:
                                    os.close(next_descriptor)
                                except BaseException as close_error:
                                    _add_cleanup_note(
                                        serialization_error,
                                        "Evaluator state ancestry untracked descriptor close failed",
                                        close_error,
                                    )
                        elif unlock_error is not None:
                            assert next_descriptor is not None
                            try:
                                os.close(next_descriptor)
                            except BaseException as close_error:
                                _add_cleanup_note(
                                    unlock_error,
                                    "Evaluator state ancestry untracked descriptor close failed",
                                    close_error,
                                )
                            raise unlock_error
                    assert next_descriptor is not None
                ancestry_handed_off = False
                try:
                    if created:
                        current_metadata = os.stat(
                            part,
                            dir_fd=descriptors[-1],
                            follow_symlinks=False,
                        )
                        if created_identity != (
                            current_metadata.st_dev,
                            current_metadata.st_ino,
                        ):
                            raise RuntimeError(
                                "Evaluator state directory was replaced during creation"
                            )
                        os.fsync(descriptors[-1])
                    _assert_owned_directory(next_descriptor, config=config)
                    next_identity = _directory_identity(current, next_descriptor)
                    descriptors.append(next_descriptor)
                    identities.append(next_identity)
                    ancestry_handed_off = True
                except BaseException as primary_error:
                    if created and created_identity is not None:
                        try:
                            _remove_created_directory_identity(
                                descriptors[-1],
                                part,
                                created_identity,
                            )
                        except BaseException as cleanup_error:
                            _add_cleanup_note(
                                primary_error,
                                "Evaluator state ancestry post-registration cleanup failed closed",
                                cleanup_error,
                            )
                    if not ancestry_handed_off:
                        os.close(next_descriptor)
                    raise
            binding = StateDirectoryBinding(
                root_path=root,
                target_path=root / relative,
                descriptors=tuple(descriptors),
                identities=tuple(identities),
                relative_parts=relative.parts[: len(descriptors) - 1],
                missing_parts=missing_parts,
            )
            binding.assert_path_binding()
            yield binding
            binding.assert_path_binding()
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)


@contextmanager
def bind_state_target(
    config: CvalConfig,
    target: str | Path,
    *,
    create: bool,
    allow_missing: bool,
    writable: bool,
    require_writable: bool,
) -> Iterator[StateTargetBinding]:
    """Retain exact ancestry and target descriptors for a path consumer."""

    with bind_state_directory(
        config,
        target,
        create=create,
        allow_missing=allow_missing,
        require_writable=require_writable,
    ) as directory:
        name = Path(target).name
        if directory.missing:
            yield StateTargetBinding(directory, name, None, None)
            return
        descriptor: int | None = None
        binding: StateTargetBinding | None = None
        created_identity: tuple[int, int] | None = None
        created_exclusive = False
        handed_off = False
        active_error: BaseException | None = None
        try:
            try:
                if create:
                    with defer_creation_signals():
                        descriptor = directory.open_file(
                            name,
                            writable=writable,
                            create_exclusive=True,
                        )
                        created_exclusive = True
                        created_metadata = os.fstat(descriptor)
                        created_identity = (
                            created_metadata.st_dev,
                            created_metadata.st_ino,
                        )
                    _state_target_creation_checkpoint("open")
                else:
                    descriptor = directory.open_file(
                        name,
                        writable=writable,
                    )
            except FileExistsError:
                descriptor = directory.open_file(name, writable=writable)
            except FileNotFoundError:
                if allow_missing and not create:
                    binding = StateTargetBinding(directory, name, None, None)
                    binding.assert_path_binding()
                    yield binding
                    binding.assert_path_binding()
                    return
                raise
            if created_identity is not None:
                os.fchmod(descriptor, _STATE_FILE_MODE)
                _state_target_creation_checkpoint("fchmod")
                os.fsync(descriptor)
                _state_target_creation_checkpoint("file_fsync")
                os.fsync(directory.parent_fd)
                _state_target_creation_checkpoint("parent_fsync")
            identity = _state_file_identity(
                descriptor,
                directory.parent_path / name,
                config=config,
            )
            if created_identity is not None:
                _state_target_creation_checkpoint("identity")
            binding = StateTargetBinding(directory, name, descriptor, identity)
            binding.assert_path_binding()
            handed_off = True
            yield binding
            binding.assert_path_binding()
        except BaseException as primary_error:
            active_error = primary_error
            cleanup_identity = created_identity
            if (
                cleanup_identity is None
                and created_exclusive
                and descriptor is not None
            ):
                try:
                    created_metadata = os.fstat(descriptor)
                    cleanup_identity = (
                        created_metadata.st_dev,
                        created_metadata.st_ino,
                    )
                except BaseException as capture_error:
                    _add_cleanup_note(
                        primary_error,
                        "U7 interrupted target identity recovery failed closed",
                        capture_error,
                    )
            if cleanup_identity is not None and not handed_off:
                try:
                    _unlink_created_file_identity(
                        directory,
                        name,
                        cleanup_identity,
                    )
                    os.fsync(directory.parent_fd)
                except BaseException as cleanup_error:
                    _add_cleanup_note(
                        primary_error,
                        "U7 pre-yield target cleanup failed closed",
                        cleanup_error,
                    )
            raise
        finally:
            retained_descriptor = binding.descriptor if binding is not None else descriptor
            if retained_descriptor is not None:
                try:
                    os.close(retained_descriptor)
                except BaseException as close_error:
                    if active_error is None:
                        raise
                    _add_cleanup_note(
                        active_error,
                        "U7 target descriptor close failed",
                        close_error,
                    )


@contextmanager
def state_test_lock(
    config: CvalConfig,
    result_db_path: str | Path,
    *,
    timeout_seconds: int | None = None,
) -> Iterator[StateTestLockGuard]:
    """Acquire the one descriptor-relative lock shared by U7/U9/backup."""

    timeout = (
        config.health_evaluator.lock_timeout_seconds
        if timeout_seconds is None
        else timeout_seconds
    )
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("Evaluator lock timeout must be a positive integer")
    result_path = _absolute_lexical_path(
        result_db_path,
        description="evaluator result database",
    )
    with bind_state_directory(
        config,
        result_path,
        create=True,
        allow_missing=False,
        require_writable=True,
    ) as directory:
        lock_name = f".{result_path.stem}.health-evaluator.lock"
        descriptor: int | None = None
        acquired = False
        active_error: BaseException | None = None
        try:
            identity: StateFileIdentity | None = None
            try:
                with defer_creation_signals():
                    descriptor = directory.open_file(
                        lock_name,
                        writable=True,
                        create_exclusive=True,
                    )
                    os.fstat(descriptor)
                    os.fchmod(descriptor, _STATE_FILE_MODE)
                    os.fsync(descriptor)
                    os.fsync(directory.parent_fd)
                    identity = _state_file_identity(
                        descriptor,
                        directory.parent_path / lock_name,
                        config=config,
                    )
            except FileExistsError:
                descriptor = directory.open_file(lock_name, writable=True)
            if identity is None:
                identity = _state_file_identity(
                    descriptor,
                    directory.parent_path / lock_name,
                    config=config,
                )
            guard = StateTestLockGuard(directory, lock_name, descriptor, identity)
            guard()
            _state_lock_checkpoint("registered", guard)
            deadline = time.monotonic() + timeout
            while True:
                guard()
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    guard()
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise StateLockError(
                            f"Timed out acquiring evaluator shared lock after {timeout}s"
                        )
                    time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
            yield guard
            guard()
        except BaseException as primary_error:
            active_error = primary_error
            raise
        finally:
            if descriptor is not None:
                unlock_error: BaseException | None = None
                try:
                    if acquired:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                except BaseException as error:
                    unlock_error = error
                try:
                    os.close(descriptor)
                except BaseException as close_error:
                    if active_error is not None:
                        _add_cleanup_note(
                            active_error,
                            "Evaluator shared-lock descriptor close failed",
                            close_error,
                        )
                    elif unlock_error is not None:
                        _add_cleanup_note(
                            unlock_error,
                            "Evaluator shared-lock descriptor close failed",
                            close_error,
                        )
                    else:
                        raise
                if unlock_error is not None:
                    if active_error is not None:
                        _add_cleanup_note(
                            active_error,
                            "Evaluator shared-lock unlock failed",
                            unlock_error,
                        )
                    else:
                        raise unlock_error


def inspect_state_target(
    config: CvalConfig,
    target: str | Path,
    *,
    allow_missing: bool,
    require_writable: bool,
) -> StateFileIdentity | None:
    """Validate an existing state file and every existing owned parent."""

    root = configured_state_root(config)
    relative = _relative_state_target(root, target)
    with open_state_root(config, require_writable=require_writable) as (root_fd, root_id):
        parent_fd, missing = _open_state_parent(
            root_fd,
            root_id,
            relative.parts[:-1],
            config=config,
            allow_missing=allow_missing,
        )
        if missing:
            os.close(parent_fd)
            return None
        try:
            try:
                descriptor = _open_file_at(parent_fd, relative.name, writable=False)
            except FileNotFoundError:
                if allow_missing:
                    return None
                raise
            try:
                identity = _state_file_identity(
                    descriptor,
                    root / relative,
                    config=config,
                )
            finally:
                os.close(descriptor)
            return identity
        finally:
            os.close(parent_fd)


def inspect_state_ancestry(
    config: CvalConfig,
    target: str | Path,
    *,
    allow_missing: bool,
    require_writable: bool,
) -> tuple[StateDirectoryIdentity, ...]:
    """Capture every existing state-root-to-target-parent directory identity."""

    root = configured_state_root(config)
    relative = _relative_state_target(root, target)
    with open_state_root(config, require_writable=require_writable) as (
        root_fd,
        root_identity,
    ):
        identities = [
            StateDirectoryIdentity(
                path=root_identity.path,
                device=root_identity.device,
                inode=root_identity.inode,
                owner_uid=root_identity.owner_uid,
                owner_gid=root_identity.owner_gid,
                mode=root_identity.mode,
            )
        ]
        current = root

        def collect(
            parent_fd: int,
            parts: tuple[str, ...],
            parent_path: Path,
        ) -> None:
            nonlocal current
            if not parts:
                return
            part = parts[0]
            current = parent_path / part
            try:
                with _opened_directory_at(parent_fd, part) as child_fd:
                    _assert_owned_directory(child_fd, config=config)
                    metadata = os.fstat(child_fd)
                    identities.append(
                        StateDirectoryIdentity(
                            path=current,
                            device=metadata.st_dev,
                            inode=metadata.st_ino,
                            owner_uid=metadata.st_uid,
                            owner_gid=metadata.st_gid,
                            mode=stat.S_IMODE(metadata.st_mode),
                        )
                    )
                    collect(child_fd, parts[1:], current)
            except FileNotFoundError:
                if not allow_missing:
                    raise

        collect(root_fd, relative.parts[:-1], root)
        return tuple(identities)


def assert_state_file_identity(
    config: CvalConfig,
    identity: SQLiteFileIdentity,
) -> None:
    """Revalidate a bound state SQLite identity and its ownership metadata."""

    current = inspect_state_target(
        config,
        identity.path,
        allow_missing=False,
        require_writable=True,
    )
    if current is None or current.sqlite_identity() != identity:
        raise RuntimeError("Evaluator state SQLite identity changed")
    assert_sqlite_file_identity(identity)


def _open_absolute_directory(path: Path) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    @contextmanager
    def opened_anchor():
        descriptor = os.open(path.anchor, flags)
        try:
            yield descriptor
        finally:
            os.close(descriptor)

    with opened_anchor() as root_fd:
        with _opened_directory_chain(root_fd, path.parts[1:]) as leaf_fd:
            return os.dup(leaf_fd)


def _open_state_parent(
    root_fd: int,
    root_identity: StateRootIdentity,
    parts: tuple[str, ...],
    *,
    config: CvalConfig,
    allow_missing: bool,
) -> tuple[int, bool]:
    def descend(parent_fd: int, remaining: tuple[str, ...]) -> tuple[int, bool]:
        if not remaining:
            _assert_open_directory_identity(root_fd, root_identity)
            return os.dup(parent_fd), False
        part = remaining[0]
        ownership = ExitStack()
        try:
            try:
                child_fd = ownership.enter_context(
                    _opened_directory_at(parent_fd, part)
                )
            except FileNotFoundError:
                if allow_missing:
                    return os.dup(parent_fd), True
                raise
            _assert_owned_directory(child_fd, config=config)
            return descend(child_fd, remaining[1:])
        finally:
            ownership.close()

    return descend(root_fd, parts)


@contextmanager
def _opened_directory_at(parent_fd: int, name: str):
    descriptor = _open_directory_at(parent_fd, name)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


@contextmanager
def _opened_directory_chain(
    parent_fd: int,
    parts: tuple[str, ...],
):
    if not parts:
        yield parent_fd
        return
    with _opened_directory_at(parent_fd, parts[0]) as child_fd:
        with _opened_directory_chain(child_fd, parts[1:]) as leaf_fd:
            yield leaf_fd


def _open_directory_at(parent_fd: int, name: str) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        return os.open(name, flags, dir_fd=parent_fd)
    except OSError as exc:
        try:
            metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError:
            raise exc
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(
                f"Evaluator state directory traversal encountered a symlink: {name}"
            ) from exc
        raise


def _open_file_at(
    parent_fd: int,
    name: str,
    *,
    writable: bool,
    create_exclusive: bool = False,
) -> int:
    # Retained state descriptors are identity/snapshot bindings, not SQLite
    # writers.  On some Linux filesystems O_RDWR updates atime even when
    # O_NOATIME is present, so existing files must be opened read-only.  The
    # exclusive creator is the separate writable path; later mutations use the
    # identity-checked SQLite writer rather than this descriptor.
    flags = os.O_RDWR if create_exclusive else os.O_RDONLY
    if not hasattr(os, "O_NOATIME"):
        raise RuntimeError(
            "Evaluator state bindings require Linux O_NOATIME support"
        )
    flags |= os.O_NOATIME
    if create_exclusive:
        flags |= os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return os.open(name, flags, _STATE_FILE_MODE, dir_fd=parent_fd)


def _state_target_creation_checkpoint(_stage: str) -> None:
    """Non-interrupting production checkpoint used by creation-race tests."""


def _state_lock_checkpoint(
    _stage: str,
    _guard: StateTestLockGuard,
) -> None:
    """Non-interrupting production checkpoint used by lock-split tests."""


def _unlink_created_file_identity(
    directory: StateDirectoryBinding,
    preferred_name: str,
    expected: tuple[int, int],
) -> bool:
    """Relocate, verify, and remove only the expected public-name file."""

    directory.assert_path_binding()
    return remove_entry_if_identity_at(
        directory.parent_fd,
        preferred_name,
        expected,
        is_directory=False,
        description="Evaluator state created-file cleanup target",
        binding_guard=directory.assert_path_binding,
    )


def _remove_created_directory_identity(
    parent_fd: int,
    preferred_name: str,
    expected: tuple[int, int],
) -> bool:
    """Relocate, verify, and remove one exact empty created directory."""

    return remove_entry_if_identity_at(
        parent_fd,
        preferred_name,
        expected,
        is_directory=True,
        description="Evaluator state ancestry cleanup target",
    )


def _add_cleanup_note(
    primary_error: BaseException,
    message: str,
    cleanup_error: BaseException,
) -> None:
    note = f"{message}: {type(cleanup_error).__name__}: {cleanup_error}"
    if hasattr(primary_error, "add_note"):
        primary_error.add_note(note)


def _assert_owned_directory(descriptor: int, *, config: CvalConfig) -> None:
    metadata = os.fstat(descriptor)
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid)
        != (
            config.health_evaluator.state_owner_uid,
            config.health_evaluator.state_owner_gid,
        )
        or mode != _STATE_DIRECTORY_MODE
    ):
        raise PermissionError(
            "Evaluator state descendant must be an exact owner 0700 directory"
        )


def _state_file_identity(
    descriptor: int,
    path: Path,
    *,
    config: CvalConfig,
) -> StateFileIdentity:
    metadata = os.fstat(descriptor)
    mode = stat.S_IMODE(metadata.st_mode)
    expected_owner = (
        config.health_evaluator.state_owner_uid,
        config.health_evaluator.state_owner_gid,
    )
    if not stat.S_ISREG(metadata.st_mode):
        raise PermissionError(f"Evaluator state target is not a regular file: {path}")
    if (metadata.st_uid, metadata.st_gid) != expected_owner:
        raise PermissionError(f"Evaluator state file owner mismatch: {path}")
    if mode != _STATE_FILE_MODE:
        raise PermissionError(
            f"Evaluator state file mode must be 0600, got {mode:04o}: {path}"
        )
    if metadata.st_nlink != 1:
        raise PermissionError(
            f"Evaluator state file link count must be exactly 1: {path}"
        )
    return StateFileIdentity(
        path=path,
        device=metadata.st_dev,
        inode=metadata.st_ino,
        owner_uid=metadata.st_uid,
        owner_gid=metadata.st_gid,
        mode=mode,
        link_count=metadata.st_nlink,
    )


def _assert_open_directory_identity(
    descriptor: int,
    identity: StateRootIdentity,
) -> None:
    metadata = os.fstat(descriptor)
    actual = (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    )
    expected = (
        identity.device,
        identity.inode,
        identity.owner_uid,
        identity.owner_gid,
        identity.mode,
    )
    if not stat.S_ISDIR(metadata.st_mode) or actual != expected:
        raise RuntimeError("Evaluator state root identity changed while in use")


def _directory_identity(path: Path, descriptor: int) -> StateDirectoryIdentity:
    metadata = os.fstat(descriptor)
    return StateDirectoryIdentity(
        path=path,
        device=metadata.st_dev,
        inode=metadata.st_ino,
        owner_uid=metadata.st_uid,
        owner_gid=metadata.st_gid,
        mode=stat.S_IMODE(metadata.st_mode),
    )


def _assert_directory_identity(
    descriptor: int,
    identity: StateDirectoryIdentity,
) -> None:
    metadata = os.fstat(descriptor)
    actual = (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    )
    expected = (
        identity.device,
        identity.inode,
        identity.owner_uid,
        identity.owner_gid,
        identity.mode,
    )
    if not stat.S_ISDIR(metadata.st_mode) or actual != expected:
        raise RuntimeError(f"Evaluator state directory identity changed: {identity.path}")


def _assert_state_file_metadata(
    descriptor: int,
    identity: StateFileIdentity,
) -> None:
    metadata = os.fstat(descriptor)
    actual = (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
        metadata.st_nlink,
    )
    expected = (
        identity.device,
        identity.inode,
        identity.owner_uid,
        identity.owner_gid,
        identity.mode,
        identity.link_count,
    )
    if not stat.S_ISREG(metadata.st_mode) or actual != expected:
        raise RuntimeError(f"Evaluator state file identity/metadata changed: {identity.path}")


def _validate_leaf_name(name: str) -> None:
    if not name or name in {".", ".."} or os.path.sep in name:
        raise ValueError(f"Unsafe evaluator state leaf name: {name!r}")


def _relative_state_target(root: Path, target: str | Path) -> Path:
    value = _absolute_lexical_path(target, description="evaluator state target")
    try:
        relative = value.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Evaluator state target escapes state root {root}: {value}") from exc
    if not relative.parts:
        raise ValueError("Evaluator state target must be beneath the state root")
    return relative


def _absolute_lexical_path(value: str | Path, *, description: str) -> Path:
    raw = os.fspath(value)
    if not raw or "\x00" in raw or not os.path.isabs(raw):
        raise ValueError(f"{description} must be an absolute filesystem path")
    if os.path.normpath(raw) != raw or raw == os.path.sep:
        raise ValueError(f"{description} must be lexical-canonical")
    path = Path(raw)
    if any(part in {"", ".", ".."} for part in path.parts[1:]):
        raise ValueError(f"{description} must not contain traversal components")
    return path


__all__ = [
    "StateDirectoryBinding",
    "StateDirectoryIdentity",
    "StateFileIdentity",
    "StateLockError",
    "StateRootIdentity",
    "StateTargetBinding",
    "StateTestLockGuard",
    "assert_evaluator_process_owner",
    "assert_state_file_identity",
    "bind_state_directory",
    "bind_state_target",
    "configured_state_root",
    "inspect_state_target",
    "inspect_state_ancestry",
    "open_state_root",
    "state_test_lock",
]
