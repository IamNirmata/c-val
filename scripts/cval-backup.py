#!/usr/bin/env python3
"""Inspect, explicitly create, or verify a coherent whole-root backup."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import math
import os
import secrets
import signal
import sqlite3
import stat
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cval.validation.secure_fs import (  # noqa: E402
    assert_lexical_directory_identity,
    descriptor_identity,
    lexical_absolute,
    mkdir_exact_at,
    open_directory_no_symlinks,
    open_parent_at,
    read_regular_file_at,
    remove_tree_at,
    rename_noreplace_at,
    safe_relative_parts,
)

SCHEMA = "cval.backup"
SCHEMA_VERSION = 1
CONFIRMATION = "backup"
QUIESCENCE_CONFIRMATION = "writers-stopped"
EXCLUDED = ["backups/"]
SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
READ_CHUNK = 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024 * 1024
DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
DIRECTORY_FLAGS |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
DIRECTORY_FLAGS |= getattr(os, "O_NONBLOCK", 0)
FILE_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)


class BackupError(RuntimeError):
    """A fail-closed backup or verification error."""


@dataclass(frozen=True)
class TreeEntry:
    path: str
    type: str
    dev: int
    inode: int
    size: int
    mode: int
    atime_ns: int
    mtime_ns: int
    ctime_ns: int
    nlink: int

    @classmethod
    def from_stat(cls, path: str, kind: str, metadata: os.stat_result) -> TreeEntry:
        return cls(
            path=path,
            type=kind,
            dev=metadata.st_dev,
            inode=metadata.st_ino,
            size=metadata.st_size,
            mode=stat.S_IMODE(metadata.st_mode),
            atime_ns=metadata.st_atime_ns,
            mtime_ns=metadata.st_mtime_ns,
            ctime_ns=metadata.st_ctime_ns,
            nlink=metadata.st_nlink,
        )

    def consistency_tuple(self) -> tuple[object, ...]:
        return (
            self.path,
            self.type,
            self.dev,
            self.inode,
            self.size,
            self.mode,
            self.mtime_ns,
            self.ctime_ns,
            self.nlink,
        )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect capacity, explicitly create, or verify a coherent whole-root c-val backup."
    )
    parser.add_argument("--source", type=Path, default=Path("/data/continuous_validation"))
    parser.add_argument("--destination-root", type=Path)
    parser.add_argument("--safety-margin-percent", type=float, default=10.0)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--quiesced", action="store_true")
    parser.add_argument("--confirm-quiesced")
    parser.add_argument("--verify", metavar="BACKUP_DIR", type=Path)
    args = parser.parse_args(argv)
    if args.verify is not None:
        incompatible = (
            args.apply
            or args.confirm is not None
            or args.quiesced
            or args.confirm_quiesced is not None
            or args.destination_root is not None
        )
        if incompatible:
            parser.error("--verify cannot be combined with backup/apply options")
    if not math.isfinite(args.safety_margin_percent) or args.safety_margin_percent < 0:
        parser.error("--safety-margin-percent must be a finite non-negative number")
    return args


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _same_metadata(left: os.stat_result, right: os.stat_result) -> bool:
    fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
        "st_nlink",
    )
    return all(getattr(left, field) == getattr(right, field) for field in fields)


def _kind(metadata: os.stat_result, path: str) -> str:
    if stat.S_ISDIR(metadata.st_mode):
        return "directory"
    if stat.S_ISREG(metadata.st_mode):
        return "file"
    if stat.S_ISLNK(metadata.st_mode):
        label = "symlink"
    elif stat.S_ISFIFO(metadata.st_mode):
        label = "FIFO"
    elif stat.S_ISCHR(metadata.st_mode) or stat.S_ISBLK(metadata.st_mode):
        label = "device"
    elif stat.S_ISSOCK(metadata.st_mode):
        label = "socket"
    else:
        label = "special entry"
    raise BackupError(f"refusing {label} in backup source: {path}")


def inventory_tree(
    root_fd: int, *, exclude_backups: bool, reject_sidecars: bool = True
) -> dict[str, TreeEntry]:
    entries: dict[str, TreeEntry] = {}

    def visit(directory_fd: int, relative: str) -> None:
        before = os.fstat(directory_fd)
        if not stat.S_ISDIR(before.st_mode):
            raise BackupError(f"source directory changed type during inventory: {relative}")
        entries[relative] = TreeEntry.from_stat(relative, "directory", before)
        names = os.listdir(directory_fd)
        for name in sorted(names):
            try:
                name.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise BackupError(f"source name is not valid UTF-8: {relative}/{name!r}") from exc
            if relative == "." and exclude_backups and name == "backups":
                continue
            child_relative = name if relative == "." else f"{relative}/{name}"
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            kind = _kind(metadata, child_relative)
            if kind == "directory":
                child_fd = os.open(name, DIRECTORY_FLAGS, dir_fd=directory_fd)
                try:
                    opened = os.fstat(child_fd)
                    if not _same_metadata(metadata, opened):
                        raise BackupError(
                            f"source directory changed while opening: {child_relative}"
                        )
                    visit(child_fd, child_relative)
                finally:
                    os.close(child_fd)
            else:
                if reject_sidecars and child_relative.endswith(SIDECAR_SUFFIXES):
                    raise BackupError(
                        "live SQLite sidecar present; checkpoint and quiesce writers first: "
                        f"{child_relative}"
                    )
                entries[child_relative] = TreeEntry.from_stat(
                    child_relative, "file", metadata
                )
        after = os.fstat(directory_fd)
        if not _same_metadata(before, after):
            raise BackupError(f"source directory changed during inventory: {relative}")

    visit(root_fd, ".")
    return entries


def _files(inventory: dict[str, TreeEntry]) -> list[TreeEntry]:
    return sorted(
        (entry for entry in inventory.values() if entry.type == "file"),
        key=lambda entry: entry.path,
    )


def hardlink_groups(inventory: dict[str, TreeEntry]) -> dict[tuple[int, int], list[TreeEntry]]:
    groups: dict[tuple[int, int], list[TreeEntry]] = {}
    for entry in _files(inventory):
        groups.setdefault((entry.dev, entry.inode), []).append(entry)
    for group in groups.values():
        expected_links = group[0].nlink
        if any(entry.nlink != expected_links for entry in group):
            raise BackupError(f"inconsistent hardlink metadata: {group[0].path}")
        if expected_links != len(group):
            raise BackupError(
                "hardlink escapes the included source tree and cannot be preserved completely: "
                f"{group[0].path} (nlink={expected_links}, included={len(group)})"
            )
    return groups


def unique_bytes(inventory: dict[str, TreeEntry]) -> int:
    return sum(group[0].size for group in hardlink_groups(inventory).values())


def required_bytes(apparent_unique_bytes: int, margin_percent: float) -> int:
    return math.ceil(apparent_unique_bytes * (1.0 + margin_percent / 100.0))


def inventory_digest(inventory: dict[str, TreeEntry]) -> str:
    values = [
        list(inventory[path].consistency_tuple())
        for path in sorted(inventory)
    ]
    payload = json.dumps(values, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def inventories_equal(
    left: dict[str, TreeEntry], right: dict[str, TreeEntry]
) -> bool:
    return set(left) == set(right) and all(
        left[path].consistency_tuple() == right[path].consistency_tuple()
        for path in left
    )


def _existing_destination_ancestor(destination_root: Path) -> tuple[Path, int]:
    current = destination_root
    missing = 0
    while True:
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            missing += 1
            parent = current.parent
            if parent == current:
                raise BackupError(f"no existing destination ancestor: {destination_root}")
            current = parent
            continue
        if not stat.S_ISDIR(metadata.st_mode):
            raise BackupError(f"destination ancestor is not a directory: {current}")
        break
    if missing > 1:
        raise BackupError(
            "destination root's immediate parent must already exist: "
            f"{destination_root.parent}"
        )
    opened, descriptor = open_directory_no_symlinks(current)
    if opened != current:
        os.close(descriptor)
        raise BackupError(f"destination path is not lexical-canonical: {current}")
    return current, descriptor


def available_bytes(descriptor: int) -> int:
    override = os.environ.get("_CVAL_BACKUP_TEST_FREE_BYTES")
    if override is not None:
        try:
            value = int(override)
        except ValueError as exc:
            raise BackupError("invalid test free-byte override") from exc
        if value < 0:
            raise BackupError("invalid test free-byte override")
        return value
    values = os.fstatvfs(descriptor)
    return values.f_bavail * values.f_frsize


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def validate_layout(source: Path, destination_root: Path) -> None:
    if source == destination_root:
        raise BackupError("destination root must differ from source")
    if _is_within(destination_root, source) and destination_root != source / "backups":
        raise BackupError(
            f"a destination inside source must be exactly {source / 'backups'}"
        )


def capacity_plan(
    source_fd: int,
    destination_fd: int,
    inventory: dict[str, TreeEntry],
    margin_percent: float,
    *,
    destination_inside_source: bool,
) -> dict[str, object]:
    apparent = unique_bytes(inventory)
    required = required_bytes(apparent, margin_percent)
    free = available_bytes(destination_fd)
    return {
        "source_file_count": len(_files(inventory)),
        "apparent_unique_bytes": apparent,
        "destination_free_bytes": free,
        "safety_margin_percent": margin_percent,
        "required_bytes": required,
        "capacity_sufficient": free >= required,
        "same_filesystem": os.fstat(source_fd).st_dev == os.fstat(destination_fd).st_dev,
        "same_source_storage": destination_inside_source,
    }


def print_inspection(
    source: Path,
    destination: Path,
    plan: dict[str, object],
) -> None:
    print("c-val whole-root backup inspection (created nothing)")
    print(f"source: {source}")
    print(f"destination: {destination}")
    print("excluded: backups/")
    print(f"source file count: {plan['source_file_count']}")
    print(f"apparent unique bytes (hardlinks deduplicated): {plan['apparent_unique_bytes']}")
    print(f"destination filesystem free bytes: {plan['destination_free_bytes']}")
    print(f"safety margin percent: {plan['safety_margin_percent']}")
    print(f"required bytes including safety margin: {plan['required_bytes']}")
    print(f"capacity sufficient: {'yes' if plan['capacity_sufficient'] else 'NO'}")
    if plan["same_filesystem"]:
        print(
            "WARNING: destination is on the source filesystem; a same-PVC backup is "
            "NOT independent disaster recovery."
        )
    print("RECOMMENDATION: use --destination-root on independent external storage.")
    print(
        "APPLY REQUIRES: stop validation ingestion and all other source writers, then "
        "pass --apply --confirm backup --quiesced "
        "--confirm-quiesced writers-stopped."
    )


def _assert_entry(descriptor: int, expected: TreeEntry, operation: str) -> os.stat_result:
    metadata = os.fstat(descriptor)
    actual = TreeEntry.from_stat(expected.path, "file", metadata)
    if actual.consistency_tuple() != expected.consistency_tuple():
        raise BackupError(f"source changed {operation}: {expected.path}")
    return metadata


def _open_source_file(root_fd: int, entry: TreeEntry) -> int:
    parent_fd, name = open_parent_at(root_fd, entry.path)
    try:
        descriptor = os.open(name, FILE_READ_FLAGS, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)
    try:
        _assert_entry(descriptor, entry, "before copy")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _sha256_fd(descriptor: int) -> str:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while True:
        chunk = os.read(descriptor, READ_CHUNK)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)


def _sha256_path(path: Path) -> str:
    descriptor = os.open(path, FILE_READ_FLAGS)
    try:
        return _sha256_fd(descriptor)
    finally:
        os.close(descriptor)


def _sqlite_magic(descriptor: int) -> bool:
    return os.pread(descriptor, 16, 0) == b"SQLite format 3\x00"


def _sqlite_quick_check(path: Path) -> None:
    uri = f"file:{path.as_posix()}?mode=ro&immutable=1"
    try:
        with closing(sqlite3.connect(uri, uri=True)) as connection:
            connection.execute("PRAGMA query_only=ON")
            rows = connection.execute("PRAGMA quick_check").fetchall()
    except sqlite3.Error as exc:
        raise BackupError(f"SQLite quick_check failed for {path}: {exc}") from exc
    if rows != [("ok",)]:
        raise BackupError(f"SQLite quick_check failed for {path}: {rows!r}")


def _copy_regular(source_fd: int, target: Path, entry: TreeEntry) -> tuple[int, str]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    target_fd = os.open(target, flags, 0o600)
    digest = hashlib.sha256()
    copied = 0
    try:
        os.lseek(source_fd, 0, os.SEEK_SET)
        while True:
            chunk = os.read(source_fd, READ_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(target_fd, view)
                if written <= 0:
                    raise BackupError(f"short write while copying: {entry.path}")
                copied += written
                view = view[written:]
        if copied != entry.size:
            raise BackupError(f"source size changed while copying: {entry.path}")
        os.fchmod(target_fd, entry.mode)
        os.utime(target_fd, ns=(entry.atime_ns, entry.mtime_ns))
        os.fsync(target_fd)
    finally:
        os.close(target_fd)
    return copied, digest.hexdigest()


def _copy_sqlite(source_fd: int, target: Path, entry: TreeEntry) -> tuple[int, str]:
    reserve_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    reserve_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    reserve_fd = os.open(target, reserve_flags, 0o600)
    os.close(reserve_fd)
    source_uri = f"file:/proc/self/fd/{source_fd}?mode=ro&immutable=1"
    try:
        with closing(sqlite3.connect(source_uri, uri=True)) as source_db:
            source_db.execute("PRAGMA query_only=ON")
            with closing(sqlite3.connect(target)) as destination_db:
                source_db.backup(destination_db)
    except sqlite3.Error as exc:
        raise BackupError(f"SQLite online backup failed for {entry.path}: {exc}") from exc
    os.chmod(target, entry.mode, follow_symlinks=False)
    os.utime(target, ns=(entry.atime_ns, entry.mtime_ns), follow_symlinks=False)
    _sqlite_quick_check(target)
    metadata = os.stat(target, follow_symlinks=False)
    return metadata.st_size, _sha256_path(target)


def _source_identity(entry: TreeEntry) -> dict[str, int]:
    return {
        "dev": entry.dev,
        "inode": entry.inode,
        "size": entry.size,
        "mtime_ns": entry.mtime_ns,
        "ctime_ns": entry.ctime_ns,
        "nlink": entry.nlink,
    }


def _apply_directory_metadata(staging: Path, directories: Iterable[TreeEntry]) -> None:
    ordered = sorted(
        directories,
        key=lambda entry: (entry.path == ".", -entry.path.count("/"), entry.path),
    )
    for entry in ordered:
        target = staging if entry.path == "." else staging / entry.path
        os.chmod(target, entry.mode, follow_symlinks=False)
        os.utime(
            target,
            ns=(entry.atime_ns, entry.mtime_ns),
            follow_symlinks=False,
        )


def _cleanup_staging(destination_fd: int, staging_fd: int | None, staging_name: str) -> None:
    if staging_fd is None:
        return
    try:
        named = os.stat(staging_name, dir_fd=destination_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if (named.st_dev, named.st_ino) != descriptor_identity(staging_fd):
        raise BackupError("staging identity changed; refusing to remove an unknown tree")
    remove_tree_at(destination_fd, staging_name)


def _publish_staging(destination_fd: int, staging_name: str, final_name: str) -> None:
    """Publish staging without overwrite, including owner-only FS fallback."""

    try:
        rename_noreplace_at(
            destination_fd,
            staging_name,
            destination_fd,
            final_name,
        )
        return
    except OSError as exc:
        unsupported = {errno.EINVAL, errno.ENOSYS}
        if hasattr(errno, "EOPNOTSUPP"):
            unsupported.add(errno.EOPNOTSUPP)
        if exc.errno not in unsupported:
            raise
    metadata = os.fstat(destination_fd)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise BackupError(
            "backup publication fallback requires an owner-only destination directory"
        )
    staging_fd = os.open(staging_name, DIRECTORY_FLAGS, dir_fd=destination_fd)
    reservation_fd: int | None = None
    renamed = False
    try:
        staging_identity = descriptor_identity(staging_fd)
        reservation_fd = mkdir_exact_at(destination_fd, final_name, 0o700)
        reservation_identity = descriptor_identity(reservation_fd)
        os.fsync(destination_fd)
        if os.listdir(reservation_fd):
            raise BackupError("backup destination reservation is not empty")
        named_staging = os.stat(
            staging_name, dir_fd=destination_fd, follow_symlinks=False
        )
        named_reservation = os.stat(
            final_name, dir_fd=destination_fd, follow_symlinks=False
        )
        if (named_staging.st_dev, named_staging.st_ino) != staging_identity:
            raise BackupError("backup staging identity changed before publication")
        if (
            named_reservation.st_dev,
            named_reservation.st_ino,
        ) != reservation_identity:
            raise BackupError("backup destination reservation identity changed")
        os.rename(
            staging_name,
            final_name,
            src_dir_fd=destination_fd,
            dst_dir_fd=destination_fd,
        )
        renamed = True
        published = os.stat(
            final_name, dir_fd=destination_fd, follow_symlinks=False
        )
        if (published.st_dev, published.st_ino) != staging_identity:
            raise BackupError("backup publication identity mismatch")
    except BaseException:
        try:
            named = os.stat(final_name, dir_fd=destination_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            named_identity = (named.st_dev, named.st_ino)
            if renamed and named_identity == descriptor_identity(staging_fd):
                remove_tree_at(destination_fd, final_name)
            elif (
                reservation_fd is not None
                and named_identity == descriptor_identity(reservation_fd)
            ):
                os.rmdir(final_name, dir_fd=destination_fd)
        raise
    finally:
        if reservation_fd is not None:
            os.close(reservation_fd)
        os.close(staging_fd)


def _test_hook_after_copy(source: Path) -> None:
    relative = os.environ.get("_CVAL_BACKUP_TEST_MUTATE_RELATIVE")
    if not relative:
        return
    parts = safe_relative_parts(relative, field_name="test mutation path")
    with (source.joinpath(*parts)).open("ab") as handle:
        handle.write(b"mutation")


def create_backup(
    source: Path,
    source_fd: int,
    source_identity: tuple[int, int],
    destination_root: Path,
    destination: Path,
    destination_fd: int,
    inventory: dict[str, TreeEntry],
    plan: dict[str, object],
) -> dict[str, object]:
    started_at = utc_now()
    pre_digest = inventory_digest(inventory)
    groups = hardlink_groups(inventory)
    group_names = {
        key: f"hardlink-{index:06d}"
        for index, (key, values) in enumerate(sorted(groups.items()), start=1)
        if len(values) > 1
    }
    final_name = destination.name
    try:
        os.stat(final_name, dir_fd=destination_fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        raise BackupError(f"backup destination already exists: {destination}")

    staging_name = f".{final_name}.staging-{secrets.token_hex(12)}"
    staging_fd: int | None = None
    published = False
    copied_unique = 0
    file_records: list[dict[str, object]] = []
    representative: dict[tuple[int, int], tuple[Path, int, str, str, bool]] = {}

    old_handlers: dict[int, Any] = {}

    def interrupted(signum: int, _frame: object) -> None:
        raise BackupError(f"backup interrupted by signal {signum}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        old_handlers[signum] = signal.signal(signum, interrupted)

    try:
        staging_fd = mkdir_exact_at(destination_fd, staging_name, 0o700)
        staging = destination_root / staging_name
        directories = sorted(
            (entry for entry in inventory.values() if entry.type == "directory" and entry.path != "."),
            key=lambda entry: (entry.path.count("/"), entry.path),
        )
        for entry in directories:
            os.mkdir(staging / entry.path, 0o700)

        for entry in _files(inventory):
            target = staging / entry.path
            key = (entry.dev, entry.inode)
            hardlink_group = group_names.get(key)
            if key in representative:
                original, backup_size, backup_digest, source_digest, sqlite_file = representative[key]
                os.link(original, target, follow_symlinks=False)
                method = "hardlink"
            else:
                source_file_fd = _open_source_file(source_fd, entry)
                try:
                    source_digest = _sha256_fd(source_file_fd)
                    _assert_entry(source_file_fd, entry, "while hashing")
                    sqlite_file = _sqlite_magic(source_file_fd)
                    if sqlite_file:
                        backup_size, backup_digest = _copy_sqlite(
                            source_file_fd, target, entry
                        )
                        method = "sqlite-backup"
                    else:
                        backup_size, backup_digest = _copy_regular(
                            source_file_fd, target, entry
                        )
                        method = "regular-copy"
                    _assert_entry(source_file_fd, entry, "during copy")
                    if _sha256_fd(source_file_fd) != source_digest:
                        raise BackupError(
                            f"source content changed during copy: {entry.path}"
                        )
                    _assert_entry(source_file_fd, entry, "after copy revalidation")
                finally:
                    os.close(source_file_fd)
                representative[key] = (
                    target,
                    backup_size,
                    backup_digest,
                    source_digest,
                    sqlite_file,
                )
                copied_unique += 1
                interrupt_after = os.environ.get("_CVAL_BACKUP_TEST_INTERRUPT_AFTER_FILES")
                if interrupt_after is not None and copied_unique >= int(interrupt_after):
                    raise BackupError("injected interruption after file copy")
            file_records.append(
                {
                    "path": entry.path,
                    "size": backup_size,
                    "mode": entry.mode,
                    "mtime_ns": entry.mtime_ns,
                    "sha256": backup_digest,
                    "method": method,
                    "sqlite": sqlite_file,
                    "hardlink_group": hardlink_group,
                    "source_identity": _source_identity(entry),
                    "source_size": entry.size,
                    "source_sha256": source_digest,
                    "source_dev": entry.dev,
                    "source_inode": entry.inode,
                    "source_mtime_ns": entry.mtime_ns,
                    "backup_size": backup_size,
                    "backup_sha256": backup_digest,
                }
            )

        _test_hook_after_copy(source)
        post_inventory = inventory_tree(source_fd, exclude_backups=True)
        post_digest = inventory_digest(post_inventory)
        if not inventories_equal(inventory, post_inventory) or pre_digest != post_digest:
            raise BackupError("source tree changed during backup; staged backup rejected")
        assert_lexical_directory_identity(source, source_identity)

        directory_records = [
            {
                "path": entry.path,
                "mode": entry.mode,
                "mtime_ns": entry.mtime_ns,
                "source_identity": _source_identity(entry),
            }
            for entry in sorted(
                (value for value in inventory.values() if value.type == "directory"),
                key=lambda value: value.path,
            )
        ]
        backup_unique_bytes = sum(value[1] for value in representative.values())
        completed_at = utc_now()
        manifest = {
            "schema": SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "source_root": str(source),
            "destination": str(destination),
            "started_at": started_at,
            "completed_at": completed_at,
            "excluded": EXCLUDED,
            "quiescence": {
                "declared": True,
                "confirmation": QUIESCENCE_CONFIRMATION,
                "contract": "validation ingestion and all other source writers stopped",
            },
            "consistency": {
                "method": "operator-quiescence + pre/post identity inventory + no-follow copies + SQLite online backup",
                "pre_inventory_sha256": pre_digest,
                "post_inventory_sha256": post_digest,
            },
            "capacity": plan,
            "source_file_count": len(file_records),
            "total_unique_bytes": plan["apparent_unique_bytes"],
            "backup_unique_bytes": backup_unique_bytes,
            "directories": directory_records,
            "files": file_records,
        }
        manifest_payload = (
            json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        ).encode("utf-8")
        manifest_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        manifest_flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        manifest_fd = os.open(staging / "manifest.json", manifest_flags, 0o600)
        try:
            os.fchmod(manifest_fd, 0o600)
            view = memoryview(manifest_payload)
            while view:
                written = os.write(manifest_fd, view)
                if written <= 0:
                    raise BackupError("short write while writing manifest")
                view = view[written:]
            os.fsync(manifest_fd)
        finally:
            os.close(manifest_fd)

        _apply_directory_metadata(
            staging,
            (entry for entry in inventory.values() if entry.type == "directory"),
        )
        os.fsync(staging_fd)
        _publish_staging(destination_fd, staging_name, final_name)
        published = True
        os.fsync(destination_fd)
        return {
            "ok": True,
            "backup": str(destination),
            "manifest": str(destination / "manifest.json"),
            "files": len(file_records),
            "total_unique_bytes": plan["apparent_unique_bytes"],
            "verification": "not yet run; use --verify",
        }
    finally:
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
        try:
            if not published:
                _cleanup_staging(destination_fd, staging_fd, staging_name)
        finally:
            if staging_fd is not None:
                os.close(staging_fd)


def _safe_manifest_path(raw: object, *, field: str) -> str:
    if not isinstance(raw, str):
        raise BackupError(f"manifest {field} path is invalid")
    parts = safe_relative_parts(raw, field_name=f"manifest {field} path")
    normalized = Path(*parts).as_posix()
    if normalized != raw or normalized == "manifest.json":
        raise BackupError(f"manifest {field} path is unsafe: {raw!r}")
    return normalized


def _require_int(value: object, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise BackupError(f"manifest {field} is invalid")
    return value


def _require_identity(value: object, field: str) -> dict[str, int]:
    keys = {"dev", "inode", "size", "mtime_ns", "ctime_ns", "nlink"}
    if not isinstance(value, dict) or set(value) != keys:
        raise BackupError(f"manifest {field} identity is invalid")
    return {key: _require_int(value[key], f"{field}.{key}") for key in keys}


def _load_manifest(backup: Path, backup_fd: int) -> dict[str, Any]:
    payload = read_regular_file_at(
        backup_fd,
        "manifest.json",
        max_bytes=MAX_MANIFEST_BYTES,
        nonblocking=True,
    )
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupError("manifest is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise BackupError("manifest must be a JSON object")
    required = {
        "schema",
        "schema_version",
        "source_root",
        "destination",
        "started_at",
        "completed_at",
        "excluded",
        "quiescence",
        "consistency",
        "capacity",
        "source_file_count",
        "total_unique_bytes",
        "backup_unique_bytes",
        "directories",
        "files",
    }
    if set(value) != required:
        raise BackupError("manifest has missing or unexpected top-level fields")
    if value["schema"] != SCHEMA or value["schema_version"] != SCHEMA_VERSION:
        raise BackupError("manifest schema is not supported")
    if value["excluded"] != EXCLUDED:
        raise BackupError("manifest exclusion set is invalid")
    if not isinstance(value["source_root"], str) or not Path(value["source_root"]).is_absolute():
        raise BackupError("manifest source_root is invalid")
    if not isinstance(value["destination"], str) or not Path(value["destination"]).is_absolute():
        raise BackupError("manifest destination is invalid")
    if lexical_absolute(value["destination"]) != backup:
        raise BackupError("manifest destination does not match the verified directory")
    for field in ("started_at", "completed_at"):
        if not isinstance(value[field], str) or not value[field].endswith("Z"):
            raise BackupError(f"manifest {field} is invalid")
    quiescence = value["quiescence"]
    if not isinstance(quiescence, dict) or set(quiescence) != {
        "declared",
        "confirmation",
        "contract",
    } or quiescence.get("declared") is not True:
        raise BackupError("manifest lacks a quiescence declaration")
    if quiescence.get("confirmation") != QUIESCENCE_CONFIRMATION:
        raise BackupError("manifest quiescence confirmation is invalid")
    consistency = value["consistency"]
    if not isinstance(consistency, dict) or set(consistency) != {
        "method",
        "pre_inventory_sha256",
        "post_inventory_sha256",
    } or not isinstance(consistency.get("method"), str):
        raise BackupError("manifest consistency record is invalid")
    before = consistency.get("pre_inventory_sha256")
    after = consistency.get("post_inventory_sha256")
    if (
        not isinstance(before, str)
        or len(before) != 64
        or before != after
        or any(character not in "0123456789abcdef" for character in before)
    ):
        raise BackupError("manifest pre/post inventory evidence is invalid")
    if not isinstance(value["files"], list) or not isinstance(value["directories"], list):
        raise BackupError("manifest entry lists are invalid")
    capacity = value["capacity"]
    if not isinstance(capacity, dict) or capacity.get("capacity_sufficient") is not True:
        raise BackupError("manifest capacity record is invalid")
    return value


def _open_backup_file(backup_fd: int, relative: str) -> tuple[int, os.stat_result]:
    parent_fd, name = open_parent_at(backup_fd, relative)
    try:
        descriptor = os.open(name, FILE_READ_FLAGS, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise BackupError(f"backup entry is not a regular file: {relative}")
    return descriptor, metadata


def verify_backup(backup: Path) -> dict[str, object]:
    backup, backup_fd = open_directory_no_symlinks(backup)
    try:
        manifest = _load_manifest(backup, backup_fd)
        actual = inventory_tree(
            backup_fd, exclude_backups=False, reject_sidecars=False
        )
        file_items: dict[str, dict[str, Any]] = {}
        for raw in manifest["files"]:
            if not isinstance(raw, dict):
                raise BackupError("manifest file entry is invalid")
            required = {
                "path",
                "size",
                "mode",
                "mtime_ns",
                "sha256",
                "method",
                "sqlite",
                "hardlink_group",
                "source_identity",
                "source_size",
                "source_sha256",
                "source_dev",
                "source_inode",
                "source_mtime_ns",
                "backup_size",
                "backup_sha256",
            }
            if set(raw) != required:
                raise BackupError("manifest file entry has missing or unexpected fields")
            path = _safe_manifest_path(raw["path"], field="file")
            if path in file_items:
                raise BackupError(f"duplicate manifest file path: {path}")
            file_items[path] = raw
        directory_items: dict[str, dict[str, Any]] = {}
        for raw in manifest["directories"]:
            if not isinstance(raw, dict) or set(raw) != {
                "path",
                "mode",
                "mtime_ns",
                "source_identity",
            }:
                raise BackupError("manifest directory entry is invalid")
            raw_path = raw.get("path")
            if raw_path == ".":
                path = "."
            else:
                path = _safe_manifest_path(raw_path, field="directory")
            if path in directory_items:
                raise BackupError(f"duplicate manifest directory path: {path}")
            directory_items[path] = raw

        actual_files = {entry.path for entry in actual.values() if entry.type == "file"}
        expected_files = set(file_items) | {"manifest.json"}
        actual_directories = {
            entry.path for entry in actual.values() if entry.type == "directory"
        }
        if actual_files != expected_files:
            missing = sorted(expected_files - actual_files)
            unexpected = sorted(actual_files - expected_files)
            raise BackupError(
                f"backup file set mismatch (missing={missing}, unexpected={unexpected})"
            )
        if actual_directories != set(directory_items):
            missing = sorted(set(directory_items) - actual_directories)
            unexpected = sorted(actual_directories - set(directory_items))
            raise BackupError(
                f"backup directory set mismatch (missing={missing}, unexpected={unexpected})"
            )
        if actual["manifest.json"].mode != 0o600:
            raise BackupError("manifest.json mode is not 0600")

        hardlinks: dict[str, list[tuple[str, tuple[int, int], int]]] = {}
        unique_backup_bytes = 0
        unique_source_bytes = 0
        seen_backup_inodes: set[tuple[int, int]] = set()
        seen_source_inodes: set[tuple[int, int]] = set()
        for path, item in sorted(file_items.items()):
            size = _require_int(item["size"], f"files[{path}].size")
            mode = _require_int(item["mode"], f"files[{path}].mode")
            mtime_ns = _require_int(item["mtime_ns"], f"files[{path}].mtime_ns")
            if mode > 0o7777:
                raise BackupError(f"manifest file mode is invalid: {path}")
            digest = item["sha256"]
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise BackupError(f"manifest checksum is invalid: {path}")
            if item["method"] not in {"regular-copy", "sqlite-backup", "hardlink"}:
                raise BackupError(f"manifest copy method is invalid: {path}")
            if not isinstance(item["sqlite"], bool):
                raise BackupError(f"manifest SQLite marker is invalid: {path}")
            source_identity = _require_identity(
                item["source_identity"], f"files[{path}].source"
            )
            source_sha256 = item["source_sha256"]
            if (
                not isinstance(source_sha256, str)
                or len(source_sha256) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in source_sha256
                )
            ):
                raise BackupError(f"manifest source checksum is invalid: {path}")
            source_fields = {
                "size": "source_size",
                "dev": "source_dev",
                "inode": "source_inode",
                "mtime_ns": "source_mtime_ns",
            }
            for identity_field, evidence_field in source_fields.items():
                evidence_value = _require_int(
                    item[evidence_field], f"files[{path}].{evidence_field}"
                )
                if evidence_value != source_identity[identity_field]:
                    raise BackupError(
                        f"manifest source evidence is inconsistent: {path}"
                    )
            if _require_int(item["backup_size"], f"files[{path}].backup_size") != size:
                raise BackupError(f"manifest backup size evidence is inconsistent: {path}")
            if item["backup_sha256"] != digest:
                raise BackupError(
                    f"manifest backup checksum evidence is inconsistent: {path}"
                )
            source_key = (
                _require_int(source_identity["dev"], f"files[{path}].source.dev"),
                _require_int(source_identity["inode"], f"files[{path}].source.inode"),
            )
            source_size = _require_int(
                source_identity["size"], f"files[{path}].source.size"
            )
            descriptor, metadata = _open_backup_file(backup_fd, path)
            try:
                actual_digest = _sha256_fd(descriptor)
                after = os.fstat(descriptor)
                if not _same_metadata(metadata, after):
                    raise BackupError(f"backup file changed during verification: {path}")
                if metadata.st_size != size or stat.S_IMODE(metadata.st_mode) != mode:
                    raise BackupError(f"backup size/mode mismatch: {path}")
                if metadata.st_mtime_ns != mtime_ns:
                    raise BackupError(f"backup mtime mismatch: {path}")
                if actual_digest != digest:
                    raise BackupError(f"backup checksum mismatch: {path}")
                sqlite_magic = _sqlite_magic(descriptor)
            finally:
                os.close(descriptor)
            if sqlite_magic != item["sqlite"]:
                raise BackupError(f"backup SQLite marker mismatch: {path}")
            if sqlite_magic:
                _sqlite_quick_check(backup / path)
            backup_key = (metadata.st_dev, metadata.st_ino)
            if backup_key not in seen_backup_inodes:
                unique_backup_bytes += metadata.st_size
                seen_backup_inodes.add(backup_key)
            if source_key not in seen_source_inodes:
                unique_source_bytes += source_size
                seen_source_inodes.add(source_key)
            group = item["hardlink_group"]
            if group is None:
                if metadata.st_nlink != 1 or item["method"] == "hardlink":
                    raise BackupError(f"unexpected hardlink for backup file: {path}")
            elif not isinstance(group, str) or not group:
                raise BackupError(f"invalid hardlink group for backup file: {path}")
            else:
                hardlinks.setdefault(group, []).append((path, backup_key, metadata.st_nlink))

        for group, members in hardlinks.items():
            if len(members) < 2:
                raise BackupError(f"hardlink group has fewer than two members: {group}")
            identities = {member[1] for member in members}
            links = {member[2] for member in members}
            if len(identities) != 1 or links != {len(members)}:
                raise BackupError(f"hardlink group was not preserved exactly: {group}")
            if sum(item["method"] != "hardlink" for path, _, _ in members for item in [file_items[path]]) != 1:
                raise BackupError(f"hardlink group has an invalid representative: {group}")
            source_identities = {
                (
                    file_items[path]["source_identity"]["dev"],
                    file_items[path]["source_identity"]["inode"],
                )
                for path, _, _ in members
            }
            source_nlinks = {
                file_items[path]["source_identity"]["nlink"]
                for path, _, _ in members
            }
            if len(source_identities) != 1 or source_nlinks != {len(members)}:
                raise BackupError(f"hardlink source identity is inconsistent: {group}")

        for path, item in directory_items.items():
            mode = _require_int(item["mode"], f"directories[{path}].mode")
            mtime_ns = _require_int(item["mtime_ns"], f"directories[{path}].mtime_ns")
            _require_identity(item["source_identity"], f"directories[{path}].source")
            entry = actual[path]
            if entry.mode != mode or entry.mtime_ns != mtime_ns:
                raise BackupError(f"backup directory metadata mismatch: {path}")

        if _require_int(manifest["source_file_count"], "source_file_count") != len(file_items):
            raise BackupError("manifest source file count is inconsistent")
        if _require_int(manifest["total_unique_bytes"], "total_unique_bytes") != unique_source_bytes:
            raise BackupError("manifest source unique-byte count is inconsistent")
        if _require_int(manifest["backup_unique_bytes"], "backup_unique_bytes") != unique_backup_bytes:
            raise BackupError("manifest backup unique-byte count is inconsistent")
        return {
            "ok": True,
            "mode": "verify",
            "backup": str(backup),
            "files": len(file_items),
            "total_unique_bytes": unique_source_bytes,
            "restore_ready": True,
        }
    finally:
        os.close(backup_fd)


def _ensure_destination_root(destination_root: Path) -> int:
    try:
        opened, descriptor = open_directory_no_symlinks(destination_root)
    except FileNotFoundError:
        parent, parent_fd = open_directory_no_symlinks(destination_root.parent)
        if parent != destination_root.parent:
            os.close(parent_fd)
            raise BackupError("destination parent is not lexical-canonical")
        try:
            descriptor = mkdir_exact_at(parent_fd, destination_root.name, 0o700)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return descriptor
    if opened != destination_root:
        os.close(descriptor)
        raise BackupError("destination root is not lexical-canonical")
    return descriptor


def run(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.verify is not None:
        result = verify_backup(lexical_absolute(args.verify))
        print(json.dumps(result, sort_keys=True))
        return 0

    if args.apply and args.confirm != CONFIRMATION:
        raise BackupError("apply requires exact --confirm backup")
    if args.apply and (
        not args.quiesced or args.confirm_quiesced != QUIESCENCE_CONFIRMATION
    ):
        raise BackupError(
            "apply requires exact --quiesced --confirm-quiesced writers-stopped"
        )

    source = lexical_absolute(args.source)
    destination_root = lexical_absolute(args.destination_root or source / "backups")
    validate_layout(source, destination_root)
    timestamp = os.environ.get("CVAL_BACKUP_TIMESTAMP") or datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")
    if (
        len(timestamp) != 16
        or timestamp[8] != "T"
        or timestamp[-1] != "Z"
        or not (timestamp[:8] + timestamp[9:15]).isdigit()
    ):
        raise BackupError("CVAL_BACKUP_TIMESTAMP must match YYYYMMDDTHHMMSSZ")
    destination = destination_root / f"cval-backup-{timestamp}"

    source, source_fd = open_directory_no_symlinks(source)
    destination_ancestor_fd: int | None = None
    destination_fd: int | None = None
    try:
        source_identity = descriptor_identity(source_fd)
        initial_inventory = inventory_tree(source_fd, exclude_backups=True)
        if "manifest.json" in initial_inventory:
            raise BackupError("source root manifest.json conflicts with backup manifest name")
        _ancestor, destination_ancestor_fd = _existing_destination_ancestor(
            destination_root
        )
        initial_plan = capacity_plan(
            source_fd,
            destination_ancestor_fd,
            initial_inventory,
            args.safety_margin_percent,
            destination_inside_source=_is_within(destination_root, source),
        )
        if not args.apply:
            print_inspection(source, destination, initial_plan)
            return 0 if initial_plan["capacity_sufficient"] else 1
        if not initial_plan["capacity_sufficient"]:
            raise BackupError(
                "insufficient destination capacity; refusing before creating any backup data"
            )

        os.close(destination_ancestor_fd)
        destination_ancestor_fd = None
        destination_fd = _ensure_destination_root(destination_root)
        inventory = inventory_tree(source_fd, exclude_backups=True)
        if not inventories_equal(inventory, initial_inventory):
            initial_without_root = dict(initial_inventory)
            current_without_root = dict(inventory)
            del initial_without_root["."]
            del current_without_root["."]
            # Creating source/backups changes only the excluded root entry and
            # source-root directory metadata. Anything else is a writer race.
            if not inventories_equal(initial_without_root, current_without_root):
                raise BackupError("source tree changed before backup staging")
        plan = capacity_plan(
            source_fd,
            destination_fd,
            inventory,
            args.safety_margin_percent,
            destination_inside_source=_is_within(destination_root, source),
        )
        if not plan["capacity_sufficient"]:
            raise BackupError("destination capacity became insufficient before staging")
        result = create_backup(
            source,
            source_fd,
            source_identity,
            destination_root,
            destination,
            destination_fd,
            inventory,
            plan,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        if destination_ancestor_fd is not None:
            os.close(destination_ancestor_fd)
        if destination_fd is not None:
            os.close(destination_fd)
        os.close(source_fd)


def main() -> None:
    try:
        raise SystemExit(run(sys.argv[1:]))
    except (BackupError, OSError, ValueError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
