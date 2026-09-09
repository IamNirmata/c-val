#!/usr/bin/env python3
"""Inventory PVC paths and explicitly archive allowlisted legacy entries."""

from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
import time
from collections import Counter
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path


COMPONENTS = (
    "numerical_correctness", "compute_performance",
    "collective_performance", "overlap_performance",
)
ARCHIVE_ENTRIES = {
    "baselines": "retired_baselines_and_classifications",
    "deeplearning_unit_test": "legacy_dl_source_and_results",
    "dltest.tar.gz": "legacy_dl_results.tar.gz",
    "old-files": "legacy_bundles",
    "test1": "legacy_scratch_tests",
}


def entry_identity(path: Path | str, *, dir_fd: int | None = None) -> dict:
    metadata = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
    if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
        raise ValueError(f"archive source must be a regular file or directory: {path}")
    return {
        "device": metadata.st_dev, "inode": metadata.st_ino,
        "mode": metadata.st_mode, "uid": metadata.st_uid, "gid": metadata.st_gid,
        "size": metadata.st_size, "mtime_ns": metadata.st_mtime_ns,
    }


def archive_plan(root: Path, archive_name: str) -> dict:
    if not root.is_absolute() or root.resolve() != root or not root.is_dir():
        raise ValueError("root must be an existing canonical absolute directory")
    if re.fullmatch(r"[0-9]{8}_[0-9]{6}_P[DS]T", archive_name) is None:
        raise ValueError("archive name must be YYYYMMDD_HHMMSS_PDT or PST")
    archive = root / "archive"
    if archive.is_symlink() or (archive.exists() and not archive.is_dir()):
        raise ValueError("archive parent must be a real directory")
    target = archive / archive_name
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"archive destination already exists: {target}")
    references = receipt_references(root, seconds=10)
    for component, report in references.items():
        if report["state"] != "queried":
            raise ValueError(f"cannot verify {component} receipt references")
        if report["reference_counts"].get("outside_root_or_invalid", 0):
            raise ValueError(f"unresolved {component} receipt references")
        for source in ARCHIVE_ENTRIES:
            if report["reference_counts"].get(source, 0):
                raise ValueError(f"archive source is referenced by {component}: {source}")
    identity = root.stat()
    entries = []
    for source, destination in ARCHIVE_ENTRIES.items():
        path = root / source
        if not path.exists() and not path.is_symlink():
            continue
        source_identity = entry_identity(path)
        if source_identity["device"] != identity.st_dev or path.is_mount():
            raise ValueError(f"archive source is a mount boundary: {source}")
        entries.append({"source": source, "destination": destination, "identity": source_identity})
    if not entries:
        raise ValueError("no allowlisted legacy entries to archive")
    plan = {
        "schema_version": "cval.pvc-archive-plan.v1", "root": str(root),
        "root_device": identity.st_dev, "root_inode": identity.st_ino,
        "archive_name": archive_name, "entries": entries,
    }
    return plan | {"plan_sha256": plan_digest(plan)}


def plan_digest(plan: dict) -> str:
    payload = {name: value for name, value in plan.items() if name != "plan_sha256"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def rename_noreplace(source_fd: int, source: str, destination_fd: int, destination: str) -> None:
    library = ctypes.CDLL(None, use_errno=True)
    rename = getattr(library, "renameat2", None)
    if rename is None:
        raise OSError(errno.ENOSYS, "renameat2 is required; no unsafe fallback")
    rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    rename.restype = ctypes.c_int
    if rename(source_fd, os.fsencode(source), destination_fd, os.fsencode(destination), 1) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), destination)


@contextmanager
def locked_archive(root: Path, *, create: bool):
    if not root.is_absolute() or root.resolve() != root or not root.is_dir():
        raise ValueError("root must be an existing canonical absolute directory")
    descriptors = []
    try:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(root_fd)
        if create:
            try:
                os.mkdir("archive", 0o700, dir_fd=root_fd)
                os.fsync(root_fd)
            except FileExistsError:
                pass
        archive_fd = os.open("archive", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        descriptors.append(archive_fd)
        metadata = os.fstat(archive_fd)
        if metadata.st_dev != os.fstat(root_fd).st_dev:
            raise ValueError("archive must use the same filesystem as its source")
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("archive directory must be owner-controlled")
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
        lock_fd = os.open(".pvc-layout.lock", flags | (os.O_CREAT if create else 0), 0o600, dir_fd=archive_fd)
        descriptors.append(lock_fd)
        metadata = os.fstat(lock_fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("archive lock must be a private regular file")
        fcntl.lockf(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield root_fd, archive_fd
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def save_manifest(directory_fd: int, manifest: dict) -> None:
    temporary = ".manifest-" + secrets.token_hex(8) + ".tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, "manifest.json", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)
    finally:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass


def apply_archive(root: Path, name: str, expected_digest: str, *, confirm: str, sources_idle: str) -> dict:
    if confirm != "archive-legacy" or sources_idle != "idle":
        raise ValueError("archive requires --confirm archive-legacy --confirm-sources-idle idle")
    plan = archive_plan(root, name)
    if expected_digest != plan["plan_sha256"]:
        raise ValueError("archive plan changed; review a fresh plan before moving data")
    with locked_archive(root, create=True) as (root_fd, archive_fd):
        if archive_plan(root, name) != plan:
            raise ValueError("archive plan changed while acquiring the lock")
        if (os.fstat(root_fd).st_dev, os.fstat(root_fd).st_ino) != (plan["root_device"], plan["root_inode"]):
            raise ValueError("PVC root identity changed")
        os.mkdir(name, 0o700, dir_fd=archive_fd)
        os.fsync(archive_fd)
        directory_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=archive_fd)
        manifest = {"schema_version": "cval.pvc-archive.v1", "plan": plan, "state": "in_progress", "moved": [], "created_at": timestamp(time.time())}
        try:
            save_manifest(directory_fd, manifest)
            for entry in plan["entries"]:
                if entry_identity(entry["source"], dir_fd=root_fd) != entry["identity"]:
                    raise ValueError(f"archive source changed: {entry['source']}")
                rename_noreplace(root_fd, entry["source"], directory_fd, entry["destination"])
                os.fsync(root_fd)
                os.fsync(directory_fd)
                if entry_identity(entry["destination"], dir_fd=directory_fd) != entry["identity"]:
                    raise ValueError(f"archived identity changed: {entry['destination']}")
                manifest["moved"].append(entry["source"])
                save_manifest(directory_fd, manifest)
            manifest["state"] = "complete"
            save_manifest(directory_fd, manifest)
        except Exception as error:
            manifest.update(state="failed", error=str(error))
            save_manifest(directory_fd, manifest)
            raise
        finally:
            os.close(directory_fd)
    return manifest


def rollback_archive(root: Path, name: str, *, confirm: str, sources_idle: str) -> dict:
    if confirm != "restore-legacy" or sources_idle != "idle":
        raise ValueError("rollback requires --confirm restore-legacy --confirm-sources-idle idle")
    if re.fullmatch(r"[0-9]{8}_[0-9]{6}_P[DS]T", name) is None:
        raise ValueError("invalid archive name")
    with locked_archive(root, create=False) as (root_fd, archive_fd):
        directory_fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=archive_fd)
        try:
            descriptor = os.open("manifest.json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                if os.fstat(handle.fileno()).st_size > 1024 * 1024:
                    raise ValueError("oversized archive manifest")
                manifest = json.load(handle)
            plan = manifest["plan"]
            if manifest.get("schema_version") != "cval.pvc-archive.v1" or plan.get("schema_version") != "cval.pvc-archive-plan.v1" or plan.get("root") != str(root) or plan.get("archive_name") != name or plan_digest(plan) != plan.get("plan_sha256"):
                raise ValueError("invalid archive manifest identity")
            if (os.fstat(root_fd).st_dev, os.fstat(root_fd).st_ino) != (plan["root_device"], plan["root_inode"]):
                raise ValueError("PVC root identity changed")
            entries = plan["entries"]
            if not entries or len({entry['source'] for entry in entries}) != len(entries) or any(ARCHIVE_ENTRIES.get(entry["source"]) != entry["destination"] for entry in entries):
                raise ValueError("archive manifest contains a non-allowlisted move")
            restore = []
            for entry in reversed(entries):
                try:
                    original = entry_identity(entry["source"], dir_fd=root_fd)
                except FileNotFoundError:
                    original = None
                try:
                    archived = entry_identity(entry["destination"], dir_fd=directory_fd)
                except FileNotFoundError:
                    archived = None
                if original is not None:
                    if original != entry["identity"] or archived is not None:
                        raise FileExistsError(f"rollback refuses to overwrite: {entry['source']}")
                elif archived == entry["identity"]:
                    restore.append(entry)
                else:
                    raise ValueError(f"archived source is missing or changed: {entry['source']}")
            for entry in restore:
                if entry_identity(entry["destination"], dir_fd=directory_fd) != entry["identity"]:
                    raise ValueError(f"archived source changed: {entry['source']}")
                rename_noreplace(directory_fd, entry["destination"], root_fd, entry["source"])
                os.fsync(root_fd)
                os.fsync(directory_fd)
                manifest.setdefault("restored", []).append(entry["source"])
                save_manifest(directory_fd, manifest)
            manifest["state"] = "rolled_back"
            save_manifest(directory_fd, manifest)
            return manifest
        finally:
            os.close(directory_fd)


def timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds")


def entry_summary(path: Path, *, entry_limit: int, seconds: float) -> dict:
    metadata = path.lstat()
    result = {
        "name": path.name,
        "kind": "directory" if stat.S_ISDIR(metadata.st_mode) else "file",
        "mode": oct(stat.S_IMODE(metadata.st_mode)),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "inode": metadata.st_ino,
        "device": metadata.st_dev,
        "modified_at": timestamp(metadata.st_mtime),
        "sample": [],
        "entries_scanned": 0,
        "files": 0,
        "directories": 0,
        "symlinks": 0,
        "logical_bytes": 0,
        "scan_state": "complete",
    }
    if stat.S_ISLNK(metadata.st_mode):
        result.update(kind="symlink", target=os.readlink(path), symlinks=1)
        return result
    if not stat.S_ISDIR(metadata.st_mode):
        result.update(files=int(stat.S_ISREG(metadata.st_mode)), logical_bytes=metadata.st_size)
        return result
    deadline = time.monotonic() + seconds
    latest = None
    earliest = None
    pending = [path]
    try:
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    if result["entries_scanned"] >= entry_limit or time.monotonic() >= deadline:
                        result["scan_state"] = "bounded_partial"
                        return result
                    item = entry.stat(follow_symlinks=False)
                    result["entries_scanned"] += 1
                    if len(result["sample"]) < 6:
                        result["sample"].append(str(Path(entry.path).relative_to(path)))
                    if stat.S_ISLNK(item.st_mode):
                        result["symlinks"] += 1
                    elif stat.S_ISDIR(item.st_mode):
                        result["directories"] += 1
                        if item.st_dev != metadata.st_dev:
                            result["scan_state"] = "mount_boundary"
                        else:
                            pending.append(Path(entry.path))
                    elif stat.S_ISREG(item.st_mode):
                        result["files"] += 1
                        result["logical_bytes"] += item.st_size
                        latest = item.st_mtime if latest is None else max(latest, item.st_mtime)
                        earliest = item.st_mtime if earliest is None else min(earliest, item.st_mtime)
                        result["file_mtime_min"] = timestamp(earliest)
                        result["file_mtime_max"] = timestamp(latest)
    except OSError as error:
        result.update(scan_state="unavailable", error=f"{type(error).__name__}: {error}")
    return result


def receipt_references(root: Path, *, seconds: float) -> dict:
    result = {}
    for component in COMPONENTS:
        path = root / "metadata" / f"dltest_{component}.db"
        try:
            if path.resolve() != path or not path.is_file():
                raise ValueError("receipt database is missing or not canonical")
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
                connection.execute("PRAGMA query_only=ON")
                deadline = time.monotonic() + seconds
                connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
                counts = Counter()
                samples = {}
                for run_key, sample_dir in connection.execute("SELECT run_key,sample_dir FROM cval_ingested_runs ORDER BY run_key"):
                    try:
                        relative = Path(sample_dir).relative_to(root)
                        if ".." in relative.parts:
                            raise ValueError("noncanonical receipt path")
                        category = relative.parts[0]
                    except (TypeError, ValueError, IndexError):
                        category = "outside_root_or_invalid"
                    counts[category] += 1
                    samples.setdefault(category, {"run_key": run_key, "sample_dir": sample_dir})
                result[component] = {"state": "queried", "path": str(path), "reference_counts": dict(counts), "samples": samples}
        except (OSError, ValueError, sqlite3.Error) as error:
            result[component] = {"state": "unavailable", "error": f"{type(error).__name__}: {error}"}
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/data/continuous_validation"))
    parser.add_argument("--entry-limit", type=int, default=5000)
    parser.add_argument("--seconds-per-entry", type=float, default=8)
    parser.add_argument("--receipts", action="store_true")
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--plan-archive", action="store_true")
    operation.add_argument("--apply-archive", action="store_true")
    operation.add_argument("--rollback-archive", action="store_true")
    parser.add_argument("--archive-name")
    parser.add_argument("--plan-sha256")
    parser.add_argument("--confirm")
    parser.add_argument("--confirm-sources-idle")
    args = parser.parse_args()
    if not args.root.is_absolute() or args.root.resolve() != args.root or not args.root.is_dir():
        parser.error("root must be an existing canonical absolute directory")
    if not 1 <= args.entry_limit <= 100000 or not 0 < args.seconds_per_entry <= 60:
        parser.error("invalid inventory bounds")
    if args.plan_archive or args.apply_archive or args.rollback_archive:
        if not args.archive_name:
            parser.error("archive operations require --archive-name")
        try:
            if args.plan_archive:
                report = archive_plan(args.root, args.archive_name)
            elif args.apply_archive:
                report = apply_archive(args.root, args.archive_name, args.plan_sha256, confirm=args.confirm, sources_idle=args.confirm_sources_idle)
            else:
                report = rollback_archive(args.root, args.archive_name, confirm=args.confirm, sources_idle=args.confirm_sources_idle)
        except (OSError, ValueError, KeyError) as error:
            parser.error(str(error))
        print(json.dumps(report, sort_keys=True, indent=2))
        return 0
    for path in sorted(args.root.iterdir()):
        report = entry_summary(path, entry_limit=args.entry_limit, seconds=args.seconds_per_entry)
        print(json.dumps({"schema_version": "cval.pvc-layout.v1", "root": str(args.root), "entry": report}, sort_keys=True), flush=True)
    if args.receipts:
        print(json.dumps({"schema_version": "cval.pvc-references.v1", "root": str(args.root), "receipts": receipt_references(args.root, seconds=args.seconds_per_entry)}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())