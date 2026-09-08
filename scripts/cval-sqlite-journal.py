#!/usr/bin/env python3
"""Back up and convert only the four quiesced raw DL DBs to DELETE journaling."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


COMPONENTS = (
    "numerical_correctness",
    "compute_performance",
    "collective_performance",
    "overlap_performance",
)


def canonical(path: Path) -> Path:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        raise ValueError(f"path must be absolute and free of symlinks: {path}")
    return path


def open_db(path: Path, mode: str):
    connection = sqlite3.connect(canonical(path).as_uri() + f"?mode={mode}", uri=True, timeout=10)
    if mode == "ro":
        connection.execute("PRAGMA query_only=ON")
    return connection


def evidence(connection, component):
    schema = connection.execute("SELECT type,name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").fetchall()
    receipts = connection.execute("SELECT run_key,sample_dir,updated_at FROM cval_ingested_runs ORDER BY run_key").fetchall()
    generation = connection.execute("SELECT generation_id,state FROM cval_ingest_metadata WHERE id=1").fetchall()
    samples = connection.execute(f'SELECT * FROM "{component}" ORDER BY rowid DESC LIMIT 6').fetchall()
    payload = {"schema": schema, "receipts": receipts, "generation": generation, "samples": samples}
    return {
        "digest": hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest(),
        "receipt_count": len(receipts),
        "sample_count": len(samples),
        "generation": generation,
    }


def emit(payload):
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")), flush=True)


def backup_database(reader, destination, source: Path, seconds: int):
    started = time.monotonic()
    reported = None
    emit({"event": "backup_started", "database": source.name, "timeout_seconds": seconds})

    def progress(status, remaining, pages):
        nonlocal reported
        now = time.monotonic()
        elapsed = round(now - started, 1)
        if reported is None or now - reported >= 10 or status == sqlite3.SQLITE_DONE or elapsed >= seconds:
            emit({"event": "backup_progress", "database": source.name, "status": status, "remaining_pages": remaining, "total_pages": pages, "elapsed_seconds": elapsed})
            reported = now
        if now - started >= seconds:
            raise TimeoutError(f"backup deadline exceeded: {source}; status={status}, remaining_pages={remaining}, total_pages={pages}, elapsed_seconds={elapsed}")

    reader.backup(destination, pages=2048, progress=progress)


def probe_backup_source(reader, source: Path):
    class ProbeFinished(Exception):
        pass

    started = time.monotonic()
    report = {"event": "backup_source_probe", "database": source.name}

    def progress(status, remaining, pages):
        report.update(status=status, remaining_pages=remaining, total_pages=pages, elapsed_seconds=round(time.monotonic() - started, 3))
        raise ProbeFinished

    with closing(sqlite3.connect(":memory:")) as destination:
        try:
            reader.backup(destination, pages=2048, progress=progress)
        except ProbeFinished:
            pass
    emit(report)
    return report


def file_digest(path: Path, deadline: float):
    digest = hashlib.sha256()
    with path.open("rb") as reader:
        while chunk := reader.read(1024 * 1024):
            digest.update(chunk)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"backup checksum deadline exceeded: {path}")
    return digest.hexdigest()


def publish_backup(staged: Path, backup: Path, deadline: float):
    digest = hashlib.sha256()
    copied = 0
    reported = time.monotonic()
    emit({"event": "backup_publish_started", "database": backup.name, "bytes": staged.stat().st_size})
    with staged.open("rb") as reader, backup.open("xb") as writer:
        while chunk := reader.read(1024 * 1024):
            writer.write(chunk)
            digest.update(chunk)
            copied += len(chunk)
            now = time.monotonic()
            if now - reported >= 10:
                emit({"event": "backup_publish_progress", "database": backup.name, "copied_bytes": copied})
                reported = now
            if now >= deadline:
                raise TimeoutError(f"backup publication deadline exceeded: {backup}")
        writer.flush()
        os.fsync(writer.fileno())
    expected = digest.hexdigest()
    if file_digest(backup, deadline) != expected:
        raise ValueError(f"published backup checksum mismatch: {backup}")
    return expected


def migrate(metadata: Path, backup_root: Path, seconds: int, *, staging_directory: Path | None = None):
    metadata = canonical(metadata)
    backup_root = canonical(backup_root)
    staging_directory = canonical(staging_directory or Path(tempfile.gettempdir()))
    if backup_root.is_relative_to(metadata):
        raise ValueError("backup directory must be outside metadata")
    if staging_directory.is_relative_to(metadata):
        raise ValueError("staging directory must be outside metadata")
    lock = os.open(metadata / ".dl-metric-ingest.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sources = [canonical(metadata / f"dltest_{component}.db") for component in COMPONENTS]
        required = sum(path.stat().st_size + (path.with_name(path.name + "-wal").stat().st_size if path.with_name(path.name + "-wal").exists() else 0) for path in sources)
        parent = backup_root.parent
        while not parent.exists():
            parent = parent.parent
        if shutil.disk_usage(parent).free < required * 1.2:
            raise RuntimeError("insufficient space for verified backups and safety margin")
        if shutil.disk_usage(staging_directory).free < max(path.stat().st_size for path in sources) * 1.2:
            raise RuntimeError("insufficient local space to stage the largest backup")
        generations = []
        for source in sources:
            with closing(open_db(source, "ro")) as connection:
                generations.append(connection.execute("SELECT generation_id,state FROM cval_ingest_metadata WHERE id=1").fetchall())
        if any(value != generations[0] for value in generations) or len(generations[0]) != 1 or generations[0][0][1] != "complete":
            raise ValueError("DL generation must be consistent and complete before migration")
        backup_root.mkdir(mode=0o700, parents=True, exist_ok=False)
        manifest = {"schema": "cval.journal-migration.v1", "created_at": datetime.now(timezone.utc).isoformat(), "databases": []}
        save_manifest(backup_root, manifest)
        for component in COMPONENTS:
            source = canonical(metadata / f"dltest_{component}.db")
            backup = backup_root / source.name
            identity = (source.stat().st_dev, source.stat().st_ino)
            emit({"event": "source_verification_started", "database": source.name, "bytes": source.stat().st_size})
            with closing(open_db(source, "ro")) as reader:
                before = evidence(reader, component)
                if len(before["generation"]) != 1 or before["generation"][0][1] != "complete":
                    raise ValueError(f"incomplete source generation: {source}")
                deadline = time.monotonic() + seconds
                with tempfile.TemporaryDirectory(prefix="cval-journal-", dir=staging_directory) as temporary:
                    staged = Path(temporary) / source.name
                    with closing(open_db(staged, "rwc")) as destination:
                        backup_database(reader, destination, source, seconds)
                        destination.execute("PRAGMA journal_mode=DELETE")
                        if evidence(destination, component) != before:
                            raise ValueError(f"staged backup verification failed: {staged}")
                    backup_sha256 = publish_backup(staged, backup, deadline)
            record = {"source": str(source), "backup": str(backup), "backup_sha256": backup_sha256, "before": before, "converted": False}
            manifest["databases"].append(record)
            save_manifest(backup_root, manifest)
            emit({"event": "backup_verified", "database": source.name, "bytes": backup.stat().st_size})
            with closing(open_db(source, "rw")) as writer:
                checkpoint = writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if checkpoint[0] != 0:
                    raise RuntimeError(f"source has active readers/writers: {source}")
                if writer.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                    raise RuntimeError(f"could not change journal mode: {source}")
                if evidence(writer, component) != before:
                    raise ValueError(f"source evidence changed: {source}")
            if (source.stat().st_dev, source.stat().st_ino) != identity:
                raise ValueError(f"source inode changed: {source}")
            record["converted"] = True
            save_manifest(backup_root, manifest)
            emit({"event": "converted", "database": source.name, "journal_mode": "delete", **before})
        return manifest
    finally:
        os.close(lock)


def save_manifest(root, payload):
    path = root / "manifest.json"
    temporary = root / "manifest.json.tmp"
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, default=Path("/data/continuous_validation/metadata"))
    parser.add_argument("--backup-root", type=Path)
    parser.add_argument("--staging-directory", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--check-backup-source", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--confirm-quiesced")
    parser.add_argument("--backup-timeout", type=int, default=600)
    args = parser.parse_args()
    if args.apply and args.check_backup_source:
        parser.error("--check-backup-source is read-only and cannot be combined with --apply")
    if not args.apply:
        for component in COMPONENTS:
            path = canonical(args.metadata / f"dltest_{component}.db")
            with closing(open_db(path, "ro")) as connection:
                emit({"database": str(path), "bytes": path.stat().st_size, "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0]})
                if args.check_backup_source:
                    probe_backup_source(connection, path)
        return 0
    if args.confirm != "migrate-delete" or args.confirm_quiesced != "writers-stopped" or args.backup_root is None or args.backup_timeout <= 0:
        parser.error("requires --confirm migrate-delete --confirm-quiesced writers-stopped and --backup-root")
    migrate(args.metadata, args.backup_root, args.backup_timeout, staging_directory=args.staging_directory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())