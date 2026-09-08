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
    samples = []
    for run_key, _sample_dir, _updated_at in receipts[-2:]:
        samples.extend(connection.execute(f'SELECT * FROM "{component}" WHERE run_key=? LIMIT 3', (run_key,)).fetchall())
    payload = {"schema": schema, "receipts": receipts, "generation": generation, "samples": samples}
    return {
        "digest": hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest(),
        "receipt_count": len(receipts),
        "sample_count": len(samples),
        "generation": generation,
    }


def emit(payload):
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")), flush=True)


def migrate(metadata: Path, backup_root: Path, seconds: int):
    metadata = canonical(metadata)
    backup_root = canonical(backup_root)
    if backup_root.is_relative_to(metadata):
        raise ValueError("backup directory must be outside metadata")
    lock = os.open(metadata, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sources = [canonical(metadata / f"dltest_{component}.db") for component in COMPONENTS]
        required = sum(path.stat().st_size + (path.with_name(path.name + "-wal").stat().st_size if path.with_name(path.name + "-wal").exists() else 0) for path in sources)
        parent = backup_root.parent
        while not parent.exists():
            parent = parent.parent
        if shutil.disk_usage(parent).free < required * 1.2:
            raise RuntimeError("insufficient space for verified backups and safety margin")
        generations = []
        for source in sources:
            with closing(open_db(source, "ro")) as connection:
                generations.append(connection.execute("SELECT generation_id,state FROM cval_ingest_metadata WHERE id=1").fetchall())
        if any(value != generations[0] for value in generations) or len(generations[0]) != 1 or generations[0][0][1] != "complete":
            raise ValueError("DL generation must be consistent and complete before migration")
        backup_root.mkdir(mode=0o700, parents=True, exist_ok=False)
        manifest = {"schema": "cval.journal-migration.v1", "created_at": datetime.now(timezone.utc).isoformat(), "databases": []}
        for component in COMPONENTS:
            source = canonical(metadata / f"dltest_{component}.db")
            backup = backup_root / source.name
            identity = (source.stat().st_dev, source.stat().st_ino)
            with closing(open_db(source, "ro")) as reader:
                before = evidence(reader, component)
                if len(before["generation"]) != 1 or before["generation"][0][1] != "complete":
                    raise ValueError(f"incomplete source generation: {source}")
                deadline = time.monotonic() + seconds

                def progress(_status, _remaining, _pages):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"backup deadline exceeded: {source}")

                with closing(open_db(backup, "rwc")) as destination:
                    reader.backup(destination, pages=2048, progress=progress)
                    destination.execute("PRAGMA journal_mode=DELETE")
                    if evidence(destination, component) != before:
                        raise ValueError(f"backup verification failed: {backup}")
            record = {"source": str(source), "backup": str(backup), "before": before, "converted": False}
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
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--confirm-quiesced")
    parser.add_argument("--backup-timeout", type=int, default=600)
    args = parser.parse_args()
    if not args.apply:
        for component in COMPONENTS:
            path = canonical(args.metadata / f"dltest_{component}.db")
            with closing(open_db(path, "ro")) as connection:
                emit({"database": str(path), "bytes": path.stat().st_size, "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0]})
        return 0
    if args.confirm != "migrate-delete" or args.confirm_quiesced != "writers-stopped" or args.backup_root is None or args.backup_timeout <= 0:
        parser.error("requires --confirm migrate-delete --confirm-quiesced writers-stopped and --backup-root")
    migrate(args.metadata, args.backup_root, args.backup_timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())