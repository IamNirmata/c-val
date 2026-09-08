"""Verify NFS SQLite and directory locks using two approved CPU pods."""

import argparse
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import select
import sqlite3
import subprocess
import sys


def hold(path):
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with closing(sqlite3.connect(path, timeout=5)) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS locking_probe(value INTEGER)")
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("INSERT INTO locking_probe VALUES (1)")
            print("locked", flush=True)
            if sys.stdin.readline().strip() != "release":
                raise RuntimeError("missing release handshake")
            connection.rollback()
    finally:
        os.close(descriptor)


def check(path, expected):
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        directory_locked = False
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            directory_locked = True
        with closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=0)) as connection:
            sqlite_locked = False
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if getattr(exc, "sqlite_errorcode", 0) & 0xFF not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                sqlite_locked = True
            else:
                if not expected:
                    connection.execute("INSERT INTO locking_probe VALUES (2)")
                    connection.commit()
                else:
                    connection.rollback()
        if directory_locked != expected or sqlite_locked != expected:
            raise RuntimeError(f"cross-node locks failed: directory={directory_locked}, sqlite={sqlite_locked}, expected={expected}")
        print(json.dumps({"expected_locked": expected, "directory_locked": directory_locked, "sqlite_locked": sqlite_locked, "ok": True}), flush=True)
    finally:
        os.close(descriptor)


def remote(namespace, pod, code, role, path):
    return ["kubectl", "--request-timeout=25s", "exec", "-i", "-n", namespace, pod, "--", "python3", "-u", "-c", code, "--role", role, "--database", str(path)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=("controller", "hold", "blocked", "released"), default="controller")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--namespace", default="gcr-admin")
    parser.add_argument("--reader", default="gcr-admin-pvc-access")
    parser.add_argument("--probe", default="cval-evaluation-lock-probe")
    args = parser.parse_args()
    if not args.database.is_absolute():
        parser.error("database path must be absolute")
    if args.role == "hold":
        hold(args.database)
    elif args.role in {"blocked", "released"}:
        check(args.database, args.role == "blocked")
    else:
        code = Path(__file__).read_text(encoding="utf-8")
        process = subprocess.Popen(remote(args.namespace, args.reader, code, "hold", args.database), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            if not select.select([process.stdout], [], [], 30)[0] or process.stdout.readline().strip() != "locked":
                raise RuntimeError("holder failed to acquire scratch locks")
            subprocess.run(remote(args.namespace, args.probe, code, "blocked", args.database), check=True, timeout=35)
        finally:
            try:
                _stdout, stderr = process.communicate("release\n", timeout=30)
                if process.returncode:
                    raise RuntimeError(f"holder failed: {stderr}")
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise
        subprocess.run(remote(args.namespace, args.probe, code, "released", args.database), check=True, timeout=35)
        print(json.dumps({"cross_node_locking": "passed", "reader": args.reader, "probe": args.probe}), flush=True)


if __name__ == "__main__":
    main()