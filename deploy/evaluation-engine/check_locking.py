"""Verify NFS SQLite and POSIX record locks using two approved CPU pods."""

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
    descriptor = os.open(path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.lockf(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
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
    descriptor = os.open(path.with_suffix(".lock"), os.O_RDWR | os.O_NOFOLLOW)
    try:
        record_locked = False
        try:
            fcntl.lockf(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            record_locked = True
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
        valid = record_locked == expected and sqlite_locked == expected
        print(json.dumps({"expected_locked": expected, "record_locked": record_locked, "sqlite_locked": sqlite_locked, "ok": valid}), flush=True)
        if not valid:
            raise RuntimeError("cross-node record or SQLite lock validation failed")
    finally:
        os.close(descriptor)


def remote(namespace, pod, code, role, path):
    return ["kubectl", "--request-timeout=25s", "exec", "-i", "-n", namespace, pod, "--", "python3", "-u", "-c", code, "--role", role, "--database", str(path)]


def run_check(command):
    result = subprocess.run(command, capture_output=True, text=True, timeout=35)
    print(result.stdout, end="", flush=True)
    if result.returncode:
        raise RuntimeError(result.stderr[-1000:])


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
            run_check(remote(args.namespace, args.probe, code, "blocked", args.database))
        finally:
            try:
                _stdout, stderr = process.communicate("release\n", timeout=30)
                if process.returncode:
                    raise RuntimeError(f"holder failed: {stderr}")
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise
        run_check(remote(args.namespace, args.probe, code, "released", args.database))
        print(json.dumps({"cross_node_locking": "passed", "reader": args.reader, "probe": args.probe}), flush=True)


if __name__ == "__main__":
    main()