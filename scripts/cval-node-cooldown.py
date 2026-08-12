#!/usr/bin/env python3
"""Read and atomically update cval-live's local node cooldown table."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
import tempfile
from pathlib import Path

_HEADER = ("node_name", "latest_job_submission_timestamp")


def load_state(path: Path) -> dict[str, int]:
    """Load one strict latest-submission row per node."""

    if not path.exists():
        return {}
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"cooldown state must be a regular non-symlink file: {path}")
    state: dict[str, int] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            header = tuple(next(reader))
        except StopIteration as exc:
            raise ValueError("cooldown state is empty") from exc
        if header != _HEADER:
            raise ValueError(
                "cooldown state header must be exactly " + ",".join(_HEADER)
            )
        for line_number, row in enumerate(reader, start=2):
            if len(row) != 2:
                raise ValueError(f"cooldown state row {line_number} must have 2 columns")
            node, timestamp_text = row
            if not node or node.strip() != node or "," in node or "\n" in node:
                raise ValueError(f"cooldown state row {line_number} has an invalid node")
            if node in state:
                raise ValueError(f"cooldown state contains duplicate node {node!r}")
            if not timestamp_text.isdigit():
                raise ValueError(
                    f"cooldown state row {line_number} has an invalid timestamp"
                )
            timestamp = int(timestamp_text)
            if timestamp <= 0:
                raise ValueError(
                    f"cooldown state row {line_number} timestamp must be positive"
                )
            state[node] = timestamp
    return state


def filter_nodes(
    state: dict[str, int], nodes: list[str], *, now: int, cooldown_seconds: int
) -> tuple[list[str], list[dict[str, int | str]]]:
    """Return eligible nodes and active cooldown explanations."""

    if now <= 0:
        raise ValueError("now must be positive")
    if cooldown_seconds < 0:
        raise ValueError("cooldown_seconds must be non-negative")
    if len(nodes) != len(set(nodes)):
        raise ValueError("node snapshot contains duplicates")
    eligible: list[str] = []
    excluded: list[dict[str, int | str]] = []
    for node in nodes:
        if not node:
            raise ValueError("node snapshot contains a blank node")
        submitted_at = state.get(node)
        cooldown_until = (
            submitted_at + cooldown_seconds if submitted_at is not None else 0
        )
        if cooldown_seconds > 0 and cooldown_until > now:
            excluded.append(
                {
                    "node": node,
                    "latest_job_submission_timestamp": submitted_at,
                    "cooldown_until": cooldown_until,
                }
            )
        else:
            eligible.append(node)
    return eligible, excluded


def _write_state(path: Path, state: dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(_HEADER)
            for node in sorted(state):
                writer.writerow((node, state[node]))
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def record_submission(path: Path, node: str, timestamp: int) -> None:
    """Atomically retain the newest submission timestamp for one node."""

    if not node or node.strip() != node or "," in node or "\n" in node:
        raise ValueError("node must be a non-empty CSV-safe value")
    if timestamp <= 0:
        raise ValueError("timestamp must be positive")
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        state = load_state(path)
        state[node] = max(timestamp, state.get(node, 0))
        _write_state(path, state)
    finally:
        os.close(lock_fd)


def _parse_nodes(value: str) -> list[str]:
    if not value:
        return []
    nodes = value.split(",")
    if any(not node for node in nodes):
        raise ValueError("nodes must be a comma-separated list without blanks")
    return nodes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    filter_parser = subparsers.add_parser("filter")
    filter_parser.add_argument("--state-file", type=Path, required=True)
    filter_parser.add_argument("--nodes", required=True)
    filter_parser.add_argument("--now", type=int, required=True)
    filter_parser.add_argument("--cooldown-seconds", type=int, required=True)
    filter_parser.add_argument("--report", type=Path, required=True)

    record_parser = subparsers.add_parser("record")
    record_parser.add_argument("--state-file", type=Path, required=True)
    record_parser.add_argument("--node", required=True)
    record_parser.add_argument("--timestamp", type=int, required=True)

    args = parser.parse_args()
    if args.command == "record":
        record_submission(args.state_file, args.node, args.timestamp)
        return 0

    nodes = _parse_nodes(args.nodes)
    state = load_state(args.state_file)
    eligible, excluded = filter_nodes(
        state,
        nodes,
        now=args.now,
        cooldown_seconds=args.cooldown_seconds,
    )
    report = {
        "schema_version": "cval.node-cooldown.v1",
        "observed_at": args.now,
        "cooldown_seconds": args.cooldown_seconds,
        "state_file": str(args.state_file),
        "gpu_inventory_nodes": nodes,
        "cooldown_excluded": excluded,
        "priority_eligible_nodes": eligible,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(",".join(eligible))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
