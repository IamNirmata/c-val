#!/usr/bin/env python3
"""Maintain compact, atomically replaced cval-live session state."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cval.jobs.manager import (
    SESSION_SCHEMA as STATE_SCHEMA,
    SESSION_JOB_FIELDS as JOB_FIELDS,
    admission_lock,
    admission_scope,
    check_admission_cluster,
    read_admission_session,
    read_session_jobs as _read_jobs,
    read_session_state as _read_state,
)

SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.tmp-", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _write_jobs(session_dir: Path, jobs: dict[str, dict[str, str]]) -> None:
    output = []
    output.append(",".join(JOB_FIELDS))
    for job_name in sorted(jobs):
        row = jobs[job_name]
        output.append(",".join(row[field] for field in JOB_FIELDS))
    _atomic_text(session_dir / "jobs.csv", "\n".join(output) + "\n")


def _render(session_dir: Path) -> None:
    state = _read_state(session_dir)
    jobs = _read_jobs(session_dir)
    phases = Counter(row["phase"] for row in jobs.values() if row["phase"])
    state["job_counts"] = {"total": len(jobs), **dict(sorted(phases.items()))}
    state["node_snapshots"] = len(list((session_dir / "nodes").glob("*.json")))
    _atomic_text(
        session_dir / "state.json",
        json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    )
    counts = " ".join(
        f"{name.lower()}={count}" for name, count in state["job_counts"].items()
    )
    lines = [
        "# cval-live",
        "",
        f"- Session: `{state['session_id']}`",
        f"- Updated: `{state['updated_at']}`",
        f"- State: `{state['state']}`",
        f"- Commit: `{state['git_ref']}`",
        f"- Cycle: `{state.get('cycle_id', '')}`",
        f"- Jobs: {counts}",
        f"- Nodes checked: {state['node_snapshots']}",
        f"- Last: {state.get('message', '')}",
    ]
    _atomic_text(session_dir / "SUMMARY.md", "\n".join(lines) + "\n")


def _init(args: argparse.Namespace) -> None:
    from dataclasses import replace

    from cval.config import encode_config_snapshot, load_config_snapshot

    config = load_config_snapshot(os.environ["CVAL_CONFIG_SNAPSHOT_B64"])
    config = replace(config, job=replace(config.job, git_ref=args.git_ref))
    session_dir = args.session_dir
    if os.path.lexists(session_dir.parent / "current-session"):
        raise ValueError("Existing active session must be reconciled and its pointer retained before approved turnover")
    session_dir.mkdir(parents=True, exist_ok=False)
    (session_dir / "cycles").mkdir()
    (session_dir / "nodes").mkdir()
    (session_dir / "loop.log").touch()
    _write_jobs(session_dir, {})
    state = {
        "schema_version": STATE_SCHEMA,
        "session_id": session_dir.name,
        "started_at": args.started_at,
        "updated_at": args.started_at,
        "state": "starting",
        "git_ref": args.git_ref,
        "config_snapshot_b64": encode_config_snapshot(config),
        "admission_scope": admission_scope(config),
        "branch": args.branch,
        "cycle_id": "",
        "message": "session initialized",
        "settings": {
            "batch_size": args.batch_size,
            "plan_limit": args.plan_limit,
            "pruning_enabled": args.pruning_enabled,
        },
    }
    _atomic_text(
        session_dir / "state.json",
        json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    )
    _atomic_text(session_dir.parent / "current-session", session_dir.name + "\n")
    _render(session_dir)


def _update(args: argparse.Namespace) -> None:
    state = _read_state(args.session_dir)
    state.update(
        {
            "updated_at": args.updated_at,
            "state": args.state,
            "message": args.message,
        }
    )
    if args.cycle_id is not None:
        state["cycle_id"] = args.cycle_id
    _atomic_text(
        args.session_dir / "state.json",
        json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    )
    _render(args.session_dir)


def _job(args: argparse.Namespace) -> None:
    jobs = _read_jobs(args.session_dir)
    current = jobs.get(args.job_name, {field: "" for field in JOB_FIELDS})
    current.update(
        {
            "job_name": args.job_name,
            "node": args.node or current["node"],
            "timestamp": args.timestamp or current["timestamp"],
            "git_ref": args.git_ref or current["git_ref"],
            "phase": args.phase,
            "last_observed_at": args.observed_at,
        }
    )
    if not current["submitted_at"]:
        current["submitted_at"] = args.observed_at
    if args.deleted_at:
        current["deleted_at"] = args.deleted_at
    jobs[args.job_name] = current
    _write_jobs(args.session_dir, jobs)
    state = _read_state(args.session_dir)
    state["updated_at"] = args.observed_at
    state["message"] = f"{args.job_name} phase={args.phase}"
    _atomic_text(
        args.session_dir / "state.json",
        json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
    )
    _render(args.session_dir)


def _receipt(args: argparse.Namespace) -> None:
    payload = json.loads(args.source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
        raise ValueError("invalid submission receipt")
    jobs = _read_jobs(args.session_dir)
    for item in payload["jobs"]:
        if not isinstance(item, dict) or item.get("submitted") is not True:
            continue
        job_name = item.get("job_name")
        node = item.get("node")
        git_ref = item.get("git_ref") or ""
        match = re.search(r"-([0-9]+)$", str(job_name))
        if (
            not isinstance(job_name, str)
            or not SAFE_NAME.fullmatch(job_name)
            or not isinstance(node, str)
            or not SAFE_NAME.fullmatch(node)
            or not isinstance(git_ref, str)
            or (git_ref and re.fullmatch(r"[0-9a-f]{40}", git_ref) is None)
            or match is None
        ):
            raise ValueError("invalid submitted job identity")
        current = jobs.get(job_name, {field: "" for field in JOB_FIELDS})
        current.update(
            {
                "job_name": job_name,
                "node": node,
                "timestamp": match.group(1),
                "git_ref": git_ref,
                "phase": current["phase"] or "Submitted",
                "submitted_at": current["submitted_at"] or args.observed_at,
                "last_observed_at": current["last_observed_at"] or args.observed_at,
            }
        )
        jobs[job_name] = current
    _write_jobs(args.session_dir, jobs)
    _render(args.session_dir)


def _node(args: argparse.Namespace) -> None:
    payload = json.loads(args.source.read_text(encoding="utf-8"))
    name = payload.get("name")
    if not isinstance(name, str) or not SAFE_NAME.fullmatch(name):
        raise ValueError("invalid node snapshot name")
    _atomic_text(
        args.session_dir / "nodes" / f"{name}.json",
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    subparsers = root.add_subparsers(dest="command", required=True)
    init = subparsers.add_parser("init")
    init.add_argument("session_dir", type=Path)
    init.add_argument("--started-at", required=True)
    init.add_argument("--git-ref", required=True)
    init.add_argument("--branch", required=True)
    init.add_argument("--batch-size", type=int, required=True)
    init.add_argument("--plan-limit", required=True)
    init.add_argument("--pruning-enabled", action="store_true")
    init.set_defaults(handler=_init)

    update = subparsers.add_parser("update")
    update.add_argument("session_dir", type=Path)
    update.add_argument("--updated-at", required=True)
    update.add_argument("--state", required=True)
    update.add_argument("--message", required=True)
    update.add_argument("--cycle-id")
    update.set_defaults(handler=_update)

    job = subparsers.add_parser("job")
    job.add_argument("session_dir", type=Path)
    job.add_argument("--job-name", required=True)
    job.add_argument("--node", default="")
    job.add_argument("--timestamp", default="")
    job.add_argument("--git-ref", default="")
    job.add_argument("--phase", required=True)
    job.add_argument("--observed-at", required=True)
    job.add_argument("--deleted-at", default="")
    job.set_defaults(handler=_job)

    receipt = subparsers.add_parser("receipt")
    receipt.add_argument("session_dir", type=Path)
    receipt.add_argument("--source", type=Path, required=True)
    receipt.add_argument("--observed-at", required=True)
    receipt.set_defaults(handler=_receipt)

    node = subparsers.add_parser("node")
    node.add_argument("session_dir", type=Path)
    node.add_argument("--source", type=Path, required=True)
    node.set_defaults(handler=_node)
    return root


def main() -> int:
    args = parser().parse_args()
    from dataclasses import replace
    from cval.config import is_exact_commit, load_config_snapshot
    from cval.k8s.client import KubectlClient

    snapshot = os.environ["CVAL_CONFIG_SNAPSHOT_B64"] if args.command == "init" else _read_state(args.session_dir)["config_snapshot_b64"]
    config = load_config_snapshot(snapshot)
    if args.command == "init":
        if not is_exact_commit(args.git_ref):
            raise ValueError("Session initialization requires an exact source commit")
        config = replace(config, job=replace(config.job, git_ref=args.git_ref))
    if args.session_dir.parent != Path(config.live.log_dir) or not SAFE_NAME.fullmatch(args.session_dir.name):
        raise ValueError("Session must be inside the configured shared admission directory")
    admission_scope(config)
    if args.command == "init":
        if args.batch_size != config.scheduling.batch_size or args.plan_limit != config.live.plan_limit or args.branch != config.live.git_branch:
            raise ValueError("Session settings must match the pinned configuration")
        Path(config.live.log_dir).mkdir(parents=True, exist_ok=True)
    with admission_lock(config):
        if args.command == "init":
            check_admission_cluster(config, KubectlClient(config=config))
        else:
            active_dir, _state = read_admission_session(config)
            if active_dir != args.session_dir:
                raise ValueError("Session writer does not own the active admission session")
        args.handler(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())