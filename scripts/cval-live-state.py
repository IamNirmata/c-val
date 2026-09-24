#!/usr/bin/env python3
"""Maintain compact, atomically replaced cval-live session state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cval.jobs.manager import (
    SESSION_SCHEMA as STATE_SCHEMA,
    atomic_session_text as _atomic_text,
    project_submission_jobs,
    retain_submission_event,
    reconcile_submission_intents,
    intent_finished,
    write_session_jobs as _write_jobs,
    admission_lock,
    admission_scope,
    check_admission_cluster,
    read_admission_session,
    read_session_jobs as _read_jobs,
    read_session_state as _read_state,
)
from cval.jobs.monitor import observe_validation_job

SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _render(session_dir: Path) -> None:
    state = _read_state(session_dir)
    project_submission_jobs(session_dir, state)
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
    approval_path = args.approval_record.resolve(strict=True)
    approval_text = approval_path.read_text(encoding="utf-8")
    if not approval_text.strip():
        raise ValueError("Session requires a nonempty approval and rollback record")
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
        "intents": {},
        "approval": {"path": str(approval_path), "sha256": hashlib.sha256(approval_text.encode()).hexdigest(), "text": approval_text},
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
    if args.reconcile:
        from cval.config import load_config_snapshot
        from cval.k8s.client import KubectlClient

        config = load_config_snapshot(state["config_snapshot_b64"])
        client = KubectlClient(config=config)
        check_admission_cluster(config, client)
        state = reconcile_submission_intents(args.session_dir, state, client, config)
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
    from cval.config import load_config_snapshot
    from cval.k8s.client import KubectlClient

    state = _read_state(args.session_dir)
    intent = state["intents"][args.job_name]
    for supplied, expected in ((args.node, intent["node_name"]), (args.timestamp, str(intent["timestamp"])), (args.git_ref, intent["git_ref"])):
        if supplied and supplied != expected:
            raise ValueError("Job update conflicts with retained intent")
    if not any(event["state"] == "submitting" for event in intent["events"]) or intent["events"][-1]["state"] == "conflict":
        raise ValueError("Job update requires explicit reconciliation of this intent")
    config = load_config_snapshot(state["config_snapshot_b64"])
    client = KubectlClient(config=config)
    check_admission_cluster(config, client)
    event = observe_validation_job(intent, config=config, client=client)
    event.update(reported_phase=args.phase, reported_at=args.observed_at, deleted_at=args.deleted_at)
    state = retain_submission_event(args.session_dir, state, args.job_name, event)
    _render(args.session_dir)
    if event["state"] != "observed" and not intent_finished(state["intents"][args.job_name]):
        raise ValueError("Job observation is unresolved; retain its node and slot")


def _receipt(args: argparse.Namespace) -> None:
    payload = json.loads(args.source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
        raise ValueError("invalid submission receipt")
    state = _read_state(args.session_dir)
    for item in payload["jobs"]:
        if not isinstance(item, dict) or item.get("submitted") is not True:
            continue
        job_name = item.get("job_name")
        intent = state["intents"].get(job_name)
        if not intent or item.get("node") != intent["node_name"] or item.get("git_ref") != intent["git_ref"]:
            raise ValueError("Submitted identity lacks a matching durable intent")
        if intent["events"][-1]["state"] not in {"submitted", "observed", "verification_finished", "tracking_finished"}:
            raise ValueError("Submission receipt has not been durably verified")
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
    init.add_argument("--approval-record", type=Path, required=True)
    init.set_defaults(handler=_init)

    update = subparsers.add_parser("update")
    update.add_argument("session_dir", type=Path)
    update.add_argument("--updated-at", required=True)
    update.add_argument("--state", required=True)
    update.add_argument("--message", required=True)
    update.add_argument("--cycle-id")
    update.add_argument("--reconcile", action="store_true")
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