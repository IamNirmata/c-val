"""Run the independent CPU evaluator; writes require explicit confirmation."""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import signal
import threading
import time
from pathlib import Path

from cval.config import is_exact_commit
from cval.storage.sqlite_uri import canonical_sqlite_path
from evaluation_engine.config import load_settings
from evaluation_engine.runtime import Context


def process_test(context, test, runs, operation="all"):
    settings = context.settings
    task = "baseline:" + test.name
    saved = context.database.read("SELECT payload,updated_at FROM worker_state WHERE task=?", (task,))
    previous = json.loads(saved[0][0]) if saved else {}
    report = {}
    due = not saved or context.now - saved[0][1] >= settings.baseline_check_seconds or previous.get("policy_digest") != test.policy_digest or previous.get("revision") != context.revision
    if operation != "classify" and due:
        try:
            built = test.baseline.build(context, test, runs)
            report.update(built)
            context.database.state(task, built | {"policy_digest": test.policy_digest, "revision": context.revision}, context.now)
        except Exception as exc:
            report["baseline_error"] = str(exc)
            context.database.state(task, {"error": str(exc)}, context.now)
    if operation == "baseline":
        return report
    task = "classify:" + test.name
    saved = context.database.read("SELECT payload FROM worker_state WHERE task=?", (task,))
    previous = json.loads(saved[0][0]).get("sequence", 0) if saved else 0
    selected = [run for run in runs if run["sequence"] > previous][:settings.batch_size]
    if not selected:
        selected = runs[:settings.batch_size]
    written = sum(int(test.classification.classify(context, test, run)) for run in selected)
    sequence = selected[-1]["sequence"] if selected else 0
    context.database.state(task, {"sequence": sequence, "processed": len(selected), "written": written}, context.now)
    return report | {"processed": len(selected), "written": written}


def run_once(settings, revision, now=None, operation="all"):
    context = Context(settings, revision, now)
    context.database.initialize()
    runs = context.candidates()
    report = {"timestamp": context.now, "tests": {}}
    for test in settings.tests:
        try:
            report["tests"][test.name] = process_test(context, test, runs, operation)
        except Exception as exc:
            report["tests"][test.name] = {"error": str(exc)}
    context.database.state("worker", {"state": "idle", "revision": revision, "tests": report["tests"]}, context.now)
    return report


def main(test_name=None, operation="all"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/cval.toml"))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--check-config", action="store_true")
    parser.add_argument("--check-sources", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--revision", default=os.environ.get("CVAL_EVALUATOR_SHA", ""))
    args = parser.parse_args()
    settings = load_settings(args.config)
    if test_name is not None:
        from dataclasses import replace
        settings = replace(settings, tests=tuple(test for test in settings.tests if test.name == test_name))
        if not settings.tests:
            parser.error("requested test evaluator is disabled")
        args.once = True
    if args.check_config:
        print(json.dumps({"valid": True, "tests": [test.name for test in settings.tests], "output": str(settings.output)}))
        return 0
    if args.check_sources:
        context = Context(settings, args.revision)
        reports = {}
        for name, path in vars(settings.raw.storage).items():
            try:
                context.read(path, "SELECT name FROM sqlite_master WHERE type='table'")
                reports[name] = {"state": "readable"}
            except Exception as exc:
                reports[name] = {"state": "blocked", "reason": str(exc)}
        print(json.dumps(reports, sort_keys=True))
        return int(any(value["state"] != "readable" for value in reports.values()))
    if args.confirm != "evaluate" or not is_exact_commit(args.revision):
        parser.error("derived DB writes require --confirm evaluate and exact --revision")
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = canonical_sqlite_path(settings.output.with_suffix(".worker.lock"), must_exist=False)
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    stopping = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_arguments: stopping.set())
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while not stopping.is_set():
            try:
                report = run_once(settings, args.revision, operation=operation)
                print(json.dumps(report, sort_keys=True), flush=True)
                if args.once and any("error" in value or "baseline_error" in value for value in report["tests"].values()):
                    return 1
            except Exception as exc:
                logging.exception("evaluation pass failed")
                try:
                    context = Context(settings, args.revision)
                    context.database.state("worker", {"state": "error", "reason": str(exc)[:1000]}, int(time.time()))
                except Exception:
                    logging.exception("unable to persist worker failure")
                if args.once:
                    return 1
            if args.once:
                return 0
            stopping.wait(settings.poll_seconds)
    finally:
        os.close(descriptor)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())