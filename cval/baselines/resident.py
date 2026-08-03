"""Supervise the storage and DL evaluator loops in one resident container."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

_STATE_SCHEMA = "cval.sqlite-evaluator-resident.v1"
_STOP_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True)
class Child:
    name: str
    process: subprocess.Popen[bytes]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _state_path(state_dir: Path) -> Path:
    return state_dir / "state.json"


def _write_state(state_dir: Path, payload: dict[str, object]) -> None:
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(state_dir, 0o700)
    target = _state_path(state_dir)
    temporary = state_dir / f".state-{os.getpid()}.tmp"
    data = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, data)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, target)


def _read_state(state_dir: Path) -> dict[str, object]:
    target = _state_path(state_dir)
    value = target.stat(follow_symlinks=False)
    if not target.is_file() or value.st_mode & 0o077:
        raise RuntimeError("resident evaluator state file is unsafe")
    payload = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != _STATE_SCHEMA:
        raise RuntimeError("resident evaluator state is invalid")
    return payload


def _pid_alive(pid: object) -> bool:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def status(state_dir: Path) -> dict[str, object]:
    payload = _read_state(state_dir)
    children = payload.get("children")
    if not isinstance(children, dict) or not children:
        raise RuntimeError("resident evaluator has no child process state")
    dead = sorted(name for name, pid in children.items() if not _pid_alive(pid))
    if dead:
        raise RuntimeError("resident evaluator child stopped: " + ", ".join(dead))
    return {
        "schema_version": _STATE_SCHEMA,
        "status": "ready",
        "started_at": payload.get("started_at"),
        "children": dict(sorted(children.items())),
    }


def _terminate(children: Sequence[Child]) -> None:
    for child in children:
        if child.process.poll() is None:
            try:
                os.killpg(child.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + _STOP_TIMEOUT_SECONDS
    while time.monotonic() < deadline and any(
        child.process.poll() is None for child in children
    ):
        time.sleep(0.1)
    for child in children:
        if child.process.poll() is None:
            try:
                os.killpg(child.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    for child in children:
        try:
            child.process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass


def run(
    *,
    repo_root: Path,
    state_dir: Path,
    environ: Mapping[str, str] | None = None,
) -> int:
    repo_root = Path(repo_root).resolve()
    state_dir = Path(state_dir)
    scripts = {
        "baseline-build": repo_root / "scripts/cval-baseline-build.sh",
        "baseline-classify": repo_root / "scripts/cval-baseline-classify.sh",
    }
    for name, path in scripts.items():
        if not path.is_file():
            raise FileNotFoundError(f"resident evaluator script missing: {name}")
    environment = dict(os.environ if environ is None else environ)
    environment.setdefault("CVAL_CONFIG", str(repo_root / "config/cval.toml"))
    environment.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    interpreter_dir = str(Path(sys.executable).resolve().parent)
    environment["PATH"] = interpreter_dir + os.pathsep + environment.get("PATH", "")

    stopped = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopped
        stopped = True

    previous = {
        watched: signal.getsignal(watched)
        for watched in (signal.SIGINT, signal.SIGTERM)
    }
    children: list[Child] = []
    try:
        for watched in previous:
            signal.signal(watched, request_stop)
        for name, path in scripts.items():
            process = subprocess.Popen(
                ["/bin/bash", str(path), "run-loop"],
                cwd=repo_root,
                env=environment,
                start_new_session=True,
            )
            children.append(Child(name, process))
        _write_state(
            state_dir,
            {
                "schema_version": _STATE_SCHEMA,
                "started_at": _utc_now(),
                "supervisor_pid": os.getpid(),
                "children": {child.name: child.process.pid for child in children},
            },
        )
        while not stopped:
            failed = next(
                (child for child in children if child.process.poll() is not None),
                None,
            )
            if failed is not None:
                print(
                    f"resident evaluator child exited: {failed.name} "
                    f"code={failed.process.returncode}",
                    file=sys.stderr,
                    flush=True,
                )
                return failed.process.returncode or 1
            time.sleep(1.0)
        return 0
    finally:
        _terminate(children)
        try:
            _state_path(state_dir).unlink()
        except FileNotFoundError:
            pass
        for watched, handler in previous.items():
            signal.signal(watched, handler)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    subparsers = value.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--repo-root", type=Path, required=True)
    run_parser.add_argument("--state-dir", type=Path, required=True)
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--state-dir", type=Path, required=True)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "run":
            return run(repo_root=args.repo_root, state_dir=args.state_dir)
        print(json.dumps(status(args.state_dir), sort_keys=True))
        return 0
    except Exception as exc:  # noqa: BLE001 - process health boundary
        print(f"resident evaluator error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
