"""Side-effect-free validation of canonical run evidence paths."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")


def preflight_run_paths(
    validation_root: str | Path,
    node: str,
    run_id: str,
    *,
    registry_json: str,
) -> None:
    """Reject unsafe identities and symlinked ancestors before the first PVC write."""

    for field_name, value in (("node", node), ("run_id", run_id)):
        if value in {".", ".."} or not SAFE_SEGMENT.fullmatch(value):
            raise ValueError(f"Invalid {field_name} path segment: {value!r}")
    root = Path(validation_root).expanduser()
    if not root.is_absolute():
        raise ValueError("validation root must be absolute")
    test_ids = _registry_test_ids(registry_json)
    global_run_dir = root / "logs/job_logs" / node / run_id
    paths = [
        global_run_dir,
        *(global_run_dir / name for name in (
            "stdout.log",
            "stderr.log",
            "job.log",
            "events.jsonl",
            "result.json",
            "result.env",
            ".run-active",
            ".ingestion-result-digest",
        )),
    ]
    for test_id in test_ids:
        paths.extend(
            (
                root / "logs" / test_id / node / run_id,
                root / "validation_tests" / test_id / "runs" / node / run_id,
            )
        )
    for path in paths:
        _reject_symlinked_ancestors(root, path)


def _reject_symlinked_ancestors(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"Run evidence path escapes validation root: {path}") from exc
    current = root
    if current.is_symlink():
        raise ValueError(f"Validation root is a symlink: {current}")
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"Run evidence path contains a symlink: {current}")
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"Run evidence path resolves outside validation root: {path}") from exc


def _registry_test_ids(payload: str) -> tuple[str, ...]:
    if not payload:
        raise ValueError("runtime test registry is required")
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("runtime test registry is invalid JSON") from exc
    if not isinstance(data, dict) or not data:
        raise ValueError("runtime test registry must be a non-empty object")
    result: list[str] = []
    for test_id in data:
        if not isinstance(test_id, str) or not SAFE_SEGMENT.fullmatch(test_id):
            raise ValueError(f"Invalid test ID path segment: {test_id!r}")
        result.append(test_id)
    return tuple(sorted(result))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validation-root", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--test-registry-json", required=True)
    args = parser.parse_args()
    preflight_run_paths(
        args.validation_root,
        args.node,
        args.run_id,
        registry_json=args.test_registry_json,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
