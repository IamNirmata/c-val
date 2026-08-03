from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize c-val plan/status JSON")
    parser.add_argument("path", nargs="?", help="JSON file path, or stdin if omitted")
    args = parser.parse_args()

    text = Path(args.path).read_text(encoding="utf-8") if args.path else sys.stdin.read()
    data = json.loads(text)

    if isinstance(data, dict) and "planned_jobs" in data:
        print("mode: read-only-plan")
        print(f"free_nodes_count: {data.get('free_nodes_count')}")
        print(f"queue_count: {data.get('queue_count')}")
        print(f"planned_jobs: {len(data.get('planned_jobs', []))}")
        for job in data.get("planned_jobs", []):
            print(f"- {job.get('node')} -> {job.get('job_name')} ({job.get('reason')})")
        return 0

    if isinstance(data, list):
        print(f"rows: {len(data)}")
        nodes = sorted(
            {row.get("node") for row in data if isinstance(row, dict) and row.get("node")}
        )
        print(f"nodes: {len(nodes)}")
        return 0

    raise ValueError("Unsupported c-val JSON shape")


if __name__ == "__main__":
    raise SystemExit(main())