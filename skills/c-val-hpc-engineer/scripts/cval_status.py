from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only c-val status checks")
    parser.add_argument("--repo", default=".", help="Path to the c-val repository")
    parser.add_argument("--plan", action="store_true", help="Also print a dry-run plan")
    parser.add_argument("--threshold-days", type=float, default=4)
    parser.add_argument("--batch-size", type=int, default=3)
    args = parser.parse_args()

    commands = [
        [sys.executable, "-m", "cval.cli", "status", "--output", "table"],
        [sys.executable, "-m", "cval.cli", "nodes", "--output", "table"],
    ]
    if args.plan:
        commands.append(
            [
                sys.executable,
                "-m",
                "cval.cli",
                "plan",
                "--live-status",
                "--threshold-days",
                str(args.threshold_days),
                "--batch-size",
                str(args.batch_size),
                "--output",
                "json",
            ]
        )

    for command in commands:
        print(f"$ {' '.join(command)}")
        subprocess.run(command, cwd=args.repo, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())