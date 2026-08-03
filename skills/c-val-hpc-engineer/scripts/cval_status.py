from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only c-val status checks")
    parser.add_argument("--repo", default=".", help="Path to the c-val repository")
    args = parser.parse_args()

    commands = [
        [sys.executable, "-m", "cval.cli", "status", "--output", "table"],
        [sys.executable, "-m", "cval.cli", "nodes", "--output", "table"],
    ]
    for command in commands:
        print(f"$ {' '.join(command)}")
        subprocess.run(command, cwd=args.repo, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())