"""Thin `kubectl` wrapper used by c-val package modules.

The wrapper centralizes subprocess execution and returns structured stdout,
stderr, and return codes. Higher-level modules decide whether a command is
read-only, dry-run, or mutating; this client only executes explicit arguments.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class CommandResult:
    """Captured result for one kubectl subprocess invocation."""

    args: Sequence[str]
    stdout: str
    stderr: str
    returncode: int


class KubectlClient:
    """Small testable adapter around the `kubectl` executable."""

    def __init__(self, kubectl: str = "kubectl") -> None:
        """Store the kubectl binary path or command name."""

        self.kubectl = kubectl

    def run(
        self,
        args: Sequence[str],
        check: bool = True,
        input_text: str | None = None,
    ) -> CommandResult:
        """Run a kubectl command and optionally raise on non-zero exit."""

        command = [self.kubectl, *args]
        completed = subprocess.run(
            command,
            input=input_text,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        result = CommandResult(
            args=command,
            stdout=completed.stdout,
            stderr=completed.stderr,
            returncode=completed.returncode,
        )
        if check and result.returncode != 0:
            # Include the full command and stderr so caller errors are actionable.
            command_text = " ".join(command)
            stderr = result.stderr.strip()
            raise RuntimeError(
                f"Command failed ({result.returncode}): {command_text}\n{stderr}"
            )
        return result

    def get_json(self, args: Sequence[str]) -> dict:
        """Run a kubectl `-o json` command and decode the object."""

        output = self.run([*args, "-o", "json"]).stdout
        return json.loads(output)

    def get_pods_json(self) -> dict:
        """Return all pods across namespaces as Kubernetes JSON."""

        return self.get_json(["get", "pods", "-A"])

    def get_nodes_json(self) -> dict:
        """Return all nodes as Kubernetes JSON, including taints and schedulability."""

        return self.get_json(["get", "nodes"])

    def get_nodes_capacity_table(self) -> str:
        """Return a compact node table containing GPU capacity and allocatable values."""

        columns = (
            r"custom-columns=NAME:.metadata.name,"
            r"CAP:.status.capacity.nvidia\.com/gpu,"
            r"ALLOC:.status.allocatable.nvidia\.com/gpu"
        )
        return self.run(
            [
                "get",
                "nodes",
                "--no-headers",
                "-o",
                columns,
            ]
        ).stdout