"""Thin `kubectl` wrapper used by c-val package modules.

The wrapper centralizes subprocess execution and returns structured stdout,
stderr, and return codes. Higher-level modules decide whether a command is
read-only, dry-run, or mutating; this client only executes explicit arguments.
"""

from __future__ import annotations

import json
import os
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

    def __init__(self, kubectl: str = "kubectl", timeout_seconds: float | None = None) -> None:
        """Store the kubectl binary path or command name."""

        self.kubectl = kubectl
        if timeout_seconds is None:
            timeout_seconds = float(os.environ.get("CVAL_KUBECTL_TIMEOUT_SECONDS", "120"))
        self.timeout_seconds = timeout_seconds if timeout_seconds and timeout_seconds > 0 else None

    def run(
        self,
        args: Sequence[str],
        check: bool = True,
        input_text: str | None = None,
        timeout: float | None = None,
    ) -> CommandResult:
        """Run a kubectl command and optionally raise on non-zero exit.

        ``timeout`` overrides the client default for this call only: a positive
        value caps this command, ``0`` disables the cap, and ``None`` falls back
        to the client's configured timeout.
        """

        if timeout is None:
            effective_timeout = self.timeout_seconds
        elif timeout > 0:
            effective_timeout = timeout
        else:
            effective_timeout = None
        command = [self.kubectl, *args]
        if effective_timeout is not None and not any(
            str(arg).startswith("--request-timeout") for arg in args
        ):
            request_timeout = f"--request-timeout={effective_timeout:g}s"
            try:
                separator_index = command.index("--", 1)
            except ValueError:
                command.append(request_timeout)
            else:
                command.insert(separator_index, request_timeout)
        try:
            completed = subprocess.run(
                command,
                input=input_text,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=effective_timeout,
            )
        except subprocess.TimeoutExpired as exc:
            timed_out = effective_timeout or 0
            stdout = _timeout_text(exc.stdout)
            stderr = _timeout_text(exc.stderr) or f"kubectl command timed out after {timed_out:g}s"
            result = CommandResult(
                args=command,
                stdout=stdout,
                stderr=stderr,
                returncode=124,
            )
            if check:
                command_text = " ".join(command)
                raise RuntimeError(
                    f"Command timed out after {timed_out:g}s: {command_text}\n{stderr.strip()}"
                ) from exc
            return result
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


def _timeout_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)