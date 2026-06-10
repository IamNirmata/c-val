from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class CommandResult:
    args: Sequence[str]
    stdout: str
    stderr: str
    returncode: int


class KubectlClient:
    def __init__(self, kubectl: str = "kubectl") -> None:
        self.kubectl = kubectl

    def run(
        self,
        args: Sequence[str],
        check: bool = True,
        input_text: str | None = None,
    ) -> CommandResult:
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
            command_text = " ".join(command)
            stderr = result.stderr.strip()
            raise RuntimeError(
                f"Command failed ({result.returncode}): {command_text}\n{stderr}"
            )
        return result

    def get_json(self, args: Sequence[str]) -> dict:
        output = self.run([*args, "-o", "json"]).stdout
        return json.loads(output)

    def get_pods_json(self) -> dict:
        return self.get_json(["get", "pods", "-A"])

    def get_nodes_json(self) -> dict:
        return self.get_json(["get", "nodes"])

    def get_nodes_capacity_table(self) -> str:
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