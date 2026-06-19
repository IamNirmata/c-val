"""Tests for the kubectl subprocess adapter."""

from __future__ import annotations

import subprocess
import unittest
from unittest.mock import patch

from cval.k8s.client import KubectlClient


class KubectlClientTests(unittest.TestCase):
    def test_passes_timeout_to_subprocess(self) -> None:
        def fake_run(command, **kwargs):
            self.assertEqual(command, ["kubectl", "get", "nodes"])
            self.assertEqual(kwargs["timeout"], 12.5)
            return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")

        with patch("cval.k8s.client.subprocess.run", side_effect=fake_run):
            result = KubectlClient(timeout_seconds=12.5).run(["get", "nodes"])

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "{}")

    def test_timeout_raises_actionable_error_by_default(self) -> None:
        with patch(
            "cval.k8s.client.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["kubectl", "get", "nodes"], 1.0),
        ):
            with self.assertRaises(RuntimeError) as exc:
                KubectlClient(timeout_seconds=1.0).run(["get", "nodes"])

        self.assertIn("Command timed out after 1s", str(exc.exception))
        self.assertIn("kubectl get nodes", str(exc.exception))

    def test_timeout_can_return_command_result_without_check(self) -> None:
        with patch(
            "cval.k8s.client.subprocess.run",
            side_effect=subprocess.TimeoutExpired(
                ["kubectl", "get", "nodes"], 2.0, output="partial", stderr="slow"
            ),
        ):
            result = KubectlClient(timeout_seconds=2.0).run(["get", "nodes"], check=False)

        self.assertEqual(result.returncode, 124)
        self.assertEqual(result.stdout, "partial")
        self.assertEqual(result.stderr, "slow")


if __name__ == "__main__":
    unittest.main()