from __future__ import annotations

import json
import importlib.metadata
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from cval.nccl_eval.runtime_evidence import (
    RUNTIME_EVIDENCE_SCHEMA,
    collect_runtime_evidence,
    load_runtime_evidence,
    normalize_nccl_version,
    normalize_topology_output,
    topology_class_from_output,
    write_runtime_evidence,
)


class FakeNccl:
    @staticmethod
    def version():
        return (2, 27, 7)


class FakeCuda:
    nccl = FakeNccl()

    @staticmethod
    def is_available():
        return True

    @staticmethod
    def device_count():
        return 2

    @staticmethod
    def get_device_name(index):
        return "NVIDIA B200"


class RuntimeEvidenceTests(unittest.TestCase):
    def test_collects_exact_facts_with_bounded_fake_commands(self) -> None:
        calls = []

        def runner(command, **kwargs):
            calls.append((tuple(command), kwargs))
            if "--query-gpu=driver_version" in command:
                output = "600.12\n600.12\n"
            else:
                output = "GPU0 GPU1 CPU Affinity\nGPU0 X NV18 0-63\nGPU1 NV18 X 0-63\n"
            return SimpleNamespace(stdout=output, stderr="", returncode=0)

        evidence = collect_runtime_evidence(
            torch_module=SimpleNamespace(cuda=FakeCuda()),
            command_runner=runner,
            package_version_resolver=lambda package: "2.27.7" if package == "nvidia-nccl-cu13" else (_ for _ in ()).throw(importlib.metadata.PackageNotFoundError()),
        )

        self.assertEqual(evidence.schema_version, RUNTIME_EVIDENCE_SCHEMA)
        self.assertEqual(evidence.gpu_model, "NVIDIA B200")
        self.assertEqual(evidence.compiled_nccl_version, "2.27.7")
        self.assertEqual(evidence.runtime_nccl_package_version, "nvidia-nccl-cu13==2.27.7")
        self.assertEqual(evidence.driver_version, "600.12")
        self.assertEqual(evidence.driver_version_group, "600.12")
        self.assertRegex(evidence.topology_class, r"^nvidia-topo-sha256:[0-9a-f]{64}$")
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(call[1]["timeout"] == 10.0 for call in calls))
        self.assertTrue(all(call[1]["check"] for call in calls))

    def test_fails_when_any_required_runtime_fact_is_absent_or_inconsistent(self) -> None:
        def no_driver(command, **kwargs):
            return SimpleNamespace(stdout="\n", stderr="", returncode=0)

        with self.assertRaisesRegex(RuntimeError, "no data"):
            collect_runtime_evidence(
                torch_module=SimpleNamespace(cuda=FakeCuda()),
                command_runner=no_driver,
            )

        class MixedCuda(FakeCuda):
            @staticmethod
            def get_device_name(index):
                return "B200" if index == 0 else "H200"

        with self.assertRaisesRegex(RuntimeError, "one exact GPU model"):
            collect_runtime_evidence(
                torch_module=SimpleNamespace(cuda=MixedCuda()),
                command_runner=no_driver,
            )

        def complete(command, **kwargs):
            return SimpleNamespace(
                stdout=(
                    "600.12\n600.12\n"
                    if "--query-gpu=driver_version" in command
                    else " GPU0 GPU1 CPU Affinity\nGPU0 X NV18 0-63\nGPU1 NV18 X 0-63\n"
                )
            )

        with self.assertRaisesRegex(RuntimeError, "package metadata is absent"):
            collect_runtime_evidence(
                torch_module=SimpleNamespace(cuda=FakeCuda()),
                command_runner=complete,
                package_version_resolver=lambda _package: (_ for _ in ()).throw(
                    importlib.metadata.PackageNotFoundError()
                ),
            )

    def test_nccl_and_topology_normalizers_are_stable(self) -> None:
        self.assertEqual(normalize_nccl_version(22707), "2.27.7")
        self.assertEqual(normalize_nccl_version(2804), "2.8.4")
        self.assertEqual(normalize_nccl_version((2, 27, 7)), "2.27.7")
        self.assertEqual(normalize_nccl_version((2, 27, 7, "cuda13")), "2.27.7+cuda13")
        left = " GPU0 GPU1 CPU Affinity\r\nGPU0 X NV18 0-63\r\nGPU1 NV18 X 0-63\r\n"
        right = " GPU1 GPU0 CPU Affinity\nGPU1 X NV18 0-63\nGPU0 NV18 X 0-63\n"
        self.assertEqual(normalize_topology_output(left), normalize_topology_output(right))
        self.assertEqual(topology_class_from_output(left), topology_class_from_output(right))

    def test_topology_graph_ignores_indices_order_and_legend_but_not_structure(self) -> None:
        left = """ GPU0 GPU1 NIC0 CPU Affinity NUMA Affinity
GPU0 X NV18 PIX 0-63 0
GPU1 NV18 X PHB 0-63 0
NIC0 PIX PHB X 0-63 0
Legend:
  X = Self
"""
        relabeled = """ NIC9 GPU7 GPU3 CPU Affinity
NIC9 X PHB PIX 0-63
GPU7 PHB X NV18 0-63
GPU3 PIX NV18 X 0-63
Legend: ignored wording
"""
        changed_edge = left.replace("PIX 0-63 0", "PHB 0-63 0", 1).replace(
            "NIC0 PIX PHB", "NIC0 PHB PHB", 1
        )
        changed_affinity = left.replace("GPU1 NV18 X PHB 0-63", "GPU1 NV18 X PHB 64-127")
        self.assertEqual(
            topology_class_from_output(left), topology_class_from_output(relabeled)
        )
        self.assertNotEqual(
            topology_class_from_output(left), topology_class_from_output(changed_edge)
        )
        self.assertNotEqual(
            topology_class_from_output(left), topology_class_from_output(changed_affinity)
        )

    def test_unsupported_topology_never_falls_back_to_raw_hash(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "unsupported"):
            topology_class_from_output("GPU0 X NV18\n")

    def test_atomic_exact_mode_retry_and_conflict(self) -> None:
        evidence = collect_runtime_evidence(
            torch_module=SimpleNamespace(cuda=FakeCuda()),
            command_runner=lambda command, **kwargs: SimpleNamespace(
                stdout=(
                    "600.12\n600.12\n"
                    if "--query-gpu=driver_version" in command
                    else " GPU0 GPU1 CPU Affinity\nGPU0 X NV18 0-63\nGPU1 NV18 X 0-63\n"
                ),
                stderr="",
                returncode=0,
            ),
            package_version_resolver=lambda package: "2.27.7" if package == "nvidia-nccl-cu13" else (_ for _ in ()).throw(importlib.metadata.PackageNotFoundError()),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "runtime-evidence.json"
            first = write_runtime_evidence(path, evidence)
            second = write_runtime_evidence(path, evidence)
            self.assertTrue(first["created"])
            self.assertFalse(second["created"])
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(load_runtime_evidence(path), evidence)

            value = json.loads(path.read_text(encoding="utf-8"))
            value["gpu_model"] = "different"
            replacement = path.with_name("replacement.json")
            replacement.write_text(json.dumps(value) + "\n", encoding="utf-8")
            replacement.chmod(0o600)
            replacement.replace(path)
            with self.assertRaisesRegex(FileExistsError, "conflict"):
                write_runtime_evidence(path, evidence)

    def test_atomic_write_accepts_inherited_supervisor_directory_fd(self) -> None:
        evidence = collect_runtime_evidence(
            torch_module=SimpleNamespace(cuda=FakeCuda()),
            command_runner=lambda command, **kwargs: SimpleNamespace(
                stdout=(
                    "600.12\n600.12\n"
                    if "--query-gpu=driver_version" in command
                    else " GPU0 GPU1 CPU Affinity\nGPU0 X NV18 0-63\nGPU1 NV18 X 0-63\n"
                ),
                stderr="",
                returncode=0,
            ),
            package_version_resolver=lambda package: "2.27.7" if package == "nvidia-nccl-cu13" else (_ for _ in ()).throw(importlib.metadata.PackageNotFoundError()),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            directory_fd = os.open(tmpdir, os.O_RDONLY | os.O_DIRECTORY)
            try:
                path = Path(f"/proc/self/fd/{directory_fd}/runtime-evidence.json")
                receipt = write_runtime_evidence(path, evidence)
                self.assertTrue(receipt["created"])
                self.assertEqual(load_runtime_evidence(path), evidence)
            finally:
                os.close(directory_fd)

    def test_subprocess_failure_does_not_leak_stderr_as_a_fact(self) -> None:
        def failed(command, **kwargs):
            raise subprocess.CalledProcessError(1, command, stderr="private diagnostic")

        with self.assertRaisesRegex(RuntimeError, "bounded command failed"):
            collect_runtime_evidence(
                torch_module=SimpleNamespace(cuda=FakeCuda()),
                command_runner=failed,
            )


if __name__ == "__main__":
    unittest.main()
