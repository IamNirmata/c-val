import unittest
import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml


class EvaluationDeploymentTests(unittest.TestCase):
    def test_cpu_pod_is_separate_and_read_only(self):
        root = Path(__file__).resolve().parents[1]
        pod = yaml.safe_load((root / "deploy/evaluation-engine/pod.yaml").read_text())
        self.assertEqual(pod["metadata"]["name"], "cval-evaluation-engine")
        spec = pod["spec"]
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertEqual(spec["restartPolicy"], "Always")
        self.assertIn("ccpu", spec["nodeSelector"]["kubernetes.io/hostname"])
        container = spec["containers"][0]
        self.assertNotIn("nvidia.com/gpu", container["resources"]["requests"])
        mounts = {mount["mountPath"]: mount for mount in container["volumeMounts"]}
        self.assertTrue(mounts["/data"]["readOnly"])
        self.assertEqual(mounts["/evaluation"]["name"], "output")
        self.assertIn("@sha256:", container["image"])
        self.assertEqual(mounts["/evaluation"]["subPath"], "continuous_validation/evaluation-engine")
        self.assertEqual(spec["initContainers"][0]["command"], ["python", "/source/bootstrap.py"])

    def test_rendered_bundle_is_immutable_and_exact(self):
        root = Path(__file__).resolve().parents[1]
        specification = importlib.util.spec_from_file_location("render_eval", root / "deploy/evaluation-engine/render.py")
        renderer = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(renderer)
        template = yaml.safe_load((root / "deploy/evaluation-engine/pod.yaml").read_text())
        source, dependency, pod = renderer.manifests("a" * 40, b"source", "pass", "dependency.whl", b"wheel", template)
        self.assertTrue(source["immutable"])
        self.assertTrue(dependency["immutable"])
        self.assertEqual(pod["metadata"]["annotations"]["cval/source-commit"], "a" * 40)
        self.assertEqual(len(pod["spec"]["containers"][0]["env"]), 7)

    def test_lock_probe_detects_contention_and_release(self):
        script = Path(__file__).resolve().parents[1] / "deploy/evaluation-engine/check_locking.py"
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "probe.db")
            holder = subprocess.Popen([sys.executable, str(script), "--role", "hold", "--database", database], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertEqual(holder.stdout.readline().strip(), "locked")
                subprocess.run([sys.executable, str(script), "--role", "blocked", "--database", database], check=True, timeout=10)
            finally:
                _output, error = holder.communicate("release\n", timeout=10)
                self.assertEqual(holder.returncode, 0, error)
            subprocess.run([sys.executable, str(script), "--role", "released", "--database", database], check=True, timeout=10)