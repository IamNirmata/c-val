import unittest
import importlib.util
import select
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml


class PvcAccessDeploymentTests(unittest.TestCase):
    def test_access_pod_has_stable_startup_and_retains_pvc(self):
        root = Path(__file__).resolve().parents[1]
        pod = yaml.safe_load((root / "ymls/gcr-admin-pvc-access.yml").read_text())
        self.assertEqual(pod["kind"], "Pod")
        self.assertEqual(pod["metadata"]["name"], "gcr-admin-pvc-access")
        self.assertEqual(pod["metadata"]["namespace"], "gcr-admin")
        spec = pod["spec"]
        self.assertNotIn("nodeName", spec)
        self.assertIn("ccpu", spec["nodeSelector"]["kubernetes.io/hostname"])
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertNotIn("initContainers", spec)
        container = spec["containers"][0]
        self.assertEqual(container["name"], "pvcaccess")
        self.assertIn("@sha256:", container["image"])
        self.assertNotIn("nvidia.com/gpu", container["resources"]["requests"])
        self.assertEqual(container["resources"]["limits"]["memory"], "8Gi")
        mounts = {entry["mountPath"]: entry for entry in container["volumeMounts"]}
        self.assertEqual(mounts["/data"]["name"], "data")
        self.assertFalse(mounts["/data"].get("readOnly", False))
        self.assertEqual(mounts["/root/.vscode-server"]["name"], "vscode-server")
        claims = [volume["persistentVolumeClaim"] for volume in spec["volumes"] if "persistentVolumeClaim" in volume]
        self.assertEqual(claims, [{"claimName": "pvc-vast-gcr-admin"}])
        script = container["args"][0]
        self.assertNotIn("/data", script)
        self.assertNotIn("pip", script)
        self.assertNotIn("git", script)
        probe = container["readinessProbe"]["exec"]["command"]
        subprocess.run([sys.executable, *probe[1:]], check=True, capture_output=True, timeout=10)
        with subprocess.Popen([sys.executable, *container["command"][1:], *container["args"]], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            try:
                self.assertTrue(select.select([process.stdout], [], [], 10)[0], "access pod did not start")
                self.assertEqual(process.stdout.readline().strip(), "PVC access ready")
            finally:
                process.terminate()
                _output, error = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0, error)


class EvaluationDeploymentTests(unittest.TestCase):
    def test_cpu_pod_is_separate_and_read_only(self):
        root = Path(__file__).resolve().parents[1]
        pod = yaml.safe_load((root / "ymls/cval-evaluation-engine.yaml").read_text())
        self.assertEqual(pod["metadata"]["name"], "cval-evaluation-engine")
        spec = pod["spec"]
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertEqual(spec["restartPolicy"], "Always")
        self.assertIn("ccpu", spec["nodeSelector"]["kubernetes.io/hostname"])
        container = spec["containers"][0]
        self.assertNotIn("nvidia.com/gpu", container["resources"]["requests"])
        mounts = {mount["mountPath"]: mount for mount in container["volumeMounts"]}
        self.assertTrue(mounts["/data"]["readOnly"])
        self.assertEqual(mounts["/data/continuous_validation"], {
            "name": "output",
            "mountPath": "/data/continuous_validation",
            "subPath": "continuous_validation",
            "readOnly": False,
        })
        self.assertEqual(mounts["/evaluation"]["name"], "output")
        self.assertEqual(mounts["/data"]["name"], mounts["/evaluation"]["name"])
        claims = [volume for volume in spec["volumes"] if "persistentVolumeClaim" in volume]
        self.assertEqual(len(claims), 1)
        self.assertFalse(claims[0]["persistentVolumeClaim"].get("readOnly", False))
        self.assertIn("@sha256:", container["image"])
        self.assertEqual(mounts["/evaluation"]["subPath"], "continuous_validation/evaluation-engine")
        self.assertEqual(spec["initContainers"][0]["command"], ["python", "/source/bootstrap.py"])
        self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
        self.assertTrue(mounts["/app"]["readOnly"])
        self.assertEqual(mounts["/root/.vscode-server"]["name"], "vscode-server")
        self.assertFalse(mounts["/root/.vscode-server"].get("readOnly", False))
        volumes = {volume["name"]: volume for volume in spec["volumes"]}
        self.assertEqual(volumes["vscode-server"]["emptyDir"], {"sizeLimit": "2Gi"})
        self.assertEqual(container["resources"]["requests"]["ephemeral-storage"], "2Gi")
        self.assertEqual(container["resources"]["limits"]["ephemeral-storage"], "4Gi")
        self.assertFalse((root / "deploy/evaluation-engine/pod.yaml").exists())

    def test_rendered_bundle_is_immutable_and_exact(self):
        root = Path(__file__).resolve().parents[1]
        specification = importlib.util.spec_from_file_location("render_eval", root / "deploy/evaluation-engine/render.py")
        renderer = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(renderer)
        template = yaml.safe_load((root / "ymls/cval-evaluation-engine.yaml").read_text())
        source, dependency, pod = renderer.manifests("a" * 40, b"source", "pass", "dependency.whl", b"wheel", template)
        self.assertTrue(source["immutable"])
        self.assertTrue(dependency["immutable"])
        self.assertEqual(pod["metadata"]["annotations"]["cval/source-commit"], "a" * 40)
        self.assertEqual(len(pod["spec"]["containers"][0]["env"]), 7)
        mounts = {mount["mountPath"]: mount for mount in pod["spec"]["containers"][0]["volumeMounts"]}
        self.assertEqual(mounts["/root/.vscode-server"]["name"], "vscode-server")
        self.assertEqual(mounts["/data/continuous_validation"]["subPath"], "continuous_validation")
        self.assertFalse(mounts["/data/continuous_validation"]["readOnly"])
        self.assertTrue(mounts["/data"]["readOnly"])

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