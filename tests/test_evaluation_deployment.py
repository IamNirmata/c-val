import unittest
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
        self.assertIn("REPLACE_WITH", container["image"])