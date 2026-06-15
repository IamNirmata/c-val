from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cval.config import config_to_dict, load_config


class ConfigTests(unittest.TestCase):
    def test_loads_repository_default_config(self) -> None:
        config = load_config()

        self.assertEqual(config.job.job_prefix, "cval")
        self.assertEqual(config.cluster.namespace, "gcr-admin")
        self.assertEqual(config.runtime.repo_dir, "/workspace/c-val")
        self.assertEqual(config.validation.gpu_count, 8)
        self.assertEqual(config.validation.ibbw_start_device, 0)
        self.assertEqual(config.validation.ibbw_end_device, 12)
        self.assertTrue(str(config.job.template_path).endswith("ymls/specific-node-job.yml"))

    def test_loads_partial_override_with_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[cluster]
namespace = "staging"

[scheduling]
batch_size = 2

[runtime]
validation_root = "/tmp/cval"

[validation]
dl_iterations = 3
""",
                encoding="utf-8",
            )

            config = load_config(config_path)

        self.assertEqual(config.cluster.namespace, "staging")
        self.assertEqual(config.scheduling.batch_size, 2)
        self.assertEqual(config.runtime.validation_root, "/tmp/cval")
        self.assertEqual(config.validation.dl_iterations, 3)
        self.assertEqual(config.job.git_ref, "main")

    def test_config_to_dict_is_json_ready(self) -> None:
        data = config_to_dict(load_config())

        self.assertIsInstance(data["job"]["template_path"], str)


if __name__ == "__main__":
    unittest.main()