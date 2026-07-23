from __future__ import annotations

import unittest

from cval.jobs.renderer import default_template_path, render_validation_job, render_validation_job_from_file


TEMPLATE = """
apiVersion: batch.volcano.sh/v1alpha1
kind: Job
metadata:
  generateName: jobname-placeholder
spec:
  tasks:
    - template:
        spec:
          nodeSelector:
            kubernetes.io/hostname: "nodename-placeholder"
          containers:
            - env:
                - name: GCRTIME
                  value: "time-placeholder"
                - name: CVAL_GIT_REPO
                  value: "git-repo-placeholder"
                - name: CVAL_GIT_REF
                  value: "git-ref-placeholder"
"""


class RendererTests(unittest.TestCase):
    def test_renders_specific_node_job(self) -> None:
        rendered = render_validation_job(
          TEMPLATE,
          "slc01-cl02-hgx-0001",
          timestamp=12345,
          git_ref="abc123",
        )

        self.assertEqual(
          rendered.job_name,
          "cval-slc01-cl02-hgx-0001-pytorch-26-05-py3-12345",
        )
        self.assertIn(
          "name: cval-slc01-cl02-hgx-0001-pytorch-26-05-py3-12345",
          rendered.yaml_text,
        )
        self.assertIn('kubernetes.io/hostname: "slc01-cl02-hgx-0001"', rendered.yaml_text)
        self.assertIn('value: "12345"', rendered.yaml_text)
        self.assertIn('value: "abc123"', rendered.yaml_text)
        self.assertNotIn("placeholder", rendered.yaml_text)

    def test_rejects_volcano_pod_name_overflow(self) -> None:
        with self.assertRaisesRegex(ValueError, "too long for Volcano pod naming"):
            render_validation_job(
                TEMPLATE,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
                job_prefix="custom-cval",
            )

    def test_rejects_missing_placeholder(self) -> None:
        with self.assertRaisesRegex(ValueError, "time-placeholder"):
            render_validation_job(TEMPLATE.replace("time-placeholder", ""), "slc01-cl02-hgx-0001")

    def test_rejects_job_name_over_kubernetes_limit(self) -> None:
        long_node = "slc01-cl02-hgx-" + "0" * 40

        with self.assertRaisesRegex(ValueError, "63-character"):
            render_validation_job(TEMPLATE, long_node, timestamp=12345)

    def test_rejects_multiline_substitution_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "git-ref-placeholder"):
            render_validation_job(
                TEMPLATE,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
                git_ref="main\nbad: yaml",
            )

    def test_rejects_empty_substitution_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "git-repo-placeholder"):
            render_validation_job(
                TEMPLATE,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
                git_repo="",
            )

    def test_repository_template_renders_configured_runtime_env(self) -> None:
      rendered = render_validation_job_from_file(
        default_template_path(),
        "slc01-cl02-hgx-0001",
        timestamp=12345,
        git_ref="abc123",
      )

      self.assertNotRegex(rendered.yaml_text, r"[a-z0-9-]+-placeholder")
      self.assertIn('name: CVAL_REPO_DIR', rendered.yaml_text)
      self.assertIn('value: "/workspace/c-val"', rendered.yaml_text)
      self.assertIn('name: CVAL_GPU_COUNT', rendered.yaml_text)
      self.assertIn('name: CVAL_GPU_COUNT\n                  value: "8"', rendered.yaml_text)
      self.assertIn('name: CVAL_IMAGE_NAME', rendered.yaml_text)
      self.assertIn('value: "pytorch:26.05-py3"', rendered.yaml_text)
      self.assertIn('name: CVAL_IBBW_START_DEVICE', rendered.yaml_text)
      self.assertIn('name: CVAL_IBBW_START_DEVICE\n                  value: "0"', rendered.yaml_text)
      self.assertIn('name: CVAL_IBBW_END_DEVICE', rendered.yaml_text)
      self.assertIn('name: CVAL_IBBW_END_DEVICE\n                  value: "13"', rendered.yaml_text)
      self.assertIn('name: CVAL_DL_ITERATIONS\n                  value: "100"', rendered.yaml_text)
      self.assertNotIn("validation-8", rendered.yaml_text)

    def test_repository_template_tolerates_cordon_taint(self) -> None:
        rendered = render_validation_job_from_file(
            default_template_path(),
            "slc01-cl02-hgx-0001",
            timestamp=12345,
            git_ref="abc123",
        )
        # Validation must be able to land on a cordoned (suspected-unhealthy) node.
        self.assertIn("node.kubernetes.io/unschedulable", rendered.yaml_text)


if __name__ == "__main__":
    unittest.main()