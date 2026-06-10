from __future__ import annotations

import unittest

from cval.jobs.renderer import render_validation_job


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

        self.assertEqual(rendered.job_name, "hari-gcr-ceval-slc01-cl02-hgx-0001-12345")
        self.assertIn("name: hari-gcr-ceval-slc01-cl02-hgx-0001-12345", rendered.yaml_text)
        self.assertIn('kubernetes.io/hostname: "slc01-cl02-hgx-0001"', rendered.yaml_text)
        self.assertIn('value: "12345"', rendered.yaml_text)
        self.assertIn('value: "abc123"', rendered.yaml_text)
        self.assertNotIn("placeholder", rendered.yaml_text)

    def test_rejects_missing_placeholder(self) -> None:
        with self.assertRaisesRegex(ValueError, "time-placeholder"):
            render_validation_job(TEMPLATE.replace("time-placeholder", ""), "slc01-cl02-hgx-0001")


if __name__ == "__main__":
    unittest.main()