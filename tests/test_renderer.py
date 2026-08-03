from __future__ import annotations

import unittest
import re
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from cval.config import load_config
from cval.jobs.renderer import (
    default_template_path,
    render_validation_job,
    render_validation_job_from_file,
)
from cval.validation.runtime import _decode_runtime_environment, effective_config_digest
from cval.validation.registry import ValidationTestRegistry, load_test_registry


TEMPLATE = (
        Path(__file__).resolve().parents[1] / "ymls/specific-node-job.yml"
).read_text(encoding="utf-8")
COMMIT = "a" * 40


class RendererTests(unittest.TestCase):
    def test_renders_specific_node_job(self) -> None:
        rendered = render_validation_job(
          TEMPLATE,
          "slc01-cl02-hgx-0001",
          timestamp=12345,
          git_ref=COMMIT,
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
        self.assertIn(f'value: "{COMMIT}"', rendered.yaml_text)
        self.assertIn('git -C "$CVAL_REPO_DIR" checkout --detach FETCH_HEAD', rendered.yaml_text)
        self.assertIn('test "$(git -C "$CVAL_REPO_DIR" rev-parse HEAD)" = "$CVAL_GIT_REF"', rendered.yaml_text)
        self.assertNotRegex(rendered.yaml_text, r"[a-z0-9-]+-placeholder")

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

    def test_rejects_runtime_contract_spoofed_in_comments(self) -> None:
        unsafe = TEMPLATE.replace(
            'command: ["/bin/bash", "-lc"]',
            'command: ["sh", "-c"]',
        ).replace(
            "                  git init",
            "                  # git init",
        )

        with self.assertRaisesRegex(ValueError, "/bin/bash -lc"):
            render_validation_job(
                unsafe,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
            )

    def test_rejects_runtime_contract_in_decoy_container(self) -> None:
        decoy = """
          initContainers:
            - name: decoy
              env:
                - name: CVAL_RUNTIME_ENV_B64
                  value: "runtime-environment-b64-placeholder"
              command: ["/bin/bash", "-lc"]
"""
        unsafe = TEMPLATE.replace("          containers:\n", decoy + "          containers:\n")

        with self.assertRaisesRegex(
            ValueError,
            "must not contain init containers|exactly one CVAL_RUNTIME_ENV_B64",
        ):
            render_validation_job(
                unsafe,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
            )

    def test_rejects_second_volcano_task(self) -> None:
        second_task = """
    - name: unsafe-extra
      replicas: 1
      template:
        spec:
          containers:
            - name: unsafe
              image: busybox
"""
        unsafe = TEMPLATE.replace(
            "  tasks:\n",
            "  tasks:\n" + second_task,
            1,
        )

        with self.assertRaisesRegex(ValueError, "exactly one Volcano task"):
            render_validation_job(
                unsafe,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
            )

    def test_rejects_flow_style_sidecar_container(self) -> None:
        unsafe = TEMPLATE.replace(
            "            - name: server\n",
            "            - {name: decoy, image: busybox}\n"
            "            - name: server\n",
            1,
        )

        with self.assertRaisesRegex(ValueError, "exactly one task workload"):
            render_validation_job(
                unsafe,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
            )

    def test_rejects_flow_style_structural_keys(self) -> None:
        variants = (
            TEMPLATE.replace(
                "          containers:\n",
                "          initContainers: [{name: decoy, image: busybox}]\n"
                "          containers:\n",
                1,
            ),
            TEMPLATE.replace(
                "  tasks:\n",
                "  tasks: []\n  tasks:\n",
                1,
            ),
            TEMPLATE.replace(
                "          containers:\n",
                "          containers: []\n          containers:\n",
                1,
            ),
            TEMPLATE.replace(
                "  tasks:\n",
                "  'tasks': []\n  tasks:\n",
                1,
            ),
            TEMPLATE.replace(
                "          containers:\n",
                '          "containers": []\n          containers:\n',
                1,
            ),
            TEMPLATE.replace(
                "          containers:\n",
                "          decoy: {'initContainers': []}\n"
                "          containers:\n",
                1,
            ),
            TEMPLATE.replace(
                "  tasks:\n",
                '  "ta\\u0073ks": []\n  tasks:\n',
                1,
            ),
            TEMPLATE.replace(
                "  tasks:\n",
                '  ? "ta\\u0073ks"\n  : []\n  tasks:\n',
                1,
            ),
            TEMPLATE.replace(
                "          containers:\n",
                '          !!str "conta\\u0069ners": []\n'
                "          containers:\n",
                1,
            ),
        )
        for unsafe in variants:
            with self.subTest(template=unsafe[:80]), self.assertRaises(ValueError):
                render_validation_job(
                    unsafe,
                    "slc01-cl02-hgx-0001",
                    timestamp=12345,
                )

    def test_rejects_post_substitution_resource_key_collision(self) -> None:
        base = load_config()
        collision = replace(
            base.job_template,
            rdma_resource_name=base.job_template.gpu_resource_name,
        )

        with self.assertRaisesRegex(ValueError, "resource names must be distinct"):
            render_validation_job(
                TEMPLATE,
                "slc01-cl02-hgx-0001",
                timestamp=12345,
                job_template_config=collision,
                cval_config=base,
            )

    def test_final_rendered_manifest_is_duplicate_checked(self) -> None:
        base = load_config()
        for resource_name in ("cpu", '"c\\u0070u"', "!!str cpu"):
            with self.subTest(resource_name=resource_name), self.assertRaisesRegex(
                ValueError,
                "duplicate-free YAML",
            ):
                render_validation_job(
                    TEMPLATE,
                    "slc01-cl02-hgx-0001",
                    timestamp=12345,
                    job_template_config=replace(
                        base.job_template,
                        gpu_resource_name=resource_name,
                    ),
                    cval_config=base,
                )

    def test_repository_template_renders_configured_runtime_env(self) -> None:
        config = load_config()
        rendered = render_validation_job_from_file(
            default_template_path(),
            "slc01-cl02-hgx-0001",
            timestamp=12345,
            git_ref=COMMIT,
            cval_config=config,
        )

        self.assertNotRegex(rendered.yaml_text, r"[a-z0-9-]+-placeholder")
        self.assertIn('name: CVAL_REPO_DIR', rendered.yaml_text)
        self.assertIn('value: "/workspace/c-val"', rendered.yaml_text)
        self.assertIn('name: CVAL_RUN_ID', rendered.yaml_text)
        self.assertIn('value: "slc01-cl02-hgx-0001-12345"', rendered.yaml_text)
        self.assertIn('name: CVAL_RUNTIME_ENV_B64', rendered.yaml_text)
        self.assertIn('name: CVAL_IMAGE_NAME', rendered.yaml_text)
        self.assertIn('value: "pytorch:26.05-py3"', rendered.yaml_text)
        runtime = self._runtime_environment_text(rendered.yaml_text)
        self.assertIn("export RUN_STORAGE=true", runtime)
        self.assertIn("export RUN_NCCL=true", runtime)
        self.assertIn("export RUN_DLTEST=true", runtime)
        self.assertIn("export CVAL_NCCL_GPU_COUNT=8", runtime)
        self.assertIn("export CVAL_DL_GPU_COUNT=8", runtime)
        self.assertIn("export CVAL_IBBW_START_DEVICE=''", runtime)
        self.assertIn("export CVAL_IBBW_END_DEVICE=''", runtime)
        self.assertIn("export CVAL_DL_ITERATIONS=100", runtime)
        self.assertIn("export CVAL_NCCL_NET=IB", runtime)
        self.assertIn("export CVAL_NCCL_EVALUATION_ENABLED=false", runtime)
        self.assertIn(
            "export CVAL_NCCL_OUTBOX_ROOT=/data/continuous_validation/nccl_eval/outbox",
            runtime,
        )
        self.assertIn("export CVAL_ENABLED_TESTS=storage,nccl,dltest", runtime)
        self.assertIn("export CVAL_CONFIG_PATH=/workspace/c-val/config/cval.toml", runtime)
        self.assertIn(
            f"export CVAL_CONFIG_DIGEST={effective_config_digest(config)}", runtime
        )
        self.assertIn("export CVAL_TEST_REGISTRY_JSON=", runtime)
        self.assertNotIn("validation-8", rendered.yaml_text)
        self.assertIn('nvidia.com/gpu: "8"', rendered.yaml_text)
        self.assertIn('rdma/rdma_shared_device_a: "1"', rendered.yaml_text)
        self.assertIn('cpu: "100"', rendered.yaml_text)
        self.assertIn('memory: "1500Gi"', rendered.yaml_text)
        self.assertIn('sizeLimit: 256Gi', rendered.yaml_text)

    def test_repository_template_tolerates_cordon_taint(self) -> None:
        rendered = render_validation_job_from_file(
            default_template_path(),
            "slc01-cl02-hgx-0001",
            timestamp=12345,
            git_ref=COMMIT,
        )
        # Validation must be able to land on a cordoned (suspected-unhealthy) node.
        self.assertIn("node.kubernetes.io/unschedulable", rendered.yaml_text)

    def test_repository_template_renders_disabled_test_switch(self) -> None:
        with TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[tests.storage]
enabled = true
[tests.nccl]
enabled = false
[tests.dltest]
enabled = true
""",
                encoding="utf-8",
            )
            config = load_config(config_path)
            rendered = render_validation_job_from_file(
                default_template_path(),
                "slc01-cl02-hgx-0001",
                timestamp=12345,
                git_ref=COMMIT,
                cval_config=config,
            )

        runtime = self._runtime_environment_text(rendered.yaml_text)
        self.assertIn("export RUN_NCCL=false", runtime)
        self.assertIn("export CVAL_ENABLED_TESTS=storage,dltest", runtime)

    def test_repository_template_has_no_test_specific_placeholders(self) -> None:
        template = default_template_path().read_text(encoding="utf-8")

        self.assertNotIn("test-storage-", template)
        self.assertNotIn("test-nccl-", template)
        self.assertNotIn("test-dltest-", template)
        self.assertIn("runtime-environment-b64-placeholder", template)
        self.assertIn("set -euo pipefail", template)
        self.assertIn("cval.validation.supervisor", template)
        self.assertNotIn("cval.validation.path_preflight", template)
        self.assertNotIn("mkdir -p", template)
        self.assertNotIn("tee -a", template)
        self.assertNotIn("set -o noclobber", template)
        for environment_name in (
            "RUN_STORAGE",
            "RUN_NCCL",
            "RUN_DLTEST",
            "CVAL_NCCL_ITERATIONS",
            "CVAL_NCCL_OUTBOX_ROOT",
            "CVAL_DL_ITERATIONS",
        ):
            self.assertNotIn(f"name: {environment_name}", template)

    def test_synthetic_fourth_test_appears_without_template_change(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            test_dir = root / "validation-tests" / "smoke"
            test_dir.mkdir(parents=True)
            (test_dir / "setup.sh").write_text("#!/bin/bash\nexit 0\n")
            (test_dir / "run-test.sh").write_text("#!/bin/bash\nexit 0\n")
            (test_dir / "test_config.toml").write_text(
                """
schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 999
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
"""
            )
            smoke_registry = load_test_registry(
                {
                    "smoke": {
                        "enabled": True,
                        "config_path": "validation-tests/smoke/test_config.toml",
                    }
                },
                repo_root=root,
                include_defaults=False,
            )
            config = load_config()
            combined = ValidationTestRegistry(
                (*config.tests.registry.tests, *smoke_registry.tests)
            )
            config = replace(config, tests=replace(config.tests, registry=combined))

            rendered = render_validation_job_from_file(
                default_template_path(),
                "slc01-cl02-hgx-0001",
                timestamp=12345,
                git_ref=COMMIT,
                cval_config=config,
            )

        runtime = self._runtime_environment_text(rendered.yaml_text)
        self.assertIn("export CVAL_ENABLED_TESTS=storage,nccl,dltest,smoke", runtime)
        self.assertIn('"smoke":', runtime)

    @staticmethod
    def _runtime_environment_text(yaml_text: str) -> str:
        match = re.search(
            r'name: CVAL_RUNTIME_ENV_B64\n\s+.*?value: "([A-Za-z0-9+/=]+)"',
            yaml_text,
            flags=re.DOTALL,
        )
        if match is None:
            raise AssertionError("CVAL_RUNTIME_ENV_B64 not found in rendered YAML")
        return _decode_runtime_environment(match.group(1))


if __name__ == "__main__":
    unittest.main()