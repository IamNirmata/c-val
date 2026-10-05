from __future__ import annotations

import unittest
import base64
import hashlib
import importlib.util
import io
import json
import os
import re
import select
import subprocess
import sys
import tarfile
import textwrap
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from cval.config import load_config
from cval.jobs.renderer import (
    _RUNTIME_BOOTSTRAP,
    default_template_path,
    render_validation_job,
    render_validation_job_from_file,
)
from cval.validation.runtime import _decode_runtime_environment, effective_config_digest
from cval.validation.registry import ValidationTestRegistry, load_test_registry
import yaml


TEMPLATE = (
        Path(__file__).resolve().parents[1] / "ymls/specific-node-job.yml"
).read_text(encoding="utf-8")
COMMIT = "a" * 40
SCHEDULER_TEMPLATE = (Path(__file__).resolve().parents[1] / "ymls/test-job.yml").read_text(encoding="utf-8")


class RendererTests(unittest.TestCase):
    def test_single_attempt_policy_uses_volcano_wire_event(self) -> None:
        config = load_config()
        events = ["PodFailed", "PodEvicted", "TaskFailed", "Unknown"]
        for template, node in ((TEMPLATE, "slc01-cl02-hgx-0062"), (SCHEDULER_TEMPLATE, None)):
            with self.subTest(node=node):
                rendered = render_validation_job(
                    template, node, timestamp=1790300635, git_ref=COMMIT, cval_config=config
                )
                spec = yaml.safe_load(rendered.yaml_text)["spec"]
                self.assertEqual(spec["policies"], [{"events": events, "action": "TerminateJob"}])
                self.assertEqual(spec["maxRetry"], 1)
                task = spec["tasks"][0]
                self.assertEqual(task["maxRetry"], 1)
                self.assertEqual(task["replicas"], 1)
                self.assertNotIn("policies", task)
                self.assertEqual(task["template"]["spec"]["restartPolicy"], "Never")
        for rejected in ((*events[:-1], "JobUnknown"), tuple(events[:-1]), (*events, "Unknown")):
            with self.subTest(events=rejected), self.assertRaisesRegex(ValueError, "failure_events"):
                render_validation_job(
                    TEMPLATE, "slc01-cl02-hgx-0062", timestamp=1790300635, git_ref=COMMIT,
                    cval_config=replace(config, job_template=replace(config.job_template, failure_events=rejected)),
                )

    def test_shared_bootstrap_is_embedded_in_both_source_transports(self) -> None:
        base = load_config()
        for offline in (False, True):
            config = replace(
                base,
                job=replace(
                    base.job,
                    source_configmap="cval-exact-source" if offline else "",
                    source_sha256="b" * 64 if offline else "",
                ),
            )
            for template, node in ((TEMPLATE, "node-a"), (SCHEDULER_TEMPLATE, None)):
                with self.subTest(offline=offline, node=node):
                    rendered = render_validation_job(
                        template, node, timestamp=12345, git_ref=COMMIT, cval_config=config
                    )
                    pod = yaml.safe_load(rendered.yaml_text)["spec"]["tasks"][0]["template"]["spec"]
                    self.assertEqual(pod["containers"][0]["args"], [_RUNTIME_BOOTSTRAP])
                    self.assertEqual(pod["containers"][0]["ports"][0]["protocol"], "TCP")
                    self.assertNotRegex(rendered.yaml_text, r"[a-z0-9-]+-placeholder")
                    subprocess.run(["bash", "-n"], input=_RUNTIME_BOOTSTRAP, text=True, check=True)

    def test_shared_bootstrap_rejects_mixed_misplaced_or_duplicate_markers(self) -> None:
        marker = "runtime-bootstrap-placeholder"
        variants = (
            TEMPLATE.replace(marker, "echo missing-bootstrap"),
            TEMPLATE.replace(marker, f"# {marker}"),
            TEMPLATE.replace(marker, f"{marker}\n                  echo unexpected"),
            TEMPLATE.replace(marker, f"{marker}\n                  {marker}"),
            TEMPLATE.replace(
                "  namespace: namespace-placeholder",
                f"  namespace: namespace-placeholder\n  annotations:\n    bootstrap: {marker}",
            ),
            TEMPLATE.replace(f"- |\n                  {marker}", f'- "{marker}"'),
        )
        for unsafe in variants:
            with self.subTest(template=unsafe), self.assertRaisesRegex(ValueError, "bootstrap|args"):
                render_validation_job(unsafe, "node-a", timestamp=12345, git_ref=COMMIT)

    def test_expanded_bootstrap_still_requires_executable_safety_contract(self) -> None:
        for original, replacement in (
            ('cd "$CVAL_REPO_DIR"', 'cd "$CVAL_REPO_DIR"+'),
            ("source /tmp/cval-runtime.env", "# source /tmp/cval-runtime.env"),
            ("exec python3 -m cval.validation.supervisor", "exec python3 -m cval.validation.path_preflight"),
        ):
            with self.subTest(line=original), patch(
                "cval.jobs.renderer._RUNTIME_BOOTSTRAP",
                _RUNTIME_BOOTSTRAP.replace(original, replacement),
            ), self.assertRaisesRegex(ValueError, "runtime contract|bootstrap"):
                render_validation_job(TEMPLATE, "node-a", timestamp=12345, git_ref=COMMIT)

    def test_valid_inline_bootstrap_templates_remain_supported(self) -> None:
        inline = TEMPLATE.replace(
            "                  runtime-bootstrap-placeholder",
            textwrap.indent(_RUNTIME_BOOTSTRAP.rstrip("\n"), "                  "),
        )
        shared = render_validation_job(TEMPLATE, "node-a", timestamp=12345, git_ref=COMMIT)
        rendered = render_validation_job(inline, "node-a", timestamp=12345, git_ref=COMMIT)
        self.assertEqual(rendered.yaml_text, shared.yaml_text)

    def test_inline_bootstrap_rejects_extra_commands(self) -> None:
        for script in ("echo unexpected\n" + _RUNTIME_BOOTSTRAP, _RUNTIME_BOOTSTRAP + "\necho unexpected\n"):
            inline = TEMPLATE.replace("                  runtime-bootstrap-placeholder",
                                      textwrap.indent(script.rstrip("\n"), "                  "))
            with self.subTest(script=script), self.assertRaisesRegex(ValueError, "canonical runtime bootstrap"):
                render_validation_job(inline, "node-a", timestamp=12345, git_ref=COMMIT)

    def test_scheduler_selected_template_uses_assigned_node_and_standard_runner(self) -> None:
        config = load_config()
        config = replace(config, job=replace(config.job, source_configmap="cval-exact-source", source_sha256="b" * 64))
        rendered = render_validation_job(SCHEDULER_TEMPLATE, None, timestamp=12345, git_ref=COMMIT, cval_config=config)
        manifest = yaml.safe_load(rendered.yaml_text)
        pod = manifest["spec"]["tasks"][0]["template"]["spec"]
        self.assertEqual(manifest["apiVersion"], "batch.volcano.sh/v1alpha1")
        self.assertEqual(rendered.node_name, "")
        self.assertIn("-scheduled-", rendered.job_name)
        self.assertFalse(any(key in pod for key in ("nodeSelector", "nodeName", "affinity")))
        self.assertNotIn("node.kubernetes.io/unschedulable", {entry["key"] for entry in pod["tolerations"]})
        container = pod["containers"][0]
        environment = {entry["name"]: entry for entry in container["env"]}
        self.assertEqual(environment["GCRNODE"]["valueFrom"]["fieldRef"]["fieldPath"], "spec.nodeName")
        self.assertEqual(environment["CVAL_RUN_ID"]["value"], "$(GCRNODE)-12345")
        self.assertEqual(environment["CVAL_GIT_REF"]["value"], COMMIT)
        self.assertEqual(pod["volumes"][-1]["configMap"]["name"], "cval-exact-source")
        pinned = yaml.safe_load(render_validation_job(TEMPLATE, "node-a", timestamp=12345, git_ref=COMMIT, cval_config=config).yaml_text)["spec"]["tasks"][0]["template"]["spec"]["containers"][0]
        self.assertEqual(container["resources"], pinned["resources"])
        self.assertEqual(environment["CVAL_RUNTIME_ENV_B64"]["value"], next(entry["value"] for entry in pinned["env"] if entry["name"] == "CVAL_RUNTIME_ENV_B64"))
        executable = lambda script: "\n".join(line for line in script.splitlines() if not line.lstrip().startswith("#"))
        self.assertEqual(executable(container["args"][0]), executable(pinned["args"][0]))
        subprocess.run(["bash", "-n"], input=container["args"][0], text=True, check=True)
        self.assertNotRegex(rendered.yaml_text, r"[a-z0-9-]+-placeholder")

    def test_scheduler_selected_render_rejects_unsafe_binding_and_moving_ref(self) -> None:
        with self.assertRaisesRegex(ValueError, "nodename-placeholder"):
            render_validation_job(SCHEDULER_TEMPLATE, "node-a", git_ref=COMMIT)
        with self.assertRaisesRegex(ValueError, "node placement"):
            render_validation_job(TEMPLATE, None, git_ref=COMMIT)
        with self.assertRaisesRegex(ValueError, "exact commit"):
            render_validation_job(SCHEDULER_TEMPLATE, None, git_ref="main")
        for original, replacement in (("spec.nodeName", "metadata.name"), ("$(GCRNODE)-time-placeholder", "synthetic-time-placeholder")):
            with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, "Scheduler-selected test"):
                render_validation_job(SCHEDULER_TEMPLATE.replace(original, replacement), None, git_ref=COMMIT)

    def test_offline_source_requires_complete_exact_identity(self) -> None:
        config = load_config()
        for source_name, source_digest, revision in (("", "b" * 64, COMMIT), ("source", "", COMMIT), ("source", "b" * 64, "main"), ("invalid/name", "b" * 64, COMMIT)):
            with self.subTest(source=source_name, revision=revision), self.assertRaisesRegex(ValueError, "offline source requires"):
                render_validation_job(TEMPLATE, "node-a", git_ref=revision, cval_config=replace(config, job=replace(config.job, source_configmap=source_name, source_sha256=source_digest)))

    def test_offline_bootstrap_verifies_digest_and_archive_commit(self) -> None:
        config = load_config()
        config = replace(config, job=replace(config.job, source_configmap="cval-exact-source", source_sha256="b" * 64))
        rendered = render_validation_job(TEMPLATE, "node-a", git_ref=COMMIT, cval_config=config)
        pod = yaml.safe_load(rendered.yaml_text)["spec"]["tasks"][0]["template"]["spec"]
        container = pod["containers"][0]
        self.assertEqual(pod["volumes"][-1]["configMap"]["name"], "cval-exact-source")
        self.assertTrue(container["volumeMounts"][-1]["readOnly"])
        script = container["args"][0].split("python3 - <<'PY'\n", 1)[1].split("\nPY", 1)[0]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            payload = io.BytesIO()
            with tarfile.open(fileobj=payload, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": COMMIT}) as archive:
                entry = tarfile.TarInfo("marker.txt")
                entry.size = 5
                archive.addfile(entry, io.BytesIO(b"exact"))
            source = root / "source.tar.gz"
            source.write_bytes(payload.getvalue())
            digest = hashlib.sha256(payload.getvalue()).hexdigest()
            script = script.replace('Path("/cval-source/source.tar.gz")', f"Path({str(source)!r})")
            for condition, revision, checksum in (("valid", COMMIT, digest), ("wrong-commit", "c" * 40, digest), ("wrong-digest", COMMIT, "0" * 64)):
                with self.subTest(condition=condition):
                    destination = root / condition
                    result = subprocess.run([sys.executable, "-c", script], env=os.environ | {"CVAL_GIT_REF": revision, "CVAL_SOURCE_SHA256": checksum, "CVAL_REPO_DIR": str(destination)}, capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode == 0, condition == "valid", result.stderr)
                    if condition == "valid":
                        self.assertEqual((destination / "marker.txt").read_bytes(), b"exact")
                    else:
                        self.assertFalse(destination.exists())

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
        self.assertNotIn("EVALUATION", runtime)
        self.assertNotIn("OUTBOX", runtime)
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
        self.assertIn("runtime-bootstrap-placeholder", template)
        self.assertNotIn("git init", template)
        self.assertNotIn("cval.validation.supervisor", template)
        rendered = render_validation_job(template, "node-a", timestamp=12345, git_ref=COMMIT)
        self.assertIn("set -euo pipefail", rendered.yaml_text)
        self.assertIn("cval.validation.supervisor", rendered.yaml_text)
        self.assertNotIn("cval.validation.path_preflight", rendered.yaml_text)
        self.assertNotIn("mkdir -p", rendered.yaml_text)
        self.assertNotIn("tee -a", rendered.yaml_text)
        self.assertNotIn("set -o noclobber", rendered.yaml_text)
        for environment_name in (
            "RUN_STORAGE",
            "RUN_NCCL",
            "RUN_DLTEST",
            "CVAL_NCCL_ITERATIONS",
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
        self.assertEqual(container["resources"]["requests"]["cpu"], "2")
        self.assertEqual(container["resources"]["limits"]["cpu"], "8")
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


class SourceConfigMapTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = Path(__file__).resolve().parents[1] / "scripts/cval-source.py"
        spec = importlib.util.spec_from_file_location("cval_source", path)
        cls.source = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.source)

    def test_committed_runtime_bundle_is_self_contained(self) -> None:
        repo = self.source.REPO
        revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, check=True,
                                  capture_output=True, text=True, timeout=15).stdout.strip()
        archives = {}
        bundles = {}
        for label, paths in (("runtime", self.source.SOURCE_PATHS),
                             ("with_tests", (*self.source.SOURCE_PATHS, "tests"))):
            bundles[label] = subprocess.run(
                ["git", "archive", "--format=tar.gz", revision, *paths], cwd=repo,
                check=True, capture_output=True, timeout=30,
            ).stdout
            with tarfile.open(fileobj=io.BytesIO(bundles[label]), mode="r:gz") as archive:
                self.assertEqual(archive.pax_headers["comment"], revision)
                archives[label] = {item.name: archive.extractfile(item).read()
                                   for item in archive.getmembers() if item.isfile()}
        removed = set(archives["with_tests"]) - set(archives["runtime"])
        self.assertTrue(removed)
        self.assertTrue(all(name.startswith("tests/") for name in removed))
        self.assertFalse(any(name.startswith("tests/") for name in archives["runtime"]))
        self.assertTrue(all(content == archives["with_tests"][name]
                            for name, content in archives["runtime"].items()))
        self.assertLess(len(bundles["runtime"]), len(bundles["with_tests"]))
        manifest = self.source.source_manifest(revision, bundles["runtime"], "gcr-admin")
        self.assertTrue(manifest["immutable"])
        self.assertEqual(base64.b64decode(manifest["binaryData"]["source.tar.gz"]), bundles["runtime"])
        with self.assertRaisesRegex(ValueError, "exact nonzero"):
            self.source.source_manifest("main", bundles["runtime"], "gcr-admin")
        check = textwrap.dedent("""
            import importlib, json, runpy, subprocess, sys
            from dataclasses import replace
            from pathlib import Path
            root = Path(sys.argv[1])
            sys.path.insert(0, str(root))
            from cval.config import load_config, encode_config_snapshot, load_config_snapshot
            from cval.cli import build_parser
            from cval.jobs.renderer import render_validation_job_from_file, _RUNTIME_BOOTSTRAP
            import yaml
            config = load_config(root / 'config/cval.toml')
            build_parser(config)
            for path in (root / 'cval').rglob('*.py'):
                relative = path.relative_to(root).with_suffix('')
                parts = relative.parts[:-1] if path.name == '__init__.py' else relative.parts
                module = importlib.import_module('.'.join(parts))
                assert Path(module.__file__).is_relative_to(root)
            for helper in ('dl-metric-lock.py', 'cval-node-cooldown.py'):
                runpy.run_path(str(root / 'scripts' / helper), run_name='bundle_check')
            assert not (root / 'tests').exists()
            count = 0
            for offline in (False, True):
                selected = replace(config, job=replace(config.job, git_ref=sys.argv[2],
                    source_configmap=sys.argv[3] if offline else '', source_sha256=sys.argv[4] if offline else ''))
                for template, node in (('specific-node-job.yml', 'slc01-cl02-hgx-0359'), ('test-job.yml', None)):
                    rendered = render_validation_job_from_file(root / 'ymls' / template,
                        node_name=node, timestamp=1790277187, git_ref=sys.argv[2], cval_config=selected)
                    pod = yaml.safe_load(rendered.yaml_text)['spec']['tasks'][0]['template']['spec']
                    assert pod['containers'][0]['args'] == [_RUNTIME_BOOTSTRAP]
                    assert ('nodeSelector' in pod) == (node is not None)
                    snapshot = encode_config_snapshot(selected)
                    assert encode_config_snapshot(load_config_snapshot(snapshot, repo_root=root)) == snapshot
                    count += 1
            subprocess.run(['bash', '-n'], input=_RUNTIME_BOOTSTRAP, text=True, check=True, timeout=10)
            print(json.dumps({'renders': count, 'dynamic_helpers': 2}))
        """)
        with TemporaryDirectory() as directory:
            with tarfile.open(fileobj=io.BytesIO(bundles["runtime"]), mode="r:gz") as archive:
                archive.extractall(directory, filter="data")
            environment = {name: value for name, value in os.environ.items()
                           if not name.startswith(("CVAL_", "PYTHON"))}
            result = subprocess.run(
                [sys.executable, "-I", "-B", "-c", check, directory, revision,
                 manifest["metadata"]["name"], hashlib.sha256(bundles["runtime"]).hexdigest()],
                cwd=directory, env=environment, text=True, capture_output=True, timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(json.loads(result.stdout), {"renders": 4, "dynamic_helpers": 2})

    @staticmethod
    def bundle(revision: str) -> bytes:
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode="w:gz", format=tarfile.PAX_FORMAT, pax_headers={"comment": revision}) as archive:
            entry = tarfile.TarInfo("cval/jobs/bootstrap.sh")
            entry.size = 4
            archive.addfile(entry, io.BytesIO(b"true"))
        return output.getvalue()

    def test_source_only_manifest_matches_validation_transport(self) -> None:
        bundle = self.bundle(COMMIT)
        source = self.source.source_manifest(COMMIT, bundle, "gcr-admin")
        self.assertTrue(source["immutable"])
        self.assertEqual(source["metadata"]["name"], "cval-source-" + COMMIT[:16])
        self.assertEqual(source["metadata"]["namespace"], "gcr-admin")
        self.assertEqual(source["metadata"]["annotations"]["cval/source-commit"], COMMIT)
        digest = hashlib.sha256(bundle).hexdigest()
        self.assertEqual(source["metadata"]["annotations"]["cval/source-sha256"], digest)
        self.assertEqual(base64.b64decode(source["binaryData"]["source.tar.gz"], validate=True), bundle)
        self.assertNotIn("data", source)
        base = load_config()
        config = replace(base, job=replace(base.job, source_configmap=source["metadata"]["name"], source_sha256=digest))
        rendered = render_validation_job(TEMPLATE, "node-a", timestamp=12345, git_ref=COMMIT, cval_config=config)
        pod = yaml.safe_load(rendered.yaml_text)["spec"]["tasks"][0]["template"]["spec"]
        self.assertEqual(pod["volumes"][-1]["configMap"]["items"], [{"key": "source.tar.gz", "path": "source.tar.gz"}])

    def test_rejects_invalid_source_identity_namespace_and_size(self) -> None:
        bundle = self.bundle(COMMIT)
        for revision in ("main", "a" * 7, "0" * 40, "A" * 40):
            with self.subTest(revision=revision), self.assertRaisesRegex(ValueError, "exact nonzero"):
                self.source.source_manifest(revision, bundle, "gcr-admin")
        with self.assertRaisesRegex(ValueError, "commit mismatch"):
            self.source.source_manifest("b" * 40, bundle, "gcr-admin")
        with self.assertRaisesRegex(ValueError, "ConfigMap limit"):
            self.source.source_manifest(COMMIT, b"x" * 900001, "gcr-admin")
        for namespace in ("", "other/namespace", "UPPER", "x" * 64):
            with self.subTest(namespace=namespace), self.assertRaisesRegex(ValueError, "DNS label"):
                self.source.source_manifest(COMMIT, bundle, namespace)

    def test_cli_writes_only_source_and_never_overwrites(self) -> None:
        bundle = self.bundle(COMMIT)
        with TemporaryDirectory() as directory:
            arguments = ["cval-source.py", "--revision", COMMIT, "--namespace", "gcr-admin", "--output-dir", directory]
            responses = [subprocess.CompletedProcess([], 0, COMMIT + "\n", ""), subprocess.CompletedProcess([], 0, bundle, b"")]
            with patch.object(sys, "argv", arguments), patch.object(self.source.subprocess, "run", side_effect=responses) as commands:
                self.source.main()
                self.assertTrue(all(call.kwargs["timeout"] == 30 for call in commands.call_args_list))
                self.assertNotIn("evaluation_engine", commands.call_args.args[0])
            output = Path(directory) / "source-configmap.yaml"
            original = output.read_bytes()
            self.assertEqual([path.name for path in Path(directory).iterdir()], [output.name])
            with patch.object(sys, "argv", arguments), patch.object(self.source.subprocess, "run", side_effect=responses), self.assertRaises(FileExistsError):
                self.source.main()
            self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()