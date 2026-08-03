from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY = REPO_ROOT / "deploy/cval-evaluator"
BASE = DEPLOY / "base"
ZERO_COMMIT = "0" * 40
REVIEWED_IMAGES = {
    "python:3.12-slim": "57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de",
    "alpine/git": "729da2347ce652f30476b064198577fe12e1147e58499be9f343039343ef2cee",
    "postgres:16-bookworm": "92620daddcd947f8d5ab5ba66e848702fe443d87fed30c4cea8e389fd78dfc55",
}
REVIEWED_REQUIREMENTS = {
    "psycopg==3.3.4": "b6bbc25ccf05c8fad3b061d9db2ef0909a555171b84b07f29458a447253d679a",
    "psycopg-binary==3.3.4": "e7510c37550f91a187e3660a8cc50d4b760f8c3b8b2f89ebc5698cd2c7f2c85d",
    "psycopg-pool==3.3.1": "2af5b432941c4c9ad5c87b3fa410aec910ec8f7c122855897983a06c45f2e4b5",
    "PyYAML==6.0.2": "80bab7bfc629882493af4aa31a4cfa43a4c57c83813253626916b8c7ada83476",
    "typing-extensions==4.15.0": "f0fa19c6845758ab08074a0cfa8b7aecb71c999ca73d62883bc25cc018c4e548",
    "setuptools==80.10.2": "95b30ddfb717250edb492926c92b5221f7ef3fbcc2b07579bcd4a27da21d0173",
}


def load(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class ResidentEvaluatorDeploymentTests(unittest.TestCase):
    def setUp(self) -> None:
        kustomization = load(BASE / "kustomization.yaml")
        self.assertEqual(
            kustomization["resources"],
            [
                "postgres-service.yaml",
                "postgres-statefulset.yaml",
                "postgres-network-policy.yaml",
                "evaluator-deployment.yaml",
            ],
        )
        self.resources = [load(BASE / name) for name in kustomization["resources"]]
        self.by_key = {
            (resource["kind"], resource["metadata"]["name"]): resource
            for resource in self.resources
        }

    def test_no_recurring_cronjob_or_volcano_evaluator_remains(self) -> None:
        self.assertFalse(list(DEPLOY.rglob("*cronjob*.yaml")))
        self.assertFalse((REPO_ROOT / "ymls/gcr-admin-pvc-access-vcjob.yml").exists())
        self.assertFalse(list((DEPLOY / "overlays/ingest").glob("*.yaml")))
        self.assertFalse(list((DEPLOY / "overlays/schema").glob("*.yaml")))
        self.assertEqual(
            {resource["kind"] for resource in self.resources},
            {"Service", "StatefulSet", "NetworkPolicy", "Deployment"},
        )

    def test_base_is_fail_closed_and_secrets_are_references_only(self) -> None:
        self.assertEqual(
            self.by_key[("StatefulSet", "cval-postgres")]["spec"]["replicas"], 0
        )
        evaluator = self.by_key[("Deployment", "cval-evaluator")]
        self.assertEqual(evaluator["spec"]["replicas"], 0)
        self.assertEqual(
            evaluator["metadata"]["annotations"]["cval.io/production-replicas"],
            "1",
        )
        self.assertEqual(evaluator["spec"]["strategy"]["type"], "Recreate")
        self.assertFalse(any(resource["kind"] == "Secret" for resource in self.resources))
        self.assertNotIn("data:", (DEPLOY / "kustomization.yaml").read_text())

    def test_resident_pod_runs_all_recurring_database_work(self) -> None:
        evaluator = self.by_key[("Deployment", "cval-evaluator")]
        pod = evaluator["spec"]["template"]["spec"]
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertEqual(pod["terminationGracePeriodSeconds"], 120)
        self.assertEqual(
            [container["name"] for container in pod["containers"]],
            ["sqlite-evaluator", "nccl-evaluator"],
        )
        sqlite = pod["containers"][0]
        self.assertEqual(sqlite["securityContext"]["runAsUser"], 0)
        self.assertTrue(sqlite["securityContext"]["readOnlyRootFilesystem"])
        self.assertIn("cval.baselines.resident", sqlite["command"])
        self.assertIn("/run/cval/sqlite", sqlite["args"])
        nccl = pod["containers"][1]
        self.assertTrue(nccl["securityContext"]["runAsNonRoot"])
        self.assertEqual(nccl["securityContext"]["runAsUser"], 65532)
        self.assertIn("resident", nccl["args"])
        self.assertEqual(nccl["args"][nccl["args"].index("--confirm") + 1], "resident")
        data_volume = next(volume for volume in pod["volumes"] if volume["name"] == "data")
        self.assertEqual(
            data_volume["persistentVolumeClaim"]["claimName"], "pvc-vast-gcr-admin"
        )
        self.assertFalse(
            next(mount for mount in sqlite["volumeMounts"] if mount["name"] == "data").get(
                "readOnly", False
            )
        )
        self.assertTrue(
            next(mount for mount in nccl["volumeMounts"] if mount["name"] == "data")[
                "readOnly"
            ]
        )

    def test_init_sequence_pulls_exact_commit_and_owns_schema_tasks(self) -> None:
        pod = self.by_key[("Deployment", "cval-evaluator")]["spec"]["template"]["spec"]
        self.assertEqual(
            [container["name"] for container in pod["initContainers"]],
            ["repo-pull", "bootstrap-cval", "wait-postgres", "schema-and-runtime-role"],
        )
        repo_pull = pod["initContainers"][0]
        git_ref = next(item["value"] for item in repo_pull["env"] if item["name"] == "CVAL_GIT_REF")
        self.assertEqual(git_ref, ZERO_COMMIT)
        self.assertIn("rev-parse HEAD", repo_pull["args"][0])
        bootstrap = pod["initContainers"][1]["args"][0]
        self.assertIn("--require-hashes", bootstrap)
        self.assertIn("--no-build-isolation", bootstrap)
        schema = pod["initContainers"][3]
        self.assertIn("nccl-eval schema --apply --confirm schema", schema["args"][0])
        self.assertIn("nccl-eval grant-runtime --apply --confirm grant-runtime", schema["args"][0])
        secret_names = {
            item["valueFrom"]["secretKeyRef"]["name"]
            for container in pod["initContainers"] + pod["containers"]
            for item in container.get("env", [])
            if "valueFrom" in item
        }
        self.assertEqual(secret_names, {"cval-postgres-admin", "cval-postgres-runtime"})

    def test_images_wheels_storage_and_network_policy_are_exact(self) -> None:
        for resource in self.resources:
            if resource["kind"] in {"StatefulSet", "Deployment"}:
                pod = resource["spec"]["template"]["spec"]
            else:
                continue
            for container in pod.get("initContainers", []) + pod.get("containers", []):
                image = container["image"]
                name, digest = image.split("@sha256:", 1)
                self.assertEqual(digest, REVIEWED_IMAGES[name])
                self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        lock = (DEPLOY / "requirements-postgresql.lock").read_text(encoding="utf-8")
        for requirement, digest in REVIEWED_REQUIREMENTS.items():
            self.assertIn(f"{requirement} --hash=sha256:{digest}", lock)
        postgres = self.by_key[("StatefulSet", "cval-postgres")]
        claim = postgres["spec"]["volumeClaimTemplates"][0]
        self.assertEqual(claim["spec"]["accessModes"], ["ReadWriteOnce"])
        self.assertEqual(
            claim["spec"]["storageClassName"],
            "replace-with-reviewed-rwo-storage-class",
        )
        policy = self.by_key[("NetworkPolicy", "cval-postgres-ingress")]
        sources = policy["spec"]["ingress"][0]["from"]
        self.assertEqual(
            sources,
            [{
                "podSelector": {
                    "matchLabels": {
                        "app.kubernetes.io/component": "resident-evaluator"
                    }
                }
            }],
        )

    def test_two_phase_overlays_are_reviewable(self) -> None:
        db = (DEPLOY / "overlays/db/postgres-replicas.yaml").read_text()
        evaluator = (DEPLOY / "overlays/evaluate/evaluator-replicas.yaml").read_text()
        self.assertRegex(db, r"replicas:\s*1")
        self.assertRegex(evaluator, r"replicas:\s*1")
        self.assertEqual(
            load(DEPLOY / "overlays/db/kustomization.yaml")["resources"],
            ["../../base"],
        )
        self.assertEqual(
            load(DEPLOY / "overlays/evaluate/kustomization.yaml")["resources"],
            ["../db"],
        )

    def test_preflight_script_is_dry_run_first_and_gated(self) -> None:
        script = REPO_ROOT / "scripts/cval-nccl-postgres-preflight.sh"
        subprocess.run(["bash", "-n", str(script)], check=True)
        text = script.read_text(encoding="utf-8")
        self.assertIn("--apply --confirm storage-preflight", text)
        self.assertIn("postgresql_storage_supported=UNDETERMINED", text)
        self.assertNotIn("rm -rf", text)

    def test_preflight_disposable_probe_cleans_up(self) -> None:
        script = REPO_ROOT / "scripts/cval-nccl-postgres-preflight.sh"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mount = root / "pvc"
            bin_dir = root / "bin"
            mount.mkdir()
            bin_dir.mkdir()
            (bin_dir / "findmnt").write_text(
                "#!/bin/bash\nprintf '%s\\n' 'ext4 rw,relatime'\n",
                encoding="utf-8",
            )
            (bin_dir / "df").write_text(
                "#!/bin/bash\nprintf '%s\\n' 'Filesystem 1024-blocks Used Available Capacity Mounted on' 'fake 1000000 0 1000000 0% /fake'\n",
                encoding="utf-8",
            )
            for name in ("findmnt", "df"):
                (bin_dir / name).chmod(0o755)
            env = os.environ | {"PATH": f"{bin_dir}:{os.environ['PATH']}"}
            command = [
                "bash", str(script), "--mount-path", str(mount),
                "--pgdata-path", str(mount / "postgresql/pgdata"),
                "--minimum-free-gib", "0",
            ]
            subprocess.run(command, env=env, check=True, stdout=subprocess.PIPE)
            applied = subprocess.run(
                [*command, "--apply", "--confirm", "storage-preflight"],
                env=env, text=True, stdout=subprocess.PIPE, check=True,
            )
            self.assertIn("disposable_write_probe=PASS", applied.stdout)
            self.assertEqual(list(mount.glob(".cval-nccl-postgres-preflight.*")), [])


if __name__ == "__main__":
    unittest.main()
