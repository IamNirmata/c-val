from __future__ import annotations

import io
import json
import os
import sqlite3
import stat
import sys
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cval.config import encode_config_snapshot, load_config
from cval.storage.write_provenance import authorize_result_write
from cval.validation.registry import load_test_registry
from cval.validation.results import load_validation_result, validation_result_digest
from cval.validation.runtime import build_runtime_environment
from cval.validation.supervisor import (
    reserve_secure_run_layout,
    supervise_validation_run,
)


REGISTRY = json.dumps(
    {
        "smoke": {
            "enabled": True,
            "config_path": "validation-tests/smoke/test_config.toml",
            "order": 10,
        }
    },
    sort_keys=True,
    separators=(",", ":"),
)
REPO_ROOT = Path(__file__).resolve().parents[1]


class SecureRunSupervisorTests(unittest.TestCase):
    def _write_test(
        self,
        repo: Path,
        test_id: str,
        *,
        order: int,
        workload: str,
    ) -> None:
        test_dir = repo / "validation-tests" / test_id
        test_dir.mkdir(parents=True)
        (test_dir / "setup.sh").write_text(
            "#!/bin/bash\nset -euo pipefail\n",
            encoding="utf-8",
        )
        (test_dir / "run-test.sh").write_text(
            "#!/bin/bash\nset -euo pipefail\n" + workload,
            encoding="utf-8",
        )
        (test_dir / "test_config.toml").write_text(
            f'''schema_version = "cval.test.v1"
[test]
id = "{test_id}"
display_name = "{test_id}"
order = {order}
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
summary_filename = "summary.json"
''',
            encoding="utf-8",
        )

    def _runtime_config(self, root: Path, repo: Path, test_ids: tuple[str, ...]):
        registrations = {
            test_id: {
                "enabled": True,
                "config_path": f"validation-tests/{test_id}/test_config.toml",
            }
            for test_id in test_ids
        }
        registry = load_test_registry(
            registrations,
            repo_root=repo,
            include_defaults=False,
        )
        base = load_config()
        return replace(
            base,
            storage=replace(
                base.storage,
                validation_db_path=str(root / "metadata/validation.db"),
                storage_db_path=str(root / "metadata/test-storage.db"),
                nccl_db_path=str(root / "metadata/test-nccl.db"),
                dl_numerical_db_path=str(root / "metadata/dl-numerical.db"),
                dl_compute_db_path=str(root / "metadata/dl-compute.db"),
                dl_collective_db_path=str(root / "metadata/dl-collective.db"),
                dl_overlap_db_path=str(root / "metadata/dl-overlap.db"),
            ),
            runtime=replace(
                base.runtime,
                repo_dir=str(REPO_ROOT),
                validation_root=str(root),
                validation_tests_dir=str(repo / "validation-tests"),
                dl_results_root_path=str(
                    root / "validation_tests/dltest/runs"
                ),
            ),
            tests=replace(base.tests, registry=registry),
        )

    def _supervisor_environment(self, config, repo: Path) -> dict[str, str]:
        return os.environ | build_runtime_environment(config) | {
            "CVAL_REPO_DIR": str(REPO_ROOT),
            "CVAL_TEST_REPO_ROOT": str(repo),
            "CVAL_VALIDATION_ROOT": config.runtime.validation_root,
            "CVAL_RUN_ID": "node-a-123",
            "CVAL_NODE": "node-a",
            "GCRNODE": "node-a",
            "CVAL_TIMESTAMP": "123",
            "GCRTIME": "123",
            "CVAL_IMAGE_NAME": "test-image",
            "CVAL_PYTORCH_VERSION": "test-pytorch",
            "CVAL_CUDA_VERSION": "test-cuda",
            "CVAL_GIT_REF": "test-ref",
        }

    def test_real_runner_and_db_update_ingest_descriptor_bound_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            repo = Path(tmpdir) / "test-repo"
            root.mkdir()
            self._write_test(
                repo,
                "smoke-pass",
                order=10,
                workload="printf 'pass workload\\n'\n",
            )
            self._write_test(
                repo,
                "smoke-fail",
                order=20,
                workload="printf 'fail workload\\n'\nexit 7\n",
            )
            config = self._runtime_config(
                root,
                repo,
                ("smoke-pass", "smoke-fail"),
            )
            stdout = io.BytesIO()
            stderr = io.BytesIO()

            return_code = supervise_validation_run(
                environment=self._supervisor_environment(config, repo),
                runner_command=(sys.executable, "-m", "cval.validation.runner"),
                db_update_command=(
                    "/bin/bash",
                    "-c",
                    "source ./0-env.sh && exec /bin/bash ./db-update.sh",
                ),
                validation_tests_dir=REPO_ROOT / "validation-tests",
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(
                return_code,
                0,
                msg=f"stdout={stdout.getvalue()!r} stderr={stderr.getvalue()!r}",
            )
            run_dir = root / "logs/job_logs/node-a/node-a-123"
            result_path = run_dir / "result.json"
            result = load_validation_result(result_path)
            with closing(sqlite3.connect(root / "metadata/validation.db")) as connection:
                rows = connection.execute(
                    "SELECT test, result FROM runs ORDER BY rowid"
                ).fetchall()
            events = [
                json.loads(line)
                for line in (run_dir / "events.jsonl").read_text(
                    encoding="utf-8"
                ).splitlines()
            ]

            self.assertEqual(result.tests["smoke-pass"].status, "pass")
            self.assertEqual(result.tests["smoke-fail"].status, "fail")
            self.assertEqual(result.overall, "fail")
            self.assertEqual(
                rows,
                [
                    ("storage", "incomplete"),
                    ("nccl", "incomplete"),
                    ("dltest", "incomplete"),
                    ("all", "fail"),
                ],
            )
            self.assertEqual(
                (run_dir / ".ingestion-result-digest").read_text(
                    encoding="utf-8"
                ),
                validation_result_digest(result) + "\n",
            )
            event_names = [event["event"] for event in events]
            self.assertIn("run_started", event_names)
            self.assertIn("run_finished", event_names)
            self.assertIn("ingestion_started", event_names)
            self.assertIn("ingestion_finished", event_names)
            self.assertFalse((run_dir / ".run-active").exists())
            self.assertFalse((root / "metadata/node-run-history.db").exists())
            self.assertEqual(
                list((root / "validation_tests").glob("*/*_results.db")),
                [],
            )

    def test_enabled_nccl_emits_outbox_after_local_compatibility_db_commits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            repo = Path(tmpdir) / "test-repo"
            test_dir = repo / "validation-tests/nccl"
            test_dir.mkdir(parents=True)
            root.mkdir()
            (test_dir / "setup.sh").write_text(
                "#!/bin/bash\nset -euo pipefail\n",
                encoding="utf-8",
            )
            (test_dir / "run-test.sh").write_text(
                '''#!/bin/bash
set -euo pipefail
[[ -z "${DATABASE_URL+x}" ]]
[[ -z "${PGPASSWORD+x}" ]]
[[ -z "${PGSSLPASSWORD+x}" ]]
[[ -z "${POSTGRES_PASSWORD_FILE+x}" ]]
[[ -z "${FUTURE_API_KEY+x}" ]]
[[ -z "${SERVICE_PRIVATE_KEY+x}" ]]
cat > "$CVAL_TEST_SUMMARY_FILE" <<'JSON'
{"GCR_ITERATIONS":20,"GCR_DATA_SIZE_GB":8,"GCR_LATENCY":1.25,"GCR_ALGBW":10.0,"GCR_BUSBW":20.0,"GCR_IB_PORT_BW_GBPS":{"mlx5_0":{"avg_gbps":9.0,"max_gbps":11.0,"last_gbps":10.0,"samples":3}}}
JSON
cat > "$NCCL_RUNTIME_EVIDENCE_FILE" <<'JSON'
{"schema_version":"cval.nccl-runtime-evidence.v1","gpu_model":"NVIDIA B200","compiled_nccl_version":"2.27.7","runtime_nccl_package_version":"nvidia-nccl-cu13==2.27.7","driver_version":"600.12","driver_version_group":"600.12","topology_class":"nvidia-topo-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
JSON
chmod 600 "$NCCL_RUNTIME_EVIDENCE_FILE"
''',
                encoding="utf-8",
            )
            (test_dir / "test_config.toml").write_text(
                '''schema_version = "cval.test.v1"
[test]
id = "nccl"
display_name = "NCCL"
order = 20
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[requirements]
gpu_count = 1
[settings]
gpu_count = 1
iterations = 20
data_size_gb = 8
ibbw_enabled = false
net = "IB"
p2p_disable = true
shm_disable = true
debug = "INFO"
evaluation_enabled = true
evaluation_test_name = "nccl-loopback-allreduce"
evaluation_test_definition_version = "nccl-loopback-ar-v1"
evaluation_collective = "all_reduce"
evaluation_datatype = "bfloat16"
evaluation_reduction = "sum"
evaluation_message_size_bytes = 17179869184
evaluation_warmup_iterations = 1
evaluation_samples_per_result = 1
evaluation_iteration_semantics = "timed_collective_repetitions"
evaluation_sample_semantics = "one_aggregate_mean_per_node"
evaluation_latency_unit = "us"
evaluation_latency_source_unit = "ms"
evaluation_latency_conversion = "ms_to_us_x1000"
evaluation_driver_group_source = "runtime_evidence"
evaluation_topology_class_source = "runtime_evidence"
[artifacts]
summary_filename = "summary.json"
''',
                encoding="utf-8",
            )
            config = self._runtime_config(root, repo, ("nccl",))
            config = replace(
                config,
                job=replace(config.job, git_ref="a" * 40),
                job_template=replace(
                    config.job_template,
                    container_image="test-image@sha256:" + "b" * 64,
                ),
            )
            environment = self._supervisor_environment(config, repo)
            environment["CVAL_IMAGE_NAME"] = "test-image@sha256:" + "b" * 64
            environment["CVAL_GIT_REF"] = "a" * 40
            environment["DATABASE_URL"] = "postgresql://must-not-reach-validation/cval"
            environment["PGPASSWORD"] = "must-not-reach-validation"
            environment["PGSSLPASSWORD"] = "must-not-reach-validation"
            environment["POSTGRES_PASSWORD_FILE"] = "/secret/file"
            environment["FUTURE_API_KEY"] = "must-not-reach-validation"
            environment["SERVICE_PRIVATE_KEY"] = "must-not-reach-validation"
            stdout = io.BytesIO()
            stderr = io.BytesIO()

            return_code = supervise_validation_run(
                environment=environment,
                runner_command=(sys.executable, "-m", "cval.validation.runner"),
                db_update_command=(
                    "/bin/bash",
                    "-c",
                    "source ./0-env.sh && exec /bin/bash ./db-update.sh",
                ),
                validation_tests_dir=REPO_ROOT / "validation-tests",
                stdout=stdout,
                stderr=stderr,
            )

            outbox = root / "nccl_eval/outbox/pending/node-a-123.json"
            marker = root / "nccl_eval/outbox/committed/node-a-123.json"
            self.assertEqual(
                return_code,
                0,
                msg=f"stdout={stdout.getvalue()!r} stderr={stderr.getvalue()!r}",
            )
            payload = json.loads(outbox.read_text(encoding="utf-8"))
            self.assertTrue(marker.is_file())
            with closing(sqlite3.connect(root / "metadata/validation.db")) as connection:
                status_rows = connection.execute(
                    "SELECT test, result FROM runs ORDER BY rowid"
                ).fetchall()
            with closing(sqlite3.connect(root / "metadata/test-nccl.db")) as connection:
                metric_count = connection.execute("SELECT count(*) FROM IB_HEALTH").fetchone()[0]

        self.assertEqual(return_code, 0)
        self.assertEqual(status_rows[1], ("nccl", "pass"))
        self.assertEqual(metric_count, 1)
        self.assertEqual(payload["node_results"][0]["latency_us"], 1250.0)
        self.assertNotIn("baseline_approved", payload["node_results"][0])

    def test_nccl_setup_failure_without_runtime_evidence_still_records_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            repo = Path(tmpdir) / "test-repo"
            test_dir = repo / "validation-tests/nccl"
            test_dir.mkdir(parents=True)
            root.mkdir()
            (test_dir / "setup.sh").write_text(
                "#!/bin/bash\nset -euo pipefail\nexit 9\n",
                encoding="utf-8",
            )
            (test_dir / "run-test.sh").write_text(
                "#!/bin/bash\nset -euo pipefail\nexit 99\n",
                encoding="utf-8",
            )
            (test_dir / "test_config.toml").write_text(
                '''schema_version = "cval.test.v1"
[test]
id = "nccl"
display_name = "NCCL"
order = 20
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[requirements]
gpu_count = 1
[settings]
gpu_count = 1
iterations = 20
data_size_gb = 8
ibbw_enabled = false
net = "IB"
p2p_disable = true
shm_disable = true
debug = "INFO"
evaluation_enabled = true
evaluation_test_name = "nccl-loopback-allreduce"
evaluation_test_definition_version = "nccl-loopback-ar-v1"
evaluation_collective = "all_reduce"
evaluation_datatype = "bfloat16"
evaluation_reduction = "sum"
evaluation_message_size_bytes = 17179869184
evaluation_warmup_iterations = 1
evaluation_samples_per_result = 1
evaluation_iteration_semantics = "timed_collective_repetitions"
evaluation_sample_semantics = "one_aggregate_mean_per_node"
evaluation_latency_unit = "us"
evaluation_latency_source_unit = "ms"
evaluation_latency_conversion = "ms_to_us_x1000"
evaluation_driver_group_source = "runtime_evidence"
evaluation_topology_class_source = "runtime_evidence"
[artifacts]
summary_filename = "summary.json"
''',
                encoding="utf-8",
            )
            config = self._runtime_config(root, repo, ("nccl",))
            config = replace(
                config,
                job=replace(config.job, git_ref="a" * 40),
                job_template=replace(
                    config.job_template,
                    container_image="test-image@sha256:" + "b" * 64,
                ),
            )
            stdout = io.BytesIO()
            stderr = io.BytesIO()

            return_code = supervise_validation_run(
                environment=self._supervisor_environment(config, repo),
                runner_command=(sys.executable, "-m", "cval.validation.runner"),
                db_update_command=(
                    "/bin/bash",
                    "-c",
                    "source ./0-env.sh && exec /bin/bash ./db-update.sh",
                ),
                validation_tests_dir=REPO_ROOT / "validation-tests",
                stdout=stdout,
                stderr=stderr,
            )

            with closing(sqlite3.connect(root / "metadata/validation.db")) as connection:
                status = connection.execute(
                    "SELECT result FROM runs WHERE test = 'nccl'"
                ).fetchone()[0]
            self.assertEqual(return_code, 0, stderr.getvalue().decode())
            self.assertEqual(status, "fail")
            self.assertFalse((root / "metadata/test-nccl.db").exists())
            self.assertFalse((root / "nccl_eval/outbox").exists())
            self.assertIn(
                b"failed test produced no runtime evidence",
                stdout.getvalue(),
            )

    def test_canonical_run_replacement_gets_no_db_or_evidence_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            repo = Path(tmpdir) / "test-repo"
            root.mkdir()
            self._write_test(
                repo,
                "smoke",
                order=10,
                workload='''canonical="$SWAP_RUN_DIR"
retained="$SWAP_RETAINED_RUN_DIR"
mv "$canonical" "$retained"
mkdir -m 700 "$canonical"
''',
            )
            config = self._runtime_config(root, repo, ("smoke",))
            environment = self._supervisor_environment(config, repo) | {
                "SWAP_RUN_DIR": str(root / "logs/job_logs/node-a/node-a-123"),
                "SWAP_RETAINED_RUN_DIR": str(
                    root / "logs/job_logs/node-a/node-a-123-retained"
                ),
            }
            replacement = root / "logs/job_logs/node-a/node-a-123"

            with self.assertRaises((OSError, RuntimeError)):
                supervise_validation_run(
                    environment=environment,
                    runner_command=(sys.executable, "-m", "cval.validation.runner"),
                    db_update_command=(
                        "/bin/bash",
                        "-c",
                        "source ./0-env.sh && exec /bin/bash ./db-update.sh",
                    ),
                    validation_tests_dir=REPO_ROOT / "validation-tests",
                    stdout=io.BytesIO(),
                    stderr=io.BytesIO(),
                )

            self.assertEqual(list(replacement.iterdir()), [])
            self.assertFalse((root / "metadata/validation.db").exists())
            self.assertFalse((replacement / ".ingestion-result-digest").exists())
            retained = root / "logs/job_logs/node-a/node-a-123-retained"
            self.assertTrue((retained / "result.json").is_file())
            self.assertTrue((retained / ".run-active").is_file())

    def test_forged_layout_with_unlisted_result_fd_authorizes_no_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "data"
            repo = Path(tmpdir) / "test-repo"
            root.mkdir()
            self._write_test(
                repo,
                "smoke",
                order=10,
                workload="printf 'pass workload\\n'\n",
            )
            config = self._runtime_config(root, repo, ("smoke",))
            layout = reserve_secure_run_layout(
                root,
                "node-a",
                "node-a-123",
                registry_json=build_runtime_environment(config)[
                    "CVAL_TEST_REGISTRY_JSON"
                ],
            )
            try:
                with patch.dict(
                    os.environ,
                    self._supervisor_environment(config, repo)
                    | layout.environment(),
                    clear=True,
                ):
                    from cval.validation.runner import run_validation_tests

                    run_validation_tests(
                        environ=dict(os.environ),
                        stdout=io.StringIO(),
                        stderr=io.StringIO(),
                    )
                layout.bind_result(
                    config_snapshot_b64=encode_config_snapshot(config),
                    config_digest=build_runtime_environment(config)[
                        "CVAL_CONFIG_DIGEST"
                    ],
                    repo_root=repo,
                )
                forged_environment = (
                    self._supervisor_environment(config, repo)
                    | layout.environment()
                )
                forged_environment["CVAL_SECURE_RUN_FDS"] = ",".join(
                    str(descriptor)
                    for descriptor in layout.inherited_fds
                    if descriptor != layout.result_file_fd
                )
                result = load_validation_result(
                    Path(f"/proc/self/fd/{layout.result_file_fd}")
                )

                with patch.dict(os.environ, forged_environment, clear=True):
                    with self.assertRaisesRegex(
                        ValueError,
                        "not inherited run descriptors",
                    ):
                        authorize_result_write(
                            f"/proc/self/fd/{layout.run_dir_fd}/result.json",
                            result_digest=validation_result_digest(result),
                            config_snapshot_b64=encode_config_snapshot(config),
                            config=config,
                        )

                run_dir = root / "logs/job_logs/node-a/node-a-123"
                self.assertFalse((root / "metadata/validation.db").exists())
                self.assertFalse((run_dir / ".ingestion-result-digest").exists())
            finally:
                layout.close()

    def test_reservation_uses_deterministic_owner_modes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            validation_root = Path(tmpdir) / "data"
            validation_root.mkdir()
            layout = reserve_secure_run_layout(
                validation_root,
                "node-a",
                "node-a-123",
                registry_json=REGISTRY,
            )
            try:
                directories = [
                    layout.run_dir_fd,
                    *layout.test_fds["smoke"],
                ]
                for descriptor in directories:
                    value = os.fstat(descriptor)
                    self.assertEqual(value.st_uid, os.geteuid())
                    self.assertEqual(stat.S_IMODE(value.st_mode), 0o700)
                for descriptor in layout.global_file_fds.values():
                    value = os.fstat(descriptor)
                    self.assertEqual(value.st_uid, os.geteuid())
                    self.assertEqual(stat.S_IMODE(value.st_mode), 0o600)
                test_log_dir = validation_root / "logs/smoke/node-a/node-a-123"
                for path in test_log_dir.iterdir():
                    self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            finally:
                layout.close()

    def test_rejects_ancestor_symlink_before_reservation_without_external_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation_root = root / "data"
            outside = root / "outside"
            validation_root.mkdir()
            outside.mkdir()
            (validation_root / "logs").symlink_to(outside, target_is_directory=True)

            with self.assertRaises(OSError):
                reserve_secure_run_layout(
                    validation_root,
                    "node-a",
                    "node-a-123",
                    registry_json=REGISTRY,
                )

            self.assertEqual(list(outside.iterdir()), [])

    def test_ancestor_swap_during_creation_never_redirects_external_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation_root = root / "data"
            outside = root / "outside"
            validation_root.mkdir()
            outside.mkdir()
            swapped = False

            def swap_after_logs(relative: tuple[str, ...], _descriptor: int) -> None:
                nonlocal swapped
                if relative == ("logs",) and not swapped:
                    (validation_root / "logs").rename(validation_root / "logs-retained")
                    (validation_root / "logs").symlink_to(
                        outside,
                        target_is_directory=True,
                    )
                    swapped = True

            with self.assertRaises(OSError):
                reserve_secure_run_layout(
                    validation_root,
                    "node-a",
                    "node-a-123",
                    registry_json=REGISTRY,
                    directory_observer=swap_after_logs,
                )

            self.assertTrue(swapped)
            self.assertEqual(list(outside.iterdir()), [])
            self.assertTrue(
                (
                    validation_root
                    / "logs-retained/job_logs/node-a/node-a-123/.run-active"
                ).is_file()
            )

    def test_runner_ancestor_swap_writes_only_through_retained_descriptors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            repo = root / "repo"
            test_dir = repo / "validation-tests/smoke"
            validation_root = root / "data"
            outside = root / "outside"
            test_dir.mkdir(parents=True)
            validation_root.mkdir()
            outside.mkdir()
            (test_dir / "setup.sh").write_text(
                "#!/bin/bash\nset -euo pipefail\n",
                encoding="utf-8",
            )
            (test_dir / "run-test.sh").write_text(
                """#!/bin/bash
set -euo pipefail
mv "$SWAP_VALIDATION_ROOT/validation_tests" "$SWAP_VALIDATION_ROOT/validation_tests-retained"
ln -s "$SWAP_OUTSIDE" "$SWAP_VALIDATION_ROOT/validation_tests"
printf 'anchored\n' > "$CVAL_TEST_OUTPUT_DIR/artifact.txt"
printf '{"ok":true}\n' > "$CVAL_TEST_SUMMARY_FILE"
""",
                encoding="utf-8",
            )
            (test_dir / "test_config.toml").write_text(
                '''schema_version = "cval.test.v1"
[test]
id = "smoke"
display_name = "Smoke"
order = 10
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
[artifacts]
summary_filename = "summary.json"
''',
                encoding="utf-8",
            )
            environment = os.environ | {
                "CVAL_REPO_DIR": str(Path(__file__).resolve().parents[1]),
                "CVAL_TEST_REPO_ROOT": str(repo),
                "CVAL_VALIDATION_TESTS_DIR": str(repo / "validation-tests"),
                "CVAL_VALIDATION_ROOT": str(validation_root),
                "CVAL_TEST_REGISTRY_JSON": REGISTRY,
                "CVAL_RUN_ID": "node-a-123",
                "CVAL_NODE": "node-a",
                "GCRNODE": "node-a",
                "CVAL_TIMESTAMP": "123",
                "GCRTIME": "123",
                "CVAL_IMAGE_NAME": "test-image",
                "CVAL_PYTORCH_VERSION": "test",
                "CVAL_CUDA_VERSION": "test",
                "CVAL_GIT_REF": "test-ref",
                "SWAP_VALIDATION_ROOT": str(validation_root),
                "SWAP_OUTSIDE": str(outside),
            }

            captured_stdout = io.BytesIO()
            captured_stderr = io.BytesIO()
            try:
                return_code = supervise_validation_run(
                    environment=environment,
                    runner_command=(sys.executable, "-m", "cval.validation.runner"),
                    db_update_command=None,
                    validation_tests_dir=repo / "validation-tests",
                    stdout=captured_stdout,
                    stderr=captured_stderr,
                )
            except OSError:
                pass
            else:
                self.fail(
                    "ancestor replacement was not detected; "
                    f"return_code={return_code}; stdout={captured_stdout.getvalue()!r}; "
                    f"stderr={captured_stderr.getvalue()!r}"
                )

            self.assertEqual(list(outside.iterdir()), [])
            retained_run = (
                validation_root
                / "validation_tests-retained/smoke/runs/node-a/node-a-123"
            )
            self.assertEqual(
                (retained_run / "artifacts/artifact.txt").read_text(encoding="utf-8"),
                "anchored\n",
            )
            result = json.loads((retained_run / "result.json").read_text(encoding="utf-8"))
            self.assertEqual(
                result["artifacts"],
                str(
                    validation_root
                    / "validation_tests/smoke/runs/node-a/node-a-123/artifacts"
                ),
            )
            self.assertTrue(
                (
                    validation_root
                    / "logs/job_logs/node-a/node-a-123/.run-active"
                ).is_file()
            )


if __name__ == "__main__":
    unittest.main()
