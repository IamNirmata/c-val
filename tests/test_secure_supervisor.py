from __future__ import annotations

import io
import json
import os
import ctypes
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cval.config import encode_config_snapshot, load_config
from cval.storage.write_provenance import authorize_result_write
from cval.validation.registry import load_test_registry
from cval.validation.results import load_validation_result, validation_result_digest, recording_complete
from cval.validation.runtime import build_runtime_environment
from cval.validation.secure_fs import publish_test_summary
from cval.validation.supervisor import (
    _run_child,
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


class SupervisorChildLifecycleTests(unittest.TestCase):
    def _capture(self, command: list[str], timeout: float = 5.0) -> int:
        self.output = io.BytesIO()
        self.errors = io.BytesIO()
        active = [None]
        with tempfile.TemporaryFile() as job_log:
            try:
                return _run_child(
                    command, cwd=REPO_ROOT, environment=dict(os.environ), pass_fds=(),
                    stdout=self.output, stderr=self.errors, job_log_fd=job_log.fileno(),
                    active=active, timeout_seconds=timeout,
                )
            finally:
                self.assertEqual(active, [None])
                job_log.seek(0)
                self.assertEqual(job_log.read(), self.output.getvalue())
                self.assertEqual(self.errors.getvalue(), b"")

    def test_output_and_exit_status_are_preserved(self) -> None:
        for exit_code in (0, 7):
            with self.subTest(exit_code=exit_code):
                code = f"import sys; print('stdout'); print('stderr', file=sys.stderr); sys.exit({exit_code})"
                self.assertEqual(self._capture([sys.executable, "-u", "-c", code]), exit_code)
                self.assertEqual(self.output.getvalue(), b"stdout\nstderr\n")
                self.assertEqual(self.errors.getvalue(), b"")

    def test_timeout_terminates_term_ignoring_child(self) -> None:
        code = "import signal; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); signal.pause()"
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired):
            self._capture([sys.executable, "-u", "-c", code], timeout=0.5)
        self.assertLess(time.monotonic() - started, 5.0)
        self.assertEqual(self.output.getvalue(), b"ready\n")

    def test_direct_ingestion_requires_authorization_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, "-m", "cval.storage.run_ingest"], cwd=directory,
                env={"PYTHONPATH": str(REPO_ROOT), "PYTHONDONTWRITEBYTECODE": "1"},
                text=True, capture_output=True, timeout=15,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("CVAL_RESULT_JSON_FILE is required", result.stderr)
            self.assertEqual(list(Path(directory).iterdir()), [])

    @unittest.skipUnless(sys.platform == "linux", "Linux subreaper required to reap owned descendants")
    def test_descendants_terminate_after_leader_exit_or_timeout(self) -> None:
        library = ctypes.CDLL(None, use_errno=True)
        previous = ctypes.c_int()
        self.assertEqual(library.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(library.prctl(36, 1, 0, 0, 0), 0)
        child = "import os,signal; signal.signal(signal.SIGTERM,signal.SIG_IGN); print(os.getpid(),flush=True); signal.pause()"
        leader = (
            "import signal,subprocess,sys; "
            "child=subprocess.Popen([sys.executable,'-u','-c',sys.argv[1]],stdout=subprocess.PIPE); "
            "print(child.stdout.readline().decode().strip(),flush=True); "
            "signal.pause() if sys.argv[2]=='wait' else None"
        )
        try:
            for mode in ("exit", "wait"):
                descendant = None
                reaped = False
                with self.subTest(mode=mode):
                    started = time.monotonic()
                    try:
                        error = RuntimeError if mode == "exit" else subprocess.TimeoutExpired
                        with self.assertRaises(error):
                            self._capture([sys.executable, "-u", "-c", leader, child, mode], timeout=5.0 if mode == "exit" else 0.5)
                        descendant = int(self.output.getvalue().strip())
                        waited, status = os.waitpid(descendant, os.WNOHANG)
                        reaped = waited == descendant
                        self.assertTrue(reaped, "Owned descendant survived supervisor cleanup")
                        self.assertTrue(os.WIFSIGNALED(status))
                        self.assertEqual(os.WTERMSIG(status), signal.SIGKILL)
                        self.assertLess(time.monotonic() - started, 5.0)
                    finally:
                        if descendant is None and self.output.getvalue().strip():
                            descendant = int(self.output.getvalue().strip())
                        if descendant is not None and not reaped:
                            try:
                                os.kill(descendant, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            os.waitpid(descendant, 0)
        finally:
            self.assertEqual(library.prctl(36, previous.value, 0, 0, 0), 0)


class SecureRunSupervisorTests(unittest.TestCase):
    def test_compact_layout_has_only_single_logs_and_anonymous_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            layout = reserve_secure_run_layout(root, "node-a", "node-a-123", registry_json=REGISTRY)
            try:
                job = root / "logs/job_logs/node-a/node-a-123"
                test = root / "logs/smoke/node-a/node-a-123"
                self.assertEqual({path.name for path in job.iterdir()}, {"job.log", ".run-active"})
                self.assertEqual({path.name for path in test.iterdir()}, {"job.log"})
                self.assertFalse((root / "logs/validation_tests").exists())
                self.assertEqual(len(set(layout.test_fds["smoke"])), 1)
                self.assertEqual(os.fstat(layout.result_file_fd).st_nlink, 0)
                self.assertNotIn("CVAL_RESULT_ENV_FILE", layout.environment())
                layout.verify()
            finally:
                layout.close()

    def test_summary_publisher_uses_retained_fd_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation_root = root / "data"
            stage_dir = root / "stage"
            validation_root.mkdir()
            stage_dir.mkdir(mode=0o700)
            staged = stage_dir / "summary.json"
            staged.write_bytes(b'{"ok":true}\n')
            staged.chmod(0o600)
            layout = reserve_secure_run_layout(
                validation_root,
                "node-a",
                "node-a-123",
                registry_json=REGISTRY,
            )
            try:
                secure_environment = layout.environment()
                run_fd = layout.test_fds["smoke"][1]
                destination = f"/proc/self/fd/{run_fd}/summary.json"
                publish_test_summary(
                    staged,
                    destination,
                    test_id="smoke",
                    summary_name="summary.json",
                    environ=secure_environment,
                )
                summary = (
                    validation_root
                    / "logs/smoke/node-a/node-a-123/summary.json"
                )
                self.assertEqual(summary.read_bytes(), b'{"ok":true}\n')
                self.assertEqual(stat.S_IMODE(summary.stat().st_mode), 0o600)
                with self.assertRaises(FileExistsError):
                    publish_test_summary(
                        staged,
                        destination,
                        test_id="smoke",
                        summary_name="summary.json",
                        environ=secure_environment,
                    )
            finally:
                layout.close()

    def test_summary_publisher_rejects_unlisted_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation_root = root / "data"
            stage_dir = root / "stage"
            validation_root.mkdir()
            stage_dir.mkdir(mode=0o700)
            staged = stage_dir / "summary.json"
            staged.write_bytes(b'{"ok":true}\n')
            staged.chmod(0o600)
            layout = reserve_secure_run_layout(
                validation_root,
                "node-a",
                "node-a-123",
                registry_json=REGISTRY,
            )
            try:
                environment = layout.environment()
                run_fd = layout.test_fds["smoke"][1]
                environment["CVAL_SECURE_RUN_FDS"] = ",".join(
                    value
                    for value in environment["CVAL_SECURE_RUN_FDS"].split(",")
                    if value != str(run_fd)
                )
                with self.assertRaisesRegex(ValueError, "not inherited"):
                    publish_test_summary(
                        staged,
                        f"/proc/self/fd/{run_fd}/summary.json",
                        test_id="smoke",
                        summary_name="summary.json",
                        environ=environment,
                    )
                self.assertFalse(
                    (
                        validation_root
                        / "logs/smoke/node-a/node-a-123/summary.json"
                    ).exists()
                )
            finally:
                layout.close()

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
            "#!/bin/bash\nset -euo pipefail\nprintf '{}\\n' > \"$CVAL_TEST_SUMMARY_FILE\"\n" + workload,
            encoding="utf-8",
        )
        (test_dir / "test_config.toml").write_text(
            f'''schema_version = "cval.test.v1"
[test]
id = "{test_id}"
display_name = "{test_id}"
description = "Supervisor fixture"
order = {order}
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
continue_on_failure = true
[requirements]
cpu = "1"
memory = "1Gi"
gpu_count = 0
rdma_count = 0
shared_memory = "0"
read_sysfs = false
[settings]
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
            job=replace(base.job, template_path=repo / "template.yml", git_ref="a" * 40, image_name="test-image"),
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
                repo_dir=str(repo),
                validation_root=str(root),
                validation_tests_dir=str(repo / "validation-tests"),
                dl_results_root_path=str(
                    root / "logs/dltest"
                ),
            ),
            tests=replace(base.tests, registry=registry),
        )

    def _supervisor_environment(self, config, repo: Path) -> dict[str, str]:
        return os.environ | build_runtime_environment(config) | {
            "PYTHONPATH": str(REPO_ROOT),
            "CVAL_TEST_REPO_ROOT": str(repo),
            "CVAL_VALIDATION_ROOT": config.runtime.validation_root,
            "CVAL_RUN_ID": "node-a-123",
            "CVAL_NODE": "node-a",
            "GCRNODE": "node-a",
            "CVAL_TIMESTAMP": "123",
            "GCRTIME": "123",
            "CVAL_PYTORCH_VERSION": "test-pytorch",
            "CVAL_CUDA_VERSION": "test-cuda",
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
                db_update_command=(sys.executable, "-m", "cval.storage.run_ingest"),
                validation_tests_dir=config.runtime.validation_tests_dir,
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
                json.loads(line.removeprefix("CVAL_EVENT "))
                for line in (run_dir / "job.log").read_text(
                    encoding="utf-8"
                ).splitlines()
                if line.startswith("CVAL_EVENT ")
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
            self.assertFalse((run_dir / ".ingestion-result-digest").exists())
            self.assertTrue(recording_complete(result))
            event_names = [event["event"] for event in events]
            self.assertIn("run_started", event_names)
            self.assertIn("run_finished", event_names)
            self.assertIn("ingestion_started", event_names)
            self.assertIn("ingestion_finished", event_names)
            self.assertFalse((run_dir / ".run-active").exists())
            self.assertFalse((root / "metadata/node-run-history.db").exists())
            self.assertEqual(
                list((root / "logs/validation_tests").glob("*/*_results.db")),
                [],
            )

    def test_nccl_success_writes_raw_status_and_metrics(self) -> None:
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
[[ -z "${DB_PASSWORD+x}" ]]
[[ -z "${DB_SSL_PASSWORD+x}" ]]
[[ -z "${FUTURE_API_KEY+x}" ]]
[[ -z "${SERVICE_PRIVATE_KEY+x}" ]]
cat > "$CVAL_TEST_SUMMARY_FILE" <<'JSON'
{"GCR_ITERATIONS":20,"GCR_DATA_SIZE_GB":8,"GCR_LATENCY":1.25,"GCR_ALGBW":10.0,"GCR_BUSBW":20.0,"GCR_IB_PORT_BW_GBPS":{"mlx5_0":{"avg_gbps":9.0,"max_gbps":11.0,"last_gbps":10.0,"samples":3}}}
JSON
''',
                encoding="utf-8",
            )
            (test_dir / "test_config.toml").write_text(
                '''schema_version = "cval.test.v1"
[test]
id = "nccl"
display_name = "NCCL"
description = "NCCL fixture"
order = 20
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
continue_on_failure = true
[requirements]
cpu = "1"
memory = "1Gi"
gpu_count = 1
rdma_count = 0
shared_memory = "0"
read_sysfs = false
[settings]
gpu_count = 1
iterations = 20
data_size_gb = 8
ibbw_enabled = false
ibbw_start_device = ""
ibbw_end_device = ""
ibbw_interval_seconds = 1
ib_sys_root = "/sys/class/infiniband"
net = "IB"
p2p_disable = true
shm_disable = true
debug = "INFO"
[artifacts]
summary_filename = "summary.json"
''',
                encoding="utf-8",
            )
            config = self._runtime_config(root, repo, ("nccl",))
            config = replace(
                config,
                job=replace(config.job, git_ref="a" * 40, image_name="test-image@sha256:" + "b" * 64),
                job_template=replace(
                    config.job_template,
                    container_image="test-image@sha256:" + "b" * 64,
                ),
            )
            environment = self._supervisor_environment(config, repo)
            environment["CVAL_IMAGE_NAME"] = "test-image@sha256:" + "b" * 64
            environment["CVAL_GIT_REF"] = "a" * 40
            environment["DATABASE_URL"] = "must-not-reach-validation"
            environment["DB_PASSWORD"] = "must-not-reach-validation"
            environment["DB_SSL_PASSWORD"] = "must-not-reach-validation"
            environment["FUTURE_API_KEY"] = "must-not-reach-validation"
            environment["SERVICE_PRIVATE_KEY"] = "must-not-reach-validation"
            stdout = io.BytesIO()
            stderr = io.BytesIO()

            return_code = supervise_validation_run(
                environment=environment,
                runner_command=(sys.executable, "-m", "cval.validation.runner"),
                db_update_command=(sys.executable, "-m", "cval.storage.run_ingest"),
                validation_tests_dir=config.runtime.validation_tests_dir,
                stdout=stdout,
                stderr=stderr,
            )

            self.assertEqual(
                return_code,
                0,
                msg=f"stdout={stdout.getvalue()!r} stderr={stderr.getvalue()!r}",
            )
            with closing(sqlite3.connect(root / "metadata/validation.db")) as connection:
                status_rows = connection.execute(
                    "SELECT test, result FROM runs ORDER BY rowid"
                ).fetchall()
            with closing(sqlite3.connect(root / "metadata/test-nccl.db")) as connection:
                metric_count = connection.execute("SELECT count(*) FROM IB_HEALTH").fetchone()[0]

        self.assertEqual(return_code, 0)
        self.assertEqual(status_rows[1], ("nccl", "pass"))
        self.assertEqual(metric_count, 1)

    def test_nccl_setup_failure_records_raw_status_without_metrics(self) -> None:
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
description = "NCCL fixture"
order = 20
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
continue_on_failure = true
[requirements]
cpu = "1"
memory = "1Gi"
gpu_count = 1
rdma_count = 0
shared_memory = "0"
read_sysfs = false
[settings]
gpu_count = 1
iterations = 20
data_size_gb = 8
ibbw_enabled = false
ibbw_start_device = ""
ibbw_end_device = ""
ibbw_interval_seconds = 1
ib_sys_root = "/sys/class/infiniband"
net = "IB"
p2p_disable = true
shm_disable = true
debug = "INFO"
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
                db_update_command=(sys.executable, "-m", "cval.storage.run_ingest"),
                validation_tests_dir=config.runtime.validation_tests_dir,
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
                    db_update_command=(sys.executable, "-m", "cval.storage.run_ingest"),
                    validation_tests_dir=config.runtime.validation_tests_dir,
                    stdout=io.BytesIO(),
                    stderr=io.BytesIO(),
                )

            self.assertEqual(list(replacement.iterdir()), [])
            self.assertFalse((root / "metadata/validation.db").exists())
            self.assertFalse((replacement / ".ingestion-result-digest").exists())
            retained = root / "logs/job_logs/node-a/node-a-123-retained"
            self.assertFalse((retained / "result.json").exists())
            self.assertTrue((retained / "job.log").is_file())
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
                with self.assertRaises(OSError):
                    os.write(layout.result_file_fd, b"changed")
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
                            f"/proc/self/fd/{layout.result_file_fd}",
                            result_digest=validation_result_digest(result),
                            config_snapshot_b64=encode_config_snapshot(config),
                            config=config,
                        )

                run_dir = root / "logs/job_logs/node-a/node-a-123"
                self.assertFalse((root / "metadata/validation.db").exists())
                self.assertFalse((run_dir / ".ingestion-result-digest").exists())
            finally:
                layout.close()

    def test_reservation_rejects_invalid_registry_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            for registry in ("", "{}", "[]", '{"../escape":{"enabled":true}}', "{invalid"):
                with self.subTest(registry=registry), self.assertRaises(ValueError):
                    reserve_secure_run_layout(root, "node-a", "node-a-123", registry_json=registry)
                self.assertFalse(root.exists())

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
mv "$SWAP_VALIDATION_ROOT/logs/smoke" "$SWAP_VALIDATION_ROOT/logs/smoke-retained"
ln -s "$SWAP_OUTSIDE" "$SWAP_VALIDATION_ROOT/logs/smoke"
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
description = "Ancestor replacement fixture"
order = 10
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = 30
continue_on_failure = true
[requirements]
cpu = "1"
memory = "1Gi"
gpu_count = 0
rdma_count = 0
shared_memory = "0"
read_sysfs = false
[settings]
[artifacts]
summary_filename = "summary.json"
''',
                encoding="utf-8",
            )
            config = self._runtime_config(validation_root, repo, ("smoke",))
            environment = self._supervisor_environment(config, repo) | {
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
                / "logs/smoke-retained/node-a/node-a-123"
            )
            self.assertEqual(
                (retained_run / "artifact.txt").read_text(encoding="utf-8"),
                "anchored\n",
            )
            self.assertFalse((retained_run / "result.json").exists())
            self.assertTrue(
                (
                    validation_root
                    / "logs/job_logs/node-a/node-a-123/.run-active"
                ).is_file()
            )


if __name__ == "__main__":
    unittest.main()
