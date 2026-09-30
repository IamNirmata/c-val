from __future__ import annotations

import io
import json
import os
import signal
import shutil
import sqlite3
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from dataclasses import replace
from pathlib import Path

from cval.config import load_config
from cval.validation.builtins import BUILTIN_TEST_EVIDENCE_ENV
from cval.validation.registry import load_test_registry
from cval.validation.results import ValidationResultV2, load_validation_result, parse_validation_result_v2
from cval.validation.runner import _load_secure_layout, _secure_test_paths, _test_environment, run_validation_tests
from cval.validation.runtime import build_runtime_environment
from cval.validation.supervisor import _add_builtin_compatibility_paths, reserve_secure_run_layout


class CompactWorkflowTests(unittest.TestCase):
    def _suite(self, root: Path, selected: str):
        source = Path(__file__).resolve().parents[1]
        repo = root / "source"
        shutil.copytree(source / "validation-tests", repo / "validation-tests")
        registry = load_test_registry({name: {"enabled": name == selected, "config_path": f"validation-tests/{name}/test_config.toml"}
                                       for name in ("storage", "nccl", "dltest")}, repo_root=repo)
        data = root / "data"
        data.mkdir()
        base = load_config()
        return replace(base, job=replace(base.job, template_path=repo / "template.yml"), tests=replace(base.tests, registry=registry),
                       storage=replace(base.storage, **{key: str(data / "metadata" / Path(value).name) for key, value in vars(base.storage).items()}),
                       runtime=replace(base.runtime, repo_dir=str(repo), validation_tests_dir=str(repo / "validation-tests"),
                                       validation_root=str(data), dl_results_root_path=str(data / "logs/dltest"), dl_unit_test_dir=str(root / "dl-source")))

    def _execute(self, root: Path, config, *, ingestion=None):
        from cval.validation.supervisor import supervise_validation_run

        source = Path(__file__).resolve().parents[1]
        environment = dict(os.environ) | build_runtime_environment(config) | {
            "PATH": str(root) + os.pathsep + os.environ["PATH"], "PYTHONPATH": str(source),
            "GCRNODE": "node-a", "GCRTIME": "123", "CVAL_RUN_ID": "node-a-123",
            "CVAL_PYTORCH_VERSION": "test", "CVAL_CUDA_VERSION": "test",
            "FIXTURE_RANK_DIR": str(root / "ranks"),
        }
        output = io.BytesIO()
        code = supervise_validation_run(
            environment=environment, runner_command=(sys.executable, "-m", "cval.validation.runner"),
            db_update_command=ingestion or (sys.executable, "-m", "cval.storage.run_ingest"),
            validation_tests_dir=config.runtime.validation_tests_dir, stdout=output, stderr=io.BytesIO(),
        )
        return code, output.getvalue().decode()

    def _verify(self, config):
        from cval.storage.verification import _MEMORY_READER, collect_exact_evidence, evidence_request
        from cval.validation.runtime import effective_config_digest

        intent = {"node_name": "node-a", "timestamp": 123, "run_id": "node-a-123", "git_ref": config.job.git_ref,
                  "config_digest": effective_config_digest(config)}

        def execute_reader(_arguments, *, input_text, timeout, check):
            return subprocess.run([sys.executable, "-I", "-B", "-c", _MEMORY_READER], input=input_text,
                                  timeout=timeout, check=check, text=True, capture_output=True)

        return collect_exact_evidence(evidence_request(intent, config, 10), config, SimpleNamespace(run=execute_reader))

    def test_storage_parser_recognizes_only_the_declared_summary(self) -> None:
        from cval.storage.ingest import STORAGE_FILE_PREFIXES, parse_storage_metrics

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fio = {"jobs": [{"read": {"iops": 1, "bw": 2}, "write": {"iops": 0, "bw": 0}}]}
            for filename in STORAGE_FILE_PREFIXES:
                (root / filename).write_text(json.dumps(fio))
            metrics = parse_storage_metrics(root)
            summary = root / "storage-summary.json"
            summary.write_text(json.dumps({"schema_version": "cval.storage.summary.v1", "metrics": metrics}))
            self.assertEqual(parse_storage_metrics(root, summary_filename=summary.name), metrics)
            with self.assertRaisesRegex(ValueError, "unexpected JSON"):
                parse_storage_metrics(root)
            summary.write_text(json.dumps({"schema_version": "cval.storage.summary.v1", "metrics": {}}))
            with self.assertRaisesRegex(ValueError, "does not match"):
                parse_storage_metrics(root, summary_filename=summary.name)

    def test_nccl_pipeline_retains_single_log_summary_and_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self._suite(root, "nccl")
            directory = Path(config.runtime.validation_tests_dir) / "nccl"
            (directory / "ibbw.sh").write_text("#!/bin/bash\nprintf 'mlx5_0: 12 GB/s\\n'\n")
            launcher = root / "torchrun"
            launcher.write_text(
                f"#!{sys.executable}\nimport json, os, pathlib, sys\n"
                "assert not pathlib.Path(os.environ['CVAL_CANONICAL_RESULT_JSON_FILE']).exists()\n"
                "output = pathlib.Path(sys.argv[sys.argv.index('--result-file')+1])\n"
                "output.write_text(json.dumps({'GCR_ITERATIONS':20,'GCR_DATA_SIZE_GB':8,'GCR_LATENCY':1.0,'GCR_ALGBW':10.0,'GCR_BUSBW':20.0}))\n"
                "print('fixture allreduce complete')\n"
            )
            launcher.chmod(0o755)
            code, output = self._execute(root, config)
            self.assertEqual(code, 0, output)
            verified = self._verify(config)
            self.assertTrue(verified["complete"], verified)
            self.assertTrue(verified["tests_passed"], verified)
            directory = Path(config.runtime.validation_root) / "logs/nccl/node-a/node-a-123"
            self.assertEqual({path.name for path in directory.iterdir()}, {"job.log", "summary.json", "ibbw-node-a-123.log"})
            self.assertEqual(verified["sources"]["nccl"]["hca_ports_compared"], 1)

    def test_dl_pipeline_keeps_rank_json_and_four_store_receipts(self) -> None:
        from tests.test_dltest_ingest import _write_rank_json

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self._suite(root, "dltest")
            for rank in range(8):
                _write_rank_json(root / "ranks" / f"fixture_RANK{rank}.json", rank)
            package = Path(config.runtime.dl_unit_test_dir) / "src/dl_unit_test"
            package.mkdir(parents=True)
            (package / "__init__.py").touch()
            (package / "__main__.py").write_text(
                "import os, pathlib, shutil\n"
                "assert not pathlib.Path(os.environ['CVAL_CANONICAL_RESULT_JSON_FILE']).exists()\n"
                "output=pathlib.Path('test_plans/80gb-example/runs'); output.mkdir(parents=True)\n"
                "for source in pathlib.Path(os.environ['FIXTURE_RANK_DIR']).glob('*.json'): shutil.copyfile(source,output/source.name)\n"
                "print('fixture DL complete')\n"
            )
            plan = package / "test_plans/80gb-example"
            plan.mkdir(parents=True)
            (plan / "test_plan.json").write_text("{}\n")
            launcher = root / "torchrun"
            launcher.write_text(f"#!{sys.executable}\nimport runpy\nrunpy.run_module('dl_unit_test',run_name='__main__')\n")
            launcher.chmod(0o755)
            code, output = self._execute(root, config)
            self.assertEqual(code, 0, output)
            verified = self._verify(config)
            self.assertTrue(verified["complete"], verified)
            self.assertTrue(verified["tests_passed"], verified)
            directory = Path(config.runtime.validation_root) / "logs/dltest/node-a/node-a-123"
            self.assertEqual({path.name for path in directory.iterdir()}, {"job.log", "summary.json", "workdir"})
            self.assertEqual(len(list(directory.glob("workdir/test_plans/*/runs/*.json"))), 8)
            for component in verified["sources"]["dltest"]["components"].values():
                self.assertEqual(component["ranks"], 8)
                self.assertEqual(component["receipts"], 1)

    def test_failed_ingestion_is_finalized_without_a_success_claim(self) -> None:
        from cval.validation.results import recording_complete

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self._suite(root, "storage")
            directory = Path(config.runtime.validation_tests_dir) / "storage"
            (directory / "setup.sh").write_text("#!/bin/bash\nexit 0\n")
            (directory / "run-test.sh").write_text("#!/bin/bash\nprintf '{}\\n' > \"$CVAL_TEST_SUMMARY_FILE\"\n")
            code, output = self._execute(root, config, ingestion=(sys.executable, "-c", "raise SystemExit(9)"))
            self.assertEqual(code, 9, output)
            directory = Path(config.runtime.validation_root) / "logs/job_logs/node-a/node-a-123"
            result = load_validation_result(directory / "result.json")
            self.assertEqual(result.overall, "pass")
            self.assertFalse(recording_complete(result))
            self.assertEqual(result.completion["ingestion"], "unconfirmed")
            self.assertEqual(result.completion["exit_code"], 9)
            self.assertFalse((Path(config.runtime.validation_root) / "metadata").exists())

    def test_invalid_ingestion_acknowledgement_cannot_confirm_completion(self) -> None:
        from cval.validation.results import recording_complete

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = self._suite(root, "storage")
            directory = Path(config.runtime.validation_tests_dir) / "storage"
            (directory / "setup.sh").write_text("#!/bin/bash\nexit 0\n")
            (directory / "run-test.sh").write_text("#!/bin/bash\nprintf '{}\\n' > \"$CVAL_TEST_SUMMARY_FILE\"\n")
            script = (
                "import json,os; from pathlib import Path; "
                "from cval.validation.results import load_validation_result,validation_result_digest; "
                "result=load_validation_result(Path(os.environ['CVAL_RESULT_JSON_FILE'])); "
                "print('CVAL_EVENT '+json.dumps({'schema_version':'cval.event.v1','event':'ingestion_finished',"
                "'run_id':result.run_id,'node_name':result.node,'producer':'ingestion','status':'pass',"
                "'result_digest':validation_result_digest(result),'config_digest':result.global_config_digest,'stores':{}}))"
            )
            with self.assertRaisesRegex(ValueError, "Incomplete ingestion acknowledgement"):
                self._execute(root, config, ingestion=(sys.executable, "-c", script))
            job = Path(config.runtime.validation_root) / "logs/job_logs/node-a/node-a-123"
            result = load_validation_result(job / "result.json")
            self.assertFalse(recording_complete(result))
            self.assertEqual(result.completion["exit_code"], 1)
            self.assertTrue((job / ".run-active").exists())
            self.assertFalse((Path(config.runtime.validation_root) / "metadata").exists())

    def test_storage_pipeline_publishes_only_final_result_and_single_logs(self) -> None:
        from cval.storage.status import read_local_coverage
        from cval.storage.verification import evidence_request, verify_local_evidence
        from cval.validation.results import recording_complete
        from cval.validation.runtime import effective_config_digest
        from cval.validation.supervisor import supervise_validation_run

        source = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "fio"
            binary.write_text(
                f"#!{sys.executable}\n"
                "import json, os, pathlib, sys\n"
                "assert not pathlib.Path(os.environ['CVAL_CANONICAL_RESULT_JSON_FILE']).exists()\n"
                "output = next(value.split('=', 1)[1] for value in sys.argv if value.startswith('--output='))\n"
                "pathlib.Path(output).write_text(json.dumps({'jobs':[{'read':{'iops':100,'bw':1048576},'write':{'iops':0,'bw':0}}]}))\n"
                "print('retained FIO output')\n"
            )
            binary.chmod(0o755)
            data = root / "data"
            data.mkdir()
            base = load_config()
            registry = replace(base.tests.registry, tests=tuple(replace(test, enabled=test.id == "storage") for test in base.tests.registry.tests))
            config = replace(base, tests=replace(base.tests, registry=registry),
                             storage=replace(base.storage, **{key: str(data / "metadata" / Path(value).name) for key, value in vars(base.storage).items()}),
                             runtime=replace(base.runtime, repo_dir=str(source), validation_tests_dir=str(source / "validation-tests"),
                                             validation_root=str(data), dl_results_root_path=str(data / "logs/dltest")))
            environment = dict(os.environ) | build_runtime_environment(config) | {
                "PATH": str(root) + os.pathsep + os.environ["PATH"], "PYTHONPATH": str(source),
                "GCRNODE": "node-a", "GCRTIME": "123", "CVAL_RUN_ID": "node-a-123",
                "CVAL_PYTORCH_VERSION": "test", "CVAL_CUDA_VERSION": "test",
            }
            output = io.BytesIO()
            result_code = supervise_validation_run(
                environment=environment, runner_command=(sys.executable, "-m", "cval.validation.runner"),
                db_update_command=(sys.executable, "-m", "cval.storage.run_ingest"),
                validation_tests_dir=config.runtime.validation_tests_dir, stdout=output, stderr=io.BytesIO(),
            )
            self.assertEqual(result_code, 0, output.getvalue().decode())
            job = data / "logs/job_logs/node-a/node-a-123"
            test_dir = data / "logs/storage/node-a/node-a-123"
            result = load_validation_result(job / "result.json")
            self.assertEqual(result.overall, "pass", output.getvalue().decode())
            self.assertTrue(recording_complete(result))
            self.assertEqual({path.name for path in job.iterdir()}, {"job.log", "result.json"})
            self.assertEqual(len(list(test_dir.glob("*.json"))), 7)
            self.assertEqual(len(list(test_dir.glob("*.log"))), 1)
            self.assertFalse((test_dir / "result.json").exists())
            self.assertFalse((data / "logs/validation_tests").exists())
            self.assertGreaterEqual((job / "result.json").stat().st_mtime_ns, (job / "job.log").stat().st_mtime_ns)
            intent = {"node_name": "node-a", "timestamp": 123, "run_id": "node-a-123", "git_ref": config.job.git_ref, "config_digest": effective_config_digest(config)}
            request = evidence_request(intent, config, 10)
            verified = verify_local_evidence(request)
            self.assertTrue(verified["complete"], verified)
            self.assertTrue(verified["tests_passed"], verified)
            coverage = read_local_coverage({"nodes": ["node-a"], "tests": ["storage"], "root": str(data), "db_path": config.storage.validation_db_path,
                                           "max_runs": 10, "timeout_seconds": 10, "sqlite_timeout_seconds": 5, "max_bytes": 10000000, "max_files": 100, "include_recording": True})
            self.assertEqual(coverage["timestamps"], {"node-a": 123}, coverage)
            self.assertEqual(coverage["nodes"][0]["recording_state"], "fully_recorded")
            (job / "result.json").unlink()
            coverage = read_local_coverage({"nodes": ["node-a"], "tests": ["storage"], "root": str(data), "db_path": config.storage.validation_db_path,
                                           "max_runs": 10, "timeout_seconds": 10, "sqlite_timeout_seconds": 5, "max_bytes": 10000000, "max_files": 100})
            self.assertEqual(coverage["timestamps"], {}, coverage)
            self.assertEqual(coverage["nodes"][0]["skipped"][0]["reason"], "missing_completion_provenance")


class RunnerReservationTests(unittest.TestCase):
    def test_test_owned_log_hooks_ignore_unrelated_logger_modules(self) -> None:
        from cval.validation.plugins import load_registered_plugin

        with patch.dict(sys.modules, {"logger": SimpleNamespace(write_log=lambda *_args: None)}):
            config = load_config()
            for test in config.tests.registry.enabled:
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / "job.log"
                    path.touch()
                    load_registered_plugin(test).write_log(path, test.id + "\n")
                    self.assertEqual(path.read_text(), test.id + "\n")

    def _inputs(self, root: Path):
        config = load_config()
        config = replace(config, runtime=replace(config.runtime, validation_root=str(root)))
        node = socket.gethostname()
        timestamp = int(time.time())
        environment = build_runtime_environment(config)
        environment.update(GCRNODE=node, GCRTIME=str(timestamp), CVAL_RUN_ID=f"{node}-{timestamp}")
        return config, environment, root / "logs/job_logs" / node / environment["CVAL_RUN_ID"]

    def test_closed_console_preserves_evidence_and_descriptor_ownership(self) -> None:
        for borrowed in (False, True):
            with self.subTest(supervisor_owned=borrowed), tempfile.TemporaryDirectory() as directory:
                config, environment, run_dir = self._inputs(Path(directory))
                before = set(os.listdir("/proc/self/fd"))
                layout = None
                if borrowed:
                    layout = reserve_secure_run_layout(
                        directory, environment["GCRNODE"], environment["CVAL_RUN_ID"],
                        registry_json=environment["CVAL_TEST_REGISTRY_JSON"],
                    )
                    environment.update(layout.environment())
                reserved = set(os.listdir("/proc/self/fd"))
                console = tempfile.TemporaryFile(mode="w+")
                console.close()
                try:
                    with self.assertRaisesRegex(ValueError, "closed file"):
                        run_validation_tests(config=config, environ=environment, stdout=console, stderr=io.StringIO())
                    self.assertEqual(set(os.listdir("/proc/self/fd")), reserved)
                    self.assertFalse((run_dir / "result.json").exists())
                    self.assertTrue((run_dir / ".run-active").is_file())
                    self.assertFalse((Path(directory) / "metadata").exists())
                    if layout is not None:
                        layout.verify()
                        self.assertEqual((run_dir / "job.log").read_bytes(), b"")
                    else:
                        self.assertIn(b"run_started", (run_dir / "job.log").read_bytes())
                    self.assertFalse((run_dir / "events.jsonl").exists())
                finally:
                    if layout is not None:
                        layout.close()
                self.assertEqual(set(os.listdir("/proc/self/fd")), before)

    def test_reused_or_marker_only_evidence_is_not_adopted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config, environment, run_dir = self._inputs(Path(directory))
            layout = reserve_secure_run_layout(
                directory, environment["GCRNODE"], environment["CVAL_RUN_ID"],
                registry_json=environment["CVAL_TEST_REGISTRY_JSON"],
            )
            layout.close()
            original = {path.name: path.read_bytes() for path in run_dir.iterdir()}
            before = set(os.listdir("/proc/self/fd"))
            with self.assertRaisesRegex(FileExistsError, "refusing run_id reuse"):
                run_validation_tests(config=config, environ=environment)
            environment.update(CVAL_EXTERNAL_GLOBAL_LOGGING="true", CVAL_RUN_MARKER_PREACQUIRED="true")
            with self.assertRaisesRegex(ValueError, "verified secure run layout"):
                run_validation_tests(config=config, environ=environment)
            self.assertEqual({path.name: path.read_bytes() for path in run_dir.iterdir()}, original)
            self.assertEqual(set(os.listdir("/proc/self/fd")), before)

    def test_symlinked_test_directory_cannot_redirect_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            config, environment, run_dir = self._inputs(root)
            test_id = config.tests.registry.enabled[0].id
            (root / "logs").mkdir()
            (root / "logs" / test_id).symlink_to(outside, target_is_directory=True)
            before = set(os.listdir("/proc/self/fd"))
            with self.assertRaises(OSError):
                run_validation_tests(config=config, environ=environment)
            self.assertEqual(list(Path(outside).iterdir()), [])
            self.assertTrue((run_dir / ".run-active").is_file())
            self.assertFalse((run_dir / "result.json").exists())
            self.assertEqual(set(os.listdir("/proc/self/fd")), before)


    def test_projection_matches_reserved_paths_and_rejects_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, environment, _run_dir = self._inputs(root)
            layout = reserve_secure_run_layout(
                root, environment["GCRNODE"], environment["CVAL_RUN_ID"],
                registry_json=environment["CVAL_TEST_REGISTRY_JSON"],
            )
            try:
                _add_builtin_compatibility_paths(environment, layout, config)
                environment.update(layout.environment())
                secure = _load_secure_layout(environment, root, config.tests.registry)
                for test in config.tests.registry.enabled:
                    paths = _secure_test_paths(secure, test)
                    arguments = dict(registered_test=test, test_paths=paths, validation_root=root,
                                     node=layout.node, timestamp=int(environment["GCRTIME"]), run_id=layout.run_id)
                    child = _test_environment(environment, **arguments)
                    aliases = BUILTIN_TEST_EVIDENCE_ENV[test.id]
                    self.assertEqual(child["CVAL_TEST_SUMMARY_FILE"], child[aliases["summary"]])
                    for name in aliases.values():
                        self.assertEqual(child[name], environment[name])
                        changed = environment | {name: str(root / "conflicting-path")}
                        before = dict(changed)
                        with self.assertRaisesRegex(ValueError, "reserved test evidence"):
                            _test_environment(changed, **arguments)
                        with self.assertRaisesRegex(ValueError, "reserved run evidence"):
                            _add_builtin_compatibility_paths(changed, layout, config)
                        self.assertEqual(changed, before)
                canonical = environment | {"NCCL_SUMMARY_FILE": environment["CVAL_CANONICAL_NCCL_SUMMARY_FILE"]}
                _add_builtin_compatibility_paths(canonical, layout, config)
                self.assertEqual(canonical, environment)
                with self.assertRaisesRegex(ValueError, "reserved run evidence"):
                    _add_builtin_compatibility_paths(environment | {"CVAL_RESULT_JSON_FILE": str(root / "wrong")}, layout, config)
                script = Path(__file__).resolve().parents[1] / "validation-tests/0-env.sh"
                result = subprocess.run(
                    ["bash", "-c", 'source "$1" && printf "%s\\n" "$NCCL_SUMMARY_FILE"', "check", str(script)],
                    env=dict(os.environ) | environment, pass_fds=layout.inherited_fds,
                    text=True, capture_output=True, check=True, timeout=30,
                )
                self.assertEqual(result.stdout.strip(), environment["NCCL_SUMMARY_FILE"])
                missing = dict(os.environ) | environment
                del missing["NCCL_SUMMARY_FILE"]
                result = subprocess.run(["bash", "-c", 'source "$1"', "check", str(script)],
                                        env=missing, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 2)
                self.assertIn("Missing projected evidence path", result.stderr)
                layout.verify()
            finally:
                layout.close()

    def test_conflicting_identity_aliases_fail_before_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, environment, _run_dir = self._inputs(root)
            for name, value in (("CVAL_NODE", environment["GCRNODE"] + "-other"),
                                ("CVAL_TIMESTAMP", str(int(environment["GCRTIME"]) + 1))):
                with self.assertRaisesRegex(ValueError, "node/timestamp aliases disagree"):
                    run_validation_tests(config=config, environ=environment | {name: value})
                self.assertEqual(list(root.iterdir()), [])


class ValidationRunnerTests(unittest.TestCase):
    def test_runs_dynamic_tests_in_order_and_preserves_canonical_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "first", order=10, exit_code=0)
            self._write_test(root, "failing", order=20, exit_code=7)
            self._write_test(root, "after", order=30, exit_code=0)
            self._write_test(root, "disabled", order=40, exit_code=0)
            registry = self._registry(
                root,
                {
                    "first": True,
                    "failing": True,
                    "after": True,
                    "disabled": False,
                },
            )
            validation_root = root / "data"
            stdout = io.StringIO()
            stderr = io.StringIO()

            result = run_validation_tests(
                config=self._config(root, registry),
                registry=registry,
                environ=self._environment(root, validation_root),
                stdout=stdout,
                stderr=stderr,
            )

            result_path = (
                validation_root
                / "logs"
                / "job_logs"
                / "node-a"
                / "node-a-123"
                / "result.json"
            )
            parsed = parse_validation_result_v2(result)
            events = self._read_events(result_path.parent / "job.log")
            self.assertFalse(result_path.exists())
            self.assertFalse((result_path.parent / "result.env").exists())

            first_run = (
                validation_root
                / "logs"
                / "first"
                / "node-a"
                / "node-a-123"
            )
            disabled_log = (
                validation_root
                / "logs"
                / "disabled"
                / "node-a"
                / "node-a-123"
            )

            self.assertIsInstance(parsed, ValidationResultV2)
            self.assertEqual(result["overall"], "fail")
            self.assertEqual(result["tests"]["first"]["status"], "pass")
            self.assertEqual(result["tests"]["failing"]["status"], "fail")
            self.assertEqual(result["tests"]["after"]["status"], "pass")
            self.assertEqual(result["tests"]["disabled"]["phase"], "not_selected")
            self.assertNotIn("run-disabled", stdout.getvalue())
            self.assertEqual(
                [event["test"] for event in events if event["event"] == "test_started"],
                ["first", "failing", "after"],
            )
            self.assertEqual(events[0]["event"], "run_started")
            self.assertEqual(events[-1]["event"], "run_finished")
            self.assertFalse((first_run / "result.json").exists())
            self.assertTrue((first_run / "summary.json").is_file())
            self.assertTrue((first_run / "artifact.txt").is_file())
            self.assertFalse(disabled_log.exists())
            self.assertIn("run-first", stdout.getvalue())
            self.assertIn("run-after", stdout.getvalue())
            self.assertIn("err-failing", stderr.getvalue())
            self.assertIn("CVAL_EVENT", stdout.getvalue())
            self.assertFalse((result_path.parent / "stdout.log").exists())
            self.assertFalse((result_path.parent / "stderr.log").exists())
            job_log = (result_path.parent / "job.log").read_text(encoding="utf-8")
            self.assertIn("run-first", job_log)
            self.assertIn("err-failing", job_log)
            self.assertEqual(list(result_path.parent.glob(".*.tmp-*")), [])

    def test_setup_failure_does_not_run_workload_or_stop_later_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "broken-setup", order=10, setup_exit=4)
            self._write_test(root, "later", order=20)
            registry = self._registry(
                root,
                {"broken-setup": True, "later": True},
            )
            validation_root = root / "data"

            result = run_validation_tests(
                config=self._config(root, registry),
                registry=registry,
                environ=self._environment(root, validation_root),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(result["tests"]["broken-setup"]["phase"], "setup_failed")
        self.assertEqual(result["tests"]["broken-setup"]["exit_code"], 4)
        self.assertEqual(result["tests"]["later"]["status"], "pass")
        self.assertEqual(result["overall"], "fail")

    def test_all_disabled_registry_completes_without_executing_scripts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "disabled", order=10)
            registry = self._registry(root, {"disabled": False})
            validation_root = root / "data"

            result = run_validation_tests(
                config=self._config(root, registry),
                registry=registry,
                environ=self._environment(root, validation_root),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(result["overall"], "incomplete")
        self.assertIsNotNone(result["completed_at"])
        self.assertEqual(result["tests"]["disabled"]["phase"], "not_selected")

    def test_timeout_terminates_test_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "slow", order=10, timeout_seconds=1)
            (root / "validation-tests" / "slow" / "run-test.sh").write_text(
                "#!/bin/bash\nwhile :; do :; done\n",
                encoding="utf-8",
            )
            self._write_test(root, "later", order=20)
            registry = self._registry(root, {"slow": True, "later": True})

            result = run_validation_tests(
                config=self._config(root, registry),
                registry=registry,
                environ=self._environment(root, root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(result["tests"]["slow"]["phase"], "timed_out")
        self.assertEqual(result["tests"]["slow"]["status"], "fail")
        self.assertEqual(result["tests"]["later"]["status"], "pass")

    def test_timeout_kills_term_ignoring_background_descendant(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "daemon", order=10, timeout_seconds=1)
            (root / "validation-tests" / "daemon" / "run-test.sh").write_text(
                """#!/bin/bash
(trap '' TERM; while :; do :; done) &
exit 0
""",
                encoding="utf-8",
            )
            registry = self._registry(root, {"daemon": True})
            started = time.monotonic()

            result = run_validation_tests(
                config=self._config(root, registry),
                registry=registry,
                environ=self._environment(root, root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            elapsed = time.monotonic() - started

        self.assertEqual(result["tests"]["daemon"]["phase"], "timed_out")
        self.assertLess(elapsed, 4.0)

    def test_runtime_registry_executes_synthetic_fourth_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=999)
            environment = self._environment(root, root / "data")
            environment.update(build_runtime_environment(self._config(root, self._registry(root, {"smoke": True}))))

            result = run_validation_tests(
                environ=environment,
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(list(result["tests"]), ["smoke"])
        self.assertEqual(result["tests"]["smoke"]["status"], "pass")

    def test_pass_fail_test_needs_only_descriptor_and_global_stanza(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=40)
            registry = self._registry(root, {"smoke": True})

            result = run_validation_tests(
                config=self._config(root, registry),
                registry=registry,
                environ=self._environment(root, root / "data"),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )

        self.assertEqual(list(result["tests"]), ["smoke"])
        self.assertEqual(result["schema_version"], "cval.results.v3")
        self.assertEqual(result["tests"]["smoke"]["status"], "pass")
        self.assertIsNone(registry.require("smoke").definition.plugin)

    def test_rejects_unsafe_run_id_before_writing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            environment = self._environment(root, root / "data")
            environment["CVAL_RUN_ID"] = "../escape"

            with self.assertRaisesRegex(ValueError, "Invalid run_id"):
                run_validation_tests(
                    config=self._config(root, registry),
                    registry=registry,
                    environ=environment,
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertFalse((root / "escape").exists())

    def test_rejects_reused_run_id_without_mixing_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            environment = self._environment(root, root / "data")
            first = run_validation_tests(
                config=self._config(root, registry),
                registry=registry,
                environ=environment,
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            with self.assertRaisesRegex(FileExistsError, "refusing run_id reuse"):
                run_validation_tests(
                    config=self._config(root, registry),
                    registry=registry,
                    environ=environment,
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

        self.assertEqual(first["tests"]["smoke"]["status"], "pass")

    def test_rejects_preexisting_per_test_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            stale_dir = (
                root
                / "data/logs/smoke/node-a/node-a-123"
            )
            stale_dir.mkdir(parents=True)
            stale_file = stale_dir / "stale.txt"
            stale_file.write_text("old evidence\n", encoding="utf-8")

            with self.assertRaisesRegex(FileExistsError, "refusing run_id reuse"):
                run_validation_tests(
                    config=self._config(root, registry),
                    registry=registry,
                    environ=self._environment(root, root / "data"),
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertEqual(stale_file.read_text(encoding="utf-8"), "old evidence\n")

    def test_rejects_symlinked_global_evidence_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            validation_root = root / "data"
            external = root / "outside"
            external.mkdir()
            (validation_root / "logs/job_logs").mkdir(parents=True)
            (validation_root / "logs/job_logs/node-a").symlink_to(
                external,
                target_is_directory=True,
            )

            with self.assertRaises(OSError):
                run_validation_tests(
                    config=self._config(root, registry),
                    registry=registry,
                    environ=self._environment(root, validation_root),
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertEqual(list(external.iterdir()), [])

    def test_rejects_symlinked_global_log_leaf(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "smoke", order=10)
            registry = self._registry(root, {"smoke": True})
            validation_root = root / "data"
            log_dir = validation_root / "logs/job_logs/node-a/node-a-123"
            log_dir.mkdir(parents=True)
            outside = root / "outside.log"
            (log_dir / "job.log").symlink_to(outside)

            with self.assertRaises(FileExistsError):
                run_validation_tests(
                    config=self._config(root, registry),
                    registry=registry,
                    environ=self._environment(root, validation_root),
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                )

            self.assertFalse(outside.exists())

    def test_sigterm_persists_interrupted_and_kills_workload_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_test(root, "signal-test", order=10, timeout_seconds=30)
            (root / "validation-tests/signal-test/run-test.sh").write_text(
                """#!/bin/bash
echo $$ > "$CVAL_TEST_OUTPUT_DIR/pid"
while :; do :; done
""",
                encoding="utf-8",
            )
            environment = self._environment(root, root / "data")
            environment.update(build_runtime_environment(self._config(root, self._registry(root, {"signal-test": True}))))
            process = subprocess.Popen(
                [sys.executable, "-m", "cval.validation.runner"],
                cwd=Path(__file__).resolve().parents[1],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            result_path = root / "data/logs/job_logs/node-a/node-a-123/result.json"
            pid_path = (
                root
                / "data/logs/signal-test/node-a/node-a-123/pid"
            )
            try:
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    if pid_path.is_file() and pid_path.stat().st_size:
                        break
                    time.sleep(0.02)
                else:
                    self.fail("runner did not reach running phase")

                workload_pid = int(pid_path.read_text(encoding="utf-8"))
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=6.0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=2.0)

            events = self._read_events(result_path.parent / "job.log")
            self.assertFalse(result_path.exists())
            with self.assertRaises(ProcessLookupError):
                os.kill(workload_pid, 0)

        self.assertEqual(process.returncode, 130)
        self.assertTrue(any(event.get("test") == "signal-test" and event.get("phase") == "interrupted" for event in events))

    @staticmethod
    def _config(root: Path, registry):
        base = load_config()
        return replace(base, job=replace(base.job, template_path=root / "template.yml", image_name="test-image", git_ref="a" * 40),
                       tests=replace(base.tests, registry=registry), runtime=replace(base.runtime, repo_dir=str(root),
                           validation_tests_dir=str(root / "validation-tests"), validation_root=str(root / "data"),
                           dl_results_root_path=str(root / "data/logs/dltest")))

    @staticmethod
    def _environment(repo_root: Path, validation_root: Path) -> dict[str, str]:
        validation_root.mkdir(parents=True, exist_ok=True)
        return os.environ | {
            "CVAL_REPO_DIR": str(repo_root),
            "CVAL_VALIDATION_ROOT": str(validation_root),
            "CVAL_RUN_ID": "node-a-123",
            "CVAL_NODE": "node-a",
            "CVAL_TIMESTAMP": "123",
            "CVAL_IMAGE_NAME": "test-image",
            "CVAL_PYTORCH_VERSION": "2.8.0",
            "CVAL_CUDA_VERSION": "12.9",
            "CVAL_GIT_REF": "a" * 40,
            "GCRNODE": "node-a",
            "GCRTIME": "123",
        }

    @staticmethod
    def _registry(root: Path, activations: dict[str, bool]):
        return load_test_registry(
            {
                test_id: {
                    "enabled": enabled,
                    "config_path": f"validation-tests/{test_id}/test_config.toml",
                }
                for test_id, enabled in activations.items()
            },
            repo_root=root,
            include_defaults=False,
            require_enabled=False,
        )

    @staticmethod
    def _write_test(
        root: Path,
        test_id: str,
        *,
        order: int,
        exit_code: int = 0,
        setup_exit: int = 0,
        timeout_seconds: int = 30,
    ) -> None:
        test_dir = root / "validation-tests" / test_id
        test_dir.mkdir(parents=True)
        (test_dir / "setup.sh").write_text(
            f'#!/bin/bash\necho "setup-{test_id}"\nexit {setup_exit}\n',
            encoding="utf-8",
        )
        (test_dir / "run-test.sh").write_text(
            f"""#!/bin/bash
set -euo pipefail
echo "run-{test_id}"
echo "err-{test_id}" >&2
python3 - "$CVAL_RESULT_JSON_FILE" "$CVAL_TEST_ID" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["tests"][sys.argv[2]]["phase"] == "running"
PY
printf '{{"test":"%s"}}\n' "$CVAL_TEST_ID" > "$CVAL_TEST_SUMMARY_FILE"
printf 'artifact-%s\n' "$CVAL_TEST_ID" > "$CVAL_TEST_OUTPUT_DIR/artifact.txt"
exit {exit_code}
""",
            encoding="utf-8",
        )
        (test_dir / "test_config.toml").write_text(
            f"""
schema_version = "cval.test.v1"

[test]
id = "{test_id}"
display_name = "{test_id.title()}"
description = "Runner fixture"
order = {order}
entrypoint = "run-test.sh"
setup = "setup.sh"
timeout_seconds = {timeout_seconds}
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
""",
            encoding="utf-8",
        )

    @staticmethod
    def _read_events(path: Path) -> list[dict[str, object]]:
        return [json.loads(line.removeprefix("CVAL_EVENT ")) for line in path.read_text(encoding="utf-8").splitlines() if line.startswith("CVAL_EVENT ")]


if __name__ == "__main__":
    unittest.main()
