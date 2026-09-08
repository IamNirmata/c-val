import tempfile
import unittest
import hashlib
import json
import sqlite3
import os
import subprocess
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from evaluation_engine.config import load_settings
from evaluation_engine.database import Database
from evaluation_engine.statistics import build_rule, classify
from evaluation_engine.runtime import Context
from evaluation_engine.__main__ import run_once
from cval.config import config_to_dict


REPO = Path(__file__).resolve().parents[1]


def fixture(root):
    base = load_settings(REPO / "config/cval.toml")
    raw_root = root / "raw"
    raw_root.mkdir()
    storage = replace(base.raw.storage, **{name: str(raw_root / (name + ".db")) for name in vars(base.raw.storage)})
    raw = replace(base.raw, storage=storage, runtime=replace(base.raw.runtime, validation_root=str(raw_root)))
    policies = tuple(replace(test, settings=test.settings | {"min_nodes": 3, "expected_ranks": 2, "holdout_seconds": 100, "window_seconds": 1000, "refresh_seconds": 10000}, policy_digest="fixture-" + test.name) for test in base.tests)
    settings = replace(base, raw=raw, output=root / "derived/evaluation.db", tests=policies, batch_size=20)
    with closing(sqlite3.connect(storage.validation_db_path)) as connection:
        connection.execute("CREATE TABLE runs(node TEXT,timestamp INTEGER,test TEXT,result TEXT,image_name TEXT,pytorch_version TEXT,cuda_version TEXT)")
    wide = (("storage_db_path", "storage_performance", "node", policies[0].settings["metrics"]["storage"]), ("nccl_db_path", "IB_HEALTH", "Node", policies[1].settings["metrics"]["nccl"]))
    for field, table, node_column, metrics in wide:
        extra = ",iterations INTEGER,data_size_gb INTEGER,samples INTEGER" if table == "IB_HEALTH" else ""
        with closing(sqlite3.connect(getattr(storage, field))) as connection:
            connection.execute(f'CREATE TABLE "{table}" ("{node_column}" TEXT,timestamp INTEGER,' + ','.join(f'"{name}" REAL' for name in metrics) + extra + ')')
    for component, field in policies[2].baseline.SOURCES.items():
        with closing(sqlite3.connect(getattr(storage, field))) as connection:
            connection.executescript(f'CREATE TABLE "{component}" (node TEXT,cval_timestamp INTEGER,run_key TEXT,rank INTEGER,test_plan TEXT,iterations INTEGER,task_group TEXT,task_name TEXT,metric_name TEXT,metric_value REAL,status TEXT); CREATE TABLE cval_ingest_metadata(id INTEGER,generation_id TEXT,state TEXT); INSERT INTO cval_ingest_metadata VALUES(1,"g1","complete"); CREATE TABLE cval_ingested_runs(run_key TEXT PRIMARY KEY);')
    for index, timestamp in enumerate((100, 200, 300, 950)):
        node = f"node-{index}"
        run_id = f"{node}-{timestamp}"
        result = {"node": node, "timestamp": timestamp, "run_id": run_id, "image_name": "image", "pytorch_version": "torch", "cuda_version": "cuda", "overall": "pass", "tests": {test.name: {"status": "pass", "enabled": True, "config_digest": "config-" + test.name} for test in policies}}
        result_path = raw_root / "logs/job_logs" / node / run_id / "result.json"
        result_path.parent.mkdir(parents=True)
        result_path.write_text(json.dumps(result))
        summary = raw_root / "validation_tests/dltest/runs" / node / run_id / "summary.json"
        summary.parent.mkdir(parents=True)
        summary.write_text(json.dumps({"gpu_count": 2, "rank_coverage_valid": True, "rank_results": [{"run_id": f"run_NVIDIA-B200_CUDA-13_RANK{rank}"} for rank in range(2)]}))
        plan = summary.parent / "artifacts/workdir/test_plans/plan/test_plan.json"
        plan.parent.mkdir(parents=True)
        plan.write_text('{"tasks": ["task"]}')
        with closing(sqlite3.connect(storage.validation_db_path)) as connection:
            connection.executemany("INSERT INTO runs VALUES(?,?,?,?,?,?,?)", [(node, timestamp, test, "pass", "image", "torch", "cuda") for test in ("all", "storage", "nccl", "dltest")])
            connection.commit()
        for field, table, node_column, metrics in wide:
            values = [node, timestamp] + [10.0] * len(metrics) + ([20, 8, 30] if table == "IB_HEALTH" else [])
            with closing(sqlite3.connect(getattr(storage, field))) as connection:
                connection.execute(f'INSERT INTO "{table}" VALUES (' + ','.join('?' for _ in values) + ')', values)
                connection.commit()
        for component, field in policies[2].baseline.SOURCES.items():
            with closing(sqlite3.connect(getattr(storage, field))) as connection:
                connection.execute("INSERT INTO cval_ingested_runs VALUES(?)", (run_id,))
                for rank in range(2):
                    for metric in policies[2].settings["metrics"][component]:
                        value = 10.0 if index < 3 or component == "numerical_correctness" else 50.0
                        connection.execute(f'INSERT INTO "{component}" VALUES(?,?,?,?,?,?,?,?,?,?,?)', (node, timestamp, run_id, rank, "plan", 100, "tasks", "task", metric, value, "completed"))
                connection.commit()
    return settings


class EvaluationTests(unittest.TestCase):
    def test_bad_source_does_not_block_other_test_and_is_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            run_once(settings, "a" * 40, now=1000)
            with closing(sqlite3.connect(settings.raw.storage.dl_compute_db_path)) as connection:
                connection.execute('ALTER TABLE compute_performance RENAME TO broken')
            report = run_once(settings, "a" * 40, now=1001)
            self.assertEqual(report["tests"]["storage"]["processed"], 4)
            self.assertEqual(Database(settings).read("SELECT status,health_class FROM latest_test_class WHERE node='node-3' AND test='dltest'"), [("error", None)])

    def test_late_arrival_is_discovered_by_rowid_not_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            run_once(settings, "a" * 40, now=1000)
            with closing(sqlite3.connect(settings.raw.storage.validation_db_path)) as connection:
                connection.executemany('INSERT INTO runs VALUES(?,?,?,?,?,?,?)', [('late-node', 50, test, 'pass', 'image', 'torch', 'cuda') for test in ('all', 'storage', 'nccl', 'dltest')])
                connection.commit()
            run_once(settings, "a" * 40, now=1001)
            self.assertEqual(Database(settings).read("SELECT COUNT(*) FROM evaluation WHERE node='late-node' AND raw_timestamp=50")[0][0], 3)

    def test_settings_do_not_enter_raw_snapshots(self):
        settings = load_settings(REPO / 'config/cval.toml')
        self.assertNotIn('evaluation', config_to_dict(settings.raw))
        for definition in settings.raw.tests.registry.to_dict().values():
            self.assertNotIn('evaluation', definition)

    def test_output_cannot_alias_raw_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / 'raw.db'
            raw.touch()
            output = root / 'output.db'
            os.link(raw, output)
            config = root / 'config.toml'
            text = (REPO / 'config/cval.toml').read_text()
            text = text.replace('/evaluation/evaluation.db', str(output)).replace('/data/continuous_validation/metadata/validation.db', str(raw))
            config.write_text(text)
            with self.assertRaisesRegex(ValueError, 'hard-link'):
                load_settings(config)

    def test_unconfirmed_worker_does_not_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'never.db'
            config = root / 'config.toml'
            config.write_text((REPO / 'config/cval.toml').read_text().replace('/evaluation/evaluation.db', str(output)))
            result = subprocess.run([sys.executable, '-m', 'evaluation_engine', '--config', str(config), '--once'], cwd=REPO, capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(output.exists())

    def test_missing_coverage_and_recovery_do_not_hide_latest_state(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            run_once(settings, "a" * 40, now=1000)
            source = Path(settings.raw.runtime.validation_root) / "logs/job_logs/node-3/node-3-950/result.json"
            content = source.read_text()
            source.write_text("{}")
            run_once(settings, "a" * 40, now=1001)
            database = Database(settings)
            self.assertEqual(database.read("SELECT DISTINCT status,health_class FROM latest_test_class WHERE node='node-3'"), [("unclassified", None)])
            source.write_text(content)
            run_once(settings, "a" * 40, now=1002)
            self.assertEqual(database.read("SELECT health_class FROM latest_test_class WHERE node='node-3' AND test='dltest'"), [("bad",)])

    def test_wal_on_shared_raw_storage_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            with closing(sqlite3.connect(settings.raw.storage.dl_compute_db_path)) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
            context = Context(settings, "a" * 40, now=1000)
            with self.assertRaisesRegex(ValueError, "shared_storage_WAL"):
                context.read(settings.raw.storage.dl_compute_db_path, "SELECT 1")

    def test_expired_baseline_has_no_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            run_once(settings, "a" * 40, now=1000)
            context = Context(settings, "a" * 40, now=1001)
            database = context.database
            database.transaction(lambda connection: connection.execute("UPDATE baseline_version SET expires_at=1000"))
            target = context.candidates()[-1]
            for test in settings.tests:
                test.classification.classify(context, test, target)
            self.assertEqual(database.read("SELECT DISTINCT health_class,status FROM latest_test_class WHERE node='node-3'"), [(None, "unclassified")])

    def test_changed_plan_does_not_use_other_cohort_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            run_once(settings, "a" * 40, now=1000)
            plan = Path(settings.raw.runtime.validation_root) / "validation_tests/dltest/runs/node-3/node-3-950/artifacts/workdir/test_plans/plan/test_plan.json"
            plan.write_text('{"tasks": ["different"]}')
            run_once(settings, "a" * 40, now=1001)
            self.assertEqual(Database(settings).read("SELECT health_class,status FROM latest_test_class WHERE node='node-3' AND test='dltest'"), [(None, "unclassified")])

    def test_full_test_owned_build_classify_replay_and_raw_immutability(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            paths = [Path(value) for value in vars(settings.raw.storage).values()]
            before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
            report = run_once(settings, "a" * 40, now=1000)
            database = Database(settings)
            self.assertEqual(sum(test["published"] for test in report["tests"].values()), 3)
            classifications = database.read("SELECT test,health_class,status FROM latest_test_class WHERE node='node-3' ORDER BY test")
            self.assertEqual(classifications, [("dltest", "bad", "complete"), ("nccl", "good", "complete"), ("storage", "good", "complete")])
            self.assertEqual(database.read("SELECT COUNT(*) FROM baseline_version")[0][0], 3)
            run_once(settings, "a" * 40, now=1001)
            self.assertEqual(database.read("SELECT COUNT(*) FROM evaluation")[0][0], 12)
            self.assertEqual(before, {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})

    def test_test_owned_modules_and_config_load(self):
        settings = load_settings(Path(__file__).resolve().parents[1] / "config/cval.toml")
        self.assertEqual([test.name for test in settings.tests], ["storage", "nccl", "dltest"])
        for test in settings.tests:
            self.assertTrue(callable(test.baseline.build))
            self.assertTrue(callable(test.baseline.read))
            self.assertTrue(callable(test.classification.classify))
        with tempfile.TemporaryDirectory() as directory:
            database = Database(replace(settings, output=Path(directory) / "evaluation.db"))
            database.initialize()
            self.assertEqual(len(database.read("SELECT name FROM sqlite_master WHERE type='table'")), 6)

    def test_metric_directions_and_zero_reference(self):
        policy = {"min_nodes": 3, "quantile": 0.95, "min_mode_fraction": 0.9, "atol": 0.001, "rtol": 0.01, "min_relative_gap": 0.05}
        low = build_rule([10, 10, 10], "lower", policy)
        high = build_rule([10, 10, 10], "higher", policy)
        zero = build_rule([0, 0, 0], "reference", policy)
        self.assertEqual(classify(11, low)[0], "bad")
        self.assertEqual(classify(9, high)[0], "bad")
        self.assertEqual(classify(0, zero)[0], "good")
        self.assertEqual(classify(1, zero)[0], "bad")
        self.assertIsNone(build_rule([1], "lower", policy))
        self.assertIsNone(classify(float("nan"), low)[0])