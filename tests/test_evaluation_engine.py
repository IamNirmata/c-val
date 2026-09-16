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


FIXTURE_PLAN = {
    "test_name": "plan",
    "nn_tasks": [{"name": "linear", "layer": "nn.Linear(4, 4)", "overlap": True}, {"name": "recurrent", "layer": "nn.GRU(4, 4)"}],
    "f_tasks": [{"name": "relu", "layer": "nn.functional.relu(a)"}],
    "coll_tasks": [{"name": "allreduce", "coll": "dist.all_reduce(tensor)", "overlap": True}],
}
FIXTURE_METRICS = {
    "numerical_correctness": [
        ("nn_tasks", "linear", ("norm_output", "weight", "bias")),
        ("nn_tasks", "recurrent", ("norm_output", "weight_ih_l0", "weight_hh_l0", "bias_ih_l0", "bias_hh_l0")),
        ("f_tasks", "relu", ("norm_output", "weight")),
        ("coll_tasks", "allreduce", ("norm_output",)),
    ],
    "compute_performance": [(group, task, ("fp_cpu_time", "fp_gpu_time", "bp_cpu_time", "bp_gpu_time")) for group, task in (("nn_tasks", "linear"), ("nn_tasks", "recurrent"), ("f_tasks", "relu"))],
    "collective_performance": [("coll_tasks", "allreduce", ("cpu_time", "gpu_time"))],
    "overlap_performance": [("overlap_tasks", "allreduce+linear", ("coll_mean", "coll_stdev", "layer_mean", "layer_stdev"))],
}


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
        summary.write_text(json.dumps({"gpu_count": 2, "test_plan": "plan", "iterations": 100, "rank_coverage_valid": True, "rank_results": [{"run_id": f"run_NVIDIA-B200_CUDA-13_RANK{rank}"} for rank in range(2)]}))
        plan = summary.parent / "artifacts/workdir/test_plans/plan/test_plan.json"
        plan.parent.mkdir(parents=True)
        plan.write_text(json.dumps(FIXTURE_PLAN))
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
                    for group, task, metrics in FIXTURE_METRICS[component]:
                        for metric in metrics:
                            value = 10.0 if index < 3 or component == "numerical_correctness" else 50.0
                            connection.execute(f'INSERT INTO "{component}" VALUES(?,?,?,?,?,?,?,?,?,?,?)', (node, timestamp, run_id, rank, "plan", 100, group, task, metric, value, "completed"))
                connection.commit()
    return settings


class EvaluationTests(unittest.TestCase):
    def test_missing_required_dl_metrics_or_tasks_cannot_train_or_classify_good(self):
        for condition in ("metric", "task"):
            with self.subTest(condition=condition), tempfile.TemporaryDirectory() as directory:
                settings = fixture(Path(directory))
                for component, field in settings.tests[2].baseline.SOURCES.items():
                    with closing(sqlite3.connect(getattr(settings.raw.storage, field))) as connection:
                        connection.execute(f'UPDATE "{component}" SET metric_value=10')
                        if condition == "metric" and component == "numerical_correctness":
                            connection.execute(f'DELETE FROM "{component}" WHERE metric_name=?', ("weight",))
                        elif condition == "task" and component in {"numerical_correctness", "compute_performance"}:
                            connection.execute(f'DELETE FROM "{component}" WHERE task_group=? AND task_name=?', ("f_tasks", "relu"))
                        connection.commit()
                report = run_once(settings, "a" * 40, now=1000)
                self.assertEqual(report["tests"]["dltest"]["published"], 0)
                self.assertEqual(Database(settings).read("SELECT status,health_class,reason FROM latest_test_class WHERE node='node-3' AND test='dltest'"), [("unclassified", None, "dl_required_metric_coverage_mismatch")])

    def test_target_dl_missing_metric_is_unclassified_with_existing_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            run_once(settings, "a" * 40, now=1000)
            with closing(sqlite3.connect(settings.raw.storage.dl_compute_db_path)) as connection:
                connection.execute("DELETE FROM compute_performance WHERE node='node-3' AND metric_name='fp_gpu_time'")
                connection.commit()
            run_once(settings, "a" * 40, now=1001)
            self.assertEqual(Database(settings).read("SELECT status,health_class,reason FROM latest_test_class WHERE node='node-3' AND test='dltest'"), [("unclassified", None, "dl_required_metric_coverage_mismatch")])

    def test_layer_contract_respects_optional_parameters_and_never_executes_code(self):
        baseline = load_settings(REPO / "config/cval.toml").tests[2].baseline
        for layer, expected in (
            ("nn.Linear(4, 4)", {"norm_output", "weight", "bias"}),
            ("nn.Linear(4, 4, False)", {"norm_output", "weight"}),
            ("nn.LayerNorm(4, elementwise_affine=False)", {"norm_output"}),
            ("nn.RMSNorm(4)", {"norm_output", "weight"}),
            ("nn.InstanceNorm1d(4)", {"norm_output"}),
            ("nn.GroupNorm(2, 4, affine=False)", {"norm_output"}),
            ("nn.LocalResponseNorm(2)", {"norm_output"}),
            ("nn.GRU(4, 4, bias=False)", {"norm_output", "weight_ih_l0", "weight_hh_l0"}),
        ):
            with self.subTest(layer=layer):
                self.assertEqual(baseline.layer_numerical_metrics(layer), expected)
        for layer in ("__import__('os').system('false')", "nn.CustomLayer(4)", "nn.Linear(4, 4, bias=unknown)", "nn.Linear(*arguments)"):
            with self.subTest(layer=layer), self.assertRaises(ValueError):
                baseline.layer_numerical_metrics(layer)

    def test_catalog_pages_keep_complete_runs_above_metric_row_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = replace(fixture(Path(directory)), batch_size=2, max_metric_rows_per_run=4)
            context = Context(settings, "a" * 40, now=1000)
            cursor = 0
            seen = []
            for _page in range(3):
                page = context.candidates(after=cursor)
                self.assertLessEqual(len(page), 2)
                if not page:
                    break
                for run in page:
                    self.assertEqual(set(run["rows"]), {"storage", "nccl", "dltest", "all"})
                    self.assertFalse(run["conflict"])
                    seen.append(run["node"])
                cursor = page[-1]["sequence"]
            self.assertEqual(seen, ["node-0", "node-1", "node-2", "node-3"])
            self.assertEqual(context.candidates(after=cursor), [])

    def test_catalog_larger_than_fifty_thousand_rows_remains_pageable(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            with closing(sqlite3.connect(settings.raw.storage.validation_db_path)) as connection:
                connection.executemany("INSERT INTO runs VALUES(?,?,?,?,?,?,?)", (
                    (f"bulk-{index}", 950, test, "pass", "image", "torch", "cuda")
                    for index in range(13000) for test in ("storage", "nccl", "dltest", "all")
                ))
                connection.commit()
            context = Context(settings, "a" * 40, now=1000)
            self.assertEqual(len(context.candidates()), settings.batch_size)
            self.assertEqual(context.candidates(after=52000)[-1]["node"], "bulk-12999")

    def test_worker_reconciles_bounded_batches_then_wraps(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = replace(fixture(Path(directory)), batch_size=2)
            for timestamp in (1000, 1001, 1002):
                report = run_once(settings, "a" * 40, now=timestamp)
                self.assertTrue(all(test["processed"] == 2 for test in report["tests"].values()))
            database = Database(settings)
            self.assertEqual(database.read("SELECT COUNT(*) FROM evaluation")[0][0], 12)
            self.assertEqual(database.read("SELECT health_class FROM latest_test_class WHERE node='node-3' AND test='dltest'"), [("bad",)])

    def test_relocated_evidence_preserves_evaluation_and_raw_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = fixture(Path(directory))
            run_once(settings, "a" * 40, now=1000)
            database = Database(settings)
            before = database.read("SELECT node,test,health_class,status FROM latest_test_class ORDER BY node,test")
            raw_before = {value: hashlib.sha256(Path(value).read_bytes()).hexdigest() for value in vars(settings.raw.storage).values()}
            root = Path(settings.raw.runtime.validation_root)
            (root / "validation_tests").rename(root / "logs/validation_tests")
            run_once(settings, "a" * 40, now=1001)
            self.assertEqual(database.read("SELECT node,test,health_class,status FROM latest_test_class ORDER BY node,test"), before)
            self.assertEqual(raw_before, {value: hashlib.sha256(Path(value).read_bytes()).hexdigest() for value in vars(settings.raw.storage).values()})

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