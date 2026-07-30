"""Tests for DL rank JSON ingestion into metric DBs."""

from __future__ import annotations

import json
import shutil
import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import cval.storage.dltest_ingest as dl_ingest_module

from cval.storage.dltest_ingest import (
    HISTORICAL_DL_ITERATIONS,
    dl_run_iterations,
    ensure_iterations_column,
    find_dl_run_dirs,
    ingest_dltest_results as _ingest_dltest_results,
    load_rank_files,
    parse_rank,
    validate_dl_metric_generation,
    connect,
)
from cval.config import encode_config_snapshot, load_config
from cval.storage.write_provenance import authorize_dl_rebuild


def ingest_dltest_results(results_root=None, output_dir=None, *, config=None):
    root = Path(results_root) if results_root is not None else Path(
        (config or load_config()).runtime.dl_results_root_path
    )
    active = config or load_config()
    if output_dir is None:
        paths = dl_ingest_module.default_dl_metric_db_paths(config)
    else:
        output = Path(output_dir)
        paths = {
            "numerical_correctness": output / "dltest_numerical_correctness.db",
            "compute_performance": output / "dltest_compute_performance.db",
            "collective_performance": output / "dltest_collective_performance.db",
            "overlap_performance": output / "dltest_overlap_performance.db",
        }
    active = replace(
        active,
        storage=replace(
            active.storage,
            dl_numerical_db_path=str(paths["numerical_correctness"]),
            dl_compute_db_path=str(paths["compute_performance"]),
            dl_collective_db_path=str(paths["collective_performance"]),
            dl_overlap_db_path=str(paths["overlap_performance"]),
        ),
        runtime=replace(active.runtime, dl_results_root_path=str(root)),
    )
    authorization = authorize_dl_rebuild(
        root,
        output_dir,
        config=active,
        config_snapshot_b64=encode_config_snapshot(active),
    )
    return _ingest_dltest_results(
        root,
        output_dir,
        config=active,
        _authorization=authorization,
    )


def _write_rank_json(path: Path, rank: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "runID": f"20260618_example_RANK{rank}",
        "test_plan": "80gb-example",
        "nn_tasks": [
            {
                "task_name": "linear_float16",
                "status": "completed",
                "norm_output": 0.5 + rank,
                "weight": 0.1 + rank,
                "bias": 0.01 + rank,
                "fp_cpu_time": 10.0 + rank,
                "fp_gpu_time": 1.0 + rank,
                "bp_cpu_time": 11.0 + rank,
                "bp_gpu_time": 1.1 + rank,
            }
        ],
        "f_tasks": [
            {
                "task_name": "functional_float16",
                "status": "completed",
                "norm_output": 0.6 + rank,
                "weight": 0.2 + rank,
                "fp_cpu_time": 20.0 + rank,
                "fp_gpu_time": 2.0 + rank,
                "bp_cpu_time": 21.0 + rank,
                "bp_gpu_time": 2.1 + rank,
            }
        ],
        "coll_tasks": [
            {
                "task_name": "allreduce_float16",
                "status": "completed",
                "norm_output": 0.7 + rank,
                "cpu_time": 30.0 + rank,
                "gpu_time": 3.0 + rank,
            }
        ],
        "overlap_tasks": [
            {
                "task_name": "overlap_float16",
                "status": "completed",
                "coll_name": "allreduce",
                "layer_name": "linear",
                "coll_mean": 4.0 + rank,
                "coll_stdev": 0.4 + rank,
                "layer_mean": 5.0 + rank,
                "layer_stdev": 0.5 + rank,
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


class DltestIngestTests(unittest.TestCase):
    def test_dl_rebuild_rejects_symlinked_results_root_before_db_write(self) -> None:
        with TemporaryDirectory() as tmpdir:
            outside = Path(tmpdir) / "outside-results"
            outside.mkdir()
            root = Path(tmpdir) / "results"
            root.symlink_to(outside, target_is_directory=True)
            output = Path(tmpdir) / "output"

            with self.assertRaisesRegex(ValueError, "non-symlink"):
                ingest_dltest_results(root, output)

            self.assertFalse(output.exists())

    def test_dl_rebuild_rejects_results_root_symlink_swap_after_authorization(
        self,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            root.mkdir()
            outside = Path(tmpdir) / "outside-results"
            outside.mkdir()
            output = Path(tmpdir) / "output"
            base = load_config()
            config = replace(
                base,
                storage=replace(
                    base.storage,
                    dl_numerical_db_path=str(
                        output / "dltest_numerical_correctness.db"
                    ),
                    dl_compute_db_path=str(output / "dltest_compute_performance.db"),
                    dl_collective_db_path=str(
                        output / "dltest_collective_performance.db"
                    ),
                    dl_overlap_db_path=str(output / "dltest_overlap_performance.db"),
                ),
                runtime=replace(base.runtime, dl_results_root_path=str(root)),
            )
            authorization = authorize_dl_rebuild(
                root,
                output,
                config=config,
                config_snapshot_b64=encode_config_snapshot(config),
            )
            root.rmdir()
            root.symlink_to(outside, target_is_directory=True)

            with self.assertRaisesRegex(ValueError, "non-symlink"):
                _ingest_dltest_results(
                    root,
                    output,
                    config=config,
                    _authorization=authorization,
                )

            self.assertFalse(output.exists())

    def test_dl_rebuild_rejects_symlinked_rank_evidence_before_db_write(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            run_dir = root / "dltest-node-a-1781649558"
            rank_path = (
                run_dir
                / "workdir/test_plans/80gb-example/runs/example_RANK0.json"
            )
            rank_path.parent.mkdir(parents=True)
            external = Path(tmpdir) / "external-rank.json"
            _write_rank_json(external, 0)
            rank_path.symlink_to(external)
            output = Path(tmpdir) / "output"

            with self.assertRaisesRegex(ValueError, "symlink"):
                ingest_dltest_results(root, output)

            self.assertFalse(output.exists())

    def test_direct_dl_rebuild_requires_configured_provenance(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            root.mkdir()
            output = Path(tmpdir) / "output"

            with self.assertRaisesRegex(PermissionError, "configured provenance"):
                _ingest_dltest_results(root, output)

            self.assertFalse(output.exists())

    def test_dl_writer_rejects_symlinked_database_target(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            external = root / "external.db"
            with closing(sqlite3.connect(external)) as connection:
                connection.execute("CREATE TABLE sentinel(value TEXT)")
                connection.commit()
            target = root / "dl.db"
            target.symlink_to(external)

            with self.assertRaisesRegex(ValueError, "symlink"):
                connect(target)

    def test_parse_rank_handles_new_and_old_run_id_shapes(self) -> None:
        self.assertEqual(parse_rank("20260617_example_RANK7"), 7)
        self.assertEqual(parse_rank("20260617_rank2_world8_node"), 2)

    def test_recursive_scanner_handles_remapped_root(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "dltest-slc01-cl02-hgx-0002-1781649558"
            _write_rank_json(
                run_dir
                / "workdir"
                / "test_plans"
                / "80gb-example"
                / "runs"
                / "example_RANK0.json",
                0,
            )

            self.assertEqual(find_dl_run_dirs(root), [run_dir])
            rank_files = list(load_rank_files(root))

        self.assertEqual(len(rank_files), 1)
        self.assertEqual(rank_files[0].node, "slc01-cl02-hgx-0002")
        self.assertEqual(rank_files[0].cval_timestamp, 1781649558)
        self.assertEqual(rank_files[0].rank, 0)

    def test_recursive_scanner_handles_nested_continuous_validation_root(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "slc01-cl02-hgx-0005" / "dltest-slc01-cl02-hgx-0005-1781655211"
            _write_rank_json(
                run_dir
                / "workdir"
                / "test_plans"
                / "80gb-example"
                / "runs"
                / "example_RANK1.json",
                1,
            )

            self.assertEqual(find_dl_run_dirs(root), [run_dir])
            rank_files = list(load_rank_files(root))

        self.assertEqual(rank_files[0].node, "slc01-cl02-hgx-0005")
        self.assertEqual(rank_files[0].rank, 1)

    def test_recursive_scanner_handles_canonical_v2_run_layout(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "validation_tests" / "dltest" / "runs"
            run_dir = root / "node-a" / "node-a-1781655211"
            _write_rank_json(
                run_dir
                / "artifacts"
                / "workdir"
                / "test_plans"
                / "80gb-example"
                / "runs"
                / "example_RANK0.json",
                0,
            )
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "iterations": 100,
                        "gpu_count": 1,
                        "rank_result_count": 1,
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(find_dl_run_dirs(root), [run_dir])
            rank_files = list(load_rank_files(root))

        self.assertEqual(rank_files[0].node, "node-a")
        self.assertEqual(rank_files[0].cval_timestamp, 1781655211)
        self.assertEqual(rank_files[0].iterations, 100)

    def test_canonical_scanner_skips_incomplete_failed_and_malformed_runs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "runs"
            missing = root / "node-a" / "node-a-1"
            failed = root / "node-b" / "node-b-2"
            malformed = root / "node-c" / "node-c-3"
            for run_dir in (missing, failed):
                _write_rank_json(
                    run_dir
                    / "artifacts/workdir/test_plans/80gb-example/runs/rank_RANK0.json",
                    0,
                )
            (failed / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "fail",
                        "iterations": 100,
                        "gpu_count": 1,
                        "rank_result_count": 1,
                    }
                ),
                encoding="utf-8",
            )
            malformed_rank = (
                malformed
                / "artifacts/workdir/test_plans/80gb-example/runs/rank_RANK0.json"
            )
            malformed_rank.parent.mkdir(parents=True)
            malformed_rank.write_text("{not-json}", encoding="utf-8")
            (malformed / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "iterations": 100,
                        "gpu_count": 1,
                        "rank_result_count": 1,
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(list(load_rank_files(root)), [])

    def test_canonical_scanner_requires_complete_rank_set(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "runs"
            run_dir = root / "node-a" / "node-a-1"
            _write_rank_json(
                run_dir
                / "artifacts/workdir/test_plans/80gb-example/runs/rank_RANK0.json",
                0,
            )
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "iterations": 100,
                        "gpu_count": 2,
                        "rank_result_count": 1,
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(list(load_rank_files(root)), [])

    def test_rebuild_purges_metrics_when_canonical_run_becomes_failed(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "runs"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "node-a" / "node-a-1"
            _write_rank_json(
                run_dir
                / "artifacts/workdir/test_plans/80gb-example/runs/rank_RANK0.json",
                0,
            )
            summary = {
                "status": "pass",
                "iterations": 100,
                "gpu_count": 1,
                "rank_result_count": 1,
            }
            (run_dir / "summary.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )
            ingest_dltest_results(root, output)
            summary["status"] = "fail"
            (run_dir / "summary.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )

            result = ingest_dltest_results(root, output)
            with closing(
                sqlite3.connect(output / "dltest_compute_performance.db")
            ) as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM compute_performance"
                ).fetchone()[0]

        self.assertEqual(result["rank_files"], 0)
        self.assertEqual(count, 0)

    def test_rebuild_purges_metrics_for_deleted_run(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "runs"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "node-a" / "node-a-1"
            _write_rank_json(
                run_dir
                / "artifacts/workdir/test_plans/80gb-example/runs/rank_RANK0.json",
                0,
            )
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "iterations": 100,
                        "gpu_count": 1,
                        "rank_result_count": 1,
                    }
                ),
                encoding="utf-8",
            )
            ingest_dltest_results(root, output)
            shutil.rmtree(run_dir)

            result = ingest_dltest_results(root, output)
            with closing(
                sqlite3.connect(output / "dltest_compute_performance.db")
            ) as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM compute_performance"
                ).fetchone()[0]

        self.assertEqual(result["rank_files"], 0)
        self.assertEqual(count, 0)

    def test_ingest_writes_four_metric_dbs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output_dir = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-slc01-cl02-hgx-0002-1781649558"
            for rank in range(2):
                _write_rank_json(
                    run_dir
                    / "workdir"
                    / "test_plans"
                    / "80gb-example"
                    / "runs"
                    / f"example_RANK{rank}.json",
                    rank,
                )

            summary = ingest_dltest_results(root, output_dir)

            self.assertEqual(summary["rank_files"], 2)
            self.assertEqual(summary["runs"], 1)
            expected_tables = {
                "dltest_numerical_correctness.db": "numerical_correctness",
                "dltest_compute_performance.db": "compute_performance",
                "dltest_collective_performance.db": "collective_performance",
                "dltest_overlap_performance.db": "overlap_performance",
            }
            for db_name, table_name in expected_tables.items():
                with closing(sqlite3.connect(output_dir / db_name)) as connection:
                    count, minimum, maximum = connection.execute(
                        f"SELECT COUNT(*), MIN(iterations), MAX(iterations) FROM {table_name}"
                    ).fetchone()
                self.assertGreater(count, 0)
                self.assertEqual((minimum, maximum), (HISTORICAL_DL_ITERATIONS, HISTORICAL_DL_ITERATIONS))
            paths = {
                component: output_dir / filename
                for filename, component in expected_tables.items()
            }
            generation = validate_dl_metric_generation(paths)
            self.assertEqual(generation, summary["generation_id"])

            with closing(sqlite3.connect(paths["compute_performance"])) as connection:
                connection.execute(
                    "UPDATE cval_ingest_metadata SET generation_id='different' WHERE id=1"
                )
                connection.commit()
            with self.assertRaisesRegex(RuntimeError, "generation"):
                validate_dl_metric_generation(paths)

    def test_default_ingest_honors_exact_configured_db_paths(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            run_dir = root / "dltest-node-a-1781649558"
            _write_rank_json(
                run_dir / "workdir/test_plans/80gb-example/runs/example_RANK0.json",
                0,
            )
            configured = {
                "numerical_correctness": Path(tmpdir) / "a/numerical.custom.db",
                "compute_performance": Path(tmpdir) / "b/compute.custom.db",
                "collective_performance": Path(tmpdir) / "c/collective.custom.db",
                "overlap_performance": Path(tmpdir) / "d/overlap.custom.db",
            }
            with patch(
                "cval.storage.dltest_ingest.default_dl_metric_db_paths",
                return_value=configured,
            ):
                ingest_dltest_results(root)

            self.assertTrue(all(path.is_file() for path in configured.values()))

    def test_dl_rebuild_preflights_all_targets_before_first_write(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            run_dir = root / "dltest-node-a-1781649558"
            _write_rank_json(
                run_dir / "workdir/test_plans/80gb-example/runs/example_RANK0.json",
                0,
            )
            output = Path(tmpdir) / "output"
            outside = Path(tmpdir) / "outside"
            outside.mkdir()
            output.mkdir()
            (output / "dltest_compute_performance.db").symlink_to(
                outside / "compute.db"
            )

            with self.assertRaisesRegex(ValueError, "symlink"):
                ingest_dltest_results(root, output)

            self.assertFalse(
                (output / "dltest_numerical_correctness.db").exists()
            )
            self.assertEqual(list(outside.iterdir()), [])

    def test_summary_iterations_are_written_to_all_metric_dbs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output_dir = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            _write_rank_json(
                run_dir / "workdir/test_plans/80gb-example/runs/example_RANK0.json", 0
            )
            (run_dir / "dltest-summary-node-a-1781649558.json").write_text(
                json.dumps({"iterations": 77}), encoding="utf-8"
            )

            self.assertEqual(dl_run_iterations(run_dir), 77)
            ingest_dltest_results(root, output_dir)

            for db_path in output_dir.glob("dltest_*.db"):
                table_name = db_path.stem.removeprefix("dltest_")
                with closing(sqlite3.connect(db_path)) as connection:
                    values = connection.execute(
                        f"SELECT DISTINCT iterations FROM {table_name}"
                    ).fetchall()
                self.assertEqual(values, [(77,)])

    def test_normal_ingestion_helper_lazily_adds_iterations(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "legacy.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "CREATE TABLE compute_performance (id INTEGER PRIMARY KEY, metric_value REAL)"
                )
                connection.executemany(
                    "INSERT INTO compute_performance(metric_value) VALUES (?)", [(1.0,), (2.0,)]
                )
                ensure_iterations_column(connection, "compute_performance")
                ensure_iterations_column(connection, "compute_performance")
                connection.commit()
                columns = [
                    r[1] for r in connection.execute("PRAGMA table_info(compute_performance)")
                ]
                values = connection.execute(
                    "SELECT DISTINCT iterations FROM compute_performance"
                ).fetchall()

        self.assertIn("iterations", columns)
        self.assertEqual(values, [(20,)])

    def test_ingest_raises_when_no_rank_jsons_exist(self) -> None:
        with TemporaryDirectory() as tmpdir:
            with self.assertRaises(FileNotFoundError):
                ingest_dltest_results(
                    Path(tmpdir) / "missing",
                    Path(tmpdir) / "out",
                )


if __name__ == "__main__":
    unittest.main()
