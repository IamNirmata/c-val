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
    StandardMetricRow,
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
from cval.storage.write_provenance import (
    authorize_dl_rebuild,
    authorize_result_write,
    validate_current_dl_write,
)
from cval.validation.results import load_validation_result, validation_result_digest
from cval.validation.runtime import effective_config_digest
from tests.test_results_v2 import payload as result_v2_payload


def ingest_dltest_results(
    results_root=None, output_dir=None, *, config=None, only_missing=False
):
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
        only_missing=only_missing,
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


def _write_flat_rank_json(path: Path, rank: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    layer_names = ["linear_float16", *(f"nn_task_{index}" for index in range(56))]
    function_names = ["f.relu_float16", *(f"f.task_{index}" for index in range(37))]
    overlap_collectives = [
        "allgather_4_float16",
        "allgather_8_float16",
        "allreduce_4_float16",
        "allreduce_8_float16",
        "alltoall_4_float16",
        "reducescatter_4_float16",
    ]
    collective_names = [
        *overlap_collectives,
        *(f"allreduce_extra_{index}" for index in range(18)),
    ]
    payload = {
        "PartitionKey": "legacy",
        "test_plan": "80gb-b200",
    }
    for index, name in enumerate(layer_names):
        metrics = {
            "norm_output": 1.0 + rank + index,
            "fp_cpu_time": 3.0 + rank + index,
            "fp_gpu_time": 4.0 + rank + index,
            "bp_cpu_time": 5.0 + rank + index,
            "bp_gpu_time": 6.0 + rank + index,
        }
        if index < 42:
            metrics.update(
                {
                    "weight": 2.0 + rank + index,
                    "bias": 2.5 + rank + index,
                }
            )
        elif index < 48:
            metrics["weight"] = 2.0 + rank + index
        elif index < 54:
            metrics.update(
                {
                    "weight_hh_l0": 2.0 + rank + index,
                    "weight_ih_l0": 2.1 + rank + index,
                    "bias_hh_l0": 2.2 + rank + index,
                    "bias_ih_l0": 2.3 + rank + index,
                }
            )
        payload[name] = metrics
    for index, name in enumerate(function_names):
        payload[name] = {
            "norm_output": 7.0 + rank + index,
            "weight": 7.5 + rank + index,
            "fp_cpu_time": 8.0 + rank + index,
            "fp_gpu_time": 9.0 + rank + index,
            "bp_cpu_time": 10.0 + rank + index,
            "bp_gpu_time": 11.0 + rank + index,
        }
    for index, name in enumerate(collective_names):
        payload[name] = {
            "norm_output": 12.0 + rank + index,
            "coll_cpu": 13.0 + rank + index,
            "coll_gpu": 14.0 + rank + index,
        }
    overlap_layers = layer_names[:8]
    payload["overlap_tasks"] = {
        layer_name: {
            metric_name: value
            for collective_index, collective in enumerate(overlap_collectives)
            for metric_name, value in (
                (f"mean_{collective}", 15.0 + rank + collective_index),
                (f"stdev_{collective}", 1.5 + rank + collective_index),
            )
        }
        for layer_name in overlap_layers
    }
    payload["overlap_tasks"].update(
        {
            collective: {
                metric_name: value
                for layer_index, layer_name in enumerate(overlap_layers)
                for metric_name, value in (
                    (f"mean_{layer_name}", 25.0 + rank + layer_index),
                    (f"stdev_{layer_name}", 2.5 + rank + layer_index),
                )
            }
            for collective in overlap_collectives
        }
    )
    path.write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


class DltestIngestTests(unittest.TestCase):
    def test_pre_registry_flat_rank_json_is_normalized_without_invented_metrics(
        self,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            for rank in range(8):
                _write_flat_rank_json(
                    run_dir
                    / "workdir/test_plans/80gb-b200/runs"
                    / f"legacy_rank{rank}_world2.json",
                    rank,
                )

            summary = ingest_dltest_results(root, output, only_missing=True)

            with closing(
                sqlite3.connect(output / "dltest_collective_performance.db")
            ) as connection:
                collective = connection.execute(
                    "SELECT metric_name, metric_value, test_plan FROM collective_performance "
                    "WHERE rank=0 AND task_name='allreduce_4_float16' "
                    "ORDER BY rank, metric_name"
                ).fetchall()
            with closing(
                sqlite3.connect(output / "dltest_overlap_performance.db")
            ) as connection:
                overlap = connection.execute(
                    "SELECT coll_name, layer_name, metric_name, metric_value "
                    "FROM overlap_performance WHERE rank=0 "
                    "AND coll_name='allgather_4_float16' "
                    "AND layer_name='linear_float16' ORDER BY metric_name"
                ).fetchall()

        self.assertEqual(summary["rank_files"], 8)
        self.assertEqual(summary["numerical_correctness_rows"], 2168)
        self.assertEqual(summary["compute_performance_rows"], 3040)
        self.assertEqual(summary["collective_performance_rows"], 384)
        self.assertEqual(summary["overlap_performance_rows"], 1536)
        self.assertEqual(
            collective,
            [("cpu_time", 15.0, "80gb-b200"), ("gpu_time", 16.0, "80gb-b200")],
        )
        self.assertEqual(
            overlap,
            [
                ("allgather_4_float16", "linear_float16", "coll_mean", 15.0),
                ("allgather_4_float16", "linear_float16", "coll_stdev", 1.5),
                ("allgather_4_float16", "linear_float16", "layer_mean", 25.0),
                ("allgather_4_float16", "linear_float16", "layer_stdev", 2.5),
            ],
        )

    def test_pre_registry_flat_run_requires_all_eight_ranks(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            for rank in range(7):
                _write_flat_rank_json(
                    run_dir
                    / "workdir/test_plans/80gb-b200/runs"
                    / f"legacy_rank{rank}_world8.json",
                    rank,
                )

            with self.assertRaisesRegex(ValueError, "no valid rank evidence"):
                ingest_dltest_results(root, output, only_missing=True)

    def test_pre_registry_flat_collective_alias_collision_is_rejected(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            for rank in range(8):
                path = (
                    run_dir
                    / "workdir/test_plans/80gb-b200/runs"
                    / f"legacy_rank{rank}_world8.json"
                )
                _write_flat_rank_json(path, rank)
                if rank == 0:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["allreduce_4_float16"]["cpu_time"] = 999.0
                    path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "no valid rank evidence"):
                ingest_dltest_results(root, output, only_missing=True)

    def test_pre_registry_flat_extra_metric_is_rejected(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            for rank in range(8):
                path = (
                    run_dir
                    / "workdir/test_plans/80gb-b200/runs"
                    / f"legacy_rank{rank}_world8.json"
                )
                _write_flat_rank_json(path, rank)
                if rank == 0:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    payload["linear_float16"]["unexpected"] = 1.0
                    path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "no valid rank evidence"):
                ingest_dltest_results(root, output, only_missing=True)

    def test_pre_registry_flat_rejects_compensating_overlap_counts(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            for rank in range(8):
                path = (
                    run_dir
                    / "workdir/test_plans/80gb-b200/runs"
                    / f"legacy_rank{rank}_world8.json"
                )
                _write_flat_rank_json(path, rank)
                if rank in {0, 1}:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    layers = list(payload["overlap_tasks"])[:8]
                    collectives = list(payload["overlap_tasks"])[8:]
                    if rank == 0:
                        layer = payload["overlap_tasks"][layers[0]]
                        extra_collective = "allreduce_extra_0"
                        layer[f"mean_{extra_collective}"] = 1.0
                        layer[f"stdev_{extra_collective}"] = 0.1
                    else:
                        collective = payload["overlap_tasks"][collectives[0]]
                        del collective["mean_nn_task_6"]
                        del collective["stdev_nn_task_6"]
                    path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "no valid rank evidence"):
                ingest_dltest_results(root, output, only_missing=True)
    def test_current_dl_write_requires_exact_v2_artifacts_path(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            metadata = root / "metadata"
            results_root = root / "validation_tests/dltest/runs"
            run_dir = results_root / "node-a/node-a-123"
            run_dir.mkdir(parents=True)
            base = load_config()
            config = replace(
                base,
                storage=replace(
                    base.storage,
                    validation_db_path=str(metadata / "validation.db"),
                    storage_db_path=str(metadata / "storage.db"),
                    nccl_db_path=str(metadata / "nccl.db"),
                    dl_numerical_db_path=str(metadata / "numerical.db"),
                    dl_compute_db_path=str(metadata / "compute.db"),
                    dl_collective_db_path=str(metadata / "collective.db"),
                    dl_overlap_db_path=str(metadata / "overlap.db"),
                ),
                runtime=replace(
                    base.runtime,
                    validation_root=str(root),
                    dl_results_root_path=str(results_root),
                ),
            )
            result_payload = result_v2_payload()
            result_payload["global_config_digest"] = effective_config_digest(config)
            result_payload["tests"]["dltest"]["artifacts"] = str(
                run_dir / "not-artifacts"
            )
            result_path = root / "logs/job_logs/node-a/node-a-123/result.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(json.dumps(result_payload), encoding="utf-8")
            result = load_validation_result(result_path)
            authorization = authorize_result_write(
                result_path,
                result_digest=validation_result_digest(result),
                config_snapshot_b64=encode_config_snapshot(config),
                config=config,
            )

            with self.assertRaisesRegex(ValueError, "artifacts are not canonical"):
                validate_current_dl_write(
                    authorization,
                    node="node-a",
                    timestamp=123,
                    results_root=run_dir,
                    db_paths=dl_ingest_module.default_dl_metric_db_paths(config),
                )

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

    def test_only_missing_preserves_existing_run_and_adds_new_run(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            first = root / "dltest-node-a-1781649558"
            _write_rank_json(
                first / "workdir/test_plans/80gb-example/runs/first_RANK0.json",
                0,
            )
            ingest_dltest_results(root, output)
            with closing(
                sqlite3.connect(output / "dltest_compute_performance.db")
            ) as connection:
                connection.execute(
                    "UPDATE compute_performance SET metric_value=999 "
                    "WHERE run_key=? AND metric_name='fp_cpu_time'",
                    (first.name,),
                )
                connection.commit()
            second = root / "dltest-node-b-1781649559"
            _write_rank_json(
                second / "workdir/test_plans/80gb-example/runs/second_RANK0.json",
                0,
            )

            summary = ingest_dltest_results(root, output, only_missing=True)

            with closing(
                sqlite3.connect(output / "dltest_compute_performance.db")
            ) as connection:
                first_value = connection.execute(
                    "SELECT metric_value FROM compute_performance "
                    "WHERE run_key=? AND metric_name='fp_cpu_time'",
                    (first.name,),
                ).fetchone()[0]
                runs = connection.execute(
                    "SELECT COUNT(DISTINCT run_key) FROM compute_performance"
                ).fetchone()[0]

        self.assertEqual(summary["runs"], 1)
        self.assertEqual(summary["skipped_existing_runs"], 1)
        self.assertEqual(first_value, 999)
        self.assertEqual(runs, 2)

    def test_only_missing_receipts_mark_valid_zero_row_components_complete(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            rank_path = (
                run_dir
                / "workdir/test_plans/80gb-example/runs/example_RANK0.json"
            )
            _write_rank_json(rank_path, 0)
            payload = json.loads(rank_path.read_text(encoding="utf-8"))
            payload["coll_tasks"] = []
            payload["overlap_tasks"] = []
            rank_path.write_text(json.dumps(payload), encoding="utf-8")

            first = ingest_dltest_results(root, output, only_missing=True)
            second = ingest_dltest_results(root, output, only_missing=True)

            with closing(
                sqlite3.connect(output / "dltest_collective_performance.db")
            ) as connection:
                rows = connection.execute(
                    "SELECT COUNT(*) FROM collective_performance"
                ).fetchone()[0]
                receipts = connection.execute(
                    "SELECT COUNT(*) FROM cval_ingested_runs WHERE run_key=?",
                    (run_dir.name,),
                ).fetchone()[0]

        self.assertEqual(first["runs"], 1)
        self.assertEqual(second["runs"], 0)
        self.assertEqual(second["skipped_existing_runs"], 1)
        self.assertEqual(rows, 0)
        self.assertEqual(receipts, 1)

    def test_only_missing_empty_root_rejects_partial_legacy_db_set(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            root.mkdir()
            output.mkdir()
            with closing(
                sqlite3.connect(output / "dltest_compute_performance.db")
            ) as connection:
                connection.execute("CREATE TABLE placeholder(value INTEGER)")
                connection.commit()

            with self.assertRaisesRegex(ValueError, "all four metric DBs"):
                ingest_dltest_results(root, output, only_missing=True)

    def test_only_missing_repairs_existing_empty_receipt_tables(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            _write_rank_json(
                run_dir / "workdir/test_plans/80gb-example/runs/example_RANK0.json",
                0,
            )
            ingest_dltest_results(root, output)
            for db_path in output.glob("dltest_*.db"):
                with closing(sqlite3.connect(db_path)) as connection:
                    connection.execute("DELETE FROM cval_ingested_runs")
                    connection.commit()

            summary = ingest_dltest_results(root, output, only_missing=True)

            receipts = []
            migrations = []
            for db_path in output.glob("dltest_*.db"):
                with closing(sqlite3.connect(db_path)) as connection:
                    receipts.append(
                        connection.execute(
                            "SELECT COUNT(*) FROM cval_ingested_runs"
                        ).fetchone()[0]
                    )
                    migrations.append(
                        connection.execute(
                            "SELECT COUNT(*) FROM cval_ingest_migrations "
                            "WHERE name LIKE 'receipts-v1:%'"
                        ).fetchone()[0]
                    )

        self.assertEqual(summary["runs"], 1)
        self.assertEqual(summary["skipped_existing_runs"], 0)
        self.assertEqual(receipts, [1, 1, 1, 1])
        self.assertEqual(migrations, [1, 1, 1, 1])

    def test_receipt_migration_marker_prevents_repeated_table_backfill(self) -> None:
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "compute.db"
            row = StandardMetricRow(
                run_key="run-a",
                node="node-a",
                cval_timestamp=1,
                iterations=20,
                sample_dir="/evidence/run-a",
                test_plan="plan",
                dltest_run_id="rank0",
                rank=0,
                task_group="nn_tasks",
                task_name="task",
                status="completed",
                metric_name="fp_cpu_time",
                metric_value=1.0,
                source_file="/evidence/run-a/rank0.json",
            )
            dl_ingest_module.write_standard_db(
                db_path,
                "compute_performance",
                [row],
                replace_run_keys={"run-a"},
                generation_id="generation-a",
            )
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("DELETE FROM cval_ingested_runs")
                connection.commit()

            dl_ingest_module.write_standard_db(
                db_path,
                "compute_performance",
                [],
                generation_id="generation-b",
            )

            with closing(sqlite3.connect(db_path)) as connection:
                receipts = connection.execute(
                    "SELECT COUNT(*) FROM cval_ingested_runs"
                ).fetchone()[0]
                migrations = connection.execute(
                    "SELECT COUNT(*) FROM cval_ingest_migrations"
                ).fetchone()[0]

        self.assertEqual(receipts, 0)
        self.assertEqual(migrations, 1)

    def test_only_missing_empty_root_rejects_missing_metric_tables(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            root.mkdir()
            output.mkdir()
            for component in (
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            ):
                with closing(sqlite3.connect(output / f"dltest_{component}.db")) as connection:
                    connection.execute(
                        "CREATE TABLE cval_ingest_metadata ("
                        "id INTEGER PRIMARY KEY, generation_id TEXT, updated_at INTEGER)"
                    )
                    connection.execute(
                        "INSERT INTO cval_ingest_metadata VALUES (1, 'same', 1)"
                    )
                    connection.commit()

            with self.assertRaisesRegex(ValueError, "missing required table"):
                ingest_dltest_results(root, output, only_missing=True)

    def test_only_missing_preflights_all_schemas_before_generation_write(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            _write_rank_json(
                run_dir / "workdir/test_plans/80gb-example/runs/example_RANK0.json",
                0,
            )
            ingest_dltest_results(root, output)
            paths = sorted(output.glob("dltest_*.db"))
            generations = {}
            for path in paths:
                with closing(sqlite3.connect(path)) as connection:
                    generations[path.name] = connection.execute(
                        "SELECT generation_id FROM cval_ingest_metadata WHERE id=1"
                    ).fetchone()[0]
            overlap = output / "dltest_overlap_performance.db"
            with closing(sqlite3.connect(overlap)) as connection:
                connection.execute("ALTER TABLE overlap_performance RENAME TO broken")
                connection.commit()

            with self.assertRaisesRegex(ValueError, "missing required table"):
                ingest_dltest_results(root, output, only_missing=True)

            after = {}
            for path in paths[:-1]:
                with closing(sqlite3.connect(path)) as connection:
                    after[path.name] = connection.execute(
                        "SELECT generation_id FROM cval_ingest_metadata WHERE id=1"
                    ).fetchone()[0]

        self.assertEqual(after, {name: generations[name] for name in after})

    def test_only_missing_rejects_incomplete_metric_table_columns(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            root.mkdir()
            output.mkdir()
            for component in (
                "numerical_correctness",
                "compute_performance",
                "collective_performance",
                "overlap_performance",
            ):
                with closing(sqlite3.connect(output / f"dltest_{component}.db")) as connection:
                    connection.execute(
                        f'CREATE TABLE "{component}" (run_key TEXT, sample_dir TEXT)'
                    )
                    connection.commit()

            with self.assertRaisesRegex(ValueError, "missing required columns"):
                ingest_dltest_results(root, output, only_missing=True)

    def test_only_missing_rejects_duplicate_run_keys_across_directories(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            first = root / "dltest-node-a-1781649558"
            second = root / "nested/dltest-node-a-1781649558"
            for run_dir in (first, second):
                _write_rank_json(
                    run_dir / "workdir/test_plans/80gb-example/runs/example_RANK0.json",
                    0,
                )

            with self.assertRaisesRegex(ValueError, "Duplicate DL run key"):
                ingest_dltest_results(root, output, only_missing=True)

    def test_generation_remains_in_progress_until_all_batches_finish(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            for index in range(26):
                run_dir = root / f"dltest-node-{index:02d}-{1781649558 + index}"
                _write_rank_json(
                    run_dir
                    / "workdir/test_plans/80gb-example/runs/example_RANK0.json",
                    0,
                )
            original = dl_ingest_module._write_dl_rows
            observed = []

            def tracked(*args, **kwargs):
                original(*args, **kwargs)
                paths = args[0]
                with self.assertRaisesRegex(RuntimeError, "not complete"):
                    validate_dl_metric_generation(paths)
                observed.append(len(args[2]))

            with patch.object(dl_ingest_module, "_write_dl_rows", side_effect=tracked):
                summary = ingest_dltest_results(root, output, only_missing=True)

            paths = {
                component: output / f"dltest_{component}.db"
                for component in (
                    "numerical_correctness",
                    "compute_performance",
                    "collective_performance",
                    "overlap_performance",
                )
            }
            final_generation = validate_dl_metric_generation(paths)

        self.assertEqual(observed, [25, 1])
        self.assertEqual(final_generation, summary["generation_id"])

    def test_only_missing_refuses_to_finalize_divergent_receipts(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            _write_rank_json(
                run_dir / "workdir/test_plans/80gb-example/runs/example_RANK0.json",
                0,
            )
            ingest_dltest_results(root, output)
            with closing(
                sqlite3.connect(output / "dltest_compute_performance.db")
            ) as connection:
                connection.execute(
                    "INSERT INTO cval_ingested_runs VALUES (?, ?, ?)",
                    ("orphan", str(root / "missing"), 1),
                )
                connection.commit()

            with self.assertRaisesRegex(RuntimeError, "receipts"):
                ingest_dltest_results(root, output, only_missing=True)

            paths = {
                component: output / f"dltest_{component}.db"
                for component in (
                    "numerical_correctness",
                    "compute_performance",
                    "collective_performance",
                    "overlap_performance",
                )
            }
            with self.assertRaisesRegex(RuntimeError, "not complete"):
                validate_dl_metric_generation(paths)

    def test_only_missing_reports_deduplicated_stored_row_counts(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "results"
            output = Path(tmpdir) / "metadata"
            run_dir = root / "dltest-node-a-1781649558"
            runs_dir = run_dir / "workdir/test_plans/80gb-example/runs"
            first = runs_dir / "first_RANK0.json"
            second = runs_dir / "second_RANK0.json"
            _write_rank_json(first, 0)
            second.write_bytes(first.read_bytes())

            summary = ingest_dltest_results(root, output, only_missing=True)

            with closing(
                sqlite3.connect(output / "dltest_compute_performance.db")
            ) as connection:
                stored = connection.execute(
                    "SELECT COUNT(*) FROM compute_performance"
                ).fetchone()[0]

        self.assertEqual(summary["compute_performance_rows"], stored)

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
