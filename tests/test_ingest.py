from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
import shutil

from cval.storage.ingest import (
    NCCL_IB_PORT_COLUMNS,
    NCCL_LATEST_STATUS_VIEW,
    NCCL_RANKING_VIEW,
    add_nccl_health_from_summary as _add_nccl_health_from_summary,
    add_nccl_health_result as _add_nccl_health_result,
    add_storage_result as _add_storage_result,
    add_validation_run_results as _add_validation_run_results,
    add_validation_result as _add_validation_result,
)
from cval.config import encode_config_snapshot, load_config
from cval.storage.write_provenance import authorize_result_write
from cval.validation.results import load_validation_result, validation_result_digest


_TEST_AUTH_DIRECTORY = tempfile.TemporaryDirectory()
unittest.addModuleCleanup(_TEST_AUTH_DIRECTORY.cleanup)
_TEST_AUTH_COUNTER = 0


def _authorization(
    node,
    timestamp,
    *,
    statuses=None,
    image_name="",
    pytorch_version="",
    cuda_version="",
    operation="validation",
    db_path=None,
):
    global _TEST_AUTH_COUNTER
    _TEST_AUTH_COUNTER += 1
    statuses = statuses or {
        "storage": "pass",
        "nccl": "pass",
        "dltest": "pass",
    }
    overall = "pass" if all(value == "pass" for value in statuses.values()) else "fail"
    root = Path(_TEST_AUTH_DIRECTORY.name) / f"auth-{_TEST_AUTH_COUNTER}"
    run_id = f"{node}-{timestamp}"
    path = root / "logs/job_logs" / str(node) / run_id / "result.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "cval.results.v1",
                "node": str(node),
                "timestamp": str(timestamp),
                "overall": overall,
                "image_name": image_name,
                "pytorch_version": pytorch_version,
                "cuda_version": cuda_version,
                "tests": {
                    test_id: {"status": status}
                    for test_id, status in statuses.items()
                },
            }
        ),
        encoding="utf-8",
    )
    base = load_config()
    storage = base.storage
    if db_path is not None:
        field_name = {
            "validation": "validation_db_path",
            "storage": "storage_db_path",
            "nccl": "nccl_db_path",
        }[operation]
        storage = replace(storage, **{field_name: str(db_path)})
    config = replace(
        base,
        storage=storage,
        runtime=replace(base.runtime, validation_root=str(root)),
    )
    result = load_validation_result(path)
    return (
        authorize_result_write(
            path,
            result_digest=validation_result_digest(result),
            config_snapshot_b64=encode_config_snapshot(config),
            config=config,
        ),
        root,
    )


def add_validation_result(*args, **kwargs):
    node, test, result, timestamp = args[:4]
    statuses = {"storage": "pass", "nccl": "pass", "dltest": "pass"}
    if test in statuses:
        statuses[test] = result
    elif test == "all" and result != "pass":
        statuses["storage"] = "fail"
    kwargs["_authorization"], _ = _authorization(
        node,
        timestamp,
        statuses=statuses,
        image_name=kwargs.get("image_name", ""),
        pytorch_version=kwargs.get("pytorch_version", ""),
        cuda_version=kwargs.get("cuda_version", ""),
        operation="validation",
        db_path=kwargs.get("db_path"),
    )
    return _add_validation_result(*args, **kwargs)


def add_validation_run_results(*args, **kwargs):
    node, timestamp, results = args[:3]
    kwargs["_authorization"], _ = _authorization(
        node,
        timestamp,
        statuses={
            key: results.get(key, "incomplete")
            for key in ("storage", "nccl", "dltest")
        },
        image_name=kwargs.get("image_name", ""),
        pytorch_version=kwargs.get("pytorch_version", ""),
        cuda_version=kwargs.get("cuda_version", ""),
        operation="validation",
        db_path=kwargs.get("db_path"),
    )
    return _add_validation_run_results(*args, **kwargs)


def add_storage_result(*args, **kwargs):
    mutable_args = list(args)
    authorization, root = _authorization(
        args[0],
        args[1],
        image_name=kwargs.get("image_name", ""),
        operation="storage",
        db_path=kwargs.get("db_path"),
    )
    canonical = root / "validation_tests/storage/runs" / str(args[0]) / f"{args[0]}-{args[1]}" / "artifacts"
    shutil.copytree(Path(args[2]), canonical, symlinks=True)
    mutable_args[2] = canonical
    kwargs["_authorization"] = authorization
    return _add_storage_result(*mutable_args, **kwargs)


def add_nccl_health_result(*args, **kwargs):
    authorization, root = _authorization(
        args[0],
        args[1],
        image_name=kwargs.get("image_name", ""),
        pytorch_version=kwargs.get("pytorch_version", ""),
        cuda_version=kwargs.get("cuda_version", ""),
        operation="nccl",
        db_path=kwargs.get("db_path"),
    )
    summary_path = (
        root
        / "validation_tests/nccl/runs"
        / str(args[0])
        / f"{args[0]}-{args[1]}"
        / "summary.json"
    )
    summary_path.parent.mkdir(parents=True)
    sample_count = int(kwargs.get("samples") or 1)
    ports = {
        label: {
            "avg_gbps": float(value),
            "max_gbps": float(value),
            "last_gbps": float(value),
            "samples": sample_count,
        }
        for label, value in (kwargs.get("port_max_gbps") or {}).items()
        if value is not None
    }
    summary_path.write_text(
        json.dumps(
            {
                "GCR_ITERATIONS": int(kwargs.get("iterations") or 1),
                "GCR_BUSBW": float(kwargs.get("bus_bw") or 1.0),
                "GCR_ALGBW": 1.0,
                "GCR_LATENCY": float(kwargs.get("latency") or 1.0),
                "GCR_IB_PORT_BW_GBPS": ports,
            }
        ),
        encoding="utf-8",
    )
    if ports and kwargs.get("samples") is None:
        kwargs["samples"] = sample_count
    kwargs["summary_json_path"] = summary_path
    kwargs["_authorization"] = authorization
    return _add_nccl_health_result(*args, **kwargs)


def add_nccl_health_from_summary(*args, **kwargs):
    mutable_args = list(args)
    authorization, root = _authorization(
        args[0],
        args[1],
        image_name=kwargs.get("image_name", ""),
        pytorch_version=kwargs.get("pytorch_version", ""),
        cuda_version=kwargs.get("cuda_version", ""),
        operation="nccl",
        db_path=kwargs.get("db_path"),
    )
    canonical = root / "validation_tests/nccl/runs" / str(args[0]) / f"{args[0]}-{args[1]}" / "summary.json"
    canonical.parent.mkdir(parents=True)
    source = Path(args[2])
    if source.is_symlink():
        canonical.symlink_to(source.resolve())
    else:
        shutil.copy2(source, canonical)
    mutable_args[2] = canonical
    kwargs["_authorization"] = authorization
    return _add_nccl_health_from_summary(*mutable_args, **kwargs)

class IngestTests(unittest.TestCase):
    def test_v1_provenance_rejects_symlinked_result_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "validation"
            outside = Path(tmpdir) / "outside"
            outside.mkdir()
            root.mkdir()
            (root / "logs").symlink_to(outside, target_is_directory=True)
            result_path = outside / "job_logs/node-a/node-a-123/result.json"
            result_path.parent.mkdir(parents=True)
            result_path.write_text(
                json.dumps(
                    {
                        "schema_version": "cval.results.v1",
                        "node": "node-a",
                        "timestamp": "123",
                        "overall": "pass",
                        "tests": {
                            "storage": {"status": "pass"},
                            "nccl": {"status": "pass"},
                            "dltest": {"status": "pass"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = replace(
                load_config(),
                runtime=replace(load_config().runtime, validation_root=str(root)),
            )
            result = load_validation_result(root / "logs/job_logs/node-a/node-a-123/result.json")

            with self.assertRaisesRegex(ValueError, "symlink"):
                authorize_result_write(
                    root / "logs/job_logs/node-a/node-a-123/result.json",
                    result_digest=validation_result_digest(result),
                    config_snapshot_b64=encode_config_snapshot(config),
                    config=config,
                )

    def test_direct_low_level_writer_requires_result_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"

            with self.assertRaisesRegex(PermissionError, "validated result provenance"):
                _add_validation_result(
                    "node-a",
                    "all",
                    "pass",
                    123,
                    db_path=db_path,
                )

            self.assertFalse(db_path.exists())

    def test_v1_storage_rejects_symlinked_evidence_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "storage.db"
            authorization, root = _authorization(
                "node-a",
                123,
                operation="storage",
                db_path=db_path,
            )
            outside = Path(tmpdir) / "outside-storage"
            outside.mkdir()
            (root / "validation_tests").mkdir()
            (root / "validation_tests/storage").symlink_to(
                outside,
                target_is_directory=True,
            )
            evidence = (
                root
                / "validation_tests/storage/runs/node-a/node-a-123/artifacts"
            )
            evidence.mkdir(parents=True)

            with self.assertRaisesRegex(ValueError, "non-symlink"):
                _add_storage_result(
                    "node-a",
                    123,
                    evidence,
                    db_path=db_path,
                    _authorization=authorization,
                )

            self.assertFalse(db_path.exists())

    def test_v1_storage_rejects_symlinked_final_or_noncanonical_evidence(self) -> None:
        for mode in ("final-symlink", "noncanonical"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmpdir:
                db_path = Path(tmpdir) / "storage.db"
                authorization, root = _authorization(
                    "node-a",
                    123,
                    operation="storage",
                    db_path=db_path,
                )
                expected = (
                    root
                    / "validation_tests/storage/runs/node-a/node-a-123/artifacts"
                )
                if mode == "final-symlink":
                    outside = Path(tmpdir) / "outside-storage"
                    outside.mkdir()
                    expected.parent.mkdir(parents=True)
                    expected.symlink_to(outside, target_is_directory=True)
                    evidence = expected
                    error = "non-symlink"
                else:
                    evidence = root / "validation_tests/storage/other-artifacts"
                    evidence.mkdir(parents=True)
                    error = "not canonical"

                with self.assertRaisesRegex(ValueError, error):
                    _add_storage_result(
                        "node-a",
                        123,
                        evidence,
                        db_path=db_path,
                        _authorization=authorization,
                    )

                self.assertFalse(db_path.exists())

    def test_direct_writer_rejects_mismatched_authorized_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            authorization, _ = _authorization(
                "node-a",
                123,
                statuses={"storage": "pass", "nccl": "pass", "dltest": "pass"},
            )

            with self.assertRaisesRegex(ValueError, "identity"):
                _add_validation_result(
                    "different-node",
                    "all",
                    "fail",
                    999,
                    db_path=db_path,
                    _authorization=authorization,
                )

            self.assertFalse(db_path.exists())

    def test_legacy_writer_rejects_symlinked_database_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            external = root / "external.db"
            with closing(sqlite3.connect(external)) as connection:
                connection.execute("CREATE TABLE sentinel(value TEXT)")
                connection.commit()
            target = root / "validation.db"
            target.symlink_to(external)

            with self.assertRaisesRegex(ValueError, "symlink"):
                add_validation_result(
                    "node-a", "all", "pass", 123, db_path=target
                )
            with closing(sqlite3.connect(external)) as connection:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }

        self.assertEqual(tables, {"sentinel"})

    def test_add_validation_run_results_is_atomic_and_creates_latest_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            timestamp = add_validation_run_results(
                "node-a",
                123,
                {
                    "storage": "pass",
                    "nccl": "fail",
                    "dltest": "incomplete",
                    "all": "fail",
                },
                image_name="image",
                db_path=db_path,
            )
            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    "SELECT test, result FROM runs ORDER BY rowid"
                ).fetchall()
                latest = connection.execute(
                    "SELECT test, result FROM latest_status ORDER BY test"
                ).fetchall()

        self.assertEqual(timestamp, 123)
        self.assertEqual(len(rows), 4)
        self.assertEqual(latest, sorted(rows))

    def test_add_validation_run_results_rejects_before_writing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            with self.assertRaisesRegex(ValueError, "exactly storage"):
                add_validation_run_results(
                    "node-a",
                    123,
                    {"storage": "pass"},
                    db_path=db_path,
                )

        self.assertFalse(db_path.exists())

    def test_add_validation_run_results_is_idempotent_and_rejects_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            results = {
                "storage": "pass",
                "nccl": "pass",
                "dltest": "pass",
                "all": "pass",
            }
            for _ in range(2):
                add_validation_run_results(
                    "node-a",
                    123,
                    results,
                    image_name="image",
                    db_path=db_path,
                )
            with self.assertRaisesRegex(ValueError, "different run evidence"):
                add_validation_run_results(
                    "node-a",
                    123,
                    results | {"nccl": "fail", "all": "fail"},
                    image_name="image",
                    db_path=db_path,
                )
            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    "SELECT test, result FROM runs ORDER BY test"
                ).fetchall()

        self.assertEqual(len(rows), 4)
        self.assertEqual(dict(rows), results)

    def test_add_validation_run_results_rejects_duplicate_existing_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            results = {
                "storage": "pass",
                "nccl": "pass",
                "dltest": "pass",
                "all": "pass",
            }
            add_validation_run_results("node-a", 123, results, db_path=db_path)
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "INSERT INTO runs(node, test, timestamp, result) "
                    "VALUES ('node-a', 'storage', 123, 'pass')"
                )
                connection.commit()

            with self.assertRaisesRegex(ValueError, "different run evidence"):
                add_validation_run_results("node-a", 123, results, db_path=db_path)

    def test_add_validation_result_writes_run_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"

            timestamp = add_validation_result(
                "slc01-cl02-hgx-0001",
                "storage",
                "pass",
                "12345",
                image_name="pytorch:26.05-py3",
                pytorch_version="2.8.0a0+abc123",
                cuda_version="12.9",
                db_path=db_path,
            )

            self.assertEqual(timestamp, 12345)
            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, test, timestamp, result, image_name, pytorch_version, cuda_version FROM runs"
                ).fetchone()
            self.assertEqual(
                row,
                (
                    "slc01-cl02-hgx-0001",
                    "storage",
                    12345,
                    "pass",
                    "pytorch:26.05-py3",
                    "2.8.0a0+abc123",
                    "12.9",
                ),
            )

    def test_add_validation_result_migrates_existing_runs_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "validation.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    "CREATE TABLE runs ("
                    "node TEXT NOT NULL, "
                    "test TEXT NOT NULL, "
                    "timestamp INTEGER NOT NULL, "
                    "result TEXT NOT NULL CHECK (result IN ('pass','fail','incomplete'))"
                    ")"
                )
                connection.commit()

            add_validation_result(
                "slc01-cl02-hgx-0001",
                "all",
                "pass",
                "12345",
                image_name="pytorch:26.05-py3",
                pytorch_version="2.8.0a0+abc123",
                cuda_version="12.9",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                row = connection.execute(
                    "SELECT node, test, timestamp, result, image_name, pytorch_version, cuda_version FROM runs"
                ).fetchone()
            self.assertEqual(
                row,
                (
                    "slc01-cl02-hgx-0001",
                    "all",
                    12345,
                    "pass",
                    "pytorch:26.05-py3",
                    "2.8.0a0+abc123",
                    "12.9",
                ),
            )

    def test_add_storage_result_parses_fio_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_dir = root / "storage"
            results_dir.mkdir()
            (results_dir / "randread.json").write_text(
                json.dumps({"jobs": [{"read": {"iops": 10, "bw": 20}, "write": {"iops": 0, "bw": 0}}]}),
                encoding="utf-8",
            )
            for filename in (
                "iodepth_read_1file.json",
                "iodepth_write_1file.json",
                "numjobs_read_nfiles.json",
                "numjobs_write_nfiles.json",
                "randwrite.json",
            ):
                (results_dir / filename).write_text(
                    json.dumps({"jobs": [{"read": {"iops": 0, "bw": 0}, "write": {"iops": 0, "bw": 0}}]}),
                    encoding="utf-8",
                )
            db_path = root / "storage.db"

            add_storage_result(
                "slc01-cl02-hgx-0001",
                12345,
                results_dir,
                image_name="pytorch:26.05-py3",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                columns = {
                    item[1]
                    for item in connection.execute(
                        "PRAGMA table_info(storage_performance)"
                    )
                }
                row = connection.execute(
                    "SELECT node, timestamp, image_name, randread_iops, randread_bw "
                    "FROM storage_performance"
                ).fetchone()
            self.assertEqual(
                row,
                ("slc01-cl02-hgx-0001", 12345, "pytorch:26.05-py3", 10.0, 20.0),
            )
            self.assertNotIn("run_id", columns)

    def test_add_storage_result_rejects_non_finite_or_negative_metrics(self) -> None:
        for bad_value in (float("inf"), -1.0, True):
            with self.subTest(bad_value=bad_value), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                results_dir = root / "storage"
                results_dir.mkdir()
                (results_dir / "randread.json").write_text(
                    json.dumps(
                        {
                            "jobs": [
                                {
                                    "read": {"iops": bad_value, "bw": 1},
                                    "write": {"iops": 0, "bw": 0},
                                }
                            ]
                        }
                    ),
                    encoding="utf-8",
                )
                db_path = root / "storage.db"

                with self.assertRaisesRegex(ValueError, "numeric|finite"):
                    add_storage_result("node-a", 123, results_dir, db_path=db_path)

                self.assertFalse(db_path.exists())

    def test_add_storage_result_rejects_symlinked_metric_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_dir = root / "storage"
            results_dir.mkdir()
            external = root / "external.json"
            external.write_text(
                json.dumps({"jobs": [{"read": {"iops": 1, "bw": 1}, "write": {"iops": 0, "bw": 0}}]}),
                encoding="utf-8",
            )
            (results_dir / "randread.json").symlink_to(external)
            db_path = root / "storage.db"

            with self.assertRaisesRegex(ValueError, "non-symlink"):
                add_storage_result("node-a", 123, results_dir, db_path=db_path)

            self.assertFalse(db_path.exists())

    def test_storage_run_id_retry_rejects_changed_evidence_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_dir = root / "storage"
            results_dir.mkdir()
            for filename in (
                "iodepth_read_1file.json",
                "iodepth_write_1file.json",
                "numjobs_read_nfiles.json",
                "numjobs_write_nfiles.json",
                "randread.json",
                "randwrite.json",
            ):
                (results_dir / filename).write_text(
                    json.dumps({"jobs": [{"read": {"iops": 1, "bw": 2}, "write": {"iops": 0, "bw": 0}}]}),
                    encoding="utf-8",
                )
            db_path = root / "storage.db"
            add_storage_result(
                "node-a", 123, results_dir, run_id="node-a-123", db_path=db_path
            )
            (results_dir / "randread.json").write_text(
                json.dumps({"jobs": [{"read": {"iops": 777, "bw": 2}, "write": {"iops": 0, "bw": 0}}]}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "different run evidence"):
                add_storage_result(
                    "node-a", 123, results_dir, run_id="node-a-123", db_path=db_path
                )
            with closing(sqlite3.connect(db_path)) as connection:
                value = connection.execute(
                    "SELECT randread_iops FROM storage_performance"
                ).fetchone()[0]

        self.assertEqual(value, 1.0)

    def test_storage_rejects_unexpected_seventh_json_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_dir = root / "storage"
            results_dir.mkdir()
            for filename in (
                "iodepth_read_1file.json",
                "iodepth_write_1file.json",
                "numjobs_read_nfiles.json",
                "numjobs_write_nfiles.json",
                "randread.json",
                "randwrite.json",
                "unexpected.json",
            ):
                (results_dir / filename).write_text(
                    json.dumps({"jobs": [{"read": {"iops": 0, "bw": 0}, "write": {"iops": 0, "bw": 0}}]}),
                    encoding="utf-8",
                )

            with self.assertRaisesRegex(ValueError, "unexpected JSON"):
                add_storage_result(
                    "node-a", 123, results_dir, db_path=root / "storage.db"
                )

    def test_storage_rejects_jobs_without_required_metric_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results_dir = root / "storage"
            results_dir.mkdir()
            for filename in (
                "iodepth_read_1file.json",
                "iodepth_write_1file.json",
                "numjobs_read_nfiles.json",
                "numjobs_write_nfiles.json",
                "randread.json",
                "randwrite.json",
            ):
                (results_dir / filename).write_text(
                    json.dumps({"jobs": [{}]}), encoding="utf-8"
                )

            with self.assertRaisesRegex(ValueError, "read/write"):
                add_storage_result(
                    "node-a", 123, results_dir, db_path=root / "storage.db"
                )

            self.assertFalse((root / "storage.db").exists())

class NcclHealthIngestTests(unittest.TestCase):
    def test_v1_nccl_rejects_symlinked_evidence_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            authorization, root = _authorization(
                "node-a",
                123,
                operation="nccl",
                db_path=db_path,
            )
            outside = Path(tmpdir) / "outside-nccl"
            outside.mkdir()
            (root / "validation_tests").mkdir()
            (root / "validation_tests/nccl").symlink_to(
                outside,
                target_is_directory=True,
            )
            summary = (
                root
                / "validation_tests/nccl/runs/node-a/node-a-123/summary.json"
            )
            summary.parent.mkdir(parents=True)
            summary.write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "non-symlink"):
                _add_nccl_health_from_summary(
                    "node-a",
                    123,
                    summary,
                    db_path=db_path,
                    _authorization=authorization,
                )

            self.assertFalse(db_path.exists())

    def test_add_nccl_health_writes_one_wide_row_with_port_maxima(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            summary = Path(tmpdir) / "nccl-summary.json"
            summary.write_text(
                json.dumps(
                    {
                        "GCR_ITERATIONS": 20,
                        "GCR_BUSBW": 44.5,
                        "GCR_ALGBW": 25.4,
                        "GCR_LATENCY": 628.2,
                        "GCR_IB_PORT_BW_GBPS": {
                            "mlx5_0": {
                                "avg_gbps": 20.0,
                                "max_gbps": 46.1,
                                "last_gbps": 45.9,
                                "samples": 26,
                            },
                            "mlx5_13": {
                                "avg_gbps": 20.2,
                                "max_gbps": 46.3,
                                "last_gbps": 46.0,
                                "samples": 26,
                            },
                            "mlx5_5.2": {
                                "avg_gbps": 50.0,
                                "max_gbps": 99.0,
                                "last_gbps": 98.0,
                                "samples": 26,
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            add_nccl_health_from_summary(
                "slc01-cl02-hgx-0001",
                "12345",
                summary,
                image_name="pytorch:26.05-py3",
                cuda_version="13.0",
                pytorch_version="2.9.0",
                db_path=db_path,
            )

            with closing(sqlite3.connect(db_path)) as connection:
                columns = [row[1] for row in connection.execute("PRAGMA table_info(IB_HEALTH)")]
                row = connection.execute(
                    "SELECT Node, timestamp, la_timestamp, iterations, image_name, cuda, pytorch, "
                    "samples, BUS_BW, LATENCY, mlx5_0, mlx5_13 FROM IB_HEALTH"
                ).fetchone()

        self.assertEqual(columns[-14:], list(NCCL_IB_PORT_COLUMNS))
        self.assertEqual(row[0:2], ("slc01-cl02-hgx-0001", 12345))
        self.assertIn("-08:00", row[2])
        self.assertEqual(row[3:10], (20, "pytorch:26.05-py3", "13.0", "2.9.0", 26, 44.5, 628.2))
        self.assertEqual(row[10:], (46.1, 46.3))

    def test_latest_status_and_five_run_ranking_views(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "nccl.db"
            for index, (bus_bw, latency) in enumerate(
                ((10, 60), (20, 50), (30, 40), (40, 30), (50, 20), (60, 10)),
                start=1,
            ):
                add_nccl_health_result(
                    "node-a",
                    100 + index,
                    iterations=20,
                    bus_bw=bus_bw,
                    latency=latency,
                    port_max_gbps={"mlx5_0": index},
                    db_path=db_path,
                )
            for index, (bus_bw, latency) in enumerate(
                ((70, 9), (80, 8), (90, 7)),
                start=1,
            ):
                add_nccl_health_result(
                    "node-b",
                    200 + index,
                    iterations=20,
                    bus_bw=bus_bw,
                    latency=latency,
                    port_max_gbps={"mlx5_0": 6 + index},
                    db_path=db_path,
                )

            with closing(sqlite3.connect(db_path)) as connection:
                latest = connection.execute(
                    f"SELECT Node, timestamp, BUS_BW FROM {NCCL_LATEST_STATUS_VIEW} "
                    "ORDER BY Node"
                ).fetchall()
                ranking = connection.execute(
                    f"SELECT node, bus_bw, bus_bw_pctl, latency, latency_pctl, mlx5_0 "
                    f"FROM {NCCL_RANKING_VIEW}"
                ).fetchall()
                views = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'view'"
                    )
                }

        self.assertEqual(latest, [("node-a", 106, 60.0), ("node-b", 203, 90.0)])
        self.assertEqual([row[0] for row in ranking], ["node-a", "node-b"])
        self.assertEqual(ranking[0], ("node-a", 40.0, 0.0, 30.0, 100.0, 4.0))
        self.assertEqual(ranking[1], ("node-b", 80.0, 100.0, 8.0, 0.0, 8.0))
        self.assertEqual(views, {NCCL_LATEST_STATUS_VIEW, NCCL_RANKING_VIEW})

    def test_add_nccl_health_rejects_invalid_summary_before_db_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = Path(tmpdir) / "summary.json"
            summary.write_text(
                json.dumps(
                    {
                        "GCR_ITERATIONS": 20,
                        "GCR_BUSBW": 44.5,
                        "GCR_LATENCY": 1.0,
                    }
                ),
                encoding="utf-8",
            )
            db_path = Path(tmpdir) / "nccl.db"

            with self.assertRaisesRegex(ValueError, "GCR_ALGBW"):
                add_nccl_health_from_summary(
                    "node-a",
                    123,
                    summary,
                    db_path=db_path,
                )

            self.assertFalse(db_path.exists())

    def test_nccl_rejects_odd_port_values_before_db_write(self) -> None:
        for field, bad_value in (
            ("max_gbps", float("inf")),
            ("max_gbps", -1.0),
            ("samples", True),
            ("samples", -1),
        ):
            with self.subTest(field=field, bad_value=bad_value), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                summary = root / "summary.json"
                port = {
                    "avg_gbps": 9.0,
                    "max_gbps": 10.0,
                    "last_gbps": 8.0,
                    "samples": 2,
                }
                port[field] = bad_value
                summary.write_text(
                    json.dumps(
                        {
                            "GCR_ITERATIONS": 20,
                            "GCR_BUSBW": 44.5,
                            "GCR_ALGBW": 25.4,
                            "GCR_LATENCY": 628.2,
                            "GCR_IB_PORT_BW_GBPS": {"mlx5_0": port},
                        }
                    ),
                    encoding="utf-8",
                )
                db_path = root / "nccl.db"

                with self.assertRaisesRegex(ValueError, "finite|integer|non-negative"):
                    add_nccl_health_from_summary(
                        "node-a", 123, summary, db_path=db_path
                    )

                self.assertFalse(db_path.exists())

    def test_nccl_rejects_symlinked_summary_before_db_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            external = root / "external.json"
            external.write_text("{}", encoding="utf-8")
            summary = root / "summary.json"
            summary.symlink_to(external)
            db_path = root / "nccl.db"

            with self.assertRaisesRegex(ValueError, "non-symlink"):
                add_nccl_health_from_summary("node-a", 123, summary, db_path=db_path)

            self.assertFalse(db_path.exists())

    def test_nccl_rejects_non_integer_iteration_identity(self) -> None:
        for bad_value in (20.9, "20", True):
            with self.subTest(bad_value=bad_value), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                summary = root / "summary.json"
                summary.write_text(
                    json.dumps(
                        {
                            "GCR_ITERATIONS": bad_value,
                            "GCR_BUSBW": 44.5,
                            "GCR_ALGBW": 25.4,
                            "GCR_LATENCY": 628.2,
                            "GCR_IB_PORT_BW_GBPS": {},
                        }
                    ),
                    encoding="utf-8",
                )
                db_path = root / "nccl.db"

                with self.assertRaisesRegex(ValueError, "iterations"):
                    add_nccl_health_from_summary(
                        "node-a", 123, summary, db_path=db_path
                    )

                self.assertFalse(db_path.exists())

    def test_nccl_run_id_retry_rejects_changed_evidence_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            summary = root / "summary.json"
            payload = {
                "GCR_ITERATIONS": 20,
                "GCR_BUSBW": 44.5,
                "GCR_ALGBW": 25.4,
                "GCR_LATENCY": 628.2,
                "GCR_IB_PORT_BW_GBPS": {},
            }
            summary.write_text(json.dumps(payload), encoding="utf-8")
            db_path = root / "nccl.db"
            add_nccl_health_from_summary(
                "node-a", 123, summary, run_id="node-a-123", db_path=db_path
            )
            payload["GCR_BUSBW"] = 777.0
            summary.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "different run evidence"):
                add_nccl_health_from_summary(
                    "node-a", 123, summary, run_id="node-a-123", db_path=db_path
                )
            with closing(sqlite3.connect(db_path)) as connection:
                value = connection.execute(
                    "SELECT BUS_BW FROM IB_HEALTH"
                ).fetchone()[0]

        self.assertEqual(value, 44.5)

    def test_nccl_rejects_distinct_run_id_at_same_node_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            summary = root / "summary.json"
            summary.write_text(
                json.dumps(
                    {
                        "GCR_ITERATIONS": 20,
                        "GCR_BUSBW": 44.5,
                        "GCR_ALGBW": 25.4,
                        "GCR_LATENCY": 628.2,
                        "GCR_IB_PORT_BW_GBPS": {},
                    }
                ),
                encoding="utf-8",
            )
            db_path = root / "nccl.db"
            add_nccl_health_from_summary(
                "node-a", 123, summary, run_id="node-a-123", db_path=db_path
            )

            with self.assertRaisesRegex(ValueError, "run ID|different run evidence"):
                add_nccl_health_from_summary(
                    "node-a", 123, summary, run_id="run-b", db_path=db_path
                )

    def test_nccl_requires_hca_samples_when_ibbw_is_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = Path(tmpdir) / "summary.json"
            summary.write_text(
                json.dumps(
                    {
                        "GCR_ITERATIONS": 20,
                        "GCR_BUSBW": 44.5,
                        "GCR_ALGBW": 25.4,
                        "GCR_LATENCY": 628.2,
                        "GCR_IB_PORT_BW_GBPS": {},
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "sampled"):
                add_nccl_health_from_summary(
                    "node-a",
                    123,
                    summary,
                    require_hca_samples=True,
                    db_path=Path(tmpdir) / "nccl.db",
                )

    def test_nccl_rejects_incomplete_or_invalid_port_schema(self) -> None:
        for port in (
            {"max_gbps": 10.0, "samples": 2},
            {
                "avg_gbps": float("inf"),
                "max_gbps": 10.0,
                "last_gbps": 8.0,
                "samples": 2,
            },
        ):
            with self.subTest(port=port), tempfile.TemporaryDirectory() as tmpdir:
                summary = Path(tmpdir) / "summary.json"
                summary.write_text(
                    json.dumps(
                        {
                            "GCR_ITERATIONS": 20,
                            "GCR_BUSBW": 44.5,
                            "GCR_ALGBW": 25.4,
                            "GCR_LATENCY": 628.2,
                            "GCR_IB_PORT_BW_GBPS": {"mlx5_0": port},
                        }
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ValueError, "fields|finite"):
                    add_nccl_health_from_summary(
                        "node-a", 123, summary, db_path=Path(tmpdir) / "nccl.db"
                    )

    def test_nccl_rejects_malformed_additional_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = Path(tmpdir) / "summary.json"
            summary.write_text(
                json.dumps(
                    {
                        "GCR_ITERATIONS": 20,
                        "GCR_BUSBW": 44.5,
                        "GCR_ALGBW": 25.4,
                        "GCR_LATENCY": 628.2,
                        "GCR_IB_PORT_BW_GBPS": {
                            "mlx5_0": {
                                "avg_gbps": 9.0,
                                "max_gbps": 10.0,
                                "last_gbps": 8.0,
                                "samples": 2,
                            },
                            "mlx5_5.2": {"max_gbps": 99.0},
                        },
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "mlx5_5.2 fields"):
                add_nccl_health_from_summary(
                    "node-a", 123, summary, db_path=Path(tmpdir) / "nccl.db"
                )

    def test_nccl_rejects_noncanonical_port_suffix(self) -> None:
        for label in ("mlx5_0.0", "mlx5_0.1", "mlx5_00", "mlx5_٠"):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmpdir:
                summary = Path(tmpdir) / "summary.json"
                summary.write_text(
                    json.dumps(
                        {
                            "GCR_ITERATIONS": 20,
                            "GCR_BUSBW": 44.5,
                            "GCR_ALGBW": 25.4,
                            "GCR_LATENCY": 628.2,
                            "GCR_IB_PORT_BW_GBPS": {
                                label: {
                                    "avg_gbps": 9.0,
                                    "max_gbps": 10.0,
                                    "last_gbps": 8.0,
                                    "samples": 2,
                                }
                            },
                        }
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ValueError, "invalid HCA port label"):
                    add_nccl_health_from_summary(
                        "node-a", 123, summary, db_path=Path(tmpdir) / "nccl.db"
                    )


if __name__ == "__main__":
    unittest.main()