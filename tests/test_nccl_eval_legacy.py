from __future__ import annotations

import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cval.nccl_eval.legacy import (
    LegacyProfileMetadata,
    legacy_summary,
    read_legacy_batches,
)
from cval.nccl_eval.models import ResultStatus


class NcclEvalLegacyTests(unittest.TestCase):
    def metadata(self, **overrides: object) -> LegacyProfileMetadata:
        values: dict[str, object] = {
            "test_definition_version": "legacy-import-v1",
            "gpu_model": "B200",
            "gpus_per_node": 8,
            "compiled_nccl_version": "2.27",
            "runtime_nccl_package_version": "2.27.7",
            "driver_version": "600.1",
            "driver_version_group": "r600",
            "topology_class": "8gpu-loopback-v1",
            "source_commit": "legacy:unknown-commit",
            "image_digest": "legacy:unknown-image",
            "implementation_identity": "legacy:ib-health-import-v1",
            "cval_result_digest": "legacy:sqlite-row",
            "runtime_evidence_sha256": "legacy:supplied-profile-metadata",
            "test_config": {
                "collective": "all_reduce",
                "datatype": "bfloat16",
                "reduction": "sum",
                "message_size": "16GiB",
                "warmup_iterations": 1,
            },
        }
        values.update(overrides)
        return LegacyProfileMetadata(**values)  # type: ignore[arg-type]

    def create_db(self, root: Path, *, empty_cuda: bool = False) -> Path:
        path = root / "copied-test-nccl.db"
        connection = sqlite3.connect(path)
        try:
            connection.execute(
                """
                CREATE TABLE IB_HEALTH (
                    Node TEXT, timestamp INTEGER, la_timestamp TEXT,
                    iterations INTEGER, image_name TEXT, cuda TEXT, pytorch TEXT,
                    samples INTEGER, BUS_BW REAL, LATENCY REAL,
                    mlx5_0 REAL, mlx5_4 REAL
                )
                """
            )
            connection.executemany(
                "INSERT INTO IB_HEALTH VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        "node-a", 1_700_000_000, "2023-11-14T14:13:20-08:00",
                        20, "image:a", "" if empty_cuda else "13.2", "2.12",
                        20, 44.0, 628.2, 40.0, 41.0,
                    ),
                    (
                        "node-b", 1_700_000_000, "2023-11-14T14:13:20-08:00",
                        20, "image:a", "" if empty_cuda else "13.2", "2.12",
                        20, 43.0, 610.0, 39.0, None,
                    ),
                    (
                        "node-c", 1_700_000_100, "2023-11-14T22:15:00+00:00",
                        20, "image:a", "" if empty_cuda else "13.2", "2.12",
                        20, None, None, None, None,
                    ),
                ],
            )
            connection.commit()
        finally:
            connection.close()
        return path

    def test_normalizes_shared_runs_nodes_and_mlx_columns_to_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self.create_db(Path(tmpdir))
            batches = read_legacy_batches(source, self.metadata())
            summary = legacy_summary(batches, source)

        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0].node_results), 2)
        self.assertEqual(batches[0].node_results[0].latency_us, 628200.0)
        self.assertEqual(batches[0].test_run.test_config["latency_unit"], "us")
        self.assertEqual(batches[0].test_run.test_config["latency_source_unit"], "ms")
        self.assertEqual(
            batches[0].test_run.test_config["latency_conversion"],
            "ms_to_us_x1000",
        )
        self.assertEqual(
            [nic.device_name for nic in batches[0].node_results[0].nics],
            ["mlx5_0", "mlx5_4"],
        )
        self.assertEqual(
            [nic.device_name for nic in batches[0].node_results[1].nics],
            ["mlx5_0"],
        )
        self.assertEqual(batches[1].node_results[0].result_status, ResultStatus.NO_RESULT)
        self.assertEqual(summary["node_result_count"], 3)
        self.assertEqual(summary["nic_result_count"], 3)
        self.assertEqual(summary["calibration_decision_count"], 0)
        self.assertEqual(summary["latency_conversion"], "ms_to_us_x1000")

    def test_deterministic_run_ids_do_not_depend_on_copy_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = self.create_db(root)
            copy = root / "second-copy.db"
            shutil.copy2(source, copy)
            first = read_legacy_batches(source, self.metadata())
            second = read_legacy_batches(copy, self.metadata())

        self.assertEqual(
            [batch.test_run.run_id for batch in first],
            [batch.test_run.run_id for batch in second],
        )
        self.assertEqual(first, second)

    def test_missing_environment_metadata_is_rejected_not_guessed(self) -> None:
        required = {
            "test_definition_version": "v1",
            "gpu_model": "B200",
            "gpus_per_node": 8,
            "compiled_nccl_version": "2.27",
            "runtime_nccl_package_version": "2.27.7",
            "driver_version": "600.1",
            "driver_version_group": "r600",
            "topology_class": "loopback",
            "source_commit": "legacy:unknown-commit",
            "image_digest": "legacy:unknown-image",
            "implementation_identity": "legacy:ib-health-import-v1",
            "cval_result_digest": "legacy:sqlite-row",
            "runtime_evidence_sha256": "legacy:supplied-profile-metadata",
            "test_config": {
                "collective": "all_reduce",
                "datatype": "bfloat16",
                "reduction": "sum",
                "message_size": "16GiB",
                "warmup_iterations": 1,
            },
        }
        for key in tuple(required):
            payload = dict(required)
            payload.pop(key)
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "requires explicit"):
                LegacyProfileMetadata.from_dict(payload)

    def test_missing_row_cuda_requires_explicit_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self.create_db(Path(tmpdir), empty_cuda=True)
            with self.assertRaisesRegex(ValueError, "cuda is missing"):
                read_legacy_batches(source, self.metadata())
            batches = read_legacy_batches(
                source, self.metadata(cuda_version="13.2", pytorch_version="2.12")
            )
        self.assertTrue(all(batch.test_run.cuda_version == "13.2" for batch in batches))

    def test_metadata_versions_are_fallback_only_and_mixed_rows_do_not_merge(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self.create_db(Path(tmpdir), empty_cuda=True)
            connection = sqlite3.connect(source)
            try:
                connection.execute(
                    "UPDATE IB_HEALTH SET cuda = '13.3', pytorch = '2.13' WHERE Node = 'node-a'"
                )
                connection.commit()
            finally:
                connection.close()
            batches = read_legacy_batches(
                source,
                self.metadata(cuda_version="13.2", pytorch_version="2.12"),
            )

        variants: dict[tuple[str, str], set[str]] = {}
        for batch in batches:
            key = (batch.test_run.cuda_version, batch.test_run.pytorch_version)
            variants.setdefault(key, set()).update(
                node.node_name for node in batch.node_results
            )
        self.assertEqual(variants[("13.3", "2.13")], {"node-a"})
        self.assertEqual(variants[("13.2", "2.12")], {"node-b", "node-c"})
        self.assertEqual(len(batches), 3)

    def test_exact_duplicate_rows_deduplicate_but_differing_duplicates_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self.create_db(Path(tmpdir))
            connection = sqlite3.connect(source)
            try:
                original = connection.execute(
                    "SELECT * FROM IB_HEALTH WHERE Node = 'node-a'"
                ).fetchone()
                connection.execute(
                    "INSERT INTO IB_HEALTH VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    original,
                )
                connection.commit()
            finally:
                connection.close()
            batches = read_legacy_batches(source, self.metadata())
            self.assertEqual(
                sum(node.node_name == "node-a" for batch in batches for node in batch.node_results),
                1,
            )

            connection = sqlite3.connect(source)
            try:
                changed = list(original)
                changed[8] = 1.0
                connection.execute(
                    "INSERT INTO IB_HEALTH VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    changed,
                )
                connection.commit()
            finally:
                connection.close()
            with self.assertRaisesRegex(ValueError, "differing duplicate"):
                read_legacy_batches(source, self.metadata())

    def test_malformed_nic_column_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "malformed.db"
            connection = sqlite3.connect(source)
            try:
                connection.execute(
                    """
                    CREATE TABLE IB_HEALTH (
                        Node TEXT, timestamp INTEGER, iterations INTEGER,
                        image_name TEXT, cuda TEXT, pytorch TEXT, samples INTEGER,
                        BUS_BW REAL, LATENCY REAL, mlx5_bad REAL
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO IB_HEALTH VALUES (?,?,?,?,?,?,?,?,?,?)",
                    ("node-a", 1700000000, 20, "image:a", "13.2", "2.12", 20, 44, 1, 1),
                )
                connection.commit()
            finally:
                connection.close()
            with self.assertRaisesRegex(ValueError, "malformed NIC"):
                read_legacy_batches(source, self.metadata())

    def test_case_colliding_columns_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "case-collision.db"
            connection = sqlite3.connect(source)
            try:
                connection.execute(
                    """
                    CREATE TABLE source_rows (
                        Node TEXT, timestamp INTEGER, iterations INTEGER,
                        image_name TEXT, cuda TEXT, pytorch TEXT, samples INTEGER,
                        BUS_BW REAL, LATENCY REAL, nic REAL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE VIEW IB_HEALTH AS
                    SELECT Node, timestamp, iterations, image_name, cuda, pytorch,
                           samples, BUS_BW, LATENCY,
                           nic AS mlx5_0, nic AS MLX5_0
                    FROM source_rows
                    """
                )
                connection.execute(
                    "INSERT INTO source_rows VALUES (?,?,?,?,?,?,?,?,?,?)",
                    ("node-a", 1700000000, 20, "image:a", "13.2", "2.12", 20, 44, 1, 1),
                )
                connection.commit()
            finally:
                connection.close()
            with self.assertRaisesRegex(ValueError, "case-colliding"):
                read_legacy_batches(source, self.metadata())

    def test_legacy_rows_have_no_embedded_calibration_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source = self.create_db(Path(tmpdir))
            batches = read_legacy_batches(source, self.metadata())
        self.assertTrue(batches)
        self.assertTrue(
            all("baseline_approved" not in node.to_dict() for batch in batches for node in batch.node_results)
        )


if __name__ == "__main__":
    unittest.main()
