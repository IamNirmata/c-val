from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from cval.cli import main
from cval.nccl_eval.models import IngestionBatch, NodeResult
from tests.test_nccl_eval_core import test_run


class NcclEvalCliTests(unittest.TestCase):
    def input_file(self, root: Path) -> Path:
        batch = IngestionBatch(
            test_run(),
            (
                NodeResult(
                    "node-a",
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                    44.0,
                    600.0,
                ),
            ),
        )
        path = root / "batch.json"
        path.write_text(json.dumps(batch.to_dict()), encoding="utf-8")
        return path

    def test_schema_and_ingest_dry_runs_need_neither_database_url_nor_psycopg(self) -> None:
        schema_output = io.StringIO()
        ingest_output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_path = self.input_file(Path(tmpdir))
            with patch.dict(
                sys.modules, {"psycopg": None, "psycopg_pool": None}
            ), patch.dict(os.environ, {}, clear=False):
                os.environ.pop("DATABASE_URL", None)
                with redirect_stdout(schema_output):
                    schema_code = main(["nccl-eval", "schema", "--output", "json"])
                with redirect_stdout(ingest_output):
                    ingest_code = main(
                        [
                            "nccl-eval",
                            "ingest",
                            "--input",
                            str(batch_path),
                            "--output",
                            "json",
                        ]
                    )
                base_output = io.StringIO()
                with redirect_stdout(base_output):
                    base_code = main(["tests", "list", "--output", "json"])

        self.assertEqual(schema_code, 0)
        self.assertEqual(ingest_code, 0)
        self.assertEqual(base_code, 0)
        self.assertEqual(json.loads(schema_output.getvalue())["mode"], "dry-run")
        self.assertTrue(json.loads(ingest_output.getvalue())["valid"])
        self.assertEqual(len(json.loads(base_output.getvalue())), 3)

    def test_every_mutation_rejects_wrong_confirmation_before_connect(self) -> None:
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            batch_path = self.input_file(root)
            sqlite_path = root / "copy.db"
            sqlite_path.write_bytes(b"not-read-because-gate-fails")
            commands = (
                ["nccl-eval", "schema", "--apply", "--confirm", "wrong"],
                ["nccl-eval", "grant-runtime", "--apply", "--confirm", "wrong"],
                [
                    "nccl-eval", "ingest", "--input", str(batch_path),
                    "--apply", "--confirm", "wrong",
                ],
                [
                    "nccl-eval", "migrate-legacy", "--sqlite", str(sqlite_path),
                    "--apply", "--confirm", "wrong",
                ],
                ["nccl-eval", "build-baselines", "--apply", "--confirm", "wrong"],
                ["nccl-eval", "evaluate", "--apply", "--confirm", "wrong"],
                ["nccl-eval", "recover", "--apply", "--confirm", "wrong"],
                ["nccl-eval", "worker", "--apply", "--confirm", "wrong"],
                [
                    "nccl-eval", "resident",
                    "--outbox-root", str(root),
                    "--apply", "--confirm", "wrong",
                ],
                [
                    "nccl-eval", "emit-outbox",
                    "--result-json", str(batch_path),
                    "--summary", str(batch_path),
                    "--runtime-evidence", str(batch_path),
                    "--outbox-root", str(root),
                    "--apply", "--confirm", "wrong",
                ],
                [
                    "nccl-eval", "ingest-outbox",
                    "--outbox-root", str(root),
                    "--apply", "--confirm", "wrong",
                ],
                [
                    "nccl-eval", "commit-outbox",
                    "--outbox-root", str(root),
                    "--pending", str(batch_path),
                    "--result-digest", "sha256:" + "a" * 64,
                    "--apply", "--confirm", "wrong",
                ],
                [
                    "nccl-eval", "calibration", "apply",
                    "--input", str(batch_path),
                    "--apply", "--confirm", "wrong",
                ],
            )
            with patch("cval.nccl_eval.repository.create_pool") as create_pool:
                for command in commands:
                    with self.subTest(command=command), redirect_stderr(stderr):
                        self.assertEqual(main(command), 2)
                create_pool.assert_not_called()
        self.assertIn("Policy violation", stderr.getvalue())

    def test_outbox_scanner_dry_run_needs_no_database_or_psycopg(self) -> None:
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(
            sys.modules, {"psycopg": None, "psycopg_pool": None}
        ), patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DATABASE_URL", None)
            root = Path(tmpdir) / "missing-outbox"
            with redirect_stdout(output):
                code = main(
                    [
                        "nccl-eval",
                        "ingest-outbox",
                        "--outbox-root",
                        str(root),
                        "--limit",
                        "25",
                        "--output",
                        "json",
                    ]
                )

        self.assertEqual(code, 0)
        payload = json.loads(output.getvalue())
        self.assertFalse(payload["root_exists"])
        self.assertEqual(payload["valid_count"], 0)

    def test_wrong_schema_confirmation_precedes_malformed_full_config_and_imports(self) -> None:
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            malformed = Path(tmpdir) / "broken.toml"
            malformed.write_text("this = [is not toml", encoding="utf-8")
            with patch("cval.cli.load_config") as load_config, patch(
                "cval.nccl_eval.repository.create_pool"
            ) as create_pool, redirect_stderr(stderr):
                code = main(
                    [
                        "--config",
                        str(malformed),
                        "nccl-eval",
                        "schema",
                        "--apply",
                        "--confirm",
                        "wrong",
                    ]
                )
        self.assertEqual(code, 2)
        load_config.assert_not_called()
        create_pool.assert_not_called()
        self.assertIn("Policy violation", stderr.getvalue())

    def test_exact_gate_reaches_config_but_fails_cleanly_without_database_url(self) -> None:
        stderr = io.StringIO()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DATABASE_URL", None)
            with redirect_stderr(stderr):
                code = main(
                    ["nccl-eval", "schema", "--apply", "--confirm", "schema"]
                )
        self.assertEqual(code, 2)
        self.assertIn("DATABASE_URL is required", stderr.getvalue())

    def test_database_credentials_are_redacted_from_errors(self) -> None:
        secret = "postgresql://private-user:private-password@db.example/cval"
        stderr = io.StringIO()
        with patch.dict(os.environ, {"DATABASE_URL": secret}), patch(
            "cval.nccl_eval.service.status",
            side_effect=RuntimeError(f"connection failed for {secret}"),
        ), redirect_stderr(stderr):
            code = main(["nccl-eval", "status"])
        self.assertEqual(code, 2)
        self.assertNotIn("private-user", stderr.getvalue())
        self.assertNotIn("private-password", stderr.getvalue())
        self.assertIn("REDACTED", stderr.getvalue())

    def test_configured_legacy_source_requires_separate_exact_allowance(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "configured.db"
            source.write_bytes(b"must-not-be-opened")
            config_path = root / "cval.toml"
            config_path.write_text(
                f'[storage]\nnccl_db_path = "{source}"\n', encoding="utf-8"
            )
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                code = main(
                    [
                        "--config", str(config_path), "nccl-eval", "migrate-legacy",
                        "--sqlite", str(source),
                    ]
                )
        self.assertEqual(code, 2)
        self.assertIn("copied-sqlite", stderr.getvalue())

    def test_worker_id_is_bounded_and_nonsecret(self) -> None:
        stderr = io.StringIO()
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://u:p@db/cval"}), patch(
            "cval.nccl_eval.service.create_pool"
        ) as create_pool, redirect_stderr(stderr):
            code = main(
                [
                    "nccl-eval", "evaluate", "--apply", "--confirm", "evaluate",
                    "--worker-id", "bad worker id",
                ]
            )
        self.assertEqual(code, 2)
        create_pool.assert_not_called()
        self.assertIn("worker_id", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
