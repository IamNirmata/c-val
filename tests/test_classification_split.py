from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from cval.baselines.storage import store_classification_results
from cval.config import load_config
from cval.storage.classification_status import latest_classification_rows_from_db
from cval.storage.classification_status import latest_classification_rows_from_dbs

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts/cval-split-classifications.py"
BACKUP_SCRIPT = REPO_ROOT / "scripts/cval-backup.sh"

SCHEMA = """
CREATE TABLE classification_results (
  classified_at INTEGER NOT NULL,
  node TEXT NOT NULL,
  test_type TEXT NOT NULL,
  baseline_id TEXT NOT NULL,
  status TEXT NOT NULL,
  passed INTEGER NOT NULL,
  n_compared INTEGER NOT NULL,
  n_degraded INTEGER NOT NULL,
  n_improved INTEGER NOT NULL,
  n_band_degraded INTEGER NOT NULL DEFAULT 0,
  degraded_metric_fraction REAL NOT NULL DEFAULT 0.0,
  worst_pct_diff REAL NOT NULL DEFAULT 0.0,
  metrics_json TEXT NOT NULL,
  PRIMARY KEY (classified_at, node, test_type, baseline_id)
)
"""

LEGACY_SCHEMA = """
CREATE TABLE classification_results (
  classified_at INTEGER NOT NULL,
  node TEXT NOT NULL,
  test_type TEXT NOT NULL,
  baseline_id TEXT NOT NULL,
  status TEXT NOT NULL,
  passed INTEGER NOT NULL,
  n_compared INTEGER NOT NULL,
  n_degraded INTEGER NOT NULL,
  n_improved INTEGER NOT NULL,
  metrics_json TEXT NOT NULL,
  PRIMARY KEY (classified_at, node, test_type, baseline_id)
)
"""

INDEX = (
    "CREATE INDEX idx_classification_node_test_time "
    "ON classification_results(node, test_type, classified_at)"
)

ROWS = [
    (1, "node-a", "storage", "s1", "normal", 1, 1, 0, 0, 0, 0.0, 0.0, "[]"),
    (2, "node-b", "nccl", "n1", "degraded", 0, 2, 1, 0, 1, 0.5, 20.0, "[]"),
]


def _load_script_module():
    spec = importlib.util.spec_from_file_location("cval_split_classifications", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ClassificationSplitTests(unittest.TestCase):
    def _source(
        self,
        path: Path,
        *,
        rows: list[tuple] | None = None,
        legacy: bool = False,
        index_sql: str = INDEX,
        extra_sql: str | None = None,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(LEGACY_SCHEMA if legacy else SCHEMA)
            connection.execute(index_sql)
            selected = ROWS if rows is None else rows
            if legacy:
                connection.executemany(
                    "INSERT INTO classification_results VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [row[:9] + (row[12],) for row in selected],
                )
            else:
                connection.executemany(
                    "INSERT INTO classification_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    selected,
                )
            if extra_sql:
                connection.execute(extra_sql)
            connection.commit()

    def _config(self, root: Path, *, baseline_root: Path | None = None) -> Path:
        target = baseline_root or root / "baselines"
        target.mkdir(parents=True, exist_ok=True)
        config = root / "cval.toml"
        config.write_text(
            f'[baseline]\nbaseline_root_path = "{target}"\n',
            encoding="utf-8",
        )
        return config

    def _manifest(
        self,
        root: Path,
        source: Path,
        *,
        size: int | None = None,
        sha256: str | None = None,
        source_root: Path | None = None,
        entry_path: str | None = None,
    ) -> Path:
        manifest_root = source_root or root
        source_stat = source.stat()
        source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
        backup_root = root / "backup-copy"
        relative = entry_path or source.relative_to(manifest_root).as_posix()
        backup_path = backup_root / relative
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        backup_path.write_bytes(source.read_bytes())
        backup_digest = hashlib.sha256(backup_path.read_bytes()).hexdigest()
        manifest = root / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema": "cval.backup",
                    "source_root": str(manifest_root),
                    "destination": str(backup_root),
                    "excluded": ["backups/"],
                    "files": [
                        {
                            "path": entry_path
                            or source.relative_to(manifest_root).as_posix(),
                            "method": "sqlite-backup",
                            "size": backup_path.stat().st_size,
                            "mode": source_stat.st_mode & 0o7777,
                            "sha256": sha256
                            or backup_digest,
                            "source_size": source_stat.st_size if size is None else size,
                            "source_sha256": sha256 or source_digest,
                            "source_dev": source_stat.st_dev,
                            "source_inode": source_stat.st_ino,
                            "source_mtime_ns": source_stat.st_mtime_ns,
                            "backup_size": backup_path.stat().st_size,
                            "backup_sha256": sha256 or backup_digest,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return manifest

    def test_validate_backup_manifest_accepts_richer_whole_root_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            manifest = self._manifest(root, source)
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            payload["schema_version"] = 1
            payload["started_at"] = "2026-08-01T12:00:00Z"
            payload["files"][0]["mtime_ns"] = source.stat().st_mtime_ns
            payload["files"][0]["sqlite"] = True
            manifest.write_text(json.dumps(payload), encoding="utf-8")

            _load_script_module().validate_backup_manifest(
                manifest,
                source,
                source_state=_load_script_module().FileState.from_stat(source.stat()),
                source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            )

    def _run(
        self,
        source: Path,
        config: Path,
        *,
        apply: bool = False,
        manifest: Path | None = None,
        use_backup_db: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(SCRIPT),
            "--source",
            str(source),
            "--config",
            str(config),
        ]
        if apply:
            command += ["--apply", "--confirm", "split-classifications"]
        if manifest is not None:
            command += ["--backup-manifest", str(manifest)]
        if use_backup_db:
            command += ["--use-backup-db"]
        return subprocess.run(
            command,
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def _apply(
        self, root: Path, source: Path, config: Path
    ) -> subprocess.CompletedProcess[str]:
        return self._run(
            source,
            config,
            apply=True,
            manifest=self._manifest(root, source),
        )

    @staticmethod
    def _stage_names(directory: Path) -> list[str]:
        return sorted(
            path.name
            for path in directory.iterdir()
            if ".split." in path.name
            or "cval-classification-snapshot" in path.name
        )

    def test_inspection_reports_counts_digests_targets_and_creates_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "tree with # and ?"
            root.mkdir()
            source = root / "classification #?.db"
            self._source(source)
            baseline_root = root / "target dbs #?"
            config = self._config(root, baseline_root=baseline_root)
            before = sorted(path.relative_to(root) for path in root.rglob("*"))

            completed = self._run(source, config)

            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["mode"], "inspect")
            self.assertEqual(payload["rows"], {"nccl": 1, "storage": 1})
            self.assertRegex(payload["source_sha256"], r"^sha256:[0-9a-f]{64}$")
            self.assertRegex(
                payload["digests"]["storage"], r"^sha256:[0-9a-f]{64}$"
            )
            self.assertEqual(
                payload["targets"]["storage"],
                str(baseline_root / "storage-classifications.db"),
            )
            self.assertEqual(
                sorted(path.relative_to(root) for path in root.rglob("*")), before
            )

            applied = self._apply(root, source, config)
            self.assertEqual(applied.returncode, 0, applied.stderr)
            self.assertTrue(
                (baseline_root / "storage-classifications.db").is_file()
            )

    def test_apply_success_projects_legacy_schema_and_preserves_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source, legacy=True)
            config = self._config(root)
            source_before = source.read_bytes()

            completed = self._apply(root, source, config)

            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = json.loads(completed.stdout)
            self.assertTrue(payload["ok"])
            self.assertEqual(source.read_bytes(), source_before)
            with closing(
                sqlite3.connect(root / "baselines/storage-classifications.db")
            ) as connection:
                row = connection.execute(
                    "SELECT test_type, n_band_degraded, degraded_metric_fraction, "
                    "worst_pct_diff FROM classification_results"
                ).fetchone()
                self.assertEqual(row, ("storage", 0, 0.0, 0.0))
                self.assertEqual(
                    connection.execute("PRAGMA quick_check").fetchone(), ("ok",)
                )
            self.assertEqual(self._stage_names(root), [])
            self.assertEqual(self._stage_names(root / "baselines"), [])

    def test_degraded_legacy_projection_matches_global_fallback_without_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            metrics = json.dumps(
                [
                    {"metric": "a", "status": "degraded", "pct_diff": -25.0},
                    {"metric": "b", "status": "normal", "pct_diff": 1.0},
                ]
            )
            row = (
                7, "node-a", "storage", "s1", "degraded", 0,
                2, 1, 0, 0, 0.0, 0.0, metrics,
            )
            self._source(source, rows=[row], legacy=True)
            config = self._config(root)

            completed = self._apply(root, source, config)

            self.assertEqual(completed.returncode, 0, completed.stderr)
            target = root / "baselines/storage-classifications.db"
            legacy_rows = latest_classification_rows_from_db(source)
            split_rows = latest_classification_rows_from_db(target)
            self.assertEqual(split_rows, legacy_rows)
            self.assertEqual(split_rows[0].n_band_degraded, 1)
            self.assertEqual(split_rows[0].degraded_metric_fraction, 0.5)
            self.assertEqual(split_rows[0].worst_pct_diff, 25.0)
            self.assertEqual(
                latest_classification_rows_from_dbs([source, target]),
                legacy_rows,
            )

    def test_apply_requires_exact_confirmation_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)

            missing = self._run(source, config, apply=True)
            wrong = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--source",
                    str(source),
                    "--config",
                    str(config),
                    "--apply",
                    "--confirm",
                    "split",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("backup-manifest", missing.stderr)
            self.assertNotEqual(wrong.returncode, 0)
            self.assertIn("exact --confirm split-classifications", wrong.stderr)

    def test_tampered_manifest_path_size_and_digest_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            cases = [
                {"entry_path": "other.db"},
                {"size": source.stat().st_size + 1},
                {"sha256": "0" * 64},
            ]
            for index, values in enumerate(cases):
                manifest = self._manifest(root, source, **values)
                renamed = root / f"manifest-{index}.json"
                manifest.replace(renamed)
                completed = self._run(
                    source, config, apply=True, manifest=renamed
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertFalse(
                    (root / "baselines/storage-classifications.db").exists()
                )

    def test_source_mutation_makes_backup_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            manifest = self._manifest(root, source)
            with closing(sqlite3.connect(source)) as connection:
                connection.execute(
                    "INSERT INTO classification_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        3,
                        "node-c",
                        "storage",
                        "s2",
                        "normal",
                        1,
                        1,
                        0,
                        0,
                        0,
                        0.0,
                        0.0,
                        "[]",
                    ),
                )
                connection.commit()

            completed = self._run(
                source, config, apply=True, manifest=manifest
            )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("source identity evidence", completed.stderr)
            self.assertFalse(
                (root / "baselines/storage-classifications.db").exists()
            )

    def test_real_backup_verify_then_split_and_stale_evidence_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "continuous_validation"
            source = root / "baselines/classification-results.db"
            self._source(source)
            config = self._config(root)
            environment = os.environ | {
                "CVAL_BACKUP_TIMESTAMP": "20260801T120000Z",
                "PYTHON": sys.executable,
            }
            applied = subprocess.run(
                [
                    "bash", str(BACKUP_SCRIPT), "--source", str(root),
                    "--apply", "--confirm", "backup", "--quiesced",
                    "--confirm-quiesced", "writers-stopped",
                ],
                cwd=REPO_ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(applied.returncode, 0, applied.stderr)
            backup = Path(json.loads(applied.stdout)["backup"])
            verified = subprocess.run(
                ["bash", str(BACKUP_SCRIPT), "--verify", str(backup)],
                cwd=REPO_ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(verified.returncode, 0, verified.stderr)
            manifest = backup / "manifest.json"

            completed = self._run(
                source,
                config,
                apply=True,
                manifest=manifest,
                use_backup_db=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            target_paths = [
                root / "baselines/storage-classifications.db",
                root / "baselines/nccl-classifications.db",
            ]
            self.assertTrue(all(path.exists() for path in target_paths))
            current_rows = latest_classification_rows_from_db(source)
            self.assertEqual(
                latest_classification_rows_from_dbs([source, *target_paths]),
                current_rows,
            )

            (root / "baselines/storage-classifications.db").unlink()
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            entry = next(
                item for item in payload["files"]
                if item["path"] == "baselines/classification-results.db"
            )
            entry["source_sha256"] = "0" * 64
            stale = backup / "stale-manifest.json"
            stale.write_text(json.dumps(payload), encoding="utf-8")
            rejected = self._run(source, config, apply=True, manifest=stale)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("source identity evidence", rejected.stderr)
            self.assertFalse((root / "baselines/storage-classifications.db").exists())

    def test_source_mutation_during_apply_publishes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            module = _load_script_module()
            manifest = self._manifest(root, source)
            original = module._copy_snapshot

            def mutate_after_snapshot(*args, **kwargs):
                snapshot = original(*args, **kwargs)
                with closing(sqlite3.connect(source)) as connection:
                    connection.execute(
                        "INSERT INTO classification_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            3,
                            "node-c",
                            "storage",
                            "s2",
                            "normal",
                            1,
                            1,
                            0,
                            0,
                            0,
                            0.0,
                            0.0,
                            "[]",
                        ),
                    )
                    connection.commit()
                return snapshot

            with patch.object(
                module, "_copy_snapshot", side_effect=mutate_after_snapshot
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "identity/size/mtime changed|snapshot digest does not match",
                ):
                    module._apply(source, manifest, config)

            self.assertFalse(
                (root / "baselines/storage-classifications.db").exists()
            )
            self.assertFalse(
                (root / "baselines/nccl-classifications.db").exists()
            )
            self.assertEqual(self._stage_names(root), [])

    def test_partial_failure_removes_stages_and_publishes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            module = _load_script_module()
            manifest = self._manifest(root, source)
            original = module._build_stage
            calls = 0

            def fail_second(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("injected stage failure")
                return original(*args, **kwargs)

            with patch.object(module, "_build_stage", side_effect=fail_second):
                with self.assertRaisesRegex(
                    RuntimeError, "injected stage failure"
                ):
                    module._apply(source, manifest, config)

            self.assertFalse(
                (root / "baselines/storage-classifications.db").exists()
            )
            self.assertFalse(
                (root / "baselines/nccl-classifications.db").exists()
            )
            self.assertEqual(self._stage_names(root), [])
            self.assertEqual(self._stage_names(root / "baselines"), [])

    def test_publication_failure_rolls_back_prior_published_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            module = _load_script_module()
            manifest = self._manifest(root, source)
            original = module.rename_noreplace_at
            calls = 0

            def fail_second(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("injected publication failure")
                return original(*args, **kwargs)

            with patch.object(
                module, "rename_noreplace_at", side_effect=fail_second
            ):
                with self.assertRaisesRegex(
                    OSError, "injected publication failure"
                ):
                    module._apply(source, manifest, config)

            self.assertFalse(
                (root / "baselines/storage-classifications.db").exists()
            )
            self.assertFalse(
                (root / "baselines/nccl-classifications.db").exists()
            )
            self.assertEqual(self._stage_names(root), [])
            self.assertEqual(self._stage_names(root / "baselines"), [])

    def test_exact_retry_is_idempotent_and_superset_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            first = self._apply(root, source, config)
            self.assertEqual(first.returncode, 0, first.stderr)

            exact = self._apply(root, source, config)
            self.assertEqual(exact.returncode, 0, exact.stderr)
            self.assertEqual(
                json.loads(exact.stdout)["tests"]["storage"]["state"], "exact"
            )

            target = root / "baselines/storage-classifications.db"
            with closing(sqlite3.connect(target)) as connection:
                connection.execute(
                    "INSERT INTO classification_results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        4,
                        "node-z",
                        "storage",
                        "post",
                        "normal",
                        1,
                        1,
                        0,
                        0,
                        0,
                        0.0,
                        0.0,
                        "[]",
                    ),
                )
                connection.commit()
            superset = self._apply(root, source, config)
            self.assertEqual(superset.returncode, 0, superset.stderr)
            self.assertEqual(
                json.loads(superset.stdout)["tests"]["storage"]["state"],
                "superset",
            )

    def test_partial_existing_target_completes_missing_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            self._source(
                root / "baselines/storage-classifications.db", rows=[ROWS[0]]
            )

            completed = self._apply(root, source, config)

            self.assertEqual(completed.returncode, 0, completed.stderr)
            states = {
                key: value["state"]
                for key, value in json.loads(completed.stdout)["tests"].items()
            }
            self.assertEqual(states, {"nccl": "created", "storage": "exact"})
            self.assertTrue(
                (root / "baselines/nccl-classifications.db").is_file()
            )

    def test_existing_primary_key_conflict_fails_without_missing_publish(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            conflict = list(ROWS[0])
            conflict[4] = "degraded"
            self._source(
                root / "baselines/storage-classifications.db",
                rows=[tuple(conflict)],
            )

            completed = self._apply(root, source, config)

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("conflicting primary key", completed.stderr)
            self.assertFalse(
                (root / "baselines/nccl-classifications.db").exists()
            )

    def test_custom_schema_and_index_manifests_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = self._config(root)
            extra = root / "extra.db"
            self._source(extra, extra_sql="CREATE TABLE custom (value TEXT)")
            wrong_index = root / "wrong-index.db"
            self._source(
                wrong_index,
                index_sql=(
                    "CREATE INDEX idx_classification_node_test_time "
                    "ON classification_results(test_type, node, classified_at)"
                ),
            )

            for source in (extra, wrong_index):
                completed = self._run(source, config)
                self.assertNotEqual(completed.returncode, 0)
                self.assertRegex(completed.stderr, "manifest|index")

    def test_source_sqlite_sidecar_is_rejected_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config = self._config(root)
            sidecar = Path(str(source) + "-wal")
            sidecar.write_bytes(b"do-not-touch")

            completed = self._run(source, config)

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("SQLite sidecar", completed.stderr)
            self.assertEqual(sidecar.read_bytes(), b"do-not-touch")
            self.assertFalse(
                (root / "baselines/storage-classifications.db").exists()
            )

    def test_reserved_test_type_and_symlink_ancestors_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            reserved = root / "reserved.db"
            row = list(ROWS[0])
            row[2] = "all"
            self._source(reserved, rows=[tuple(row)])
            config = self._config(root)
            reserved_result = self._run(reserved, config)
            self.assertNotEqual(reserved_result.returncode, 0)
            self.assertIn("Reserved operational target", reserved_result.stderr)

            real = root / "real"
            real.mkdir()
            linked_source = real / "source.db"
            self._source(linked_source)
            link = root / "linked"
            link.symlink_to(real, target_is_directory=True)
            linked_result = self._run(link / "source.db", config)
            self.assertNotEqual(linked_result.returncode, 0)

            final_link = root / "source-link.db"
            final_link.symlink_to(linked_source)
            final_result = self._run(final_link, config)
            self.assertNotEqual(final_result.returncode, 0)

            symlink_root = root / "real-targets"
            symlink_root.mkdir()
            linked_targets = root / "linked-targets"
            linked_targets.symlink_to(symlink_root, target_is_directory=True)
            linked_config = self._config(root, baseline_root=linked_targets)
            target_result = self._apply(root, linked_source, linked_config)
            self.assertNotEqual(target_result.returncode, 0)
            self.assertFalse(any(symlink_root.iterdir()))

    def test_split_target_is_writable_by_current_classification_storage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "classification-results.db"
            self._source(source)
            config_path = self._config(root)
            completed = self._apply(root, source, config_path)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            config = load_config(config_path)
            target = root / "baselines/storage-classifications.db"
            current_rows = latest_classification_rows_from_db(target)
            self.assertEqual(
                [(row.node, row.test_type, row.status) for row in current_rows],
                [("node-a", "storage", "normal")],
            )
            verdict = {
                "node": "node-new",
                "test_type": "storage",
                "baseline_test_type": "storage",
                "dl_component": "",
                "baseline_id": "s2",
                "status": "normal",
                "n_metrics": 1,
                "n_compared": 1,
                "n_degraded": 0,
                "n_band_degraded": 0,
                "n_improved": 0,
                "degraded_metric_fraction": 0.0,
                "degraded_metric_percent": 0.0,
                "worst_pct_diff": 0.0,
                "metrics": [
                    {
                        "metric": "x",
                        "component": "",
                        "value": 100.0,
                        "median": 100.0,
                        "status": "normal",
                        "pct_diff": 0.0,
                        "abs_pct_diff": 0.0,
                        "counts_for_degraded_status": False,
                        "direction": "low_bad",
                        "lower_bound": 90.0,
                        "upper_bound": None,
                    }
                ],
            }

            count = store_classification_results(
                [verdict], classified_at=10, config=config
            )

            self.assertEqual(count, 1)
            with closing(
                sqlite3.connect(target)
            ) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM classification_results"
                    ).fetchone()[0],
                    2,
                )


if __name__ == "__main__":
    unittest.main()
