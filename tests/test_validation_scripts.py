from __future__ import annotations

import unittest
import subprocess
import json
import tempfile
from pathlib import Path

from cval.validation.results import load_validation_result
from cval.validation.results import validation_result_to_env


REPO_ROOT = Path(__file__).resolve().parents[1]


class ValidationScriptTests(unittest.TestCase):
    def test_validation_scripts_have_valid_bash_syntax(self) -> None:
        scripts = [
            REPO_ROOT / "validation-tests" / "0-env.sh",
            REPO_ROOT / "validation-tests" / "run-test.sh",
            REPO_ROOT / "validation-tests" / "db-update.sh",
            REPO_ROOT / "validation-tests" / "dltest" / "dltest.sh",
            REPO_ROOT / "validation-tests" / "storage" / "storage.sh",
        ]

        subprocess.run(["bash", "-n", *map(str, scripts)], check=True)

    def test_run_test_persists_results_for_db_update(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "run-test.sh").read_text(encoding="utf-8")

        self.assertIn("CVAL_RESULT_ENV_FILE", script)
        self.assertIn("CVAL_RESULT_JSON_FILE", script)
        self.assertIn("write_result_state", script)
        self.assertIn("set -uo pipefail", script)

    def test_db_update_records_per_test_results(self) -> None:
        script = (REPO_ROOT / "validation-tests" / "db-update.sh").read_text(encoding="utf-8")

        self.assertIn('Loading structured test result state from $CVAL_RESULT_JSON_FILE', script)
        self.assertIn('python3 -m cval.cli result-env', script)
        self.assertIn('source "$CVAL_RESULT_ENV_FILE"', script)
        self.assertIn('add_main_result "storage" "$GCRRESULT1"', script)
        self.assertIn('add_main_result "nccl" "$GCRRESULT2"', script)
        self.assertIn('add_main_result "dltest" "$GCRRESULT3"', script)
        self.assertIn('add_main_result "all" "$overall_result"', script)
        self.assertNotIn('"all" \\\n    "pass"', script)

    def test_structured_validation_result_schema(self) -> None:
        payload = {
            "schema_version": "cval.results.v1",
            "node": "slc01-cl02-hgx-0001",
            "timestamp": "12345",
            "overall": "fail",
            "tests": {
                "storage": {"status": "pass", "log": "storage.log", "summary": "storage.txt"},
                "nccl": {"status": "fail", "log": "nccl.log", "summary": "nccl.json"},
                "dltest": {"status": "pass", "log": "dl.log", "summary": "dl.txt"},
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            result_path = Path(tmpdir) / "result.json"
            result_path.write_text(json.dumps(payload), encoding="utf-8")

            result = load_validation_result(result_path)

        self.assertEqual(result.overall, "fail")
        self.assertEqual(result.tests["nccl"].status, "fail")
        self.assertEqual(
            validation_result_to_env(result),
            {
                "GCRRESULT1": "pass",
                "GCRRESULT2": "fail",
                "GCRRESULT3": "pass",
                "overall_result": "fail",
            },
        )


if __name__ == "__main__":
    unittest.main()