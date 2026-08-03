from __future__ import annotations

import json
import re
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path

from cval.config import load_config, load_config_snapshot
from cval.validation.runtime import (
    build_runtime_environment,
    _decode_runtime_environment,
    effective_config_digest,
    encode_runtime_environment,
)


class ValidationRuntimeTests(unittest.TestCase):
    def test_runtime_environment_contains_registry_and_builtin_values(self) -> None:
        config = load_config()

        values = build_runtime_environment(config)

        self.assertEqual(values["CVAL_ENABLED_TESTS"], "storage,nccl,dltest")
        self.assertEqual(values["RUN_STORAGE"], "true")
        self.assertEqual(values["RUN_NCCL"], "true")
        self.assertEqual(values["RUN_DLTEST"], "true")
        self.assertEqual(values["CVAL_CONFIG_PATH"], "/workspace/c-val/config/cval.toml")
        self.assertNotIn("CVAL_RUN_HISTORY_DB_PATH", values)
        self.assertNotIn("CVAL_RUN_HISTORY_ENABLED", values)
        self.assertNotIn("CVAL_PER_TEST_INGESTION_ENABLED", values)
        snapshot = load_config_snapshot(values["CVAL_CONFIG_SNAPSHOT_B64"])
        self.assertEqual(effective_config_digest(snapshot), effective_config_digest(config))
        self.assertEqual(values["CVAL_NCCL_ITERATIONS"], "20")
        self.assertEqual(values["CVAL_NCCL_EVALUATION_ENABLED"], "false")
        self.assertEqual(
            values["CVAL_NCCL_EVALUATION_TEST_NAME"], "nccl-loopback-allreduce"
        )
        self.assertEqual(
            values["CVAL_NCCL_OUTBOX_ROOT"],
            "/data/continuous_validation/nccl_eval/outbox",
        )
        self.assertEqual(values["CVAL_DL_ITERATIONS"], "100")
        registry = json.loads(values["CVAL_TEST_REGISTRY_JSON"])
        self.assertEqual(registry["nccl"]["order"], 20)
        self.assertEqual(
            registry["nccl"]["config_path"],
            "validation-tests/nccl/test_config.toml",
        )

    def test_encoded_runtime_environment_round_trips_shell_values(self) -> None:
        values = {
            "ALPHA": "plain",
            "COMPLEX": "spaces ' quotes $ and ; separators",
            "EMPTY": "",
        }

        encoded = encode_runtime_environment(values)
        decoded = _decode_runtime_environment(encoded)
        parsed: dict[str, str] = {}
        for line in decoded.splitlines():
            match = re.fullmatch(r"export ([A-Z_][A-Z0-9_]*)=(.*)", line)
            self.assertIsNotNone(match)
            assert match is not None
            parsed[match.group(1)] = shlex.split(match.group(2))[0]

        self.assertEqual(parsed, values)
        self.assertNotIn("\n", encoded)

        with tempfile.TemporaryDirectory() as tmpdir:
            env_file = Path(tmpdir) / "runtime.env"
            env_file.write_text(decoded, encoding="utf-8")
            completed = subprocess.run(
                [
                    "bash",
                    "-c",
                    'set -eu; source "$1"; printf "%s\\n" "$COMPLEX" "$EMPTY"',
                    "bash",
                    str(env_file),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )

        self.assertEqual(completed.stdout.splitlines(), [values["COMPLEX"], ""])

    def test_effective_config_digest_is_stable_and_well_formed(self) -> None:
        config = load_config()

        first = effective_config_digest(config)
        second = effective_config_digest(config)

        self.assertEqual(first, second)
        self.assertRegex(first, r"^sha256:[0-9a-f]{64}$")

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cval.toml"
            config_path.write_text(
                """
[tests.nccl]
enabled = false
""",
                encoding="utf-8",
            )
            changed = effective_config_digest(load_config(config_path))

        self.assertNotEqual(first, changed)

    def test_alternate_config_survives_effective_runtime_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "alternate.toml"
            config_path.write_text(
                f'''
[runtime]
validation_root = "{tmpdir}/validation"
''',
                encoding="utf-8",
            )
            config = load_config(config_path)

            values = build_runtime_environment(config)
            restored = load_config_snapshot(values["CVAL_CONFIG_SNAPSHOT_B64"])

        self.assertEqual(restored.runtime.validation_root, f"{tmpdir}/validation")
        self.assertEqual(effective_config_digest(restored), effective_config_digest(config))

    def test_decode_rejects_invalid_payload(self) -> None:
        with self.assertRaisesRegex(ValueError, "Invalid c-val runtime"):
            _decode_runtime_environment("not base64!")

    def test_encode_rejects_invalid_environment_data(self) -> None:
        with self.assertRaisesRegex(ValueError, "Invalid runtime environment name"):
            encode_runtime_environment({"BAD-NAME": "value"})
        with self.assertRaisesRegex(ValueError, "contains NUL"):
            encode_runtime_environment({"VALID": "bad\x00value"})


if __name__ == "__main__":
    unittest.main()
