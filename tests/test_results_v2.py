from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    parse_validation_result,
    parse_validation_result_v2,
    validation_result_to_env,
    validation_result_v2_digest,
)
from cval.validation.builtins import project_builtin_statuses


DIGEST = "sha256:" + "a" * 64


def test_result(
    test_id: str,
    *,
    order: int,
    enabled: bool = True,
    selected: bool = True,
    status: str = "pass",
    phase: str = "finished",
    exit_code: int | None = 0,
) -> dict[str, object]:
    terminal = phase in {
        "finished",
        "setup_failed",
        "timed_out",
        "interrupted",
        "framework_error",
    }
    started = (
        "2026-07-28T16:00:00Z"
        if selected and phase not in {"pending", "not_selected"}
        else None
    )
    completed = "2026-07-28T16:00:01Z" if terminal else None
    return {
        "display_name": test_id.title(),
        "enabled": enabled,
        "selected": selected,
        "order": order,
        "status": status,
        "phase": phase,
        "started_at": started,
        "completed_at": completed,
        "duration_ms": 1000 if terminal else None,
        "exit_code": exit_code,
        "config_path": f"validation-tests/{test_id}/test_config.toml",
        "config_digest": DIGEST,
        "stdout": f"/logs/{test_id}/stdout.log" if selected else "",
        "stderr": f"/logs/{test_id}/stderr.log" if selected else "",
        "log": f"/logs/{test_id}/events.jsonl" if selected else "",
        "summary": f"/runs/{test_id}/summary.json" if selected else "",
        "result": f"/runs/{test_id}/result.json" if selected else "",
        "artifacts": f"/runs/{test_id}/artifacts" if selected else "",
        "message": "",
    }


def payload() -> dict[str, object]:
    return {
        "schema_version": "cval.results.v2",
        "run_id": "node-a-123",
        "node": "node-a",
        "timestamp": 123,
        "timestamp_la": "1969-12-31T16:02:03-08:00",
        "generated_at": "2026-07-28T16:00:01Z",
        "completed_at": "2026-07-28T16:00:01Z",
        "overall": "pass",
        "image_name": "pytorch:26.05-py3",
        "pytorch_version": "2.8.0",
        "cuda_version": "12.9",
        "git_ref": "abc123",
        "global_config_digest": DIGEST,
        "tests": {
            "storage": test_result("storage", order=10),
            "nccl": test_result("nccl", order=20),
            "dltest": test_result("dltest", order=30),
            "smoke": test_result("smoke", order=40),
        },
        "errors": [],
    }


class ResultSchemaV2Tests(unittest.TestCase):
    def test_parses_dynamic_four_test_result(self) -> None:
        result = parse_validation_result_v2(payload())

        self.assertIsInstance(result, ValidationResultV2)
        self.assertEqual(result.overall, "pass")
        self.assertEqual(list(result.tests), ["storage", "nccl", "dltest", "smoke"])
        self.assertEqual(result.tests["smoke"].phase, "finished")
        self.assertEqual(
            validation_result_to_env(result),
            {
                "GCRRESULT1": "pass",
                "GCRRESULT2": "pass",
                "GCRRESULT3": "pass",
                "RUN_STORAGE": "true",
                "RUN_NCCL": "true",
                "RUN_DLTEST": "true",
                "overall_result": "pass",
                "image_name": "pytorch:26.05-py3",
                "pytorch_version": "2.8.0",
                "cuda_version": "12.9",
                "result_node": "node-a",
                "result_timestamp": "123",
                "result_run_id": "node-a-123",
                "result_schema_version": "cval.results.v2",
                "result_global_config_digest": DIGEST,
                "result_digest": validation_result_v2_digest(result),
                "result_storage_artifacts": "/runs/storage/artifacts",
                "result_nccl_summary": "/runs/nccl/summary.json",
            },
        )

    def test_loader_dispatches_v2_from_disk(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "result.json"
            path.write_text(json.dumps(payload()), encoding="utf-8")
            result = load_validation_result(path)

        self.assertIsInstance(result, ValidationResultV2)
        self.assertEqual(result.schema_version, "cval.results.v2")

    def test_loader_dispatches_canonical_current_schema(self) -> None:
        value = payload()
        value["schema_version"] = "cval.results"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "result.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            result = load_validation_result(path)

        self.assertIsInstance(result, ValidationResultV2)
        self.assertEqual(result.schema_version, "cval.results")
        self.assertEqual(
            project_builtin_statuses(validation_result_to_env(result)),
            {"storage": "pass", "nccl": "pass", "dltest": "pass", "all": "pass"},
        )

    def test_historical_v1_reader_and_builtin_projection_remain_exact(self) -> None:
        result = parse_validation_result(
            {
                "schema_version": "cval.results.v1",
                "node": "node-a",
                "timestamp": "123",
                "overall": "fail",
                "tests": {
                    "storage": {"status": "pass", "enabled": True},
                    "nccl": {"status": "fail", "enabled": True},
                    "dltest": {"status": "incomplete", "enabled": False},
                },
            }
        )
        projected = validation_result_to_env(result)

        self.assertEqual(result.schema_version, "cval.results.v1")
        self.assertEqual(
            project_builtin_statuses(projected),
            {
                "storage": "pass",
                "nccl": "fail",
                "dltest": "incomplete",
                "all": "fail",
            },
        )
        self.assertEqual(
            {name: projected[name] for name in ("RUN_STORAGE", "RUN_NCCL", "RUN_DLTEST")},
            {"RUN_STORAGE": "true", "RUN_NCCL": "true", "RUN_DLTEST": "false"},
        )

    def test_allows_disabled_not_selected_test(self) -> None:
        value = payload()
        value["tests"]["smoke"] = test_result(  # type: ignore[index]
            "smoke",
            order=40,
            enabled=False,
            selected=False,
            status="incomplete",
            phase="not_selected",
            exit_code=None,
        )

        result = parse_validation_result_v2(value)

        self.assertFalse(result.tests["smoke"].selected)
        self.assertEqual(result.overall, "pass")

    def test_rejects_inconsistent_overall(self) -> None:
        value = payload()
        value["tests"]["nccl"] = test_result(  # type: ignore[index]
            "nccl", order=20, status="fail", exit_code=1
        )

        with self.assertRaisesRegex(ValueError, "overall must be 'fail'"):
            parse_validation_result_v2(value)

    def test_rejects_disabled_selected_test(self) -> None:
        value = payload()
        value["tests"]["smoke"] = test_result(  # type: ignore[index]
            "smoke", order=40, enabled=False
        )

        with self.assertRaisesRegex(ValueError, "cannot be selected"):
            parse_validation_result_v2(value)

    def test_rejects_duplicate_selected_order(self) -> None:
        value = payload()
        value["tests"]["smoke"]["order"] = 20  # type: ignore[index]

        with self.assertRaisesRegex(ValueError, "unique order"):
            parse_validation_result_v2(value)

    def test_rejects_passing_nonzero_exit(self) -> None:
        value = payload()
        value["tests"]["nccl"]["exit_code"] = 9  # type: ignore[index]

        with self.assertRaisesRegex(ValueError, "must have exit_code 0"):
            parse_validation_result_v2(value)

    def test_rejects_completed_run_with_pending_test(self) -> None:
        value = payload()
        value["overall"] = "incomplete"
        value["tests"]["smoke"] = test_result(  # type: ignore[index]
            "smoke",
            order=40,
            status="incomplete",
            phase="pending",
            exit_code=None,
        )

        with self.assertRaisesRegex(ValueError, "completed_at must be set exactly"):
            parse_validation_result_v2(value)

    def test_rejects_unknown_fields(self) -> None:
        value = copy.deepcopy(payload())
        value["unexpected"] = True

        with self.assertRaisesRegex(ValueError, "Unknown field"):
            parse_validation_result_v2(value)

    def test_rejects_impossible_running_state(self) -> None:
        value = payload()
        value["overall"] = "incomplete"
        value["completed_at"] = None
        value["tests"]["smoke"] = test_result(  # type: ignore[index]
            "smoke",
            order=40,
            status="incomplete",
            phase="running",
            exit_code=None,
        )
        value["tests"]["smoke"]["duration_ms"] = 1  # type: ignore[index]

        with self.assertRaisesRegex(ValueError, "terminal values"):
            parse_validation_result_v2(value)

    def test_rejects_timestamp_la_mismatch(self) -> None:
        value = payload()
        value["timestamp_la"] = "2026-07-28T16:00:00Z"

        with self.assertRaisesRegex(ValueError, "must represent timestamp"):
            parse_validation_result_v2(value)

    def test_rejects_node_path_traversal(self) -> None:
        value = payload()
        value["node"] = "../../../outside"

        with self.assertRaisesRegex(ValueError, "node must be a safe path segment"):
            parse_validation_result_v2(value)

    def test_rejects_empty_selected_path(self) -> None:
        value = payload()
        value["tests"]["smoke"]["stdout"] = ""  # type: ignore[index]

        with self.assertRaisesRegex(ValueError, "empty paths"):
            parse_validation_result_v2(value)

    def test_rejects_selected_not_selected_false_pass(self) -> None:
        value = payload()
        value["tests"]["smoke"] = test_result(  # type: ignore[index]
            "smoke",
            order=40,
            status="pass",
            phase="not_selected",
            exit_code=None,
        )

        with self.assertRaisesRegex(ValueError, "must be unselected/incomplete"):
            parse_validation_result_v2(value)


if __name__ == "__main__":
    unittest.main()
