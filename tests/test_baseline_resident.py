from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from cval.baselines import resident


class BaselineResidentTests(unittest.TestCase):
    def test_status_requires_every_child_to_be_alive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            (state_dir / "state.json").write_text(
                json.dumps(
                    {
                        "schema_version": "cval.sqlite-evaluator-resident.v1",
                        "started_at": "2026-08-03T00:00:00Z",
                        "children": {"baseline-build": 101, "baseline-classify": 102},
                    }
                ),
                encoding="utf-8",
            )
            (state_dir / "state.json").chmod(0o600)
            with patch("cval.baselines.resident.os.kill") as kill:
                payload = resident.status(state_dir)
            self.assertEqual(payload["status"], "ready")
            self.assertEqual(kill.call_count, 2)

            def one_dead(pid, signum):
                if pid == 102:
                    raise ProcessLookupError

            with patch("cval.baselines.resident.os.kill", side_effect=one_dead):
                with self.assertRaisesRegex(RuntimeError, "baseline-classify"):
                    resident.status(state_dir)

    def test_run_fails_when_a_child_exits(self) -> None:
        first = Mock(pid=101)
        second = Mock(pid=102)
        first.returncode = 7
        first_polls = iter((None, 7))
        first.poll.side_effect = lambda: next(first_polls, 7)
        second_polls = iter((None, None, 0))
        second.poll.side_effect = lambda: next(second_polls, 0)
        first.wait.return_value = 7
        second.wait.return_value = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            scripts = root / "scripts"
            scripts.mkdir()
            (root / "config").mkdir()
            for name in ("cval-baseline-build.sh", "cval-baseline-classify.sh"):
                (scripts / name).write_text("#!/bin/bash\n", encoding="utf-8")
            state = root / "state"
            with patch(
                "cval.baselines.resident.subprocess.Popen",
                side_effect=[first, second],
            ), patch("cval.baselines.resident.time.sleep"), patch(
                "cval.baselines.resident.os.killpg"
            ):
                code = resident.run(repo_root=root, state_dir=state, environ={})
        self.assertEqual(code, 7)
        self.assertFalse((state / "state.json").exists())

    def test_state_file_is_owner_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = Path(tmpdir)
            resident._write_state(
                state,
                {
                    "schema_version": "cval.sqlite-evaluator-resident.v1",
                    "started_at": "now",
                    "children": {"one": os.getpid()},
                },
            )
            self.assertEqual((state / "state.json").stat().st_mode & 0o777, 0o600)

    def test_children_receive_supervisor_virtual_environment_first(self) -> None:
        first = Mock(pid=101, returncode=1)
        second = Mock(pid=102, returncode=None)
        first.poll.return_value = 1
        second.poll.return_value = 0
        first.wait.return_value = 1
        second.wait.return_value = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            scripts = root / "scripts"
            scripts.mkdir()
            (root / "config").mkdir()
            for name in ("cval-baseline-build.sh", "cval-baseline-classify.sh"):
                (scripts / name).write_text("#!/bin/bash\n", encoding="utf-8")
            with patch(
                "cval.baselines.resident.subprocess.Popen",
                side_effect=[first, second],
            ) as popen, patch("cval.baselines.resident.os.killpg"):
                resident.run(
                    repo_root=root,
                    state_dir=root / "state",
                    environ={"PATH": "/usr/bin"},
                )
        expected = str(Path(resident.sys.executable).resolve().parent)
        self.assertEqual(popen.call_args_list[0].kwargs["env"]["PATH"], f"{expected}:/usr/bin")


if __name__ == "__main__":
    unittest.main()
