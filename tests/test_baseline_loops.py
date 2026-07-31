"""Executable fake-catalog tests for U10 baseline background loops."""

from __future__ import annotations

import os
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

FAKE_PYTHON = r'''#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_CALLS"
if [[ "${1:-}" == "-" ]]; then
    printf '%s\n' "${!#}"
    exit 0
fi
if [[ " $* " == *" operational-targets "* ]]; then
    cat "$FAKE_CATALOG"
    exit 0
fi
if [[ " $* " == *" db-rebuild-dltest-metrics "* ]]; then
    printf '{"refreshed":true}\n'
    exit 0
fi
if [[ " $* " == *" baseline build "* || " $* " == *" baseline classify "* ]]; then
    target=""
    previous=""
    for value in "$@"; do
        if [[ "$previous" == "--test-type" ]]; then
            target="$value"
            break
        fi
        previous="$value"
    done
    printf '{"target":"%s"}\n' "$target"
    if [[ "$target" == "$FAKE_FAIL_TARGET" ]]; then
        exit 7
    fi
    exit 0
fi
printf '{}\n'
'''


def _catalog_line(
    name: str,
    owner: str | None = None,
    *,
    alias: bool = False,
    refresh_group: str = "",
) -> str:
    owner = owner or name
    return "\t".join(
        (
            "cval.operational-target.v1",
            name,
            owner,
            owner,
            owner,
            str(alias).lower(),
            refresh_group or "-",
        )
    )


class BaselineLoopTests(unittest.TestCase):
    def _environment(self, root: Path, catalog: list[str], fail_target: str) -> dict[str, str]:
        fake_bin = root / "bin"
        fake_bin.mkdir()
        fake_python = fake_bin / "python"
        fake_python.write_text(FAKE_PYTHON, encoding="utf-8")
        fake_python.chmod(0o755)
        catalog_path = root / "catalog.tsv"
        catalog_path.write_text("\n".join(catalog) + "\n", encoding="utf-8")
        calls = root / "calls.log"
        return os.environ | {
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "FAKE_CALLS": str(calls),
            "FAKE_CATALOG": str(catalog_path),
            "FAKE_FAIL_TARGET": fail_target,
            "CVAL_CONFIG": str(root / "local-only.toml"),
            "CVAL_BASELINE_ROOT": str(root / "baselines"),
            "CVAL_BASELINE_WINDOW_DAYS": "30",
            "CVAL_BASELINE_MIN_SAMPLES": "3",
            "CVAL_BASELINE_DL_TEST_PLAN": "fixture-plan",
            "CVAL_DL_RESULTS_ROOT": str(root / "dl-runs"),
            "CVAL_DL_METRIC_LOCK_FILE": str(root / "baselines/.dl-refresh.lock"),
            "CVAL_DL_METRIC_LOCK_PYTHON": sys.executable,
            "CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS": "0",
            # Legacy enable overrides must have no effect on catalog membership.
            "CVAL_STORAGE_ENABLED": "true",
            "CVAL_NCCL_ENABLED": "true",
            "CVAL_DLTEST_ENABLED": "true",
        }

    @staticmethod
    def _calls(root: Path) -> list[str]:
        return (root / "calls.log").read_text(encoding="utf-8").splitlines()

    def test_build_loop_dynamic_targets_isolates_failure_and_refreshes_dl_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self._environment(
                root,
                [
                    _catalog_line("storage"),
                    _catalog_line("synthetic"),
                    _catalog_line("dltest", refresh_group="dltest"),
                ],
                fail_target="synthetic",
            )
            env["CVAL_BASELINE_BUILD_LOG_DIR"] = str(root / "build-logs")
            completed = subprocess.run(
                ["bash", str(REPO_ROOT / "scripts/cval-baseline-build.sh"), "run-once"],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            calls = self._calls(root)

        self.assertNotEqual(completed.returncode, 0)
        build_calls = [line for line in calls if " baseline build " in f" {line} "]
        self.assertEqual(
            [self._target(line) for line in build_calls],
            ["storage", "synthetic", "dltest"],
        )
        self.assertEqual(
            sum("db-rebuild-dltest-metrics" in line for line in calls), 1
        )
        self.assertNotIn("disabled", "\n".join(calls))
        self.assertIn("acquired DL metric lock", completed.stdout)
        self.assertIn("failed=1", completed.stdout)

    def test_classify_loop_preserves_allowlist_and_cannot_reenable_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self._environment(
                root,
                [
                    _catalog_line("storage"),
                    _catalog_line("nccl"),
                    _catalog_line("synthetic"),
                    _catalog_line("dltest", refresh_group="dltest"),
                    _catalog_line(
                        "dltest-numerical", "dltest", alias=True, refresh_group="dltest"
                    ),
                    _catalog_line(
                        "dltest-compute", "dltest", alias=True, refresh_group="dltest"
                    ),
                    _catalog_line(
                        "dltest-collective", "dltest", alias=True, refresh_group="dltest"
                    ),
                    _catalog_line(
                        "dltest-overlap", "dltest", alias=True, refresh_group="dltest"
                    ),
                ],
                fail_target="synthetic",
            )
            env["CVAL_BASELINE_CLASSIFY_LOG_DIR"] = str(root / "classify-logs")
            env["CVAL_BASELINE_CLASSIFY_TESTS"] = (
                "storage, synthetic, dltest-compute, dltest-overlap, disabled"
            )
            completed = subprocess.run(
                [
                    "bash",
                    str(REPO_ROOT / "scripts/cval-baseline-classify.sh"),
                    "run-once",
                ],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            calls = self._calls(root)
            lock_path = root / "baselines/.dl-refresh.lock"
            lock_directory_exists = lock_path.parent.is_dir()
            lock_directory_mode = stat.S_IMODE(lock_path.parent.stat().st_mode)

        self.assertNotEqual(completed.returncode, 0)
        classify_calls = [line for line in calls if " baseline classify " in f" {line} "]
        self.assertEqual(
            [self._target(line) for line in classify_calls],
            ["storage", "synthetic", "dltest-compute", "dltest-overlap"],
        )
        self.assertEqual(
            sum("db-rebuild-dltest-metrics" in line for line in calls), 1
        )
        self.assertTrue(lock_directory_exists)
        self.assertEqual(lock_directory_mode & 0o022, 0)
        self.assertFalse(lock_path.exists())
        self.assertNotIn("--test-type disabled", "\n".join(calls))
        self.assertNotIn("--test-type nccl", "\n".join(calls))
        self.assertIn("acquired DL metric lock", completed.stdout)
        self.assertIn("failed=1", completed.stdout)

    def test_loops_reject_empty_catalog_and_empty_allowlist_intersection(self) -> None:
        for script, catalog, allowlist, diagnostic in (
            (
                "cval-baseline-build.sh",
                [],
                "",
                "no enabled baseline-build targets",
            ),
            (
                "cval-baseline-classify.sh",
                [_catalog_line("storage")],
                "disabled",
                "allowlist intersects no enabled target",
            ),
        ):
            with self.subTest(script=script), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(root, catalog, fail_target="")
                env["CVAL_BASELINE_CLASSIFY_TESTS"] = allowlist
                env["CVAL_BASELINE_BUILD_LOG_DIR"] = str(root / "build-logs")
                env["CVAL_BASELINE_CLASSIFY_LOG_DIR"] = str(root / "classify-logs")
                completed = subprocess.run(
                    ["bash", str(REPO_ROOT / f"scripts/{script}"), "run-once"],
                    cwd=REPO_ROOT,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn(diagnostic, completed.stdout)

    def test_dl_loops_fail_closed_for_missing_helper_or_insecure_lock_directory(self) -> None:
        for script, lock_mode, diagnostic in (
            ("cval-baseline-build.sh", "missing-helper", "helper unavailable"),
            (
                "cval-baseline-classify.sh",
                "insecure-directory",
                "must not be group/other writable",
            ),
            ("cval-baseline-build.sh", "directory", "must not be a directory"),
        ):
            with self.subTest(script=script, lock_mode=lock_mode), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(
                    root,
                    [_catalog_line("dltest", refresh_group="dltest")],
                    fail_target="",
                )
                env["CVAL_BASELINE_BUILD_LOG_DIR"] = str(root / "build-logs")
                env["CVAL_BASELINE_CLASSIFY_LOG_DIR"] = str(root / "classify-logs")
                if lock_mode == "missing-helper":
                    env["CVAL_DL_METRIC_LOCK_HELPER"] = str(root / "missing-helper.py")
                elif lock_mode == "insecure-directory":
                    lock_path = Path(env["CVAL_DL_METRIC_LOCK_FILE"])
                    lock_path.parent.mkdir(parents=True)
                    lock_path.parent.chmod(0o777)
                else:
                    env["CVAL_DL_METRIC_LOCK_FILE"] = str(root / "baselines")
                completed = subprocess.run(
                    ["bash", str(REPO_ROOT / f"scripts/{script}"), "run-once"],
                    cwd=REPO_ROOT,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                calls = self._calls(root)

            self.assertNotEqual(completed.returncode, 0)
            self.assertNotIn("acquired DL metric lock", completed.stdout)
            self.assertFalse(any("db-rebuild-dltest-metrics" in line for line in calls))
            self.assertFalse(any(" baseline build " in f" {line} " for line in calls))
            self.assertFalse(any(" baseline classify " in f" {line} " for line in calls))
            self.assertIn(diagnostic, completed.stdout + completed.stderr)

    def test_both_dl_loops_ignore_replaceable_marker_without_touching_victim(self) -> None:
        for script in (
            "cval-baseline-build.sh",
            "cval-baseline-classify.sh",
        ):
            with self.subTest(script=script), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(
                    root,
                    [_catalog_line("dltest", refresh_group="dltest")],
                    fail_target="",
                )
                env["CVAL_BASELINE_BUILD_LOG_DIR"] = str(root / "build-logs")
                env["CVAL_BASELINE_CLASSIFY_LOG_DIR"] = str(root / "classify-logs")
                victim = root / "victim.txt"
                victim.write_text("must remain unchanged\n", encoding="utf-8")
                lock_path = Path(env["CVAL_DL_METRIC_LOCK_FILE"])
                lock_path.parent.mkdir(parents=True)
                lock_path.symlink_to(victim)

                completed = subprocess.run(
                    ["bash", str(REPO_ROOT / f"scripts/{script}"), "run-once"],
                    cwd=REPO_ROOT,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                calls = self._calls(root)

                self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
                self.assertEqual(victim.read_text(encoding="utf-8"), "must remain unchanged\n")
                self.assertTrue(lock_path.is_symlink())
                self.assertTrue(any("db-rebuild-dltest-metrics" in line for line in calls))
                self.assertIn("acquired DL metric lock", completed.stdout)

    def test_two_helpers_stay_serialized_when_marker_path_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            lock_directory = root / "baselines"
            lock_directory.mkdir(mode=0o700)
            marker = lock_directory / ".dl-refresh.lock"
            marker.write_text("first marker\n", encoding="utf-8")
            victim = root / "victim.txt"
            victim.write_text("must remain unchanged\n", encoding="utf-8")
            events = root / "events.log"
            worker = root / "worker.py"
            worker.write_text(
                """import sys
import time

events, label = sys.argv[1:]
with open(events, "a", encoding="utf-8") as stream:
    stream.write(label + ":start\\n")
    stream.flush()
time.sleep(0.35)
with open(events, "a", encoding="utf-8") as stream:
    stream.write(label + ":end\\n")
    stream.flush()
""",
                encoding="utf-8",
            )
            helper = REPO_ROOT / "scripts/dl-metric-lock.py"

            first = subprocess.Popen(
                [
                    sys.executable,
                    str(helper),
                    str(marker),
                    "--",
                    sys.executable,
                    str(worker),
                    str(events),
                    "first",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if events.exists() and "first:start" in events.read_text(encoding="utf-8"):
                    break
                time.sleep(0.01)
            else:
                first.kill()
                self.fail("first helper did not start its child")

            marker.unlink()
            marker.symlink_to(victim)
            second = subprocess.Popen(
                [
                    sys.executable,
                    str(helper),
                    str(marker),
                    "--",
                    sys.executable,
                    str(worker),
                    str(events),
                    "second",
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            first_stdout, first_stderr = first.communicate(timeout=10)
            second_stdout, second_stderr = second.communicate(timeout=10)
            event_rows = events.read_text(encoding="utf-8").splitlines()
            victim_text = victim.read_text(encoding="utf-8")

        self.assertEqual(first.returncode, 0, first_stdout + first_stderr)
        self.assertEqual(second.returncode, 0, second_stdout + second_stderr)
        self.assertEqual(
            event_rows,
            ["first:start", "first:end", "second:start", "second:end"],
        )
        self.assertEqual(victim_text, "must remain unchanged\n")

    def test_helper_reports_canonical_directory_replacement_after_child(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            lock_directory = root / "baselines"
            lock_directory.mkdir(mode=0o700)
            marker = lock_directory / ".dl-refresh.lock"
            replacement = root / "replace-directory.py"
            replacement.write_text(
                """import os
import sys

path = sys.argv[1]
os.rename(path, path + ".moved")
os.mkdir(path, 0o700)
""",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts/dl-metric-lock.py"),
                    str(marker),
                    "--",
                    sys.executable,
                    str(replacement),
                    str(lock_directory),
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("path/device/inode changed", completed.stderr)

    def test_directory_replacement_reaps_shell_descendants_before_unlock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            lock_directory = root / "baselines"
            lock_directory.mkdir(mode=0o700)
            moved_lock_directory = root / "baselines.moved"
            marker = lock_directory / ".dl-refresh.lock"
            events = root / "events.log"
            active = root / "active.marker"
            overlap = root / "overlap.marker"
            ignoring_pid_path = root / "ignoring.pid"
            graceful_pid_path = root / "graceful.pid"
            ignoring = root / "ignoring.sh"
            ignoring.write_text(
                """#!/usr/bin/env bash
trap '' INT TERM
printf '%s\n' "$BASHPID" >"$1"
while :; do sleep 0.1; done
""",
                encoding="utf-8",
            )
            graceful = root / "graceful.sh"
            graceful.write_text(
                """#!/usr/bin/env bash
events=$1
active=$2
pid_path=$3
trap 'rm -f "$active"; printf "%s\\n" "graceful:term" >>"$events"; exit 0' TERM
trap 'rm -f "$active"; printf "%s\\n" "graceful:int" >>"$events"; exit 0' INT
printf '%s\n' "$BASHPID" >"$pid_path"
while :; do sleep 0.1; done
""",
                encoding="utf-8",
            )
            wrapper = root / "wrapper.sh"
            wrapper.write_text(
                """#!/usr/bin/env bash
events=$1
active=$2
ignoring=$3
ignoring_pid=$4
graceful=$5
graceful_pid=$6
printf '%s\n' 'first:start' >>"$events"
touch "$active"
bash "$ignoring" "$ignoring_pid" &
bash "$graceful" "$events" "$active" "$graceful_pid" &
wait
""",
                encoding="utf-8",
            )
            checker = root / "checker.sh"
            checker.write_text(
                """#!/usr/bin/env bash
events=$1
active=$2
overlap=$3
shift 3
if [[ -e "$active" ]]; then
    touch "$overlap"
    exit 20
fi
for pid_path in "$@"; do
    pid=$(cat "$pid_path")
    if kill -0 "$pid" 2>/dev/null; then
        touch "$overlap"
        exit 21
    fi
done
printf '%s\n' 'second:start' >>"$events"
""",
                encoding="utf-8",
            )
            helper = REPO_ROOT / "scripts/dl-metric-lock.py"
            first = subprocess.Popen(
                [
                    sys.executable,
                    str(helper),
                    str(marker),
                    "--",
                    "bash",
                    str(wrapper),
                    str(events),
                    str(active),
                    str(ignoring),
                    str(ignoring_pid_path),
                    str(graceful),
                    str(graceful_pid_path),
                ],
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            second: subprocess.Popen[str] | None = None
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if (
                        active.exists()
                        and ignoring_pid_path.exists()
                        and graceful_pid_path.exists()
                    ):
                        break
                    time.sleep(0.01)
                else:
                    self.fail("shell descendants did not start")

                os.rename(lock_directory, moved_lock_directory)
                lock_directory.mkdir(mode=0o700)
                second = subprocess.Popen(
                    [
                        sys.executable,
                        str(helper),
                        str(moved_lock_directory / marker.name),
                        "--",
                        "bash",
                        str(checker),
                        str(events),
                        str(active),
                        str(overlap),
                        str(ignoring_pid_path),
                        str(graceful_pid_path),
                    ],
                    cwd=REPO_ROOT,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                first_stdout, first_stderr = first.communicate(timeout=10)
                second_stdout, second_stderr = second.communicate(timeout=10)
            finally:
                for process in (first, second):
                    if process is not None and process.poll() is None:
                        process.kill()
                        process.communicate(timeout=2)

            ignoring_pid = int(ignoring_pid_path.read_text(encoding="utf-8"))
            graceful_pid = int(graceful_pid_path.read_text(encoding="utf-8"))
            event_rows = events.read_text(encoding="utf-8").splitlines()
            active_exists = active.exists()
            overlap_exists = overlap.exists()

        self.assertNotEqual(first.returncode, 0, first_stdout + first_stderr)
        self.assertIn("path/device/inode changed", first_stderr)
        self.assertEqual(second.returncode, 0, second_stdout + second_stderr)
        self.assertEqual(event_rows, ["first:start", "graceful:term", "second:start"])
        self.assertFalse(active_exists)
        self.assertFalse(overlap_exists)
        for descendant_pid in (ignoring_pid, graceful_pid):
            with self.assertRaises(ProcessLookupError):
                os.kill(descendant_pid, 0)

    def test_helper_forwards_int_and_term_to_command_process_group(self) -> None:
        for signal_number in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signal.Signals(signal_number).name), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                lock_directory = root / "baselines"
                lock_directory.mkdir(mode=0o700)
                marker = lock_directory / ".dl-refresh.lock"
                signal_marker = root / "signal.txt"
                descendant_pid_path = root / "descendant.pid"
                descendant = root / "descendant.py"
                descendant.write_text(
                    """import os
import signal
import sys
import time
from pathlib import Path

marker = Path(sys.argv[1])
pid_path = Path(sys.argv[2])

def handle_signal(signal_number, _frame):
    marker.write_text(signal.Signals(signal_number).name + "\\n", encoding="utf-8")
    raise SystemExit(0)

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)
pid_path.write_text(str(os.getpid()) + "\\n", encoding="utf-8")
while True:
    time.sleep(0.1)
""",
                    encoding="utf-8",
                )
                wrapper = root / "signal-wrapper.sh"
                wrapper.write_text(
                    """#!/usr/bin/env bash
trap 'wait || true; exit 0' INT TERM
"$1" "$2" "$3" "$4"
""",
                    encoding="utf-8",
                )
                helper_process = subprocess.Popen(
                    [
                        sys.executable,
                        str(REPO_ROOT / "scripts/dl-metric-lock.py"),
                        str(marker),
                        "--",
                        "bash",
                        str(wrapper),
                        sys.executable,
                        str(descendant),
                        str(signal_marker),
                        str(descendant_pid_path),
                    ],
                    cwd=REPO_ROOT,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                try:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        if descendant_pid_path.exists():
                            break
                        time.sleep(0.01)
                    else:
                        self.fail("signal descendant did not start")
                    descendant_pid = int(
                        descendant_pid_path.read_text(encoding="utf-8")
                    )
                    helper_process.send_signal(signal_number)
                    stdout, stderr = helper_process.communicate(timeout=5)
                finally:
                    if helper_process.poll() is None:
                        helper_process.kill()
                        helper_process.communicate(timeout=2)

                self.assertEqual(
                    helper_process.returncode,
                    128 + signal_number,
                    stdout + stderr,
                )
                self.assertEqual(
                    signal_marker.read_text(encoding="utf-8").strip(),
                    signal.Signals(signal_number).name,
                )
                with self.assertRaises(ProcessLookupError):
                    os.kill(descendant_pid, 0)

    def test_loops_reject_malformed_or_wrong_version_catalog_rows(self) -> None:
        for row in (
            "cval.operational-target.v0\tstorage\tstorage\tstorage\tstorage\tfalse\t-",
            "cval.operational-target.v1\tstorage\tstorage",
        ):
            with self.subTest(row=row), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(root, [row], fail_target="")
                env["CVAL_BASELINE_BUILD_LOG_DIR"] = str(root / "build-logs")
                completed = subprocess.run(
                    [
                        "bash",
                        str(REPO_ROOT / "scripts/cval-baseline-build.sh"),
                        "run-once",
                    ],
                    cwd=REPO_ROOT,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )

            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("catalog validation failed", completed.stdout)

    @staticmethod
    def _target(call: str) -> str:
        fields = call.split()
        return fields[fields.index("--test-type") + 1]


if __name__ == "__main__":
    unittest.main()
