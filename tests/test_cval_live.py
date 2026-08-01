"""Hermetic safety and workflow tests for the tmux-backed cval-live loop."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts/cval-live.sh"
ORIGIN_SHA = "a" * 40
EXPLICIT_SHA = "b" * 40
NODE = "slc01-cl02-hgx-0001"


FAKE_GIT = r'''#!/usr/bin/env bash
set -euo pipefail
printf 'git\t%s\n' "$*" >>"$FAKE_CALLS"
if [[ -n "$FAKE_GIT_FAIL_MATCH" && "$*" == *"$FAKE_GIT_FAIL_MATCH"* ]]; then
    exit "$FAKE_GIT_FAIL_RC"
fi
if [[ "$*" == *" fetch "* || "$*" == *" worktree prune"* ]]; then
    exit 0
fi
if [[ "$*" == *" checkout "* ]]; then
    printf '%s\n' "${!#}" >"$FAKE_GIT_HEAD_FILE"
    touch "$CVAL_RUNNER_WORKTREE/.fake-worktree"
    if [[ "$FAKE_GIT_INVALIDATE_AFTER_CHECKOUT" == "1" ]]; then
        rm -rf "$CVAL_RUNNER_WORKTREE"
    fi
    exit 0
fi
if [[ "$*" == *" rev-parse --is-inside-work-tree"* ]]; then
    if [[ -d "$CVAL_RUNNER_WORKTREE" && -f "$CVAL_RUNNER_WORKTREE/.fake-worktree" ]]; then
        printf 'true\n'
        exit 0
    fi
    exit 1
fi
if [[ "$*" == *" rev-parse HEAD"* ]]; then
    cat "$FAKE_GIT_HEAD_FILE"
    exit 0
fi
if [[ "$*" == *" rev-parse --verify "* ]]; then
    if [[ "$*" == *"origin/main"* ]]; then
        printf '%s\n' "$FAKE_ORIGIN_SHA"
    else
        printf '%s\n' "$FAKE_EXPLICIT_SHA"
    fi
    exit 0
fi
if [[ "$*" == *" worktree add "* ]]; then
    args=("$@")
    target="${args[${#args[@]}-2]}"
    sha="${args[${#args[@]}-1]}"
    mkdir -p "$target"
    touch "$target/.fake-worktree"
    printf '%s\n' "$sha" >"$FAKE_GIT_HEAD_FILE"
    exit 0
fi
exit 0
'''


FAKE_KUBECTL = r'''#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl\t%s\n' "$*" >>"$FAKE_CALLS"
if [[ " $* " == *" delete "* ]]; then
    printf 'deleted\n'
    exit 0
fi
if [[ " $* " == *" get vcjob "* ]]; then
    if [[ "$*" == *"jsonpath="* ]]; then
        printf '2000-01-01T00:00:00Z'
    else
        cat <<'JSON'
{"items":[{"metadata":{"name":"cval-old","creationTimestamp":"2000-01-01T00:00:00Z"},"status":{"state":{"phase":"Pending"}}}]}
JSON
    fi
    exit 0
fi
printf '{}\n'
'''


FAKE_TIMEOUT = r'''#!/usr/bin/env bash
set -euo pipefail
printf 'timeout\t%s\n' "$*" >>"$FAKE_CALLS"
shift
shift
exec "$@"
'''


FAKE_TMUX = r'''#!/usr/bin/env bash
set -euo pipefail
printf 'tmux\t%s\n' "$*" >>"$FAKE_CALLS"
if [[ "${1:-}" == "has-session" ]]; then
    exit 1
fi
exit 0
'''


FAKE_SLEEP = r'''#!/usr/bin/env bash
set -euo pipefail
printf 'sleep\t%s\n' "$*" >>"$FAKE_CALLS"
exit "$FAKE_SLEEP_RC"
'''


FAKE_PYTHON = r'''#!/usr/bin/env bash
set -euo pipefail
printf 'python\t%s\n' "$*" >>"$FAKE_CALLS"
printf 'python-cwd\t%s\t%s\n' "$PWD" "$*" >>"$FAKE_CALLS"
if [[ "${1:-}" == "-" ]]; then
    exec "$REAL_PYTHON" "$@"
fi
if [[ " $* " == *" -m cval.cli "* ]]; then
    command=""
    for value in "$@"; do
        case "$value" in
            nodes|status|plan|run|jobs) command="$value"; break ;;
        esac
    done
    occurrence=0
    if [[ "$command" == "$FAKE_INVALIDATE_AFTER_COMMAND" ]]; then
        occurrence=$(cat "$FAKE_INVALIDATE_OCCURRENCE_FILE")
        occurrence=$((occurrence + 1))
        printf '%s\n' "$occurrence" >"$FAKE_INVALIDATE_OCCURRENCE_FILE"
    fi
    case "$command" in
        nodes)
            if [[ "$FAKE_FAIL_COMPONENT" == "nodes" ]]; then
                printf 'discovery failed\n' >&2
                exit 31
            fi
            if [[ "$FAKE_TWO_NODES" == "1" ]]; then
                printf '{"nodes":[{"name":"%s","capacity":8,"allocatable":8,"used":0,"resource_ready":true,"free":8},{"name":"%s","capacity":8,"allocatable":8,"used":0,"resource_ready":true,"free":8}],"totals":{"capacity":16,"allocatable":16,"used":0,"free":16}}\n' "$FAKE_NODE" "$FAKE_NODE_2"
            else
                printf '{"nodes":[{"name":"%s","capacity":8,"allocatable":8,"used":0,"resource_ready":true,"free":8}],"totals":{"capacity":8,"allocatable":8,"used":0,"free":8}}\n' "$FAKE_NODE"
            fi
            ;;
        status)
            if [[ "$FAKE_FAIL_COMPONENT" == "status" ]]; then
                printf 'status failed\n' >&2
                exit 32
            fi
            if [[ "$FAKE_PLAN_DUE" == "1" ]]; then
                printf '[]\n'
            else
                printf '[{"node":"%s","test":"all","latest_timestamp":9999999999,"result":"pass"}]\n' "$FAKE_NODE"
            fi
            ;;
        plan)
            if [[ "$FAKE_FAIL_COMPONENT" == "plan" ]]; then
                printf 'plan failed\n' >&2
                exit 33
            fi
            if [[ -n "$FAKE_PLAN_JSON" ]]; then
                printf '%s\n' "$FAKE_PLAN_JSON"
                exit 0
            fi
            if [[ "$FAKE_PLAN_DUE" == "1" ]]; then
                printf '{"dry_run":true,"batch_size":%s,"days_threshold":7,"free_nodes_count":1,"queue_count":1,"planned_jobs":[{"priority":1,"node":"%s","reason":"never-tested","last_tested_timestamp":0,"age_days":null,"job_name":"cval-%s-123"}]}\n' "$CVAL_PLAN_LIMIT" "$FAKE_NODE" "$FAKE_NODE"
            else
                printf '{"dry_run":true,"batch_size":%s,"days_threshold":7,"free_nodes_count":1,"queue_count":0,"planned_jobs":[]}\n' "$CVAL_PLAN_LIMIT"
            fi
            ;;
        run)
            if [[ " $* " == *" --submit "* ]]; then
                printf '{"namespace":"test","dry_run":false,"submitted_count":1,"jobs":[{"node":"%s","job_name":"cval-%s-123","action":"submitted","submitted":true,"stdout":"created"}]}\n' "$FAKE_NODE" "$FAKE_NODE"
            else
                if [[ "$FAKE_PLAN_DUE" == "1" ]]; then
                    printf '{"dry_run":true,"batch_size":%s,"days_threshold":7,"free_nodes_count":1,"queue_count":1,"planned_jobs":[{"priority":1,"node":"%s","reason":"never-tested","last_tested_timestamp":0,"age_days":null,"job_name":"cval-%s-123"}]}\n' "$CVAL_PLAN_LIMIT" "$FAKE_NODE" "$FAKE_NODE"
                else
                    printf '{"dry_run":true,"batch_size":%s,"days_threshold":7,"free_nodes_count":1,"queue_count":0,"planned_jobs":[]}\n' "$CVAL_PLAN_LIMIT"
                fi
            fi
            ;;
        jobs)
            if [[ "$FAKE_FAIL_COMPONENT" == "jobs" ]]; then
                printf 'jobs failed\n' >&2
                exit 34
            fi
            printf '[{"job_name":"cval-%s-123","phase":"%s"}]\n' "$FAKE_NODE" "$FAKE_JOB_PHASE"
            ;;
        *)
            printf '{}\n'
            ;;
    esac
    if [[ -n "$FAKE_INVALIDATE_AFTER_COMMAND" && "$command" == "$FAKE_INVALIDATE_AFTER_COMMAND" && "$occurrence" == "$FAKE_INVALIDATE_AFTER_OCCURRENCE" ]]; then
        case "$FAKE_INVALIDATE_ACTION" in
            remove)
                rm -rf "$CVAL_RUNNER_WORKTREE"
                ;;
            replace)
                rm -rf "$CVAL_RUNNER_WORKTREE"
                mkdir -p "$CVAL_RUNNER_WORKTREE"
                touch "$CVAL_RUNNER_WORKTREE/.fake-worktree"
                ;;
            invalidate)
                rm -f "$CVAL_RUNNER_WORKTREE/.fake-worktree"
                ;;
            head)
                printf '%s\n' "$FAKE_CHANGED_SHA" >"$FAKE_GIT_HEAD_FILE"
                ;;
        esac
    elif [[ "$command" == "jobs" && "$FAKE_INVALIDATE_AFTER_JOBS" == "1" ]]; then
        rm -rf "$CVAL_RUNNER_WORKTREE"
    fi
    exit 0
fi
exec "$REAL_PYTHON" "$@"
'''


class CvalLiveTests(unittest.TestCase):
    def _environment(
        self,
        root: Path,
        *,
        due: bool = True,
        mode: str | None = None,
        confirm: str | None = None,
        prune_confirm: str | None = None,
        git_ref: str | None = None,
        fail_component: str = "",
    ) -> dict[str, str]:
        fake_bin = root / "bin"
        fake_bin.mkdir()
        for name, content in (
            ("git", FAKE_GIT),
            ("kubectl", FAKE_KUBECTL),
            ("python", FAKE_PYTHON),
            ("timeout", FAKE_TIMEOUT),
            ("tmux", FAKE_TMUX),
            ("sleep", FAKE_SLEEP),
        ):
            path = fake_bin / name
            path.write_text(content, encoding="utf-8")
            path.chmod(0o755)
        calls = root / "calls.log"
        calls.touch()
        source = root / "source"
        source.mkdir()
        worktree = root / "worktree"
        worktree.mkdir()
        (worktree / ".fake-worktree").touch()
        git_head = root / "git-head"
        git_head.write_text(f"{ORIGIN_SHA}\n", encoding="utf-8")
        invalidate_occurrence = root / "invalidate-occurrence"
        invalidate_occurrence.write_text("0\n", encoding="utf-8")
        config = root / "config.toml"
        config.write_text("", encoding="utf-8")
        env = os.environ | {
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "REAL_PYTHON": sys.executable,
            "FAKE_CALLS": str(calls),
            "FAKE_ORIGIN_SHA": ORIGIN_SHA,
            "FAKE_EXPLICIT_SHA": EXPLICIT_SHA,
            "FAKE_NODE": NODE,
            "FAKE_NODE_2": "slc01-cl02-hgx-0002",
            "FAKE_TWO_NODES": "0",
            "FAKE_PLAN_DUE": "1" if due else "0",
            "FAKE_FAIL_COMPONENT": fail_component,
            "FAKE_GIT_FAIL_MATCH": "",
            "FAKE_GIT_FAIL_RC": "42",
            "FAKE_GIT_INVALIDATE_AFTER_CHECKOUT": "0",
            "FAKE_INVALIDATE_AFTER_JOBS": "0",
            "FAKE_INVALIDATE_AFTER_COMMAND": "",
            "FAKE_INVALIDATE_AFTER_OCCURRENCE": "1",
            "FAKE_INVALIDATE_ACTION": "",
            "FAKE_INVALIDATE_OCCURRENCE_FILE": str(invalidate_occurrence),
            "FAKE_CHANGED_SHA": "c" * 40,
            "FAKE_GIT_HEAD_FILE": str(git_head),
            "FAKE_PLAN_JSON": "",
            "FAKE_JOB_PHASE": "Completed",
            "FAKE_SLEEP_RC": "0",
            "CVAL_SOURCE_REPO": str(source),
            "CVAL_CONFIG": str(config),
            "CVAL_RUNNER_WORKTREE": str(worktree),
            "CVAL_LIVE_LOG_DIR": str(root / "logs"),
            "CVAL_BATCH_SIZE": "1",
            "CVAL_PLAN_LIMIT": "9",
            "CVAL_DAYS_THRESHOLD": "7",
            "CVAL_KUBECTL_TIMEOUT_SECONDS": "17",
            "CVAL_WATCH_POLL_SECONDS": "0",
            "CVAL_LOOP_SLEEP_SECONDS": "0",
            "CVAL_TMUX_SESSION": "test-live",
        }
        for key in (
            "CVAL_LIVE_MODE",
            "CVAL_LIVE_CONFIRM",
            "CVAL_PRUNE_CONFIRM",
            "CVAL_GIT_REF",
        ):
            env.pop(key, None)
        if mode is not None:
            env["CVAL_LIVE_MODE"] = mode
        if confirm is not None:
            env["CVAL_LIVE_CONFIRM"] = confirm
        if prune_confirm is not None:
            env["CVAL_PRUNE_CONFIRM"] = prune_confirm
        if git_ref is not None:
            env["CVAL_GIT_REF"] = git_ref
        return env

    @staticmethod
    def _run(env: dict[str, str], command: str = "run-once") -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT), command],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    @staticmethod
    def _calls(env: dict[str, str]) -> list[str]:
        return Path(env["FAKE_CALLS"]).read_text(encoding="utf-8").splitlines()

    @staticmethod
    def _latest_artifact(env: dict[str, str], name: str) -> Path:
        matches = sorted(Path(env["CVAL_LIVE_LOG_DIR"]).glob(f"*/{name}"))
        if not matches:
            raise AssertionError(f"missing artifact: {name}")
        return matches[-1]

    @staticmethod
    def _write_resume_submission(env: dict[str, str]) -> None:
        old = Path(env["CVAL_LIVE_LOG_DIR"]) / "old"
        old.mkdir(parents=True)
        (old / "submit.json").write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "node": NODE,
                            "job_name": f"cval-{NODE}-123",
                            "submitted": True,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

    def test_default_audit_is_complete_dry_run_without_resume_or_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self._environment(root)
            old = Path(env["CVAL_LIVE_LOG_DIR"]) / "old"
            old.mkdir(parents=True)
            (old / "submit.json").write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "node": NODE,
                                "job_name": f"cval-{NODE}-old",
                                "submitted": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            completed = self._run(env)
            calls = self._calls(env)
            summary = json.loads(
                self._latest_artifact(env, "audit-summary.json").read_text(encoding="utf-8")
            )

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        joined = "\n".join(calls)
        self.assertIn("python\t-m cval.cli --config", joined)
        self.assertIn(" nodes --output json", joined)
        self.assertIn(" status --output json", joined)
        self.assertIn(" plan --free-nodes", joined)
        self.assertNotIn("--submit", joined)
        self.assertNotIn(" -m cval.cli jobs ", joined)
        self.assertFalse(any("kubectl\t" in line for line in calls))
        self.assertNotIn(" delete ", joined)
        self.assertNotIn(" apply ", joined)
        self.assertNotIn(" create ", joined)
        self.assertEqual(summary["mode"], "audit")
        self.assertEqual(summary["action"], "dry-run")
        self.assertEqual(summary["cluster_mutations"], 0)
        self.assertEqual(summary["selected_nodes"], [NODE])
        self.assertIn("free_nodes_count=1", completed.stdout)
        self.assertIn("queue_count=1 planned_count=1", completed.stdout)

    def test_submit_wrong_confirmation_fails_before_git_or_kubernetes(self) -> None:
        for confirm in (None, "wrong"):
            with self.subTest(confirm=confirm), tempfile.TemporaryDirectory() as tmpdir:
                env = self._environment(Path(tmpdir), mode="submit", confirm=confirm)
                completed = self._run(env)
                calls = self._calls(env)

            self.assertEqual(completed.returncode, 2)
            self.assertIn("CVAL_LIVE_CONFIRM=submit", completed.stderr)
            self.assertEqual(calls, [])

    def test_submit_exact_gate_passes_cli_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = self._environment(Path(tmpdir), mode="submit", confirm="submit")
            completed = self._run(env)
            calls = self._calls(env)

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        submit_calls = [
            line
            for line in calls
            if line.startswith("python\t") and " --submit " in f" {line} "
        ]
        self.assertEqual(len(submit_calls), 1)
        self.assertIn("--confirm submit", submit_calls[0])
        self.assertIn("pruning=disabled", completed.stdout)
        self.assertFalse(any("kubectl\t" in line and " delete " in f" {line} " for line in calls))

    def test_prune_is_default_off_and_separately_gated_with_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            wrong_env = self._environment(
                Path(tmpdir),
                due=True,
                mode="submit",
                confirm="submit",
                prune_confirm="wrong",
            )
            wrong = self._run(wrong_env)
            wrong_calls = self._calls(wrong_env)

        self.assertEqual(wrong.returncode, 0, wrong.stdout + wrong.stderr)
        self.assertIn("pruning=disabled", wrong.stdout)
        self.assertFalse(
            any("kubectl\t" in line and " delete " in f" {line} " for line in wrong_calls)
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            env = self._environment(
                Path(tmpdir),
                due=True,
                mode="submit",
                confirm="submit",
                prune_confirm="delete-pending",
            )
            completed = self._run(env)
            calls = self._calls(env)

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        kubectl_calls = [line for line in calls if line.startswith("kubectl\t")]
        self.assertTrue(any(" get vcjob " in f" {line} " for line in kubectl_calls))
        self.assertTrue(any(" delete vcjob " in f" {line} " for line in kubectl_calls))
        self.assertTrue(all("--request-timeout=17s" in line for line in kubectl_calls))
        timeout_calls = [line for line in calls if line.startswith("timeout\t")]
        self.assertTrue(timeout_calls)
        self.assertTrue(all("--foreground 17s kubectl" in line for line in timeout_calls))
        self.assertIn("pruning=enabled", completed.stdout)

    def test_default_ref_fetches_origin_main_and_explicit_ref_does_not(self) -> None:
        for explicit, expected_sha, expected_source, expect_fetch in (
            (None, ORIGIN_SHA, "origin-main", True),
            ("release-ref", EXPLICIT_SHA, "explicit", False),
        ):
            with self.subTest(explicit=explicit), tempfile.TemporaryDirectory() as tmpdir:
                env = self._environment(Path(tmpdir), git_ref=explicit)
                completed = self._run(env)
                calls = self._calls(env)
                summary = json.loads(
                    self._latest_artifact(env, "audit-summary.json").read_text(
                        encoding="utf-8"
                    )
                )

            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertEqual(summary["git_ref"], expected_sha)
            self.assertEqual(summary["git_ref_source"], expected_source)
            source_fetches = [
                line
                for line in calls
                if f"-C {env['CVAL_SOURCE_REPO']} fetch --quiet origin main" in line
            ]
            self.assertEqual(bool(source_fetches), expect_fetch)
            self.assertTrue(
                any(
                    f"-C {env['CVAL_RUNNER_WORKTREE']} fetch --quiet origin main" in line
                    for line in calls
                )
            )
            self.assertIn(f"source={expected_source} sha={expected_sha}", completed.stdout)

    def test_git_fetch_failure_stops_cycle_before_ref_checkout_or_components(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = self._environment(Path(tmpdir))
            env["FAKE_GIT_FAIL_MATCH"] = "fetch --quiet origin main"
            env["FAKE_SLEEP_RC"] = "55"
            failed = self._run(env, "run-loop")
            calls = self._calls(env)

        self.assertEqual(failed.returncode, 55, failed.stdout + failed.stderr)
        joined = "\n".join(calls)
        self.assertIn("git fetch failed source=origin-main exit_code=42", failed.stderr)
        self.assertEqual(failed.stdout.count("cycle failed; see logs above"), 1)
        self.assertNotIn("rev-parse --verify", joined)
        self.assertNotIn("checkout", joined)
        self.assertFalse(any("-m cval.cli" in line for line in calls))
        self.assertNotIn("audit component=", failed.stdout)
        self.assertNotIn("audit cycle complete", failed.stdout)

    def test_worktree_removed_after_checkout_fails_closed_before_cval_cli(self) -> None:
        for scenario, mode, operation in (
            ("audit", "audit", "audit"),
            ("submit", "submit", "submit"),
            ("resume", "submit", "resume-status"),
        ):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(
                    root,
                    mode=mode,
                    confirm="submit" if mode == "submit" else None,
                )
                env["FAKE_GIT_INVALIDATE_AFTER_CHECKOUT"] = "1"
                if scenario == "resume":
                    old = Path(env["CVAL_LIVE_LOG_DIR"]) / "old"
                    old.mkdir(parents=True)
                    (old / "submit.json").write_text(
                        json.dumps(
                            {
                                "jobs": [
                                    {
                                        "node": NODE,
                                        "job_name": f"cval-{NODE}-123",
                                        "submitted": True,
                                    }
                                ]
                            }
                        ),
                        encoding="utf-8",
                    )

                failed = self._run(env)
                calls = self._calls(env)
                summaries = list(
                    Path(env["CVAL_LIVE_LOG_DIR"]).glob("*/audit-summary.json")
                )

            joined = "\n".join(calls)
            self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
            self.assertIn(
                f"runner worktree entry failed operation={operation}", failed.stderr
            )
            self.assertFalse(any("-m cval.cli" in line for line in calls))
            self.assertNotIn(" status --output json", joined)
            self.assertNotIn(" jobs --jobs ", joined)
            self.assertNotIn(" plan --free-nodes", joined)
            self.assertNotIn("--submit", joined)
            self.assertNotIn("cycle complete", failed.stdout)
            if scenario == "audit":
                self.assertEqual(summaries, [])
                self.assertNotIn(
                    "audit component=priority-and-render status=ok", failed.stdout
                )
            if scenario == "resume":
                self.assertIn(
                    "resume observation failed closed; deferring new cycle",
                    failed.stdout,
                )

    def test_post_entry_worktree_invalidation_fails_before_any_later_cli(self) -> None:
        for scenario, command in (
            ("audit", "nodes"),
            ("submit", "nodes"),
            ("resume", "jobs"),
        ):
            for action in ("invalidate", "remove", "replace", "head"):
                with self.subTest(scenario=scenario, action=action), tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    mode = "submit" if scenario in {"submit", "resume"} else "audit"
                    env = self._environment(
                        root,
                        mode=mode,
                        confirm="submit" if mode == "submit" else None,
                    )
                    env["FAKE_INVALIDATE_AFTER_COMMAND"] = command
                    env["FAKE_INVALIDATE_ACTION"] = action
                    if scenario == "resume":
                        self._write_resume_submission(env)

                    failed = self._run(env)
                    calls = self._calls(env)

                cli_calls = [
                    line
                    for line in calls
                    if line.startswith("python\t") and "-m cval.cli" in line
                ]
                self.assertEqual(failed.returncode, 2, failed.stdout + failed.stderr)
                self.assertIn("runner worktree assertion failed", failed.stderr)
                self.assertEqual(len(cli_calls), 1)
                self.assertIn(f" {command} ", f" {cli_calls[0]} ")
                self.assertFalse(any(" --submit " in f" {line} " for line in calls))
                self.assertNotIn("cycle complete", failed.stdout)

    def test_invalidation_immediately_after_entry_prevents_first_cli(self) -> None:
        bash_env_text = r'''pushd() {
    builtin pushd "$@"
    local rc=$?
    if (( rc == 0 )); then
        case "$FAKE_INVALIDATE_ACTION" in
            remove)
                rm -rf "$CVAL_RUNNER_WORKTREE"
                ;;
            replace)
                rm -rf "$CVAL_RUNNER_WORKTREE"
                mkdir -p "$CVAL_RUNNER_WORKTREE"
                touch "$CVAL_RUNNER_WORKTREE/.fake-worktree"
                ;;
            invalidate)
                rm -f "$CVAL_RUNNER_WORKTREE/.fake-worktree"
                ;;
            head)
                printf '%s\n' "$FAKE_CHANGED_SHA" >"$FAKE_GIT_HEAD_FILE"
                ;;
        esac
    fi
    return "$rc"
}
'''
        for scenario in ("audit", "submit", "resume"):
            for action in ("invalidate", "remove", "replace", "head"):
                with self.subTest(scenario=scenario, action=action), tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    mode = "submit" if scenario in {"submit", "resume"} else "audit"
                    env = self._environment(
                        root,
                        mode=mode,
                        confirm="submit" if mode == "submit" else None,
                    )
                    env["FAKE_INVALIDATE_ACTION"] = action
                    bash_env = root / "bash-env.sh"
                    bash_env.write_text(bash_env_text, encoding="utf-8")
                    env["BASH_ENV"] = str(bash_env)
                    if scenario == "resume":
                        self._write_resume_submission(env)

                    failed = self._run(env)
                    calls = self._calls(env)

                cli_calls = [line for line in calls if "-m cval.cli" in line]
                self.assertEqual(failed.returncode, 2, failed.stdout + failed.stderr)
                self.assertIn("runner worktree assertion failed", failed.stderr)
                self.assertEqual(cli_calls, [])
                self.assertFalse(any(" --submit " in f" {line} " for line in calls))
                self.assertNotIn("cycle complete", failed.stdout)

    def test_post_job_observation_invalidation_prevents_later_cli_and_submit(self) -> None:
        for scenario in ("submit", "resume"):
            for action in ("invalidate", "remove", "replace", "head"):
                with self.subTest(scenario=scenario, action=action), tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    env = self._environment(root, mode="submit", confirm="submit")
                    env["FAKE_JOB_PHASE"] = "Running"
                    env["FAKE_INVALIDATE_AFTER_COMMAND"] = "jobs"
                    env["FAKE_INVALIDATE_AFTER_OCCURRENCE"] = (
                        "1" if scenario == "submit" else "2"
                    )
                    env["FAKE_INVALIDATE_ACTION"] = action
                    if scenario == "resume":
                        self._write_resume_submission(env)

                    failed = self._run(env)
                    calls = self._calls(env)

                cli_calls = [
                    line
                    for line in calls
                    if line.startswith("python\t") and "-m cval.cli" in line
                ]
                jobs_indexes = [
                    index
                    for index, line in enumerate(cli_calls)
                    if " jobs --jobs " in line
                ]
                submit_calls = [
                    line for line in cli_calls if " --submit " in f" {line} "
                ]
                self.assertEqual(failed.returncode, 2, failed.stdout + failed.stderr)
                self.assertIn("runner worktree assertion failed", failed.stderr)
                self.assertTrue(jobs_indexes)
                self.assertEqual(jobs_indexes[-1], len(cli_calls) - 1)
                later_cli_calls = cli_calls[jobs_indexes[-1] + 1 :]
                self.assertEqual(later_cli_calls, [])
                self.assertFalse(
                    any(" --submit " in f" {line} " for line in later_cli_calls)
                )
                if scenario == "submit":
                    self.assertEqual(len(submit_calls), 1)
                    self.assertLess(cli_calls.index(submit_calls[0]), jobs_indexes[-1])
                else:
                    self.assertEqual(len(jobs_indexes), 2)
                    self.assertEqual(submit_calls, [])
                    self.assertIn(
                        "resume observation failed closed; deferring new cycle",
                        failed.stdout,
                    )
                self.assertNotIn("cycle complete", failed.stdout)

    def test_popd_failure_fails_cycle_and_preserves_primary_failure(self) -> None:
        for component, expected_rc in (("", 2), ("plan", 33)):
            with self.subTest(component=component), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(root, fail_component=component)
                bash_env = root / "bash-env.sh"
                bash_env.write_text("popd() { return 73; }\n", encoding="utf-8")
                env["BASH_ENV"] = str(bash_env)

                failed = self._run(env)
                calls = self._calls(env)
                summaries = list(
                    Path(env["CVAL_LIVE_LOG_DIR"]).glob("*/audit-summary.json")
                )

            self.assertEqual(
                failed.returncode, expected_rc, failed.stdout + failed.stderr
            )
            self.assertIn(
                "runner worktree exit failed operation=audit", failed.stderr
            )
            self.assertEqual(summaries, [])
            self.assertNotIn("audit cycle complete", failed.stdout)
            cli_cwds = [
                line
                for line in calls
                if line.startswith("python-cwd\t") and "-m cval.cli" in line
            ]
            self.assertTrue(cli_cwds)
            self.assertTrue(
                all(
                    line.startswith(f"python-cwd\t{env['CVAL_RUNNER_WORKTREE']}\t")
                    for line in cli_cwds
                )
            )

    def test_resume_watch_worktree_loss_fails_before_second_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env = self._environment(root, mode="submit", confirm="submit")
            env["FAKE_JOB_PHASE"] = "Running"
            env["FAKE_INVALIDATE_AFTER_JOBS"] = "1"
            old = Path(env["CVAL_LIVE_LOG_DIR"]) / "old"
            old.mkdir(parents=True)
            (old / "submit.json").write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "node": NODE,
                                "job_name": f"cval-{NODE}-123",
                                "submitted": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            failed = self._run(env)
            calls = self._calls(env)

        self.assertEqual(failed.returncode, 2, failed.stdout + failed.stderr)
        self.assertIn(
            "runner worktree assertion failed context=resume-status-jobs-after",
            failed.stderr,
        )
        self.assertIn(
            "resume observation failed closed; deferring new cycle", failed.stdout
        )
        cli_calls = [
            line for line in calls if line.startswith("python\t") and "-m cval.cli" in line
        ]
        self.assertEqual(len(cli_calls), 1)
        self.assertIn(" jobs --jobs ", cli_calls[0])
        self.assertNotIn(" status --output json", "\n".join(cli_calls))
        self.assertNotIn("--submit", "\n".join(calls))
        self.assertFalse(any(" nodes --output json" in line for line in cli_calls))
        self.assertFalse(any(" plan --free-nodes" in line for line in cli_calls))
        jobs_cwds = [
            line
            for line in calls
            if line.startswith("python-cwd\t") and " jobs --jobs " in line
        ]
        self.assertEqual(len(jobs_cwds), 1)
        self.assertTrue(
            jobs_cwds[0].startswith(
                f"python-cwd\t{env['CVAL_RUNNER_WORKTREE']}\t"
            )
        )

    def test_zero_due_is_success_and_component_failures_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = self._environment(Path(tmpdir), due=False)
            completed = self._run(env)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertIn("state=no-due-candidates", completed.stdout)
        self.assertIn("queue_count=0 planned_count=0", completed.stdout)

        for component, diagnostic in (
            ("nodes", "component=discovery status=failed exit_code=31"),
            ("plan", "audit component=plan status=failed exit_code=33"),
        ):
            with self.subTest(component=component), tempfile.TemporaryDirectory() as tmpdir:
                env = self._environment(Path(tmpdir), fail_component=component)
                failed = self._run(env)
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn(diagnostic, failed.stdout)
            self.assertNotIn("state=no-due-candidates", failed.stdout)

    def test_malformed_plan_json_is_a_failed_component_without_audit_summary(self) -> None:
        valid_job = {
            "priority": 1,
            "node": NODE,
            "reason": "never-tested",
            "last_tested_timestamp": 0,
            "age_days": None,
            "job_name": f"cval-{NODE}-123",
        }
        valid_plan = {
            "dry_run": True,
            "batch_size": 9,
            "days_threshold": 7,
            "free_nodes_count": 1,
            "queue_count": 1,
            "planned_jobs": [valid_job],
        }
        malformed: list[tuple[str, object]] = [
            ("not-object", []),
            ("missing-key", {key: value for key, value in valid_plan.items() if key != "dry_run"}),
            ("extra-key", valid_plan | {"submitted_count": 0}),
            ("non-dry", valid_plan | {"dry_run": False}),
            ("bool-batch", valid_plan | {"batch_size": True}),
            ("zero-batch", valid_plan | {"batch_size": 0}),
            ("batch-request-mismatch", valid_plan | {"batch_size": 1}),
            ("bool-free", valid_plan | {"free_nodes_count": False}),
            ("free-snapshot-mismatch", valid_plan | {"free_nodes_count": 2}),
            ("negative-queue", valid_plan | {"queue_count": -1}),
            ("bool-queue", valid_plan | {"queue_count": True}),
            ("negative-days", valid_plan | {"days_threshold": -1}),
            ("bool-days", valid_plan | {"days_threshold": True}),
            ("queue-exceeds-free", valid_plan | {"queue_count": 2}),
            ("count-mismatch", valid_plan | {"planned_jobs": []}),
            ("jobs-not-list", valid_plan | {"planned_jobs": {}}),
            ("blank-node", valid_plan | {"planned_jobs": [valid_job | {"node": ""}]}),
            ("blank-job-name", valid_plan | {"planned_jobs": [valid_job | {"job_name": " "}]}),
            ("bool-priority", valid_plan | {"planned_jobs": [valid_job | {"priority": True}]}),
            ("submitted-field", valid_plan | {"planned_jobs": [valid_job | {"submitted": False}]}),
            (
                "duplicate-node",
                valid_plan
                | {
                    "batch_size": 9,
                    "free_nodes_count": 2,
                    "queue_count": 2,
                    "planned_jobs": [valid_job, valid_job | {"job_name": "other"}],
                },
            ),
            (
                "duplicate-job-name",
                valid_plan
                | {
                    "batch_size": 9,
                    "free_nodes_count": 2,
                    "queue_count": 2,
                    "planned_jobs": [valid_job, valid_job | {"node": "other"}],
                },
            ),
        ]
        for name, payload in malformed:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                env = self._environment(Path(tmpdir))
                if name in {"duplicate-node", "duplicate-job-name"}:
                    env["FAKE_TWO_NODES"] = "1"
                env["FAKE_PLAN_JSON"] = json.dumps(payload)
                failed = self._run(env)
                summaries = list(Path(env["CVAL_LIVE_LOG_DIR"]).glob("*/audit-summary.json"))

            self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
            self.assertIn("audit component=plan status=invalid-json", failed.stdout)
            self.assertEqual(summaries, [])

    def test_current_cycle_unknown_or_jobs_failure_keeps_slot_and_submits_no_replacement(self) -> None:
        for phase, fail_component, diagnostic in (
            ("Unknown", "", "is indeterminate; retaining active jobs and ending cycle"),
            ("Completed", "jobs", "jobs observation failed exit_code=34"),
        ):
            with self.subTest(phase=phase, fail_component=fail_component), tempfile.TemporaryDirectory() as tmpdir:
                env = self._environment(
                    Path(tmpdir), mode="submit", confirm="submit"
                )
                env["FAKE_JOB_PHASE"] = phase
                env["FAKE_FAIL_COMPONENT"] = fail_component
                failed = self._run(env)
                calls = self._calls(env)

            submit_calls = [
                line
                for line in calls
                if line.startswith("python\t") and " --submit " in f" {line} "
            ]
            self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
            self.assertEqual(len(submit_calls), 1)
            self.assertIn(diagnostic, failed.stdout)
            self.assertNotIn("no longer active", failed.stdout)
            self.assertNotIn("cycle complete", failed.stdout)

    def test_resume_unknown_or_jobs_failure_defers_new_cycle_and_keeps_tracking(self) -> None:
        for phase, fail_component, diagnostic in (
            ("Unknown", "", "resume jobs observation is indeterminate"),
            ("Completed", "jobs", "resume jobs observation failed exit_code=34"),
        ):
            with self.subTest(phase=phase, fail_component=fail_component), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(root, mode="submit", confirm="submit")
                env["FAKE_JOB_PHASE"] = phase
                env["FAKE_FAIL_COMPONENT"] = fail_component
                old = Path(env["CVAL_LIVE_LOG_DIR"]) / "old"
                old.mkdir(parents=True)
                (old / "submit.json").write_text(
                    json.dumps(
                        {
                            "jobs": [
                                {
                                    "node": NODE,
                                    "job_name": f"cval-{NODE}-123",
                                    "submitted": True,
                                }
                            ]
                        }
                    ),
                    encoding="utf-8",
                )
                failed = self._run(env)
                calls = self._calls(env)

            self.assertEqual(failed.returncode, 2, failed.stdout + failed.stderr)
            self.assertIn(diagnostic, failed.stdout)
            self.assertIn("resume observation failed closed; deferring new cycle", failed.stdout)
            self.assertFalse(any(" --submit " in f" {line} " for line in calls))
            self.assertFalse(any(" nodes --output json" in line for line in calls))
            self.assertFalse(any(" plan --free-nodes" in line for line in calls))
            self.assertNotIn("no longer active", failed.stdout)

    def test_tmux_start_propagates_mode_gates_limits_and_only_current_explicit_ref(self) -> None:
        for git_ref in (None, "release-ref"):
            with self.subTest(git_ref=git_ref), tempfile.TemporaryDirectory() as tmpdir:
                env = self._environment(
                    Path(tmpdir),
                    mode="submit",
                    confirm="submit",
                    prune_confirm="delete-pending",
                    git_ref=git_ref,
                )
                completed = self._run(env, "start")
                tmux_call = "\n".join(
                    line for line in self._calls(env) if line.startswith("tmux\tnew-session")
                )

            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertIn("CVAL_LIVE_MODE=submit", tmux_call)
            self.assertIn("CVAL_LIVE_CONFIRM=submit", tmux_call)
            self.assertIn("CVAL_PRUNE_CONFIRM=delete-pending", tmux_call)
            self.assertIn("CVAL_PLAN_LIMIT=9", tmux_call)
            self.assertIn("CVAL_KUBECTL_TIMEOUT_SECONDS=17", tmux_call)
            if git_ref is None:
                self.assertNotIn("CVAL_GIT_REF=", tmux_call)
            else:
                self.assertIn("CVAL_GIT_REF=release-ref", tmux_call)

    def test_status_and_stop_remain_nonmutating_without_submit_confirmation(self) -> None:
        for command in ("status", "stop"):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as tmpdir:
                env = self._environment(Path(tmpdir))
                env["CVAL_LIVE_MODE"] = "submit"
                completed = self._run(env, command)
                calls = self._calls(env)

            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertTrue(any(line.startswith("tmux\t") for line in calls))
            self.assertFalse(any(line.startswith("git\t") for line in calls))
            self.assertFalse(any(line.startswith("kubectl\t") for line in calls))
            self.assertFalse(any("-m cval.cli" in line for line in calls))

    def test_non_operational_commands_ignore_malformed_or_missing_config(self) -> None:
        for config_kind in ("malformed", "missing"):
            for command in ("stop", "status", "attach", "help"):
                with self.subTest(config_kind=config_kind, command=command), tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    env = self._environment(root)
                    config = root / "bad.toml"
                    if config_kind == "malformed":
                        config.write_text("not = [valid\n", encoding="utf-8")
                    env["CVAL_CONFIG"] = str(config)
                    completed = self._run(env, command)
                    calls = self._calls(env)

                self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
                self.assertFalse(any(line.startswith("python\t") for line in calls))
                if command in {"stop", "status", "attach"}:
                    self.assertTrue(any(line.startswith("tmux\t") for line in calls))
                else:
                    self.assertEqual(calls, [])

    def test_operational_commands_still_parse_config_after_submit_startup_gate(self) -> None:
        for command in ("start", "run-once", "run-loop"):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                env = self._environment(root)
                config = root / "bad.toml"
                config.write_text("not = [valid\n", encoding="utf-8")
                env["CVAL_CONFIG"] = str(config)
                failed = self._run(env, command)
                calls = self._calls(env)

            self.assertNotEqual(failed.returncode, 0)
            self.assertTrue(any(line.startswith("python\t-") for line in calls))
            self.assertFalse(any(line.startswith("git\t") for line in calls))
            self.assertFalse(any(line.startswith("kubectl\t") for line in calls))
            self.assertFalse(any(line.startswith("tmux\t") for line in calls))

    def test_script_contains_no_unbounded_direct_kubectl_calls(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('timeout --foreground "${KUBECTL_TIMEOUT_SECONDS}s"', text)
        self.assertIn('kubectl --request-timeout="${KUBECTL_TIMEOUT_SECONDS}s"', text)
        executable_lines = [
            line.strip()
            for line in text.splitlines()
            if "kubectl " in line and not line.lstrip().startswith("#")
        ]
        self.assertEqual(
            [line for line in executable_lines if line.startswith("kubectl ")],
            ['kubectl --request-timeout="${KUBECTL_TIMEOUT_SECONDS}s" "$@"'],
        )

    def test_script_contains_no_unchecked_runner_worktree_directory_changes(self) -> None:
        executable_lines = [
            line.strip()
            for line in SCRIPT.read_text(encoding="utf-8").splitlines()
            if not line.lstrip().startswith("#")
        ]
        self.assertEqual(
            [line for line in executable_lines if line.startswith("pushd ")],
            ['pushd "$RUNNER_WORKTREE" >/dev/null 2>&1 || rc=$?'],
        )
        self.assertEqual(
            [line for line in executable_lines if line.startswith("popd ")],
            ['popd >/dev/null 2>&1 || popd_rc=$?'],
        )

    def test_every_post_entry_cval_invocation_has_adjacent_guards(self) -> None:
        lines = SCRIPT.read_text(encoding="utf-8").splitlines()
        invocation_count = 0
        for start, line in enumerate(lines):
            if "python -m cval.cli" not in line:
                continue
            invocation_count += 1
            end = start
            while lines[end].rstrip().endswith("\\"):
                end += 1
            before = "\n".join(lines[max(0, start - 6) : start])
            after = "\n".join(lines[end + 1 : end + 7])
            self.assertIn(
                "assert_runner_worktree ",
                before,
                f"missing guard immediately before c-val invocation at source line {start + 1}",
            )
            self.assertIn(
                "assert_runner_worktree ",
                after,
                f"missing guard immediately after c-val invocation at source line {start + 1}",
            )
        self.assertEqual(invocation_count, 11)

        git_commands = [
            line.strip()
            for line in lines
            if line.strip().startswith("git ")
        ]
        self.assertTrue(git_commands)
        self.assertTrue(all(line.startswith('git -C "') for line in git_commands))


if __name__ == "__main__":
    unittest.main()
