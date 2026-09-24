"""Targeted single-node validation: submit, live-track, and report raw evidence.

``cval validate --node <node> --git-ref <commit> --submit --confirm submit`` is
the cluster-first development path. It submits one validation job for a
specific eligible node, streams progress while the in-pod tests run, and
reports the canonical raw evidence written by that job.

The pure helpers (`parse_test_progress`, `build_validation_report`,
`render_validation_report`) are import-only and
unit-tested; the orchestration in `run_node_validation` wires them to the live
cluster through a `KubectlClient`.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Callable

from cval.config import CvalConfig, is_exact_commit, load_config
from cval.jobs.manager import submit_workflow_plan
from cval.jobs.monitor import TERMINAL_PHASES, get_job_phase
from cval.jobs.renderer import render_validation_job_from_file
from cval.k8s.client import KubectlClient
from cval.k8s.discovery import NodeStatus, describe_node
from cval.models import PlannedJob, QueueCandidate, WorkflowPlan
from cval.policy import ExecutionPolicy, PolicyViolation
from cval.storage.status import get_latest_status_rows
from cval.validation.builtins import (
    BUILTIN_DB_UPDATE_DONE_MARKER,
    BUILTIN_DONE_MARKERS,
    BUILTIN_FINAL_RESULT_PREFIX,
    BUILTIN_RUNNING_MARKERS,
    BUILTIN_SKIPPED_MARKERS,
    BUILTIN_TEST_IDS,
)

# In-pod run-test.sh log markers that signal each phase finished.
_FINAL_RESULT_RE = re.compile(
    re.escape(BUILTIN_FINAL_RESULT_PREFIX)
    + "".join(rf"\s*{re.escape(test_id)}=(\w+)" for test_id in BUILTIN_TEST_IDS)
)
_CVAL_EVENT_RE = re.compile(r"^CVAL_EVENT\s+(\{.*\})$", re.MULTILINE)

SUCCESS_PHASES = frozenset({"Completed", "Succeeded"})


def _normalize_result(value: str) -> str:
    text = (value or "").strip().lower()
    return text if text in {"pass", "fail", "incomplete"} else "fail"


def parse_test_progress(log_text: str) -> dict[str, str]:
    """Map each test to its observed state from in-pod logs.

    States: ``running`` (started, not finished), ``pass``/``fail`` (finished).
    The authoritative ``Final c-val test results:`` line wins when present.
    """

    progress: dict[str, str] = {}
    if not log_text:
        return progress

    for test, (done_marker, fail_marker) in BUILTIN_DONE_MARKERS.items():
        if done_marker in log_text:
            progress[test] = "pass"
        elif fail_marker in log_text:
            progress[test] = "fail"

    for test, running_marker in BUILTIN_RUNNING_MARKERS.items():
        if running_marker in log_text and test not in progress:
            progress[test] = "running"

    for test, skipped_marker in BUILTIN_SKIPPED_MARKERS.items():
        if skipped_marker in log_text:
            progress[test] = "incomplete"

    match = _FINAL_RESULT_RE.search(log_text)
    if match:
        progress.update(
            {
                test_id: _normalize_result(value)
                for test_id, value in zip(BUILTIN_TEST_IDS, match.groups(), strict=True)
            }
        )

    for event in _structured_progress_events(log_text):
        test = event.get("test")
        if not isinstance(test, str) or not test:
            continue
        event_name = event.get("event")
        if event_name in {"test_setup_started", "test_started"}:
            progress[test] = "running"
        elif event_name in {"test_finished", "test_timed_out"}:
            progress[test] = _normalize_result(str(event.get("status", "fail")))
        elif event_name == "test_skipped":
            progress[test] = "incomplete"

    return progress


def raw_results_from_log(
    log_text: str,
    enabled_tests: set[str] | None = None,
) -> dict[str, str]:
    """Extract per-test results and aggregate across enabled phases."""

    results: dict[str, str] = {}
    for test, skipped_marker in BUILTIN_SKIPPED_MARKERS.items():
        if skipped_marker in (log_text or ""):
            results[test] = "incomplete"
    for event in _structured_progress_events(log_text or ""):
        test = event.get("test")
        event_name = event.get("event")
        if isinstance(test, str) and event_name in {
            "test_finished",
            "test_timed_out",
            "test_skipped",
        }:
            results[test] = _normalize_result(str(event.get("status", "incomplete")))
        if event_name == "run_finished":
            results["all"] = _normalize_result(
                str(event.get("overall", event.get("status", "incomplete")))
            )

    match = _FINAL_RESULT_RE.search(log_text or "")
    if match:
        results.update(
            {
                test_id: _normalize_result(value)
                for test_id, value in zip(BUILTIN_TEST_IDS, match.groups(), strict=True)
            }
        )
    if not results:
        return {}
    enabled = enabled_tests if enabled_tests is not None else set(BUILTIN_TEST_IDS)
    if "all" in results:
        return results
    if not enabled:
        results["all"] = "incomplete"
    else:
        results["all"] = (
            "pass" if all(results.get(test) == "pass" for test in enabled) else "fail"
        )
    return results


def _structured_progress_events(log_text: str) -> list[dict[str, Any]]:
    """Return valid structured c-val event objects in log order."""

    events: list[dict[str, Any]] = []
    for match in _CVAL_EVENT_RE.finditer(log_text or ""):
        try:
            event = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get("schema_version") == "cval.event.v1":
            events.append(event)
    return events


def log_signals_db_updated(log_text: str) -> bool:
    """Return True once the in-pod DB ingestion has finished."""

    ingestion_events = [
        event
        for event in _structured_progress_events(log_text or "")
        if event.get("event") == "ingestion_finished"
    ]
    if ingestion_events:
        return ingestion_events[-1].get("status") == "pass"
    return bool(log_text) and BUILTIN_DB_UPDATE_DONE_MARKER in log_text


def render_test_progress_line(
    elapsed: float,
    phase: str,
    test_ids: list[str],
    progress: dict[str, str],
) -> str:
    """Render one dynamic live-status line in registry order."""

    cells = " ".join(
        f"{test_id}={progress.get(test_id, '-').upper()}" for test_id in test_ids
    )
    return f"[{elapsed:5.0f}s] phase={phase:<9} {cells}".rstrip()


def build_validation_report(
    *,
    node: str,
    timestamp: int,
    job_name: str,
    job_phase: str,
    git_ref: str = "",
    schedulability: dict[str, Any],
    raw_results: dict[str, str],
    interrupted: bool = False,
    ingestion_complete: bool = True,
    fresh_status_complete: bool = True,
    notes: list[str] | None = None,
    test_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Assemble the structured single-node validation report."""

    report_tests = tuple(
        test_ids
        if test_ids is not None
        else [test for test in raw_results if test != "all"]
    )

    raw_overall = raw_results.get("all")
    if not raw_overall:
        phase_tests = [raw_results.get(t) for t in report_tests]
        if phase_tests and all(v == "pass" for v in phase_tests):
            raw_overall = "pass"
        elif any(v == "fail" for v in phase_tests):
            raw_overall = "fail"
        else:
            raw_overall = "unknown"

    job_ok = job_phase in SUCCESS_PHASES
    return {
        "node": node,
        "timestamp": timestamp,
        "job_name": job_name,
        "job_phase": job_phase,
        "git_ref": git_ref,
        "interrupted": interrupted,
        "ok": (
            job_ok
            and not interrupted
            and ingestion_complete
            and fresh_status_complete
            and raw_overall == "pass"
        ),
        "ingestion_complete": ingestion_complete,
        "fresh_status_complete": fresh_status_complete,
        "schedulability": schedulability,
        "raw_results": raw_results,
        "test_order": list(report_tests),
        "raw_overall": raw_overall,
        "notes": notes or [],
    }


def render_validation_report(report: dict[str, Any]) -> str:
    """Render the structured report as an operator-facing text summary."""

    lines: list[str] = []
    lines.append("=" * 72)
    lines.append(f"c-val validation report: {report['node']}")
    lines.append("=" * 72)
    lines.append(f"job:        {report['job_name']}")
    if report.get("git_ref"):
        lines.append(f"commit:     {report['git_ref']}")
    lines.append(f"timestamp:  {report['timestamp']}")
    lines.append(f"job phase:  {report['job_phase']}")

    sched = report.get("schedulability") or {}
    if sched:
        status_label = sched.get("status_label")
        cordoned = sched.get("cordoned")
        cordon_note = " [CORDONED]" if cordoned else ""
        label_line = f" status={status_label}" if status_label else ""
        lines.append(
            f"node state:{label_line}{cordon_note} "
            f"ready={sched.get('ready')} free={sched.get('fully_free')} "
            f"schedulable={sched.get('schedulable')} "
            f"resource_ready={sched.get('resource_ready')} "
            f"({sched.get('reason', '')})"
        )

    lines.append("-" * 72)
    lines.append(f"{'TEST':<20} {'RAW RESULT':<12}")
    raw = report.get("raw_results", {})
    report_tests = report.get("test_order") or [
        test for test in raw if test != "all"
    ]
    for test in report_tests:
        lines.append(f"{test:<20} {raw.get(test, '-'):<12}")

    lines.append("-" * 72)
    lines.append(f"RAW overall: {report.get('raw_overall', 'unknown').upper()}")

    if report.get("notes"):
        lines.append("-" * 72)
        for note in report["notes"]:
            lines.append(f"! {note}")

    lines.append("=" * 72)
    return "\n".join(lines)


# --- Orchestration ----------------------------------------------------------


def _first_line(text: str) -> str:
    text = (text or "").strip()
    return text.splitlines()[0] if text else ""


def _build_single_node_plan(
    node: str,
    timestamp: int,
    config: CvalConfig,
    git_ref: str | None,
) -> tuple[WorkflowPlan, Any]:
    rendered = render_validation_job_from_file(
        config.job.template_path,
        node_name=node,
        timestamp=timestamp,
        job_prefix=config.job.job_prefix,
        git_repo=config.job.git_repo,
        git_ref=git_ref or config.job.git_ref,
        job_template_config=config.job_template,
        cval_config=config,
    )
    candidate = QueueCandidate(
        node=node,
        priority=1,
        last_tested_timestamp=0,
        age_days=None,
        reason="manual-validate",
    )
    plan = WorkflowPlan(
        free_nodes=[node],
        queue=[candidate],
        planned_jobs=[PlannedJob(candidate=candidate, rendered_job=rendered)],
        batch_size=1,
        days_threshold=0.0,
    )
    return plan, rendered


def _pod_logs(
    kubectl: KubectlClient,
    namespace: str,
    pod_name: str,
    *,
    tail_lines: int,
    timeout: float | None = None,
) -> str:
    result = kubectl.run(
        ["logs", "-n", namespace, pod_name, f"--tail={tail_lines}"],
        check=False,
        timeout=timeout,
    )
    return result.stdout if result.returncode == 0 else ""


def run_node_validation(
    node: str,
    *,
    config: CvalConfig | None = None,
    client: KubectlClient | None = None,
    namespace: str | None = None,
    git_ref: str | None = None,
    timestamp: int | None = None,
    poll_interval: float | None = None,
    overall_timeout: float | None = None,
    pending_timeout: float | None = None,
    submit: bool = False,
    confirmation: str | None = None,
    verbose: bool = True,
    printer: Callable[[str], None] = print,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Submit, live-track, and report one real cluster validation job."""

    config = config or load_config()
    namespace = namespace or config.cluster.namespace
    kubectl = client or KubectlClient(config=config)
    poll_interval = config.monitoring.validation_poll_seconds if poll_interval is None else poll_interval
    pending_timeout = config.monitoring.validation_pending_seconds if pending_timeout is None else pending_timeout
    timestamp = int(time.time()) if timestamp is None else int(timestamp)
    overall_timeout = (
        config.monitoring.timeout_seconds if overall_timeout is None else overall_timeout
    )
    if overall_timeout <= 0:
        raise ValueError("overall_timeout must be positive")
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive")
    if not submit:
        raise PolicyViolation(
            "Cluster validation requires explicit --submit --confirm submit"
        )
    resolved_git_ref = git_ref or config.job.git_ref
    if not is_exact_commit(resolved_git_ref):
        raise PolicyViolation(
            "Cluster validation requires --git-ref to be an exact lowercase "
            "40-hex commit"
        )
    notes: list[str] = []
    enabled_test_order = [test.id for test in config.tests.registry.enabled]
    enabled_tests = set(enabled_test_order)
    disabled_tests = [
        test.id for test in config.tests.registry.tests if not test.enabled
    ]
    if disabled_tests:
        notes.append(f"disabled tests skipped: {', '.join(disabled_tests)}")

    def emit(message: str) -> None:
        if verbose:
            printer(message)

    # 1. Fail closed unless the node can run the complete validation workload.
    schedulability: dict[str, Any] = {}
    try:
        status: NodeStatus = describe_node(node, client=kubectl, config=config, allow_cordoned=True)
        schedulability = {
            "found": status.found,
            "is_gpu_node": status.is_gpu_node,
            "status_label": status.status_label,
            "ready": status.ready,
            "cordoned": status.cordoned,
            "schedulable": status.schedulable,
            "resource_ready": status.resource_ready,
            "fully_free": status.fully_free,
            "free_gpus": status.free,
            "allocatable_gpus": status.allocatable,
            "reason": status.reason,
        }
        cordon_note = " [CORDONED]" if status.cordoned else ""
        emit(
            f"node {node}: status={status.status_label}{cordon_note} "
            f"ready={status.ready} free={status.fully_free} schedulable={status.schedulable} "
            f"resource_ready={status.resource_ready} gpus_free={status.free}/{status.allocatable} "
            f"-> {status.reason}"
        )
        if status.cordoned:
            notes.append(
                "node is cordoned; validation job tolerates the cordon taint and targets it anyway"
            )
        if not status.eligible:
            raise PolicyViolation(
                f"Node {node!r} is not eligible for cluster validation: "
                f"{status.status_label or 'unknown'} ({status.reason})"
            )
    except PolicyViolation:
        raise
    except Exception as exc:  # noqa: BLE001 - unknown state must block mutation
        raise PolicyViolation(
            f"Could not verify node {node!r} eligibility: {_first_line(str(exc))}"
        ) from exc

    # 2. Render the single-node job.
    plan, rendered = _build_single_node_plan(
        node, timestamp, config, resolved_git_ref
    )
    job_name = rendered.job_name
    pod_name = f"{job_name}-server-0"

    # 3. Submit only after the operator supplied the exact confirmation.
    policy = ExecutionPolicy(
        namespace_allowlist=tuple(config.policy.namespace_allowlist),
        max_batch_size=max(1, config.policy.max_batch_size),
        confirmation_phrase=config.policy.confirmation_phrase,
    )
    submission = submit_workflow_plan(
        plan,
        config=config,
        namespace=namespace,
        client=kubectl,
        policy=policy,
        submit=True,
        confirmation=confirmation,
    )
    record = submission.records[0]
    emit(f"queued job {record.job_name} for node {node} (timestamp {timestamp})")

    # 4. Live tracking: poll phase + parse logs for per-test progress.
    start = clock()
    deadline = start + overall_timeout
    last_status_line = ""
    seen_progress: dict[str, str] = {}
    phase = "Unknown"
    interrupted = False
    logs = ""
    try:
        while True:
            remaining = deadline - clock()
            if remaining <= 0:
                notes.append(
                    f"overall timeout ({overall_timeout:.0f}s) reached before terminal phase"
                )
                emit(f"[{overall_timeout:5.0f}s] overall timeout reached; stopping live tracking")
                break
            phase = get_job_phase(
                job_name,
                config=config,
                namespace=namespace,
                client=kubectl,
                timeout=remaining,
            ).phase
            remaining = deadline - clock()
            if remaining <= 0:
                notes.append(
                    f"overall timeout ({overall_timeout:.0f}s) reached before log fetch"
                )
                break
            logs = _pod_logs(
                kubectl,
                namespace,
                pod_name,
                tail_lines=config.monitoring.log_tail_lines,
                timeout=remaining,
            )
            elapsed = clock() - start
            progress = parse_test_progress(logs)
            seen_progress.update(progress)

            status_line = render_test_progress_line(
                elapsed,
                phase,
                enabled_test_order,
                seen_progress,
            )
            if status_line != last_status_line:
                emit(status_line)
                last_status_line = status_line

            if phase in TERMINAL_PHASES:
                emit(f"[{elapsed:5.0f}s] job reached terminal phase: {phase}")
                break
            if phase == "Pending" and elapsed >= pending_timeout and not seen_progress:
                emit(
                    f"[{elapsed:5.0f}s] still Pending after {pending_timeout:.0f}s; "
                    "node not schedulable yet (job left queued)"
                )
            remaining = deadline - clock()
            if remaining <= 0:
                continue
            sleeper(min(poll_interval, remaining))
    except KeyboardInterrupt:
        interrupted = True
        notes.append("interrupted by operator; job left running")
        emit("\ninterrupted; the validation job is still running and was left untouched.")

    # 5. Raw pass/fail from logs, augmented only by rows from this exact run.
    raw_results = raw_results_from_log(logs, enabled_tests=enabled_tests)
    ingestion_complete = log_signals_db_updated(logs)
    fresh_status_tests: set[str] = set()
    fresh_status_results: dict[str, str] = {}
    try:
        rows = get_latest_status_rows(
            client=kubectl,
            namespace=namespace,
            pod=config.cluster.pvc_access_pod,
            db_path=config.storage.validation_db_path,
            config=config,
        )
        for row in rows:
            if row.node == node and row.latest_timestamp == timestamp:
                raw_results.setdefault(row.test, row.result)
                fresh_status_tests.add(row.test)
                fresh_status_results[row.test] = row.result
            elif row.node == node and row.test in enabled_tests:
                notes.append(
                    f"ignored stale {row.test} DB row at timestamp "
                    f"{row.latest_timestamp}; expected {timestamp}"
                )
    except Exception as exc:  # noqa: BLE001
        notes.append(f"could not read validation.db status: {_first_line(str(exc))}")

    # 6. Verify fresh raw rows.
    compatibility_status_tests = enabled_tests & set(BUILTIN_TEST_IDS)
    required_fresh_tests = compatibility_status_tests | {"all"}
    fresh_rows_complete = required_fresh_tests.issubset(fresh_status_tests)
    fresh_rows_match = all(
        fresh_status_results.get(test) == raw_results.get(test)
        for test in required_fresh_tests
    )
    # 7. Build and render the report.
    report = build_validation_report(
        node=node,
        timestamp=timestamp,
        job_name=job_name,
        job_phase=phase,
        git_ref=resolved_git_ref,
        schedulability=schedulability,
        raw_results=raw_results,
        test_ids=enabled_test_order,
        interrupted=interrupted,
        ingestion_complete=ingestion_complete,
        fresh_status_complete=fresh_rows_complete and fresh_rows_match,
        notes=notes,
    )

    if verbose:
        printer("")
        printer(render_validation_report(report))
    return report
