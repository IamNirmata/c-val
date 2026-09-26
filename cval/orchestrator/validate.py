"""Targeted single-node validation: submit, live-track, and report raw evidence.

``cval validate --node <node> --git-ref <commit> --submit --confirm submit`` is
the cluster-first development path. It submits one validation job for a
specific eligible node, streams progress while the in-pod tests run, and
reports the canonical raw evidence written by that job.

Report helpers are import-only; orchestration uses the shared Job/pod/log observer
and exact evidence verifier through a `KubectlClient`.
"""

from __future__ import annotations

import json
from dataclasses import replace
import time
from typing import Any, Callable

from cval.config import CvalConfig, is_exact_commit, load_config
from cval.jobs.manager import admission_lock, read_admission_session, retain_submission_event, submit_workflow_plan
from cval.jobs.monitor import TERMINAL_PHASES, observe_validation_job
from cval.jobs.renderer import render_validation_job_from_file
from cval.k8s.client import KubectlClient
from cval.k8s.discovery import NodeStatus, describe_node
from cval.models import PlannedJob, QueueCandidate, WorkflowPlan
from cval.policy import ExecutionPolicy, PolicyViolation
from cval.storage.verification import verify_run_evidence

SUCCESS_PHASES = frozenset({"Completed", "Succeeded"})


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
    ingestion_complete: bool = False,
    fresh_status_complete: bool = False,
    notes: list[str] | None = None,
    test_ids: list[str] | tuple[str, ...] | None = None,
    verification: dict[str, Any] | None = None,
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
            and bool(report_tests)
            and all(raw_results.get(test) == "pass" for test in report_tests)
            and ingestion_complete
            and fresh_status_complete
            and raw_overall == "pass"
            and verification is not None
            and verification.get("accepted") is True
            and verification.get("complete") is True
            and verification.get("identity_verified") is True
            and all(verification.get("request", {}).get(key) == value for key, value in (
                ("node", node), ("timestamp", timestamp), ("job_name", job_name), ("git_ref", git_ref),
            ))
        ),
        "verification": verification or {"state": "unavailable", "accepted": False, "reason": "exact-run evidence was not verified"},
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
    verification = report.get("verification", {})
    lines.append(f"evidence:   {verification.get('state', 'unavailable')} accepted={report['ok']}")
    for name, source in verification.get("sources", {}).items():
        if source.get("state") not in {"complete", "not_required"}:
            lines.append(f"  {name}: {source.get('state')} {source.get('reason', '')}")

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
        allow_cordoned=True,
    )
    record = submission.records[0]
    emit(f"queued job {record.job_name} for node {node} (timestamp {timestamp})")
    expected_config = replace(config, job=replace(config.job, git_ref=resolved_git_ref))
    with admission_lock(expected_config):
        session_dir, state = read_admission_session(expected_config)
        intent = state["intents"][job_name]

    start = clock()
    deadline = start + overall_timeout
    last_status_line = ""
    seen_progress: dict[str, str] = {}
    phase = "Unknown"
    interrupted = False
    observation: dict[str, Any] = {}
    try:
        while True:
            remaining = deadline - clock()
            if remaining <= 0:
                notes.append(
                    f"overall timeout ({overall_timeout:.0f}s) reached before terminal phase"
                )
                emit(f"[{overall_timeout:5.0f}s] overall timeout reached; stopping live tracking")
                break
            observation = observe_validation_job(intent, config=expected_config, client=kubectl, timeout=remaining)
            phase = observation["phase"]
            intent = {**intent, "events": [*intent["events"], observation]}
            elapsed = clock() - start
            progress = observation["runtime"].get("progress", {})
            seen_progress.update(progress)

            status_line = render_test_progress_line(
                elapsed,
                phase,
                enabled_test_order,
                seen_progress,
            )
            status_line += f" logs={observation['log_state']} ingestion={observation['runtime'].get('ingestion_outcome', 'unconfirmed')}"
            if status_line != last_status_line:
                emit(status_line)
                last_status_line = status_line

            if observation["tracking_complete"]:
                emit(f"[{elapsed:5.0f}s] job reached terminal phase: {phase}")
                break
            if observation["state"] in {"conflict", "uncertain"}:
                notes.append(f"Job observation is {observation['observation']}: {observation.get('reason', '')}")
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

    raw_results = {name: status for name, status in seen_progress.items() if status in {"pass", "fail", "incomplete"}}
    overall = observation.get("runtime", {}).get("tests_outcome")
    if overall in {"pass", "fail", "incomplete"}:
        raw_results["all"] = overall
    verification = {"state": "unavailable", "accepted": False, "complete": False, "release": False, "sources": {}}
    try:
        if observation:
            with admission_lock(expected_config):
                session_dir, state = read_admission_session(expected_config)
                state = retain_submission_event(session_dir, state, job_name, observation)
        if interrupted or deadline <= clock():
            raise TimeoutError("No remaining verification budget; retain pending evidence")
        with admission_lock(expected_config):
            session_dir, state = read_admission_session(expected_config)
            intent = state["intents"][job_name]
        verification = verify_run_evidence(intent, expected_config, kubectl,
                                           timeout=min(deadline - clock(), config.monitoring.ingestion_grace_seconds))
        raw_results = verification.get("raw_results", raw_results)
        with admission_lock(expected_config):
            session_dir, state = read_admission_session(expected_config)
            event = {**observation, "state": observation.get("state", "verification_finished"), "verification": verification}
            if verification.get("identity_verified"):
                if event.get("uid") not in {None, verification["request"]["job_uid"]}:
                    raise ValueError("Job identity changed between tracking and acceptance verification")
                event["uid"] = verification["request"]["job_uid"]
            retain_submission_event(session_dir, state, job_name, event)
    except Exception as exc:
        verification = {**verification, "state": "unavailable", "accepted": False, "release": False, "reason": str(exc)}
        notes.append(f"Exact-run verification remains pending: {_first_line(str(exc))}")
    ingestion_complete = verification["sources"].get("ingestion", {}).get("state") == "complete"
    fresh_status_complete = verification["sources"].get("validation", {}).get("state") == "complete"
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
        fresh_status_complete=fresh_status_complete,
        verification=verification,
        notes=notes,
    )
    report["tracking"] = observation

    if verbose:
        printer("")
        printer(render_validation_report(report))
    return report
