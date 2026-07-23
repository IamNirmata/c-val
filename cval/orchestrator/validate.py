"""Targeted single-node validation: submit, live-track, classify, and report.

``cval validate --node <node>`` is the on-demand counterpart to the rolling
live runner. It renders and submits one validation job for a specific node,
streams progress while the in-pod tests run, then classifies the fresh result
against the active baselines (on the PVC access pod, where ``/data`` is mounted)
and prints a pass/fail + degraded-metric report.

The pure helpers (`parse_test_progress`, `degraded_metrics_from_verdict`,
`build_validation_report`, `render_validation_report`) are import-only and
unit-tested; the orchestration in `run_node_validation` wires them to the live
cluster through a `KubectlClient`.
"""

from __future__ import annotations

import base64
import io
import json
import re
import time
import zipfile
from pathlib import Path
from typing import Any, Callable

from cval.config import CvalConfig, load_config
from cval.jobs.manager import submit_workflow_plan
from cval.jobs.monitor import TERMINAL_PHASES, get_job_phase
from cval.jobs.renderer import render_validation_job_from_file
from cval.k8s.client import KubectlClient
from cval.k8s.discovery import NodeStatus, describe_node
from cval.models import PlannedJob, QueueCandidate, WorkflowPlan
from cval.policy import ExecutionPolicy
from cval.storage.status import get_latest_status_rows, resolve_status_pod

# In-pod run-test.sh log markers that signal each phase finished.
_TEST_DONE_MARKERS = {
    "storage": ("Storage test is complete.", "Storage test FAILED."),
    "nccl": ("NCCL test is complete.", "NCCL test FAILED."),
}
_DLTEST_RUNNING_MARKER = "Running DL Test..."
_DB_UPDATE_DONE_MARKER = "Main DB update completed."
_FINAL_RESULT_RE = re.compile(
    r"Final c-val test results:\s*storage=(\w+)\s+nccl=(\w+)\s+dltest=(\w+)"
)

REPORT_TEST_TYPES = ("storage", "nccl", "dltest")
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

    for test, (done_marker, fail_marker) in _TEST_DONE_MARKERS.items():
        if done_marker in log_text:
            progress[test] = "pass"
        elif fail_marker in log_text:
            progress[test] = "fail"

    if _DLTEST_RUNNING_MARKER in log_text and "dltest" not in progress:
        progress["dltest"] = "running"

    match = _FINAL_RESULT_RE.search(log_text)
    if match:
        progress["storage"] = _normalize_result(match.group(1))
        progress["nccl"] = _normalize_result(match.group(2))
        progress["dltest"] = _normalize_result(match.group(3))

    return progress


def raw_results_from_log(
    log_text: str,
    enabled_tests: set[str] | None = None,
) -> dict[str, str]:
    """Extract per-test results and aggregate across enabled phases."""

    match = _FINAL_RESULT_RE.search(log_text or "")
    if not match:
        return {}
    results = {
        "storage": _normalize_result(match.group(1)),
        "nccl": _normalize_result(match.group(2)),
        "dltest": _normalize_result(match.group(3)),
    }
    enabled = enabled_tests if enabled_tests is not None else set(REPORT_TEST_TYPES)
    if not enabled:
        results["all"] = "incomplete"
    else:
        results["all"] = (
            "pass" if all(results.get(test) == "pass" for test in enabled) else "fail"
        )
    return results


def log_signals_db_updated(log_text: str) -> bool:
    """Return True once the in-pod DB ingestion has finished."""

    return bool(log_text) and _DB_UPDATE_DONE_MARKER in log_text


def degraded_metrics_from_verdict(
    verdict: dict[str, Any] | None, limit: int | None = None
) -> list[dict[str, Any]]:
    """Return degraded metrics from a classify verdict, worst deviation first."""

    if not verdict:
        return []
    metrics = verdict.get("metrics", []) or []
    degraded = [m for m in metrics if m.get("status") == "degraded"]
    degraded.sort(key=lambda m: abs(float(m.get("pct_diff") or 0.0)), reverse=True)
    if limit is not None:
        degraded = degraded[:limit]
    return [
        {
            "metric": m.get("metric"),
            "component": m.get("component", ""),
            "value": m.get("value"),
            "median": m.get("median"),
            "pct_diff": m.get("pct_diff"),
            "direction": m.get("direction"),
        }
        for m in degraded
    ]


def build_validation_report(
    *,
    node: str,
    timestamp: int,
    job_name: str,
    job_phase: str,
    schedulability: dict[str, Any],
    raw_results: dict[str, str],
    verdicts: dict[str, dict[str, Any] | None],
    metric_limit: int | None = 25,
    dry_run: bool = False,
    interrupted: bool = False,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    """Assemble the structured single-node validation report."""

    classification: dict[str, Any] = {}
    for test in REPORT_TEST_TYPES:
        verdict = verdicts.get(test)
        if verdict is None:
            classification[test] = {
                "status": "unknown",
                "n_compared": 0,
                "n_degraded": 0,
                "degraded_metric_percent": 0.0,
                "worst_pct_diff": 0.0,
                "components": {},
                "degraded_metrics": [],
            }
            continue
        classification[test] = {
            "status": verdict.get("status", "unknown"),
            "n_compared": int(verdict.get("n_compared", 0) or 0),
            "n_degraded": int(verdict.get("n_degraded", 0) or 0),
            "degraded_metric_percent": float(verdict.get("degraded_metric_percent", 0.0) or 0.0),
            "worst_pct_diff": float(verdict.get("worst_pct_diff", 0.0) or 0.0),
            "components": verdict.get("components", {}) or {},
            "degraded_metrics": degraded_metrics_from_verdict(verdict, limit=metric_limit),
        }

    raw_overall = raw_results.get("all")
    if not raw_overall:
        phase_tests = [raw_results.get(t) for t in REPORT_TEST_TYPES]
        if phase_tests and all(v == "pass" for v in phase_tests):
            raw_overall = "pass"
        elif any(v == "fail" for v in phase_tests):
            raw_overall = "fail"
        else:
            raw_overall = "unknown"

    statuses = [classification[t]["status"] for t in REPORT_TEST_TYPES]
    if "degraded" in statuses:
        health = "degraded"
    elif "improved" in statuses:
        health = "improved"
    elif all(s in {"normal", "unknown"} for s in statuses) and "normal" in statuses:
        health = "normal"
    else:
        health = "unknown"

    job_ok = job_phase in SUCCESS_PHASES
    return {
        "node": node,
        "timestamp": timestamp,
        "job_name": job_name,
        "job_phase": job_phase,
        "dry_run": dry_run,
        "interrupted": interrupted,
        "ok": job_ok and not interrupted,
        "schedulability": schedulability,
        "raw_results": raw_results,
        "raw_overall": raw_overall,
        "health": health,
        "classification": classification,
        "notes": notes or [],
    }


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return "n/a"


def render_validation_report(report: dict[str, Any]) -> str:
    """Render the structured report as an operator-facing text summary."""

    lines: list[str] = []
    lines.append("=" * 72)
    lines.append(f"c-val validation report: {report['node']}")
    lines.append("=" * 72)
    lines.append(f"job:        {report['job_name']}")
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

    if report.get("dry_run"):
        lines.append("")
        lines.append("DRY RUN: no job was submitted.")
        return "\n".join(lines)

    lines.append("-" * 72)
    lines.append(f"{'TEST':<10} {'RAW':<8} {'VERDICT':<10} {'BAD':>5} {'BAD%':>7} {'WORST':>9}")
    raw = report.get("raw_results", {})
    classification = report.get("classification", {})
    for test in REPORT_TEST_TYPES:
        verdict = classification.get(test, {})
        lines.append(
            f"{test:<10} {raw.get(test, '-'):<8} {verdict.get('status', 'unknown'):<10} "
            f"{verdict.get('n_degraded', 0):>5} "
            f"{verdict.get('degraded_metric_percent', 0.0):>6.2f}% "
            f"{_fmt_pct(verdict.get('worst_pct_diff', 0.0)):>9}"
        )

    lines.append("-" * 72)
    lines.append(
        f"RAW overall: {report.get('raw_overall', 'unknown').upper()}   "
        f"HEALTH: {report.get('health', 'unknown').upper()}"
    )

    # DL component breakdown when present.
    dl_components = (classification.get("dltest", {}) or {}).get("components", {})
    if dl_components:
        lines.append("-" * 72)
        lines.append("DL components:")
        for component, summary in sorted(dl_components.items()):
            lines.append(
                f"  {component:<24} {summary.get('status', 'unknown'):<9} "
                f"degraded={summary.get('n_degraded', 0)} "
                f"({summary.get('degraded_metric_percent', 0.0):.2f}%) "
                f"worst={_fmt_pct(summary.get('worst_pct_diff', 0.0))}"
            )

    # Degraded metric detail per test.
    any_degraded = False
    for test in REPORT_TEST_TYPES:
        degraded = (classification.get(test, {}) or {}).get("degraded_metrics", [])
        if not degraded:
            continue
        any_degraded = True
        lines.append("-" * 72)
        lines.append(f"Degraded {test} metrics (deviation from baseline):")
        for metric in degraded:
            name = metric.get("metric", "")
            component = metric.get("component", "")
            label = f"{component}/{name}" if component and component not in str(name) else name
            lines.append(
                f"  {label:<48} {_fmt_pct(metric.get('pct_diff')):>9} "
                f"[{metric.get('direction', '')}]"
            )

    if not any_degraded:
        lines.append("-" * 72)
        lines.append("No degraded metrics: all compared metrics are within baseline bands.")

    if report.get("notes"):
        lines.append("-" * 72)
        for note in report["notes"]:
            lines.append(f"! {note}")

    download = report.get("download")
    if isinstance(download, dict) and download.get("path"):
        lines.append("-" * 72)
        lines.append(
            f"Artifacts: {download['path']} "
            f"({download.get('files', 0)} file(s), {download.get('bytes', 0)} bytes)"
        )

    lines.append("=" * 72)
    return "\n".join(lines)


# --- Orchestration ----------------------------------------------------------


def _first_line(text: str) -> str:
    text = (text or "").strip()
    return text.splitlines()[0] if text else ""


def _pod_repo_paths(pod_repo_dir: str, pod_config_path: str | None) -> tuple[str, str]:
    repo = pod_repo_dir.rstrip("/")
    config_path = pod_config_path or f"{repo}/config/cval.toml"
    return repo, config_path


def _run_pod_cval(
    kubectl: KubectlClient,
    namespace: str,
    pod: str,
    repo: str,
    config_path: str,
    cval_args: list[str],
    lock_file: str | None = None,
    lock_wait: float | None = None,
    timeout: float | None = None,
):
    """Run `python -m cval.cli ...` inside the PVC pod, optionally under flock.

    ``lock_wait`` bounds how long ``flock`` waits for the shared DL metric lock
    before giving up, so a busy baseline rebuild cannot stall the call forever.
    ``timeout`` overrides the client kubectl timeout for this command only.
    """

    args = ["exec", "-i", "-n", namespace, pod, "--"]
    if lock_file:
        args += ["flock"]
        if lock_wait is not None:
            args += ["-w", str(int(lock_wait))]
        args += ["-x", lock_file]
    args += ["env", f"PYTHONPATH={repo}", "python3", "-m", "cval.cli", "--config", config_path]
    args += cval_args
    return kubectl.run(args, check=False)


# Pod-side collector: zips this run's logs/summaries/result files from the PVC
# and prints the archive as base64 on stdout (diagnostics go to stderr). Run via
# `python3 - <node> <timestamp> <validation_root>` over kubectl exec stdin.
_DOWNLOAD_SCRIPT = r"""
import base64, io, os, sys, zipfile

node, ts, root = sys.argv[1], sys.argv[2], sys.argv[3]
max_bytes = 25 * 1024 * 1024
candidates = [
    root + "/storage/" + node + "/storage-" + node + "-" + ts,
    root + "/nccl/" + node + "/nccl-" + node + "-" + ts,
    root + "/dltest/" + node + "/dltest-" + node + "-" + ts,
    root + "/results/" + node + "/cval-results-" + node + "-" + ts + ".json",
    root + "/results/" + node + "/cval-results-" + node + "-" + ts + ".env",
]

def add_file(zf, path):
    try:
        if os.path.getsize(path) > max_bytes:
            sys.stderr.write("skip (too large): " + path + "\n")
            return 0
        zf.write(path, os.path.relpath(path, root))
        return 1
    except OSError as exc:
        sys.stderr.write("skip (" + str(exc) + "): " + path + "\n")
        return 0

buf = io.BytesIO()
count = 0
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in candidates:
        if os.path.isdir(path):
            for dirpath, _dirs, files in os.walk(path):
                for name in files:
                    count += add_file(zf, os.path.join(dirpath, name))
        elif os.path.isfile(path):
            count += add_file(zf, path)
data = buf.getvalue()
sys.stderr.write("archived " + str(count) + " file(s), " + str(len(data)) + " bytes\n")
sys.stdout.write(base64.b64encode(data).decode("ascii"))
"""


def finalize_download_zip(
    pod_zip_b64: str,
    report: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    """Decode the pod archive, add the baseline-comparison report, and save it.

    The pod streams a base64 zip of the run's logs/summaries/result files; this
    writes it locally and appends ``report.json`` (structured verdicts) and
    ``report.txt`` (the rendered operator report) so the bundle is self-contained.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    raw = base64.b64decode((pod_zip_b64 or "").strip() or _empty_zip_b64())
    output_path.write_bytes(raw)
    with zipfile.ZipFile(output_path, "a", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("report.json", json.dumps(report, indent=2, sort_keys=True))
        archive.writestr("report.txt", render_validation_report(report))
    with zipfile.ZipFile(output_path) as archive:
        names = archive.namelist()
    return {
        "path": str(output_path),
        "files": len(names),
        "bytes": output_path.stat().st_size,
    }


def _empty_zip_b64() -> str:
    """Return a base64 empty-zip so a run with no artifacts still packages cleanly."""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w"):
        pass
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def download_validation_artifacts(
    node: str,
    timestamp: int,
    report: dict[str, Any],
    *,
    kubectl: KubectlClient,
    namespace: str,
    pod: str,
    validation_root: str,
    output_dir: str | Path,
    timeout: float = 180.0,
) -> dict[str, Any]:
    """Collect this run's artifacts from the PVC pod and save a local zip."""

    pvc_pod = resolve_status_pod(kubectl, namespace, pod)
    result = kubectl.run(
        [
            "exec",
            "-i",
            "-n",
            namespace,
            pvc_pod,
            "--",
            "python3",
            "-",
            node,
            str(timestamp),
            validation_root,
        ],
        input_text=_DOWNLOAD_SCRIPT,
        check=False,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"artifact collection failed on {pvc_pod}: {_first_line(result.stderr) or 'unknown error'}"
        )
    output_path = Path(output_dir).expanduser().resolve() / f"cval-{node}-{timestamp}.zip"
    return finalize_download_zip(result.stdout, report, output_path)


def _parse_classify_verdict(stdout: str) -> dict[str, Any] | None:
    """Extract the single-node verdict from `baseline classify` JSON output."""

    try:
        data = json.loads(stdout or "")
    except json.JSONDecodeError:
        return None
    if isinstance(data, dict):
        verdicts = data.get("verdicts")
        if isinstance(verdicts, list) and verdicts:
            return verdicts[0]
        return None
    if isinstance(data, list) and data:
        return data[0]
    return None


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
        dry_run=True,
    )
    return plan, rendered


def _pod_logs(kubectl: KubectlClient, namespace: str, pod_name: str) -> str:
    result = kubectl.run(["logs", "-n", namespace, pod_name], check=False)
    return result.stdout if result.returncode == 0 else ""


def run_node_validation(
    node: str,
    *,
    config: CvalConfig | None = None,
    client: KubectlClient | None = None,
    namespace: str | None = None,
    git_ref: str | None = None,
    timestamp: int | None = None,
    poll_interval: float = 3.0,
    overall_timeout: float | None = None,
    pending_timeout: float = 600.0,
    skip_dl_rebuild: bool = False,
    dl_rebuild_timeout: float = 300.0,
    dl_lock_wait: float = 120.0,
    pod: str | None = None,
    pod_repo_dir: str = "/tmp/c-val",
    pod_config_path: str | None = None,
    window_days: int | None = None,
    dry_run: bool = False,
    download: bool = False,
    download_dir: str | Path = ".",
    verbose: bool = True,
    printer: Callable[[str], None] = print,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Submit, live-track, classify, and report validation for one node."""

    config = config or load_config()
    namespace = namespace or config.cluster.namespace
    kubectl = client or KubectlClient()
    timestamp = int(time.time()) if timestamp is None else int(timestamp)
    overall_timeout = (
        config.monitoring.timeout_seconds if overall_timeout is None else overall_timeout
    )
    window_days = config.baseline.window_days if window_days is None else window_days
    notes: list[str] = []
    enabled_tests = {
        test
        for test, enabled in (
            ("storage", config.tests.storage.enabled),
            ("nccl", config.tests.nccl.enabled),
            ("dltest", config.tests.dltest.enabled),
        )
        if enabled
    }
    disabled_tests = sorted(set(REPORT_TEST_TYPES) - enabled_tests)
    if disabled_tests:
        notes.append(f"disabled tests skipped: {', '.join(disabled_tests)}")

    def emit(message: str) -> None:
        if verbose:
            printer(message)

    # 1. Node schedulability (informational only; we submit regardless).
    schedulability: dict[str, Any] = {}
    try:
        status: NodeStatus = describe_node(node, client=kubectl, config=config)
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
    except Exception as exc:  # noqa: BLE001 - status is best-effort, never blocks submit
        schedulability = {"found": False, "reason": f"could not determine node state: {exc}"}
        notes.append(f"node status check failed: {_first_line(str(exc))}")
        emit(f"node {node}: could not determine state ({_first_line(str(exc))}); submitting anyway")

    # 2. Render the single-node job.
    plan, rendered = _build_single_node_plan(node, timestamp, config, git_ref)
    job_name = rendered.job_name
    pod_name = f"{job_name}-server-0"

    if dry_run:
        emit(f"[dry-run] would submit job {job_name} for node {node} (timestamp {timestamp})")
        report = build_validation_report(
            node=node,
            timestamp=timestamp,
            job_name=job_name,
            job_phase="DryRun",
            schedulability=schedulability,
            raw_results={},
            verdicts={t: None for t in REPORT_TEST_TYPES},
            dry_run=True,
            notes=notes,
        )
        if verbose:
            printer(render_validation_report(report))
        return report

    # 3. Submit immediately (the explicit `validate` invocation is the approval).
    policy = ExecutionPolicy(
        namespace_allowlist=tuple(config.policy.namespace_allowlist),
        max_batch_size=max(1, config.policy.max_batch_size),
        confirmation_phrase=config.policy.confirmation_phrase,
    )
    submission = submit_workflow_plan(
        plan,
        namespace=namespace,
        client=kubectl,
        policy=policy,
        submit=True,
        confirmation=config.policy.confirmation_phrase,
    )
    record = submission.records[0]
    emit(f"queued job {record.job_name} for node {node} (timestamp {timestamp})")

    # 4. Live tracking: poll phase + parse logs for per-test progress.
    start = clock()
    last_status_line = ""
    seen_progress: dict[str, str] = {}
    phase = "Unknown"
    interrupted = False
    logs = ""
    try:
        while True:
            phase = get_job_phase(job_name, namespace=namespace, client=kubectl).phase
            elapsed = clock() - start
            logs = _pod_logs(kubectl, namespace, pod_name)
            progress = parse_test_progress(logs)
            seen_progress.update(progress)

            def cell(test: str) -> str:
                value = seen_progress.get(test)
                return value.upper() if value else "-"

            status_line = (
                f"[{elapsed:5.0f}s] phase={phase:<9} "
                f"storage={cell('storage')} nccl={cell('nccl')} dltest={cell('dltest')}"
            )
            if status_line != last_status_line:
                emit(status_line)
                last_status_line = status_line

            if phase in TERMINAL_PHASES:
                emit(f"[{elapsed:5.0f}s] job reached terminal phase: {phase}")
                break
            if elapsed >= overall_timeout:
                notes.append(f"overall timeout ({overall_timeout:.0f}s) reached before terminal phase")
                emit(f"[{elapsed:5.0f}s] overall timeout reached; stopping live tracking")
                break
            if phase == "Pending" and elapsed >= pending_timeout and not seen_progress:
                emit(
                    f"[{elapsed:5.0f}s] still Pending after {pending_timeout:.0f}s; "
                    "node not schedulable yet (job left queued)"
                )
            sleeper(poll_interval)
    except KeyboardInterrupt:
        interrupted = True
        notes.append("interrupted by operator; job left running")
        emit("\ninterrupted; the validation job is still running. Re-run classification later.")

    # 5. Raw pass/fail from the in-pod logs, augmented by validation.db.
    raw_results = raw_results_from_log(logs, enabled_tests=enabled_tests)
    try:
        rows = get_latest_status_rows(
            client=kubectl,
            namespace=namespace,
            pod=config.cluster.pvc_access_pod,
            db_path=config.storage.validation_db_path,
        )
        for row in rows:
            if row.node == node:
                raw_results.setdefault(row.test, row.result)
    except Exception as exc:  # noqa: BLE001
        notes.append(f"could not read validation.db status: {_first_line(str(exc))}")

    verdicts: dict[str, dict[str, Any] | None] = {t: None for t in REPORT_TEST_TYPES}

    # 6. Classify on the PVC pod (where /data is mounted), unless interrupted.
    if not interrupted:
        try:
            pvc_pod = resolve_status_pod(kubectl, namespace, pod or config.cluster.pvc_access_pod)
            repo, pod_config = _pod_repo_paths(pod_repo_dir, pod_config_path)
            emit(f"classifying results on PVC pod {pvc_pod} ...")

            if config.tests.dltest.enabled and not skip_dl_rebuild:
                node_root = f"{config.runtime.dl_results_root_path.rstrip('/')}/{node}"
                meta_dir = str(Path(config.storage.dl_numerical_db_path).parent)
                lock_file = f"{config.baseline.baseline_root_path.rstrip('/')}/.dl-metric-refresh.lock"
                emit("  refreshing DL metric DBs for this node (scoped, locked) ...")
                rebuild = _run_pod_cval(
                    kubectl,
                    namespace,
                    pvc_pod,
                    repo,
                    pod_config,
                    [
                        "db-rebuild-dltest-metrics",
                        "--results-root",
                        node_root,
                        "--output-dir",
                        meta_dir,
                        "--output",
                        "json",
                    ],
                    lock_file=lock_file,
                    lock_wait=dl_lock_wait,
                    timeout=dl_rebuild_timeout,
                )
                if rebuild.returncode != 0:
                    note = (
                        "DL metric refresh skipped (lock busy or timeout); "
                        "DL classified against latest available data"
                    )
                    notes.append(note)
                    emit(f"  {note}")

            for test in REPORT_TEST_TYPES:
                if test not in enabled_tests:
                    emit(f"  skipping {test} classification (test disabled) ...")
                    continue
                emit(f"  classifying {test} ...")
                result = _run_pod_cval(
                    kubectl,
                    namespace,
                    pvc_pod,
                    repo,
                    pod_config,
                    [
                        "baseline",
                        "classify",
                        "--test-type",
                        test,
                        "--node",
                        node,
                        "--window-days",
                        str(window_days),
                        "--store-results",
                        "--output",
                        "json",
                    ],
                )
                if result.returncode == 0:
                    verdicts[test] = _parse_classify_verdict(result.stdout)
                    if verdicts[test] is None:
                        notes.append(f"{test} classification returned no verdict")
                else:
                    note = f"{test} classification failed: {_first_line(result.stderr)}"
                    notes.append(note)
                    emit(f"  {note}")
        except Exception as exc:  # noqa: BLE001
            note = f"classification step failed: {_first_line(str(exc))}"
            notes.append(note)
            emit(f"  {note}")

    # 7. Build and render the report.
    report = build_validation_report(
        node=node,
        timestamp=timestamp,
        job_name=job_name,
        job_phase=phase,
        schedulability=schedulability,
        raw_results=raw_results,
        verdicts=verdicts,
        interrupted=interrupted,
        notes=notes,
    )

    # 8. Optionally package logs, results, and the baseline comparison as a zip.
    if download:
        try:
            info = download_validation_artifacts(
                node,
                timestamp,
                report,
                kubectl=kubectl,
                namespace=namespace,
                pod=pod or config.cluster.pvc_access_pod,
                validation_root=config.runtime.validation_root,
                output_dir=download_dir,
            )
            report["download"] = info
            emit(
                f"downloaded artifacts -> {info['path']} "
                f"({info['files']} file(s), {info['bytes']} bytes)"
            )
        except Exception as exc:  # noqa: BLE001 - download is best-effort
            note = f"artifact download failed: {_first_line(str(exc))}"
            notes.append(note)
            report["notes"] = list(notes)
            emit(f"  {note}")

    if verbose:
        printer("")
        printer(render_validation_report(report))
    return report
