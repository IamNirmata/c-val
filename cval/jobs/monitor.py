"""Read-only Volcano job monitoring.

Monitoring only polls job phase. It deliberately does not delete or cancel jobs
on timeout so operators retain evidence for diagnosis.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Callable, Sequence

from cval.config import load_config
from cval.k8s.client import KubectlClient


TERMINAL_PHASES = frozenset(
    {"Completed", "Succeeded", "Failed", "Aborted", "Terminated", "Missing"}
)


@dataclass(frozen=True)
class JobPhase:
    """Current phase for one Volcano job."""

    job_name: str
    phase: str


@dataclass(frozen=True)
class MonitoredJob:
    """Final monitor classification for one job."""

    job_name: str
    phase: str
    terminal: bool
    timed_out: bool
    elapsed_seconds: float


def get_job_phase(
    job_name: str,
    namespace: str | None = None,
    client: KubectlClient | None = None,
    timeout: float | None = None,
) -> JobPhase:
    """Read one Volcano job phase using a non-mutating kubectl get."""

    kubectl = client or KubectlClient()
    resolved_namespace = namespace or load_config().cluster.namespace
    args = [
        "get",
        "vcjob",
        "-n",
        resolved_namespace,
        job_name,
        "-o",
        "jsonpath={.status.state.phase}",
    ]
    if timeout is None:
        result = kubectl.run(args, check=False)
    else:
        result = kubectl.run(args, check=False, timeout=timeout)
    if result.returncode == 0:
        phase = result.stdout.strip() or "Unknown"
    elif "(notfound)" in result.stderr.lower():
        phase = "Missing"
    else:
        phase = "Unknown"
    return JobPhase(job_name=job_name, phase=phase or "Unknown")


def get_job_phases(
    job_names: Sequence[str],
    namespace: str | None = None,
    client: KubectlClient | None = None,
) -> list[JobPhase]:
    """Read phases for multiple jobs with a shared kubectl client."""

    kubectl = client or KubectlClient()
    resolved_namespace = namespace or load_config().cluster.namespace
    return [get_job_phase(job_name, namespace=resolved_namespace, client=kubectl) for job_name in job_names]


def list_job_phases(
    namespace: str | None = None,
    prefix: str | None = None,
    client: KubectlClient | None = None,
) -> list[JobPhase]:
    """List phases for all Volcano jobs in a namespace, optionally by name prefix.

    Read-only: a single `kubectl get vcjob -o json`. Returns an empty list on
    any API or parse failure so callers (e.g. the overview) degrade gracefully.
    """

    kubectl = client or KubectlClient()
    resolved_namespace = namespace or load_config().cluster.namespace
    result = kubectl.run(
        ["get", "vcjob", "-n", resolved_namespace, "-o", "json"], check=False
    )
    if result.returncode != 0:
        return []
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return []

    phases: list[JobPhase] = []
    for item in payload.get("items", []):
        name = item.get("metadata", {}).get("name", "")
        if not name or (prefix and not name.startswith(f"{prefix}-")):
            continue
        phase = item.get("status", {}).get("state", {}).get("phase", "") or "Unknown"
        phases.append(JobPhase(job_name=name, phase=phase))
    return phases


def monitor_jobs_until_terminal(
    job_names: Sequence[str],
    namespace: str | None = None,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
    client: KubectlClient | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> list[MonitoredJob]:
    """Poll jobs until every job is terminal or the timeout expires."""

    config = load_config()
    resolved_namespace = namespace or config.cluster.namespace
    resolved_timeout_seconds = (
        timeout_seconds if timeout_seconds is not None else config.monitoring.timeout_seconds
    )
    resolved_poll_interval_seconds = (
        poll_interval_seconds if poll_interval_seconds is not None else config.monitoring.poll_interval_seconds
    )
    kubectl = client or KubectlClient()
    start = clock()
    latest: dict[str, JobPhase] = {
        job_name: JobPhase(job_name, "Unknown") for job_name in job_names
    }

    while True:
        elapsed = max(0.0, clock() - start)
        phases = get_job_phases(job_names, namespace=resolved_namespace, client=kubectl)
        latest = {phase.job_name: phase for phase in phases}
        all_terminal = all(phase.phase in TERMINAL_PHASES for phase in latest.values())
        timed_out = elapsed >= resolved_timeout_seconds
        if all_terminal or timed_out:
            # Timeout marks only jobs that are still non-terminal at the deadline.
            return [
                MonitoredJob(
                    job_name=job_name,
                    phase=latest.get(job_name, JobPhase(job_name, "Unknown")).phase,
                    terminal=latest.get(job_name, JobPhase(job_name, "Unknown")).phase
                    in TERMINAL_PHASES,
                    timed_out=latest.get(job_name, JobPhase(job_name, "Unknown")).phase
                    not in TERMINAL_PHASES
                    and timed_out,
                    elapsed_seconds=elapsed,
                )
                for job_name in job_names
            ]
        sleeper(max(0.0, resolved_poll_interval_seconds))


def monitored_jobs_to_dict(jobs: list[MonitoredJob]) -> list[dict[str, object]]:
    """Convert monitor results to JSON-serializable dictionaries."""

    return [
        {
            "job_name": job.job_name,
            "phase": job.phase,
            "terminal": job.terminal,
            "timed_out": job.timed_out,
            "elapsed_seconds": job.elapsed_seconds,
        }
        for job in jobs
    ]