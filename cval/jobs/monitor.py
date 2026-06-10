from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Sequence

from cval.k8s.client import KubectlClient


TERMINAL_PHASES = frozenset({"Completed", "Succeeded", "Failed", "Aborted", "Terminated"})


@dataclass(frozen=True)
class JobPhase:
    job_name: str
    phase: str


@dataclass(frozen=True)
class MonitoredJob:
    job_name: str
    phase: str
    terminal: bool
    timed_out: bool
    elapsed_seconds: float


def get_job_phase(
    job_name: str,
    namespace: str = "gcr-admin",
    client: KubectlClient | None = None,
) -> JobPhase:
    kubectl = client or KubectlClient()
    result = kubectl.run(
        [
            "get",
            "vcjob",
            "-n",
            namespace,
            job_name,
            "-o",
            "jsonpath={.status.state.phase}",
        ],
        check=False,
    )
    phase = result.stdout.strip() if result.returncode == 0 else "Unknown"
    return JobPhase(job_name=job_name, phase=phase or "Unknown")


def get_job_phases(
    job_names: Sequence[str],
    namespace: str = "gcr-admin",
    client: KubectlClient | None = None,
) -> list[JobPhase]:
    kubectl = client or KubectlClient()
    return [get_job_phase(job_name, namespace=namespace, client=kubectl) for job_name in job_names]


def monitor_jobs_until_terminal(
    job_names: Sequence[str],
    namespace: str = "gcr-admin",
    timeout_seconds: float = 180,
    poll_interval_seconds: float = 60,
    client: KubectlClient | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> list[MonitoredJob]:
    kubectl = client or KubectlClient()
    start = clock()
    latest: dict[str, JobPhase] = {
        job_name: JobPhase(job_name, "Unknown") for job_name in job_names
    }

    while True:
        elapsed = max(0.0, clock() - start)
        phases = get_job_phases(job_names, namespace=namespace, client=kubectl)
        latest = {phase.job_name: phase for phase in phases}
        all_terminal = all(phase.phase in TERMINAL_PHASES for phase in latest.values())
        timed_out = elapsed >= timeout_seconds
        if all_terminal or timed_out:
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
        sleeper(max(0.0, poll_interval_seconds))


def monitored_jobs_to_dict(jobs: list[MonitoredJob]) -> list[dict[str, object]]:
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