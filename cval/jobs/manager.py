"""Approval-gated validation job submission.

This module is the only package layer that can create Kubernetes resources. It
calls `ExecutionPolicy` before every real `kubectl create` operation.
"""

from __future__ import annotations

from contextlib import contextmanager
import csv
from dataclasses import dataclass, replace
import fcntl
import json
import os
from pathlib import Path
import re
import socket
import stat

from cval.config import CvalConfig, encode_config_snapshot, is_exact_commit, load_config
from cval.jobs.monitor import TERMINAL_PHASES
from cval.jobs.renderer import render_validation_job_from_file
from cval.k8s.client import KubectlClient
from cval.k8s.discovery import NodeStatus, describe_node
from cval.models import PlannedJob, WorkflowPlan
from cval.policy import ExecutionPolicy, PolicyViolation, validate_submit_request


SESSION_SCHEMA = "cval.live-state.v1"
SESSION_JOB_FIELDS = (
    "job_name", "node", "timestamp", "git_ref", "phase", "submitted_at",
    "last_observed_at", "deleted_at",
)
_TERMINAL_PHASES = TERMINAL_PHASES - {"Missing"}


def admission_scope(config: CvalConfig) -> dict[str, str]:
    """Bind the configured control host and cluster scope without credentials."""

    if socket.gethostname() != config.live.control_host:
        raise PolicyViolation("Submission must run on the configured admission control host")
    root = Path(config.live.log_dir)
    if not root.is_absolute() or root.resolve() != root:
        raise PolicyViolation("Admission state directory must be absolute and non-symlinked")
    return {
        "control_host": config.live.control_host,
        "cluster_server": config.live.cluster_server,
        "namespace": config.cluster.namespace,
        "validation_root": config.runtime.validation_root,
    }


def check_admission_cluster(config: CvalConfig, client: KubectlClient) -> None:
    """Verify the selected kubectl endpoint without reading credentials."""

    admission_scope(config)
    server = client.run([
        "config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}",
    ]).stdout.strip()
    if server != config.live.cluster_server:
        raise PolicyViolation("Selected Kubernetes cluster differs from the admission scope")


@contextmanager
def admission_lock(config: CvalConfig):
    """Serialize short admission/state transitions; contention fails without waiting."""

    admission_scope(config)
    path = Path(config.live.log_dir) / ".cval-admission.lock"
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise PolicyViolation("Admission lock must be a regular file owned by this operator")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PolicyViolation("Another admission or session update holds the shared lock") from exc
        if path.stat().st_ino != info.st_ino:
            raise PolicyViolation("Admission lock identity changed")
        yield
    finally:
        os.close(descriptor)


def read_session_state(session_dir: Path) -> dict[str, object]:
    """Read existing session state without rewriting historical schemas."""

    path = session_dir / "state.json"
    if path.is_symlink() or not path.is_file():
        raise PolicyViolation("Missing or non-regular session state")
    state = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(state, dict) or state.get("schema_version") != SESSION_SCHEMA:
        raise PolicyViolation("Invalid cval-live state schema")
    return state


def read_session_jobs(session_dir: Path) -> dict[str, dict[str, str]]:
    """Read the strict existing jobs projection; missing state is not an empty queue."""

    path = session_dir / "jobs.csv"
    if path.is_symlink() or not path.is_file():
        raise PolicyViolation("Missing or non-regular session jobs")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != SESSION_JOB_FIELDS:
            raise PolicyViolation("Invalid cval-live jobs header")
        jobs = {}
        for row in reader:
            name = row.get("job_name", "")
            if set(row) != set(SESSION_JOB_FIELDS) or any(value is None for value in row.values()):
                raise PolicyViolation("Malformed session job row")
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name) or name in jobs:
                raise PolicyViolation("Invalid or duplicate session job identity")
            jobs[name] = row
    return jobs


def read_admission_session(config: CvalConfig) -> tuple[Path, dict[str, object]]:
    """Require a compatible active session; this check is read-only."""

    root = Path(config.live.log_dir)
    pointer = root / "current-session"
    if pointer.is_symlink() or not pointer.is_file():
        raise PolicyViolation("Missing active admission session; approved initialization is required")
    session_id = pointer.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", session_id):
        raise PolicyViolation("Invalid active session identity")
    session_dir = root / session_id
    if session_dir.is_symlink() or not session_dir.is_dir():
        raise PolicyViolation("Missing or non-regular active session directory")
    state = read_session_state(session_dir)
    if state.get("admission_scope") != admission_scope(config):
        raise PolicyViolation("Legacy or incompatible admission session; approved turnover is required")
    if state.get("session_id") != session_id or state.get("git_ref") != config.job.git_ref:
        raise PolicyViolation("Admission session/source identity differs from the request")
    if state.get("config_snapshot_b64") != encode_config_snapshot(config):
        raise PolicyViolation("Admission session configuration differs from the request")
    read_session_jobs(session_dir)
    return session_dir, state


def check_active_jobs(
    planned: PlannedJob, *, client: KubectlClient, config: CvalConfig, session_dir: Path
) -> None:
    """Count live and unreconciled Jobs under the shared admission lock."""

    payload = client.get_json(["get", "vcjob", "-n", config.cluster.namespace])
    if not isinstance(payload.get("items"), list) or payload.get("metadata", {}).get("continue"):
        raise PolicyViolation("Incomplete validation Job inventory")
    active: dict[str, str] = {}
    for name, row in read_session_jobs(session_dir).items():
        if name == planned.rendered_job.job_name:
            raise PolicyViolation("Run identity already exists in session history")
        if row["phase"] not in _TERMINAL_PHASES:
            if not row["node"]:
                raise PolicyViolation("Unreconciled session Job lacks node identity")
            active[name] = row["node"]
    for job in payload["items"]:
        name = job["metadata"]["name"]
        if name == planned.rendered_job.job_name:
            raise PolicyViolation("Requested Job name already exists")
        templates = [task["template"] for task in job["spec"]["tasks"]]
        is_validation = name.startswith(config.job.job_prefix + "-") or any(
            template.get("metadata", {}).get("labels", {}).get("app") == config.job_template.app_label
            or any(env.get("name") == "CVAL_RUN_ID" for container in template["spec"]["containers"] for env in container.get("env", []))
            for template in templates
        )
        if not is_validation:
            continue
        if job.get("status", {}).get("state", {}).get("phase") not in _TERMINAL_PHASES:
            targets = {template["spec"].get("nodeSelector", {}).get("kubernetes.io/hostname") for template in templates}
            if len(targets) != 1 or None in targets:
                raise PolicyViolation(f"Active validation Job {name!r} has unresolved placement")
            active[name] = targets.pop()
    if planned.candidate.node in active.values():
        raise PolicyViolation(f"Node {planned.candidate.node!r} already has outstanding validation")
    if len(active) >= min(config.scheduling.batch_size, config.policy.max_batch_size):
        raise PolicyViolation("Shared validation inflight budget is full")


@dataclass(frozen=True)
class JobSubmissionRecord:
    """One submitted job action in a submission result."""

    node: str
    job_name: str
    git_ref: str
    action: str
    submitted: bool
    stdout: str = ""


@dataclass(frozen=True)
class JobSubmissionResult:
    """Submission response containing all planned/submitted job records."""

    namespace: str
    records: list[JobSubmissionRecord]

    @property
    def submitted_count(self) -> int:
        """Count records that actually created Kubernetes resources."""

        return sum(1 for record in self.records if record.submitted)


def check_node_for_submission(
    planned: PlannedJob,
    *,
    client: KubectlClient,
    config: CvalConfig,
    allow_cordoned: bool = False,
) -> NodeStatus:
    """Recheck the named node without submitting or relying on planning evidence."""

    node = planned.rendered_job.node_name
    if node != planned.candidate.node:
        raise PolicyViolation("Rendered node does not match the selected candidate")
    try:
        status = describe_node(node, client=client, config=config, allow_cordoned=allow_cordoned)
    except Exception as exc:
        raise PolicyViolation(f"Could not verify node {node!r} eligibility: {exc}") from exc
    if not status.eligible:
        raise PolicyViolation(f"Node {node!r} is not eligible: {status.status_label} ({status.reason})")
    return status


def submit_workflow_plan(
    plan: WorkflowPlan,
    namespace: str | None = None,
    client: KubectlClient | None = None,
    policy: ExecutionPolicy | None = None,
    submit: bool = False,
    confirmation: str | None = None,
    config: CvalConfig | None = None,
    *,
    allow_cordoned: bool = False,
) -> JobSubmissionResult:
    """Submit a workflow plan after exact policy validation."""

    if not submit:
        raise ValueError(
            "Submission requires submit=True; use the plan command for read-only inspection"
        )

    active_config = config or load_config()
    resolved_namespace = active_config.cluster.namespace if namespace is None else namespace
    active_policy = policy or ExecutionPolicy(
        namespace_allowlist=active_config.policy.namespace_allowlist,
        max_batch_size=active_config.policy.max_batch_size,
        confirmation_phrase=active_config.policy.confirmation_phrase,
    )
    invalid_refs = sorted(
        {
            planned.rendered_job.git_ref
            for planned in plan.planned_jobs
            if not is_exact_commit(planned.rendered_job.git_ref)
        }
    )
    if invalid_refs:
        raise PolicyViolation(
            "Real cluster submission requires every rendered job to use an exact "
            "nonzero lowercase 40-hex commit"
        )
    refs = {planned.rendered_job.git_ref for planned in plan.planned_jobs}
    if len(refs) != 1 or resolved_namespace != active_config.cluster.namespace:
        raise PolicyViolation("Submission must bind one source and the configured namespace")
    active_config = replace(active_config, job=replace(active_config.job, git_ref=next(iter(refs))))
    validate_submit_request(
        namespace=resolved_namespace,
        planned_jobs_count=len(plan.planned_jobs),
        policy=active_policy,
        submit=submit,
        confirmation=confirmation,
    )

    kubectl = client or KubectlClient(config=active_config)
    records: list[JobSubmissionRecord] = []
    with admission_lock(active_config):
        check_admission_cluster(active_config, kubectl)
        session_dir, _state = read_admission_session(active_config)
        for planned in plan.planned_jobs:
            expected = render_validation_job_from_file(
                active_config.job.template_path,
                node_name=planned.candidate.node,
                timestamp=planned.rendered_job.timestamp,
                job_prefix=active_config.job.job_prefix,
                git_repo=active_config.job.git_repo,
                git_ref=active_config.job.git_ref,
                job_template_config=active_config.job_template,
                cval_config=active_config,
            )
            if expected != planned.rendered_job:
                raise PolicyViolation("Requested manifest differs from the pinned session configuration")
            check_active_jobs(planned, client=kubectl, config=active_config, session_dir=session_dir)
            validate_submit_request(
                namespace=resolved_namespace, planned_jobs_count=len(plan.planned_jobs),
                policy=active_policy, submit=submit, confirmation=confirmation,
            )
            check_node_for_submission(
                planned, client=kubectl, config=active_config, allow_cordoned=allow_cordoned
            )
            result = kubectl.run(
                ["create", "-n", resolved_namespace, "-f", "-"],
                input_text=planned.rendered_job.yaml_text,
            )
            records.append(
                JobSubmissionRecord(
                    node=planned.candidate.node,
                    job_name=planned.rendered_job.job_name,
                    git_ref=planned.rendered_job.git_ref,
                    action="submitted",
                    submitted=True,
                    stdout=result.stdout.strip(),
                )
            )

    return JobSubmissionResult(namespace=resolved_namespace, records=records)


def submission_result_to_dict(result: JobSubmissionResult) -> dict[str, object]:
    """Convert submission result to JSON-serializable CLI output."""

    return {
        "namespace": result.namespace,
        "submitted_count": result.submitted_count,
        "jobs": [
            {
                "node": record.node,
                "job_name": record.job_name,
                "git_ref": record.git_ref,
                "action": record.action,
                "submitted": record.submitted,
                "stdout": record.stdout,
            }
            for record in result.records
        ],
    }