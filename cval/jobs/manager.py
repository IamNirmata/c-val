"""Approval-gated validation job submission.

This module is the only package layer that can create Kubernetes resources. It
calls `ExecutionPolicy` before every real `kubectl create` operation.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import csv
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import runpy
import socket
import stat
import tempfile
import time

import yaml

from cval.config import CvalConfig, encode_config_snapshot, is_exact_commit, load_config
from cval.jobs.monitor import TERMINAL_PHASES, observe_validation_jobs
from cval.jobs.renderer import render_validation_job_from_file
from cval.k8s.client import KubectlClient
from cval.k8s.discovery import NodeStatus, describe_node
from cval.models import PlannedJob, WorkflowPlan
from cval.policy import ExecutionPolicy, PolicyViolation, validate_submit_request
from cval.validation.runtime import effective_config_digest


SESSION_SCHEMA = "cval.live-state.v2"
SESSION_JOB_FIELDS = (
    "job_name", "node", "timestamp", "git_ref", "phase", "submitted_at",
    "last_observed_at", "deleted_at",
)
_TERMINAL_PHASES = TERMINAL_PHASES - {"Missing"}


def atomic_session_text(path: Path, content: str) -> None:
    """Replace operational state durably, retaining the previous file on write failure."""

    if path.is_symlink():
        raise PolicyViolation("Session output must not be a symlink")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def submission_intent(planned: PlannedJob, config: CvalConfig, confirmation: str, allow_cordoned: bool, approval_sha256: str) -> dict:
    """Bind one exact request before any create can be attempted."""

    rendered = planned.rendered_job
    return {
        "node_name": rendered.node_name,
        "timestamp": rendered.timestamp,
        "run_id": f"{rendered.node_name}-{rendered.timestamp}",
        "job_name": rendered.job_name,
        "git_ref": rendered.git_ref,
        "config_digest": effective_config_digest(config),
        "manifest_sha256": hashlib.sha256(rendered.yaml_text.encode()).hexdigest(),
        "manifest": rendered.yaml_text,
        "confirmation": confirmation,
        "allow_cordoned": allow_cordoned,
        "approval_sha256": approval_sha256,
        "events": [],
    }


def retain_submission_event(session_dir: Path, state: dict, name: str, event: dict) -> dict:
    """Durably append an observation without losing earlier create outcomes."""

    updated = read_session_state(session_dir)
    if name not in updated["intents"]:
        updated["intents"][name] = deepcopy(state["intents"][name])
    elif {key: value for key, value in updated["intents"][name].items() if key != "events"} != {key: value for key, value in state["intents"][name].items() if key != "events"}:
        raise PolicyViolation("Retained intent changed before outcome persistence")
    updated["intents"][name]["events"].append({
        "observed_at": datetime.now(timezone.utc).isoformat(), **event,
    })
    updated["updated_at"] = updated["intents"][name]["events"][-1]["observed_at"]
    updated["message"] = f"{name} submission={event['state']}"
    atomic_session_text(session_dir / "state.json", json.dumps(updated, sort_keys=True) + "\n")
    project_submission_jobs(session_dir, updated)
    return updated


def write_session_jobs(session_dir: Path, jobs: dict[str, dict[str, str]]) -> None:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=SESSION_JOB_FIELDS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(jobs[name] for name in sorted(jobs))
    atomic_session_text(session_dir / "jobs.csv", output.getvalue())


def project_submission_jobs(session_dir: Path, state: dict) -> None:
    """Rebuild the compatible CSV from authoritative intents, including lost replies."""

    jobs = {}
    for name, intent in state["intents"].items():
        events = intent["events"]
        if events[-1]["state"] == "not_created" or not any(event["state"] == "submitting" for event in events):
            continue
        created = [event["created_at"] for event in events if event.get("created_at")]
        deleted = [event["deleted_at"] for event in events if event.get("deleted_at")]
        jobs[name] = {
            "job_name": name, "node": intent["node_name"], "timestamp": str(intent["timestamp"]),
            "git_ref": intent["git_ref"], "phase": events[-1].get("phase", "Unknown"),
            "submitted_at": created[0] if created else "", "last_observed_at": events[-1]["observed_at"],
            "deleted_at": deleted[-1] if deleted else "",
        }
    write_session_jobs(session_dir, jobs)
    from cval.storage.status import project_status_csvs

    project_status_csvs(session_dir, state)


def _requested_fields_match(expected, observed) -> bool:
    if isinstance(expected, dict):
        return isinstance(observed, dict) and all(
            key in observed and _requested_fields_match(value, observed[key])
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return isinstance(observed, list) and len(expected) == len(observed) and all(
            _requested_fields_match(wanted, actual) for wanted, actual in zip(expected, observed)
        )
    return type(expected) is type(observed) and expected == observed


def verify_submission_identity(intent: dict, observed: dict, namespace: str) -> str:
    """Verify controlled fields and exact pod workload, ignoring server metadata."""

    if hashlib.sha256(intent["manifest"].encode()).hexdigest() != intent["manifest_sha256"]:
        raise PolicyViolation("Retained requested manifest digest changed")
    expected = yaml.safe_load(intent["manifest"])
    metadata = observed["metadata"]
    if metadata["name"] != intent["job_name"] or metadata["namespace"] != namespace:
        raise PolicyViolation("Observed Job name or namespace conflicts with intent")
    uid = metadata.get("uid")
    if not isinstance(uid, str) or not uid or metadata.get("deletionTimestamp"):
        raise PolicyViolation("Observed Job identity is missing or being deleted")
    known_uids = {event["uid"] for event in intent["events"] if event.get("uid")}
    for event in intent["events"]:
        if event.get("returncode") == 0 and event.get("stdout", "").strip():
            response = json.loads(event["stdout"])
            response_uid = response.get("metadata", {}).get("uid")
            if response_uid:
                known_uids.add(response_uid)
    if known_uids and known_uids != {uid}:
        raise PolicyViolation("Observed Job UID conflicts with retained receipt")
    if not _requested_fields_match(expected, observed):
        raise PolicyViolation("Observed Job controlled fields conflict with the request")
    for wanted, actual in zip(expected["spec"]["tasks"], observed["spec"]["tasks"]):
        if wanted["template"]["spec"] != actual["template"]["spec"]:
            raise PolicyViolation("Observed pod workload differs from the requested workload")
    return uid


def reconcile_submission_intents(session_dir: Path, state: dict, client: KubectlClient, config: CvalConfig) -> dict:
    """Recover receipts, never create; preserve reservations for unknown outcomes."""

    pending = []
    for name, intent in state["intents"].items():
        last = intent["events"][-1]
        if intent_finished(intent):
            continue
        if last["state"] == "conflict" or not any(event["state"] == "submitting" for event in intent["events"]):
            raise PolicyViolation(f"Intent {name!r} requires explicit reconciliation; no retry is authorized")
        pending.append(intent)
    events = observe_validation_jobs(pending, config=config, client=client, timeout=config.monitoring.ingestion_grace_seconds)
    for intent, event in zip(pending, events):
        name = intent["job_name"]
        state = retain_submission_event(session_dir, state, name, event)
    for intent, event in zip(pending, events):
        name = intent["job_name"]
        if event["state"] != "observed" and not intent_finished(state["intents"][name]):
            raise PolicyViolation(f"Intent {name!r} is {event['state']}; node and slot remain reserved")
    project_submission_jobs(session_dir, state)
    return state


def check_node_cooldown(node: str, state: dict, config: CvalConfig) -> None:
    """Apply the existing cooldown contract to every supported submitter."""

    helper = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts/cval-node-cooldown.py"))
    recorded = helper["load_state"](Path(config.live.log_dir) / "node_cool_down.csv")
    for intent in state.get("intents", {}).values():
        if intent["node_name"] != node:
            continue
        for event in intent["events"]:
            if event.get("created_at"):
                timestamp = int(datetime.fromisoformat(event["created_at"].replace("Z", "+00:00")).timestamp())
                recorded[node] = max(recorded.get(node, 0), timestamp)
    _eligible, excluded = helper["filter_nodes"](recorded, [node], now=int(time.time()), cooldown_seconds=config.scheduling.node_cooldown_seconds)
    if excluded:
        raise PolicyViolation(f"Node {node!r} is in submission cooldown until {excluded[0]['cooldown_until']}")


def verification_releases(intent: dict, report: dict) -> bool:
    """Release only an identity-bound terminal evidence decision, never a phase alone."""

    if report.get("schema_version") != "cval.verification.v1" or report.get("release") is not True or report.get("identity_verified") is not True:
        return False
    expected = {"node": intent["node_name"], "timestamp": intent["timestamp"], "run_id": intent["run_id"],
                "git_ref": intent["git_ref"], "config_digest": intent["config_digest"],
                "job_name": intent["job_name"], "manifest_sha256": intent["manifest_sha256"]}
    actual = report.get("request", {})
    job = report.get("sources", {}).get("job", {})
    known_uids = {event["uid"] for event in intent["events"] if event.get("uid")}
    return (
        all(actual.get(key) == value for key, value in expected.items())
        and bool(known_uids) and known_uids == {actual.get("job_uid")}
        and job.get("uid") == actual.get("job_uid") and job.get("state") == "complete"
        and job.get("phase") in _TERMINAL_PHASES and report.get("state") in {"complete", "incomplete", "conflicting"}
        and not any(source.get("state") in {"forbidden", "unavailable"} for source in report["sources"].values())
    )


def intent_finished(intent: dict) -> bool:
    last = intent["events"][-1]
    return last["state"] == "not_created" or (
        last["state"] == "verification_finished" and verification_releases(intent, last["verification"])
    ) or (
        last["state"] == "tracking_finished" and tracking_releases(intent, last)
    )


def tracking_releases(intent: dict, observation: dict) -> bool:
    """Release terminal execution, retaining unconfirmed logs/ingestion as evidence."""

    known_uids = {event["uid"] for event in intent["events"] if event.get("uid")}
    return (
        observation.get("state") == "tracking_finished" and observation.get("observation") == "queried"
        and observation.get("tracking_complete") is True and observation.get("phase") in _TERMINAL_PHASES
        and bool(observation.get("uid")) and known_uids == {observation["uid"]}
        and all(observation.get(key) == intent[key] for key in (
            "job_name", "node_name", "run_id", "timestamp", "git_ref", "config_digest", "manifest_sha256",
        ))
        and isinstance(observation.get("pods"), list)
        and all(pod.get("phase") in {"Succeeded", "Failed"} and pod.get("uid") for pod in observation["pods"])
    )


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
def admission_lock(config: CvalConfig, *, timeout_seconds: float = 0):
    """Serialize short state transitions; fail fast unless a bounded wait is requested."""

    if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
        raise ValueError("Admission lock timeout must be finite and nonnegative")
    admission_scope(config)
    path = Path(config.live.log_dir) / ".cval-admission.lock"
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise PolicyViolation("Admission lock must be a regular file owned by this operator")
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PolicyViolation("Another admission or session update holds the shared lock") from exc
                time.sleep(min(0.1, remaining))
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
    if not isinstance(state, dict) or state.get("schema_version") not in {"cval.live-state.v1", SESSION_SCHEMA}:
        raise PolicyViolation("Invalid cval-live state schema")
    if state["schema_version"] == SESSION_SCHEMA and not isinstance(state.get("intents"), dict):
        raise PolicyViolation("V2 session lacks durable submission intents")
    if state["schema_version"] == SESSION_SCHEMA:
        approval = state.get("approval")
        if not isinstance(approval, dict) or not isinstance(approval.get("text"), str) or not approval.get("path") or hashlib.sha256(approval["text"].encode()).hexdigest() != approval.get("sha256"):
            raise PolicyViolation("V2 session lacks a retained approval/rollback record")
        for name, intent in state["intents"].items():
            if not isinstance(intent, dict) or intent.get("job_name") != name or not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", name):
                raise PolicyViolation("Invalid retained intent identity")
            required = {"node_name", "timestamp", "run_id", "git_ref", "config_digest", "manifest", "manifest_sha256", "confirmation", "allow_cordoned", "approval_sha256", "events", "job_name"}
            if set(intent) != required or not intent["events"] or not isinstance(intent["events"], list):
                raise PolicyViolation("Incomplete retained intent")
            if not isinstance(intent["node_name"], str) or not intent["node_name"] or type(intent["timestamp"]) is not int or intent["timestamp"] <= 0:
                raise PolicyViolation("Invalid retained node/run identity")
            if intent["run_id"] != f"{intent['node_name']}-{intent['timestamp']}" or not is_exact_commit(intent["git_ref"]):
                raise PolicyViolation("Retained run/source identity mismatch")
            if type(intent["allow_cordoned"]) is not bool or intent["approval_sha256"] != approval["sha256"]:
                raise PolicyViolation("Retained intent approval binding mismatch")
            if not isinstance(intent["manifest"], str) or hashlib.sha256(intent["manifest"].encode()).hexdigest() != intent["manifest_sha256"]:
                raise PolicyViolation("Retained intent manifest digest mismatch")
            for event in intent["events"]:
                if not isinstance(event, dict) or event.get("state") not in {"prepared", "submitting", "not_created", "submitted", "observed", "uncertain", "conflict", "verification_finished", "tracking_finished"} or not event.get("observed_at"):
                    raise PolicyViolation("Invalid retained create outcome")
                if event["state"] == "verification_finished" and not isinstance(event.get("verification"), dict):
                    raise PolicyViolation("Missing retained evidence-verification report")
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
    if state["schema_version"] != SESSION_SCHEMA:
        raise PolicyViolation("Legacy session requires approved turnover before durable admission")
    if state.get("admission_scope") != admission_scope(config):
        raise PolicyViolation("Legacy or incompatible admission session; approved turnover is required")
    if state.get("session_id") != session_id or state.get("git_ref") != config.job.git_ref:
        raise PolicyViolation("Admission session/source identity differs from the request")
    if state.get("config_snapshot_b64") != encode_config_snapshot(config):
        raise PolicyViolation("Admission session configuration differs from the request")
    for intent in state["intents"].values():
        if intent["git_ref"] != config.job.git_ref or intent["config_digest"] != effective_config_digest(config) or intent["confirmation"] != config.policy.confirmation_phrase:
            raise PolicyViolation("Retained intent differs from the approved session inputs")
    return session_dir, state


def check_active_jobs(
    planned: PlannedJob, *, client: KubectlClient, config: CvalConfig, session_dir: Path
) -> None:
    """Count live and unreconciled Jobs under the shared admission lock."""

    payload = client.get_json(["get", "vcjob", "-n", config.cluster.namespace])
    if not isinstance(payload.get("items"), list) or payload.get("metadata", {}).get("continue"):
        raise PolicyViolation("Incomplete validation Job inventory")
    active: dict[str, str] = {}
    session = read_session_state(session_dir)
    check_node_cooldown(planned.candidate.node, session, config)
    for name, intent in session.get("intents", {}).items():
        if name == planned.rendered_job.job_name or intent["run_id"] == f"{planned.candidate.node}-{planned.rendered_job.timestamp}":
            raise PolicyViolation("Run identity already has a durable intent; never recreate it")
        if not intent_finished(intent):
            active[name] = intent["node_name"]
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


class JobSubmissionError(PolicyViolation):
    """Carry every earlier receipt when a later submission step fails."""

    def __init__(self, message: str, result: JobSubmissionResult):
        super().__init__(message)
        self.result = result


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
        session_dir, state = read_admission_session(active_config)
        state = reconcile_submission_intents(session_dir, state, kubectl, active_config)
        for planned in plan.planned_jobs:
            name = planned.rendered_job.job_name
            prepared = False
            attempted = False
            created = False
            result = None
            uid = None
            identity_conflict = False
            try:
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
                state["intents"][name] = submission_intent(planned, active_config, confirmation, allow_cordoned, state["approval"]["sha256"])
                state = retain_submission_event(session_dir, state, name, {"state": "prepared"})
                prepared = True
                state = retain_submission_event(session_dir, state, name, {"state": "submitting"})
                check_node_for_submission(
                    planned, client=kubectl, config=active_config, allow_cordoned=allow_cordoned
                )
                attempted = True
                result = kubectl.run(
                    ["create", "-n", resolved_namespace, "-f", "-", "-o", "json"],
                    input_text=planned.rendered_job.yaml_text, check=False,
                )
                state = retain_submission_event(session_dir, state, name, {
                    "state": "uncertain", "returncode": result.returncode,
                    "stdout": result.stdout, "stderr": result.stderr,
                })
                if result.returncode != 0:
                    raise PolicyViolation("Create response failed; retained intent requires read-only reconciliation")
                observed = json.loads(result.stdout)
                try:
                    uid = verify_submission_identity(state["intents"][name], observed, resolved_namespace)
                except (PolicyViolation, KeyError, TypeError, ValueError):
                    identity_conflict = True
                    raise
                created = True
                state = retain_submission_event(session_dir, state, name, {
                    "state": "submitted", "uid": uid,
                    "phase": observed.get("status", {}).get("state", {}).get("phase", "Unknown"),
                    "created_at": observed["metadata"]["creationTimestamp"],
                })
                records.append(JobSubmissionRecord(
                    node=planned.candidate.node,
                    job_name=name,
                    git_ref=planned.rendered_job.git_ref,
                    action="submitted", submitted=True, stdout=f"{name} created; uid={uid}",
                ))
            except Exception as exc:
                message = str(exc)
                if prepared:
                    try:
                        event = {"state": "uncertain" if attempted else "not_created", "error": message}
                        if identity_conflict:
                            event["state"] = "conflict"
                        if attempted and result is not None:
                            event.update(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
                        if created:
                            event["uid"] = uid
                        state = retain_submission_event(session_dir, state, name, event)
                    except Exception as persistence_error:
                        message += f"; outcome persistence failed: {persistence_error}"
                failure = {"error": message}
                if result is not None:
                    failure["create_response"] = {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
                records.append(JobSubmissionRecord(
                    planned.candidate.node, name, planned.rendered_job.git_ref,
                    "receipt_unpersisted" if created else "conflict" if identity_conflict else "uncertain" if attempted else "not_created",
                    created, json.dumps(failure, sort_keys=True),
                ))
                raise JobSubmissionError(
                    f"Admission stopped; retain {session_dir / 'state.json'}: {message}",
                    JobSubmissionResult(namespace=resolved_namespace, records=records),
                ) from exc

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