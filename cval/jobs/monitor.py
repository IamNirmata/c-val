"""Bounded read-only Volcano Job, owned-pod and runtime-log observations."""

from __future__ import annotations

import json
import hashlib
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Sequence

from cval.config import CvalConfig, load_config
from cval.k8s.client import KubectlClient


TERMINAL_PHASES = frozenset(
    {"Completed", "Succeeded", "Failed", "Aborted", "Terminated"}
)


def structured_progress_events(log_text: str, run_id: str | None = None) -> list[dict]:
    events = []
    for line in log_text.splitlines():
        if not line.startswith("CVAL_EVENT "):
            continue
        try:
            event = json.loads(line.removeprefix("CVAL_EVENT "))
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict) and event.get("schema_version") == "cval.event.v1" and (run_id is None or event.get("run_id") == run_id):
            events.append(event)
    return events


def _runtime_progress(log_text: str, intent: dict, config: CvalConfig, previous: dict | None = None) -> dict:
    previous = previous or {}
    progress = dict(previous.get("progress", {}))
    selected = {item.id for item in config.tests.registry.enabled}
    owners = {"storage": "storage", "nccl": "nccl", "numerical_correctness": "dltest", "compute_performance": "dltest",
              "collective_performance": "dltest", "overlap_performance": "dltest"}
    summary = previous.get("summary")
    acknowledgement = previous.get("acknowledgement")
    for event in structured_progress_events(log_text, intent["run_id"]):
        name = event.get("event")
        test = event.get("test")
        if isinstance(test, str) and test in selected:
            if name in {"test_setup_started", "test_started"}:
                progress[test] = "running"
            elif name in {"test_finished", "test_timed_out", "test_skipped"} and event.get("status") in {"pass", "fail", "incomplete"}:
                progress[test] = event["status"]
        if name == "ingestion_started":
            acknowledgement = None
            summary = None
        elif name == "ingestion_finished":
            acknowledgement = event
        elif name == "supervisor_started":
            summary = None
            acknowledgement = None
            progress.clear()
        elif name == "supervisor_finished":
            summary = event
    runtime = {"progress": progress, "tests_outcome": "unconfirmed", "ingestion_outcome": "unconfirmed",
               "logs_state": "unconfirmed", "result_state": "unconfirmed", "completion_confirmed": False}
    stores = acknowledgement.get("stores") if acknowledgement else None
    ack_valid = bool(
        acknowledgement and acknowledgement.get("producer") == "ingestion"
        and acknowledgement.get("node_name") == intent["node_name"] and acknowledgement.get("config_digest") == intent["config_digest"]
        and acknowledgement.get("status") in {"pass", "fail"}
        and re.fullmatch(r"sha256:[0-9a-f]{64}", str(acknowledgement.get("result_digest", "")))
        and isinstance(stores, dict) and set(stores) == set(owners) | {"validation"}
        and all(value in {"pending", "not_required", "committed", "unconfirmed"} for value in stores.values())
        and (acknowledgement["status"] != "pass" or (stores["validation"] == "committed" and all(value in {"committed", "not_required"} for value in stores.values())))
    )
    if ack_valid:
        runtime["ingestion_outcome"] = acknowledgement["status"]
        runtime["stores"] = stores
        runtime["acknowledgement"] = acknowledgement
    if not summary or summary.get("producer") != "supervisor" or summary.get("node_name") != intent["node_name"]:
        return runtime
    runtime["summary"] = summary
    for field in ("tests_outcome", "logs_state", "result_state"):
        runtime[field] = summary.get(field, "unconfirmed")
    tests = summary.get("tests")
    if not isinstance(tests, dict) or not selected or not selected.issubset(tests):
        return runtime
    progress.update({name: tests[name]["status"] for name in selected
                     if isinstance(tests[name], dict) and tests[name].get("status") in {"pass", "fail", "incomplete"}})
    if any(not isinstance(tests[name], dict) or tests[name].get("phase") != "finished" or tests[name].get("status") != "pass" for name in selected):
        return runtime
    expected_stores = {name: "committed" if owner in selected else "not_required" for name, owner in owners.items()} | {"validation": "committed"}
    runtime["completion_confirmed"] = bool(
        ack_valid and runtime["ingestion_outcome"] == "pass" and summary.get("ingestion_outcome") == "pass"
        and summary.get("stores") == acknowledgement.get("stores") == expected_stores
        and re.fullmatch(r"sha256:[0-9a-f]{64}", str(summary.get("result_digest", "")))
        and summary["result_digest"] == acknowledgement.get("result_digest")
        and summary.get("status") == summary.get("tests_outcome") == "pass"
        and type(summary.get("exit_code")) is int and summary["exit_code"] == 0
        and summary.get("logs_state") == "retained" and summary.get("result_state") == "finalized"
    )
    return runtime


def observe_validation_job(intent: dict, *, config: CvalConfig, client: KubectlClient | None = None,
                           timeout: float | None = None) -> dict:
    """Observe one owned Job/pod and bounded logs; never query PVC or retry work."""

    return observe_validation_jobs([intent], config=config, client=client, timeout=timeout)[0]


def _read_inventory(args: list[str], *, config: CvalConfig, client: KubectlClient, deadline: float) -> dict:
    remaining = min(deadline - time.monotonic(), config.cluster.kubectl_timeout_seconds)
    if remaining <= 0:
        return {"state": "unavailable", "reason": "Observation deadline expired", "items": {}}
    try:
        result = client.run([*args, "-o", "json"], check=False, timeout=remaining)
        if result.returncode:
            return {"state": "forbidden" if "forbidden" in result.stderr.lower() else "unavailable",
                    "reason": result.stderr.strip(), "items": {}}
        payload = json.loads(result.stdout)
        if not isinstance(payload["items"], list) or payload.get("metadata", {}).get("continue"):
            raise ValueError("Incomplete resource inventory")
        items = {item["metadata"]["name"]: item for item in payload["items"]}
        if len(items) != len(payload["items"]) or any(not name for name in items):
            raise ValueError("Duplicate or empty resource identity")
        return {"state": "queried", "items": items}
    except (OSError, KeyError, TypeError, ValueError, AttributeError) as error:
        return {"state": "unavailable", "reason": str(error), "items": {}}


def observe_validation_jobs(intents: Sequence[dict], *, config: CvalConfig, client: KubectlClient | None = None,
                            timeout: float | None = None) -> list[dict]:
    """Batch inventories, retaining one shared deadline and per-container log identity."""

    if not intents:
        return []
    kubectl = client or KubectlClient(config=config)
    deadline = time.monotonic() + (config.cluster.kubectl_timeout_seconds if timeout is None else timeout)
    jobs = _read_inventory(["get", "vcjob", "-n", config.cluster.namespace], config=config, client=kubectl, deadline=deadline)
    names = sorted({intent["job_name"] for intent in intents})
    pods = _read_inventory(["get", "pods", "-n", config.cluster.namespace, "-l", f"volcano.sh/job-name in ({','.join(names)})"],
                           config=config, client=kubectl, deadline=deadline) if jobs["state"] == "queried" else jobs
    return [_observe_validation_job(intent, config=config, client=kubectl, deadline=deadline, jobs=jobs, pod_inventory=pods)
            for intent in intents]


def _observe_validation_job(intent: dict, *, config: CvalConfig, client: KubectlClient,
                            deadline: float, jobs: dict, pod_inventory: dict) -> dict:
    from cval.jobs.manager import _requested_fields_match, verify_submission_identity

    observation = {"state": "uncertain", "observation": "unavailable", "phase": "Unknown",
                   "job_name": intent["job_name"], "run_id": intent["run_id"], "node_name": intent["node_name"],
                   "timestamp": intent["timestamp"], "git_ref": intent["git_ref"],
                   "config_digest": intent["config_digest"], "manifest_sha256": intent["manifest_sha256"],
                   "tracking_complete": False, "completion_confirmed": False,
                   "log_state": "unavailable", "runtime": {}, "pods": []}

    def command(args):
        remaining = min(deadline - time.monotonic(), config.cluster.kubectl_timeout_seconds)
        if remaining <= 0:
            raise TimeoutError("Job observation deadline expired")
        return client.run(args, check=False, timeout=remaining)

    def read(args):
        response = command([*args, "-o", "json"])
        if response.returncode:
            observation["observation"] = "forbidden" if "forbidden" in response.stderr.lower() else "unavailable"
            raise OSError(response.stderr.strip())
        return json.loads(response.stdout) if response.stdout.strip() else None

    try:
        namespace = config.cluster.namespace
        for inventory in (jobs, pod_inventory):
            if inventory["state"] != "queried":
                observation.update(observation=inventory["state"], reason=inventory["reason"])
                return observation
        job = jobs["items"].get(intent["job_name"])
        if job is None:
            observation.update(phase="Missing", observation="absent", reason="Exact Job not found; do not recreate")
            return observation
        uid = verify_submission_identity(intent, job, namespace)
        phase = job.get("status", {}).get("state", {}).get("phase", "Unknown")
        observation.update(uid=uid, phase=phase, created_at=job["metadata"]["creationTimestamp"])
        pods = [pod for pod in pod_inventory["items"].values()
            if pod["metadata"].get("labels", {}).get("volcano.sh/job-name") == intent["job_name"]]
        for pod in pods:
            if not any(owner.get("uid") == uid and owner.get("name") == intent["job_name"] for owner in pod["metadata"].get("ownerReferences", [])):
                raise ValueError("Pod owner differs from the exact submitted Job")
            if pod["spec"].get("nodeName") not in {None, "", intent["node_name"]}:
                raise ValueError("Pod is assigned to a different node")
        if len(pods) > 1:
            raise ValueError("Multiple pods observed for a single-attempt validation")
        terminal = phase in TERMINAL_PHASES and all(pod.get("status", {}).get("phase") in {"Succeeded", "Failed"} for pod in pods)
        observation.update(state="tracking_finished" if terminal else "observed", observation="queried", tracking_complete=terminal)
        if not pods:
            observation.update(log_state="absent", reason="No retained pod; log/ingestion completion unconfirmed")
            return observation
        pod = pods[0]
        pod_uid = pod["metadata"]["uid"]
        prior_uids = {item["uid"] for event in intent["events"] for item in event.get("pods", [])}
        if prior_uids and prior_uids != {pod_uid}:
            raise ValueError("Owned pod UID changed between observations")
        expected = job["spec"]["tasks"][0]["template"]["spec"]["containers"][0]
        containers = pod["spec"]["containers"]
        if len(containers) != 1 or not _requested_fields_match(
            {key: expected[key] for key in ("name", "image", "command", "args")}, containers[0]
        ):
            raise ValueError("Pod runtime identity differs from the submitted container")
        for entry in expected["env"]:
            values = [value for value in containers[0].get("env", []) if value.get("name") == entry["name"]]
            if len(values) != 1 or not _requested_fields_match(entry, values[0]):
                raise ValueError("Pod environment differs from the submitted container")
        observation["pods"] = [{"name": pod["metadata"]["name"], "uid": pod_uid, "phase": pod.get("status", {}).get("phase", "Unknown"),
                                "container": expected["name"], "node_name": pod["spec"].get("nodeName")}]
        statuses = pod.get("status", {}).get("containerStatuses", [])
        observation["pods"][0]["container_statuses"] = [
            {"name": item["name"], "restart_count": item.get("restartCount"),
             "terminated": {key: value for key, value in item.get("state", {}).get("terminated", {}).items()
                            if key in {"exitCode", "reason", "finishedAt"}}} for item in statuses
        ]
        previous = next((event for event in reversed(intent["events"]) if event.get("log_cursor")), None)
        identity_keys = ("job_name", "run_id", "node_name", "timestamp", "git_ref", "config_digest", "manifest_sha256", "uid")
        if previous and (any(previous.get(key) != observation[key] for key in identity_keys)
                         or previous["log_cursor"].get("pod_uid") != pod_uid
                         or previous["log_cursor"].get("container") != expected["name"]):
            raise ValueError("Retained log cursor differs from the submitted Job/pod/run")
        since_time = previous["log_cursor"]["since_time"] if previous else None
        log_args = ["logs", "-n", namespace, pod["metadata"]["name"], "-c", expected["name"],
                    "--timestamps=true", f"--limit-bytes={config.monitoring.verification_max_bytes}"]
        log_args += [f"--since-time={since_time}", "--tail=-1"] if since_time else [f"--tail={config.monitoring.log_tail_lines}"]
        observation["log_read_mode"] = "incremental" if since_time else "tail"
        observation["log_since_time"] = since_time
        response = command(log_args)
        observation["log_state"] = "queried" if response.returncode == 0 else "forbidden" if "forbidden" in response.stderr.lower() else "unavailable"
        if response.returncode:
            observation["reason"] = response.stderr.strip()
            return observation
        after = read(["get", "pod", pod["metadata"]["name"], "-n", namespace, "--ignore-not-found"])
        if after is None or after["metadata"]["uid"] != pod_uid:
            raise ValueError("Pod identity changed during log fetch")
        observation["log_sha256"] = hashlib.sha256(response.stdout.encode()).hexdigest()
        observation["log_bytes"] = len(response.stdout.encode())
        cursor_time = datetime.fromisoformat(since_time) if since_time else None
        messages = []
        for line in response.stdout.splitlines(keepends=True):
            if not line.endswith("\n"):
                continue
            timestamp, separator, message = line.partition(" ")
            try:
                instant = datetime.fromisoformat(timestamp)
                if not separator or instant.tzinfo is None:
                    raise ValueError("Missing timestamp timezone")
            except ValueError:
                observation.update(log_state="unavailable", reason="Unparseable timestamped container log")
                return observation
            cursor_time = max(cursor_time, instant) if cursor_time else instant
            messages.append(message)
        observation["runtime"] = _runtime_progress("".join(messages), intent, config, previous.get("runtime") if previous else None)
        if cursor_time is not None:
            observation["log_cursor"] = {"pod_uid": pod_uid, "container": expected["name"],
                                         "since_time": cursor_time.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")}
        if observation["log_bytes"] >= config.monitoring.verification_max_bytes or (response.stdout and not response.stdout.endswith("\n")):
            observation.update(log_state="limited", reason="Bounded log response may be truncated; completion unconfirmed")
            observation["runtime"]["completion_confirmed"] = False
        pod_success = pod["status"].get("phase") == "Succeeded" and len(statuses) == 1 and statuses[0].get("restartCount") == 0 and statuses[0].get("state", {}).get("terminated", {}).get("exitCode") == 0
        observation["completion_confirmed"] = terminal and phase in {"Completed", "Succeeded"} and pod_success and observation["log_state"] == "queried" and observation["runtime"]["completion_confirmed"]
    except (TimeoutError, OSError) as error:
        observation.update(state="uncertain", tracking_complete=False, completion_confirmed=False, reason=str(error))
    except Exception as error:
        observation.update(state="conflict", observation="conflicting", tracking_complete=False, completion_confirmed=False, reason=str(error))
    return observation


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
    config: CvalConfig | None = None,
) -> JobPhase:
    """Read one Volcano job phase using a non-mutating kubectl get."""

    active_config = config or load_config()
    kubectl = client or KubectlClient(config=active_config)
    resolved_namespace = active_config.cluster.namespace if namespace is None else namespace
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
    config: CvalConfig | None = None,
    timeout: float | None = None,
) -> list[JobPhase]:
    """Read requested phases from one complete namespace inventory."""

    active_config = config or load_config()
    kubectl = client or KubectlClient(config=active_config)
    resolved_namespace = active_config.cluster.namespace if namespace is None else namespace
    unknown = [JobPhase(name, "Unknown") for name in job_names]
    if not job_names or (timeout is not None and timeout <= 0):
        return unknown
    deadline = time.monotonic() + (active_config.cluster.kubectl_timeout_seconds if timeout is None else timeout)
    inventory = _read_inventory(["get", "vcjob", "-n", resolved_namespace], config=active_config, client=kubectl, deadline=deadline)
    if inventory["state"] != "queried":
        return unknown
    try:
        phases = {name: item.get("status", {}).get("state", {}).get("phase") or "Unknown"
                  for name, item in inventory["items"].items()}
        return [JobPhase(name, phases.get(name, "Missing")) for name in job_names]
    except (KeyError, TypeError, ValueError, AttributeError):
        return unknown


def monitor_jobs_until_terminal(
    job_names: Sequence[str],
    namespace: str | None = None,
    timeout_seconds: float | None = None,
    poll_interval_seconds: float | None = None,
    client: KubectlClient | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    config: CvalConfig | None = None,
) -> list[MonitoredJob]:
    """Poll jobs until every job is terminal or the timeout expires."""

    config = config or load_config()
    resolved_namespace = namespace or config.cluster.namespace
    resolved_timeout_seconds = (
        timeout_seconds if timeout_seconds is not None else config.monitoring.timeout_seconds
    )
    resolved_poll_interval_seconds = (
        poll_interval_seconds if poll_interval_seconds is not None else config.monitoring.poll_interval_seconds
    )
    kubectl = client or KubectlClient(config=config)
    start = clock()
    deadline = start + resolved_timeout_seconds
    latest: dict[str, JobPhase] = {
        job_name: JobPhase(job_name, "Unknown") for job_name in job_names
    }

    while True:
        remaining = deadline - clock()
        if remaining > 0:
            phases = get_job_phases(job_names, namespace=resolved_namespace, client=kubectl, config=config, timeout=remaining)
            latest = {phase.job_name: phase for phase in phases}
        elapsed = max(0.0, clock() - start)
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
        sleeper(min(max(0.0, resolved_poll_interval_seconds), max(0.0, deadline - clock())))


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