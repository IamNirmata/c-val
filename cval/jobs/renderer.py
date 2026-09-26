"""Volcano validation job rendering.

This module turns the checked-in YAML template into a concrete one-node
validation job. It does not submit anything; it only replaces placeholders and
returns the rendered manifest for inspection or policy-gated submission.
"""

from __future__ import annotations

import json
import re
import textwrap
import time
from dataclasses import replace
from importlib.resources import files
from pathlib import Path
from typing import Any

import yaml
from yaml.constructor import ConstructorError
from yaml.resolver import BaseResolver

from cval.config import CvalConfig, JobTemplateConfig, is_exact_commit, load_config, validate_single_attempt_config
from cval.models import RenderedJob
from cval.validation.runtime import build_runtime_environment, encode_runtime_environment


NODE_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
MAX_VOLCANO_NAME_PART_LENGTH = 63
VOLCANO_TASK_POD_SUFFIX = "-server-0"

_RUNTIME_BOOTSTRAP = files("cval.jobs").joinpath("bootstrap.sh").read_text(encoding="utf-8")


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects every duplicate semantic mapping key."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable mapping key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def default_template_path() -> Path:
    """Return the repository default Volcano job template path."""

    return load_config().job.template_path


def make_job_name(
    node_name: str,
    timestamp: int,
    job_prefix: str | None = None,
    image_name: str | None = None,
) -> str:
    """Build a deterministic Kubernetes-compatible validation job name."""

    resolved_prefix = job_prefix if job_prefix is not None else load_config().job.job_prefix
    validate_kubernetes_name(node_name, "node_name")
    validate_kubernetes_name(resolved_prefix, "job_prefix")
    image_label = _image_name_label(image_name) if image_name else ""
    job_name = (
        f"{resolved_prefix}-{node_name}-{image_label}-{timestamp}"
        if image_label
        else f"{resolved_prefix}-{node_name}-{timestamp}"
    )
    _validate_volcano_pod_name(job_name)
    return job_name


def render_validation_job(
    template_text: str,
    node_name: str | None,
    timestamp: int | None = None,
    job_prefix: str | None = None,
    git_repo: str | None = None,
    git_ref: str | None = None,
    job_template_config: JobTemplateConfig | None = None,
    cval_config: CvalConfig | None = None,
) -> RenderedJob:
    """Render a pinned job, or a scheduler-selected test when node_name is None."""

    config = cval_config or load_config()
    resolved_job_prefix = job_prefix if job_prefix is not None else config.job.job_prefix
    resolved_git_repo = git_repo if git_repo is not None else config.job.git_repo
    resolved_git_ref = git_ref if git_ref is not None else config.job.git_ref
    template_config = job_template_config or config.job_template
    validate_single_attempt_config(template_config)
    config = replace(config, job=replace(config.job, git_ref=resolved_git_ref, git_repo=resolved_git_repo, job_prefix=resolved_job_prefix), job_template=template_config)
    resolved_image_name = config.job.image_name
    if template_config.gpu_resource_name == template_config.rdma_resource_name:
        raise ValueError("GPU and RDMA resource names must be distinct YAML keys")
    rendered_timestamp = int(time.time()) if timestamp is None else int(timestamp)
    job_name = make_job_name(
        "scheduled" if node_name is None else node_name,
        rendered_timestamp,
        job_prefix=resolved_job_prefix,
        image_name=resolved_image_name,
    )

    required_placeholders = [
        "time-placeholder",
        "jobname-placeholder",
        "runtime-environment-b64-placeholder",
    ]
    if node_name is not None:
        required_placeholders.append("nodename-placeholder")
    # These placeholders are required by the legacy template and core scheduler logic.
    missing = [
        placeholder
        for placeholder in required_placeholders
        if placeholder not in template_text
    ]
    if missing:
        raise ValueError(f"Template is missing placeholder(s): {', '.join(missing)}")
    template_text = _expand_runtime_bootstrap(template_text)
    _validate_runtime_bootstrap(template_text)
    if node_name is None:
        if not is_exact_commit(resolved_git_ref):
            raise ValueError("Scheduler-selected tests require an exact commit")
        template = yaml.load(template_text, Loader=_UniqueKeySafeLoader)
        pod = template["spec"]["tasks"][0]["template"]["spec"]
        if pod.get("nodeName") or pod.get("nodeSelector") or pod.get("affinity"):
            raise ValueError("Scheduler-selected test template must not set node placement")
        environment = pod["containers"][0]["env"]
        nodes = [entry for entry in environment if entry.get("name") == "GCRNODE"]
        run_ids = [entry for entry in environment if entry.get("name") == "CVAL_RUN_ID"]
        if len(nodes) != 1 or nodes[0].get("valueFrom", {}).get("fieldRef", {}).get("fieldPath") != "spec.nodeName" or "value" in nodes[0]:
            raise ValueError("Scheduler-selected test must obtain GCRNODE from spec.nodeName")
        if len(run_ids) != 1 or run_ids[0].get("value") != "$(GCRNODE)-time-placeholder" or environment.index(nodes[0]) >= environment.index(run_ids[0]):
            raise ValueError("Scheduler-selected test run ID must use the assigned GCRNODE")

    yaml_text = template_text.replace("nodename-placeholder", node_name or "")
    yaml_text = yaml_text.replace("time-placeholder", str(rendered_timestamp))
    yaml_text = yaml_text.replace("generateName: jobname-placeholder", f"name: {job_name}")
    yaml_text = yaml_text.replace("jobname-placeholder", job_name)
    _validate_substitution_value("git-repo-placeholder", resolved_git_repo)
    _validate_substitution_value("git-ref-placeholder", resolved_git_ref)
    yaml_text = yaml_text.replace("git-repo-placeholder", resolved_git_repo)
    yaml_text = yaml_text.replace("git-ref-placeholder", resolved_git_ref)
    template_replacements = _job_template_replacements(template_config)
    runtime_replacements = _runtime_replacements(config)
    replacements = {
        "image-name-placeholder": resolved_image_name,
        **template_replacements,
        **runtime_replacements,
    }
    for placeholder, value in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
        _validate_substitution_value(placeholder, value)
        yaml_text = yaml_text.replace(placeholder, value)

    # Refuse partially rendered manifests; a placeholder in submitted YAML is dangerous.
    known_placeholders = [
        *required_placeholders,
        *replacements.keys(),
    ]
    remaining = [placeholder for placeholder in known_placeholders if placeholder in yaml_text]
    if remaining:
        raise ValueError(f"Template still contains placeholder(s): {', '.join(remaining)}")
    if config.job.source_configmap or config.job.source_sha256:
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", config.job.source_configmap) or not re.fullmatch(r"[a-f0-9]{64}", config.job.source_sha256) or not is_exact_commit(resolved_git_ref):
            raise ValueError("offline source requires a ConfigMap name, SHA-256, and exact commit")
        manifest = yaml.load(yaml_text, Loader=_UniqueKeySafeLoader)
        pod = manifest["spec"]["tasks"][0]["template"]["spec"]
        container = pod["containers"][0]
        if any(volume.get("name") == "exact-source" for volume in pod.get("volumes", [])) or any(mount.get("mountPath") == "/cval-source" for mount in container.get("volumeMounts", [])):
            raise ValueError("offline source volume conflicts with the template")
        if any(entry.get("name") == "CVAL_SOURCE_SHA256" for entry in container["env"]):
            raise ValueError("template must not supply an unverified source digest")
        pod["volumes"].append({"name": "exact-source", "configMap": {"name": config.job.source_configmap, "items": [{"key": "source.tar.gz", "path": "source.tar.gz"}]}})
        container["volumeMounts"].append({"name": "exact-source", "mountPath": "/cval-source", "readOnly": True})
        container["env"].append({"name": "CVAL_SOURCE_SHA256", "value": config.job.source_sha256})
        yaml_text = yaml.safe_dump(manifest, sort_keys=False)
    _validate_semantic_runtime_manifest(
        yaml_text,
        expected_runtime_environment=runtime_replacements[
            "runtime-environment-b64-placeholder"
        ],
        job_template_config=template_config,
    )

    return RenderedJob(
        job_name=job_name,
        node_name=node_name or "",
        timestamp=rendered_timestamp,
        git_ref=resolved_git_ref,
        yaml_text=yaml_text,
    )


def _expand_runtime_bootstrap(template_text: str) -> str:
    """Embed the shared payload without changing template structure or placement."""

    placeholder = "runtime-bootstrap-placeholder"
    if placeholder not in template_text:
        return template_text
    container = _validate_semantic_runtime_manifest(template_text)
    if container["args"][0].strip() != placeholder:
        raise ValueError("Shared runtime bootstrap must occupy the entire server args block")
    if template_text.count(placeholder) != 1:
        raise ValueError("Template must contain exactly one runtime bootstrap placeholder")
    expanded, count = re.subn(
        rf"(?m)^( +){placeholder}[ \t]*$",
        lambda match: textwrap.indent(_RUNTIME_BOOTSTRAP.rstrip("\n"), match.group(1)),
        template_text,
    )
    if count != 1:
        raise ValueError("Runtime bootstrap placeholder must be on its own indented line")
    return expanded


def _validate_runtime_bootstrap(template_text: str) -> None:
    """Accept only the packaged startup program in the actual workload container."""

    container = _validate_semantic_runtime_manifest(template_text)
    canonical = Path(__file__).with_name("bootstrap.sh").read_text(encoding="utf-8")
    if container["args"][0].strip() != canonical.strip():
        raise ValueError("Template must use the packaged canonical runtime bootstrap")


def _validate_semantic_runtime_manifest(
    template_text: str,
    *,
    expected_runtime_environment: str = "runtime-environment-b64-placeholder",
    job_template_config: JobTemplateConfig | None = None,
) -> dict[str, Any]:
    """Validate decoded YAML keys and the actual Volcano workload structure."""

    try:
        manifest = yaml.load(template_text, Loader=_UniqueKeySafeLoader)
    except yaml.YAMLError as exc:
        raise ValueError(f"Template must be valid duplicate-free YAML: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ValueError("Template must contain one YAML mapping document")

    structural_counts = {"tasks": 0, "containers": 0, "initContainers": 0}

    def count_structural_keys(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if isinstance(key, str) and key in structural_counts:
                    structural_counts[key] += 1
                count_structural_keys(child)
        elif isinstance(value, list):
            for child in value:
                count_structural_keys(child)

    count_structural_keys(manifest)
    if structural_counts["tasks"] != 1:
        raise ValueError("Template must contain exactly one semantic Volcano tasks key")
    if structural_counts["initContainers"]:
        raise ValueError("Template must not contain init containers")

    spec = manifest.get("spec")
    tasks = spec.get("tasks") if isinstance(spec, dict) else None
    if not isinstance(tasks, list) or len(tasks) != 1:
        raise ValueError("Template must contain exactly one Volcano task named server")
    task = tasks[0]
    if not isinstance(task, dict) or task.get("name") != "server":
        raise ValueError("Template must contain exactly one Volcano task named server")
    if structural_counts["containers"] != 1:
        raise ValueError("Template must contain exactly one semantic containers key")
    template = task.get("template")
    pod_spec = template.get("spec") if isinstance(template, dict) else None
    containers = pod_spec.get("containers") if isinstance(pod_spec, dict) else None
    if not isinstance(containers, list) or len(containers) != 1:
        raise ValueError(
            "Template must contain exactly one task workload container named server"
        )
    container = containers[0]
    if not isinstance(container, dict) or container.get("name") != "server":
        raise ValueError(
            "Template must contain exactly one task workload container named server"
        )
    if job_template_config is not None:
        _validate_single_attempt_manifest(manifest, job_template_config)
    if container.get("command") != ["/bin/bash", "-lc"]:
        raise ValueError(
            "The CVAL_RUNTIME_ENV_B64 workload container must use /bin/bash -lc"
        )
    environment = container.get("env")
    if not isinstance(environment, list):
        raise ValueError("The server container must contain an env list")
    runtime_entries = [
        entry
        for entry in environment
        if isinstance(entry, dict) and entry.get("name") == "CVAL_RUNTIME_ENV_B64"
    ]
    if (
        len(runtime_entries) != 1
        or runtime_entries[0].get("value") != expected_runtime_environment
    ):
        raise ValueError("Template must transport exactly one CVAL_RUNTIME_ENV_B64")
    args = container.get("args")
    if (
        not isinstance(args, list)
        or len(args) != 1
        or not isinstance(args[0], str)
        or not args[0].strip()
    ):
        raise ValueError("Template must contain one executable Bash args block")
    return container


def _validate_single_attempt_manifest(manifest: dict, config: JobTemplateConfig) -> None:
    """Enforce the decoded manifest contract, including task-policy precedence."""

    validate_single_attempt_config(config)
    spec = manifest["spec"]
    task = spec["tasks"][0]
    pod = task["template"]["spec"]
    policy = [{"events": list(config.failure_events), "action": config.failure_action}]
    if manifest.get("apiVersion") != "batch.volcano.sh/v1alpha1" or manifest.get("kind") != "Job":
        raise ValueError("Single-attempt validation requires a Volcano v1alpha1 Job")
    if type(spec.get("maxRetry")) is not int or spec["maxRetry"] != config.job_max_retry:
        raise ValueError("Job maxRetry differs from single-attempt configuration")
    if type(task.get("maxRetry")) is not int or task["maxRetry"] != config.task_max_retry:
        raise ValueError("Task maxRetry differs from single-attempt configuration")
    if type(task.get("replicas")) is not int or task["replicas"] != 1:
        raise ValueError("Single-attempt validation requires exactly one replica")
    if spec.get("policies") != policy or "policies" in task:
        raise ValueError("Validation requires exact termination policies with no task overrides")
    if "ttlSecondsAfterFinished" in spec:
        raise ValueError("Validation must retain terminal Job/pod evidence without automatic TTL cleanup")
    if pod.get("restartPolicy") != config.pod_restart_policy:
        raise ValueError("Validation pod restartPolicy must match Never")
    for container in pod["containers"]:
        if "restartPolicy" in container or "restartPolicyRules" in container:
            raise ValueError("Container restart overrides are forbidden for validation")


def render_validation_job_from_file(
    template_path: Path,
    node_name: str | None,
    timestamp: int | None = None,
    job_prefix: str | None = None,
    git_repo: str | None = None,
    git_ref: str | None = None,
    job_template_config: JobTemplateConfig | None = None,
    cval_config: CvalConfig | None = None,
) -> RenderedJob:
    """Read a template file and render a validation job from it."""

    return render_validation_job(
        template_path.read_text(encoding="utf-8"),
        node_name=node_name,
        timestamp=timestamp,
        job_prefix=job_prefix,
        git_repo=git_repo,
        git_ref=git_ref,
        job_template_config=job_template_config,
        cval_config=cval_config,
    )


def validate_kubernetes_name(value: str, field_name: str) -> None:
    """Validate the subset of DNS-label syntax used by node/job names."""

    if not NODE_NAME_PATTERN.match(value):
        raise ValueError(f"{field_name} must be a lowercase DNS label-compatible name: {value!r}")


def _validate_substitution_value(placeholder: str, value: str) -> None:
    """Reject empty or multiline values before they are templated into YAML.

    An empty value would leave an empty field; a multiline value could break
    the manifest structure or inject additional YAML keys, so both are refused
    before substitution.
    """

    if not value:
        raise ValueError(
            f"Substitution value for {placeholder!r} must not be empty"
        )
    if "\n" in value or "\r" in value:
        raise ValueError(
            f"Substitution value for {placeholder!r} must be a single line: {value!r}"
        )


def _validate_volcano_pod_name(job_name: str) -> None:
    pod_name = f"{job_name}{VOLCANO_TASK_POD_SUFFIX}"
    if len(pod_name) > MAX_VOLCANO_NAME_PART_LENGTH:
        raise ValueError(
            "Rendered job name is too long for Volcano pod naming: "
            f"{pod_name!r} is {len(pod_name)} characters, over the "
            f"{MAX_VOLCANO_NAME_PART_LENGTH}-character limit"
        )


def _image_name_label(image_name: str) -> str:
    """Return a DNS-label-safe image segment for Kubernetes object names."""

    label = re.sub(r"[^a-z0-9]+", "-", image_name.lower()).strip("-")
    validate_kubernetes_name(label, "image_name")
    return label


def _job_template_replacements(config: JobTemplateConfig) -> dict[str, str]:
    """Return optional placeholder replacements for environment-specific job values."""

    return {
        "namespace-placeholder": config.namespace,
        "queue-placeholder": config.queue,
        "app-label-placeholder": config.app_label,
        "pvc-claim-placeholder": config.pvc_claim,
        "container-image-placeholder": config.container_image,
        "shared-memory-size-placeholder": config.shared_memory_size,
        "gpu-resource-name-placeholder": config.gpu_resource_name,
        "gpu-count-placeholder": config.gpu_count,
        "cpu-placeholder": config.cpu,
        "memory-placeholder": config.memory,
        "rdma-resource-name-placeholder": config.rdma_resource_name,
        "rdma-count-placeholder": config.rdma_count,
        "rdma-toleration-key-placeholder": config.rdma_toleration_key,
        "gpu-toleration-key-placeholder": config.gpu_toleration_key,
        "pod-restart-policy-placeholder": config.pod_restart_policy,
        "job-max-retry-placeholder": str(config.job_max_retry),
        "task-max-retry-placeholder": str(config.task_max_retry),
        "failure-action-placeholder": config.failure_action,
        "failure-events-placeholder": json.dumps(list(config.failure_events)),
    }


def _runtime_replacements(config: CvalConfig) -> dict[str, str]:
    """Return validation-runtime placeholder replacements from config."""

    return {
        "runtime-repo-dir-placeholder": config.runtime.repo_dir,
        "runtime-validation-root-placeholder": config.runtime.validation_root,
        "runtime-environment-b64-placeholder": encode_runtime_environment(
            build_runtime_environment(config)
        ),
    }