"""Volcano validation job rendering.

This module turns the checked-in YAML template into a concrete one-node
validation job. It does not submit anything; it only replaces placeholders and
returns the rendered manifest for inspection or policy-gated submission.
"""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any

import yaml
from yaml.constructor import ConstructorError
from yaml.resolver import BaseResolver

from cval.config import CvalConfig, JobTemplateConfig, load_config
from cval.models import RenderedJob
from cval.validation.runtime import build_runtime_environment, encode_runtime_environment


NODE_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
MAX_VOLCANO_NAME_PART_LENGTH = 63
VOLCANO_TASK_POD_SUFFIX = "-server-0"


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
    node_name: str,
    timestamp: int | None = None,
    job_prefix: str | None = None,
    git_repo: str | None = None,
    git_ref: str | None = None,
    job_template_config: JobTemplateConfig | None = None,
    cval_config: CvalConfig | None = None,
) -> RenderedJob:
    """Render one validation job manifest for one target node."""

    config = cval_config or load_config()
    resolved_job_prefix = job_prefix if job_prefix is not None else config.job.job_prefix
    resolved_git_repo = git_repo if git_repo is not None else config.job.git_repo
    resolved_git_ref = git_ref if git_ref is not None else config.job.git_ref
    template_config = job_template_config or config.job_template
    resolved_image_name = config.job.image_name or _image_name_from_container(
        template_config.container_image
    )
    if template_config.gpu_resource_name == template_config.rdma_resource_name:
        raise ValueError("GPU and RDMA resource names must be distinct YAML keys")
    rendered_timestamp = int(time.time()) if timestamp is None else int(timestamp)
    job_name = make_job_name(
        node_name,
        rendered_timestamp,
        job_prefix=resolved_job_prefix,
        image_name=resolved_image_name,
    )

    required_placeholders = [
        "nodename-placeholder",
        "time-placeholder",
        "jobname-placeholder",
        "runtime-environment-b64-placeholder",
    ]
    # These placeholders are required by the legacy template and core scheduler logic.
    missing = [
        placeholder
        for placeholder in required_placeholders
        if placeholder not in template_text
    ]
    if missing:
        raise ValueError(f"Template is missing placeholder(s): {', '.join(missing)}")
    _validate_runtime_bootstrap(template_text)

    yaml_text = template_text.replace("nodename-placeholder", node_name)
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
    _validate_semantic_runtime_manifest(
        yaml_text,
        expected_runtime_environment=runtime_replacements[
            "runtime-environment-b64-placeholder"
        ],
    )

    return RenderedJob(
        job_name=job_name,
        node_name=node_name,
        timestamp=rendered_timestamp,
        yaml_text=yaml_text,
    )


def _validate_runtime_bootstrap(template_text: str) -> None:
    """Require the ordered executable safety bootstrap in the Bash args block."""

    _validate_semantic_runtime_manifest(template_text)
    lines = template_text.splitlines()
    task_sections = [
        index
        for index, line in enumerate(lines)
        if re.match(r"^\s*tasks\s*:", line)
    ]
    if len(task_sections) != 1 or lines[task_sections[0]].strip() != "tasks:":
        raise ValueError(
            "Template must contain exactly one block-style Volcano tasks section"
        )
    tasks_index = task_sections[0]
    tasks_indent = len(lines[tasks_index]) - len(lines[tasks_index].lstrip())
    task_entries: list[str] = []
    for line in lines[tasks_index + 1 :]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= tasks_indent:
            break
        if indent == tasks_indent + 2 and line.strip().startswith("-"):
            task_entries.append(line.strip())
    if task_entries != ["- name: server"]:
        raise ValueError("Template must contain exactly one Volcano task named server")
    container_sections = [
        line
        for line in lines
        if re.match(r"^\s*containers\s*:", line)
    ]
    if len(container_sections) != 1 or container_sections[0].strip() != "containers:":
        raise ValueError(
            "Template must contain exactly one block-style task containers section"
        )
    if any(re.match(r"^\s*initContainers\s*:", line) for line in lines):
        raise ValueError("Template must not contain init containers")
    runtime_env_occurrences = [
        index
        for index, line in enumerate(lines)
        if line.strip() == "- name: CVAL_RUNTIME_ENV_B64"
    ]
    if len(runtime_env_occurrences) != 1:
        raise ValueError(
            "Template must contain exactly one CVAL_RUNTIME_ENV_B64 environment entry"
        )
    env_index = next(
        (
            index
            for index, line in enumerate(lines)
            if line.strip() == "- name: CVAL_RUNTIME_ENV_B64"
        ),
        None,
    )
    if env_index is None or not any(
        line.strip() == 'value: "runtime-environment-b64-placeholder"'
        for line in lines[env_index + 1 : env_index + 4]
    ):
        raise ValueError("Template must transport CVAL_RUNTIME_ENV_B64")

    env_parent_index = next(
        (
            index
            for index in range(env_index, -1, -1)
            if lines[index].strip() in {"env:", "- env:"}
        ),
        None,
    )
    if env_parent_index is None:
        raise ValueError("CVAL_RUNTIME_ENV_B64 must belong to a container env block")
    env_parent_indent = len(lines[env_parent_index]) - len(
        lines[env_parent_index].lstrip()
    )
    container_indent = (
        env_parent_indent
        if lines[env_parent_index].strip() == "- env:"
        else env_parent_indent - 2
    )
    container_start = next(
        (
            index
            for index in range(env_parent_index, -1, -1)
            if lines[index].lstrip().startswith("-")
            and len(lines[index]) - len(lines[index].lstrip()) == container_indent
        ),
        None,
    )
    if container_start is None:
        raise ValueError("Could not identify the runtime workload container")
    if lines[container_start].strip() != "- name: server":
        raise ValueError("CVAL runtime bootstrap must belong to the server container")
    containers_index = next(
        (
            index
            for index in range(container_start, -1, -1)
            if lines[index].strip() == "containers:"
            and len(lines[index]) - len(lines[index].lstrip()) == container_indent - 2
        ),
        None,
    )
    if containers_index is None:
        raise ValueError("The server runtime must belong to the task containers block")
    container_end = len(lines)
    for index in range(env_index + 1, len(lines)):
        stripped = lines[index].strip()
        indent = len(lines[index]) - len(lines[index].lstrip())
        if stripped.startswith("- name:") and indent == container_indent:
            container_end = index
            break
    containers_end = len(lines)
    containers_indent = container_indent - 2
    for index in range(containers_index + 1, len(lines)):
        if not lines[index].strip():
            continue
        indent = len(lines[index]) - len(lines[index].lstrip())
        if indent <= containers_indent:
            containers_end = index
            break
    workload_containers = [
        line.strip()
        for line in lines[containers_index + 1 : containers_end]
        if len(line) - len(line.lstrip()) == container_indent
        and line.strip().startswith("-")
    ]
    if workload_containers != ["- name: server"]:
        raise ValueError(
            "Template must contain exactly one task workload container named server"
        )
    bootstrap_markers = (
        "python3 -m cval.validation.path_preflight",
        "run_child bash run-test.sh",
        "run_child bash db-update.sh",
    )
    for marker in bootstrap_markers:
        occurrences = [
            index
            for index, line in enumerate(lines)
            if not line.strip().startswith("#") and marker in line
        ]
        if len(occurrences) != 1 or not (
            container_start <= occurrences[0] < container_end
        ):
            raise ValueError(
                f"Runtime bootstrap marker must appear exactly once in server: {marker}"
            )
    container_lines = lines[container_start:container_end]
    if not any(
        line.strip() == 'command: ["/bin/bash", "-lc"]'
        for line in container_lines
    ):
        raise ValueError(
            "The CVAL_RUNTIME_ENV_B64 workload container must use /bin/bash -lc"
        )

    script_lines: list[str] | None = None
    for index, line in enumerate(container_lines[:-1]):
        if line.strip() != "args:":
            continue
        for item_index in range(index + 1, min(index + 4, len(container_lines))):
            if container_lines[item_index].strip() != "- |":
                continue
            base_indent = len(container_lines[item_index]) - len(
                container_lines[item_index].lstrip()
            )
            collected: list[str] = []
            for script_line in container_lines[item_index + 1 :]:
                indent = len(script_line) - len(script_line.lstrip())
                if script_line.strip() and indent <= base_indent:
                    break
                stripped = script_line.strip()
                if stripped and not stripped.startswith("#"):
                    collected.append(stripped)
            script_lines = collected
            break
        if script_lines is not None:
            break
    if script_lines is None:
        raise ValueError("Template must contain one executable Bash args block")

    ordered_contract: tuple[tuple[str, str], ...] = (
        ('git clone "$CVAL_GIT_REPO" "$CVAL_REPO_DIR"', "exact"),
        ('git checkout "$CVAL_GIT_REF"', "exact"),
        ('printf \'%s\' "$CVAL_RUNTIME_ENV_B64" | base64 -d > /tmp/cval-runtime.env', "exact"),
        ("source /tmp/cval-runtime.env", "exact"),
        ("python3 -m cval.validation.path_preflight", "prefix"),
        ('--test-registry-json "$CVAL_TEST_REGISTRY_JSON"', "contains"),
        ('mkdir -p "$CVAL_JOB_LOG_DIR"', "exact"),
        ("(set -o noclobber;", "prefix"),
        ("exec > >(", "prefix"),
        ("run_child bash run-test.sh", "exact"),
        ("source 0-env.sh", "exact"),
        ("run_child bash db-update.sh", "exact"),
    )
    cursor = 0
    missing: list[str] = []
    for token, mode in ordered_contract:
        match = next(
            (
                index
                for index in range(cursor, len(script_lines))
                if (
                    token in script_lines[index]
                    if mode == "contains"
                    else script_lines[index].startswith(token)
                    if mode == "prefix"
                    else script_lines[index] == token
                )
            ),
            None,
        )
        if match is None:
            missing.append(token)
            continue
        cursor = match + 1
    if missing:
        raise ValueError(
            "Template is missing ordered executable runtime contract line(s): "
            f"{', '.join(missing)}"
        )


def _validate_semantic_runtime_manifest(
    template_text: str,
    *,
    expected_runtime_environment: str = "runtime-environment-b64-placeholder",
) -> None:
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


def render_validation_job_from_file(
    template_path: Path,
    node_name: str,
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


def _image_name_from_container(container_image: str) -> str:
    """Return the human image name from a full container image reference."""

    return container_image.rsplit("/", 1)[-1]


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