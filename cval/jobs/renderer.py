"""Volcano validation job rendering.

This module turns the checked-in YAML template into a concrete one-node
validation job. It does not submit anything; it only replaces placeholders and
returns the rendered manifest for inspection or policy-gated submission.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

from cval.config import CvalConfig, JobTemplateConfig, load_config
from cval.models import RenderedJob


NODE_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def default_template_path() -> Path:
    """Return the repository default Volcano job template path."""

    return load_config().job.template_path


def make_job_name(node_name: str, timestamp: int, job_prefix: str | None = None) -> str:
    """Build a deterministic Kubernetes-compatible validation job name."""

    resolved_prefix = job_prefix if job_prefix is not None else load_config().job.job_prefix
    validate_kubernetes_name(node_name, "node_name")
    validate_kubernetes_name(resolved_prefix, "job_prefix")
    return f"{resolved_prefix}-{node_name}-{timestamp}"


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
    rendered_timestamp = int(time.time()) if timestamp is None else int(timestamp)
    job_name = make_job_name(node_name, rendered_timestamp, job_prefix=resolved_job_prefix)

    required_placeholders = [
        "nodename-placeholder",
        "time-placeholder",
        "jobname-placeholder",
    ]
    # These placeholders are required by the legacy template and core scheduler logic.
    missing = [
        placeholder
        for placeholder in required_placeholders
        if placeholder not in template_text
    ]
    if missing:
        raise ValueError(f"Template is missing placeholder(s): {', '.join(missing)}")

    yaml_text = template_text.replace("nodename-placeholder", node_name)
    yaml_text = yaml_text.replace("time-placeholder", str(rendered_timestamp))
    yaml_text = yaml_text.replace("generateName: jobname-placeholder", f"name: {job_name}")
    yaml_text = yaml_text.replace("jobname-placeholder", job_name)
    yaml_text = yaml_text.replace("git-repo-placeholder", resolved_git_repo)
    yaml_text = yaml_text.replace("git-ref-placeholder", resolved_git_ref)
    template_replacements = _job_template_replacements(template_config)
    runtime_replacements = _runtime_replacements(config)
    replacements = {**template_replacements, **runtime_replacements}
    for placeholder, value in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
        yaml_text = yaml_text.replace(placeholder, value)

    # Refuse partially rendered manifests; a placeholder in submitted YAML is dangerous.
    known_placeholders = [
        *required_placeholders,
        *replacements.keys(),
    ]
    remaining = [placeholder for placeholder in known_placeholders if placeholder in yaml_text]
    if remaining:
        raise ValueError(f"Template still contains placeholder(s): {', '.join(remaining)}")

    return RenderedJob(
        job_name=job_name,
        node_name=node_name,
        timestamp=rendered_timestamp,
        yaml_text=yaml_text,
    )


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
        "runtime-validation-tests-dir-placeholder": config.runtime.validation_tests_dir,
        "runtime-dl-unit-test-dir-placeholder": config.runtime.dl_unit_test_dir,
        "storage-validation-db-path-placeholder": config.storage.validation_db_path,
        "storage-storage-db-path-placeholder": config.storage.storage_db_path,
        "storage-nccl-db-path-placeholder": config.storage.nccl_db_path,
        "validation-gpu-count-placeholder": str(config.validation.gpu_count),
        "validation-nccl-iterations-placeholder": str(config.validation.nccl_iterations),
        "validation-nccl-data-size-gb-placeholder": str(config.validation.nccl_data_size_gb),
        "validation-dl-test-plan-placeholder": config.validation.dl_test_plan,
        "validation-dl-baseline-test-id-placeholder": config.validation.dl_baseline_test_id,
        "validation-dl-iterations-placeholder": str(config.validation.dl_iterations),
    }