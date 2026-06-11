"""Volcano validation job rendering.

This module turns the checked-in YAML template into a concrete one-node
validation job. It does not submit anything; it only replaces placeholders and
returns the rendered manifest for inspection or policy-gated submission.
"""

from __future__ import annotations

import re
import time
from pathlib import Path

from cval.models import RenderedJob


NODE_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def default_template_path() -> Path:
    """Return the repository default Volcano job template path."""

    return Path(__file__).resolve().parents[2] / "ymls" / "specific-node-job.yml"


def make_job_name(node_name: str, timestamp: int, job_prefix: str = "hari-gcr-ceval") -> str:
    """Build a deterministic Kubernetes-compatible validation job name."""

    validate_kubernetes_name(node_name, "node_name")
    validate_kubernetes_name(job_prefix, "job_prefix")
    return f"{job_prefix}-{node_name}-{timestamp}"


def render_validation_job(
    template_text: str,
    node_name: str,
    timestamp: int | None = None,
    job_prefix: str = "hari-gcr-ceval",
    git_repo: str = "https://github.com/IamNirmata/c-val.git",
    git_ref: str = "main",
) -> RenderedJob:
    """Render one validation job manifest for one target node."""

    rendered_timestamp = int(time.time()) if timestamp is None else int(timestamp)
    job_name = make_job_name(node_name, rendered_timestamp, job_prefix=job_prefix)

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
    yaml_text = yaml_text.replace("git-repo-placeholder", git_repo)
    yaml_text = yaml_text.replace("git-ref-placeholder", git_ref)

    # Refuse partially rendered manifests; a placeholder in submitted YAML is dangerous.
    remaining = [placeholder for placeholder in required_placeholders if placeholder in yaml_text]
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
    job_prefix: str = "hari-gcr-ceval",
    git_repo: str = "https://github.com/IamNirmata/c-val.git",
    git_ref: str = "main",
) -> RenderedJob:
    """Read a template file and render a validation job from it."""

    return render_validation_job(
        template_path.read_text(encoding="utf-8"),
        node_name=node_name,
        timestamp=timestamp,
        job_prefix=job_prefix,
        git_repo=git_repo,
        git_ref=git_ref,
    )


def validate_kubernetes_name(value: str, field_name: str) -> None:
    """Validate the subset of DNS-label syntax used by node/job names."""

    if not NODE_NAME_PATTERN.match(value):
        raise ValueError(f"{field_name} must be a lowercase DNS label-compatible name: {value!r}")