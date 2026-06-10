from __future__ import annotations

import re
import time
from pathlib import Path

from cval.models import RenderedJob


NODE_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def default_template_path() -> Path:
    return Path(__file__).resolve().parents[2] / "ymls" / "specific-node-job.yml"


def make_job_name(node_name: str, timestamp: int, job_prefix: str = "hari-gcr-ceval") -> str:
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
    rendered_timestamp = int(time.time()) if timestamp is None else int(timestamp)
    job_name = make_job_name(node_name, rendered_timestamp, job_prefix=job_prefix)

    required_placeholders = [
        "nodename-placeholder",
        "time-placeholder",
        "jobname-placeholder",
    ]
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
    return render_validation_job(
        template_path.read_text(encoding="utf-8"),
        node_name=node_name,
        timestamp=timestamp,
        job_prefix=job_prefix,
        git_repo=git_repo,
        git_ref=git_ref,
    )


def validate_kubernetes_name(value: str, field_name: str) -> None:
    if not NODE_NAME_PATTERN.match(value):
        raise ValueError(f"{field_name} must be a lowercase DNS label-compatible name: {value!r}")