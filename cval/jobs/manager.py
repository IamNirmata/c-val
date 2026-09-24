"""Approval-gated validation job submission.

This module is the only package layer that can create Kubernetes resources. It
calls `ExecutionPolicy` before every real `kubectl create` operation.
"""

from __future__ import annotations

from dataclasses import dataclass

from cval.config import CvalConfig, is_exact_commit, load_config
from cval.k8s.client import KubectlClient
from cval.models import WorkflowPlan
from cval.policy import ExecutionPolicy, PolicyViolation, validate_submit_request


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


def submit_workflow_plan(
    plan: WorkflowPlan,
    namespace: str | None = None,
    client: KubectlClient | None = None,
    policy: ExecutionPolicy | None = None,
    submit: bool = False,
    confirmation: str | None = None,
    config: CvalConfig | None = None,
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
    validate_submit_request(
        namespace=resolved_namespace,
        planned_jobs_count=len(plan.planned_jobs),
        policy=active_policy,
        submit=submit,
        confirmation=confirmation,
    )

    kubectl = client or KubectlClient(config=active_config)
    records: list[JobSubmissionRecord] = []
    for planned in plan.planned_jobs:
        # Manifest is sent through stdin so no temporary YAML file is needed.
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