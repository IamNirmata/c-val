"""Dry-run and approval-gated validation job submission.

This module is the only package layer that can create Kubernetes resources. It
is dry-run by default and calls `ExecutionPolicy` before any real `kubectl
create` operation.
"""

from __future__ import annotations

from dataclasses import dataclass

from cval.k8s.client import KubectlClient
from cval.models import WorkflowPlan
from cval.policy import ExecutionPolicy, validate_submit_request


@dataclass(frozen=True)
class JobSubmissionRecord:
    """One dry-run or submitted job action in a submission result."""

    node: str
    job_name: str
    action: str
    submitted: bool
    stdout: str = ""


@dataclass(frozen=True)
class JobSubmissionResult:
    """Submission response containing all planned/submitted job records."""

    namespace: str
    dry_run: bool
    records: list[JobSubmissionRecord]

    @property
    def submitted_count(self) -> int:
        """Count records that actually created Kubernetes resources."""

        return sum(1 for record in self.records if record.submitted)


def submit_workflow_plan(
    plan: WorkflowPlan,
    namespace: str = "gcr-admin",
    client: KubectlClient | None = None,
    policy: ExecutionPolicy | None = None,
    submit: bool = False,
    confirmation: str | None = None,
) -> JobSubmissionResult:
    """Preview or submit a workflow plan after policy validation."""

    active_policy = policy or ExecutionPolicy(namespace_allowlist=(namespace,))
    # Run policy checks before dry-run output too, so bad plans are visible early.
    validate_submit_request(
        namespace=namespace,
        planned_jobs_count=len(plan.planned_jobs),
        policy=active_policy,
        submit=submit,
        confirmation=confirmation,
    )

    if not submit:
        # Dry-run is the normal path; it returns intended actions without kubectl create.
        return JobSubmissionResult(
            namespace=namespace,
            dry_run=True,
            records=[
                JobSubmissionRecord(
                    node=planned.candidate.node,
                    job_name=planned.rendered_job.job_name,
                    action="dry-run",
                    submitted=False,
                )
                for planned in plan.planned_jobs
            ],
        )

    kubectl = client or KubectlClient()
    records: list[JobSubmissionRecord] = []
    for planned in plan.planned_jobs:
        # Manifest is sent through stdin so no temporary YAML file is needed.
        result = kubectl.run(
            ["create", "-n", namespace, "-f", "-"],
            input_text=planned.rendered_job.yaml_text,
        )
        records.append(
            JobSubmissionRecord(
                node=planned.candidate.node,
                job_name=planned.rendered_job.job_name,
                action="submitted",
                submitted=True,
                stdout=result.stdout.strip(),
            )
        )

    return JobSubmissionResult(namespace=namespace, dry_run=False, records=records)


def submission_result_to_dict(result: JobSubmissionResult) -> dict[str, object]:
    """Convert submission result to JSON-serializable CLI output."""

    return {
        "namespace": result.namespace,
        "dry_run": result.dry_run,
        "submitted_count": result.submitted_count,
        "jobs": [
            {
                "node": record.node,
                "job_name": record.job_name,
                "action": record.action,
                "submitted": record.submitted,
                "stdout": record.stdout,
            }
            for record in result.records
        ],
    }