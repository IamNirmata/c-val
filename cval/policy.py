"""Submission policy gates for c-val mutating operations.

This module is intentionally small and explicit: it guards the only path that
can create Kubernetes validation jobs. Dry-run planning does not need these
checks, but real submission always does.
"""

from __future__ import annotations

from dataclasses import dataclass


class PolicyViolation(ValueError):
    """Raised when an operator or agent attempts a disallowed submit action."""

    pass


@dataclass(frozen=True)
class ExecutionPolicy:
    """Runtime limits for approval-gated job submission."""

    namespace_allowlist: tuple[str, ...] = ("gcr-admin",)
    max_batch_size: int = 5
    confirmation_phrase: str = "submit"


def validate_submit_request(
    namespace: str,
    planned_jobs_count: int,
    policy: ExecutionPolicy,
    submit: bool,
    confirmation: str | None,
) -> None:
    """Validate namespace, batch size, and confirmation for real submission."""

    # Namespace scoping prevents accidental submission into arbitrary clusters or tenants.
    if namespace not in policy.namespace_allowlist:
        allowed = ", ".join(policy.namespace_allowlist)
        raise PolicyViolation(f"Namespace {namespace!r} is not allowed. Allowed: {allowed}")

    # Batch-size limits cap the blast radius of validation jobs that reserve full GPU nodes.
    if planned_jobs_count > policy.max_batch_size:
        raise PolicyViolation(
            f"Planned job count {planned_jobs_count} exceeds max batch size "
            f"{policy.max_batch_size}"
        )

    # The confirmation phrase makes real create operations hard to trigger accidentally.
    if submit and confirmation != policy.confirmation_phrase:
        raise PolicyViolation(
            f"Real submission requires --confirm {policy.confirmation_phrase!r}"
        )