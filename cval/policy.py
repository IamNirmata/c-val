"""Submission policy gates for c-val mutating operations.

This module is intentionally small and explicit: it guards the only path that
can create Kubernetes validation jobs. Read-only queue inspection is separate;
real submission always passes these checks.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cval.config import load_config


class PolicyViolation(ValueError):
    """Raised when an operator or agent attempts a disallowed submit action."""

    pass


@dataclass(frozen=True)
class ExecutionPolicy:
    """Runtime limits for approval-gated job submission."""

    namespace_allowlist: tuple[str, ...] = field(
        default_factory=lambda: load_config().policy.namespace_allowlist
    )
    max_batch_size: int = field(default_factory=lambda: load_config().policy.max_batch_size)
    confirmation_phrase: str = field(
        default_factory=lambda: load_config().policy.confirmation_phrase
    )


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