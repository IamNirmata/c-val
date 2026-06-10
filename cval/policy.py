from __future__ import annotations

from dataclasses import dataclass


class PolicyViolation(ValueError):
    pass


@dataclass(frozen=True)
class ExecutionPolicy:
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
    if namespace not in policy.namespace_allowlist:
        allowed = ", ".join(policy.namespace_allowlist)
        raise PolicyViolation(f"Namespace {namespace!r} is not allowed. Allowed: {allowed}")
    if planned_jobs_count > policy.max_batch_size:
        raise PolicyViolation(
            f"Planned job count {planned_jobs_count} exceeds max batch size "
            f"{policy.max_batch_size}"
        )
    if submit and confirmation != policy.confirmation_phrase:
        raise PolicyViolation(
            f"Real submission requires --confirm {policy.confirmation_phrase!r}"
        )