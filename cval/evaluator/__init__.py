"""U11 deployment preparation for the one-shot c-val evaluator service."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "backup_local_evaluator_state",
    "build_shadow_parity_report",
    "run_deployment_preflight",
    "run_evaluator_service",
]

_EXPORTS = {
    "backup_local_evaluator_state": ("cval.evaluator.backup", "backup_local_evaluator_state"),
    "build_shadow_parity_report": ("cval.evaluator.parity", "build_shadow_parity_report"),
    "run_deployment_preflight": ("cval.evaluator.preflight", "run_deployment_preflight"),
    "run_evaluator_service": ("cval.evaluator.service", "run_evaluator_service"),
}


def __getattr__(name: str) -> Any:
    """Load public evaluator APIs only when requested by runtime callers."""

    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    value = getattr(import_module(target[0]), target[1])
    globals()[name] = value
    return value
