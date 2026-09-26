"""Compatible imports for registry-selected raw export operations."""

from cval.validation.operational_targets import resolve_operational_target
from cval.validation.plugins import export_result_rows

__all__ = ["export_result_rows", "resolve_operational_target"]
