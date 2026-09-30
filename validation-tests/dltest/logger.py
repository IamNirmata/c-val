"""DL output uses one reserved job.log through the shared safe writer."""

from cval.validation.execution import append_text as write_log


def format_output(text: str, *, label: str, stream: str) -> str:
	"""Retain rank output verbatim after the DL stage and stream prefix."""

	return f"[{label}:{stream}] {text}"