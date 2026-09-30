"""Storage output uses one reserved job.log through the shared safe writer."""

from cval.validation.execution import append_text as write_log


def format_output(text: str, *, label: str, stream: str) -> str:
	"""Keep FIO setup and workload streams identifiable in the single test log."""

	return f"[{label}:{stream}] {text}"