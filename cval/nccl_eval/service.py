"""One-shot service operations and secret-safe structured receipts."""

from __future__ import annotations

import time
import signal
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from cval.nccl_eval.config import NcclEvaluationConfig
from cval.nccl_eval.models import IngestionBatch
from cval.nccl_eval.repository import (
    CalibrationDecision,
    NcclEvaluationRepository,
    create_pool,
    validate_worker_id,
)
from cval.nccl_eval.schema import apply_migrations
from cval.nccl_eval.schema import provision_runtime_role


@contextmanager
def open_repository(
    config: NcclEvaluationConfig,
) -> Iterator[NcclEvaluationRepository]:
    """Open and always close the explicitly initialized bounded pool."""

    repository = NcclEvaluationRepository(create_pool(config), config)
    try:
        yield repository
    finally:
        repository.close()


def apply_schema(config: NcclEvaluationConfig) -> dict[str, object]:
    pool = create_pool(config)
    try:
        return apply_migrations(pool)
    finally:
        pool.close()


def grant_runtime(
    config: NcclEvaluationConfig, *, username: str, password: str
) -> dict[str, object]:
    pool = create_pool(config)
    try:
        return provision_runtime_role(pool, username=username, password=password)
    finally:
        pool.close()


def ingest(config: NcclEvaluationConfig, batch: IngestionBatch) -> dict[str, object]:
    started = time.monotonic()
    with open_repository(config) as repository:
        receipt = repository.ingest_batch(batch)
    return _event("nccl_results_ingested", receipt, started)


def build_baselines(config: NcclEvaluationConfig) -> dict[str, object]:
    started = time.monotonic()
    with open_repository(config) as repository:
        receipt = repository.build_baselines()
    return _event("nccl_baseline_build_completed", receipt, started)


def baseline_report(config: NcclEvaluationConfig) -> dict[str, object]:
    with open_repository(config) as repository:
        return repository.baseline_eligibility_report()


def calibration_plan(
    config: NcclEvaluationConfig, decisions: tuple[CalibrationDecision, ...]
) -> dict[str, object]:
    with open_repository(config) as repository:
        return repository.calibration_plan(decisions)


def apply_calibration(
    config: NcclEvaluationConfig, decisions: tuple[CalibrationDecision, ...]
) -> dict[str, object]:
    started = time.monotonic()
    with open_repository(config) as repository:
        receipt = repository.apply_calibration(decisions)
    return _event("nccl_calibration_decisions_applied", receipt, started)


def calibration_report(
    config: NcclEvaluationConfig, *, limit: int = 100
) -> dict[str, object]:
    with open_repository(config) as repository:
        return repository.calibration_report(limit=limit)


def queue_report(config: NcclEvaluationConfig) -> dict[str, object]:
    with open_repository(config) as repository:
        return repository.queue_report()


def evaluate_once(
    config: NcclEvaluationConfig,
    *,
    worker_id: str,
    batch_size: int | None = None,
) -> dict[str, object]:
    """Claim a short batch, release locks, then atomically complete each job."""

    worker_id = validate_worker_id(worker_id)
    started = time.monotonic()
    with open_repository(config) as repository:
        claims = repository.claim_jobs(worker_id, batch_size=batch_size)
        jobs: list[dict[str, object]] = []
        for claim in claims:
            try:
                result = repository.evaluate_claimed(claim)
            except Exception as exc:  # noqa: BLE001 - durable retry boundary
                result = repository.schedule_retry(claim, exc)
            jobs.append(result | {"attempt_count": claim.attempt_count})
    return _event(
        "nccl_evaluation_batch_completed",
        {
            "worker_id": worker_id,
            "claimed_count": len(claims),
            "completed_count": sum(item.get("job_status") == "COMPLETED" for item in jobs),
            "waiting_count": sum(
                item.get("job_status") == "WAITING_FOR_BASELINE" for item in jobs
            ),
            "failed_count": sum(item.get("job_status") == "FAILED" for item in jobs),
            "retry_count": sum(item.get("job_status") == "RETRY" for item in jobs),
            "jobs": jobs,
        },
        started,
    )


def poll(
    config: NcclEvaluationConfig,
    *,
    worker_id: str,
    max_cycles: int,
) -> list[dict[str, object]]:
    """Run a deliberately bounded polling loop; deployment wiring is future work."""

    if isinstance(max_cycles, bool) or not isinstance(max_cycles, int) or not 1 <= max_cycles <= 1000:
        raise ValueError("max_cycles must be between 1 and 1000")
    receipts: list[dict[str, object]] = []
    for cycle in range(max_cycles):
        receipt = evaluate_once(config, worker_id=worker_id)
        receipts.append(receipt)
        if cycle + 1 < max_cycles:
            time.sleep(config.evaluator_poll_interval_seconds)
    return receipts


def worker(
    config: NcclEvaluationConfig,
    *,
    worker_id: str,
    recover_every_cycles: int = 12,
    stop_event: threading.Event | None = None,
    install_signal_handlers: bool = True,
) -> dict[str, object]:
    """Run one-shot batches until signalled, never claiming after stop is requested."""

    worker_id = validate_worker_id(worker_id)
    if (
        isinstance(recover_every_cycles, bool)
        or not isinstance(recover_every_cycles, int)
        or not 1 <= recover_every_cycles <= 1000
    ):
        raise ValueError("recover_every_cycles must be between 1 and 1000")
    stopped = stop_event or threading.Event()
    previous: dict[signal.Signals, object] = {}

    def request_stop(_signum: int, _frame: object) -> None:
        stopped.set()

    if install_signal_handlers:
        for watched in (signal.SIGINT, signal.SIGTERM):
            previous[watched] = signal.getsignal(watched)
            signal.signal(watched, request_stop)
    started = time.monotonic()
    cycles = 0
    claimed = 0
    completed = 0
    recovered = 0
    try:
        while not stopped.is_set():
            receipt = evaluate_once(config, worker_id=worker_id)
            cycles += 1
            claimed += int(receipt["claimed_count"])
            completed += int(receipt["completed_count"])
            if stopped.is_set():
                break
            if cycles % recover_every_cycles == 0:
                recovery = recover(config)
                recovered += int(recovery["recovered_count"])
            if stopped.wait(config.evaluator_poll_interval_seconds):
                break
    finally:
        for watched, handler in previous.items():
            signal.signal(watched, handler)
    return _event(
        "nccl_evaluator_worker_stopped",
        {
            "worker_id": worker_id,
            "stop_requested": stopped.is_set(),
            "cycles_completed": cycles,
            "claimed_count": claimed,
            "completed_count": completed,
            "recovered_count": recovered,
        },
        started,
    )


def resident(
    config: NcclEvaluationConfig,
    *,
    worker_id: str,
    outbox_root: Path,
    ingest_limit: int = 5000,
    recover_every_cycles: int = 12,
    stop_event: threading.Event | None = None,
    install_signal_handlers: bool = True,
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    """Run every recurring NCCL DB task in one signal-aware resident loop."""

    worker_id = validate_worker_id(worker_id)
    outbox_root = Path(outbox_root)
    if not outbox_root.is_absolute():
        raise ValueError("outbox_root must be absolute")
    if isinstance(ingest_limit, bool) or not isinstance(ingest_limit, int) or not 1 <= ingest_limit <= 5000:
        raise ValueError("ingest_limit must be between 1 and 5000")
    if (
        isinstance(recover_every_cycles, bool)
        or not isinstance(recover_every_cycles, int)
        or not 1 <= recover_every_cycles <= 1000
    ):
        raise ValueError("recover_every_cycles must be between 1 and 1000")
    stopped = stop_event or threading.Event()
    previous: dict[signal.Signals, object] = {}

    def request_stop(_signum: int, _frame: object) -> None:
        stopped.set()

    if install_signal_handlers:
        for watched in (signal.SIGINT, signal.SIGTERM):
            previous[watched] = signal.getsignal(watched)
            signal.signal(watched, request_stop)

    def emit(receipt: dict[str, object]) -> None:
        if event_sink is not None:
            event_sink(receipt)

    from cval.nccl_eval.outbox import ingest_outbox_progression, scan_outbox

    started = time.monotonic()
    cycles = 0
    ingested = 0
    rejected = 0
    baselines_built = 0
    recovered = 0
    claimed = 0
    completed = 0
    next_baseline_at = 0.0
    try:
        while not stopped.is_set():
            scan = scan_outbox(outbox_root, limit=ingest_limit)
            if scan.root_exists and scan.discovered_json_count > 0:
                ingestion = ingest_outbox_progression(
                    config,
                    outbox_root,
                    limit=ingest_limit,
                )
                ingested += int(ingestion["ingested_count"])
                rejected += int(ingestion["rejected_count"])
                emit({"event": "nccl_outbox_cycle_completed", **ingestion})
            else:
                emit(
                    {
                        "event": "nccl_outbox_cycle_skipped",
                        "outbox_root": str(outbox_root),
                        "reason": (
                            "no-committed-markers"
                            if scan.root_exists
                            else "outbox-root-absent"
                        ),
                    }
                )

            now = time.monotonic()
            if now >= next_baseline_at:
                baseline = build_baselines(config)
                baselines_built += int(baseline["built_count"])
                emit(baseline)
                next_baseline_at = now + config.baseline_builder_interval_seconds

            evaluation = evaluate_once(config, worker_id=worker_id)
            cycles += 1
            claimed += int(evaluation["claimed_count"])
            completed += int(evaluation["completed_count"])
            emit(evaluation)

            if stopped.is_set():
                break
            if cycles % recover_every_cycles == 0:
                recovery = recover(config)
                recovered += int(recovery["recovered_count"])
                emit(recovery)
            if stopped.wait(config.evaluator_poll_interval_seconds):
                break
    finally:
        for watched, handler in previous.items():
            signal.signal(watched, handler)
    return _event(
        "nccl_resident_evaluator_stopped",
        {
            "worker_id": worker_id,
            "outbox_root": str(outbox_root),
            "stop_requested": stopped.is_set(),
            "cycles_completed": cycles,
            "ingested_count": ingested,
            "rejected_count": rejected,
            "baselines_built_count": baselines_built,
            "recovered_count": recovered,
            "claimed_count": claimed,
            "completed_count": completed,
        },
        started,
    )


def stale_report(config: NcclEvaluationConfig) -> dict[str, object]:
    with open_repository(config) as repository:
        return repository.stale_claim_report()


def recover(config: NcclEvaluationConfig) -> dict[str, object]:
    started = time.monotonic()
    with open_repository(config) as repository:
        receipt = repository.recover_stale_claims()
    return _event("nccl_stale_claim_recovery_completed", receipt, started)


def status(config: NcclEvaluationConfig, *, latest_limit: int = 20) -> dict[str, object]:
    with open_repository(config) as repository:
        return repository.status(latest_limit=latest_limit)


def _event(
    event: str, payload: dict[str, object], started: float
) -> dict[str, object]:
    """Add bounded operational metadata; never accept or emit a database URL."""

    if "database_url" in payload or "DATABASE_URL" in payload:
        raise ValueError("structured NCCL events must not contain database credentials")
    return {
        "event": event,
        "duration_seconds": round(max(0.0, time.monotonic() - started), 6),
        **payload,
    }
