"""Immutable value objects for the versioned c-val health-class engine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum, StrEnum
from pathlib import Path
from typing import Any


class HealthClassCode(IntEnum):
    """Stable health ordering; larger values are more severe."""

    EXCELLENT = 0
    NOMINAL = 1
    UNDERPERFORMING = 2
    VERY_BAD = 3
    TERRIBLE = 4
    DNR = 5

    @property
    def class_name(self) -> str:
        return HEALTH_CLASS_NAMES[self]


HEALTH_CLASS_NAMES = {
    HealthClassCode.EXCELLENT: "Excellent",
    HealthClassCode.NOMINAL: "Nominal",
    HealthClassCode.UNDERPERFORMING: "Underperforming",
    HealthClassCode.VERY_BAD: "Very Bad",
    HealthClassCode.TERRIBLE: "Terrible",
    HealthClassCode.DNR: "DNR",
}

HEALTH_CLASS_DEFINITIONS = (
    (0, "Excellent", "Materially better than the active nominal baseline."),
    (1, "Nominal", "Inside the active nominal acceptance band."),
    (2, "Underperforming", "Outside nominal by the first degradation band."),
    (3, "Very Bad", "Outside nominal by the second degradation band."),
    (4, "Terrible", "Outside nominal by more than three baseline deltas."),
    (5, "DNR", "No compatible, complete, evaluable health observation."),
)


class DnrReason(StrEnum):
    RAW_FAILED = "raw_failed"
    RAW_INCOMPLETE = "raw_incomplete"
    MISSING_COMBINATION = "missing_combination"
    NO_ACTIVE_BASELINE = "no_active_baseline"
    NO_OBSERVATIONS = "no_observations"
    INCOMPLETE_METRIC_COVERAGE = "incomplete_metric_coverage"
    INCOMPATIBLE_ADAPTER_VERSION = "incompatible_adapter_version"


class BaselineLifecycle(StrEnum):
    CANDIDATE = "candidate"
    ACTIVE = "active"
    SUPERSEDED = "superseded"


@dataclass(frozen=True)
class EnvironmentCombination:
    """Canonical comparable-environment identity."""

    key: str
    factors_json: str


@dataclass(frozen=True)
class MetricSpec:
    """One configured metric source rule supplied by a health adapter."""

    name: str
    source: str
    direction: str
    tolerance_pct: float
    units: str = ""
    weight: float = 1.0


@dataclass(frozen=True)
class MetricObservation:
    """One finite adapter observation bound to an ingested result."""

    result_id: int
    run_id: str
    completed_timestamp: int
    source: str
    metric_name: str
    sample_key: str
    value: float


@dataclass(frozen=True)
class SourceResult:
    """One exact raw result used by a candidate."""

    result_id: int
    run_id: str
    completed_timestamp: int
    result_digest: str
    raw_result_digest: str
    test_config_digest: str
    combination_key: str
    adapter_schema_version: int
    receipt_evidence_digest: str


@dataclass(frozen=True)
class SourceSnapshot:
    """Exact reproducible candidate source membership."""

    results: tuple[SourceResult, ...]

    @property
    def result_ids(self) -> tuple[int, ...]:
        return tuple(result.result_id for result in self.results)

    @property
    def first_timestamp(self) -> int | None:
        return min((result.completed_timestamp for result in self.results), default=None)

    @property
    def last_timestamp(self) -> int | None:
        return max((result.completed_timestamp for result in self.results), default=None)

    @property
    def max_result_id(self) -> int | None:
        return max(self.result_ids, default=None)


@dataclass(frozen=True)
class ResultSampleCoverage:
    """Exact sample-key membership for one metric in one source result."""

    result_id: int
    sample_keys: tuple[str, ...]


@dataclass(frozen=True)
class SourceCoverage:
    """Exact per-result sample membership observed for one expanded metric."""

    source: str
    metric_name: str
    results: tuple[ResultSampleCoverage, ...]

    @property
    def result_ids(self) -> tuple[int, ...]:
        return tuple(result.result_id for result in self.results)

    @property
    def expected_sample_keys(self) -> tuple[str, ...] | None:
        if not self.results:
            return None
        expected = self.results[0].sample_keys
        return (
            expected
            if all(result.sample_keys == expected for result in self.results)
            else None
        )


@dataclass(frozen=True)
class BuildDecision:
    eligible: bool
    qualifying_count: int
    new_result_count: int
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ThresholdBand:
    """One normalized interval for one threshold-bearing class."""

    class_code: HealthClassCode
    band_index: int
    lower_bound: float | None
    upper_bound: float | None
    lower_inclusive: bool
    upper_inclusive: bool


@dataclass(frozen=True)
class MetricBaseline:
    """Robust statistics and normalized class bands for one expanded metric."""

    spec_name: str
    source: str
    metric_name: str
    direction: str
    units: str
    weight: float
    tolerance_pct: float
    center: float
    mad: float
    mad_sigma: float
    delta: float
    p05: float
    p95: float
    sample_count: int
    excluded_count: int
    statistics_json: str
    thresholds: tuple[ThresholdBand, ...]


@dataclass(frozen=True)
class QualityGate:
    name: str
    passed: bool
    message: str


@dataclass(frozen=True)
class QualityReport:
    gates: tuple[QualityGate, ...]

    @property
    def activation_ready(self) -> bool:
        return bool(self.gates) and all(gate.passed for gate in self.gates)


@dataclass(frozen=True)
class HealthCandidate:
    """Content-addressed immutable candidate; lifecycle is storage-owned."""

    baseline_id: str
    payload_digest: str
    test_id: str
    combination: EnvironmentCombination
    test_config_digest: str
    health_policy_version: str
    adapter_schema_version: int
    evaluator_version: str
    method: str
    robust_z_threshold: float
    observation_digest: str
    source_snapshot: SourceSnapshot
    observations: tuple[MetricObservation, ...]
    source_coverage: tuple[SourceCoverage, ...]
    metrics: tuple[MetricBaseline, ...]
    parent_baseline_id: str | None
    created_at: int
    excluded_result_count: int = 0


@dataclass(frozen=True)
class MetricVerdict:
    source: str
    metric_name: str
    value: float
    class_code: HealthClassCode
    class_name: str
    pct_diff: float
    severity_pct: float


@dataclass(frozen=True)
class HealthVerdict:
    test_id: str
    combination_key: str
    baseline_id: str | None
    class_code: HealthClassCode
    class_name: str
    dnr_reason: DnrReason | None
    metrics: tuple[MetricVerdict, ...]
    details_json: str


@dataclass(frozen=True)
class HealthContext:
    """Immutable read context for repository health adapter hooks."""

    definition: Any
    result_db_path: Path
    combination: EnvironmentCombination | None
    source_snapshot: SourceSnapshot
    parent_baseline_id: str | None = None
    evaluator_version: str = "cval.health.v1"
    robust_z_threshold: float | None = None
    raw_status: str = "pass"
    created_at: int | None = None


@dataclass(frozen=True)
class StoredHealthBaseline:
    candidate: HealthCandidate
    lifecycle: BaselineLifecycle
    quality: QualityReport
    updated_at: int
    activated_at: int | None
    superseded_at: int | None


@dataclass(frozen=True)
class HealthBuildState:
    test_id: str
    combination_key: str
    last_seen_result_id: int | None
    last_candidate_id: str | None
    qualifying_result_count: int
    new_result_count: int
    last_checked_at: int | None
    last_built_at: int | None
    last_error: str
    candidate_source_result_ids: tuple[int, ...] = ()
