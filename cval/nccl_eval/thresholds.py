"""Pure percentile, threshold, classification, and severity logic."""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from enum import Enum
from statistics import fmean
from typing import Iterable, Sequence


class MetricName(str, Enum):
    BUS_BW = "BUS_BW"
    LATENCY = "LATENCY"


@dataclass(frozen=True)
class ThresholdRange:
    metric_name: MetricName
    class_id: int
    lower_bound: float
    upper_bound: float | None
    unit: str

    def __post_init__(self) -> None:
        if not isinstance(self.metric_name, MetricName):
            raise TypeError("metric_name must be a MetricName")
        if self.class_id not in {1, 2, 3, 4, 5}:
            raise ValueError("class_id must be between 1 and 5")
        _finite_nonnegative(self.lower_bound, "lower_bound")
        if self.upper_bound is not None:
            _finite_nonnegative(self.upper_bound, "upper_bound")
            if self.lower_bound >= self.upper_bound:
                raise ValueError("threshold lower_bound must be less than upper_bound")
        if not isinstance(self.unit, str) or not self.unit.strip():
            raise ValueError("threshold unit must be non-empty")


@dataclass(frozen=True)
class DistributionSummary:
    count: int
    mean: float
    p05: float
    p50: float
    p95: float


@dataclass(frozen=True)
class DerivedThresholds:
    metric_name: MetricName
    derivation_method_version: str
    summary: DistributionSummary
    ranges: tuple[ThresholdRange, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.derivation_method_version, str) or not self.derivation_method_version:
            raise ValueError("derivation_method_version must be non-empty")
        validate_ranges(self.ranges, metric_name=self.metric_name)


def percentile(samples: Sequence[float] | Iterable[float], quantile: float) -> float:
    """Return a deterministic linearly interpolated percentile.

    The rank is ``(n - 1) * quantile``.  This works for one-element and small
    distributions without selecting an implementation-dependent convention.
    """

    values = _samples(samples)
    if isinstance(quantile, bool) or not isinstance(quantile, int | float):
        raise TypeError("quantile must be numeric")
    q = float(quantile)
    if not math.isfinite(q) or not 0.0 <= q <= 1.0:
        raise ValueError("quantile must be between 0 and 1")
    rank = (len(values) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return values[lower]
    fraction = rank - lower
    return values[lower] + (values[upper] - values[lower]) * fraction


def summarize(samples: Sequence[float] | Iterable[float]) -> DistributionSummary:
    values = _samples(samples)
    return DistributionSummary(
        count=len(values),
        mean=fmean(values),
        p05=percentile(values, 0.05),
        p50=percentile(values, 0.50),
        p95=percentile(values, 0.95),
    )


def derive_thresholds(
    metric_name: MetricName,
    samples: Sequence[float] | Iterable[float],
    *,
    derivation_method_version: str,
) -> DerivedThresholds:
    """Create five contiguous median-centered ranges for one performance metric."""

    if not isinstance(metric_name, MetricName):
        raise TypeError("metric_name must be a MetricName")
    values = _performance_samples(samples)
    summary = summarize(values)
    if metric_name is MetricName.BUS_BW:
        boundaries = (
            0.70 * summary.p50,
            0.85 * summary.p50,
            max(
                math.nextafter(0.85 * summary.p50, math.inf),
                min(summary.p05, 0.95 * summary.p50),
            ),
            max(summary.p95, 1.05 * summary.p50),
        )
    else:
        within_lower = min(summary.p05, 0.95 * summary.p50)
        within_upper = max(summary.p95, 1.05 * summary.p50)
        underperforming_upper = max(
            1.15 * summary.p50,
            math.nextafter(within_upper, math.inf),
        )
        boundaries = (
            within_lower,
            within_upper,
            underperforming_upper,
            max(
                1.30 * summary.p50,
                math.nextafter(underperforming_upper, math.inf),
            ),
        )
    if not boundaries[0] > 0.0 or not all(
        left < right for left, right in zip(boundaries, boundaries[1:])
    ):
        raise ValueError("could not derive strict median-centered threshold boundaries")
    class_order = (
        (5, 4, 3, 2, 1)
        if metric_name is MetricName.BUS_BW
        else (1, 2, 3, 4, 5)
    )
    unit = "GB/s" if metric_name is MetricName.BUS_BW else "us"
    lower_bounds = (0.0, *boundaries)
    upper_bounds = (*boundaries, None)
    ranges = tuple(
        ThresholdRange(metric_name, class_id, lower, upper, unit)
        for class_id, lower, upper in zip(class_order, lower_bounds, upper_bounds)
    )
    validate_ranges(ranges, metric_name=metric_name)
    return DerivedThresholds(
        metric_name=metric_name,
        derivation_method_version=derivation_method_version,
        summary=summary,
        ranges=ranges,
    )


def validate_ranges(
    ranges: Sequence[ThresholdRange], *, metric_name: MetricName | None = None
) -> None:
    """Reject gaps, overlaps, duplicate classes, and incomplete coverage."""

    if len(ranges) != 5:
        raise ValueError("exactly five threshold ranges are required")
    if not all(isinstance(item, ThresholdRange) for item in ranges):
        raise TypeError("ranges must contain ThresholdRange values")
    metrics = {item.metric_name for item in ranges}
    if len(metrics) != 1 or (metric_name is not None and metrics != {metric_name}):
        raise ValueError("all threshold ranges must use the same metric")
    if {item.class_id for item in ranges} != {1, 2, 3, 4, 5}:
        raise ValueError("threshold ranges must contain classes 1 through 5 exactly once")
    ordered = sorted(ranges, key=lambda item: item.lower_bound)
    if ordered[0].lower_bound != 0.0:
        raise ValueError("threshold ranges must start at zero")
    for current, following in zip(ordered, ordered[1:]):
        if current.upper_bound is None or current.upper_bound != following.lower_bound:
            raise ValueError("threshold ranges must be contiguous and non-overlapping")
    if ordered[-1].upper_bound is not None:
        raise ValueError("threshold ranges must end at positive infinity")
    if any(item.upper_bound is None for item in ordered[:-1]):
        raise ValueError("only the final threshold range may have no upper bound")


def classify(value: float, ranges: Sequence[ThresholdRange]) -> int:
    """Classify an exact value using ``lower <= value < upper``."""

    _finite_nonnegative(value, "value")
    validate_ranges(ranges)
    matches = [
        item.class_id
        for item in ranges
        if value >= item.lower_bound
        and (item.upper_bound is None or value < item.upper_bound)
    ]
    if len(matches) != 1:
        raise ValueError("value must match exactly one threshold range")
    return matches[0]


def empirical_severity(
    samples: Sequence[float] | Iterable[float],
    value: float,
    *,
    higher_is_better: bool,
) -> float:
    """Return tie-stable empirical severity from 0 healthy to 100 unhealthy."""

    values = _samples(samples)
    _finite_nonnegative(value, "value")
    median = percentile(values, 0.50)
    if value == median:
        return 50.0
    if len(values) == 1:
        healthy_percentile = 50.0 if value == values[0] else (0.0 if value < values[0] else 100.0)
    elif value < values[0]:
        healthy_percentile = 0.0
    elif value > values[-1]:
        healthy_percentile = 100.0
    else:
        left = bisect.bisect_left(values, value)
        right = bisect.bisect_right(values, value)
        position = (left + right - 1) / 2.0
        healthy_percentile = 100.0 * position / (len(values) - 1)
    severity = 100.0 - healthy_percentile if higher_is_better else healthy_percentile
    return _clamp(severity)


def piecewise_severity(
    value: float,
    summary: DistributionSummary,
    *,
    higher_is_better: bool,
) -> float:
    """Approximate empirical severity from persisted p05/p50/p95 anchors."""

    _finite_nonnegative(value, "value")
    for name in ("p05", "p50", "p95"):
        _finite_nonnegative(getattr(summary, name), name)
    if not summary.p05 <= summary.p50 <= summary.p95:
        raise ValueError("distribution percentiles must be ordered")
    if value == summary.p50:
        return 50.0
    if summary.p05 == summary.p95:
        if value == summary.p50:
            ascending = 50.0
        else:
            ascending = 0.0 if value < summary.p50 else 100.0
    elif value <= summary.p05:
        ascending = 0.0
    elif value < summary.p50:
        width = summary.p50 - summary.p05
        ascending = 50.0 if width == 0.0 else 50.0 * (value - summary.p05) / width
    elif value < summary.p95:
        width = summary.p95 - summary.p50
        ascending = 50.0 if width == 0.0 else 50.0 + 50.0 * (value - summary.p50) / width
    else:
        ascending = 100.0
    return _clamp(100.0 - ascending if higher_is_better else ascending)


def overall_health(
    bus_bw_class: int,
    latency_class: int,
    bus_bw_severity: float,
    latency_severity: float,
) -> tuple[int, float]:
    """Return the worse class and worse severity; never average them."""

    for class_id in (bus_bw_class, latency_class):
        if class_id not in {1, 2, 3, 4, 5}:
            raise ValueError("health classes must be between 1 and 5")
    for value in (bus_bw_severity, latency_severity):
        if not math.isfinite(value) or not 0.0 <= value <= 100.0:
            raise ValueError("severity must be between 0 and 100")
    return max(bus_bw_class, latency_class), max(bus_bw_severity, latency_severity)


def _samples(samples: Sequence[float] | Iterable[float]) -> list[float]:
    values = list(samples)
    if not values:
        raise ValueError("at least one sample is required")
    for value in values:
        _finite_nonnegative(value, "sample")
    return sorted(float(value) for value in values)


def _performance_samples(
    samples: Sequence[float] | Iterable[float],
) -> list[float]:
    values = _samples(samples)
    if values[0] <= 0.0:
        raise ValueError("performance samples must be strictly positive")
    return values


def _finite_nonnegative(value: float, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{field_name} must be numeric")
    if not math.isfinite(float(value)) or float(value) < 0.0:
        raise ValueError(f"{field_name} must be finite and non-negative")


def _clamp(value: float) -> float:
    return min(100.0, max(0.0, float(value)))
