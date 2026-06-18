"""Robust statistics for dynamic baseline construction.

Pure standard library (no numpy/scipy) so c-val stays dependency-free.

Estimators here are deliberately *robust* (median, MAD, IQR, percentiles)
instead of mean/standard-deviation. GPU-cluster performance metrics are
skewed and routinely contaminated by a few degraded nodes; the mean has a
breakdown point of 0 (one bad run shifts it) and an inflated standard
deviation widens the acceptance band and hides the next anomaly. The median
tolerates up to 50% contamination, which is what fleet validation needs.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from statistics import median as _median

# MAD -> sigma consistency constant for normally distributed data.
_MAD_TO_SIGMA = 1.4826
# 0.6745 = inverse-normal CDF at 0.75; modified z-score (Iglewicz & Hoaglin).
_MODIFIED_Z_CONST = 0.6745
# sqrt(pi/2); MeanAD -> sigma factor used as the MAD == 0 fallback.
_MEANAD_TO_SIGMA = 1.253314

# Metric directionality controls which side of the band is a failure.
DIRECTION_LOW_BAD = "low_bad"      # higher is better (bandwidth, IOPS, busbw)
DIRECTION_HIGH_BAD = "high_bad"    # lower is better (latency, wall-clock time)
DIRECTION_TWO_SIDED = "two_sided"  # correctness / variance (DL numerical)

VALID_DIRECTIONS = frozenset(
    (DIRECTION_LOW_BAD, DIRECTION_HIGH_BAD, DIRECTION_TWO_SIDED)
)


def median(values: list[float]) -> float:
    """Return the median as a float."""

    return float(_median(values))


def percentile(values: list[float], p: float) -> float:
    """Linear-interpolation percentile (type 7, the numpy default).

    ``p`` is in the closed interval [0, 100].
    """

    if not values:
        raise ValueError("percentile of empty data")
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (p / 100.0) * (len(ordered) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(ordered[int(rank)])
    frac = rank - low
    return float(ordered[low] * (1.0 - frac) + ordered[high] * frac)


def iqr(values: list[float]) -> float:
    """Interquartile range (Q3 - Q1)."""

    return percentile(values, 75) - percentile(values, 25)


def mad(values: list[float], center: float | None = None) -> float:
    """Median absolute deviation about the median."""

    if not values:
        raise ValueError("mad of empty data")
    pivot = median(values) if center is None else center
    return float(_median([abs(x - pivot) for x in values]))


def mean_abs_deviation(values: list[float], center: float | None = None) -> float:
    """Mean absolute deviation about the median (MAD == 0 fallback scale)."""

    if not values:
        raise ValueError("mean_abs_deviation of empty data")
    pivot = median(values) if center is None else center
    return sum(abs(x - pivot) for x in values) / len(values)


def mad_sigma(values: list[float]) -> float:
    """Robust standard-deviation estimate: 1.4826 * MAD."""

    return _MAD_TO_SIGMA * mad(values)


def modified_zscores(values: list[float]) -> list[float]:
    """Robust z-scores; fall back to MeanAD when MAD == 0.

    Returns an all-zero list when every value is identical.
    """

    if not values:
        return []
    center = median(values)
    spread = mad(values, center)
    if spread > 0:
        return [_MODIFIED_Z_CONST * (x - center) / spread for x in values]
    mean_spread = mean_abs_deviation(values, center)
    if mean_spread > 0:
        return [(x - center) / (_MEANAD_TO_SIGMA * mean_spread) for x in values]
    return [0.0 for _ in values]


def tukey_fences(values: list[float], k: float = 1.5) -> tuple[float, float]:
    """Distribution-free outlier fences [Q1 - k*IQR, Q3 + k*IQR].

    ``k = 1.5`` flags outliers; ``k = 3.0`` flags extreme outliers.
    """

    q1 = percentile(values, 25)
    q3 = percentile(values, 75)
    spread = q3 - q1
    return (q1 - k * spread, q3 + k * spread)


def skewness(values: list[float]) -> float:
    """Population skewness (0 for a symmetric distribution)."""

    n = len(values)
    if n < 3:
        return 0.0
    mean = sum(values) / n
    m2 = sum((x - mean) ** 2 for x in values) / n
    if m2 <= 0:
        return 0.0
    m3 = sum((x - mean) ** 3 for x in values) / n
    return m3 / (m2 ** 1.5)


def kurtosis(values: list[float]) -> float:
    """Excess kurtosis (0 for a normal distribution)."""

    n = len(values)
    if n < 4:
        return 0.0
    mean = sum(values) / n
    m2 = sum((x - mean) ** 2 for x in values) / n
    if m2 <= 0:
        return 0.0
    m4 = sum((x - mean) ** 4 for x in values) / n
    return m4 / (m2 ** 2) - 3.0


def is_deterministic(values: list[float]) -> bool:
    """True when MAD == 0 (>50% identical values): a deterministic metric.

    DL numerical-correctness outputs should be bit-reproducible for a fixed
    image and seed, so any real spread is itself a signal worth flagging.
    """

    if not values:
        return False
    return mad(values) <= 0.0


def trim_outliers(
    values: list[float],
    z_threshold: float = 3.5,
    max_iterations: int = 3,
) -> tuple[list[float], int]:
    """Iteratively drop points with |modified z| > threshold.

    Returns ``(kept_values, removed_count)``. Stops early once nothing is
    removed or fewer than 3 points remain (median/MAD become unstable).
    """

    kept = list(values)
    removed = 0
    for _ in range(max_iterations):
        if len(kept) < 3:
            break
        zscores = modified_zscores(kept)
        survivors = [x for x, z in zip(kept, zscores) if abs(z) <= z_threshold]
        if len(survivors) == len(kept):
            break
        removed += len(kept) - len(survivors)
        kept = survivors
    return kept, removed


def bootstrap_median_ci(
    values: list[float],
    confidence: float = 0.95,
    n_boot: int = 1000,
    seed: int = 1729,
) -> tuple[float, float]:
    """Bootstrap confidence interval for the median.

    Deterministic for a fixed ``seed`` so baselines are reproducible.
    """

    if not values:
        raise ValueError("bootstrap of empty data")
    if len(values) == 1:
        only = float(values[0])
        return (only, only)
    rng = random.Random(seed)
    n = len(values)
    medians = [median([values[rng.randrange(n)] for _ in range(n)]) for _ in range(n_boot)]
    alpha = (1.0 - confidence) / 2.0 * 100.0
    return (percentile(medians, alpha), percentile(medians, 100.0 - alpha))


@dataclass
class MetricStat:
    """Robust summary of one metric plus its acceptance band."""

    metric: str
    direction: str
    n: int
    n_excluded: int
    median: float
    mad: float
    mad_sigma: float
    iqr: float
    p01: float
    p05: float
    p25: float
    p50: float
    p75: float
    p95: float
    p99: float
    minimum: float
    maximum: float
    skewness: float
    kurtosis: float
    ci_low: float
    ci_high: float
    deterministic: bool
    lower_bound: float
    upper_bound: float
    method: str

    def to_dict(self) -> dict[str, object]:
        """JSON-serializable view (infinities become null for storage)."""

        data = dict(self.__dict__)
        for key in ("lower_bound", "upper_bound"):
            if math.isinf(data[key]):
                data[key] = None
        return data


def summarize_metric(
    metric: str,
    raw_values: list[float],
    direction: str = DIRECTION_TWO_SIDED,
    tolerance_pct: float = 0.0,
    z_threshold: float = 3.5,
    trim: bool = True,
    bootstrap: bool = True,
) -> MetricStat:
    """Compute a robust baseline summary and acceptance band for one metric.

    The acceptance half-width is ``max(z * 1.4826 * MAD, tolerance_pct/100 *
    |median|)`` so a freakishly tight MAD can never make classification more
    sensitive than the configured engineering tolerance. ``direction`` makes
    the band one-sided for performance metrics and two-sided for correctness.
    """

    if direction not in VALID_DIRECTIONS:
        raise ValueError(f"unknown direction: {direction!r}")

    values = [
        float(v)
        for v in raw_values
        if v is not None and math.isfinite(float(v))
    ]
    if not values:
        raise ValueError(f"no finite values for metric {metric!r}")

    removed = 0
    if trim:
        kept, removed = trim_outliers(values, z_threshold=z_threshold)
        values_clean = kept if len(kept) >= 3 else values
        if len(kept) < 3:
            removed = 0
    else:
        values_clean = values

    center = median(values_clean)
    spread = mad(values_clean, center)
    sigma = _MAD_TO_SIGMA * spread
    deterministic = spread <= 0.0
    method = "deterministic" if deterministic else "robust_mad"

    rel_floor = abs(center) * (tolerance_pct / 100.0)
    stat_delta = z_threshold * sigma
    delta = max(stat_delta, rel_floor)
    # A deterministic metric has no statistical spread; rely on the tight
    # relative tolerance (e.g. DL numerical 0.1%) rather than a zero band.
    if deterministic and rel_floor > 0:
        delta = rel_floor

    if direction == DIRECTION_LOW_BAD:
        lower_bound, upper_bound = center - delta, math.inf
    elif direction == DIRECTION_HIGH_BAD:
        lower_bound, upper_bound = -math.inf, center + delta
    else:
        lower_bound, upper_bound = center - delta, center + delta

    if bootstrap and not deterministic:
        ci_low, ci_high = bootstrap_median_ci(values_clean)
    else:
        ci_low = ci_high = center

    return MetricStat(
        metric=metric,
        direction=direction,
        n=len(values_clean),
        n_excluded=removed,
        median=center,
        mad=spread,
        mad_sigma=sigma,
        iqr=iqr(values_clean) if len(values_clean) >= 2 else 0.0,
        p01=percentile(values_clean, 1),
        p05=percentile(values_clean, 5),
        p25=percentile(values_clean, 25),
        p50=center,
        p75=percentile(values_clean, 75),
        p95=percentile(values_clean, 95),
        p99=percentile(values_clean, 99),
        minimum=min(values_clean),
        maximum=max(values_clean),
        skewness=skewness(values_clean),
        kurtosis=kurtosis(values_clean),
        ci_low=ci_low,
        ci_high=ci_high,
        deterministic=deterministic,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        method=method,
    )


def classify_value(value: float, stat: "MetricStat | dict") -> tuple[str, float]:
    """Classify one observed value against a metric baseline.

    Returns ``(status, pct_diff)`` where status is ``normal``, ``degraded``,
    or ``improved``. ``stat`` may be a :class:`MetricStat` or a stored dict
    (with ``None`` bounds meaning unbounded on that side).
    """

    if isinstance(stat, MetricStat):
        center = stat.median
        direction = stat.direction
        lower = stat.lower_bound
        upper = stat.upper_bound
        p05, p95 = stat.p05, stat.p95
    else:
        center = float(stat["median"])
        direction = str(stat["direction"])
        lower = stat.get("lower_bound")
        upper = stat.get("upper_bound")
        lower = -math.inf if lower is None else float(lower)
        upper = math.inf if upper is None else float(upper)
        p05 = float(stat.get("p05", center))
        p95 = float(stat.get("p95", center))

    pct_diff = ((value - center) / center * 100.0) if center else 0.0

    if value < lower or value > upper:
        return "degraded", pct_diff

    if direction == DIRECTION_LOW_BAD and value > p95:
        return "improved", pct_diff
    if direction == DIRECTION_HIGH_BAD and value < p05:
        return "improved", pct_diff
    return "normal", pct_diff
