"""Pure scalar projection for legacy classification rows."""

from __future__ import annotations

import json


def legacy_classification_scalars(
    metrics_json: object,
    *,
    n_compared: int,
    n_degraded: int,
) -> tuple[int, float, float]:
    """Derive current scalar fields from one legacy ``metrics_json`` value."""

    try:
        metrics = json.loads(metrics_json or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        metrics = []
    band_degraded = 0
    worst_pct_diff = 0.0
    if isinstance(metrics, list):
        for metric in metrics:
            if not isinstance(metric, dict) or metric.get("status") != "degraded":
                continue
            band_degraded += 1
            try:
                worst_pct_diff = max(
                    worst_pct_diff,
                    abs(float(metric.get("pct_diff") or 0.0)),
                )
            except (TypeError, ValueError):
                continue
    if band_degraded == 0:
        band_degraded = int(n_degraded)
    degraded_fraction = n_degraded / n_compared if n_compared else 0.0
    return band_degraded, degraded_fraction, worst_pct_diff


__all__ = ["legacy_classification_scalars"]
