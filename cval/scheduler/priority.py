from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

from cval.models import QueueCandidate


SECONDS_PER_DAY = 86400


def build_priority_queue(
    free_nodes: Sequence[str],
    latest_status_by_node: Mapping[str, int],
    days_threshold: float = 7,
    now: datetime | None = None,
    shuffle: bool = False,
    rng: random.Random | None = None,
) -> list[QueueCandidate]:
    current_time = now or datetime.now(timezone.utc)
    current_timestamp = current_time.timestamp()
    threshold_seconds = days_threshold * SECONDS_PER_DAY
    candidates: list[tuple[str, int, float | None, str]] = []

    for node in free_nodes:
        last_tested = int(latest_status_by_node.get(node, 0) or 0)
        if last_tested <= 0:
            candidates.append((node, 0, None, "never-tested"))
            continue

        age_seconds = current_timestamp - last_tested
        age_days = age_seconds / SECONDS_PER_DAY
        if age_seconds > threshold_seconds:
            candidates.append((node, last_tested, age_days, "expired"))

    if shuffle:
        randomizer = rng or random.Random()
        randomizer.shuffle(candidates)
    else:
        candidates.sort(key=lambda candidate: (candidate[1], candidate[0]))

    return [
        QueueCandidate(
            node=node,
            priority=index,
            last_tested_timestamp=last_tested,
            age_days=age_days,
            reason=reason,
        )
        for index, (node, last_tested, age_days, reason) in enumerate(candidates, start=1)
    ]