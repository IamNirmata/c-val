from __future__ import annotations

import unittest
from datetime import datetime, timezone

from cval.scheduler.priority import SECONDS_PER_DAY, build_priority_queue


class PriorityTests(unittest.TestCase):
    def test_never_tested_and_expired_nodes_are_prioritized(self) -> None:
        now = datetime.fromtimestamp(10_000_000, tz=timezone.utc)
        latest_status = {
            "fresh-node": int(now.timestamp() - SECONDS_PER_DAY),
            "expired-node": int(now.timestamp() - 10 * SECONDS_PER_DAY),
            "older-expired-node": int(now.timestamp() - 20 * SECONDS_PER_DAY),
        }

        queue = build_priority_queue(
            ["fresh-node", "never-node", "expired-node", "older-expired-node"],
            latest_status,
            days_threshold=4,
            now=now,
        )

        self.assertEqual(
            [candidate.node for candidate in queue],
            ["never-node", "older-expired-node", "expired-node"],
        )
        self.assertEqual(
            [candidate.reason for candidate in queue],
            ["never-tested", "expired", "expired"],
        )
        self.assertEqual([candidate.priority for candidate in queue], [1, 2, 3])


if __name__ == "__main__":
    unittest.main()