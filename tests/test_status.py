from __future__ import annotations

import unittest

from cval.storage.status import (
    latest_status_rows_to_node_map,
    latest_status_rows_to_tsv,
    parse_latest_status_rows_json,
    parse_latest_status_tsv,
)


class StatusParsingTests(unittest.TestCase):
    def test_latest_status_tsv_uses_max_timestamp_per_node(self) -> None:
        output = "\n".join(
            [
                "node\ttest\tlatest_timestamp_num\tlatest_timestamp\tresult",
                "slc01-cl02-hgx-0001\tstorage\t100\t1970-01-01T00:01:40Z\tpass",
                "slc01-cl02-hgx-0001\tnccl\t200\t1970-01-01T00:03:20Z\tpass",
                "slc01-cl02-hgx-0002\tstorage\t\t\tfail",
                "malformed",
            ]
        )

        status = parse_latest_status_tsv(output)

        self.assertEqual(status, {"slc01-cl02-hgx-0001": 200, "slc01-cl02-hgx-0002": 0})

    def test_latest_status_json_round_trips_to_tsv_and_node_map(self) -> None:
        rows = parse_latest_status_rows_json(
            """
            [
                            {
                                "node": "slc01-cl02-hgx-0001",
                                "test": "storage",
                                "latest_timestamp": 100,
                                "result": "pass"
                            },
                            {
                                "node": "slc01-cl02-hgx-0001",
                                "test": "nccl",
                                "latest_timestamp": 200,
                                "result": "pass"
                            }
            ]
            """
        )

        self.assertEqual(latest_status_rows_to_node_map(rows), {"slc01-cl02-hgx-0001": 200})
        self.assertIn("1970-01-01T00:03:20Z", latest_status_rows_to_tsv(rows))


if __name__ == "__main__":
    unittest.main()