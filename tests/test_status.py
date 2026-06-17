from __future__ import annotations

import unittest

from cval.storage.status import (
    latest_status_rows_to_node_map,
    latest_status_rows_to_tsv,
    parse_latest_status_rows_json,
    parse_latest_status_tsv,
    resolve_status_pod,
)
from cval.k8s.client import CommandResult


class FakeKubectlClient:
    def __init__(self, responses: dict[tuple[str, ...], CommandResult]) -> None:
        self.responses = responses

    def run(self, args, check=True, input_text=None):
        result = self.responses.get(
            tuple(args),
            CommandResult(args=("kubectl", *args), stdout="", stderr="not found", returncode=1),
        )
        if check and result.returncode != 0:
            raise RuntimeError(result.stderr)
        return result


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

    def test_resolve_status_pod_uses_volcano_access_pod(self) -> None:
        client = FakeKubectlClient(
            {
                (
                    "get",
                    "pod",
                    "-n",
                    "gcr-admin",
                    "gcr-admin-pvc-access-server-0",
                    "-o",
                    "json",
                ): CommandResult(
                    args=(),
                    stdout='{"status":{"phase":"Running"}}',
                    stderr="",
                    returncode=0,
                )
            }
        )

        self.assertEqual(
            resolve_status_pod(client, "gcr-admin", "gcr-admin-pvc-access"),
            "gcr-admin-pvc-access-server-0",
        )

    def test_resolve_status_pod_uses_label_selector(self) -> None:
        client = FakeKubectlClient(
            {
                (
                    "get",
                    "pods",
                    "-n",
                    "gcr-admin",
                    "-l",
                    "volcano.sh/job-name=gcr-admin-pvc-access",
                    "-o",
                    "json",
                ): CommandResult(
                    args=(),
                    stdout=(
                        '{"items":[{"metadata":{"name":"access-pod"},'
                        '"status":{"phase":"Running"}}]}'
                    ),
                    stderr="",
                    returncode=0,
                )
            }
        )

        self.assertEqual(
            resolve_status_pod(client, "gcr-admin", "gcr-admin-pvc-access"),
            "access-pod",
        )


if __name__ == "__main__":
    unittest.main()