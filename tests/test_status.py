from __future__ import annotations

import unittest
from dataclasses import replace
from unittest.mock import patch

from cval.storage.status import (
    get_latest_status_rows,
    latest_status_rows_to_node_map,
    latest_status_rows_to_tsv,
    parse_latest_status_rows_json,
    parse_latest_status_tsv,
    resolve_status_pod,
)
from cval.config import load_config
from cval.k8s.client import CommandResult
from cval.storage.metrics import (
    get_latest_nccl_health_metrics,
    get_latest_nccl_metrics,
    get_latest_storage_metrics,
)


class FakeKubectlClient:
    def __init__(self, responses: dict[tuple[str, ...], CommandResult]) -> None:
        self.responses = responses
        self.calls = []

    def run(self, args, check=True, input_text=None):
        self.calls.append((tuple(args), input_text))
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

    def test_status_and_metric_helpers_prefer_explicit_config(self) -> None:
        base = load_config()
        config = replace(
            base,
            cluster=replace(
                base.cluster,
                namespace="explicit-namespace",
                pvc_access_pod="explicit-pod",
            ),
            storage=replace(
                base.storage,
                validation_db_path="/explicit/status ?#%.db",
                nccl_db_path="/explicit/nccl ?#%.db",
                storage_db_path="/explicit/storage ?#%.db",
            ),
        )
        client = FakeKubectlClient({})
        response = CommandResult(args=(), stdout="[]", stderr="", returncode=0)

        def run(args, check=True, input_text=None):
            client.calls.append((tuple(args), input_text))
            return response

        client.run = run  # type: ignore[method-assign]
        with patch(
            "cval.storage.status.resolve_status_pod",
            return_value="resolved-explicit-pod",
        ), patch(
            "cval.storage.metrics.resolve_status_pod",
            return_value="resolved-explicit-pod",
        ), patch(
            "cval.storage.status.load_config",
            side_effect=AssertionError("ambient status config reload"),
        ), patch(
            "cval.storage.metrics.load_config",
            side_effect=AssertionError("ambient metric config reload"),
        ):
            self.assertEqual(get_latest_status_rows(client=client, config=config), [])
            self.assertEqual(get_latest_nccl_metrics(client=client, config=config), {})
            self.assertEqual(get_latest_storage_metrics(client=client, config=config), {})
            self.assertEqual(
                get_latest_nccl_health_metrics(client=client, config=config), {}
            )

        commands = [call[0] for call in client.calls]
        self.assertTrue(all("explicit-namespace" in command for command in commands))
        self.assertEqual(
            [command[-1] for command in commands],
            [
                "/explicit/status ?#%.db",
                "/explicit/nccl ?#%.db",
                "/explicit/storage ?#%.db",
                "/explicit/nccl ?#%.db",
            ],
        )
        self.assertTrue(
            all("connect_sqlite_readonly" in (call[1] or "") for call in client.calls)
        )


if __name__ == "__main__":
    unittest.main()