from __future__ import annotations

import unittest
from unittest.mock import Mock

from cval.k8s.discovery import (
    describe_node,
    describe_nodes_from_outputs,
    discover_free_nodes_from_outputs,
    discover_gpu_node_names,
    fully_free_node_names,
    node_is_cordoned,
    node_has_blocking_no_schedule_taint,
    node_is_ready,
    parse_cpu_millicores,
    parse_memory_bytes,
)
from cval.config import load_config


class DiscoveryTests(unittest.TestCase):
    def _node(self, name, *, spec=None, allocatable=None):
        return {
            "metadata": {"name": name}, "spec": spec or {},
            "status": {"capacity": {"nvidia.com/gpu": "8"},
                       "allocatable": allocatable if allocatable is not None else {
                           "cpu": "110", "memory": "2Ti", "nvidia.com/gpu": "8", "rdma/rdma_shared_device_a": "1"},
                       "conditions": [{"type": "Ready", "status": "True"}]},
        }

    def test_gpu_inventory_filters_name_and_positive_capacity_without_pods(self) -> None:
        client = Mock()
        client.get_nodes_json.return_value = {"items": [
            self._node("slc01-cl02-hgx-0001"), self._node("slc01-cl02-hgx-0002"),
            {"metadata": {"name": "slc01-cl02-cpu-0001"}, "status": {"capacity": {}}},
            self._node("other-gpu-node"),
        ]}
        self.assertEqual(
            discover_gpu_node_names(client=client, node_name_filter="hgx"),
            ["slc01-cl02-hgx-0001", "slc01-cl02-hgx-0002"],
        )
        client.get_pods_json.assert_not_called()

    def test_targeted_node_status_reads_only_one_node_and_its_pods(self) -> None:
        class Client:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def get_node_json(self, node: str):
                self.calls.append(("node", node))
                return {
                    "metadata": {"name": node},
                    "spec": {},
                    "status": {
                        "capacity": {"nvidia.com/gpu": "8"},
                        "allocatable": {
                            "cpu": "110",
                            "memory": "3036180572Ki",
                            "nvidia.com/gpu": "8",
                            "rdma/rdma_shared_device_a": "63",
                        },
                        "conditions": [{"type": "Ready", "status": "True"}],
                    },
                }

            def get_pods_for_node_json(self, node: str):
                self.calls.append(("pods", node))
                return {"items": []}

        client = Client()
        status = describe_node("slc01-cl02-hgx-0001", client=client)

        self.assertTrue(status.fully_free)
        self.assertEqual(status.status_label, "ready")
        self.assertEqual(
            client.calls,
            [
                ("node", "slc01-cl02-hgx-0001"),
                ("pods", "slc01-cl02-hgx-0001"),
            ],
        )

    def test_parses_cpu_and_memory_quantities(self) -> None:
        self.assertEqual(parse_cpu_millicores("250m"), 250)
        self.assertEqual(parse_cpu_millicores("100"), 100_000)
        self.assertEqual(parse_memory_bytes("1500Gi"), 1500 * 1024**3)
        self.assertEqual(parse_memory_bytes("2877141Mi"), 2877141 * 1024**2)

    def test_discovers_fully_free_gpu_nodes(self) -> None:
        pods_json = {
            "items": [
                {
                    "spec": {
                        "nodeName": "slc01-cl02-hgx-0001",
                        "containers": [{"resources": {"requests": {"nvidia.com/gpu": "4"}}}],
                    },
                    "status": {"phase": "Running"},
                },
                {
                    "spec": {
                        "nodeName": "slc01-cl02-hgx-0002",
                        "containers": [{"resources": {"requests": {"nvidia.com/gpu": "8"}}}],
                    },
                    "status": {"phase": "Succeeded"},
                },
                {
                    "spec": {
                        "nodeName": "slc01-cl02-hgx-0003",
                        "containers": [{"resources": {"requests": {"nvidia.com/gpu": "1"}}}],
                        "initContainers": [{"resources": {"requests": {"nvidia.com/gpu": "2"}}}],
                    },
                    "status": {"phase": "Pending"},
                },
            ]
        }
        inventory = {"items": [self._node(f"slc01-cl02-hgx-{number:04d}") for number in (1, 2, 3)]}
        nodes, totals = discover_free_nodes_from_outputs(pods_json, "", inventory)

        self.assertEqual(totals, {"capacity": 24, "allocatable": 24, "used": 6, "free": 18})
        self.assertEqual(fully_free_node_names(nodes), ["slc01-cl02-hgx-0002"])

    def test_excludes_unschedulable_nodes_from_free_list(self) -> None:
        pods_json = {"items": []}
        nodes_json = {"items": [
            self._node("slc01-cl02-hgx-0001", spec={"unschedulable": True}),
            self._node("slc01-cl02-hgx-0002", spec={"taints": [{"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}]}),
            self._node("slc01-cl02-hgx-0003", spec={"taints": [{"key": "nvidia.com/gpu", "effect": "NoSchedule"}]}),
        ]}
        nodes, totals = discover_free_nodes_from_outputs(pods_json, "", nodes_json)

        self.assertEqual(fully_free_node_names(nodes), ["slc01-cl02-hgx-0003"])
        self.assertEqual(totals["capacity"], 8)

    def test_shared_description_reports_schedulability(self) -> None:
        nodes_json = {"items": [
            self._node("cordoned", spec={"unschedulable": True}),
            self._node("tainted", spec={"taints": [{"key": "maintenance", "effect": "NoSchedule"}]}),
            self._node("tolerated", spec={"taints": [{"key": "rdma", "effect": "NoSchedule"}]}),
        ]}
        statuses = describe_nodes_from_outputs({"items": []}, nodes_json)
        self.assertEqual({status.name for status in statuses if not status.schedulable}, {"cordoned", "tainted"})

    def test_node_is_cordoned(self) -> None:
        self.assertTrue(node_is_cordoned({"spec": {"unschedulable": True}}))
        self.assertFalse(node_is_cordoned({"spec": {}}))
        self.assertFalse(node_is_cordoned({}))

    def test_node_is_ready(self) -> None:
        ready = {"status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        not_ready = {"status": {"conditions": [{"type": "Ready", "status": "False"}]}}
        unknown = {"status": {"conditions": [{"type": "Ready", "status": "Unknown"}]}}
        self.assertTrue(node_is_ready(ready))
        self.assertFalse(node_is_ready(not_ready))
        self.assertFalse(node_is_ready(unknown))
        self.assertFalse(node_is_ready({"status": {}}))

    def test_targeted_cordoned_node_keeps_unrelated_taint_blocking(self) -> None:
        node = {
            "spec": {
                "unschedulable": True,
                "taints": [
                    {"key": "maintenance", "effect": "NoSchedule"},
                    {"key": "nvidia.com/gpu", "effect": "NoSchedule"},
                ],
            }
        }
        self.assertTrue(
            node_has_blocking_no_schedule_taint(
                node, {"nvidia.com/gpu", "rdma"}
            )
        )

    def test_standard_cordon_taint_is_not_an_unrelated_blocker(self) -> None:
        node = {
            "spec": {
                "unschedulable": True,
                "taints": [
                    {
                        "key": "node.kubernetes.io/unschedulable",
                        "effect": "NoSchedule",
                    },
                    {"key": "nvidia.com/gpu", "effect": "NoSchedule"},
                ],
            }
        }
        self.assertFalse(
            node_has_blocking_no_schedule_taint(
                node,
                {
                    "nvidia.com/gpu",
                    "rdma",
                    "node.kubernetes.io/unschedulable",
                },
            )
        )

    def test_excludes_nodes_without_validation_pod_resources(self) -> None:
        config = load_config()
        nodes_json = {"items": [self._node("node-starved"), self._node("node-free")]}
        pods_json = {
            "items": [
                {
                    "spec": {
                        "nodeName": "node-starved",
                        "containers": [
                            {
                                "resources": {
                                    "requests": {
                                        "cpu": "3001m",
                                        "memory": "2877141Mi",
                                        "nvidia.com/gpu": "1",
                                        "rdma/rdma_shared_device_a": "2",
                                    }
                                }
                            }
                        ],
                    },
                    "status": {"phase": "Running"},
                }
            ]
        }

        statuses = describe_nodes_from_outputs(pods_json, nodes_json, config)
        nodes, _totals = discover_free_nodes_from_outputs(
            pods_json,
            "",
            nodes_json,
        )

        self.assertEqual({status.name for status in statuses if not status.resource_ready}, {"node-starved"})
        self.assertEqual(fully_free_node_names(nodes), ["node-free"])

    def test_missing_required_rdma_resource_is_blocked(self) -> None:
        nodes = {"items": [self._node("node-no-rdma", allocatable={"cpu": "100", "memory": "2Ti", "nvidia.com/gpu": "8"})]}
        status = describe_nodes_from_outputs({"items": []}, nodes, load_config())[0]
        self.assertFalse(status.resource_ready)
        self.assertFalse(status.eligible)
        self.assertIn("rdma/rdma_shared_device_a", status.reason)


if __name__ == "__main__":
    unittest.main()