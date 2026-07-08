from __future__ import annotations

import unittest

from cval.k8s.discovery import (
    discover_free_nodes_from_outputs,
    fully_free_node_names,
    node_is_cordoned,
    node_is_ready,
    parse_cpu_millicores,
    parse_memory_bytes,
    resource_insufficient_node_names,
    unschedulable_node_names,
)
from cval.config import CvalConfig, JobTemplateConfig


class DiscoveryTests(unittest.TestCase):
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
        nodes_output = "\n".join(
            [
                "slc01-cl02-hgx-0001 8 8",
                "slc01-cl02-hgx-0002 8 8",
                "slc01-cl02-hgx-0003 8 8",
                "slc01-cl02-ccpu-001 <none> <none>",
            ]
        )

        nodes, totals = discover_free_nodes_from_outputs(pods_json, nodes_output)

        self.assertEqual(totals, {"capacity": 24, "allocatable": 24, "used": 6, "free": 18})
        self.assertEqual(fully_free_node_names(nodes), ["slc01-cl02-hgx-0002"])

    def test_excludes_unschedulable_nodes_from_free_list(self) -> None:
        pods_json = {"items": []}
        nodes_output = "\n".join(
            [
                "slc01-cl02-hgx-0001 8 8",
                "slc01-cl02-hgx-0002 8 8",
                "slc01-cl02-hgx-0003 8 8",
            ]
        )
        nodes_json = {
            "items": [
                {
                    "metadata": {"name": "slc01-cl02-hgx-0001"},
                    "spec": {"unschedulable": True},
                },
                {
                    "metadata": {"name": "slc01-cl02-hgx-0002"},
                    "spec": {
                        "taints": [
                            {
                                "key": "node.kubernetes.io/unschedulable",
                                "effect": "NoSchedule",
                            }
                        ]
                    },
                },
                {
                    "metadata": {"name": "slc01-cl02-hgx-0003"},
                    "spec": {
                        "taints": [
                            {"key": "nvidia.com/gpu", "effect": "NoSchedule"}
                        ]
                    },
                },
            ]
        }

        nodes, totals = discover_free_nodes_from_outputs(pods_json, nodes_output, nodes_json)

        self.assertEqual(fully_free_node_names(nodes), ["slc01-cl02-hgx-0003"])
        self.assertEqual(totals["capacity"], 8)

    def test_unschedulable_node_names(self) -> None:
        nodes_json = {
            "items": [
                {"metadata": {"name": "cordoned"}, "spec": {"unschedulable": True}},
                {
                    "metadata": {"name": "tainted"},
                    "spec": {"taints": [{"key": "maintenance", "effect": "NoSchedule"}]},
                },
                {
                    "metadata": {"name": "tolerated"},
                    "spec": {"taints": [{"key": "rdma", "effect": "NoSchedule"}]},
                },
            ]
        }

        self.assertEqual(unschedulable_node_names(nodes_json), {"cordoned", "tainted"})

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
        # Missing Ready condition is treated as ready so partial fixtures are benign.
        self.assertTrue(node_is_ready({"status": {}}))

    def test_excludes_nodes_without_validation_pod_resources(self) -> None:
        config = CvalConfig(
            job_template=JobTemplateConfig(
                cpu="100",
                memory="1500Gi",
                gpu_resource_name="nvidia.com/gpu",
                gpu_count="8",
                rdma_resource_name="rdma/rdma_shared_device_a",
                rdma_count="1",
            )
        )
        nodes_json = {
            "items": [
                {
                    "metadata": {"name": "node-starved"},
                    "status": {
                        "allocatable": {
                            "cpu": "110",
                            "memory": "3036180572Ki",
                            "nvidia.com/gpu": "8",
                            "rdma/rdma_shared_device_a": "63",
                        }
                    },
                },
                {
                    "metadata": {"name": "node-free"},
                    "status": {
                        "allocatable": {
                            "cpu": "110",
                            "memory": "3036180572Ki",
                            "nvidia.com/gpu": "8",
                            "rdma/rdma_shared_device_a": "63",
                        }
                    },
                },
            ]
        }
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

        excluded = resource_insufficient_node_names(pods_json, nodes_json, config)
        nodes, _totals = discover_free_nodes_from_outputs(
            pods_json,
            "node-starved 8 8\nnode-free 8 8",
            nodes_json,
        )

        self.assertEqual(excluded, {"node-starved"})
        self.assertEqual(fully_free_node_names(nodes), ["node-free"])


if __name__ == "__main__":
    unittest.main()