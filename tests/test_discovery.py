from __future__ import annotations

import unittest

from cval.k8s.discovery import (
    discover_free_nodes_from_outputs,
    fully_free_node_names,
    unschedulable_node_names,
)


class DiscoveryTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()