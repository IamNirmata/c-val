from __future__ import annotations

import unittest

from cval.k8s.discovery import (
    describe_node,
    discover_free_nodes_from_outputs,
    fully_free_node_names,
    gpu_node_names_from_capacity_table,
    node_is_cordoned,
    node_has_blocking_no_schedule_taint,
    node_is_ready,
    parse_cpu_millicores,
    parse_memory_bytes,
    resource_insufficient_node_names,
    unschedulable_node_names,
)
from cval.config import CvalConfig, JobTemplateConfig


class DiscoveryTests(unittest.TestCase):
    def test_gpu_inventory_filters_name_and_positive_capacity_without_pods(self) -> None:
        output = "\n".join(
            (
                "slc01-cl02-hgx-0001 8 8",
                "slc01-cl02-hgx-0002 8 8",
                "slc01-cl02-cpu-0001 <none> <none>",
                "other-gpu-node 8 8",
            )
        )

        self.assertEqual(
            gpu_node_names_from_capacity_table(output, "hgx"),
            ["slc01-cl02-hgx-0001", "slc01-cl02-hgx-0002"],
        )

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
                    "status": {"allocatable": {"cpu": "110", "memory": "2Ti", "nvidia.com/gpu": "8", "rdma/rdma_shared_device_a": "1"}},
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
                    "status": {"allocatable": {"cpu": "110", "memory": "2Ti", "nvidia.com/gpu": "8", "rdma/rdma_shared_device_a": "1"}},
                },
                {
                    "metadata": {"name": "slc01-cl02-hgx-0003"},
                    "spec": {
                        "taints": [
                            {"key": "nvidia.com/gpu", "effect": "NoSchedule"}
                        ]
                    },
                    "status": {"allocatable": {"cpu": "110", "memory": "2Ti", "nvidia.com/gpu": "8", "rdma/rdma_shared_device_a": "1"}},
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

    def test_missing_required_rdma_resource_is_blocked(self) -> None:
        config = CvalConfig(
            job_template=JobTemplateConfig(
                cpu="1",
                memory="1Gi",
                gpu_resource_name="nvidia.com/gpu",
                gpu_count="8",
                rdma_resource_name="rdma/rdma_shared_device_a",
                rdma_count="1",
            )
        )
        nodes = {
            "items": [
                {
                    "metadata": {"name": "node-no-rdma"},
                    "status": {
                        "allocatable": {
                            "cpu": "100",
                            "memory": "2Ti",
                            "nvidia.com/gpu": "8",
                        }
                    },
                }
            ]
        }
        self.assertEqual(
            resource_insufficient_node_names({"items": []}, nodes, config),
            {"node-no-rdma"},
        )


if __name__ == "__main__":
    unittest.main()