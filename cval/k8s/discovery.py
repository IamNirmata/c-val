from __future__ import annotations

from collections.abc import Mapping

from cval.k8s.client import KubectlClient
from cval.models import NodeResource


def parse_gpu_quantity(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text or text == "<none>":
        return 0
    return int(float(text))


def pod_effective_gpu_request(pod: Mapping[str, object]) -> int:
    spec = _mapping(pod.get("spec"))
    containers = _list(spec.get("containers"))
    init_containers = _list(spec.get("initContainers"))

    app_request = sum(_container_gpu_request(container) for container in containers)
    init_requests = [_container_gpu_request(container) for container in init_containers]
    init_request = max(init_requests) if init_requests else 0
    return max(app_request, init_request)


def pod_gpu_usage_by_node(pods_json: Mapping[str, object]) -> dict[str, int]:
    usage_by_node: dict[str, int] = {}
    for pod in _list(pods_json.get("items")):
        spec = _mapping(pod.get("spec"))
        status = _mapping(pod.get("status"))
        node_name = spec.get("nodeName")
        if not isinstance(node_name, str) or not node_name:
            continue
        if status.get("phase") in {"Succeeded", "Failed"}:
            continue
        usage_by_node[node_name] = usage_by_node.get(node_name, 0) + pod_effective_gpu_request(pod)
    return usage_by_node


def parse_node_resources(
    nodes_output: str,
    usage_by_node: Mapping[str, int] | None = None,
    node_name_filter: str | None = "hgx",
    excluded_node_names: set[str] | None = None,
) -> list[NodeResource]:
    usage = usage_by_node or {}
    excluded = excluded_node_names or set()
    nodes: list[NodeResource] = []
    for line in nodes_output.splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        node_name = parts[0]
        if node_name_filter and node_name_filter not in node_name:
            continue
        if node_name in excluded:
            continue
        capacity = parse_gpu_quantity(parts[1])
        allocatable = parse_gpu_quantity(parts[2])
        nodes.append(
            NodeResource(
                name=node_name,
                capacity=capacity,
                allocatable=allocatable,
                used=parse_gpu_quantity(usage.get(node_name, 0)),
            )
        )
    return nodes


def summarize_node_resources(nodes: list[NodeResource]) -> dict[str, int]:
    return {
        "capacity": sum(node.capacity for node in nodes),
        "allocatable": sum(node.allocatable for node in nodes),
        "used": sum(node.used for node in nodes),
        "free": sum(node.free for node in nodes),
    }


def discover_free_nodes_from_outputs(
    pods_json: Mapping[str, object],
    nodes_output: str,
    nodes_json: Mapping[str, object] | None = None,
    node_name_filter: str | None = "hgx",
) -> tuple[list[NodeResource], dict[str, int]]:
    usage_by_node = pod_gpu_usage_by_node(pods_json)
    excluded = unschedulable_node_names(nodes_json or {})
    nodes = parse_node_resources(
        nodes_output,
        usage_by_node,
        node_name_filter=node_name_filter,
        excluded_node_names=excluded,
    )
    return nodes, summarize_node_resources(nodes)


def unschedulable_node_names(
    nodes_json: Mapping[str, object],
    tolerated_no_schedule_taints: set[str] | None = None,
) -> set[str]:
    tolerated = tolerated_no_schedule_taints or {"nvidia.com/gpu", "rdma"}
    excluded: set[str] = set()
    for node in _list(nodes_json.get("items")):
        node_map = _mapping(node)
        metadata = _mapping(node_map.get("metadata"))
        spec = _mapping(node_map.get("spec"))
        name = metadata.get("name")
        if not isinstance(name, str) or not name:
            continue
        if spec.get("unschedulable") is True:
            excluded.add(name)
            continue
        for taint in _list(spec.get("taints")):
            taint_map = _mapping(taint)
            if taint_map.get("effect") == "NoSchedule" and taint_map.get("key") not in tolerated:
                excluded.add(name)
                break
    return excluded


def fully_free_node_names(nodes: list[NodeResource]) -> list[str]:
    return [node.name for node in nodes if node.is_fully_free]


def discover_free_nodes(
    client: KubectlClient | None = None,
    node_name_filter: str | None = "hgx",
) -> tuple[list[NodeResource], dict[str, int]]:
    kubectl = client or KubectlClient()
    return discover_free_nodes_from_outputs(
        kubectl.get_pods_json(),
        kubectl.get_nodes_capacity_table(),
        nodes_json=kubectl.get_nodes_json(),
        node_name_filter=node_name_filter,
    )


def _container_gpu_request(container: object) -> int:
    container_map = _mapping(container)
    resources = _mapping(container_map.get("resources"))
    requests = _mapping(resources.get("requests"))
    return parse_gpu_quantity(requests.get("nvidia.com/gpu", 0))


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _list(value: object) -> list[object]:
    return value if isinstance(value, list) else []