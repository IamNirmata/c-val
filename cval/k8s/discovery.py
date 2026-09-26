"""Kubernetes node discovery and GPU free-capacity calculation.

This module turns raw Kubernetes pod/node JSON into c-val `NodeResource` models.
It deliberately stays read-only and excludes nodes that cannot schedule c-val
jobs, such as cordoned nodes or nodes with non-tolerated `NoSchedule` taints.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_CEILING
import warnings

from cval.config import CvalConfig, load_config
from cval.k8s.client import KubectlClient
from cval.models import NodeResource
from cval.validation.registry import parse_resource_quantity


def parse_gpu_quantity(value: object) -> int:
    """Require a non-negative whole count for GPU/RDMA resources."""

    quantity = parse_resource_quantity(str(value))
    if quantity != quantity.to_integral_value():
        raise ValueError(f"Extended resource count must be integral: {value!r}")
    return int(quantity)


def parse_cpu_millicores(value: object) -> int:
    """Parse Kubernetes CPU quantities into millicores."""

    return int((parse_resource_quantity(str(value)) * 1000).to_integral_value(rounding=ROUND_CEILING))


def parse_memory_bytes(value: object) -> int:
    """Parse common Kubernetes memory quantities into bytes."""

    return int(parse_resource_quantity(str(value)).to_integral_value(rounding=ROUND_CEILING))


def pod_resource_usage_by_node(
    pods_json: Mapping[str, object],
    resource_name: str,
    parser,
) -> dict[str, int]:
    """Aggregate active pod requests for one resource by node."""

    usage_by_node: dict[str, int] = {}
    for pod in _objects(pods_json, "pod"):
        spec = _mapping(pod.get("spec"))
        status = _mapping(pod.get("status"))
        node_name = spec.get("nodeName")
        if not isinstance(node_name, str) or not node_name:
            continue
        if status.get("phase") in {"Succeeded", "Failed"}:
            continue
        if status.get("phase") not in {"Pending", "Running"}:
            raise ValueError(f"Unknown active pod phase on {node_name!r}")
        requested = pod_effective_resource_request(pod, resource_name, parser)
        usage_by_node[node_name] = usage_by_node.get(node_name, 0) + requested
    return usage_by_node


def pod_effective_resource_request(pod: Mapping[str, object], resource_name: str, parser) -> int:
    """Return scheduler-effective request for one resource on a pod."""

    spec = _mapping(pod.get("spec"))
    containers = _list(spec.get("containers"))
    init_containers = _list(spec.get("initContainers"))
    status = _mapping(pod.get("status"))
    if not containers or "resources" in spec or status.get("resize"):
        raise ValueError("Missing container requests or unsupported pod-level/resize resource accounting")
    if any(
        "allocatedResources" in _mapping(container) or "resources" in _mapping(container)
        for container in _list(status.get("containerStatuses"))
    ):
        raise ValueError("Allocated container resource accounting requires separate verification")
    app_request = sum(_container_resource_request(container, resource_name, parser) for container in containers)
    restartable_request = 0
    init_request = 0
    for container in init_containers:
        requested = _container_resource_request(container, resource_name, parser)
        restart_policy = _mapping(container).get("restartPolicy")
        if restart_policy == "Always":
            restartable_request += requested
            requested = restartable_request
        elif restart_policy is None:
            requested += restartable_request
        else:
            raise ValueError(f"Unknown init-container restart policy: {restart_policy!r}")
        init_request = max(init_request, requested)
    overhead = parser(_mapping(spec.get("overhead")).get(resource_name, 0))
    return max(app_request + restartable_request, init_request) + overhead


def summarize_node_resources(nodes: list[NodeResource]) -> dict[str, int]:
    """Return aggregate capacity, allocatable, used, and free GPUs."""

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
    node_name_filter: str | None = None,
    config: CvalConfig | None = None,
) -> tuple[list[NodeResource], dict[str, int]]:
    """Build node resources and totals from raw pod/node command outputs."""

    active_config = config or load_config()
    statuses = describe_nodes_from_outputs(
        pods_json, nodes_json, active_config, skip_unverified=True
    )
    nodes = [
        NodeResource(status.name, status.capacity, status.allocatable, status.used, status.resource_ready)
        for status in statuses
        if status.found and status.is_gpu_node and status.ready and status.schedulable
        and (not node_name_filter or node_name_filter in status.name)
    ]
    return nodes, summarize_node_resources(nodes)


def fully_free_node_names(nodes: list[NodeResource]) -> list[str]:
    """Return node names whose allocatable GPUs are entirely free."""

    return [node.name for node in nodes if node.is_fully_free]


def discover_gpu_node_names(
    client: KubectlClient | None = None,
    node_name_filter: str | None = None,
    config: CvalConfig | None = None,
) -> list[str]:
    """List matching GPU nodes without reading any pod inventory."""

    active_config = config or load_config()
    kubectl = client or KubectlClient(config=active_config)
    resolved_filter = active_config.cluster.node_filter if node_name_filter is None else node_name_filter
    return [
        name for name, node in _nodes_by_name(kubectl.get_nodes_json()).items()
        if (not resolved_filter or resolved_filter in name)
        and parse_gpu_quantity(_mapping(_mapping(node.get("status")).get("capacity")).get(active_config.job_template.gpu_resource_name, 0)) > 0
    ]


def discover_free_nodes(
    client: KubectlClient | None = None,
    node_name_filter: str | None = None,
    config: CvalConfig | None = None,
) -> tuple[list[NodeResource], dict[str, int]]:
    """Discover schedulable GPU nodes from live Kubernetes read-only calls."""

    active_config = config or load_config()
    kubectl = client or KubectlClient(config=active_config)
    resolved_node_name_filter = active_config.cluster.node_filter if node_name_filter is None else node_name_filter
    return discover_free_nodes_from_outputs(
        kubectl.get_pods_json(),
        "",
        nodes_json=kubectl.get_nodes_json(),
        node_name_filter=resolved_node_name_filter,
        config=active_config,
    )


@dataclass(frozen=True)
class NodeStatus:
    """Schedulability snapshot for one named node, used by targeted validation."""

    name: str
    found: bool
    is_gpu_node: bool
    schedulable: bool
    resource_ready: bool
    capacity: int
    allocatable: int
    used: int
    free: int
    fully_free: bool
    reason: str
    cordoned: bool
    ready: bool
    status_label: str

    @property
    def eligible(self) -> bool:
        """Return the shared readiness, placement and capacity decision."""

        return self.fully_free


def _objects(payload: Mapping[str, object] | None, kind: str) -> list[Mapping[str, object]]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("items"), list):
        raise ValueError(f"Missing or invalid Kubernetes {kind} inventory")
    if _mapping(payload.get("metadata")).get("continue"):
        raise ValueError(f"Incomplete Kubernetes {kind} inventory")
    items = payload["items"]
    if not all(isinstance(item, Mapping) for item in items):
        raise ValueError(f"Malformed Kubernetes {kind} inventory entry")
    return items


def _nodes_by_name(payload: Mapping[str, object] | None) -> dict[str, Mapping[str, object]]:
    nodes = {}
    for node in _objects(payload, "node"):
        name = _mapping(node.get("metadata")).get("name")
        if not isinstance(name, str) or not name or name in nodes:
            raise ValueError("Missing or duplicate Kubernetes node name")
        nodes[name] = node
    return nodes


def node_is_cordoned(node_map: Mapping[str, object]) -> bool:
    """Return True when the node has `spec.unschedulable: true` (kubectl cordon)."""

    value = _mapping(node_map.get("spec")).get("unschedulable", False)
    if not isinstance(value, bool):
        raise ValueError("Unknown node scheduling state")
    return value


def node_is_ready(node_map: Mapping[str, object]) -> bool:
    """Return the kubelet Ready state.

    Only an explicit ``Ready=True`` condition is accepted. Missing, false, or
    unknown readiness is ineligible for a real targeted validation job.
    """

    status = _mapping(node_map.get("status"))
    conditions = [_mapping(condition) for condition in _list(status.get("conditions"))]
    readiness = [condition.get("status") for condition in conditions if condition.get("type") == "Ready"]
    return readiness == ["True"]


def node_has_blocking_no_schedule_taint(
    node_map: Mapping[str, object], tolerated: set[str]
) -> bool:
    """Check blocking taints; c-val templates tolerate NoSchedule only."""

    for taint in _list(_mapping(node_map.get("spec")).get("taints")):
        taint_map = _mapping(taint)
        if not isinstance(taint_map.get("key"), str) or taint_map.get("effect") not in {"NoSchedule", "NoExecute", "PreferNoSchedule"}:
            raise ValueError("Unknown node taint evidence")
        if (
            taint_map.get("effect") == "NoExecute"
            or (taint_map.get("effect") == "NoSchedule" and taint_map.get("key") not in tolerated)
        ):
            return True
    return False


def describe_node_from_outputs(
    node_name: str,
    pods_json: Mapping[str, object],
    nodes_output: str,
    nodes_json: Mapping[str, object] | None = None,
    config: CvalConfig | None = None,
    *,
    allow_cordoned: bool = False,
) -> NodeStatus:
    """Describe structured evidence; the legacy capacity text is not authoritative."""

    statuses = describe_nodes_from_outputs(pods_json, nodes_json, config, allow_cordoned=allow_cordoned)
    for status in statuses:
        if status.name == node_name:
            return status
    return NodeStatus(node_name, False, False, False, False, 0, 0, 0, 0, False,
                      "node not found in the cluster node list", False, False, "not_found")


def describe_nodes_from_outputs(
    pods_json: Mapping[str, object],
    nodes_json: Mapping[str, object] | None,
    config: CvalConfig | None = None,
    *,
    allow_cordoned: bool = False,
    skip_unverified: bool = False,
) -> list[NodeStatus]:
    """Compute eligibility once per node using one shared resource inventory."""

    active_config = config or load_config()
    nodes = _nodes_by_name(nodes_json)
    job = active_config.job_template
    requirements = {
        job.gpu_resource_name: (parse_gpu_quantity(job.gpu_count), parse_gpu_quantity),
        "cpu": (parse_cpu_millicores(job.cpu), parse_cpu_millicores),
        "memory": (parse_memory_bytes(job.memory), parse_memory_bytes),
        job.rdma_resource_name: (parse_gpu_quantity(job.rdma_count), parse_gpu_quantity),
    }
    pods_by_node: dict[str, list[Mapping[str, object]]] = {}
    for pod in _objects(pods_json, "pod"):
        spec = _mapping(pod.get("spec"))
        node_name = spec.get("nodeName", "")
        if not spec or not isinstance(node_name, str):
            raise ValueError("Cannot identify node for malformed pod evidence")
        if node_name:
            pods_by_node.setdefault(node_name, []).append(pod)
    tolerated = set(active_config.cluster.tolerated_no_schedule_taints) & {job.gpu_toleration_key, job.rdma_toleration_key}
    statuses = []
    for name, node in nodes.items():
        try:
            node_pods = {"items": pods_by_node.get(name, [])}
            usage = {
                resource: pod_resource_usage_by_node(node_pods, resource, parser)
                for resource, (_required, parser) in requirements.items()
            }
            status = _describe_node(
                name, node, usage, requirements, job.gpu_resource_name, tolerated, allow_cordoned
            )
        except ValueError as exc:
            if not skip_unverified:
                raise ValueError(f"{name}: eligibility unverified: {exc}") from exc
            warnings.warn(f"{name}: eligibility unverified; excluded: {exc}", stacklevel=2)
        else:
            statuses.append(status)
    return statuses


def _describe_node(node_name, node_map, usage, requirements, gpu_resource, tolerated, allow_cordoned) -> NodeStatus:
    status = _mapping(node_map.get("status"))
    capacities = _mapping(status.get("capacity"))
    allocations = _mapping(status.get("allocatable"))
    capacity = parse_gpu_quantity(capacities.get(gpu_resource, 0))
    allocatable = parse_gpu_quantity(allocations.get(gpu_resource, 0))
    used = usage[gpu_resource].get(node_name, 0)
    free = max(allocatable - used, 0)
    shortfalls = []
    for name, (required, parser) in requirements.items():
        if required == 0:
            continue
        if name not in allocations:
            shortfalls.append(f"{name}: allocatable missing")
        else:
            available = parser(allocations[name]) - usage[name].get(node_name, 0)
            if available < required:
                shortfalls.append(f"{name}: free={available}, required={required}")
    resource_ready = not shortfalls
    is_gpu_node = capacity > 0
    cordoned = node_is_cordoned(node_map)
    targeted_tolerations = set(tolerated)
    if cordoned and allow_cordoned:
        targeted_tolerations.add("node.kubernetes.io/unschedulable")
    schedulable = (
        isinstance(node_map.get("spec"), Mapping)
        and (not cordoned or allow_cordoned)
        and not node_has_blocking_no_schedule_taint(node_map, targeted_tolerations)
    )
    ready = node_is_ready(node_map)
    fully_free = (
        is_gpu_node
        and ready
        and schedulable
        and resource_ready
        and allocatable > 0
        and used == 0
    )

    if not is_gpu_node:
        status_label = "not_gpu"
        reason = f"node has no {gpu_resource} capacity"
    elif not ready:
        status_label = "not_ready"
        reason = "node Ready condition is missing, false or unknown"
    elif not schedulable:
        status_label = "unschedulable"
        reason = "node is cordoned or carries a blocking scheduling taint"
    elif used > 0:
        status_label = "busy"
        reason = f"{used}/{allocatable} GPUs already in use"
    elif not resource_ready:
        status_label = "resource_pressure"
        reason = "; ".join(shortfalls)
    elif cordoned:
        status_label = "cordoned"
        reason = "free Ready node; cordon permitted only for explicit targeted validation"
    else:
        status_label = "ready"
        reason = "free and schedulable"

    return NodeStatus(
        name=node_name,
        found=True,
        is_gpu_node=is_gpu_node,
        schedulable=schedulable,
        resource_ready=resource_ready,
        capacity=capacity,
        allocatable=allocatable,
        used=used,
        free=free,
        fully_free=fully_free,
        reason=reason,
        cordoned=cordoned,
        ready=ready,
        status_label=status_label,
    )


def describe_node(
    node_name: str,
    client: KubectlClient | None = None,
    config: CvalConfig | None = None,
    *,
    allow_cordoned: bool = False,
) -> NodeStatus:
    """Describe one node's free/schedulable state from live read-only calls."""

    active_config = config or load_config()
    kubectl = client or KubectlClient(config=active_config)
    node = kubectl.get_node_json(node_name)
    metadata = _mapping(node.get("metadata"))
    observed_name = metadata.get("name")
    if observed_name != node_name:
        raise ValueError(f"Targeted node response identity mismatch for {node_name!r}")
    return describe_node_from_outputs(
        node_name,
        kubectl.get_pods_for_node_json(node_name),
        "",
        nodes_json={"items": [node]},
        config=active_config,
        allow_cordoned=allow_cordoned,
    )


def _container_resource_request(container: object, resource_name: str, parser) -> int:
    """Read one resource request from a container spec."""

    if not isinstance(container, Mapping):
        raise ValueError("Malformed container resource evidence")
    container_map = _mapping(container)
    resources = _mapping(container_map.get("resources"))
    requests = _mapping(resources.get("requests"))
    if resource_name not in requests and resource_name in _mapping(resources.get("limits")):
        raise ValueError(f"Observed {resource_name} limit without an explicit request")
    return parser(requests.get(resource_name, 0))


def _mapping(value: object) -> Mapping[str, object]:
    """Allow omitted optional objects, but never hide malformed evidence."""

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("Expected a Kubernetes object")
    return value


def _list(value: object) -> list[object]:
    """Allow omitted optional lists, but never hide malformed evidence."""

    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("Expected a Kubernetes list")
    return value