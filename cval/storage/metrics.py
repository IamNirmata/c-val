"""Fetch latest per-node metric snapshots from the PVC access pod.

These helpers query test-nccl.db and test-storage.db in read-only mode via a
kubectl exec stdin call. They return only the most recent row per node so that
``cval results`` can join metric columns into the export CSV.

Exact status timestamps can be supplied instead of selecting latest metrics.
Read failures never become empty observations.
"""

from __future__ import annotations

import json

from cval.config import load_config
from cval.k8s.client import KubectlClient
from cval.models import NcclHealthMetric, NcclMetrics, StorageMetrics
from cval.storage.ingest import NCCL_IB_PORT_COLUMNS
from cval.storage.sqlite_uri import sqlite_readonly_script_prelude
from cval.storage.status import resolve_status_pod


STORAGE_METRIC_FIELDS = (
    "iodepth_read_1file_iops",
    "iodepth_read_1file_bw",
    "iodepth_write_1file_iops",
    "iodepth_write_1file_bw",
    "numjobs_read_nfiles_iops",
    "numjobs_read_nfiles_bw",
    "numjobs_write_nfiles_iops",
    "numjobs_write_nfiles_bw",
    "randread_iops",
    "randread_bw",
    "randwrite_iops",
    "randwrite_bw",
)

def _fetch_metrics(kind, client, pod, namespace, db_path, config, timestamps):
    active = config or load_config()
    kubectl = client or KubectlClient(config=active)
    namespace = namespace or active.cluster.namespace
    reader = resolve_status_pod(kubectl, namespace, pod or active.cluster.pvc_access_pod)
    table = "storage_performance" if kind == "storage" else "IB_HEALTH"
    columns = (
        "node,timestamp," + ",".join(STORAGE_METRIC_FIELDS) if kind == "storage" else
        "Node AS node,timestamp,BUS_BW AS busbw,LATENCY AS latency" if kind == "nccl" else
        "Node AS node,timestamp,la_timestamp,iterations,image_name,cuda,pytorch,samples,BUS_BW AS bus_bw,LATENCY AS latency," + ",".join(NCCL_IB_PORT_COLUMNS)
    )
    if timestamps is not None and any(not isinstance(node, str) or not node or type(timestamp) is not int or timestamp <= 0 for node, timestamp in timestamps.items()):
        raise ValueError("Exact metric selection requires node names and positive timestamps")
    path = db_path or (active.storage.storage_db_path if kind == "storage" else active.storage.nccl_db_path)
    code = f"CVAL_SQLITE_TIMEOUT = {active.sqlite_retry.timeout_seconds!r}\n" + sqlite_readonly_script_prelude() + r'''
import json,sys,time
path,table,columns,selection,limit = sys.argv[1:]
deadline = time.monotonic() + float(limit)
connection = connect_sqlite_readonly(path)
try:
    connection.row_factory = sqlite3.Row
    connection.set_progress_handler(lambda: int(time.monotonic() >= deadline),1000)
    selection = json.loads(selection)
    if selection is None:
        rows = [dict(row) for row in connection.execute(f"SELECT {columns} FROM {table} WHERE (node,timestamp) IN (SELECT node,MAX(timestamp) FROM {table} GROUP BY node) ORDER BY node")]
    else:
        rows = []
        for node,timestamp in selection.items():
            rows.extend(dict(row) for row in connection.execute(f"SELECT {columns} FROM {table} WHERE node=? AND timestamp=?",(node,timestamp)))
    print(json.dumps(rows,allow_nan=False))
finally:
    connection.close()
'''
    result = kubectl.run(["exec", "-i", "-n", namespace, reader, "--", "python3", "-", str(path), table, columns,
                          json.dumps(timestamps), str(active.cluster.kubectl_timeout_seconds)], input_text=code, check=False)
    if result.returncode:
        state = "forbidden" if "forbidden" in result.stderr.lower() or "permission denied" in result.stderr.lower() else "unavailable"
        raise RuntimeError(f"{state}: {kind} metric read failed: {result.stderr.strip()}")
    return result.stdout


def _metric_rows(output):
    rows = json.loads(output)
    if not isinstance(rows, list):
        raise ValueError("Metric response must be a JSON array")
    seen = {}
    for item in rows:
        if not isinstance(item, dict) or not isinstance(item.get("node"), str) or not item["node"]:
            raise ValueError("Invalid metric row identity")
        if item["node"] in seen and seen[item["node"]] != item:
            raise ValueError("Conflicting metric rows for the requested identity")
        seen[item["node"]] = item
    return seen.values()


def get_latest_nccl_metrics(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
    config=None,
    *,
    timestamps: dict[str, int] | None = None,
) -> dict[str, NcclMetrics]:
    """Read latest metrics, or exact status timestamps when supplied."""
    return _parse_nccl_json(_fetch_metrics("nccl", client, pod, namespace, db_path, config, timestamps))


def get_latest_storage_metrics(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
    config=None,
    *,
    timestamps: dict[str, int] | None = None,
) -> dict[str, StorageMetrics]:
    """Read latest metrics, or exact status timestamps when supplied."""
    return _parse_storage_json(_fetch_metrics("storage", client, pod, namespace, db_path, config, timestamps))


def _parse_nccl_json(output: str) -> dict[str, NcclMetrics]:
    """Parse JSON list of {node, busbw, latency} into a node→NcclMetrics map."""
    data = _metric_rows(output)
    metrics: dict[str, NcclMetrics] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        node = str(item.get("node", "")).strip()
        if not node:
            continue
        busbw = item.get("busbw")
        latency = item.get("latency")
        metrics[node] = NcclMetrics(
            busbw=float(busbw) if busbw is not None else None,
            latency=float(latency) if latency is not None else None,
            timestamp=int(item["timestamp"]) if item.get("timestamp") is not None else None,
        )
    return metrics


def _parse_storage_json(output: str) -> dict[str, StorageMetrics]:
    """Parse JSON list of storage metric rows into a node→StorageMetrics map."""
    data = _metric_rows(output)
    metrics: dict[str, StorageMetrics] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        node = str(item.get("node", "")).strip()
        if not node:
            continue

        def _f(key: str) -> float | None:
            val = item.get(key)
            return float(val) if val is not None else None

        metrics[node] = StorageMetrics(
            iodepth_read_1file_iops=_f("iodepth_read_1file_iops"),
            iodepth_read_1file_bw=_f("iodepth_read_1file_bw"),
            iodepth_write_1file_iops=_f("iodepth_write_1file_iops"),
            iodepth_write_1file_bw=_f("iodepth_write_1file_bw"),
            numjobs_read_nfiles_iops=_f("numjobs_read_nfiles_iops"),
            numjobs_read_nfiles_bw=_f("numjobs_read_nfiles_bw"),
            numjobs_write_nfiles_iops=_f("numjobs_write_nfiles_iops"),
            numjobs_write_nfiles_bw=_f("numjobs_write_nfiles_bw"),
            randread_iops=_f("randread_iops"),
            randread_bw=_f("randread_bw"),
            randwrite_iops=_f("randwrite_iops"),
            randwrite_bw=_f("randwrite_bw"),
            timestamp=int(item["timestamp"]) if item.get("timestamp") is not None else None,
        )
    return metrics


def get_latest_nccl_health_metrics(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
    config=None,
    *,
    timestamps: dict[str, int] | None = None,
) -> dict[str, NcclHealthMetric]:
    """Read latest wide metrics, or exact status timestamps when supplied."""
    return _parse_nccl_health_json(_fetch_metrics("health", client, pod, namespace, db_path, config, timestamps))


def _parse_nccl_health_json(output: str) -> dict[str, NcclHealthMetric]:
    """Parse wide ``IB_HEALTH`` JSON rows into a node map."""
    data = _metric_rows(output)
    metrics: dict[str, NcclHealthMetric] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        node = str(item.get("node", "")).strip()
        if not node:
            continue

        def _f(key: str) -> float | None:
            val = item.get(key)
            return float(val) if val is not None else None

        def _i(key: str) -> int | None:
            val = item.get(key)
            return int(val) if val not in {None, ""} else None

        metrics[node] = NcclHealthMetric(
            node=node,
            timestamp=_i("timestamp"),
            la_timestamp=str(item.get("la_timestamp", "")),
            iterations=_i("iterations"),
            image_name=str(item.get("image_name", "")),
            cuda=str(item.get("cuda", "")),
            pytorch=str(item.get("pytorch", "")),
            samples=_i("samples"),
            bus_bw=_f("bus_bw"),
            latency=_f("latency"),
            port_max_gbps={column: _f(column) for column in NCCL_IB_PORT_COLUMNS},
        )
    return metrics
