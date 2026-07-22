"""Fetch latest per-node metric snapshots from the PVC access pod.

These helpers query test-nccl.db and test-storage.db in read-only mode via a
kubectl exec stdin call. They return only the most recent row per node so that
``cval results`` can join metric columns into the export CSV.

Errors (missing DB, pod unavailable) are caught and returned as an empty dict
so the main export still succeeds—just without metric columns.
"""

from __future__ import annotations

import json
import logging

from cval.config import load_config
from cval.k8s.client import KubectlClient
from cval.models import NcclHealthMetric, NcclMetrics, StorageMetrics
from cval.storage.ingest import NCCL_IB_PORT_COLUMNS
from cval.storage.status import resolve_status_pod

logger = logging.getLogger(__name__)

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

_NCCL_FETCH_SCRIPT = """\
import json, sqlite3, sys
db_path = {db_path!r}
rows_out = []
try:
    conn = sqlite3.connect(f"file:{{db_path}}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT Node AS node, BUS_BW AS busbw, LATENCY AS latency "
        "FROM IB_HEALTH "
        "WHERE (Node, timestamp) IN "
        "  (SELECT Node, MAX(timestamp) FROM IB_HEALTH GROUP BY Node) "
        "ORDER BY Node"
    ).fetchall()
    for row in rows:
        rows_out.append({{"node": row["node"], "busbw": row["busbw"], "latency": row["latency"]}})
except Exception as exc:
    print(f"nccl metrics error: {{exc}}", file=sys.stderr)
print(json.dumps(rows_out))
"""

_STORAGE_FETCH_SCRIPT = """\
import json, sqlite3, sys
db_path = {db_path!r}
cols = {cols!r}
rows_out = []
try:
    conn = sqlite3.connect(f"file:{{db_path}}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    col_list = ", ".join(cols)
    rows = conn.execute(
        f"SELECT node, {{col_list}} FROM storage_performance "
        "WHERE (node, timestamp) IN "
        "  (SELECT node, MAX(timestamp) FROM storage_performance GROUP BY node) "
        "ORDER BY node"
    ).fetchall()
    for row in rows:
        entry = {{"node": row["node"]}}
        for col in cols:
            entry[col] = row[col]
        rows_out.append(entry)
except Exception as exc:
    print(f"storage metrics error: {{exc}}", file=sys.stderr)
print(json.dumps(rows_out))
"""

_NCCL_HEALTH_FETCH_SCRIPT = """\
import json, sqlite3, sys
db_path = {db_path!r}
port_columns = {port_columns!r}
rows_out = []
try:
    conn = sqlite3.connect(f"file:{{db_path}}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    selected = ", ".join(port_columns)
    rows = conn.execute(
        "SELECT Node, timestamp, la_timestamp, iterations, image_name, cuda, pytorch, "
        "samples, BUS_BW, LATENCY, " + selected + " FROM IB_HEALTH "
        "WHERE (Node, timestamp) IN "
        "  (SELECT Node, MAX(timestamp) FROM IB_HEALTH GROUP BY Node) "
        "ORDER BY Node"
    ).fetchall()
    for row in rows:
        entry = {{
            "node": row["Node"],
            "timestamp": row["timestamp"],
            "la_timestamp": row["la_timestamp"],
            "iterations": row["iterations"],
            "image_name": row["image_name"],
            "cuda": row["cuda"],
            "pytorch": row["pytorch"],
            "samples": row["samples"],
            "bus_bw": row["BUS_BW"],
            "latency": row["LATENCY"],
        }}
        for column in port_columns:
            entry[column] = row[column]
        rows_out.append(entry)
except Exception as exc:
    print(f"IB_HEALTH metrics error: {{exc}}", file=sys.stderr)
print(json.dumps(rows_out))
"""


def get_latest_nccl_metrics(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
) -> dict[str, NcclMetrics]:
    """Return the latest NCCL busbw/latency per node from test-nccl.db.

    Returns an empty dict on any error so callers can proceed without metrics.
    """
    config = load_config()
    pod = pod or config.cluster.pvc_access_pod
    namespace = namespace or config.cluster.namespace
    db_path = db_path or config.storage.nccl_db_path
    kubectl = client or KubectlClient()
    try:
        status_pod = resolve_status_pod(kubectl, namespace, pod)
        code = _NCCL_FETCH_SCRIPT.format(db_path=str(db_path))
        result = kubectl.run(
            ["exec", "-i", "-n", namespace, status_pod, "--", "python3", "-"],
            input_text=code,
        )
        return _parse_nccl_json(result.stdout)
    except Exception as exc:
        logger.warning("Could not fetch NCCL metrics: %s", exc)
        return {}


def get_latest_storage_metrics(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
) -> dict[str, StorageMetrics]:
    """Return the latest FIO storage metrics per node from test-storage.db.

    Returns an empty dict on any error so callers can proceed without metrics.
    """
    config = load_config()
    pod = pod or config.cluster.pvc_access_pod
    namespace = namespace or config.cluster.namespace
    db_path = db_path or config.storage.storage_db_path
    kubectl = client or KubectlClient()
    try:
        status_pod = resolve_status_pod(kubectl, namespace, pod)
        code = _STORAGE_FETCH_SCRIPT.format(
            db_path=str(db_path),
            cols=list(STORAGE_METRIC_FIELDS),
        )
        result = kubectl.run(
            ["exec", "-i", "-n", namespace, status_pod, "--", "python3", "-"],
            input_text=code,
        )
        return _parse_storage_json(result.stdout)
    except Exception as exc:
        logger.warning("Could not fetch storage metrics: %s", exc)
        return {}


def _parse_nccl_json(output: str) -> dict[str, NcclMetrics]:
    """Parse JSON list of {node, busbw, latency} into a node→NcclMetrics map."""
    try:
        data = json.loads(output or "[]")
    except json.JSONDecodeError:
        return {}
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
        )
    return metrics


def _parse_storage_json(output: str) -> dict[str, StorageMetrics]:
    """Parse JSON list of storage metric rows into a node→StorageMetrics map."""
    try:
        data = json.loads(output or "[]")
    except json.JSONDecodeError:
        return {}
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
        )
    return metrics


def get_latest_nccl_health_metrics(
    client: KubectlClient | None = None,
    pod: str | None = None,
    namespace: str | None = None,
    db_path: str | None = None,
) -> dict[str, NcclHealthMetric]:
    """Return the latest consolidated ``IB_HEALTH`` row per node.

    Returns an empty dict on any error so callers can proceed without metrics.
    """
    config = load_config()
    pod = pod or config.cluster.pvc_access_pod
    namespace = namespace or config.cluster.namespace
    db_path = db_path or config.storage.nccl_db_path
    kubectl = client or KubectlClient()
    try:
        status_pod = resolve_status_pod(kubectl, namespace, pod)
        code = _NCCL_HEALTH_FETCH_SCRIPT.format(
            db_path=str(db_path), port_columns=list(NCCL_IB_PORT_COLUMNS)
        )
        result = kubectl.run(
            ["exec", "-i", "-n", namespace, status_pod, "--", "python3", "-"],
            input_text=code,
        )
        return _parse_nccl_health_json(result.stdout)
    except Exception as exc:
        logger.warning("Could not fetch IB_HEALTH metrics: %s", exc)
        return {}


def _parse_nccl_health_json(output: str) -> dict[str, NcclHealthMetric]:
    """Parse wide ``IB_HEALTH`` JSON rows into a node map."""
    try:
        data = json.loads(output or "[]")
    except json.JSONDecodeError:
        return {}
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
