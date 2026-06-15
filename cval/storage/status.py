"""Read and format c-val latest validation status.

Status reads are intentionally opened through SQLite `mode=ro` so operators and
agents can inspect validation history without mutating DB tables or views.
"""

from __future__ import annotations

import datetime
import json

from cval.config import load_config
from cval.k8s.client import KubectlClient
from cval.models import LatestStatusRow


_CONFIG = load_config()
DEFAULT_NAMESPACE = _CONFIG.cluster.namespace
DEFAULT_PVC_ACCESS_POD = _CONFIG.cluster.pvc_access_pod
DEFAULT_DB_PATH = _CONFIG.storage.validation_db_path


def parse_latest_status_tsv(output: str) -> dict[str, int]:
    """Parse legacy TSV latest-status output into node -> newest timestamp."""

    latest_by_node: dict[str, int] = {}
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("node\ttest\t"):
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        node_name = parts[0]
        timestamp_text = parts[2]
        timestamp = int(timestamp_text) if timestamp_text.isdigit() else 0
        latest_by_node[node_name] = max(latest_by_node.get(node_name, 0), timestamp)
    return latest_by_node


def parse_latest_status_rows_json(output: str) -> list[LatestStatusRow]:
    """Parse JSON rows emitted by the read-only status helper."""

    data = json.loads(output or "[]")
    if not isinstance(data, list):
        raise ValueError("latest status JSON must be a list")
    rows: list[LatestStatusRow] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        timestamp = item.get("latest_timestamp")
        rows.append(
            LatestStatusRow(
                node=str(item.get("node", "")),
                test=str(item.get("test", "")),
                latest_timestamp=int(timestamp) if timestamp not in {None, ""} else None,
                result=str(item.get("result", "")),
            )
        )
    return rows


def latest_status_rows_to_node_map(rows: list[LatestStatusRow]) -> dict[str, int]:
    """Collapse per-test rows into node -> newest timestamp for scheduling."""

    latest_by_node: dict[str, int] = {}
    for row in rows:
        timestamp = row.latest_timestamp or 0
        latest_by_node[row.node] = max(latest_by_node.get(row.node, 0), timestamp)
    return latest_by_node


def latest_status_rows_to_tsv(rows: list[LatestStatusRow]) -> str:
    """Format latest-status rows as the TSV shape used by older scripts."""

    lines = ["node\ttest\tlatest_timestamp_num\tlatest_timestamp\tresult"]
    for row in rows:
        timestamp = row.latest_timestamp
        timestamp_text = "" if timestamp is None else str(timestamp)
        timestamp_iso = "" if timestamp is None else _timestamp_to_iso(timestamp)
        lines.append(f"{row.node}\t{row.test}\t{timestamp_text}\t{timestamp_iso}\t{row.result}")
    return "\n".join(lines)


def get_latest_status_rows(
    client: KubectlClient | None = None,
    pod: str = DEFAULT_PVC_ACCESS_POD,
    namespace: str = DEFAULT_NAMESPACE,
    db_path: str = DEFAULT_DB_PATH,
) -> list[LatestStatusRow]:
    """Read latest status rows from the PVC access pod using SQLite read-only mode."""

    kubectl = client or KubectlClient()
    status_pod = resolve_status_pod(kubectl, namespace, pod)
    code = r'''
import json
import sqlite3
import sys

db_path = sys.argv[1]
rows_out = []
try:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT node, test, latest_timestamp, result FROM latest_status ORDER BY node, test"
    ).fetchall()
    for row in rows:
        rows_out.append(
            {
                "node": row["node"],
                "test": row["test"],
                "latest_timestamp": row["latest_timestamp"],
                "result": row["result"],
            }
        )
except Exception as exc:
    print(f"Error reading latest_status from {db_path}: {exc}", file=sys.stderr)
    sys.exit(1)

print(json.dumps(rows_out))
'''
    result = kubectl.run(
        ["exec", "-n", namespace, status_pod, "--", "python3", "-c", code, db_path]
    )
    return parse_latest_status_rows_json(result.stdout)


def resolve_status_pod(kubectl: KubectlClient, namespace: str, pod: str) -> str:
    """Resolve the configured status pod or the pod created by its access job."""

    direct_candidates = (pod, f"{pod}-server-0")
    for candidate in direct_candidates:
        if _pod_is_running(kubectl, namespace, candidate):
            return candidate

    selectors = (
        f"volcano.sh/job-name={pod}",
        f"job-name={pod}",
        f"app.kubernetes.io/name={pod}",
    )
    for selector in selectors:
        selected = _running_pod_for_selector(kubectl, namespace, selector)
        if selected:
            return selected

    raise RuntimeError(
        f"Could not find a running status pod for {pod!r} in namespace {namespace!r}"
    )


def _pod_is_running(kubectl: KubectlClient, namespace: str, pod: str) -> bool:
    result = kubectl.run(
        ["get", "pod", "-n", namespace, pod, "-o", "json"],
        check=False,
    )
    if result.returncode != 0:
        return False
    payload = json.loads(result.stdout or "{}")
    return payload.get("status", {}).get("phase") == "Running"


def _running_pod_for_selector(
    kubectl: KubectlClient,
    namespace: str,
    selector: str,
) -> str:
    result = kubectl.run(
        ["get", "pods", "-n", namespace, "-l", selector, "-o", "json"],
        check=False,
    )
    if result.returncode != 0:
        return ""
    payload = json.loads(result.stdout or "{}")
    for item in payload.get("items", []):
        if item.get("status", {}).get("phase") == "Running":
            return str(item.get("metadata", {}).get("name", ""))
    return ""


def _timestamp_to_iso(timestamp: int) -> str:
    """Render an epoch timestamp as UTC ISO-8601 with `Z` suffix."""

    return (
        datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )