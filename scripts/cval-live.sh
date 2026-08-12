#!/usr/bin/env bash
set -euo pipefail

# Start/stop a tmux-backed c-val live runner.
#
# The immutable operation mode is explicit and audit-first:
# - audit (default): inventory, read latest_status, prioritize, check nodes in
#   order, render, and select slots without submitting, monitoring, or pruning.
# - submit: additionally requires CVAL_LIVE_CONFIRM=submit. The CLI submission
#   remains independently gated by --submit --confirm submit.
# - pruning is disabled unless submit mode also has
#   CVAL_PRUNE_CONFIRM=delete-pending.

COMMAND=${1:-start}

# Fail the independent submit startup gate before config helpers, worktree
# operations, or any Kubernetes-capable command is invoked. Non-operational
# help/status/stop/attach commands remain available regardless of mode env.
case "$COMMAND" in
    start|run-once|run-loop)
        case "${CVAL_LIVE_MODE:-audit}" in
            audit) ;;
            submit)
                if [[ "${CVAL_LIVE_CONFIRM:-}" != "submit" ]]; then
                    echo "submit mode requires exact CVAL_LIVE_CONFIRM=submit" >&2
                    exit 2
                fi
                ;;
            *)
                echo "CVAL_LIVE_MODE must be exactly audit or submit (got: ${CVAL_LIVE_MODE:-})" >&2
                exit 2
                ;;
        esac
        ;;
esac

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
SOURCE_REPO=${CVAL_SOURCE_REPO:-$(cd "$SCRIPT_DIR/.." && pwd)}
CONFIG_PATH=${CVAL_CONFIG:-$SOURCE_REPO/config/cval.toml}
SESSION_NAME=${CVAL_TMUX_SESSION:-cval-live}
LOG_DIR=${CVAL_LIVE_LOG_DIR:-$SOURCE_REPO/run-logs/cval-live}
LIVE_MODE=${CVAL_LIVE_MODE:-audit}
LIVE_CONFIRM=${CVAL_LIVE_CONFIRM:-}
PRUNE_CONFIRM=${CVAL_PRUNE_CONFIRM:-}

config_value() {
    local section="$1"
    local key="$2"
    local default_value="$3"
    python - "$CONFIG_PATH" "$section" "$key" "$default_value" <<'PY'
import sys
import tomllib
from pathlib import Path

path, section, key, default = sys.argv[1:]
try:
    data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    value = data.get(section, {}).get(key, default)
except FileNotFoundError:
    value = default
print(value)
PY
}

validate_operational_config() {
    python - "$CONFIG_PATH" <<'PY'
import sys
import tomllib
from pathlib import Path

path = Path(sys.argv[1])
with path.open("rb") as handle:
    data = tomllib.load(handle)
if not isinstance(data, dict):
    raise ValueError("c-val config must be a TOML table")
PY
}

load_operational_settings() {
    validate_operational_config
    RUNNER_WORKTREE=${CVAL_RUNNER_WORKTREE:-/tmp/cval-live-worktree}
    LOOP_SLEEP_SECONDS=${CVAL_LOOP_SLEEP_SECONDS:-300}
    PLAN_LIMIT=${CVAL_PLAN_LIMIT:-50}
    KUBECTL_TIMEOUT_SECONDS=${CVAL_KUBECTL_TIMEOUT_SECONDS:-120}
    EXPLICIT_GIT_REF=${CVAL_GIT_REF:-}
    BATCH_SIZE=${CVAL_BATCH_SIZE:-$(config_value scheduling batch_size 2)}
    DAYS_THRESHOLD=${CVAL_DAYS_THRESHOLD:-$(config_value scheduling days_threshold 7)}
    NODE_COOLDOWN_SECONDS=${CVAL_NODE_COOLDOWN_SECONDS:-$(config_value scheduling node_cooldown_seconds 14400)}
    NODE_COOLDOWN_STATE_FILE=${CVAL_NODE_COOLDOWN_STATE_FILE:-$LOG_DIR/node_cool_down.csv}
    NODE_COOLDOWN_HELPER=${CVAL_NODE_COOLDOWN_HELPER:-$SCRIPT_DIR/cval-node-cooldown.py}
    WATCH_TIMEOUT_SECONDS=${CVAL_WATCH_TIMEOUT_SECONDS:-$(config_value monitoring timeout_seconds 3600)}
    WATCH_POLL_SECONDS=${CVAL_WATCH_POLL_SECONDS:-$(config_value monitoring poll_interval_seconds 60)}
    PENDING_START_TIMEOUT_SECONDS=${CVAL_PENDING_START_TIMEOUT_SECONDS:-$(config_value monitoring pending_start_timeout_seconds 480)}
    NAMESPACE=${CVAL_NAMESPACE:-$(config_value cluster namespace gcr-admin)}
    JOB_PREFIX=${CVAL_JOB_PREFIX:-$(config_value job job_prefix cval)}
}

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  start      Start tmux session '$SESSION_NAME' running the live loop
  stop       Stop the tmux session; does not delete Kubernetes jobs
  attach     Attach to the tmux session
  status     Show session status and latest log tail
  run-once   Run one live cycle in the current shell
  run-loop   Internal: run cycles forever

Environment overrides:
    CVAL_LIVE_MODE=audit                           # audit (default) or submit
    CVAL_LIVE_CONFIRM=submit                       # required only for submit mode
    CVAL_PRUNE_CONFIRM=delete-pending              # optional; submit mode only
  CVAL_CONFIG=$CONFIG_PATH
    CVAL_BATCH_SIZE=<positive-integer>
    CVAL_DAYS_THRESHOLD=<days>
    CVAL_NODE_COOLDOWN_SECONDS=14400              # 4 hours; 0 disables
    CVAL_NODE_COOLDOWN_STATE_FILE=$LOG_DIR/node_cool_down.csv
    CVAL_PENDING_START_TIMEOUT_SECONDS=<seconds>
    CVAL_GIT_REF=<40-hex-commit>                   # explicit session pin only
    CVAL_KUBECTL_TIMEOUT_SECONDS=120
    CVAL_PLAN_LIMIT=50
  CVAL_TMUX_SESSION=$SESSION_NAME
    CVAL_LIVE_LOG_DIR=$LOG_DIR
    CVAL_LOOP_SLEEP_SECONDS=300
EOF
}

log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "Required command not found: $1" >&2
        exit 1
    }
}

acquire_live_lock() {
    mkdir -p "$LOG_DIR"
    local lock_file="$LOG_DIR/.cval-live.lock"
    exec {LIVE_LOCK_FD}>"$lock_file"
    if ! flock -n "$LIVE_LOCK_FD"; then
        echo "Another cval-live operational loop holds $lock_file" >&2
        return 2
    fi
}

validate_runtime_settings() {
    case "$LIVE_MODE" in
        audit|submit) ;;
        *)
            echo "CVAL_LIVE_MODE must be exactly audit or submit (got: $LIVE_MODE)" >&2
            return 2
            ;;
    esac
    local name value
    for name in CVAL_BATCH_SIZE CVAL_PLAN_LIMIT CVAL_KUBECTL_TIMEOUT_SECONDS; do
        case "$name" in
            CVAL_BATCH_SIZE) value="$BATCH_SIZE" ;;
            CVAL_PLAN_LIMIT) value="$PLAN_LIMIT" ;;
            CVAL_KUBECTL_TIMEOUT_SECONDS) value="$KUBECTL_TIMEOUT_SECONDS" ;;
        esac
        if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
            echo "$name must be a positive integer (got: $value)" >&2
            return 2
        fi
    done
    if [[ ! "$PENDING_START_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
        echo "CVAL_PENDING_START_TIMEOUT_SECONDS must be a positive integer (got: $PENDING_START_TIMEOUT_SECONDS)" >&2
        return 2
    fi
    if [[ ! "$NODE_COOLDOWN_SECONDS" =~ ^[0-9]+$ ]]; then
        echo "CVAL_NODE_COOLDOWN_SECONDS must be a non-negative integer (got: $NODE_COOLDOWN_SECONDS)" >&2
        return 2
    fi
    if [[ ! -f "$NODE_COOLDOWN_HELPER" ]]; then
        echo "Node cooldown helper not found: $NODE_COOLDOWN_HELPER" >&2
        return 2
    fi
}

require_submit_startup_gate() {
    if [[ "$LIVE_MODE" == "submit" && "$LIVE_CONFIRM" != "submit" ]]; then
        echo "submit mode requires exact CVAL_LIVE_CONFIRM=submit" >&2
        return 2
    fi
}

pruning_enabled() {
    [[ "$LIVE_MODE" == "submit" && "$PRUNE_CONFIRM" == "delete-pending" ]]
}

resolve_git_ref() {
    local requested_ref source resolved rc
    if [[ -n "$EXPLICIT_GIT_REF" ]]; then
        requested_ref="$EXPLICIT_GIT_REF"
        source="explicit"
    else
        rc=0
        git -C "$SOURCE_REPO" fetch --quiet origin main || rc=$?
        if (( rc != 0 )); then
            log "git fetch failed source=origin-main exit_code=$rc" >&2
            return "$rc"
        fi
        requested_ref="origin/main"
        source="origin-main"
    fi
    rc=0
    resolved=$(git -C "$SOURCE_REPO" rev-parse --verify "${requested_ref}^{commit}") || rc=$?
    if (( rc != 0 )); then
        log "git ref resolution failed source=$source requested_ref=$requested_ref exit_code=$rc" >&2
        return "$rc"
    fi
    if [[ ! "$resolved" =~ ^[0-9a-fA-F]{40}$ ]]; then
        log "git ref resolution rejected non-commit source=$source value=$resolved" >&2
        return 1
    fi
    RESOLVED_GIT_REF="$resolved"
    RESOLVED_GIT_REF_SOURCE="$source"
    log "resolved git ref source=$source sha=$resolved"
}

ensure_runner_worktree() {
    local git_ref="$1"
    local rc=0
    mkdir -p "$(dirname "$RUNNER_WORKTREE")"
    if [[ ! -d "$RUNNER_WORKTREE" ]] || ! git -C "$RUNNER_WORKTREE" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        rm -rf "$RUNNER_WORKTREE"
        # /tmp may be cleaned while Git still retains the worktree registration.
        # Prune stale registrations before recreating the detached runner.
        git -C "$SOURCE_REPO" worktree prune || rc=$?
        if (( rc != 0 )); then
            log "git worktree prune failed exit_code=$rc" >&2
            return "$rc"
        fi
        git -C "$SOURCE_REPO" worktree add --detach "$RUNNER_WORKTREE" "$git_ref" || rc=$?
        if (( rc != 0 )); then
            log "git worktree add failed git_ref=$git_ref exit_code=$rc" >&2
            return "$rc"
        fi
    else
        git -C "$RUNNER_WORKTREE" fetch --quiet origin main || rc=$?
        if (( rc != 0 )); then
            log "runner worktree git fetch failed exit_code=$rc" >&2
            return "$rc"
        fi
        git -C "$RUNNER_WORKTREE" checkout --quiet --detach "$git_ref" || rc=$?
        if (( rc != 0 )); then
            log "runner worktree checkout failed git_ref=$git_ref exit_code=$rc" >&2
            return "$rc"
        fi
    fi
}

enter_runner_worktree() {
    local operation="$1"
    local rc=0
    pushd "$RUNNER_WORKTREE" >/dev/null 2>&1 || rc=$?
    if (( rc != 0 )); then
        log "runner worktree entry failed operation=$operation path=$RUNNER_WORKTREE exit_code=$rc" >&2
        return 2
    fi
}

assert_runner_worktree() {
    local context="$1"
    local expected_sha="$2"
    local current_physical runner_physical inside_worktree head_sha rc

    if [[ ! "$expected_sha" =~ ^[0-9a-fA-F]{40}$ ]]; then
        log "runner worktree assertion failed context=$context reason=invalid-expected-sha" >&2
        return 2
    fi
    if [[ ! -d "$RUNNER_WORKTREE" ]]; then
        log "runner worktree assertion failed context=$context reason=missing-directory path=$RUNNER_WORKTREE" >&2
        return 2
    fi
    rc=0
    current_physical=$(pwd -P) || rc=$?
    if (( rc != 0 )) || [[ -z "$current_physical" ]]; then
        log "runner worktree assertion failed context=$context reason=invalid-current-directory" >&2
        return 2
    fi
    rc=0
    runner_physical=$(cd -P -- "$RUNNER_WORKTREE" 2>/dev/null && pwd -P) || rc=$?
    if (( rc != 0 )) || [[ -z "$runner_physical" ]]; then
        log "runner worktree assertion failed context=$context reason=unresolvable-runner-directory path=$RUNNER_WORKTREE" >&2
        return 2
    fi
    if [[ "$current_physical" != "$runner_physical" ]]; then
        log "runner worktree assertion failed context=$context reason=physical-cwd-mismatch current=$current_physical expected=$runner_physical" >&2
        return 2
    fi
    if [[ ! "." -ef "$RUNNER_WORKTREE" ]]; then
        log "runner worktree assertion failed context=$context reason=directory-identity-mismatch path=$RUNNER_WORKTREE" >&2
        return 2
    fi
    rc=0
    inside_worktree=$(git -C "$RUNNER_WORKTREE" rev-parse --is-inside-work-tree 2>/dev/null) || rc=$?
    if (( rc != 0 )) || [[ "$inside_worktree" != "true" ]]; then
        log "runner worktree assertion failed context=$context reason=not-git-worktree path=$RUNNER_WORKTREE" >&2
        return 2
    fi
    rc=0
    head_sha=$(git -C "$RUNNER_WORKTREE" rev-parse HEAD 2>/dev/null) || rc=$?
    if (( rc != 0 )) || [[ ! "$head_sha" =~ ^[0-9a-fA-F]{40}$ ]]; then
        log "runner worktree assertion failed context=$context reason=invalid-head path=$RUNNER_WORKTREE" >&2
        return 2
    fi
    if [[ "$head_sha" != "$expected_sha" ]]; then
        log "runner worktree assertion failed context=$context reason=head-mismatch expected=$expected_sha actual=$head_sha" >&2
        return 2
    fi
}

leave_runner_worktree() {
    local operation="$1"
    local primary_rc="${2:-0}"
    local popd_rc=0
    popd >/dev/null 2>&1 || popd_rc=$?
    if (( popd_rc != 0 )); then
        log "runner worktree exit failed operation=$operation path=$RUNNER_WORKTREE exit_code=$popd_rc" >&2
    fi
    if (( primary_rc != 0 )); then
        return "$primary_rc"
    fi
    if (( popd_rc != 0 )); then
        return 2
    fi
}

json_submitted_jobs_csv_from_dir() {
    local cycle_dir="$1"
    python - "$cycle_dir" <<'PY'
import json
import re
import sys
from pathlib import Path

cycle_dir = Path(sys.argv[1])
pruned_path = cycle_dir / "pruned-jobs.csv"
pruned = set()
if pruned_path.exists():
    import csv
    with pruned_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["job_name", "deleted_at"]:
            raise ValueError("pruned-jobs.csv has an invalid header")
        for row in reader:
            name = row.get("job_name", "")
            deleted_at = row.get("deleted_at", "")
            if not name or not deleted_at.isdigit() or name in pruned:
                raise ValueError("pruned-jobs.csv has an invalid row")
            pruned.add(name)
legacy_deleted_path = cycle_dir / "deleted-jobs.log"
if legacy_deleted_path.exists():
    patterns = (
        re.compile(
            r'^job\.batch\.volcano\.sh/([^ ]+) deleted'
            r'(?: from [a-z0-9.-]+ namespace)?$'
        ),
        re.compile(
            r'^job\.batch\.volcano\.sh "([^"]+)" deleted'
            r'(?: from [a-z0-9.-]+ namespace)?$'
        ),
    )
    for raw_line in legacy_deleted_path.read_text(encoding="utf-8").splitlines():
        matches = [pattern.fullmatch(raw_line) for pattern in patterns]
        match = next((value for value in matches if value is not None), None)
        if match is not None:
            pruned.add(match.group(1))
jobs = []
for path in sorted(cycle_dir.glob("submit*.json")):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        continue
    for job in data.get("jobs", []):
        if (
            job.get("submitted")
            and job.get("job_name") not in pruned
            and job.get("job_name") not in jobs
        ):
            jobs.append(job["job_name"])
print(",".join(jobs))
PY
}

json_submitted_job_tsv() {
    python - "$1" "$2" "$3" <<'PY'
import json
import sys

path, expected_node, expected_git_ref = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    payload = json.load(handle)
if not isinstance(payload, dict) or set(payload) != {"namespace", "submitted_count", "jobs"}:
    raise ValueError("submission JSON has an unexpected top-level shape")
if type(payload["submitted_count"]) is not int or payload["submitted_count"] != 1:
    raise ValueError("submission JSON must report exactly one submitted job")
jobs = payload["jobs"]
if not isinstance(jobs, list) or len(jobs) != 1 or not isinstance(jobs[0], dict):
    raise ValueError("submission JSON must contain exactly one job object")
job = jobs[0]
if job.get("submitted") is not True or job.get("action") != "submitted":
    raise ValueError("submission JSON does not confirm creation")
if job.get("node") != expected_node or job.get("git_ref") != expected_git_ref:
    raise ValueError("submission JSON identity does not match the requested job")
job_name = job.get("job_name")
if not isinstance(job_name, str) or not job_name:
    raise ValueError("submission JSON has an invalid job name")
print(f"{job_name}\t{expected_node}")
PY
}

apply_node_cooldown() {
    local cycle_dir="$1"
    local stem="$2"
    local observed_at
    observed_at=$(date +%s)
    local report="$cycle_dir/$stem-node-cooldown.json"
    local filtered
    if ! filtered=$(python "$NODE_COOLDOWN_HELPER" filter \
        --state-file "$NODE_COOLDOWN_STATE_FILE" \
        --nodes "$PLAN_FREE_NODES" \
        --now "$observed_at" \
        --cooldown-seconds "$NODE_COOLDOWN_SECONDS" \
        --report "$report"); then
        log "component=node-cooldown status=failed state=$NODE_COOLDOWN_STATE_FILE" >&2
        return 1
    fi
    PLAN_FREE_NODES="$filtered"
    PLAN_FREE_NODE_COUNT=$(csv_count "$PLAN_FREE_NODES")
    local excluded_count
    excluded_count=$(python - "$report" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    print(len(json.load(handle)["cooldown_excluded"]))
PY
)
    log "component=node-cooldown status=ok excluded_count=$excluded_count priority_candidate_count=$PLAN_FREE_NODE_COUNT period=${NODE_COOLDOWN_SECONDS}s"
}

record_node_submission() {
    local node="$1"
    local timestamp="$2"
    python "$NODE_COOLDOWN_HELPER" record \
        --state-file "$NODE_COOLDOWN_STATE_FILE" \
        --node "$node" \
        --timestamp "$timestamp"
}

repair_cycle_cooldowns() {
    local cycle_dir="$1"
    local rows
    if ! rows=$(python - "$cycle_dir" <<'PY'
import json
import re
import sys
from pathlib import Path

cycle_dir = Path(sys.argv[1])
for path in sorted(cycle_dir.glob("submit-*.json")):
    match = re.fullmatch(r"submit-([0-9]+)\.json", path.name)
    if match is None:
        continue
    timestamp = int(match.group(1))
    if timestamp <= 0:
        raise ValueError("submission artifact has an invalid timestamp")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
        raise ValueError("submission artifact has an invalid shape")
    for job in payload["jobs"]:
        if not isinstance(job, dict):
            raise ValueError("submission artifact has an invalid job row")
        if job.get("submitted") is not True:
            continue
        node = job.get("node")
        job_name = job.get("job_name")
        git_ref = job.get("git_ref")
        if (
            not isinstance(node, str)
            or not node
            or not isinstance(job_name, str)
            or not job_name
            or not isinstance(git_ref, str)
            or re.fullmatch(r"[0-9a-f]{40}", git_ref) is None
            or git_ref == "0" * 40
        ):
            raise ValueError("submission artifact has an invalid submitted job identity")
        print(f"{node}\t{timestamp}")
PY
); then
        log "cooldown repair failed for saved cycle $cycle_dir" >&2
        return 2
    fi
    while IFS=$'\t' read -r node timestamp; do
        [[ -n "$node" ]] || continue
        if ! record_node_submission "$node" "$timestamp"; then
            log "cooldown repair write failed for node $node" >&2
            return 2
        fi
    done <<<"$rows"
}

json_plan_summary_tsv() {
    python - "$1" "$2" "$3" <<'PY'
import json
import math
import sys

plan_path, expected_free_text, expected_batch_text = sys.argv[1:]
with open(plan_path, encoding="utf-8") as handle:
    data = json.load(handle)
if not isinstance(data, dict):
    raise ValueError("plan JSON must be an object")
expected_keys = {
    "batch_size",
    "days_threshold",
    "free_nodes_count",
    "queue_count",
    "planned_jobs",
}
if set(data) != expected_keys:
    raise ValueError("plan JSON has an unexpected top-level shape")
def positive_int(name, value):
    if type(value) is not int or value <= 0:
        raise ValueError(f"plan JSON {name} must be a positive integer")
    return value

def nonnegative_int(name, value):
    if type(value) is not int or value < 0:
        raise ValueError(f"plan JSON {name} must be a non-negative integer")
    return value

batch_size = positive_int("batch_size", data["batch_size"])
free_nodes_count = nonnegative_int("free_nodes_count", data["free_nodes_count"])
queue_count = nonnegative_int("queue_count", data["queue_count"])
if batch_size != int(expected_batch_text):
    raise ValueError("plan JSON batch_size does not match the requested plan limit")
if free_nodes_count != int(expected_free_text):
    raise ValueError("plan JSON free_nodes_count does not match the discovery snapshot")
days_threshold = data["days_threshold"]
if (
    isinstance(days_threshold, bool)
    or not isinstance(days_threshold, (int, float))
    or not math.isfinite(days_threshold)
    or days_threshold < 0
):
    raise ValueError("plan JSON days_threshold must be a non-negative number")
planned_jobs = data["planned_jobs"]
if not isinstance(planned_jobs, list):
    raise ValueError("plan JSON planned_jobs must be a list")
if queue_count > free_nodes_count:
    raise ValueError("plan JSON queue_count cannot exceed free_nodes_count")
if len(planned_jobs) != min(batch_size, queue_count):
    raise ValueError("plan JSON planned job count is inconsistent with batch_size and queue_count")
nodes = []
job_names = []
expected_job_keys = {
    "priority",
    "node",
    "reason",
    "last_tested_timestamp",
    "age_days",
    "job_name",
    "git_ref",
}
for job in planned_jobs:
    if not isinstance(job, dict) or set(job) != expected_job_keys:
        raise ValueError("plan JSON contains a planned job with an unexpected shape")
    node = job["node"]
    job_name = job["job_name"]
    git_ref = job["git_ref"]
    if not isinstance(node, str) or not node.strip():
        raise ValueError("plan JSON contains an invalid planned job node")
    if not isinstance(job_name, str) or not job_name.strip():
        raise ValueError("plan JSON contains an invalid planned job name")
    if (
        not isinstance(git_ref, str)
        or len(git_ref) != 40
        or any(character not in "0123456789abcdef" for character in git_ref)
        or git_ref == "0" * 40
    ):
        raise ValueError("plan JSON contains an invalid exact git_ref")
    positive_int("planned job priority", job["priority"])
    if not isinstance(job["reason"], str) or not job["reason"].strip():
        raise ValueError("plan JSON contains an invalid planned job reason")
    nonnegative_int("planned job last_tested_timestamp", job["last_tested_timestamp"])
    age_days = job["age_days"]
    if age_days is not None and (
        isinstance(age_days, bool)
        or not isinstance(age_days, (int, float))
        or not math.isfinite(age_days)
        or age_days < 0
    ):
        raise ValueError("plan JSON contains an invalid planned job age_days")
    nodes.append(node)
    job_names.append(job_name)
if len(set(nodes)) != len(nodes):
    raise ValueError("plan JSON planned job nodes must be unique")
if len(set(job_names)) != len(job_names):
    raise ValueError("plan JSON planned job names must be unique")
print(f"{free_nodes_count}\t{queue_count}\t{len(planned_jobs)}\t{','.join(nodes)}")
PY
}

json_inventory_summary_tsv() {
    python - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    data = json.load(handle)
if not isinstance(data, dict) or set(data) != {"nodes", "node_count"}:
    raise ValueError("GPU inventory JSON has an unexpected shape")
nodes = data["nodes"]
if not isinstance(nodes, list) or type(data["node_count"]) is not int:
    raise ValueError("GPU inventory JSON contains invalid fields")
if data["node_count"] != len(nodes):
    raise ValueError("GPU inventory node_count does not match nodes")
for name in nodes:
    if not isinstance(name, str) or not name:
        raise ValueError("GPU inventory contains an invalid node name")
if len(nodes) != len(set(nodes)):
    raise ValueError("GPU inventory contains duplicate node names")
print(f"{len(nodes)}\t{','.join(nodes)}")
PY
}

json_status_to_map_tsv() {
    python - "$1" "$2" <<'PY'
import json
import sys

status_path, map_path = sys.argv[1:]
with open(status_path, encoding="utf-8") as handle:
    data = json.load(handle)
if not isinstance(data, list):
    raise ValueError("status JSON must be a list")
latest = {}
for item in data:
    if not isinstance(item, dict):
        raise ValueError("status JSON contains a non-object row")
    node = item.get("node")
    timestamp = item.get("latest_timestamp")
    if not isinstance(node, str) or not node:
        raise ValueError("status JSON contains an invalid node")
    if timestamp is None:
        value = 0
    elif isinstance(timestamp, int) and not isinstance(timestamp, bool):
        value = timestamp
    else:
        raise ValueError("status JSON contains an invalid latest_timestamp")
    latest[node] = max(latest.get(node, 0), value)
with open(map_path, "w", encoding="utf-8") as handle:
    json.dump(latest, handle, sort_keys=True)
    handle.write("\n")
print(f"{len(data)}\t{len(latest)}")
PY
}

write_audit_summary() {
    python - "$@" <<'PY'
import json
import sys

(
    output_path,
    git_ref,
    git_ref_source,
    free_nodes_count,
    queue_count,
    planned_count,
    selected_nodes_csv,
    plan_path,
) = sys.argv[1:]
payload = {
    "schema": "cval.live-audit.v1",
    "mode": "audit",
    "action": "audit",
    "cluster_mutations": 0,
    "git_ref": git_ref,
    "git_ref_source": git_ref_source,
    "free_nodes_count": int(free_nodes_count),
    "queue_count": int(queue_count),
    "planned_count": int(planned_count),
    "selected_nodes": [item for item in selected_nodes_csv.split(",") if item],
    "plan_path": plan_path,
}
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

collect_plan_inputs() {
    local cycle_dir="$1"
    local stem="$2"
    local expected_sha="$3"
    local nodes_file="$cycle_dir/$stem-nodes.json"
    local nodes_error="$cycle_dir/$stem-nodes.stderr"
    local status_file="$cycle_dir/$stem-latest-status.json"
    local status_error="$cycle_dir/$stem-latest-status.stderr"
    local status_map="$cycle_dir/$stem-latest-status-map.json"
    local failed=0

    local nodes_rc=0
    assert_runner_worktree "$stem-nodes-before" "$expected_sha" || return "$?"
    python -m cval.cli --config "$CONFIG_PATH" nodes --inventory-only --output json >"$nodes_file" 2>"$nodes_error" || nodes_rc=$?
    assert_runner_worktree "$stem-nodes-after" "$expected_sha" || return "$?"
    if (( nodes_rc == 0 )); then
        local inventory_summary
        local inventory_parse_rc=0
        inventory_summary=$(json_inventory_summary_tsv "$nodes_file") || inventory_parse_rc=$?
        if (( inventory_parse_rc == 0 )); then
            IFS=$'\t' read -r PLAN_DISCOVERED_NODE_COUNT PLAN_FREE_NODES <<<"$inventory_summary"
            PLAN_FREE_NODE_COUNT="$PLAN_DISCOVERED_NODE_COUNT"
            log "component=gpu-inventory status=ok candidate_node_count=$PLAN_DISCOVERED_NODE_COUNT"
        else
            log "component=gpu-inventory status=invalid-json exit_code=$inventory_parse_rc artifact=$nodes_file"
            failed=1
        fi
    else
        log "component=gpu-inventory status=failed exit_code=$nodes_rc stderr=$nodes_error"
        [[ ! -s "$nodes_error" ]] || cat "$nodes_error" >&2
        failed=1
    fi

    local status_rc=0
    assert_runner_worktree "$stem-status-before" "$expected_sha" || return "$?"
    python -m cval.cli --config "$CONFIG_PATH" status --output json >"$status_file" 2>"$status_error" || status_rc=$?
    assert_runner_worktree "$stem-status-after" "$expected_sha" || return "$?"
    if (( status_rc == 0 )); then
        local status_summary
        local status_parse_rc=0
        status_summary=$(json_status_to_map_tsv "$status_file" "$status_map") || status_parse_rc=$?
        if (( status_parse_rc == 0 )); then
            IFS=$'\t' read -r PLAN_STATUS_ROW_COUNT PLAN_STATUS_NODE_COUNT <<<"$status_summary"
            PLAN_STATUS_MAP="$status_map"
            log "component=latest-status status=ok row_count=$PLAN_STATUS_ROW_COUNT node_count=$PLAN_STATUS_NODE_COUNT source=validation.db/latest_status"
        else
            log "component=latest-status status=invalid-json exit_code=$status_parse_rc artifact=$status_file"
            failed=1
        fi
    else
        log "component=latest-status status=failed exit_code=$status_rc stderr=$status_error"
        [[ ! -s "$status_error" ]] || cat "$status_error" >&2
        failed=1
    fi

    if (( failed != 0 )); then
        return 1
    fi
    apply_node_cooldown "$cycle_dir" "$stem"
}

json_node_check_tsv() {
    python - "$1" "$2" <<'PY'
import json
import sys

path, expected_node = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    item = json.load(handle)
expected = {
    "name", "found", "is_gpu_node", "schedulable", "resource_ready",
    "capacity", "allocatable", "used", "free", "fully_free", "reason",
    "cordoned", "ready", "status_label", "eligible",
}
if not isinstance(item, dict) or set(item) != expected:
    raise ValueError("targeted node status JSON has an unexpected shape")
if item["name"] != expected_node:
    raise ValueError("targeted node status identity mismatch")
for key in (
    "found", "is_gpu_node", "schedulable", "resource_ready", "fully_free",
    "cordoned", "ready", "eligible",
):
    if not isinstance(item[key], bool):
        raise ValueError(f"targeted node status {key} must be boolean")
for key in ("capacity", "allocatable", "used", "free"):
    if type(item[key]) is not int or item[key] < 0:
        raise ValueError(f"targeted node status {key} must be non-negative integer")
for key in ("reason", "status_label"):
    if not isinstance(item[key], str) or not item[key]:
        raise ValueError(f"targeted node status {key} must be non-empty")
print(
    f"{str(item['eligible']).lower()}\t{item['status_label']}\t"
    f"{item['free']}\t{item['allocatable']}\t{item['reason']}"
)
PY
}

select_available_nodes() {
    local plan_file="$1"
    local limit="$2"
    local cycle_dir="$3"
    local stem="$4"
    local expected_sha="$5"
    shift 5
    local excluded=("$@")
    local candidate_file="$cycle_dir/$stem-priority-candidates.txt"
    python - "$plan_file" "${excluded[@]}" >"$candidate_file" <<'PY'
import json
import sys

path = sys.argv[1]
excluded = set(sys.argv[2:])
with open(path, encoding="utf-8") as handle:
    payload = json.load(handle)
for job in payload.get("planned_jobs", []):
    node = job.get("node")
    if isinstance(node, str) and node and node not in excluded:
        print(node)
PY
    SELECTED_AVAILABLE_NODES=""
    CHECKED_NODE_NAMES=""
    local selected_count=0
    local node
    while IFS= read -r node; do
        [[ -n "$node" ]] || continue
        local check_file="$cycle_dir/$stem-node-check-$node.json"
        local check_error="$check_file.stderr"
        local check_rc=0
        assert_runner_worktree "$stem-check-$node-before" "$expected_sha" || return "$?"
        python -m cval.cli --config "$CONFIG_PATH" nodes \
            --check-node "$node" --output json >"$check_file" 2>"$check_error" || check_rc=$?
        assert_runner_worktree "$stem-check-$node-after" "$expected_sha" || return "$?"
        if (( check_rc != 0 )); then
            log "component=node-check node=$node status=failed exit_code=$check_rc stderr=$check_error"
            [[ ! -s "$check_error" ]] || cat "$check_error" >&2
            return "$check_rc"
        fi
        local check_summary
        local parse_rc=0
        check_summary=$(json_node_check_tsv "$check_file" "$node") || parse_rc=$?
        if (( parse_rc != 0 )); then
            log "component=node-check node=$node status=invalid-json artifact=$check_file"
            return "$parse_rc"
        fi
        local eligible status_label free allocatable reason
        IFS=$'\t' read -r eligible status_label free allocatable reason <<<"$check_summary"
        log "component=node-check node=$node status=$status_label eligible=$eligible free=$free/$allocatable reason=$reason"
        CHECKED_NODE_NAMES=$(csv_append_unique "$CHECKED_NODE_NAMES" "$node")
        if [[ "$eligible" == "true" ]]; then
            SELECTED_AVAILABLE_NODES=$(csv_append_unique "$SELECTED_AVAILABLE_NODES" "$node")
            selected_count=$((selected_count + 1))
            if (( selected_count >= limit )); then
                break
            fi
        fi
    done <"$candidate_file"
}

exclude_nodes_from_plan_snapshot() {
    local excluded_csv="$1"
    [[ -n "$excluded_csv" ]] || return 0
    PLAN_FREE_NODES=$(python - "$PLAN_FREE_NODES" "$excluded_csv" <<'PY'
import sys
nodes = [item for item in sys.argv[1].split(",") if item]
excluded = {item for item in sys.argv[2].split(",") if item}
print(",".join(node for node in nodes if node not in excluded))
PY
)
    PLAN_FREE_NODE_COUNT=$(csv_count "$PLAN_FREE_NODES")
}

run_kubectl() {
    timeout --foreground "${KUBECTL_TIMEOUT_SECONDS}s" \
        kubectl --request-timeout="${KUBECTL_TIMEOUT_SECONDS}s" "$@"
}

json_phase() {
    local status_file="$1"
    local job_name="$2"
    python - "$status_file" "$job_name" <<'PY'
import json
import sys

status_file, job_name = sys.argv[1:]
with open(status_file, encoding="utf-8") as handle:
    data = json.load(handle)
for item in data:
    if item.get("job_name") == job_name:
        print(item.get("phase", "Unknown"))
        break
else:
    print("Unknown")
PY
}

csv_contains() {
    local csv="$1"
    local needle="$2"
    [[ ",$csv," == *",$needle,"* ]]
}

csv_append_unique() {
    local csv="$1"
    local value="$2"
    if [[ -z "$value" ]]; then
        printf '%s\n' "$csv"
    elif [[ -z "$csv" ]]; then
        printf '%s\n' "$value"
    elif csv_contains "$csv" "$value"; then
        printf '%s\n' "$csv"
    else
        printf '%s,%s\n' "$csv" "$value"
    fi
}

csv_remove_value() {
    local csv="$1"
    local remove="$2"
    python - "$csv" "$remove" <<'PY'
import sys

csv, remove = sys.argv[1:]
values = [item for item in csv.split(",") if item and item != remove]
print(",".join(values))
PY
}

csv_count() {
    local csv="$1"
    if [[ -z "$csv" ]]; then
        echo 0
    else
        python - "$csv" <<'PY'
import sys
print(len([item for item in sys.argv[1].split(",") if item]))
PY
    fi
}

job_node_from_name() {
    local job_name="$1"
    python - "$job_name" <<'PY'
import re
import sys

name = sys.argv[1]
match = re.search(r"(slc01-cl02-hgx-[0-9]{4})", name)
print(match.group(1) if match else "")
PY
}

latest_cycle_dir_with_submits() {
    find "$LOG_DIR" -mindepth 2 -maxdepth 2 \( -name 'submit.json' -o -name 'submit-*.json' \) -printf '%T@ %h\n' 2>/dev/null \
        | sort -nr \
        | head -1 \
        | cut -d' ' -f2-
}

json_jobs_observation_state() {
    python - "$1" "$2" <<'PY'
import json
import sys

status_path, jobs_csv = sys.argv[1:]
tracked = [item for item in jobs_csv.split(",") if item]
with open(status_path, encoding="utf-8") as handle:
    phases = json.load(handle)
if not isinstance(phases, list):
    raise ValueError("jobs JSON must be a list")
observed = {}
for item in phases:
    if not isinstance(item, dict):
        raise ValueError("jobs JSON contains a non-object row")
    job_name = item.get("job_name")
    phase = item.get("phase")
    if not isinstance(job_name, str) or not job_name or not isinstance(phase, str):
        raise ValueError("jobs JSON contains an invalid row")
    if job_name in observed:
        raise ValueError("jobs JSON contains duplicate job rows")
    observed[job_name] = phase
tracked_phases = [observed.get(job_name, "Unknown") for job_name in tracked]
terminal = {"Completed", "Succeeded", "Failed", "Aborted", "Terminated"}
if any(phase not in terminal | {"Pending", "Running"} for phase in tracked_phases):
    print("unknown")
elif any(phase in {"Pending", "Running"} for phase in tracked_phases):
    print("active")
else:
    print("terminal")
PY
}

record_pruned_job() {
    local cycle_dir="$1"
    local job_name="$2"
    local deleted_at="$3"
    python - "$cycle_dir/pruned-jobs.csv" "$job_name" "$deleted_at" <<'PY'
import csv
import os
import sys
import tempfile
from pathlib import Path

path = Path(sys.argv[1])
job_name = sys.argv[2]
deleted_at = sys.argv[3]
rows = {}
if path.exists():
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["job_name", "deleted_at"]:
            raise ValueError("pruned-jobs.csv has an invalid header")
        for row in reader:
            name = row.get("job_name", "")
            timestamp = row.get("deleted_at", "")
            if not name or not timestamp.isdigit() or name in rows:
                raise ValueError("pruned-jobs.csv has an invalid row")
            rows[name] = timestamp
if not job_name or not deleted_at.isdigit():
    raise ValueError("invalid pruned job receipt")
rows[job_name] = deleted_at
path.parent.mkdir(parents=True, exist_ok=True)
descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
temporary = Path(temporary_name)
try:
    with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("job_name", "deleted_at"))
        for name in sorted(rows):
            writer.writerow((name, rows[name]))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
finally:
    try:
        temporary.unlink()
    except FileNotFoundError:
        pass
PY
}

delete_job() {
    local job_name="$1"
    local deleted_log="$2"
    local cycle_dir="$3"
    if ! pruning_enabled; then
        log "refusing pending-job delete because pruning is not independently enabled"
        return 2
    fi
    log "deleting pending job after timeout: $job_name"
    if ! run_kubectl delete vcjob -n "$NAMESPACE" "$job_name" --ignore-not-found=true | tee -a "$deleted_log"; then
        return 1
    fi
    record_pruned_job "$cycle_dir" "$job_name" "$(date +%s)"
}

watch_existing_jobs_until_clear() {
    local cycle_dir="$1"
    local active_jobs="$2"
    local expected_sha="$3"

    enter_runner_worktree "resume-watch" || return "$?"
    while [[ -n "$active_jobs" ]]; do
        local status_file="$cycle_dir/resume-status-$(date -u +%H%M%S).json"
        local status_error="$status_file.stderr"
        local status_rc=0
        assert_runner_worktree "resume-watch-jobs-before" "$expected_sha" || {
            leave_runner_worktree "resume-watch" 2 || return "$?"
            return 2
        }
        python -m cval.cli --config "$CONFIG_PATH" jobs --jobs "$active_jobs" --output json \
            >"$status_file" 2>"$status_error" || status_rc=$?
        assert_runner_worktree "resume-watch-jobs-after" "$expected_sha" || {
            leave_runner_worktree "resume-watch" 2 || return "$?"
            return 2
        }
        if (( status_rc != 0 )); then
            log "resume jobs observation failed exit_code=$status_rc; retaining tracked jobs for a later bounded observation"
            [[ ! -s "$status_error" ]] || cat "$status_error" >&2
            leave_runner_worktree "resume-watch" 2 || return "$?"
            return 2
        fi
        cat "$status_file"

        local observation_state
        local observation_rc=0
        observation_state=$(json_jobs_observation_state "$status_file" "$active_jobs") || observation_rc=$?
        if (( observation_rc != 0 )); then
            log "resume jobs observation was invalid; retaining tracked jobs for a later bounded observation"
            leave_runner_worktree "resume-watch" 2 || return "$?"
            return 2
        fi
        if [[ "$observation_state" == "unknown" ]]; then
            log "resume jobs observation is indeterminate; retaining tracked jobs and ending this observation pass"
            leave_runner_worktree "resume-watch" 2 || return "$?"
            return 2
        fi

        IFS=',' read -r -a active_array <<< "$active_jobs"
        for job_name in "${active_array[@]}"; do
            [[ -n "$job_name" ]] || continue
            local phase
            local phase_rc=0
            phase=$(json_phase "$status_file" "$job_name") || phase_rc=$?
            if (( phase_rc != 0 )); then
                log "resume job $job_name observation was invalid; retaining tracked jobs for a later bounded observation"
                leave_runner_worktree "resume-watch" 2 || return "$?"
                return 2
            fi
            log "resume job $job_name phase=$phase"

            case "$phase" in
                Completed|Succeeded|Failed|Aborted|Terminated)
                    active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                    ;;
                Pending)
                    if pruning_enabled; then
                        local created_ts
                        created_ts=$(run_kubectl get vcjob -n "$NAMESPACE" "$job_name" -o jsonpath='{.metadata.creationTimestamp}' 2>/dev/null || true)
                        local created_epoch
                        created_epoch=$(python -c 'import datetime,sys; s=sys.stdin.read().strip(); print(int(datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()) if s else 0)' <<< "$created_ts")
                        local now_epoch
                        now_epoch=$(date +%s)
                        if [[ "$created_epoch" != "0" && $((now_epoch - created_epoch)) -ge "$PENDING_START_TIMEOUT_SECONDS" ]]; then
                            if ! delete_job "$job_name" "$cycle_dir/deleted-jobs.log" "$cycle_dir"; then
                                log "resume prune failed for $job_name; retaining tracked job"
                                leave_runner_worktree "resume-watch" 2 || return "$?"
                                return 2
                            fi
                            active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                        fi
                    fi
                    ;;
                Running) ;;
                Unknown|*)
                    log "resume job $job_name phase=$phase is indeterminate; retaining tracked jobs and ending this observation pass"
                    leave_runner_worktree "resume-watch" 2 || return "$?"
                    return 2
                    ;;
            esac
        done

        [[ -z "$active_jobs" ]] && break
        sleep "$WATCH_POLL_SECONDS"
    done

    local final_status_rc=0
    assert_runner_worktree "resume-final-status-before" "$expected_sha" || {
        leave_runner_worktree "resume-watch" 2 || return "$?"
        return 2
    }
    python -m cval.cli --config "$CONFIG_PATH" status --output json \
        >"$cycle_dir/status.json" || final_status_rc=$?
    assert_runner_worktree "resume-final-status-after" "$expected_sha" || {
        leave_runner_worktree "resume-watch" 2 || return "$?"
        return 2
    }
    if (( final_status_rc != 0 )); then
        log "resume final status observation failed exit_code=$final_status_rc"
    fi
    leave_runner_worktree "resume-watch" "$final_status_rc" || return "$?"
}

resume_latest_cycle_if_needed() {
    if [[ "$LIVE_MODE" != "submit" ]]; then
        log "mode=audit; submitted-job resume disabled"
        return 1
    fi
    local cycle_dir
    cycle_dir=$(latest_cycle_dir_with_submits)
    if [[ -z "$cycle_dir" ]]; then
        return 1
    fi

    repair_cycle_cooldowns "$cycle_dir" || return "$?"

    local jobs_csv
    jobs_csv=$(json_submitted_jobs_csv_from_dir "$cycle_dir")
    if [[ -z "$jobs_csv" ]]; then
        return 1
    fi

    mkdir -p "$cycle_dir"
    resolve_git_ref || return "$?"
    local resume_git_ref="$RESOLVED_GIT_REF"
    ensure_runner_worktree "$resume_git_ref" || return "$?"

    enter_runner_worktree "resume-status" || return "$?"
    local resume_error="$cycle_dir/resume-status.stderr"
    local resume_rc=0
    assert_runner_worktree "resume-status-jobs-before" "$resume_git_ref" || {
        leave_runner_worktree "resume-status" 2 || return "$?"
        return 2
    }
    python -m cval.cli --config "$CONFIG_PATH" jobs --jobs "$jobs_csv" --output json \
        >"$cycle_dir/resume-status.json" 2>"$resume_error" || resume_rc=$?
    assert_runner_worktree "resume-status-jobs-after" "$resume_git_ref" || {
        leave_runner_worktree "resume-status" 2 || return "$?"
        return 2
    }
    if (( resume_rc != 0 )); then
        log "resume jobs observation failed exit_code=$resume_rc; deferring new submission cycle"
        [[ ! -s "$resume_error" ]] || cat "$resume_error" >&2
        leave_runner_worktree "resume-status" 2 || return "$?"
        return 2
    fi
    leave_runner_worktree "resume-status" 0 || return "$?"

    local observation_state
    local observation_rc=0
    observation_state=$(json_jobs_observation_state "$cycle_dir/resume-status.json" "$jobs_csv") || observation_rc=$?
    if (( observation_rc != 0 )); then
        log "resume jobs observation was invalid; deferring new submission cycle"
        return 2
    fi
    case "$observation_state" in
        active)
            log "resuming watch for active jobs from $cycle_dir: $jobs_csv"
            watch_existing_jobs_until_clear "$cycle_dir" "$jobs_csv" "$resume_git_ref"
            return "$?"
            ;;
        unknown)
            log "resume jobs observation is indeterminate; retaining tracked jobs and deferring new submission cycle"
            return 2
            ;;
        terminal) ;;
        *)
            log "resume jobs observation returned an invalid state; deferring new submission cycle"
            return 2
            ;;
    esac

    log "latest submitted jobs are already terminal; no resume needed"
    return 1
}

new_cycle_dir() {
    mkdir -p "$LOG_DIR"
    local cycle_id
    cycle_id=$(date -u +%Y%m%dT%H%M%SZ)
    local cycle_dir="$LOG_DIR/$cycle_id"
    local suffix=0
    while [[ -e "$cycle_dir" ]]; do
        suffix=$((suffix + 1))
        cycle_dir="$LOG_DIR/$cycle_id-$suffix"
    done
    mkdir "$cycle_dir"
    CYCLE_DIR="$cycle_dir"
}

log_operation_settings() {
    log "mode=$LIVE_MODE config=$CONFIG_PATH"
    log "batch_size=$BATCH_SIZE plan_limit=$PLAN_LIMIT days_threshold=$DAYS_THRESHOLD node_cooldown=${NODE_COOLDOWN_SECONDS}s kubectl_timeout=${KUBECTL_TIMEOUT_SECONDS}s"
    log "node_cooldown_state=$NODE_COOLDOWN_STATE_FILE"
    if pruning_enabled; then
        log "pruning=enabled confirmation=delete-pending namespace=$NAMESPACE prefix=$JOB_PREFIX"
    else
        log "pruning=disabled"
    fi
}

run_audit_cycle() {
    require_command git
    require_command kubectl
    require_command timeout
    require_command python

    new_cycle_dir
    local cycle_dir="$CYCLE_DIR"
    resolve_git_ref || return "$?"
    local git_ref="$RESOLVED_GIT_REF"
    local git_ref_source="$RESOLVED_GIT_REF_SOURCE"
    log_operation_settings
    ensure_runner_worktree "$git_ref" || return "$?"

    enter_runner_worktree "audit" || return "$?"
    local plan_file="$cycle_dir/audit-plan.json"
    local plan_error="$cycle_dir/audit-plan.stderr"
    log "audit action=inspect: gpu inventory -> latest_status -> cooldown -> priority -> targeted availability checks"
    local inputs_rc=0
    collect_plan_inputs "$cycle_dir" audit "$git_ref" || inputs_rc=$?
    if (( inputs_rc != 0 )); then
        log "audit cycle failed: one or more read-only input components failed"
        leave_runner_worktree "audit" "$inputs_rc" || return "$?"
        return "$inputs_rc"
    fi
    local plan_rc=0
    assert_runner_worktree "audit-plan-before" "$git_ref" || {
        leave_runner_worktree "audit" 2 || return "$?"
        return 2
    }
    python -m cval.cli --config "$CONFIG_PATH" plan \
        --free-nodes "$PLAN_FREE_NODES" \
        --db-status-json "$PLAN_STATUS_MAP" \
        --threshold-days "$DAYS_THRESHOLD" \
        --batch-size "$PLAN_LIMIT" \
        --timestamp "$(date +%s)" \
        --git-ref "$git_ref" \
        --output json >"$plan_file" 2>"$plan_error" || plan_rc=$?
    assert_runner_worktree "audit-plan-after" "$git_ref" || {
        leave_runner_worktree "audit" 2 || return "$?"
        return 2
    }
    if (( plan_rc != 0 )); then
        log "audit component=plan status=failed exit_code=$plan_rc stderr=$plan_error"
        [[ ! -s "$plan_error" ]] || cat "$plan_error" >&2
        leave_runner_worktree "audit" "$plan_rc" || return "$?"
        return "$plan_rc"
    fi

    local summary
    local parse_rc=0
    summary=$(json_plan_summary_tsv "$plan_file" "$PLAN_FREE_NODE_COUNT" "$PLAN_LIMIT") || parse_rc=$?
    if (( parse_rc != 0 )); then
        log "audit component=plan status=invalid-json exit_code=$parse_rc artifact=$plan_file"
        leave_runner_worktree "audit" "$parse_rc" || return "$?"
        return "$parse_rc"
    fi
    local candidate_nodes_count queue_count planned_count planned_nodes
    IFS=$'\t' read -r candidate_nodes_count queue_count planned_count planned_nodes <<<"$summary"
    local selected_nodes=""
    if (( planned_count > 0 )); then
        local selection_rc=0
        select_available_nodes \
            "$plan_file" "$BATCH_SIZE" "$cycle_dir" audit "$git_ref" \
            || selection_rc=$?
        if (( selection_rc != 0 )); then
            leave_runner_worktree "audit" "$selection_rc" || return "$?"
            return "$selection_rc"
        fi
        selected_nodes="$SELECTED_AVAILABLE_NODES"
    fi
    leave_runner_worktree "audit" 0 || return "$?"

    log "audit component=inventory-and-status status=ok candidate_node_count=$candidate_nodes_count"
    log "audit component=priority-and-render status=ok queue_count=$queue_count planned_count=$planned_count"
    if [[ -n "$selected_nodes" ]]; then
        log "audit action=inspect selected_nodes=$selected_nodes submitted_count=0"
    elif (( queue_count > 0 )); then
        log "audit state=no-currently-available-prioritized-node action=inspect submitted_count=0"
    else
        log "audit state=no-due-candidates action=inspect submitted_count=0"
    fi
    write_audit_summary \
        "$cycle_dir/audit-summary.json" \
        "$git_ref" \
        "$git_ref_source" \
        "$candidate_nodes_count" \
        "$queue_count" \
        "$planned_count" \
        "$selected_nodes" \
        "$(basename "$plan_file")"
    log "audit cycle complete; artifacts in $cycle_dir"
}

run_submit_cycle() {
    require_command git
    require_command kubectl
    require_command timeout
    require_command python

    new_cycle_dir
    local cycle_dir="$CYCLE_DIR"

    resolve_git_ref || return "$?"
    local git_ref="$RESOLVED_GIT_REF"
    log_operation_settings
    log "pending_start_timeout=${PENDING_START_TIMEOUT_SECONDS}s"

    ensure_runner_worktree "$git_ref" || return "$?"

    enter_runner_worktree "submit" || return "$?"

    local inputs_rc=0
    collect_plan_inputs "$cycle_dir" preflight "$git_ref" || inputs_rc=$?
    if (( inputs_rc != 0 )); then
        log "submit cycle preflight failed: discovery/status failure is not an empty queue"
        leave_runner_worktree "submit" "$inputs_rc" || return "$?"
        return "$inputs_rc"
    fi
    local preflight_plan="$cycle_dir/preflight-plan.json"
    local preflight_error="$cycle_dir/preflight-plan.stderr"
    local preflight_rc=0
    assert_runner_worktree "submit-preflight-plan-before" "$git_ref" || {
        leave_runner_worktree "submit" 2 || return "$?"
        return 2
    }
    python -m cval.cli --config "$CONFIG_PATH" plan \
        --free-nodes "$PLAN_FREE_NODES" \
        --db-status-json "$PLAN_STATUS_MAP" \
        --threshold-days "$DAYS_THRESHOLD" \
        --batch-size "$PLAN_LIMIT" \
        --timestamp "$(date +%s)" \
        --git-ref "$git_ref" \
        --output json >"$preflight_plan" 2>"$preflight_error" || preflight_rc=$?
    assert_runner_worktree "submit-preflight-plan-after" "$git_ref" || {
        leave_runner_worktree "submit" 2 || return "$?"
        return 2
    }
    if (( preflight_rc != 0 )); then
        log "submit component=preflight-plan status=failed exit_code=$preflight_rc stderr=$preflight_error"
        [[ ! -s "$preflight_error" ]] || cat "$preflight_error" >&2
        leave_runner_worktree "submit" "$preflight_rc" || return "$?"
        return "$preflight_rc"
    fi
    local preflight_summary
    local preflight_parse_rc=0
    preflight_summary=$(json_plan_summary_tsv "$preflight_plan" "$PLAN_FREE_NODE_COUNT" "$PLAN_LIMIT") || preflight_parse_rc=$?
    if (( preflight_parse_rc != 0 )); then
        log "submit component=preflight-plan status=invalid-json exit_code=$preflight_parse_rc artifact=$preflight_plan"
        leave_runner_worktree "submit" "$preflight_parse_rc" || return "$?"
        return "$preflight_parse_rc"
    fi
    local preflight_free preflight_queue preflight_planned preflight_nodes
    IFS=$'\t' read -r preflight_free preflight_queue preflight_planned preflight_nodes <<<"$preflight_summary"
    log "submit component=preflight-plan status=ok candidate_node_count=$preflight_free queue_count=$preflight_queue planned_count=$preflight_planned"
    if (( preflight_queue == 0 )); then
        log "submit state=no-due-candidates; preflight succeeded and no jobs will be submitted"
        leave_runner_worktree "submit" 0 || return "$?"
        log "cycle complete; artifacts in $cycle_dir"
        return 0
    fi

    local active_jobs=""
    local submitted_nodes=""
    local completed_jobs=""
    local skipped_nodes=""
    local idle_rounds=0

    while true; do
        local status_file="$cycle_dir/status-$(date -u +%H%M%S).json"
        if [[ -n "$active_jobs" ]]; then
            local status_error="$status_file.stderr"
            local status_rc=0
            assert_runner_worktree "submit-jobs-before" "$git_ref" || {
                leave_runner_worktree "submit" 2 || return "$?"
                return 2
            }
            python -m cval.cli --config "$CONFIG_PATH" jobs --jobs "$active_jobs" --output json \
                >"$status_file" 2>"$status_error" || status_rc=$?
            assert_runner_worktree "submit-jobs-after" "$git_ref" || {
                leave_runner_worktree "submit" 2 || return "$?"
                return 2
            }
            if (( status_rc != 0 )); then
                log "submit jobs observation failed exit_code=$status_rc; retaining active jobs and ending cycle"
                [[ ! -s "$status_error" ]] || cat "$status_error" >&2
                leave_runner_worktree "submit" "$status_rc" || return "$?"
                return "$status_rc"
            fi

            local observation_state
            local observation_rc=0
            observation_state=$(json_jobs_observation_state "$status_file" "$active_jobs") || observation_rc=$?
            if (( observation_rc != 0 )); then
                log "submit jobs observation was invalid; retaining active jobs and ending cycle"
                leave_runner_worktree "submit" 1 || return "$?"
                return 1
            fi
            if [[ "$observation_state" == "unknown" ]]; then
                log "submit jobs observation is indeterminate; retaining active jobs and ending cycle"
                leave_runner_worktree "submit" 1 || return "$?"
                return 1
            fi

            IFS=',' read -r -a active_array <<< "$active_jobs"
            for job_name in "${active_array[@]}"; do
                [[ -n "$job_name" ]] || continue
                local phase
                local phase_rc=0
                phase=$(json_phase "$status_file" "$job_name") || phase_rc=$?
                if (( phase_rc != 0 )); then
                    log "job $job_name observation was invalid; retaining active jobs and ending cycle"
                    leave_runner_worktree "submit" 1 || return "$?"
                    return 1
                fi
                local node
                node=$(job_node_from_name "$job_name")
                log "job $job_name phase=$phase"

                case "$phase" in
                    Completed|Succeeded|Failed|Aborted|Terminated)
                        active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                        completed_jobs=$(csv_append_unique "$completed_jobs" "$job_name")
                        ;;
                    Pending)
                        if pruning_enabled; then
                            local created_ts
                            created_ts=$(run_kubectl get vcjob -n "$NAMESPACE" "$job_name" -o jsonpath='{.metadata.creationTimestamp}' 2>/dev/null || true)
                            local created_epoch
                            created_epoch=$(python -c 'import datetime,sys; s=sys.stdin.read().strip(); print(int(datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()) if s else 0)' <<< "$created_ts")
                            local now_epoch
                            now_epoch=$(date +%s)
                            if [[ "$phase" == "Pending" && "$created_epoch" != "0" && $((now_epoch - created_epoch)) -ge "$PENDING_START_TIMEOUT_SECONDS" ]]; then
                                if ! delete_job "$job_name" "$cycle_dir/deleted-jobs.log" "$cycle_dir"; then
                                    log "pending job prune failed for $job_name; retaining it and ending cycle"
                                    leave_runner_worktree "submit" 1 || return "$?"
                                    return 1
                                fi
                                active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                                skipped_nodes=$(csv_append_unique "$skipped_nodes" "$node")
                            fi
                        fi
                        ;;
                    Running) ;;
                    Unknown|*)
                        log "job $job_name phase=$phase is indeterminate; retaining active jobs and ending cycle"
                        leave_runner_worktree "submit" 1 || return "$?"
                        return 1
                        ;;
                esac
            done
        fi

        local active_count
        active_count=$(csv_count "$active_jobs")
        local slots=$((BATCH_SIZE - active_count))
        while (( slots > 0 )); do
            local slot_stem="slot-$slots-$(date -u +%H%M%S)"
            inputs_rc=0
            collect_plan_inputs "$cycle_dir" "$slot_stem" "$git_ref" || inputs_rc=$?
            if (( inputs_rc != 0 )); then
                log "submit cycle failed: slot discovery/status failure is not an empty queue"
                leave_runner_worktree "submit" "$inputs_rc" || return "$?"
                return "$inputs_rc"
            fi
            local cycle_excluded_nodes=""
            IFS=',' read -r -a skipped_before_plan <<< "$skipped_nodes"
            for node in "${skipped_before_plan[@]}"; do
                cycle_excluded_nodes=$(csv_append_unique "$cycle_excluded_nodes" "$node")
            done
            exclude_nodes_from_plan_snapshot "$cycle_excluded_nodes"
            local plan_file="$cycle_dir/plan-$(date -u +%H%M%S)-slot-$slots.json"
            log "rebuilding live ranked list for one open slot ($slots slot(s) available)"
            local plan_rc=0
            assert_runner_worktree "submit-slot-plan-before" "$git_ref" || {
                leave_runner_worktree "submit" 2 || return "$?"
                return 2
            }
            python -m cval.cli --config "$CONFIG_PATH" plan \
                --free-nodes "$PLAN_FREE_NODES" \
                --db-status-json "$PLAN_STATUS_MAP" \
                --threshold-days "$DAYS_THRESHOLD" \
                --batch-size "$PLAN_LIMIT" \
                --timestamp "$(date +%s)" \
                --git-ref "$git_ref" \
                --output json > "$plan_file" 2>&1 || plan_rc=$?
            assert_runner_worktree "submit-slot-plan-after" "$git_ref" || {
                leave_runner_worktree "submit" 2 || return "$?"
                return 2
            }
            if (( plan_rc != 0 )); then
                cat "$plan_file" >&2
                log "submit component=plan status=failed exit_code=$plan_rc; ending cycle"
                leave_runner_worktree "submit" "$plan_rc" || return "$?"
                return "$plan_rc"
            fi
            cat "$plan_file"

            if [[ ! -s "$plan_file" ]]; then
                log "submit component=plan status=failed reason=empty-output"
                leave_runner_worktree "submit" 1 || return "$?"
                return 1
            fi
            local plan_summary
            local parse_rc=0
            plan_summary=$(json_plan_summary_tsv "$plan_file" "$PLAN_FREE_NODE_COUNT" "$PLAN_LIMIT") || parse_rc=$?
            if (( parse_rc != 0 )); then
                log "submit component=plan status=invalid-json exit_code=$parse_rc artifact=$plan_file"
                leave_runner_worktree "submit" "$parse_rc" || return "$?"
                return "$parse_rc"
            fi
            local candidate_nodes_count queue_count planned_count planned_nodes
            IFS=$'\t' read -r candidate_nodes_count queue_count planned_count planned_nodes <<<"$plan_summary"
            log "submit component=plan status=ok candidate_node_count=$candidate_nodes_count queue_count=$queue_count planned_count=$planned_count"

            local exclude_args=()
            IFS=',' read -r -a submitted_array <<< "$submitted_nodes"
            for node in "${submitted_array[@]}"; do [[ -n "$node" ]] && exclude_args+=("$node"); done
            IFS=',' read -r -a skipped_array <<< "$skipped_nodes"
            for node in "${skipped_array[@]}"; do [[ -n "$node" ]] && exclude_args+=("$node"); done

            local selection_rc=0
            select_available_nodes \
                "$plan_file" 1 "$cycle_dir" "$slot_stem" "$git_ref" \
                "${exclude_args[@]}" || selection_rc=$?
            if (( selection_rc != 0 )); then
                leave_runner_worktree "submit" "$selection_rc" || return "$?"
                return "$selection_rc"
            fi
            local nodes_csv="$SELECTED_AVAILABLE_NODES"
            if [[ -n "$nodes_csv" ]]; then
                local run_timestamp
                run_timestamp=$(date +%s)
                while [[ -e "$cycle_dir/submit-$run_timestamp.json" ]]; do
                    run_timestamp=$((run_timestamp + 1))
                done
                local submit_file="$cycle_dir/submit-$run_timestamp.json"
                log "submitting node: $nodes_csv timestamp=$run_timestamp"
                local submit_rc=0
                assert_runner_worktree "submit-run-before" "$git_ref" || {
                    leave_runner_worktree "submit" 2 || return "$?"
                    return 2
                }
                python -m cval.cli --config "$CONFIG_PATH" run \
                    --free-nodes "$nodes_csv" \
                    --threshold-days "$DAYS_THRESHOLD" \
                    --batch-size 1 \
                    --timestamp "$run_timestamp" \
                    --git-ref "$git_ref" \
                    --submit \
                    --confirm submit \
                    --output json > "$submit_file" 2>&1 || submit_rc=$?
                assert_runner_worktree "submit-run-after" "$git_ref" || {
                    leave_runner_worktree "submit" 2 || return "$?"
                    return 2
                }
                if (( submit_rc != 0 )); then
                    cat "$submit_file"
                    log "submission outcome is indeterminate for node $nodes_csv; retaining artifact and ending cycle without replacement"
                    leave_runner_worktree "submit" "$submit_rc" || return "$?"
                    return "$submit_rc"
                fi
                cat "$submit_file"

                local submitted_identity
                local submitted_parse_rc=0
                submitted_identity=$(json_submitted_job_tsv "$submit_file" "$nodes_csv" "$git_ref") || submitted_parse_rc=$?
                if (( submitted_parse_rc != 0 )); then
                    log "submission response was invalid for node $nodes_csv; saved artifact retained and cycle stopped"
                    leave_runner_worktree "submit" "$submitted_parse_rc" || return "$?"
                    return "$submitted_parse_rc"
                fi
                local new_job submitted_node
                IFS=$'\t' read -r new_job submitted_node <<<"$submitted_identity"
                if ! record_node_submission "$submitted_node" "$run_timestamp"; then
                    log "cooldown state update failed after submitting $new_job; ending cycle without further submissions"
                    leave_runner_worktree "submit" 1 || return "$?"
                    return 1
                fi
                active_jobs=$(csv_append_unique "$active_jobs" "$new_job")

                IFS=',' read -r -a submitted_nodes_array <<< "$nodes_csv"
                for node in "${submitted_nodes_array[@]}"; do
                    submitted_nodes=$(csv_append_unique "$submitted_nodes" "$node")
                done
                idle_rounds=0
                slots=$((slots - 1))
            else
                IFS=',' read -r -a checked_array <<< "$CHECKED_NODE_NAMES"
                for node in "${checked_array[@]}"; do
                    [[ -n "$node" ]] && skipped_nodes=$(csv_append_unique "$skipped_nodes" "$node")
                done
                idle_rounds=$((idle_rounds + 1))
                if (( candidate_nodes_count > 0 && queue_count == 0 )); then
                    log "submit state=no-due-candidates; no additional eligible nodes for open slots"
                    break
                elif (( queue_count > 0 )); then
                    if (( planned_count < queue_count && planned_count > 0 )); then
                        log "submit state=priority-page-busy; rebuilding without $planned_count checked node(s) to continue the queue"
                        continue
                    fi
                    log "submit state=no-currently-available-prioritized-node; checked priority order without finding a free node"
                    break
                else
                    log "submit state=no-additional-candidates; candidates were excluded in this cycle"
                    break
                fi
            fi
        done

        active_count=$(csv_count "$active_jobs")
        if (( active_count == 0 && idle_rounds > 0 )); then
            log "no active jobs and no currently available prioritized nodes; ending cycle"
            break
        fi

        sleep "$WATCH_POLL_SECONDS"
    done

    local final_status_rc=0
    assert_runner_worktree "submit-final-status-before" "$git_ref" || {
        leave_runner_worktree "submit" 2 || return "$?"
        return 2
    }
    python -m cval.cli --config "$CONFIG_PATH" status --output json \
        >"$cycle_dir/status.json" || final_status_rc=$?
    assert_runner_worktree "submit-final-status-after" "$git_ref" || {
        leave_runner_worktree "submit" 2 || return "$?"
        return 2
    }
    if (( final_status_rc != 0 )); then
        log "submit final status observation failed exit_code=$final_status_rc"
    fi

    leave_runner_worktree "submit" "$final_status_rc" || return "$?"
    log "cycle complete; artifacts in $cycle_dir"
}

run_cycle() {
    if [[ "$LIVE_MODE" == "audit" ]]; then
        run_audit_cycle
    else
        run_submit_cycle
    fi
}

run_once() {
    require_command flock
    acquire_live_lock || return "$?"
    if [[ "$LIVE_MODE" == "submit" ]]; then
        local resume_rc=0
        resume_latest_cycle_if_needed || resume_rc=$?
        case "$resume_rc" in
            0) return 0 ;;
            1) ;;
            *)
                log "resume observation failed closed; deferring new cycle"
                return "$resume_rc"
                ;;
        esac
    fi
    run_cycle
}

run_loop() {
    require_command flock
    acquire_live_lock || return "$?"
    trap 'log "received stop signal; exiting loop"; exit 0' INT TERM
    while true; do
        if [[ "$LIVE_MODE" == "submit" ]]; then
            local resume_rc=0
            resume_latest_cycle_if_needed || resume_rc=$?
            if (( resume_rc == 0 )); then
                log "sleeping $LOOP_SLEEP_SECONDS seconds before next cycle"
                sleep "$LOOP_SLEEP_SECONDS"
                continue
            elif (( resume_rc != 1 )); then
                log "resume observation failed closed; deferring new cycle"
                log "sleeping $LOOP_SLEEP_SECONDS seconds before next cycle"
                sleep "$LOOP_SLEEP_SECONDS"
                continue
            fi
        fi
        if ! run_cycle; then
            log "cycle failed; see logs above"
        fi
        log "sleeping $LOOP_SLEEP_SECONDS seconds before next cycle"
        sleep "$LOOP_SLEEP_SECONDS"
    done
}

start_session() {
    require_command tmux
    mkdir -p "$LOG_DIR"
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "tmux session already running: $SESSION_NAME"
        echo "Attach with: $0 attach"
        return 0
    fi

    local session_log="$LOG_DIR/tmux-$(date -u +%Y%m%dT%H%M%SZ).log"
    local runner_cmd explicit_git_ref_env=""
    if [[ -n "$EXPLICIT_GIT_REF" ]]; then
        printf -v explicit_git_ref_env ' CVAL_GIT_REF=%q' "$EXPLICIT_GIT_REF"
    fi
    printf -v runner_cmd \
        'CVAL_LIVE_MODE=%q CVAL_LIVE_CONFIRM=%q CVAL_PRUNE_CONFIRM=%q CVAL_CONFIG=%q CVAL_SOURCE_REPO=%q CVAL_LIVE_LOG_DIR=%q CVAL_RUNNER_WORKTREE=%q CVAL_BATCH_SIZE=%q CVAL_PLAN_LIMIT=%q CVAL_DAYS_THRESHOLD=%q CVAL_NODE_COOLDOWN_SECONDS=%q CVAL_NODE_COOLDOWN_STATE_FILE=%q CVAL_NODE_COOLDOWN_HELPER=%q CVAL_PENDING_START_TIMEOUT_SECONDS=%q CVAL_NAMESPACE=%q CVAL_JOB_PREFIX=%q CVAL_LOOP_SLEEP_SECONDS=%q CVAL_WATCH_TIMEOUT_SECONDS=%q CVAL_WATCH_POLL_SECONDS=%q CVAL_KUBECTL_TIMEOUT_SECONDS=%q%s bash %q run-loop' \
        "$LIVE_MODE" "$LIVE_CONFIRM" "$PRUNE_CONFIRM" "$CONFIG_PATH" "$SOURCE_REPO" "$LOG_DIR" "$RUNNER_WORKTREE" "$BATCH_SIZE" "$PLAN_LIMIT" "$DAYS_THRESHOLD" "$NODE_COOLDOWN_SECONDS" "$NODE_COOLDOWN_STATE_FILE" "$NODE_COOLDOWN_HELPER" "$PENDING_START_TIMEOUT_SECONDS" "$NAMESPACE" "$JOB_PREFIX" "$LOOP_SLEEP_SECONDS" "$WATCH_TIMEOUT_SECONDS" "$WATCH_POLL_SECONDS" "$KUBECTL_TIMEOUT_SECONDS" "$explicit_git_ref_env" "$SCRIPT_PATH"

    local tmux_body
    printf -v tmux_body \
        'set -o pipefail; %s 2>&1 | tee -a %q; rc=${PIPESTATUS[0]}; echo "runner exited with code $rc; pane kept open"; exec bash' \
        "$runner_cmd" "$session_log"

    tmux new-session -d -s "$SESSION_NAME" "bash -lc $(printf '%q' "$tmux_body")"
    echo "Started tmux session: $SESSION_NAME"
    echo "Attach with: $0 attach"
    echo "Logs: $session_log"
}

stop_session() {
    require_command tmux
    if ! tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "tmux session is not running: $SESSION_NAME"
        return 0
    fi
    tmux send-keys -t "$SESSION_NAME" C-c
    tmux kill-session -t "$SESSION_NAME"
    echo "Stopped tmux session: $SESSION_NAME"
    echo "Kubernetes jobs were not deleted. Use cval jobs/status to inspect them."
}

show_status() {
    require_command tmux
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "tmux session running: $SESSION_NAME"
    else
        echo "tmux session not running: $SESSION_NAME"
    fi
    if [[ -d "$LOG_DIR" ]]; then
        local latest_log
        latest_log=$(ls -t "$LOG_DIR"/tmux-*.log 2>/dev/null | head -1 || true)
        if [[ -n "$latest_log" ]]; then
            echo "Latest log: $latest_log"
            tail -40 "$latest_log"
        fi
    fi
}

case "$COMMAND" in
    start)
        load_operational_settings
        validate_runtime_settings
        require_submit_startup_gate
        start_session
        ;;
    stop) stop_session ;;
    attach) exec tmux attach -t "$SESSION_NAME" ;;
    status) show_status ;;
    run-once)
        load_operational_settings
        validate_runtime_settings
        require_submit_startup_gate
        run_once
        ;;
    run-loop)
        load_operational_settings
        validate_runtime_settings
        require_submit_startup_gate
        run_loop
        ;;
    -h|--help|help) usage ;;
    *) usage; exit 2 ;;
esac