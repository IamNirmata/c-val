#!/usr/bin/env bash
set -euo pipefail

# Start/stop a tmux-backed c-val live runner.
# The runner uses a clean worktree from origin/main for code, while reading the
# operator config from CVAL_CONFIG/config/cval.toml in this repository.
#
# Rolling scheduling policy:
# - Keep at most CVAL_BATCH_SIZE active validation jobs.
# - Before filling each individual open slot, rebuild the live ranked list from
#   current Kubernetes state and latest validation DB status.
# - Skip nodes already submitted in the current cycle and nodes deleted for
#   pending-start timeout in the current cycle.
# - Delete a validation job if it remains Pending and never reaches Running
#   within CVAL_PENDING_START_TIMEOUT_SECONDS.

COMMAND=${1:-start}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCRIPT_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
SOURCE_REPO=${CVAL_SOURCE_REPO:-$(cd "$SCRIPT_DIR/.." && pwd)}
CONFIG_PATH=${CVAL_CONFIG:-$SOURCE_REPO/config/cval.toml}
SESSION_NAME=${CVAL_TMUX_SESSION:-cval-live}
LOG_DIR=${CVAL_LIVE_LOG_DIR:-$SOURCE_REPO/run-logs/cval-live}
RUNNER_WORKTREE=${CVAL_RUNNER_WORKTREE:-/tmp/cval-live-worktree}
LOOP_SLEEP_SECONDS=${CVAL_LOOP_SLEEP_SECONDS:-300}
CONFIRM_PHRASE=${CVAL_CONFIRM_PHRASE:-submit}
PLAN_LIMIT=${CVAL_PLAN_LIMIT:-50}

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

BATCH_SIZE=${CVAL_BATCH_SIZE:-$(config_value scheduling batch_size 2)}
DAYS_THRESHOLD=${CVAL_DAYS_THRESHOLD:-$(config_value scheduling days_threshold 7)}
WATCH_TIMEOUT_SECONDS=${CVAL_WATCH_TIMEOUT_SECONDS:-$(config_value monitoring timeout_seconds 3600)}
WATCH_POLL_SECONDS=${CVAL_WATCH_POLL_SECONDS:-$(config_value monitoring poll_interval_seconds 60)}
PENDING_START_TIMEOUT_SECONDS=${CVAL_PENDING_START_TIMEOUT_SECONDS:-$(config_value monitoring pending_start_timeout_seconds 480)}
NAMESPACE=${CVAL_NAMESPACE:-$(config_value cluster namespace gcr-admin)}
JOB_PREFIX=${CVAL_JOB_PREFIX:-$(config_value job job_prefix hari-gcr-cval)}

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
  CVAL_CONFIG=$CONFIG_PATH
  CVAL_BATCH_SIZE=$BATCH_SIZE
  CVAL_DAYS_THRESHOLD=$DAYS_THRESHOLD
    CVAL_PENDING_START_TIMEOUT_SECONDS=$PENDING_START_TIMEOUT_SECONDS
  CVAL_GIT_REF=<commit-or-branch>; default is current origin/main commit each cycle
    CVAL_PLAN_LIMIT=$PLAN_LIMIT
  CVAL_TMUX_SESSION=$SESSION_NAME
  CVAL_LOOP_SLEEP_SECONDS=$LOOP_SLEEP_SECONDS
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

resolve_git_ref() {
    if [[ -n "${CVAL_GIT_REF:-}" ]]; then
        printf '%s\n' "$CVAL_GIT_REF"
        return
    fi
    git -C "$SOURCE_REPO" fetch --quiet origin main
    git -C "$SOURCE_REPO" rev-parse origin/main
}

ensure_runner_worktree() {
    local git_ref="$1"
    mkdir -p "$(dirname "$RUNNER_WORKTREE")"
    if [[ ! -d "$RUNNER_WORKTREE" ]] || ! git -C "$RUNNER_WORKTREE" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        rm -rf "$RUNNER_WORKTREE"
        git -C "$SOURCE_REPO" worktree add --detach "$RUNNER_WORKTREE" "$git_ref"
    else
        git -C "$RUNNER_WORKTREE" fetch --quiet origin main
        git -C "$RUNNER_WORKTREE" checkout --quiet --detach "$git_ref"
    fi
}

json_job_count() {
    python - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    data = json.load(handle)
print(len(data.get("jobs", [])))
PY
}

json_submitted_jobs_csv() {
    python - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    data = json.load(handle)
jobs = [job["job_name"] for job in data.get("jobs", []) if job.get("submitted")]
print(",".join(jobs))
PY
}

json_submitted_jobs_csv_from_dir() {
    local cycle_dir="$1"
    python - "$cycle_dir" <<'PY'
import json
import sys
from pathlib import Path

cycle_dir = Path(sys.argv[1])
jobs = []
for path in sorted(cycle_dir.glob("submit*.json")):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        continue
    for job in data.get("jobs", []):
        if job.get("submitted") and job.get("job_name") not in jobs:
            jobs.append(job["job_name"])
print(",".join(jobs))
PY
}

json_nodes_csv() {
    python - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    data = json.load(handle)
nodes = [job["node"] for job in data.get("jobs", [])]
print(",".join(nodes))
PY
}

json_selected_nodes_csv() {
    local plan_file="$1"
    local limit="$2"
    shift 2
    python - "$plan_file" "$limit" "$@" <<'PY'
import json
import sys

plan_file = sys.argv[1]
limit = int(sys.argv[2])
excluded = set(sys.argv[3:])
with open(plan_file, encoding="utf-8") as handle:
    data = json.load(handle)
selected = []
for job in data.get("jobs", []):
    node = job.get("node")
    if not node or node in excluded:
        continue
    selected.append(node)
    if len(selected) >= limit:
        break
print(",".join(selected))
PY
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

latest_submit_file() {
    find "$LOG_DIR" -mindepth 2 -maxdepth 2 -name submit.json -printf '%T@ %p\n' 2>/dev/null \
        | sort -nr \
        | head -1 \
        | cut -d' ' -f2-
}

latest_cycle_dir_with_submits() {
    find "$LOG_DIR" -mindepth 2 -maxdepth 2 \( -name 'submit.json' -o -name 'submit-*.json' \) -printf '%T@ %h\n' 2>/dev/null \
        | sort -nr \
        | head -1 \
        | cut -d' ' -f2-
}

json_any_nonterminal() {
    python - "$1" <<'PY'
import json
import sys

active = {"Pending", "Running"}
with open(sys.argv[1], encoding="utf-8") as handle:
    phases = json.load(handle)
raise SystemExit(0 if any(item.get("phase") in active for item in phases) else 1)
PY
}

delete_job() {
    local job_name="$1"
    log "deleting pending job after timeout: $job_name"
    kubectl delete vcjob -n "$NAMESPACE" "$job_name" --ignore-not-found=true | tee -a "$2"
}

stale_pending_jobs() {
    kubectl get vcjob -n "$NAMESPACE" -o json \
        | python - "$JOB_PREFIX" "$PENDING_START_TIMEOUT_SECONDS" <<'PY'
import datetime as dt
import json
import sys
import time

prefix = sys.argv[1]
timeout_seconds = int(float(sys.argv[2]))
payload = json.load(sys.stdin)
now = int(time.time())

for item in payload.get("items", []):
    metadata = item.get("metadata", {})
    status = item.get("status", {})
    name = metadata.get("name", "")
    phase = status.get("state", {}).get("phase", "")
    created = metadata.get("creationTimestamp", "")
    if not name.startswith(prefix + "-") or phase != "Pending" or not created:
        continue
    created_epoch = int(dt.datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp())
    if now - created_epoch >= timeout_seconds:
        print(name)
PY
}

prune_stale_pending_jobs() {
    local cycle_dir="$1"
    local stale_jobs
    stale_jobs=$(stale_pending_jobs || true)
    if [[ -z "$stale_jobs" ]]; then
        return 0
    fi

    while IFS= read -r job_name; do
        [[ -n "$job_name" ]] || continue
        log "pruning stale pending c-val job: $job_name"
        delete_job "$job_name" "$cycle_dir/deleted-jobs.log"
    done <<< "$stale_jobs"
}

wait_for_jobs_once() {
    local cycle_dir="$1"
    local jobs_csv="$2"

    pushd "$RUNNER_WORKTREE" >/dev/null
    log "watching jobs once: $jobs_csv"
    if python -m cval.cli --config "$CONFIG_PATH" jobs \
        --jobs "$jobs_csv" \
        --watch \
        --timeout-seconds "$WATCH_TIMEOUT_SECONDS" \
        --poll-interval-seconds "$WATCH_POLL_SECONDS" \
        --output json | tee "$cycle_dir/monitor.json"; then
        log "capturing latest validation status"
        python -m cval.cli --config "$CONFIG_PATH" status --output json \
            | tee "$cycle_dir/status.json" >/dev/null
        popd >/dev/null
        return 0
    fi

    local rc=$?
    popd >/dev/null
    log "job watch failed with exit code $rc"
    return "$rc"
}

watch_jobs() {
    wait_for_jobs_once "$@"
}

watch_existing_jobs_until_clear() {
    local cycle_dir="$1"
    local active_jobs="$2"

    pushd "$RUNNER_WORKTREE" >/dev/null
    while [[ -n "$active_jobs" ]]; do
        local status_file="$cycle_dir/resume-status-$(date -u +%H%M%S).json"
        python -m cval.cli --config "$CONFIG_PATH" jobs --jobs "$active_jobs" --output json \
            | tee "$status_file"

        IFS=',' read -r -a active_array <<< "$active_jobs"
        for job_name in "${active_array[@]}"; do
            [[ -n "$job_name" ]] || continue
            local phase
            phase=$(json_phase "$status_file" "$job_name")
            log "resume job $job_name phase=$phase"

            case "$phase" in
                Completed|Succeeded|Failed|Aborted|Terminated|Unknown)
                    if [[ "$phase" == "Unknown" ]]; then
                        log "resume job $job_name is unknown; treating it as no longer active"
                    fi
                    active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                    ;;
                Pending)
                    local created_ts
                    created_ts=$(kubectl get vcjob -n "$NAMESPACE" "$job_name" -o jsonpath='{.metadata.creationTimestamp}' 2>/dev/null || true)
                    local created_epoch
                    created_epoch=$(python -c 'import datetime,sys; s=sys.stdin.read().strip(); print(int(datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()) if s else 0)' <<< "$created_ts")
                    local now_epoch
                    now_epoch=$(date +%s)
                    if [[ "$created_epoch" != "0" && $((now_epoch - created_epoch)) -ge "$PENDING_START_TIMEOUT_SECONDS" ]]; then
                        delete_job "$job_name" "$cycle_dir/deleted-jobs.log"
                        active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                    fi
                    ;;
            esac
        done

        [[ -z "$active_jobs" ]] && break
        sleep "$WATCH_POLL_SECONDS"
    done

    python -m cval.cli --config "$CONFIG_PATH" status --output json \
        | tee "$cycle_dir/status.json" >/dev/null
    popd >/dev/null
}

resume_latest_cycle_if_needed() {
    local cycle_dir
    cycle_dir=$(latest_cycle_dir_with_submits)
    if [[ -z "$cycle_dir" ]]; then
        return 1
    fi

    local jobs_csv
    jobs_csv=$(json_submitted_jobs_csv_from_dir "$cycle_dir")
    if [[ -z "$jobs_csv" ]]; then
        return 1
    fi

    mkdir -p "$cycle_dir"
    local git_ref
    git_ref=$(resolve_git_ref)
    ensure_runner_worktree "$git_ref"

    pushd "$RUNNER_WORKTREE" >/dev/null
    python -m cval.cli --config "$CONFIG_PATH" jobs --jobs "$jobs_csv" --output json \
        | tee "$cycle_dir/resume-status.json" >/dev/null
    popd >/dev/null

    if json_any_nonterminal "$cycle_dir/resume-status.json"; then
        log "resuming watch for active jobs from $cycle_dir: $jobs_csv"
        watch_existing_jobs_until_clear "$cycle_dir" "$jobs_csv"
        return 0
    fi

    log "latest submitted jobs are already terminal; no resume needed"
    return 1
}

run_cycle() {
    require_command git
    require_command kubectl
    require_command python

    mkdir -p "$LOG_DIR"
    local cycle_id
    cycle_id=$(date -u +%Y%m%dT%H%M%SZ)
    local cycle_dir="$LOG_DIR/$cycle_id"
    mkdir -p "$cycle_dir"

    local git_ref
    git_ref=$(resolve_git_ref)
    log "using git_ref=$git_ref"
    log "using config=$CONFIG_PATH"
    log "batch_size=$BATCH_SIZE days_threshold=$DAYS_THRESHOLD pending_start_timeout=${PENDING_START_TIMEOUT_SECONDS}s"

    ensure_runner_worktree "$git_ref"

    pushd "$RUNNER_WORKTREE" >/dev/null

    local active_jobs=""
    local submitted_nodes=""
    local completed_jobs=""
    local skipped_nodes=""
    local idle_rounds=0

    while true; do
        prune_stale_pending_jobs "$cycle_dir"

        local status_file="$cycle_dir/status-$(date -u +%H%M%S).json"
        if [[ -n "$active_jobs" ]]; then
            python -m cval.cli --config "$CONFIG_PATH" jobs --jobs "$active_jobs" --output json \
                | tee "$status_file" >/dev/null

            IFS=',' read -r -a active_array <<< "$active_jobs"
            for job_name in "${active_array[@]}"; do
                [[ -n "$job_name" ]] || continue
                local phase
                phase=$(json_phase "$status_file" "$job_name")
                local node
                node=$(job_node_from_name "$job_name")
                log "job $job_name phase=$phase"

                case "$phase" in
                    Completed|Succeeded|Failed|Aborted|Terminated|Unknown)
                        if [[ "$phase" == "Unknown" ]]; then
                            log "job $job_name is unknown; treating it as no longer active"
                        fi
                        active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                        completed_jobs=$(csv_append_unique "$completed_jobs" "$job_name")
                        ;;
                    Pending|Unknown|*)
                        local created_ts
                        created_ts=$(kubectl get vcjob -n "$NAMESPACE" "$job_name" -o jsonpath='{.metadata.creationTimestamp}' 2>/dev/null || true)
                        local created_epoch
                        created_epoch=$(python -c 'import datetime,sys; s=sys.stdin.read().strip(); print(int(datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()) if s else 0)' <<< "$created_ts")
                        local now_epoch
                        now_epoch=$(date +%s)
                        if [[ "$phase" == "Pending" && "$created_epoch" != "0" && $((now_epoch - created_epoch)) -ge "$PENDING_START_TIMEOUT_SECONDS" ]]; then
                            delete_job "$job_name" "$cycle_dir/deleted-jobs.log"
                            active_jobs=$(csv_remove_value "$active_jobs" "$job_name")
                            skipped_nodes=$(csv_append_unique "$skipped_nodes" "$node")
                        fi
                        ;;
                esac
            done
        fi

        local active_count
        active_count=$(csv_count "$active_jobs")
        local slots=$((BATCH_SIZE - active_count))
        while (( slots > 0 )); do
            local plan_file="$cycle_dir/dry-run-$(date -u +%H%M%S)-slot-$slots.json"
            log "rebuilding live ranked list for one open slot ($slots slot(s) available)"
            if ! python -m cval.cli --config "$CONFIG_PATH" run \
                --live-status \
                --threshold-days "$DAYS_THRESHOLD" \
                --batch-size "$PLAN_LIMIT" \
                --max-batch-size "$PLAN_LIMIT" \
                --timestamp "$(date +%s)" \
                --git-ref "$git_ref" \
                --output json | tee "$plan_file"; then
                log "dry-run planning failed; ending cycle so the loop can retry"
                break
            fi

            if [[ ! -s "$plan_file" ]]; then
                log "dry-run planning produced an empty plan file; ending cycle so the loop can retry"
                break
            fi

            local exclude_args=()
            IFS=',' read -r -a submitted_array <<< "$submitted_nodes"
            for node in "${submitted_array[@]}"; do [[ -n "$node" ]] && exclude_args+=("$node"); done
            IFS=',' read -r -a skipped_array <<< "$skipped_nodes"
            for node in "${skipped_array[@]}"; do [[ -n "$node" ]] && exclude_args+=("$node"); done

            local nodes_csv
            nodes_csv=$(json_selected_nodes_csv "$plan_file" 1 "${exclude_args[@]}")
            if [[ -n "$nodes_csv" ]]; then
                local run_timestamp
                run_timestamp=$(date +%s)
                local submit_file="$cycle_dir/submit-$run_timestamp.json"
                log "submitting node: $nodes_csv timestamp=$run_timestamp"
                python -m cval.cli --config "$CONFIG_PATH" run \
                    --free-nodes "$nodes_csv" \
                    --threshold-days "$DAYS_THRESHOLD" \
                    --batch-size 1 \
                    --timestamp "$run_timestamp" \
                    --git-ref "$git_ref" \
                    --submit \
                    --confirm "$CONFIRM_PHRASE" \
                    --output json | tee "$submit_file"

                local new_jobs
                new_jobs=$(json_submitted_jobs_csv "$submit_file")
                active_jobs=$(csv_append_unique "$active_jobs" "$new_jobs")

                IFS=',' read -r -a submitted_nodes_array <<< "$nodes_csv"
                for node in "${submitted_nodes_array[@]}"; do
                    submitted_nodes=$(csv_append_unique "$submitted_nodes" "$node")
                done
                idle_rounds=0
                slots=$((slots - 1))
            else
                idle_rounds=$((idle_rounds + 1))
                log "no additional eligible nodes for open slots"
                break
            fi
        done

        active_count=$(csv_count "$active_jobs")
        if (( active_count == 0 && idle_rounds > 0 )); then
            log "no active jobs and no eligible queued nodes; ending cycle"
            break
        fi

        sleep "$WATCH_POLL_SECONDS"
    done

    python -m cval.cli --config "$CONFIG_PATH" status --output json \
        | tee "$cycle_dir/status.json" >/dev/null

    popd >/dev/null
    log "cycle complete; artifacts in $cycle_dir"
}

run_loop() {
    trap 'log "received stop signal; exiting loop"; exit 0' INT TERM
    if resume_latest_cycle_if_needed; then
        log "sleeping $LOOP_SLEEP_SECONDS seconds before next cycle"
        sleep "$LOOP_SLEEP_SECONDS"
    fi

    while true; do
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
    local runner_cmd
    printf -v runner_cmd \
        'CVAL_CONFIG=%q CVAL_SOURCE_REPO=%q CVAL_LIVE_LOG_DIR=%q CVAL_RUNNER_WORKTREE=%q CVAL_BATCH_SIZE=%q CVAL_DAYS_THRESHOLD=%q CVAL_PENDING_START_TIMEOUT_SECONDS=%q CVAL_NAMESPACE=%q CVAL_LOOP_SLEEP_SECONDS=%q CVAL_WATCH_TIMEOUT_SECONDS=%q CVAL_WATCH_POLL_SECONDS=%q bash %q run-loop' \
        "$CONFIG_PATH" "$SOURCE_REPO" "$LOG_DIR" "$RUNNER_WORKTREE" "$BATCH_SIZE" "$DAYS_THRESHOLD" "$PENDING_START_TIMEOUT_SECONDS" "$NAMESPACE" "$LOOP_SLEEP_SECONDS" "$WATCH_TIMEOUT_SECONDS" "$WATCH_POLL_SECONDS" "$SCRIPT_PATH"

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
    start) start_session ;;
    stop) stop_session ;;
    attach) exec tmux attach -t "$SESSION_NAME" ;;
    status) show_status ;;
    run-once) run_cycle ;;
    run-loop) run_loop ;;
    -h|--help|help) usage ;;
    *) usage; exit 2 ;;
esac