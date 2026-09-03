#!/usr/bin/env bash

# Shared runtime for the baseline build and classification launchers.

config_value() {
    local section="$1"
    local key="$2"
    local default_value="$3"
    PYTHONPATH="$REPO_DIR" python - "$CONFIG_PATH" "$section" "$key" "$default_value" <<'PY'
import sys
from pathlib import Path

from cval.config import config_to_dict, load_config

path, section, key, default = sys.argv[1:]
try:
    data = config_to_dict(load_config(Path(path)))
    current = data
    for part in section.split("."):
        current = current.get(part, {}) if isinstance(current, dict) else {}
    value = current.get(key, default) if isinstance(current, dict) else default
except FileNotFoundError:
    value = default
print(value)
PY
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

ensure_baseline_root_writable() {
    if ! mkdir -p "$BASELINE_ROOT" 2>/dev/null; then
        cat >&2 <<EOF
Cannot create baseline root: $BASELINE_ROOT

This usually means you are running on a machine that does not have the c-val PVC
mounted at /data. Run this script where /data/continuous_validation is mounted
(for example, inside the gcr-admin PVC access pod). Local $LOCAL_WRITE_DESCRIPTION
are not part of the cluster-first development workflow.
EOF
        return 1
    fi
    if [[ ! -w "$BASELINE_ROOT" ]]; then
        cat >&2 <<EOF
Baseline root is not writable: $BASELINE_ROOT

Run inside the reviewed PVC-mounted environment.
EOF
        return 1
    fi
}

refresh_dl_metric_dbs() {
    local target=${DL_METRIC_OUTPUT_DIR:-configured-db-paths}
    local args=(
        python -m cval.cli --config "$CONFIG_PATH" db-rebuild-dltest-metrics
        --results-root "$DL_RESULTS_ROOT"
        --output json
    )
    if [[ -n "$DL_METRIC_OUTPUT_DIR" ]]; then
        args+=(--output-dir "$DL_METRIC_OUTPUT_DIR")
    fi
    log "refreshing DL metric DBs from $DL_RESULTS_ROOT -> $target"
    "${args[@]}" | tee "$1/dltest-ingest.json"
}

with_dl_metric_lock() {
    local label="$1"
    shift
    local lock_python
    if ! lock_python=$(command -v "$DL_METRIC_LOCK_PYTHON" 2>/dev/null) || [[ ! -x "$lock_python" ]]; then
        log "DL metric lock Python unavailable; refusing unlocked work ($label)"
        return 1
    fi
    if [[ ! -f "$DL_METRIC_LOCK_HELPER" ]]; then
        log "DL metric lock helper unavailable; refusing unlocked work ($label)"
        return 1
    fi
    if ! mkdir -p "$(dirname "$DL_METRIC_LOCK_FILE")"; then
        log "could not prepare DL metric lock directory; refusing work ($label)"
        return 1
    fi
    log "waiting for DL metric lock: $DL_METRIC_LOCK_FILE ($label)"
    if ! "$lock_python" "$DL_METRIC_LOCK_HELPER" \
        "$DL_METRIC_LOCK_FILE" -- "$@"; then
        log "could not safely acquire DL metric lock; refusing work ($label)"
        return 1
    fi
}

run_loop() {
    trap 'log "received stop signal; exiting loop"; exit 0' INT TERM
    while true; do
        if ! run_cycle; then
            log "$LOOP_FAILURE_MESSAGE"
        fi
        log "sleeping $INTERVAL_SECONDS seconds before next $LOOP_SLEEP_LABEL"
        sleep "$INTERVAL_SECONDS"
    done
}

start_session() {
    require_command tmux
    ensure_baseline_root_writable
    mkdir -p "$LOG_DIR"
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "tmux session already running: $SESSION_NAME"
        echo "Attach with: $0 attach"
        return 0
    fi

    local session_log="$LOG_DIR/tmux-$(date -u +%Y%m%dT%H%M%SZ).log"
    local runner_cmd
    runner_cmd=$(build_runner_command)
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