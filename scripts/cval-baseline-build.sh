#!/usr/bin/env bash
set -euo pipefail

# Build dynamic baselines on a cadence. Intended to run in the environment that
# can see /data/continuous_validation (for example, the gcr-admin PVC access
# pod) and manage itself in a tmux session.

COMMAND=${1:-start}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
CONFIG_PATH=${CVAL_CONFIG:-$REPO_DIR/config/cval.toml}
SESSION_NAME=${CVAL_BASELINE_BUILD_TMUX_SESSION:-cval-baseline-build}

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

BASELINE_ROOT=${CVAL_BASELINE_ROOT:-$(config_value baseline baseline_root_path /data/continuous_validation/baselines)}
INTERVAL_SECONDS=${CVAL_BASELINE_BUILD_INTERVAL_SECONDS:-$(config_value baseline build_interval_seconds 86400)}
WINDOW_DAYS=${CVAL_BASELINE_WINDOW_DAYS:-$(config_value baseline window_days 30)}
MIN_SAMPLES=${CVAL_BASELINE_MIN_SAMPLES:-$(config_value baseline min_samples 8)}
DL_TEST_PLAN=${CVAL_BASELINE_DL_TEST_PLAN:-$(config_value validation dl_test_plan 80gb-example)}
DL_RESULTS_ROOT=${CVAL_DL_RESULTS_ROOT:-$(config_value runtime dl_results_root_path /data/continuous_validation/dltest)}
DL_METRIC_OUTPUT_DIR=${CVAL_DL_METRIC_OUTPUT_DIR:-$(dirname "$(config_value storage dl_numerical_db_path /data/continuous_validation/metadata/dltest_numerical_correctness.db)")}
LOG_DIR=${CVAL_BASELINE_BUILD_LOG_DIR:-$BASELINE_ROOT/logs/build}

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  start      Start tmux session '$SESSION_NAME' running daily baseline builds
  stop       Stop the tmux session
  attach     Attach to the tmux session
  status     Show session status and latest log tail
  run-once   Build baselines once in the current shell
  run-loop   Internal: run baseline builds forever

Environment overrides:
  CVAL_CONFIG=$CONFIG_PATH
  CVAL_BASELINE_ROOT=$BASELINE_ROOT
  CVAL_BASELINE_BUILD_INTERVAL_SECONDS=$INTERVAL_SECONDS
  CVAL_BASELINE_WINDOW_DAYS=$WINDOW_DAYS
  CVAL_BASELINE_MIN_SAMPLES=$MIN_SAMPLES
  CVAL_BASELINE_DL_TEST_PLAN=$DL_TEST_PLAN
    CVAL_DL_RESULTS_ROOT=$DL_RESULTS_ROOT
    CVAL_DL_METRIC_OUTPUT_DIR=$DL_METRIC_OUTPUT_DIR
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

ensure_baseline_root_writable() {
    if ! mkdir -p "$BASELINE_ROOT" 2>/dev/null; then
        cat >&2 <<EOF
Cannot create baseline root: $BASELINE_ROOT

This usually means you are running on a machine that does not have the c-val PVC
mounted at /data. Run this script where /data/continuous_validation is mounted
(for example, inside the gcr-admin PVC access pod), or override CVAL_BASELINE_ROOT
for local testing:

  CVAL_BASELINE_ROOT=/tmp/cval-baselines $0 run-once
EOF
        return 1
    fi
    if [[ ! -w "$BASELINE_ROOT" ]]; then
        cat >&2 <<EOF
Baseline root is not writable: $BASELINE_ROOT

Run inside the PVC-mounted environment or override CVAL_BASELINE_ROOT for local
testing.
EOF
        return 1
    fi
}

refresh_dl_metric_dbs() {
    log "refreshing DL metric DBs from $DL_RESULTS_ROOT -> $DL_METRIC_OUTPUT_DIR"
    python -m cval.cli --config "$CONFIG_PATH" db-rebuild-dltest-metrics \
        --results-root "$DL_RESULTS_ROOT" \
        --output-dir "$DL_METRIC_OUTPUT_DIR" \
        --output json | tee "$1/dltest-ingest.json"
}

run_cycle() {
    ensure_baseline_root_writable
    mkdir -p "$LOG_DIR"
    local cycle_id
    cycle_id=$(date -u +%Y%m%dT%H%M%SZ)
    local cycle_dir="$LOG_DIR/$cycle_id"
    mkdir -p "$cycle_dir"

    pushd "$REPO_DIR" >/dev/null
    log "baseline build cycle start: root=$BASELINE_ROOT window_days=$WINDOW_DAYS min_samples=$MIN_SAMPLES"

    local baseline_id
    baseline_id="auto-$(date -u +%Y%m%dT%H%M%SZ)"

    for test_type in storage nccl; do
        log "building $test_type baseline_id=${test_type}-${baseline_id}"
        python -m cval.cli --config "$CONFIG_PATH" baseline build \
            --test-type "$test_type" \
            --window-days "$WINDOW_DAYS" \
            --min-samples "$MIN_SAMPLES" \
            --baseline-id "${test_type}-${baseline_id}" \
            --activate \
            --output json | tee "$cycle_dir/${test_type}.json"
    done

    refresh_dl_metric_dbs "$cycle_dir"

    log "building dltest baseline_id=dltest-${baseline_id} test_plan=$DL_TEST_PLAN"
    python -m cval.cli --config "$CONFIG_PATH" baseline build \
        --test-type dltest \
        --test-plan "$DL_TEST_PLAN" \
        --window-days "$WINDOW_DAYS" \
        --min-samples "$MIN_SAMPLES" \
        --baseline-id "dltest-${baseline_id}" \
        --activate \
        --output json | tee "$cycle_dir/dltest.json"

    log "baseline build cycle complete: artifacts=$cycle_dir"
    popd >/dev/null
}

run_loop() {
    trap 'log "received stop signal; exiting loop"; exit 0' INT TERM
    while true; do
        if ! run_cycle; then
            log "baseline build cycle failed"
        fi
        log "sleeping $INTERVAL_SECONDS seconds before next baseline build"
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
    printf -v runner_cmd \
        'CVAL_CONFIG=%q CVAL_BASELINE_ROOT=%q CVAL_BASELINE_BUILD_INTERVAL_SECONDS=%q CVAL_BASELINE_WINDOW_DAYS=%q CVAL_BASELINE_MIN_SAMPLES=%q CVAL_BASELINE_DL_TEST_PLAN=%q CVAL_DL_RESULTS_ROOT=%q CVAL_DL_METRIC_OUTPUT_DIR=%q bash %q run-loop' \
        "$CONFIG_PATH" "$BASELINE_ROOT" "$INTERVAL_SECONDS" "$WINDOW_DAYS" "$MIN_SAMPLES" "$DL_TEST_PLAN" "$DL_RESULTS_ROOT" "$DL_METRIC_OUTPUT_DIR" "$0"

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
