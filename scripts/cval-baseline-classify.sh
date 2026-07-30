#!/usr/bin/env bash
set -euo pipefail

# Periodically classify node metrics against active baselines. Intended to run
# where /data/continuous_validation is visible (for example, the gcr-admin PVC
# access pod) and manage itself in a tmux session.

COMMAND=${1:-start}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
CONFIG_PATH=${CVAL_CONFIG:-$REPO_DIR/config/cval.toml}
SESSION_NAME=${CVAL_BASELINE_CLASSIFY_TMUX_SESSION:-cval-baseline-classify}

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

BASELINE_ROOT=${CVAL_BASELINE_ROOT:-$(config_value baseline baseline_root_path /data/continuous_validation/baselines)}
INTERVAL_SECONDS=${CVAL_BASELINE_CLASSIFY_INTERVAL_SECONDS:-$(config_value baseline classify_interval_seconds 300)}
WINDOW_DAYS=${CVAL_BASELINE_WINDOW_DAYS:-$(config_value baseline window_days 30)}
DL_RESULTS_ROOT=${CVAL_DL_RESULTS_ROOT:-$(config_value runtime dl_results_root_path /data/continuous_validation/validation_tests/dltest/runs)}
DL_METRIC_OUTPUT_DIR=${CVAL_DL_METRIC_OUTPUT_DIR:-}
DL_NUMERICAL_DB=$(config_value storage dl_numerical_db_path /data/continuous_validation/metadata/dltest_numerical_correctness.db)
DL_COMPUTE_DB=$(config_value storage dl_compute_db_path /data/continuous_validation/metadata/dltest_compute_performance.db)
DL_COLLECTIVE_DB=$(config_value storage dl_collective_db_path /data/continuous_validation/metadata/dltest_collective_performance.db)
DL_OVERLAP_DB=$(config_value storage dl_overlap_db_path /data/continuous_validation/metadata/dltest_overlap_performance.db)
DL_METRIC_LOCK_FILE=${CVAL_DL_METRIC_LOCK_FILE:-$BASELINE_ROOT/.dl-metric-refresh.lock}
DL_METRIC_REFRESH_INTERVAL_SECONDS=${CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS:-3600}
LOG_DIR=${CVAL_BASELINE_CLASSIFY_LOG_DIR:-$BASELINE_ROOT/logs/classify}
TEST_TYPES=${CVAL_BASELINE_CLASSIFY_TESTS:-storage,nccl,dltest,dltest-numerical,dltest-compute,dltest-collective,dltest-overlap}
STORAGE_ENABLED=${CVAL_STORAGE_ENABLED:-$(config_value tests.storage enabled true)}
NCCL_ENABLED=${CVAL_NCCL_ENABLED:-$(config_value tests.nccl enabled true)}
DLTEST_ENABLED=${CVAL_DLTEST_ENABLED:-$(config_value tests.dltest enabled true)}

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  start      Start tmux session '$SESSION_NAME' running periodic classification
  stop       Stop the tmux session
  attach     Attach to the tmux session
  status     Show session status and latest log tail
  run-once   Classify once in the current shell
  run-loop   Internal: run classification forever

Environment overrides:
  CVAL_CONFIG=$CONFIG_PATH
  CVAL_BASELINE_ROOT=$BASELINE_ROOT
  CVAL_BASELINE_CLASSIFY_INTERVAL_SECONDS=$INTERVAL_SECONDS
  CVAL_BASELINE_WINDOW_DAYS=$WINDOW_DAYS
  CVAL_BASELINE_CLASSIFY_TESTS=$TEST_TYPES
    CVAL_DL_RESULTS_ROOT=$DL_RESULTS_ROOT
    CVAL_DL_METRIC_OUTPUT_DIR=$DL_METRIC_OUTPUT_DIR
        CVAL_DL_METRIC_LOCK_FILE=$DL_METRIC_LOCK_FILE
    CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS=$DL_METRIC_REFRESH_INTERVAL_SECONDS
EOF
}

log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

is_enabled() {
    case "${1,,}" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

test_is_enabled() {
    case "$1" in
        storage) is_enabled "$STORAGE_ENABLED" ;;
        nccl) is_enabled "$NCCL_ENABLED" ;;
        dltest|dltest-*) is_enabled "$DLTEST_ENABLED" ;;
        *) return 1 ;;
    esac
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

dl_metric_dbs_are_fresh() {
    local now
    now=$(date +%s)
    local db
    local db_paths=("$DL_NUMERICAL_DB" "$DL_COMPUTE_DB" "$DL_COLLECTIVE_DB" "$DL_OVERLAP_DB")
    if [[ -n "$DL_METRIC_OUTPUT_DIR" ]]; then
        db_paths=(
            "$DL_METRIC_OUTPUT_DIR/dltest_numerical_correctness.db"
            "$DL_METRIC_OUTPUT_DIR/dltest_compute_performance.db"
            "$DL_METRIC_OUTPUT_DIR/dltest_collective_performance.db"
            "$DL_METRIC_OUTPUT_DIR/dltest_overlap_performance.db"
        )
    fi
    for db in "${db_paths[@]}"; do
        [[ -s "$db" ]] || return 1
        local mtime
        mtime=$(stat -c %Y "$db")
        if (( now - mtime >= DL_METRIC_REFRESH_INTERVAL_SECONDS )); then
            return 1
        fi
    done
    return 0
}

refresh_dl_metric_dbs_if_needed() {
    local cycle_dir="$1"
    if (( DL_METRIC_REFRESH_INTERVAL_SECONDS > 0 )) && dl_metric_dbs_are_fresh; then
        log "DL metric DBs are fresh; skipping rebuild (interval=${DL_METRIC_REFRESH_INTERVAL_SECONDS}s)"
        printf '{"skipped": true, "reason": "fresh", "interval_seconds": %s}\n' \
            "$DL_METRIC_REFRESH_INTERVAL_SECONDS" | tee "$cycle_dir/dltest-ingest.json"
        return 0
    fi
    refresh_dl_metric_dbs "$cycle_dir"
}

with_dl_metric_lock() {
    local label="$1"
    shift
    mkdir -p "$(dirname "$DL_METRIC_LOCK_FILE")"
    if command -v flock >/dev/null 2>&1; then
        log "waiting for DL metric lock: $DL_METRIC_LOCK_FILE ($label)"
        (
            flock -x 9
            log "acquired DL metric lock ($label)"
            "$@"
        ) 9>"$DL_METRIC_LOCK_FILE"
    else
        log "flock not found; running without DL metric lock ($label)"
        "$@"
    fi
}

is_dl_test() {
    [[ "$1" == "dltest" || "$1" == dltest-* ]]
}

tests_include_dltest() {
    [[ ",$TEST_TYPES," == *",dltest,"* ]] || [[ ",$TEST_TYPES," == *",dltest-"* ]]
}

classify_one_test() {
    local cycle_dir="$1"
    local test_type="$2"
    log "classifying $test_type against active baseline"
    if ! python -m cval.cli --config "$CONFIG_PATH" baseline classify \
        --test-type "$test_type" \
        --window-days "$WINDOW_DAYS" \
        --store-results \
        --output json | tee "$cycle_dir/${test_type}.json"; then
        log "classification skipped or failed for $test_type"
    fi
}

classify_dl_tests() {
    local cycle_dir="$1"
    shift
    refresh_dl_metric_dbs_if_needed "$cycle_dir"
    for test_type in "$@"; do
        classify_one_test "$cycle_dir" "$test_type"
    done
}

run_cycle() {
    ensure_baseline_root_writable
    mkdir -p "$LOG_DIR"
    local cycle_id
    cycle_id=$(date -u +%Y%m%dT%H%M%SZ)
    local cycle_dir="$LOG_DIR/$cycle_id"
    mkdir -p "$cycle_dir"

    pushd "$REPO_DIR" >/dev/null
    log "baseline classification cycle start: root=$BASELINE_ROOT window_days=$WINDOW_DAYS tests=$TEST_TYPES"

    IFS=',' read -r -a tests <<< "$TEST_TYPES"
    local dl_tests=()
    for test_type in "${tests[@]}"; do
        test_type=$(echo "$test_type" | xargs)
        [[ -n "$test_type" ]] || continue
        if ! test_is_enabled "$test_type"; then
            log "skipping $test_type classification (test disabled)"
            continue
        fi
        if is_dl_test "$test_type"; then
            dl_tests+=("$test_type")
        else
            classify_one_test "$cycle_dir" "$test_type"
        fi
    done

    if (( ${#dl_tests[@]} > 0 )); then
        with_dl_metric_lock "baseline-classify" classify_dl_tests "$cycle_dir" "${dl_tests[@]}"
    fi

    log "baseline classification cycle complete: artifacts=$cycle_dir"
    popd >/dev/null
}

run_loop() {
    trap 'log "received stop signal; exiting loop"; exit 0' INT TERM
    while true; do
        if ! run_cycle; then
            log "baseline classification cycle failed"
        fi
        log "sleeping $INTERVAL_SECONDS seconds before next classification"
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
        'CVAL_CONFIG=%q CVAL_BASELINE_ROOT=%q CVAL_BASELINE_CLASSIFY_INTERVAL_SECONDS=%q CVAL_BASELINE_WINDOW_DAYS=%q CVAL_BASELINE_CLASSIFY_TESTS=%q CVAL_DL_RESULTS_ROOT=%q CVAL_DL_METRIC_OUTPUT_DIR=%q CVAL_DL_METRIC_LOCK_FILE=%q CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS=%q bash %q run-loop' \
        "$CONFIG_PATH" "$BASELINE_ROOT" "$INTERVAL_SECONDS" "$WINDOW_DAYS" "$TEST_TYPES" "$DL_RESULTS_ROOT" "$DL_METRIC_OUTPUT_DIR" "$DL_METRIC_LOCK_FILE" "$DL_METRIC_REFRESH_INTERVAL_SECONDS" "$0"

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
