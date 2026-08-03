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
DL_METRIC_LOCK_HELPER=${CVAL_DL_METRIC_LOCK_HELPER:-$SCRIPT_DIR/dl-metric-lock.py}
DL_METRIC_LOCK_PYTHON=${CVAL_DL_METRIC_LOCK_PYTHON:-python3}
DL_METRIC_REFRESH_INTERVAL_SECONDS=${CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS:-3600}
LOG_DIR=${CVAL_BASELINE_CLASSIFY_LOG_DIR:-$BASELINE_ROOT/logs/classify}
TEST_TYPES=${CVAL_BASELINE_CLASSIFY_TESTS:-}

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
    CVAL_DL_METRIC_LOCK_HELPER=$DL_METRIC_LOCK_HELPER
    CVAL_DL_METRIC_LOCK_PYTHON=$DL_METRIC_LOCK_PYTHON
    CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS=$DL_METRIC_REFRESH_INTERVAL_SECONDS
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
(for example, inside the gcr-admin PVC access pod). Local classification writes
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

target_is_allowed() {
    local requested="$1"
    [[ -n "$TEST_TYPES" ]] || return 0
    local allowed
    IFS=',' read -r -a allowlist <<< "$TEST_TYPES"
    for allowed in "${allowlist[@]}"; do
        allowed=${allowed//[[:space:]]/}
        [[ "$allowed" == "$requested" ]] && return 0
    done
    return 1
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
        log "classification failed for $test_type"
        return 1
    fi
}

classify_dl_tests() {
    local cycle_dir="$1"
    shift
    if ! refresh_dl_metric_dbs_if_needed "$cycle_dir"; then
        log "DL metric refresh failed; skipping DL classification target group"
        return 1
    fi
    local failed=0
    local test_type
    for test_type in "$@"; do
        if ! classify_one_test "$cycle_dir" "$test_type"; then
            failed=1
        fi
    done
    return "$failed"
}

run_cycle() {
    ensure_baseline_root_writable
    mkdir -p "$LOG_DIR"
    local cycle_id
    cycle_id=$(date -u +%Y%m%dT%H%M%SZ)
    local cycle_dir="$LOG_DIR/$cycle_id"
    mkdir -p "$cycle_dir"

    pushd "$REPO_DIR" >/dev/null
    log "baseline classification cycle start: root=$BASELINE_ROOT window_days=$WINDOW_DAYS tests=${TEST_TYPES:-all-enabled}"

    local catalog_file="$cycle_dir/operational-targets.tsv"
    if ! python -m cval.cli --config "$CONFIG_PATH" operational-targets \
        --operation baseline-classify --output tsv >"$catalog_file"; then
        log "could not enumerate baseline-classify targets"
        popd >/dev/null
        return 1
    fi

    local direct_tests=()
    local dl_tests=()
    local catalog_count=0
    local catalog_failed=0
    local line format_version test_type owner baseline_type status_test alias refresh_group
    local fields=()
    while IFS= read -r line; do
        [[ -n "$line" ]] || continue
        IFS=$'\t' read -r -a fields <<< "$line"
        if (( ${#fields[@]} != 7 )); then
            log "invalid classification target catalog row: expected 7 fields"
            catalog_failed=1
            continue
        fi
        format_version=${fields[0]}
        test_type=${fields[1]}
        owner=${fields[2]}
        baseline_type=${fields[3]}
        status_test=${fields[4]}
        alias=${fields[5]}
        refresh_group=${fields[6]}
        if [[ "$format_version" != "cval.operational-target.v1" \
            || -z "$test_type" || -z "$owner" || -z "$baseline_type" \
            || -z "$status_test" || ! "$alias" =~ ^(true|false)$ \
            || -z "$refresh_group" ]]; then
            log "invalid classification target catalog row: bad version or field value"
            catalog_failed=1
            continue
        fi
        [[ "$refresh_group" == "-" ]] && refresh_group=""
        ((catalog_count+=1))
        if ! target_is_allowed "$test_type"; then
            log "skipping $test_type classification (not in environment allowlist)"
            continue
        fi
        if [[ "$refresh_group" == "dltest" ]]; then
            dl_tests+=("$test_type")
        else
            direct_tests+=("$test_type")
        fi
    done <"$catalog_file"

    if (( catalog_failed != 0 )); then
        log "baseline-classify target catalog validation failed"
        popd >/dev/null
        return 1
    fi
    if (( catalog_count == 0 )); then
        log "no enabled baseline-classify targets were enumerated; refusing empty cycle"
        popd >/dev/null
        return 1
    fi
    if (( ${#direct_tests[@]} + ${#dl_tests[@]} == 0 )); then
        log "baseline classification allowlist intersects no enabled target; refusing empty cycle"
        popd >/dev/null
        return 1
    fi

    local cycle_failed=0
    for test_type in "${direct_tests[@]}"; do
        if ! classify_one_test "$cycle_dir" "$test_type"; then
            cycle_failed=1
        fi
    done
    if (( ${#dl_tests[@]} > 0 )); then
        if ! with_dl_metric_lock "baseline-classify" bash "$0" \
            run-dl-classifications "$cycle_dir" "${dl_tests[@]}"; then
            cycle_failed=1
        fi
    fi

    log "baseline classification cycle complete: artifacts=$cycle_dir failed=$cycle_failed"
    popd >/dev/null
    return "$cycle_failed"
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
        'CVAL_CONFIG=%q CVAL_BASELINE_ROOT=%q CVAL_BASELINE_CLASSIFY_INTERVAL_SECONDS=%q CVAL_BASELINE_WINDOW_DAYS=%q CVAL_BASELINE_CLASSIFY_TESTS=%q CVAL_DL_RESULTS_ROOT=%q CVAL_DL_METRIC_OUTPUT_DIR=%q CVAL_DL_METRIC_LOCK_FILE=%q CVAL_DL_METRIC_LOCK_HELPER=%q CVAL_DL_METRIC_LOCK_PYTHON=%q CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS=%q bash %q run-loop' \
        "$CONFIG_PATH" "$BASELINE_ROOT" "$INTERVAL_SECONDS" "$WINDOW_DAYS" "$TEST_TYPES" "$DL_RESULTS_ROOT" "$DL_METRIC_OUTPUT_DIR" "$DL_METRIC_LOCK_FILE" "$DL_METRIC_LOCK_HELPER" "$DL_METRIC_LOCK_PYTHON" "$DL_METRIC_REFRESH_INTERVAL_SECONDS" "$0"

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
    run-dl-classifications) shift; classify_dl_tests "$@" ;;
    -h|--help|help) usage ;;
    *) usage; exit 2 ;;
esac
