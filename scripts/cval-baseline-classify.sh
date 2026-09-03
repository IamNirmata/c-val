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
source "$SCRIPT_DIR/cval-baseline-common.sh"

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
LOCAL_WRITE_DESCRIPTION="classification writes"
LOOP_FAILURE_MESSAGE="baseline classification cycle failed"
LOOP_SLEEP_LABEL="classification"

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

build_runner_command() {
    printf \
        'CVAL_CONFIG=%q CVAL_BASELINE_ROOT=%q CVAL_BASELINE_CLASSIFY_INTERVAL_SECONDS=%q CVAL_BASELINE_WINDOW_DAYS=%q CVAL_BASELINE_CLASSIFY_TESTS=%q CVAL_DL_RESULTS_ROOT=%q CVAL_DL_METRIC_OUTPUT_DIR=%q CVAL_DL_METRIC_LOCK_FILE=%q CVAL_DL_METRIC_LOCK_HELPER=%q CVAL_DL_METRIC_LOCK_PYTHON=%q CVAL_DL_METRIC_REFRESH_INTERVAL_SECONDS=%q bash %q run-loop' \
        "$CONFIG_PATH" "$BASELINE_ROOT" "$INTERVAL_SECONDS" "$WINDOW_DAYS" "$TEST_TYPES" "$DL_RESULTS_ROOT" "$DL_METRIC_OUTPUT_DIR" "$DL_METRIC_LOCK_FILE" "$DL_METRIC_LOCK_HELPER" "$DL_METRIC_LOCK_PYTHON" "$DL_METRIC_REFRESH_INTERVAL_SECONDS" "$0"
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
