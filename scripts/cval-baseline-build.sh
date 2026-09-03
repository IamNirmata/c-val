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
source "$SCRIPT_DIR/cval-baseline-common.sh"

BASELINE_ROOT=${CVAL_BASELINE_ROOT:-$(config_value baseline baseline_root_path /data/continuous_validation/baselines)}
INTERVAL_SECONDS=${CVAL_BASELINE_BUILD_INTERVAL_SECONDS:-$(config_value baseline build_interval_seconds 86400)}
WINDOW_DAYS=${CVAL_BASELINE_WINDOW_DAYS:-$(config_value baseline window_days 30)}
MIN_SAMPLES=${CVAL_BASELINE_MIN_SAMPLES:-$(config_value baseline min_samples 8)}
DL_TEST_PLAN=${CVAL_BASELINE_DL_TEST_PLAN:-$(config_value tests.dltest.settings test_plan 80gb-example)}
DL_RESULTS_ROOT=${CVAL_DL_RESULTS_ROOT:-$(config_value runtime dl_results_root_path /data/continuous_validation/validation_tests/dltest/runs)}
DL_METRIC_OUTPUT_DIR=${CVAL_DL_METRIC_OUTPUT_DIR:-}
DL_METRIC_LOCK_FILE=${CVAL_DL_METRIC_LOCK_FILE:-$BASELINE_ROOT/.dl-metric-refresh.lock}
DL_METRIC_LOCK_HELPER=${CVAL_DL_METRIC_LOCK_HELPER:-$SCRIPT_DIR/dl-metric-lock.py}
DL_METRIC_LOCK_PYTHON=${CVAL_DL_METRIC_LOCK_PYTHON:-python3}
LOG_DIR=${CVAL_BASELINE_BUILD_LOG_DIR:-$BASELINE_ROOT/logs/build}
LOCAL_WRITE_DESCRIPTION="baseline writes"
LOOP_FAILURE_MESSAGE="baseline build cycle failed"
LOOP_SLEEP_LABEL="baseline build"

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
    CVAL_DL_METRIC_LOCK_FILE=$DL_METRIC_LOCK_FILE
    CVAL_DL_METRIC_LOCK_HELPER=$DL_METRIC_LOCK_HELPER
    CVAL_DL_METRIC_LOCK_PYTHON=$DL_METRIC_LOCK_PYTHON
EOF
}

build_one_target() {
    local cycle_dir="$1"
    local test_type="$2"
    local baseline_id="$3"
    local refresh_group="$4"
    local args=(
        python -m cval.cli --config "$CONFIG_PATH" baseline build
        --test-type "$test_type"
        --window-days "$WINDOW_DAYS" \
        --min-samples "$MIN_SAMPLES" \
        --baseline-id "${test_type}-${baseline_id}"
        --activate \
        --output json
    )
    if [[ "$refresh_group" == "dltest" ]]; then
        args+=(--test-plan "$DL_TEST_PLAN")
    fi
    log "building $test_type baseline_id=${test_type}-${baseline_id}"
    if ! "${args[@]}" | tee "$cycle_dir/${test_type}.json"; then
        log "baseline build failed for $test_type"
        return 1
    fi
}

run_dl_baseline_builds() {
    local cycle_dir="$1"
    local baseline_id="$2"
    shift 2
    if ! refresh_dl_metric_dbs "$cycle_dir"; then
        log "DL metric refresh failed; skipping DL baseline target group"
        return 1
    fi
    local failed=0
    local test_type
    for test_type in "$@"; do
        if ! build_one_target "$cycle_dir" "$test_type" "$baseline_id" dltest; then
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
    log "baseline build cycle start: root=$BASELINE_ROOT window_days=$WINDOW_DAYS min_samples=$MIN_SAMPLES"

    local baseline_id
    baseline_id="auto-$(date -u +%Y%m%dT%H%M%SZ)"
    local catalog_file="$cycle_dir/operational-targets.tsv"
    if ! python -m cval.cli --config "$CONFIG_PATH" operational-targets \
        --operation baseline-build --output tsv >"$catalog_file"; then
        log "could not enumerate baseline-build targets"
        popd >/dev/null
        return 1
    fi

    local direct_targets=()
    local dl_targets=()
    local catalog_failed=0
    local line format_version test_type owner baseline_type status_test alias refresh_group
    local fields=()
    while IFS= read -r line; do
        [[ -n "$line" ]] || continue
        IFS=$'\t' read -r -a fields <<< "$line"
        if (( ${#fields[@]} != 7 )); then
            log "invalid baseline target catalog row: expected 7 fields"
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
            log "invalid baseline target catalog row: bad version or field value"
            catalog_failed=1
            continue
        fi
        [[ "$refresh_group" == "-" ]] && refresh_group=""
        if [[ "$refresh_group" == "dltest" ]]; then
            dl_targets+=("$test_type")
        else
            direct_targets+=("$test_type")
        fi
    done <"$catalog_file"

    if (( catalog_failed != 0 )); then
        log "baseline-build target catalog validation failed"
        popd >/dev/null
        return 1
    fi
    if (( ${#direct_targets[@]} + ${#dl_targets[@]} == 0 )); then
        log "no enabled baseline-build targets were enumerated; refusing empty cycle"
        popd >/dev/null
        return 1
    fi

    local cycle_failed=0
    for test_type in "${direct_targets[@]}"; do
        if ! build_one_target "$cycle_dir" "$test_type" "$baseline_id" ""; then
            cycle_failed=1
        fi
    done
    if (( ${#dl_targets[@]} > 0 )); then
        if ! with_dl_metric_lock "baseline-build" bash "$0" \
            run-dl-baseline-builds "$cycle_dir" "$baseline_id" \
            "${dl_targets[@]}"; then
            cycle_failed=1
        fi
    fi

    log "baseline build cycle complete: artifacts=$cycle_dir failed=$cycle_failed"
    popd >/dev/null
    return "$cycle_failed"
}

build_runner_command() {
    printf \
        'CVAL_CONFIG=%q CVAL_BASELINE_ROOT=%q CVAL_BASELINE_BUILD_INTERVAL_SECONDS=%q CVAL_BASELINE_WINDOW_DAYS=%q CVAL_BASELINE_MIN_SAMPLES=%q CVAL_BASELINE_DL_TEST_PLAN=%q CVAL_DL_RESULTS_ROOT=%q CVAL_DL_METRIC_OUTPUT_DIR=%q CVAL_DL_METRIC_LOCK_FILE=%q CVAL_DL_METRIC_LOCK_HELPER=%q CVAL_DL_METRIC_LOCK_PYTHON=%q bash %q run-loop' \
        "$CONFIG_PATH" "$BASELINE_ROOT" "$INTERVAL_SECONDS" "$WINDOW_DAYS" "$MIN_SAMPLES" "$DL_TEST_PLAN" "$DL_RESULTS_ROOT" "$DL_METRIC_OUTPUT_DIR" "$DL_METRIC_LOCK_FILE" "$DL_METRIC_LOCK_HELPER" "$DL_METRIC_LOCK_PYTHON" "$0"
}

case "$COMMAND" in
    start) start_session ;;
    stop) stop_session ;;
    attach) exec tmux attach -t "$SESSION_NAME" ;;
    status) show_status ;;
    run-once) run_cycle ;;
    run-loop) run_loop ;;
    run-dl-baseline-builds) shift; run_dl_baseline_builds "$@" ;;
    -h|--help|help) usage ;;
    *) usage; exit 2 ;;
esac
