#!/usr/bin/env bash
set -u -o pipefail

# Run the JSON-native dl_unit_test package and summarize rank JSON outputs.

GCRNODE=${GCRNODE:-unknown}
GCRTIME=${GCRTIME:-unknown}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}
CVAL_DL_UNIT_TEST_DIR=${CVAL_DL_UNIT_TEST_DIR:-$CVAL_VALIDATION_ROOT/deep-learning-unit-test-main}
CVAL_DL_TEST_PLAN=${CVAL_DL_TEST_PLAN:-80gb-example}
CVAL_DL_ITERATIONS=${CVAL_DL_ITERATIONS:-100}
GPU_COUNT=${1:-${CVAL_DL_GPU_COUNT:-8}}
CVAL_RUN_ID=${CVAL_RUN_ID:-$GCRNODE-$GCRTIME}
DLTEST_RUN_DIR=${DLTEST_RUN_DIR:-\
$CVAL_VALIDATION_ROOT/validation_tests/dltest/runs/$GCRNODE/$CVAL_RUN_ID}
DLTEST_OUTPUT_DIR=${DLTEST_OUTPUT_DIR:-\
${CVAL_TEST_OUTPUT_DIR:-}}
DLTEST_OUTPUT_DIR=${DLTEST_OUTPUT_DIR:-$DLTEST_RUN_DIR/artifacts}
DLTEST_WORK_DIR=${DLTEST_WORK_DIR:-$DLTEST_OUTPUT_DIR/workdir}
DLTEST_RUNS_DIR="$DLTEST_WORK_DIR/test_plans/$CVAL_DL_TEST_PLAN/runs"
DLTEST_LOG_FILE=${DLTEST_LOG_FILE:-${CVAL_TEST_LOG_DIR:-$DLTEST_RUN_DIR}/workload.log}
DLTEST_SUMMARY_FILE=${DLTEST_SUMMARY_FILE:-\
${CVAL_TEST_SUMMARY_FILE:-}}
DLTEST_SUMMARY_FILE=${DLTEST_SUMMARY_FILE:-$DLTEST_RUN_DIR/summary.json}
SUMMARY_SCRIPT="$CVAL_VALIDATION_TESTS_DIR/dltest/summarize_results.py"

export GCRNODE GCRTIME DLTEST_OUTPUT_DIR DLTEST_LOG_FILE DLTEST_SUMMARY_FILE

echo "Running DL Test on node: $GCRNODE at time: $GCRTIME"

find_source_plan_dir() {
    local candidate
    for candidate in \
        "$CVAL_DL_UNIT_TEST_DIR/test_plans/$CVAL_DL_TEST_PLAN" \
        "$CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test/test_plans/$CVAL_DL_TEST_PLAN"; do
        if [[ -f "$candidate/test_plan.json" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    echo "DL test plan '$CVAL_DL_TEST_PLAN' not found under $CVAL_DL_UNIT_TEST_DIR" >&2
    return 1
}

prepare_workdir() {
    local source_plan_dir
    local target_plan_dir="$DLTEST_WORK_DIR/test_plans/$CVAL_DL_TEST_PLAN"
    local output_real
    local work_real

    if [[ ! "$CVAL_DL_TEST_PLAN" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
        echo "Unsafe DL test plan name: $CVAL_DL_TEST_PLAN" >&2
        return 1
    fi
    mkdir -p "$DLTEST_OUTPUT_DIR"
    output_real=$(realpath -m -- "$DLTEST_OUTPUT_DIR")
    work_real=$(realpath -m -- "$DLTEST_WORK_DIR")
    if [[ "$work_real" != "$output_real/workdir" || -L "$DLTEST_WORK_DIR" ]]; then
        echo "Unsafe DL test work directory: $DLTEST_WORK_DIR" >&2
        return 1
    fi
    if [[ ! -d "$CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test" ]]; then
        echo "DL unit test package not found under $CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test" >&2
        return 1
    fi
    source_plan_dir=$(find_source_plan_dir) || return 1

    rm -rf "$DLTEST_WORK_DIR"
    mkdir -p "$target_plan_dir" "$DLTEST_OUTPUT_DIR"
    cp "$source_plan_dir/test_plan.json" "$target_plan_dir/test_plan.json"
    if [[ -d "$source_plan_dir/baseline" ]]; then
        cp -a "$source_plan_dir/baseline" "$target_plan_dir/baseline"
    fi
}

write_summary() {
    local status="$1"
    python3 "$SUMMARY_SCRIPT" \
        --runs-dir "$DLTEST_RUNS_DIR" \
        --summary-file "$DLTEST_SUMMARY_FILE" \
        --status "$status" \
        --test-plan "$CVAL_DL_TEST_PLAN" \
        --iterations "$CVAL_DL_ITERATIONS" \
        --gpu-count "$GPU_COUNT" \
        --log-file "$DLTEST_LOG_FILE" \
        --source-dir "$CVAL_DL_UNIT_TEST_DIR" \
        --work-dir "$DLTEST_WORK_DIR"
}

mkdir -p "$DLTEST_OUTPUT_DIR"
if ! prepare_workdir; then
    echo "DL Test setup FAILED. Check log file: $DLTEST_LOG_FILE"
    write_summary fail || true
    exit 1
fi

echo "DL unit test source: $CVAL_DL_UNIT_TEST_DIR"
echo "DL test working dir: $DLTEST_WORK_DIR"
echo "DL test runs dir: $DLTEST_RUNS_DIR"
echo "DL summary file: $DLTEST_SUMMARY_FILE"
echo "DL test plan: $CVAL_DL_TEST_PLAN"
echo "DL iterations: $CVAL_DL_ITERATIONS"
echo "DL GPU count: $GPU_COUNT"

(
    cd "$DLTEST_WORK_DIR" || exit 1
    PYTHONPATH="$CVAL_DL_UNIT_TEST_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
        torchrun --nnodes=1 --nproc_per_node="$GPU_COUNT" -m dl_unit_test \
            --test_plan "$CVAL_DL_TEST_PLAN" \
            --iterations "$CVAL_DL_ITERATIONS"
)
rc=$?

if [[ $rc -ne 0 ]]; then
    echo "DL Test torchrun FAILED with rc=$rc"
    echo "Check log file: $DLTEST_LOG_FILE"
    write_summary fail || true
    exit "$rc"
fi

if write_summary pass; then
    echo "DL Test completed successfully. Log file: $DLTEST_LOG_FILE "\
"Summary file: $DLTEST_SUMMARY_FILE"
    exit 0
fi

echo "DL Test summary validation FAILED. Check summary file: $DLTEST_SUMMARY_FILE"
exit 1
