#!/usr/bin/env bash
set -u -o pipefail

# Run the JSON-native dl_unit_test package and summarize rank JSON outputs for c-val.

echo "Running DL Test on node: $GCRNODE at time: $GCRTIME"
GPU_COUNT=${1:-${CVAL_GPU_COUNT:-8}}
CVAL_DL_UNIT_TEST_DIR=${CVAL_DL_UNIT_TEST_DIR:-/data/continuous_validation/deep-learning-unit-test-main}
CVAL_DL_TEST_PLAN=${CVAL_DL_TEST_PLAN:-80gb-example}
CVAL_DL_ITERATIONS=${CVAL_DL_ITERATIONS:-100}
DLTEST_WORK_DIR=${DLTEST_WORK_DIR:-$DLTEST_OUTPUT_DIR/workdir}
DLTEST_RUNS_DIR="$DLTEST_WORK_DIR/test_plans/$CVAL_DL_TEST_PLAN/runs"
DLTEST_SUMMARY_FILE=${DLTEST_SUMMARY_FILE:-$DLTEST_OUTPUT_DIR/dltest-summary-$GCRNODE-$GCRTIME.json}
SUMMARY_SCRIPT="$CVAL_VALIDATION_TESTS_DIR/dltest/summarize_results.py"

prepare_workdir() {
  local source_plan_dir
  local target_plan_dir="$DLTEST_WORK_DIR/test_plans/$CVAL_DL_TEST_PLAN"

  if [ ! -d "$CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test" ]; then
    echo "DL unit test package not found under $CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test" >&2
    return 1
  fi
  source_plan_dir=$(find_source_plan_dir) || return 1
  if [ ! -f "$source_plan_dir/test_plan.json" ]; then
    echo "DL test plan not found: $source_plan_dir/test_plan.json" >&2
    return 1
  fi

  rm -rf "$DLTEST_WORK_DIR"
  mkdir -p "$target_plan_dir" "$DLTEST_OUTPUT_DIR"
  cp "$source_plan_dir/test_plan.json" "$target_plan_dir/test_plan.json"
  if [ -d "$source_plan_dir/baseline" ]; then
    cp -a "$source_plan_dir/baseline" "$target_plan_dir/baseline"
  fi
}

find_source_plan_dir() {
  local candidate
  for candidate in \
    "$CVAL_DL_UNIT_TEST_DIR/test_plans/$CVAL_DL_TEST_PLAN" \
    "$CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test/test_plans/$CVAL_DL_TEST_PLAN"; do
    if [ -f "$candidate/test_plan.json" ]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  echo "DL test plan '$CVAL_DL_TEST_PLAN' not found under $CVAL_DL_UNIT_TEST_DIR" >&2
  return 1
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

if ! prepare_workdir >"$DLTEST_LOG_FILE" 2>&1; then
  echo "DL Test setup FAILED. Check log file: $DLTEST_LOG_FILE"
  write_summary fail >/dev/null 2>&1 || true
  export GCRRESULT3=fail
  exit 1
fi

{
  echo "DL unit test source: $CVAL_DL_UNIT_TEST_DIR"
  echo "DL test working dir: $DLTEST_WORK_DIR"
  echo "DL test runs dir: $DLTEST_RUNS_DIR"
  echo "DL summary file: $DLTEST_SUMMARY_FILE"
  echo "DL test plan: $CVAL_DL_TEST_PLAN"
  echo "DL iterations: $CVAL_DL_ITERATIONS"
  echo "DL GPU count: $GPU_COUNT"
} >>"$DLTEST_LOG_FILE"

(
  cd "$DLTEST_WORK_DIR" || exit 1
  PYTHONPATH="$CVAL_DL_UNIT_TEST_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
    torchrun --nnodes=1 --nproc_per_node="$GPU_COUNT" -m dl_unit_test \
      --test_plan "$CVAL_DL_TEST_PLAN" \
      --iterations "$CVAL_DL_ITERATIONS"
) >>"$DLTEST_LOG_FILE" 2>&1
rc=$?

if [ $rc -ne 0 ]; then
  echo "DL Test torchrun FAILED with rc=$rc"
  echo "Check log file: $DLTEST_LOG_FILE"
  write_summary fail >>"$DLTEST_LOG_FILE" 2>&1 || true
  export GCRRESULT3=fail
  exit "$rc"
fi

if write_summary pass >>"$DLTEST_LOG_FILE" 2>&1; then
  echo "DL Test completed successfully. Log file: $DLTEST_LOG_FILE Summary file: $DLTEST_SUMMARY_FILE"
  export GCRRESULT3=pass
  exit 0
fi

echo "DL Test summary validation FAILED. Check summary file: $DLTEST_SUMMARY_FILE"
export GCRRESULT3=fail
exit 1
