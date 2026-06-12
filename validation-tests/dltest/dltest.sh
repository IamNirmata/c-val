#!/usr/bin/env bash

# Run the deep learning validation workload and map torchrun's exit code to
# the c-val DL result variable. Detailed evidence is written to DLTEST_LOG_FILE.

echo "Running DL Test on node: $GCRNODE at time: $GCRTIME"
GPU_COUNT=${1:-${CVAL_GPU_COUNT:-8}}
CVAL_DL_UNIT_TEST_DIR=${CVAL_DL_UNIT_TEST_DIR:-/data/continuous_validation/deeplearning_unit_test}
CVAL_DL_TEST_PLAN=${CVAL_DL_TEST_PLAN:-80gb-b200}
CVAL_DL_BASELINE_TEST_ID=${CVAL_DL_BASELINE_TEST_ID:-b200-pt2.8.0-cuda12.9}
CVAL_DL_ITERATIONS=${CVAL_DL_ITERATIONS:-20}
DLTEST_COMMAND="$CVAL_DL_UNIT_TEST_DIR/main.py"


cd "$CVAL_DL_UNIT_TEST_DIR" || exit 1
torchrun --nnodes=1 --nproc-per-node "$GPU_COUNT" "$DLTEST_COMMAND" \
  --test_plan "$CVAL_DL_TEST_PLAN" \
  --baseline_test_id "$CVAL_DL_BASELINE_TEST_ID" \
  --iterations "$CVAL_DL_ITERATIONS" \
  >"$DLTEST_LOG_FILE" 2>&1
rc=$?

if [ $rc -ne 0 ]; then
  echo "DL Test torchrun FAILED with rc=$rc"
  echo "Check log file: $DLTEST_LOG_FILE"
  export GCRRESULT3=fail
else
  # A zero exit from the DL harness is the contract for DL phase success.
  echo "DL Test completed successfully. Log file: $DLTEST_LOG_FILE"
  export GCRRESULT3=pass
fi

exit "$rc"