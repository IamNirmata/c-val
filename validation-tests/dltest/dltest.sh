#!/usr/bin/env bash

# Run the deep learning validation workload and map torchrun's exit code to
# the c-val DL result variable. Detailed evidence is written to DLTEST_LOG_FILE.

echo "Running DL Test on node: $GCRNODE at time: $GCRTIME"
DLTEST_COMMAND="/data/continuous_validation/deeplearning_unit_test/main.py"


cd /data/continuous_validation/deeplearning_unit_test || exit 1
torchrun --nnodes=1 --nproc-per-node "$1" "$DLTEST_COMMAND" \
  --test_plan 80gb-b200 \
  --baseline_test_id b200-pt2.8.0-cuda12.9 \
  --iterations 20 \
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