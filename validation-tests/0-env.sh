# Set up per-run artifact directories and environment variables.
# This file is sourced inside the validation pod before tests start. Every path
# includes node and timestamp so concurrent or repeated runs do not collide.

CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}
CVAL_DL_UNIT_TEST_DIR=${CVAL_DL_UNIT_TEST_DIR:-$CVAL_VALIDATION_ROOT/deep-learning-unit-test-main}
CVAL_VALIDATION_DB_PATH=${CVAL_VALIDATION_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/validation.db}
CVAL_RUN_HISTORY_ENABLED=${CVAL_RUN_HISTORY_ENABLED:-false}
CVAL_RUN_HISTORY_DB_PATH=${CVAL_RUN_HISTORY_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/node-run-history.db}
CVAL_STORAGE_DB_PATH=${CVAL_STORAGE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-storage.db}
CVAL_NCCL_DB_PATH=${CVAL_NCCL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-nccl.db}
CVAL_DL_NUMERICAL_DB_PATH=${CVAL_DL_NUMERICAL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_numerical_correctness.db}
CVAL_DL_COMPUTE_DB_PATH=${CVAL_DL_COMPUTE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_compute_performance.db}
CVAL_DL_COLLECTIVE_DB_PATH=${CVAL_DL_COLLECTIVE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_collective_performance.db}
CVAL_DL_OVERLAP_DB_PATH=${CVAL_DL_OVERLAP_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_overlap_performance.db}
CVAL_RUN_ID=${CVAL_RUN_ID:-${GCRNODE:-unknown}-${GCRTIME:-unknown}}
RUN_STORAGE=${RUN_STORAGE:-true}
RUN_NCCL=${RUN_NCCL:-true}
RUN_DLTEST=${RUN_DLTEST:-true}

export CVAL_VALIDATION_ROOT CVAL_REPO_DIR CVAL_VALIDATION_TESTS_DIR CVAL_DL_UNIT_TEST_DIR
export CVAL_VALIDATION_DB_PATH CVAL_RUN_HISTORY_ENABLED CVAL_RUN_HISTORY_DB_PATH
export CVAL_STORAGE_DB_PATH CVAL_NCCL_DB_PATH
export CVAL_DL_NUMERICAL_DB_PATH CVAL_DL_COMPUTE_DB_PATH
export CVAL_DL_COLLECTIVE_DB_PATH CVAL_DL_OVERLAP_DB_PATH
export CVAL_RUN_ID RUN_STORAGE RUN_NCCL RUN_DLTEST

# Detect framework versions from the running image so each result records the
# exact PyTorch/CUDA build that produced it, alongside image_name. Detection is
# best-effort: a missing or CPU-only torch leaves the value empty.
CVAL_PYTORCH_VERSION=${CVAL_PYTORCH_VERSION:-$(python3 -c 'import torch; print(torch.__version__)' 2>/dev/null || echo "")}
CVAL_CUDA_VERSION=${CVAL_CUDA_VERSION:-$(python3 -c 'import torch; print(torch.version.cuda or "")' 2>/dev/null || echo "")}
export CVAL_PYTORCH_VERSION CVAL_CUDA_VERSION


# Canonical global logs and per-test run roots on the shared validation PVC.
export CVAL_JOB_LOG_DIR="$CVAL_VALIDATION_ROOT/logs/job_logs/$GCRNODE/$CVAL_RUN_ID"
export STORAGE_RUN_DIR="$CVAL_VALIDATION_ROOT/validation_tests/storage/runs/$GCRNODE/$CVAL_RUN_ID"
export NCCL_RUN_DIR="$CVAL_VALIDATION_ROOT/validation_tests/nccl/runs/$GCRNODE/$CVAL_RUN_ID"
export DLTEST_RUN_DIR="$CVAL_VALIDATION_ROOT/validation_tests/dltest/runs/$GCRNODE/$CVAL_RUN_ID"

# Legacy output variable names now point at each canonical artifacts directory.
export STORAGE_OUTPUT_DIR="$STORAGE_RUN_DIR/artifacts"
export NCCL_OUTPUT_DIR="$NCCL_RUN_DIR/artifacts"
export DLTEST_OUTPUT_DIR="$DLTEST_RUN_DIR/artifacts"

# Structured result artifacts bridge child shell test outcomes to db-update.sh.
export CVAL_RESULT_DIR="$CVAL_JOB_LOG_DIR"
export CVAL_RESULT_ENV_FILE="$CVAL_JOB_LOG_DIR/result.env"
export CVAL_RESULT_JSON_FILE="$CVAL_JOB_LOG_DIR/result.json"

# Log files capture raw command output for each validation phase.
export STORAGE_LOG_FILE="$CVAL_VALIDATION_ROOT/logs/storage/$GCRNODE/$CVAL_RUN_ID/stdout.log"
export NCCL_LOG_FILE="$CVAL_VALIDATION_ROOT/logs/nccl/$GCRNODE/$CVAL_RUN_ID/workload.log"
export NCCL_IBBW_LOG_FILE="$NCCL_OUTPUT_DIR/ibbw-$GCRNODE-$GCRTIME.log"
export DLTEST_LOG_FILE="$CVAL_VALIDATION_ROOT/logs/dltest/$GCRNODE/$CVAL_RUN_ID/workload.log"

# Summary files contain compact machine- or human-readable phase results.
export NCCL_SUMMARY_FILE="$NCCL_RUN_DIR/summary.json"
export STORAGE_SUMMARY_FILE="$STORAGE_RUN_DIR/summary.txt"
export DLTEST_SUMMARY_FILE="$DLTEST_RUN_DIR/summary.json"

# Default every phase to fail; tests must opt into pass after successful completion.
export GCRRESULT1=fail
export GCRRESULT2=fail
export GCRRESULT3=fail