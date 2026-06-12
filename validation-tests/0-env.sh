# Set up per-run artifact directories and environment variables.
# This file is sourced inside the validation pod before tests start. Every path
# includes node and timestamp so concurrent or repeated runs do not collide.

CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}
CVAL_DL_UNIT_TEST_DIR=${CVAL_DL_UNIT_TEST_DIR:-$CVAL_VALIDATION_ROOT/deeplearning_unit_test}
CVAL_VALIDATION_DB_PATH=${CVAL_VALIDATION_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/validation.db}
CVAL_STORAGE_DB_PATH=${CVAL_STORAGE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-storage.db}
CVAL_NCCL_DB_PATH=${CVAL_NCCL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-nccl.db}
CVAL_GPU_COUNT=${CVAL_GPU_COUNT:-8}

export CVAL_VALIDATION_ROOT CVAL_REPO_DIR CVAL_VALIDATION_TESTS_DIR CVAL_DL_UNIT_TEST_DIR
export CVAL_VALIDATION_DB_PATH CVAL_STORAGE_DB_PATH CVAL_NCCL_DB_PATH CVAL_GPU_COUNT


# Test-specific output roots on the shared validation PVC.
export STORAGE_OUTPUT_DIR="$CVAL_VALIDATION_ROOT/storage/$GCRNODE/storage-$GCRNODE-$GCRTIME"
export NCCL_OUTPUT_DIR="$CVAL_VALIDATION_ROOT/nccl/$GCRNODE/nccl-$GCRNODE-$GCRTIME"
export DLTEST_OUTPUT_DIR="$CVAL_VALIDATION_ROOT/dltest/$GCRNODE/dltest-$GCRNODE-$GCRTIME"

# Structured result artifacts bridge child shell test outcomes to db-update.sh.
export CVAL_RESULT_DIR="$CVAL_VALIDATION_ROOT/results/$GCRNODE"
export CVAL_RESULT_ENV_FILE="$CVAL_RESULT_DIR/cval-results-$GCRNODE-$GCRTIME.env"
export CVAL_RESULT_JSON_FILE="$CVAL_RESULT_DIR/cval-results-$GCRNODE-$GCRTIME.json"

mkdir -p "$STORAGE_OUTPUT_DIR"
mkdir -p "$NCCL_OUTPUT_DIR"
mkdir -p "$DLTEST_OUTPUT_DIR"
mkdir -p "$CVAL_RESULT_DIR"

# Log files capture raw command output for each validation phase.
export STORAGE_LOG_FILE="$STORAGE_OUTPUT_DIR/storage-$GCRNODE-$GCRTIME.log"
export NCCL_LOG_FILE="$NCCL_OUTPUT_DIR/nccl-$GCRNODE-$GCRTIME.log"
export DLTEST_LOG_FILE="$DLTEST_OUTPUT_DIR/dltest-$GCRNODE-$GCRTIME.log"

# Summary files contain compact machine- or human-readable phase results.
export NCCL_SUMMARY_FILE="$NCCL_OUTPUT_DIR/nccl-summary-$GCRNODE-$GCRTIME.json"
export STORAGE_SUMMARY_FILE="$STORAGE_OUTPUT_DIR/storage-summary-$GCRNODE-$GCRTIME.txt"
export DLTEST_SUMMARY_FILE="$DLTEST_OUTPUT_DIR/dltest-summary-$GCRNODE-$GCRTIME.txt"

# Default every phase to fail; tests must opt into pass after successful completion.
export GCRRESULT1=fail
export GCRRESULT2=fail
export GCRRESULT3=fail