# Set up per-run artifact directories and environment variables.
# This file is sourced inside the validation pod before tests start. Every path
# includes node and timestamp so concurrent or repeated runs do not collide.

CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}
CVAL_DL_UNIT_TEST_DIR=${CVAL_DL_UNIT_TEST_DIR:-$CVAL_VALIDATION_ROOT/deep-learning-unit-test-main}
CVAL_VALIDATION_DB_PATH=${CVAL_VALIDATION_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/validation.db}
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
export CVAL_VALIDATION_DB_PATH
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
CVAL_JOB_LOG_DIR=${CVAL_JOB_LOG_DIR:-$CVAL_VALIDATION_ROOT/logs/job_logs/$GCRNODE/$CVAL_RUN_ID}
STORAGE_RUN_DIR=${STORAGE_RUN_DIR:-$CVAL_VALIDATION_ROOT/validation_tests/storage/runs/$GCRNODE/$CVAL_RUN_ID}
NCCL_RUN_DIR=${NCCL_RUN_DIR:-$CVAL_VALIDATION_ROOT/validation_tests/nccl/runs/$GCRNODE/$CVAL_RUN_ID}
DLTEST_RUN_DIR=${DLTEST_RUN_DIR:-$CVAL_VALIDATION_ROOT/validation_tests/dltest/runs/$GCRNODE/$CVAL_RUN_ID}
export CVAL_JOB_LOG_DIR STORAGE_RUN_DIR NCCL_RUN_DIR DLTEST_RUN_DIR

# Legacy output variable names now point at each canonical artifacts directory.
STORAGE_OUTPUT_DIR=${STORAGE_OUTPUT_DIR:-$STORAGE_RUN_DIR/artifacts}
NCCL_OUTPUT_DIR=${NCCL_OUTPUT_DIR:-$NCCL_RUN_DIR/artifacts}
DLTEST_OUTPUT_DIR=${DLTEST_OUTPUT_DIR:-$DLTEST_RUN_DIR/artifacts}
export STORAGE_OUTPUT_DIR NCCL_OUTPUT_DIR DLTEST_OUTPUT_DIR

# Structured result artifacts bridge child shell test outcomes to db-update.sh.
CVAL_RESULT_DIR=${CVAL_RESULT_DIR:-$CVAL_JOB_LOG_DIR}
CVAL_RESULT_ENV_FILE=${CVAL_RESULT_ENV_FILE:-$CVAL_JOB_LOG_DIR/result.env}
CVAL_RESULT_JSON_FILE=${CVAL_RESULT_JSON_FILE:-$CVAL_JOB_LOG_DIR/result.json}
export CVAL_RESULT_DIR CVAL_RESULT_ENV_FILE CVAL_RESULT_JSON_FILE

# Log files capture raw command output for each validation phase.
STORAGE_LOG_FILE=${STORAGE_LOG_FILE:-$CVAL_VALIDATION_ROOT/logs/storage/$GCRNODE/$CVAL_RUN_ID/stdout.log}
NCCL_LOG_FILE=${NCCL_LOG_FILE:-$CVAL_VALIDATION_ROOT/logs/nccl/$GCRNODE/$CVAL_RUN_ID/workload.log}
NCCL_IBBW_LOG_FILE=${NCCL_IBBW_LOG_FILE:-$NCCL_OUTPUT_DIR/ibbw-$GCRNODE-$GCRTIME.log}
NCCL_RUNTIME_EVIDENCE_FILE=${NCCL_RUNTIME_EVIDENCE_FILE:-$NCCL_OUTPUT_DIR/runtime-evidence.json}
DLTEST_LOG_FILE=${DLTEST_LOG_FILE:-$CVAL_VALIDATION_ROOT/logs/dltest/$GCRNODE/$CVAL_RUN_ID/workload.log}
export STORAGE_LOG_FILE NCCL_LOG_FILE NCCL_IBBW_LOG_FILE NCCL_RUNTIME_EVIDENCE_FILE DLTEST_LOG_FILE

# Summary files contain compact machine- or human-readable phase results.
NCCL_SUMMARY_FILE=${NCCL_SUMMARY_FILE:-$NCCL_RUN_DIR/summary.json}
STORAGE_SUMMARY_FILE=${STORAGE_SUMMARY_FILE:-$STORAGE_RUN_DIR/summary.txt}
DLTEST_SUMMARY_FILE=${DLTEST_SUMMARY_FILE:-$DLTEST_RUN_DIR/summary.json}
export NCCL_SUMMARY_FILE STORAGE_SUMMARY_FILE DLTEST_SUMMARY_FILE

# Default every phase to fail; tests must opt into pass after successful completion.
export GCRRESULT1=fail
export GCRRESULT2=fail
export GCRRESULT3=fail