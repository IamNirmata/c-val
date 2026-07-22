#!/bin/bash
set -euo pipefail

# Ingest one validation run into metadata databases.
# The structured result JSON is authoritative when available; env files are a
# compatibility fallback for older runtime artifacts.

# main db update
echo "Updating main db with all test results"
CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_DB_PATH=${CVAL_VALIDATION_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/validation.db}
CVAL_STORAGE_DB_PATH=${CVAL_STORAGE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-storage.db}
CVAL_NCCL_DB_PATH=${CVAL_NCCL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-nccl.db}

STORAGE_OUTPUT_DIR=${STORAGE_OUTPUT_DIR:-$CVAL_VALIDATION_ROOT/storage/$GCRNODE/storage-$GCRNODE-$GCRTIME}
echo "Storage Output dir: $STORAGE_OUTPUT_DIR"
NCCL_OUTPUT_DIR=${NCCL_OUTPUT_DIR:-$CVAL_VALIDATION_ROOT/nccl/$GCRNODE/nccl-$GCRNODE-$GCRTIME}
echo "NCCL Output dir: $NCCL_OUTPUT_DIR"

GCRRESULT1=${GCRRESULT1:-fail}
GCRRESULT2=${GCRRESULT2:-fail}
GCRRESULT3=${GCRRESULT3:-fail}
CVAL_IMAGE_NAME=${CVAL_IMAGE_NAME:-}
CVAL_PYTORCH_VERSION=${CVAL_PYTORCH_VERSION:-}
CVAL_CUDA_VERSION=${CVAL_CUDA_VERSION:-}
CVAL_NCCL_ITERATIONS=${CVAL_NCCL_ITERATIONS:-20}

if [ -n "${CVAL_RESULT_JSON_FILE:-}" ] && [ -f "$CVAL_RESULT_JSON_FILE" ]; then
    echo "Loading structured test result state from $CVAL_RESULT_JSON_FILE"
    while IFS='=' read -r key value; do
        case "$key" in
            GCRRESULT1) GCRRESULT1="$value" ;;
            GCRRESULT2) GCRRESULT2="$value" ;;
            GCRRESULT3) GCRRESULT3="$value" ;;
            overall_result) overall_result="$value" ;;
            image_name) CVAL_IMAGE_NAME="$value" ;;
            pytorch_version) CVAL_PYTORCH_VERSION="$value" ;;
            cuda_version) CVAL_CUDA_VERSION="$value" ;;
        esac
    done < <(PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli result --result-json "$CVAL_RESULT_JSON_FILE"
    )
elif [ -n "${CVAL_RESULT_ENV_FILE:-}" ] && [ -f "$CVAL_RESULT_ENV_FILE" ]; then
    # Legacy fallback: only use this when the v1 JSON artifact is missing.
    echo "Loading test result state from $CVAL_RESULT_ENV_FILE"
    source "$CVAL_RESULT_ENV_FILE"
else
    echo "Warning: c-val result state file not found; using fail defaults."
fi

overall_result=${overall_result:-pass}
# Recompute aggregate status defensively before writing the `all` row.
if [ "$GCRRESULT1" != "pass" ] || [ "$GCRRESULT2" != "pass" ] || [ "$GCRRESULT3" != "pass" ]; then
    overall_result=fail
fi

add_main_result() {
    # Keep the main DB as one row per test plus one aggregate `all` row.
    local test_name="$1"
    local result="$2"
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-add-result \
        "$GCRNODE" \
        "$test_name" \
        "$result" \
        "$GCRTIME" \
        --image-name "$CVAL_IMAGE_NAME" \
        --pytorch-version "$CVAL_PYTORCH_VERSION" \
        --cuda-version "$CVAL_CUDA_VERSION" \
        --db-path "$CVAL_VALIDATION_DB_PATH"
}

#main DB update
echo "Updating main db with test results"
add_main_result "storage" "$GCRRESULT1"
add_main_result "nccl" "$GCRRESULT2"
add_main_result "dltest" "$GCRRESULT3"
add_main_result "all" "$overall_result"
echo "Main DB update completed."


# Storage metrics are valid only when the storage phase itself passed.
if [ "$GCRRESULT1" = "pass" ]; then
    echo "Updating storage db with test results"
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-add-storage-result \
        "$GCRNODE" \
        "$GCRTIME" \
        "$STORAGE_OUTPUT_DIR" \
        --image-name "$CVAL_IMAGE_NAME" \
        --db-path "$CVAL_STORAGE_DB_PATH"
    echo "Storage DB update completed."
else
    echo "Skipping storage metrics DB update because storage result is $GCRRESULT1."
fi


# NCCL metric ingestion depends on the all-reduce summary JSON.

echo "Updating nccl db with test results"
NCCL_LOG_FILE=${NCCL_LOG_FILE:-$NCCL_OUTPUT_DIR/nccl-$GCRNODE-$GCRTIME.log}
echo "NCCL Log file: $NCCL_LOG_FILE"


if [ "$GCRRESULT2" = "pass" ] && [ -f "$NCCL_SUMMARY_FILE" ]; then
    # Use python to parse the JSON (available on all systems, unlike 'jq')
    export GCR_LATENCY=$(python3 -c "import json; print(json.load(open('$NCCL_SUMMARY_FILE'))['GCR_LATENCY'])")
    export GCR_ALGBW=$(python3 -c "import json; print(json.load(open('$NCCL_SUMMARY_FILE'))['GCR_ALGBW'])")
    export GCR_BUSBW=$(python3 -c "import json; print(json.load(open('$NCCL_SUMMARY_FILE'))['GCR_BUSBW'])")
    echo "--------------------------------"
    echo "Successfully Loaded Metrics:"
    echo "GCR_BUSBW:   $GCR_BUSBW"
    echo "GCR_ALGBW:   $GCR_ALGBW"
    echo "GCR_LATENCY: $GCR_LATENCY"
    echo "--------------------------------"

    # Persist one consolidated row: aggregate all-reduce BUS_BW/LATENCY plus
    # each HCA port's maximum observed bandwidth (mlx5_0..mlx5_13).
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-add-nccl-health \
        "$GCRNODE" \
        "$GCRTIME" \
        "$NCCL_SUMMARY_FILE" \
        --iterations "$CVAL_NCCL_ITERATIONS" \
        --image-name "$CVAL_IMAGE_NAME" \
        --cuda-version "$CVAL_CUDA_VERSION" \
        --pytorch-version "$CVAL_PYTORCH_VERSION" \
        --db-path "$CVAL_NCCL_DB_PATH"

    echo "NCCL IB_HEALTH DB update completed."
else
    echo "Skipping NCCL metrics DB update because result is $GCRRESULT2 or summary is missing."
fi