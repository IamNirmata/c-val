#!/usr/bin/env bash
set -euo pipefail

# Run the storage validation suite against the shared validation PVC. Each fio
# job writes raw JSON; the summary extracts IOPS and bandwidth for operators and
# metric ingestion.

CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}

if [[ -z "${GCRNODE:-}" ]]; then
    echo "WARNING: GCRNODE is not set. Using 'unknown_node'"
    GCRNODE="unknown_node"
fi

if [[ -z "${GCRTIME:-}" ]]; then
    GCRTIME=$(TZ=America/Los_Angeles date +%Y%m%d_%H%M%S)
    echo "WARNING: GCRTIME is not set. Generated timestamp: $GCRTIME"
fi

JOB_DIR="$CVAL_VALIDATION_TESTS_DIR/storage/fio_jobs"
CVAL_RUN_ID=${CVAL_RUN_ID:-$GCRNODE-$GCRTIME}
STORAGE_RUN_DIR=${STORAGE_RUN_DIR:-\
$CVAL_VALIDATION_ROOT/validation_tests/storage/runs/$GCRNODE/$CVAL_RUN_ID}
STORAGE_OUTPUT_DIR=${STORAGE_OUTPUT_DIR:-\
${CVAL_TEST_OUTPUT_DIR:-}}
STORAGE_OUTPUT_DIR=${STORAGE_OUTPUT_DIR:-$STORAGE_RUN_DIR/artifacts}
STORAGE_SUMMARY_FILE=${STORAGE_SUMMARY_FILE:-\
${CVAL_TEST_SUMMARY_FILE:-}}
STORAGE_SUMMARY_FILE=${STORAGE_SUMMARY_FILE:-$STORAGE_RUN_DIR/summary.txt}
STORAGE_DATA_DIR="$STORAGE_OUTPUT_DIR/fio-data"

export GCRNODE GCRTIME STORAGE_OUTPUT_DIR STORAGE_SUMMARY_FILE

echo "================================================================================"
echo " STORAGE VALIDATION SUITE"
echo " Node:       $GCRNODE"
echo " Time:       $GCRTIME"
echo " Job Source: $JOB_DIR"
echo " Output Dir: $STORAGE_OUTPUT_DIR"
echo "================================================================================"

if [[ ! -d "$JOB_DIR" ]]; then
    echo "CRITICAL ERROR: FIO jobs directory not found at $JOB_DIR" >&2
    exit 1
fi
if ! command -v fio >/dev/null 2>&1; then
    echo "CRITICAL ERROR: fio is not installed; run storage/setup.sh first" >&2
    exit 1
fi

mkdir -p "$STORAGE_OUTPUT_DIR" "$STORAGE_DATA_DIR"
if [[ -L "$STORAGE_DATA_DIR" || "$STORAGE_DATA_DIR" != "$STORAGE_OUTPUT_DIR/fio-data" ]]; then
    echo "CRITICAL ERROR: unsafe FIO data directory: $STORAGE_DATA_DIR" >&2
    exit 1
fi

cleanup_fio_data() {
    if [[ -d "$STORAGE_DATA_DIR" && ! -L "$STORAGE_DATA_DIR" && \
          "$STORAGE_DATA_DIR" == "$STORAGE_OUTPUT_DIR/fio-data" ]]; then
        rm -rf -- "$STORAGE_DATA_DIR"
    fi
}
trap cleanup_fio_data EXIT
trap 'cleanup_fio_data; exit 130' INT TERM

echo "Starting storage tests..."
echo "Running random write test... (1/6)"
fio "$JOB_DIR/randwrite.fio" --output-format=json \
    --directory="$STORAGE_DATA_DIR" \
    --output="$STORAGE_OUTPUT_DIR/randwrite.json"

echo "Running random read test... (2/6)"
fio "$JOB_DIR/randread.fio" --output-format=json \
    --directory="$STORAGE_DATA_DIR" \
    --output="$STORAGE_OUTPUT_DIR/randread.json"

echo "Running iodepth write test... (3/6)"
fio "$JOB_DIR/iodepth_write_1file.fio" --output-format=json \
    --directory="$STORAGE_DATA_DIR" \
    --output="$STORAGE_OUTPUT_DIR/iodepth_write_1file.json"

echo "Running iodepth read test... (4/6)"
fio "$JOB_DIR/iodepth_read_1file.fio" --output-format=json \
    --directory="$STORAGE_DATA_DIR" \
    --output="$STORAGE_OUTPUT_DIR/iodepth_read_1file.json"

echo "Running numjobs write test... (5/6)"
fio "$JOB_DIR/numjobs_write_nfiles.fio" --output-format=json \
    --directory="$STORAGE_DATA_DIR" \
    --output="$STORAGE_OUTPUT_DIR/numjobs_write_nfiles.json"

echo "Running numjobs read test... (6/6)"
fio "$JOB_DIR/numjobs_read_nfiles.fio" --output-format=json \
    --directory="$STORAGE_DATA_DIR" \
    --output="$STORAGE_OUTPUT_DIR/numjobs_read_nfiles.json"
echo "Storage tests completed. Results saved in $STORAGE_OUTPUT_DIR"

if ! command -v jq >/dev/null 2>&1; then
    echo "Warning: jq is not installed; raw fio JSON remains available in $STORAGE_OUTPUT_DIR"
    exit 0
fi

{
    echo "================================================================================"
    echo " PERFORMANCE SUMMARY REPORT"
    echo " Node: $GCRNODE"
    echo " Date: $GCRTIME"
    echo "================================================================================"
    printf "%-35s | %-15s | %-15s\n" "Test Filename" "IOPS" "Bandwidth (GB/s)"
    echo "--------------------------------------------------------------------------------"

    for file in "$STORAGE_OUTPUT_DIR"/*.json; do
        [[ -e "$file" ]] || continue
        filename=$(basename "$file")
        vals=$(jq -r '.jobs[0] | "\(.read.iops + .write.iops) \(.read.bw + .write.bw)"' "$file")
        read -r iops bw_kb <<< "$vals"
        bw_gb=$(awk "BEGIN {printf \"%.2f\", $bw_kb / 1024 / 1024}")
        iops_fixed=$(awk "BEGIN {printf \"%.2f\", $iops}")
        printf "%-35s | %-15s | %-15s\n" "$filename" "$iops_fixed" "$bw_gb"
    done
    echo "================================================================================"
} > "$STORAGE_SUMMARY_FILE"

echo "Summary report generated at: $STORAGE_SUMMARY_FILE"
cat "$STORAGE_SUMMARY_FILE"
