#!/bin/bash

# --- CONFIGURATION ---
# Ensure global variables are set; default to safe values if missing
if [ -z "$GCRNODE" ]; then
    echo "WARNING: GCRNODE is not set. Using 'unknown_node'"
    GCRNODE="unknown_node"
fi

if [ -z "$GCRTIME" ]; then
    # Generate timestamp if not provided (California Time)
    GCRTIME=$(date +%Y%m%d_%H%M%S -d 'TZ="America/Los_Angeles" now')
    echo "WARNING: GCRTIME is not set. Generated timestamp: $GCRTIME"
fi

# Define paths
JOB_DIR="/workspace/c-val/validation-tests/storage/fio_jobs"
OUTPUT_DIR="/data/continuous_validation/storage/$GCRNODE/storage-$GCRNODE-$GCRTIME"
SUMMARY_FILE="$OUTPUT_DIR/summary.txt"

echo "================================================================================"
echo " STORAGE VALIDATION SUITE"
echo " Node:       $GCRNODE"
echo " Time:       $GCRTIME"
echo " Job Source: $JOB_DIR"
echo " Output Dir: $STORAGE_OUTPUT_DIR"
echo "================================================================================"

# --- CHECKS ---
if [ ! -d "$JOB_DIR" ]; then
    echo "CRITICAL ERROR: FIO jobs directory not found at $JOB_DIR"
    exit 1
fi

echo "Setting up output directory..."
mkdir -p "$STORAGE_OUTPUT_DIR"
echo "Directory created."

# --- EXECUTION ---
echo "Starting storage tests..."

# 1. Random Write
echo "Running random write test... (1/6)"
fio "$JOB_DIR/randwrite.fio" --output-format=json --output="$STORAGE_OUTPUT_DIR/randwrite.json"

# 2. Random Read
echo "Running random read test... (2/6)"
fio "$JOB_DIR/randread.fio" --output-format=json --output="$STORAGE_OUTPUT_DIR/randread.json"
# 3. Sequential Write (QD128)
echo "Running iodepth write test... (3/6)"
fio "$JOB_DIR/iodepth_write_1file.fio" --output-format=json --output="$STORAGE_OUTPUT_DIR/iodepth_write_1file.json"

# 4. Sequential Read (QD128)
echo "Running iodepth read test... (4/6)"
fio "$JOB_DIR/iodepth_read_1file.fio" --output-format=json --output="$STORAGE_OUTPUT_DIR/iodepth_read_1file.json"
# 5. Aggregate Write (Numjobs)
echo "Running numjobs write test... (5/6)"
fio "$JOB_DIR/numjobs_write_nfiles.fio" --output-format=json --output="$STORAGE_OUTPUT_DIR/numjobs_write_nfiles.json"

# 6. Aggregate Read (Numjobs)
echo "Running numjobs read test... (6/6)"
fio "$JOB_DIR/numjobs_read_nfiles.fio" --output-format=json --output="$STORAGE_OUTPUT_DIR/numjobs_read_nfiles.json"
echo "Storage tests completed."
echo "Results saved in $STORAGE_OUTPUT_DIR"

# --- SUMMARY REPORTING ---
echo "Generating summary report..."

# Check if jq is installed before trying to generate the file
if ! command -v jq &> /dev/null; then
    echo "Error: 'jq' is not installed. Cannot generate summary table."
    echo "Raw JSON files are available in $STORAGE_OUTPUT_DIR"
    exit 0
fi

# We wrap the entire block in curly braces and redirect to the file
{
    echo "================================================================================"
    echo " PERFORMANCE SUMMARY REPORT"
    echo " Node: $GCRNODE"
    echo " Date: $GCRTIME"
    echo "================================================================================"
    printf "%-35s | %-15s | %-15s\n" "Test Filename" "IOPS" "Bandwidth (GB/s)"
    echo "--------------------------------------------------------------------------------"

    # Parse all JSON files in the output directory
    for file in "$STORAGE_OUTPUT_DIR"/*.json; do
        [ -e "$file" ] || continue
        
        filename=$(basename "$file")
        
        # Extract IOPS and Bandwidth (KB/s) sum for read+write
        vals=$(jq -r '.jobs[0] | "\(.read.iops + .write.iops) \(.read.bw + .write.bw)"' "$file")
        
        read -r iops bw_kb <<< "$vals"
        
        # Convert to readable formats (GB/s for bandwidth, 2 decimal places)
        bw_gb=$(awk "BEGIN {printf \"%.2f\", $bw_kb / 1024 / 1024}")
        iops_fixed=$(awk "BEGIN {printf \"%.2f\", $iops}")
        
        printf "%-35s | %-15s | %-15s\n" "$filename" "$iops_fixed" "$bw_gb"
    done
    echo "================================================================================"

} > "$STORAGE_SUMMARY_FILE"

echo "Summary report generated at: $STORAGE_SUMMARY_FILE"
# Optional: print the file to console as well so you see it immediately
cat "$STORAGE_SUMMARY_FILE"