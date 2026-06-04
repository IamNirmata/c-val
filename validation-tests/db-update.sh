# main db update
echo "Updating main db with all test results"
STORAGE_OUTPUT_DIR="/data/continuous_validation/storage/$GCRNODE/storage-$GCRNODE-$GCRTIME"
echo "Storage Output dir: $STORAGE_OUTPUT_DIR"
NCCL_OUTPUT_DIR="/data/continuous_validation/nccl/$GCRNODE/nccl-$GCRNODE-$GCRTIME"
echo "NCCL Output dir: $NCCL_OUTPUT_DIR"

#main DB update
echo "Updating main db with test results"
python /workspace/c-val/utils/functions.py add-result \
    "$GCRNODE" \
    "all" \
    "pass" \
    "$GCRTIME" \
    --db-path /data/continuous_validation/metadata/validation.db
echo "Main DB update completed."


#storage DB update
echo "Updating storage db with test results"
python3 /workspace/c-val/utils/functions.py add-storage-result \
    "$GCRNODE" \
    "$GCRTIME" \
    "$STORAGE_OUTPUT_DIR" \
    --db-path /data/continuous_validation/metadata/test-storage.db
echo "Storage DB update completed."


#nccl DB update

echo "Updating nccl db with test results"
NCCL_LOG_FILE="$NCCL_OUTPUT_DIR/nccl-$GCRNODE-$GCRTIME.log"
echo "NCCL Log file: $NCCL_LOG_FILE"


if [ -f "$NCCL_SUMMARY_FILE" ]; then
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
else
    echo "Error: Result file $NCCL_SUMMARY_FILE was not created."
    exit 1
fi

python3 /workspace/c-val/utils/functions.py add-nccl-result \
    "$GCRNODE" \
    "$GCRTIME" \
    "$GCR_BUSBW" \
    "$GCR_LATENCY" \
    --db-path /data/continuous_validation/metadata/test-nccl.db

echo "NCCl DB update completed."