#setup directories and environment variables


export STORAGE_OUTPUT_DIR="/data/continuous_validation/storage/$GCRNODE/storage-$GCRNODE-$GCRTIME"
export NCCL_OUTPUT_DIR="/data/continuous_validation/nccl/$GCRNODE/nccl-$GCRNODE-$GCRTIME"
export DLTEST_OUTPUT_DIR="/data/continuous_validation/dltest/$GCRNODE/dltest-$GCRNODE-$GCRTIME"

mkdir -p "$STORAGE_OUTPUT_DIR"
mkdir -p "$NCCL_OUTPUT_DIR"
mkdir -p "$DLTEST_OUTPUT_DIR"

#log files
export STORAGE_LOG_FILE="$STORAGE_OUTPUT_DIR/storage-$GCRNODE-$GCRTIME.log"
export NCCL_LOG_FILE="$NCCL_OUTPUT_DIR/nccl-$GCRNODE-$GCRTIME.log"
export DLTEST_LOG_FILE="$DLTEST_OUTPUT_DIR/dltest-$GCRNODE-$GCRTIME.log"

#summary files
export NCCL_SUMMARY_FILE="$NCCL_OUTPUT_DIR/nccl-summary-$GCRNODE-$GCRTIME.json"
export STORAGE_SUMMARY_FILE="$STORAGE_OUTPUT_DIR/storage-summary-$GCRNODE-$GCRTIME.txt"
export DLTEST_SUMMARY_FILE="$DLTEST_OUTPUT_DIR/dltest-summary-$GCRNODE-$GCRTIME.txt"

#result variables
export GCRRESULT1=fail
export GCRRESULT2=fail
export GCRRESULT3=fail