#!/bin/bash

#prep
apt-get update > /dev/null 2>&1 && apt-get install -y fio > /dev/null 2>&1 


echo "######################PHASE: Test Execution#############################"
echo "Running tests on node: $GCRNODE at time: $GCRTIME"

echo "STORAGE_OUTPUT_DIR: $STORAGE_OUTPUT_DIR"
echo "NCCL_OUTPUT_DIR: $NCCL_OUTPUT_DIR"
echo "DLTEST_OUTPUT_DIR: $DLTEST_OUTPUT_DIR"

echo "STORAGE_LOG_FILE: $STORAGE_LOG_FILE"
echo "STORAGE_SUMMARY_FILE: $STORAGE_SUMMARY_FILE"
echo "DLTEST_LOG_FILE: $DLTEST_LOG_FILE"

echo "NCCL_LOG_FILE: $NCCL_LOG_FILE"
echo "NCCL_SUMMARY_FILE: $NCCL_SUMMARY_FILE"
echo "DLTEST_SUMMARY_FILE: $DLTEST_SUMMARY_FILE"
echo "#########################################################################"

#storage test
bash /workspace/c-val/validation-tests/storage/storage.sh | tee "$STORAGE_LOG_FILE"
echo "Storage test is complete. Log file: $STORAGE_LOG_FILE Summary file: $STORAGE_SUMMARY_FILE"
export GCRRESULT1=pass


#nccl test
NCCL_SCRIPT="/workspace/c-val/validation-tests/nccl/single-node-allreduce.py"
NCCL_ARGS="--result-file $NCCL_SUMMARY_FILE"

echo "Running NCCL Test..."
NCCL_NET=IB NCCL_P2P_DISABLE=1 NCCL_SHM_DISABLE=1 NCCL_DEBUG=INFO \
torchrun --nproc_per_node=8 "$NCCL_SCRIPT" $NCCL_ARGS | tee "$NCCL_LOG_FILE"

echo "NCCL test is complete. Log file: $NCCL_LOG_FILE Summary file: $NCCL_SUMMARY_FILE"
export GCRRESULT2=pass

#dltest
echo "Running DL Test..."
bash /workspace/c-val/validation-tests/dltest/dltest.sh 8
