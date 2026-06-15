#!/bin/bash
set -uo pipefail

# Execute all in-pod validation phases and continuously persist result state.
# The script does not use `set -e` because each phase should run and record its
# own pass/fail status before the aggregate result is written.

GCRRESULT1=${GCRRESULT1:-fail}
GCRRESULT2=${GCRRESULT2:-fail}
GCRRESULT3=${GCRRESULT3:-fail}
CVAL_IMAGE_NAME=${CVAL_IMAGE_NAME:-}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}
CVAL_GPU_COUNT=${CVAL_GPU_COUNT:-8}
CVAL_NCCL_ITERATIONS=${CVAL_NCCL_ITERATIONS:-20}
CVAL_NCCL_DATA_SIZE_GB=${CVAL_NCCL_DATA_SIZE_GB:-8}
CVAL_RESULT_ENV_FILE=${CVAL_RESULT_ENV_FILE:-"/tmp/cval-results-${GCRNODE:-unknown}-${GCRTIME:-unknown}.env"}
CVAL_RESULT_JSON_FILE=${CVAL_RESULT_JSON_FILE:-"/tmp/cval-results-${GCRNODE:-unknown}-${GCRTIME:-unknown}.json"}

write_result_state() {
	# Persist both the legacy env file and the structured JSON schema after each
	# phase, so an interrupted run still leaves the best available status.
	mkdir -p "$(dirname "$CVAL_RESULT_ENV_FILE")"
	mkdir -p "$(dirname "$CVAL_RESULT_JSON_FILE")"
	{
		printf 'GCRRESULT1=%q\n' "$GCRRESULT1"
		printf 'GCRRESULT2=%q\n' "$GCRRESULT2"
		printf 'GCRRESULT3=%q\n' "$GCRRESULT3"
	} > "$CVAL_RESULT_ENV_FILE"

	export GCRRESULT1 GCRRESULT2 GCRRESULT3 CVAL_RESULT_JSON_FILE
	python3 - <<'PY'
import datetime
import json
import os


def env(name, default=""):
	return os.environ.get(name, default)


tests = {
	"storage": {
		"status": env("GCRRESULT1", "fail"),
		"log": env("STORAGE_LOG_FILE"),
		"summary": env("STORAGE_SUMMARY_FILE"),
	},
	"nccl": {
		"status": env("GCRRESULT2", "fail"),
		"log": env("NCCL_LOG_FILE"),
		"summary": env("NCCL_SUMMARY_FILE"),
	},
	"dltest": {
		"status": env("GCRRESULT3", "fail"),
		"log": env("DLTEST_LOG_FILE"),
		"summary": env("DLTEST_SUMMARY_FILE"),
	},
}
overall = "pass" if all(test["status"] == "pass" for test in tests.values()) else "fail"
payload = {
	"schema_version": "cval.results.v1",
	"node": env("GCRNODE", "unknown"),
	"image_name": env("CVAL_IMAGE_NAME", ""),
	"timestamp": env("GCRTIME", "unknown"),
	"generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
	"overall": overall,
	"tests": tests,
}
path = env("CVAL_RESULT_JSON_FILE")
tmp_path = f"{path}.tmp"
with open(tmp_path, "w", encoding="utf-8") as handle:
	json.dump(payload, handle, indent=2, sort_keys=True)
	handle.write("\n")
os.replace(tmp_path, path)
PY
}

write_result_state

# Install fio inside the validation image before storage testing.
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
echo "CVAL_RESULT_ENV_FILE: $CVAL_RESULT_ENV_FILE"
echo "CVAL_RESULT_JSON_FILE: $CVAL_RESULT_JSON_FILE"
echo "CVAL_IMAGE_NAME: $CVAL_IMAGE_NAME"
echo "#########################################################################"

# Phase 1: PVC/NFS storage performance and correctness smoke test.
if bash "$CVAL_VALIDATION_TESTS_DIR/storage/storage.sh" | tee "$STORAGE_LOG_FILE"; then
	echo "Storage test is complete. Log file: $STORAGE_LOG_FILE Summary file: $STORAGE_SUMMARY_FILE"
	GCRRESULT1=pass
else
	echo "Storage test FAILED. Log file: $STORAGE_LOG_FILE Summary file: $STORAGE_SUMMARY_FILE"
	GCRRESULT1=fail
fi
write_result_state


# Phase 2: single-node NCCL all-reduce over the requested GPU set.
NCCL_SCRIPT="$CVAL_VALIDATION_TESTS_DIR/nccl/single-node-allreduce.py"
NCCL_ARGS="--result-file $NCCL_SUMMARY_FILE --iterations $CVAL_NCCL_ITERATIONS --data-size-gb $CVAL_NCCL_DATA_SIZE_GB"

echo "Running NCCL Test..."
if NCCL_NET=IB NCCL_P2P_DISABLE=1 NCCL_SHM_DISABLE=1 NCCL_DEBUG=INFO \
	torchrun --nproc_per_node="$CVAL_GPU_COUNT" "$NCCL_SCRIPT" $NCCL_ARGS | tee "$NCCL_LOG_FILE"; then
	echo "NCCL test is complete. Log file: $NCCL_LOG_FILE Summary file: $NCCL_SUMMARY_FILE"
	GCRRESULT2=pass
else
	echo "NCCL test FAILED. Log file: $NCCL_LOG_FILE Summary file: $NCCL_SUMMARY_FILE"
	GCRRESULT2=fail
fi
write_result_state

# Phase 3: deep learning unit test workload and numerical checks.
echo "Running DL Test..."
if bash "$CVAL_VALIDATION_TESTS_DIR/dltest/dltest.sh" "$CVAL_GPU_COUNT"; then
	GCRRESULT3=pass
else
	GCRRESULT3=fail
fi
write_result_state

echo "Final c-val test results: storage=$GCRRESULT1 nccl=$GCRRESULT2 dltest=$GCRRESULT3"
