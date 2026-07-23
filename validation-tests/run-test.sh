#!/bin/bash
set -uo pipefail

# Execute all in-pod validation phases and continuously persist result state.
# The script does not use `set -e` because each phase should run and record its
# own pass/fail status before the aggregate result is written.

is_enabled() {
	case "${1,,}" in
		1|true|yes|on) return 0 ;;
		*) return 1 ;;
	esac
}

bool_to_int() {
	if is_enabled "$1"; then echo 1; else echo 0; fi
}

RUN_STORAGE=${RUN_STORAGE:-true}
RUN_NCCL=${RUN_NCCL:-true}
RUN_DLTEST=${RUN_DLTEST:-true}
GCRRESULT1=${GCRRESULT1:-fail}
GCRRESULT2=${GCRRESULT2:-fail}
GCRRESULT3=${GCRRESULT3:-fail}
CVAL_IMAGE_NAME=${CVAL_IMAGE_NAME:-}
CVAL_PYTORCH_VERSION=${CVAL_PYTORCH_VERSION:-}
CVAL_CUDA_VERSION=${CVAL_CUDA_VERSION:-}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}
CVAL_STORAGE_INSTALL_FIO=${CVAL_STORAGE_INSTALL_FIO:-true}
CVAL_NCCL_GPU_COUNT=${CVAL_NCCL_GPU_COUNT:-8}
CVAL_NCCL_ITERATIONS=${CVAL_NCCL_ITERATIONS:-20}
CVAL_NCCL_DATA_SIZE_GB=${CVAL_NCCL_DATA_SIZE_GB:-8}
CVAL_IBBW_ENABLED=${CVAL_IBBW_ENABLED:-true}
CVAL_NCCL_NET=${CVAL_NCCL_NET:-IB}
CVAL_NCCL_P2P_DISABLE=${CVAL_NCCL_P2P_DISABLE:-true}
CVAL_NCCL_SHM_DISABLE=${CVAL_NCCL_SHM_DISABLE:-true}
CVAL_NCCL_DEBUG=${CVAL_NCCL_DEBUG:-INFO}
CVAL_DL_GPU_COUNT=${CVAL_DL_GPU_COUNT:-8}
# IB port monitoring auto-detects every /sys/class/infiniband port by default.
# Set both bounds to restrict monitoring to a contiguous mlx5_<n> range.
CVAL_IBBW_START_DEVICE=${CVAL_IBBW_START_DEVICE:-}
CVAL_IBBW_END_DEVICE=${CVAL_IBBW_END_DEVICE:-}
CVAL_RESULT_ENV_FILE=${CVAL_RESULT_ENV_FILE:-"/tmp/cval-results-${GCRNODE:-unknown}-${GCRTIME:-unknown}.env"}
CVAL_RESULT_JSON_FILE=${CVAL_RESULT_JSON_FILE:-"/tmp/cval-results-${GCRNODE:-unknown}-${GCRTIME:-unknown}.json"}

is_enabled "$RUN_STORAGE" || GCRRESULT1=incomplete
is_enabled "$RUN_NCCL" || GCRRESULT2=incomplete
is_enabled "$RUN_DLTEST" || GCRRESULT3=incomplete

write_result_state() {
	# Persist both the legacy env file and the structured JSON schema after each
	# phase, so an interrupted run still leaves the best available status.
	mkdir -p "$(dirname "$CVAL_RESULT_ENV_FILE")"
	mkdir -p "$(dirname "$CVAL_RESULT_JSON_FILE")"
	{
		printf 'GCRRESULT1=%q\n' "$GCRRESULT1"
		printf 'GCRRESULT2=%q\n' "$GCRRESULT2"
		printf 'GCRRESULT3=%q\n' "$GCRRESULT3"
		printf 'RUN_STORAGE=%q\n' "$RUN_STORAGE"
		printf 'RUN_NCCL=%q\n' "$RUN_NCCL"
		printf 'RUN_DLTEST=%q\n' "$RUN_DLTEST"
	} > "$CVAL_RESULT_ENV_FILE"

	export GCRRESULT1 GCRRESULT2 GCRRESULT3 RUN_STORAGE RUN_NCCL RUN_DLTEST CVAL_RESULT_JSON_FILE
	python3 - <<'PY'
import datetime
import json
import os


def env(name, default=""):
	return os.environ.get(name, default)


def enabled(name):
	return env(name, "true").strip().lower() in {"1", "true", "yes", "on"}


tests = {
	"storage": {
		"enabled": enabled("RUN_STORAGE"),
		"status": env("GCRRESULT1", "fail"),
		"log": env("STORAGE_LOG_FILE"),
		"summary": env("STORAGE_SUMMARY_FILE"),
	},
	"nccl": {
		"enabled": enabled("RUN_NCCL"),
		"status": env("GCRRESULT2", "fail"),
		"log": env("NCCL_LOG_FILE"),
		"summary": env("NCCL_SUMMARY_FILE"),
	},
	"dltest": {
		"enabled": enabled("RUN_DLTEST"),
		"status": env("GCRRESULT3", "fail"),
		"log": env("DLTEST_LOG_FILE"),
		"summary": env("DLTEST_SUMMARY_FILE"),
	},
}
enabled_tests = [test for test in tests.values() if test["enabled"]]
if not enabled_tests:
	overall = "incomplete"
else:
	overall = "pass" if all(test["status"] == "pass" for test in enabled_tests) else "fail"
payload = {
	"schema_version": "cval.results.v1",
	"node": env("GCRNODE", "unknown"),
	"image_name": env("CVAL_IMAGE_NAME", ""),
	"pytorch_version": env("CVAL_PYTORCH_VERSION", ""),
	"cuda_version": env("CVAL_CUDA_VERSION", ""),
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

# Install fio only for an enabled storage phase when requested by config.
if is_enabled "$RUN_STORAGE" && is_enabled "$CVAL_STORAGE_INSTALL_FIO"; then
	apt-get update > /dev/null 2>&1 && apt-get install -y fio > /dev/null 2>&1
fi


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
echo "NCCL_IBBW_LOG_FILE: ${NCCL_IBBW_LOG_FILE:-}"
echo "DLTEST_SUMMARY_FILE: $DLTEST_SUMMARY_FILE"
echo "CVAL_RESULT_ENV_FILE: $CVAL_RESULT_ENV_FILE"
echo "CVAL_RESULT_JSON_FILE: $CVAL_RESULT_JSON_FILE"
echo "CVAL_IMAGE_NAME: $CVAL_IMAGE_NAME"
echo "RUN_STORAGE: $RUN_STORAGE"
echo "RUN_NCCL: $RUN_NCCL"
echo "RUN_DLTEST: $RUN_DLTEST"
if [ -n "$CVAL_IBBW_START_DEVICE" ] && [ -n "$CVAL_IBBW_END_DEVICE" ]; then
	echo "CVAL_IBBW_DEVICE_RANGE: mlx5_$CVAL_IBBW_START_DEVICE..mlx5_$CVAL_IBBW_END_DEVICE"
else
	echo "CVAL_IBBW_DEVICE_RANGE: auto-detect all /sys/class/infiniband ports"
fi
echo "#########################################################################"

# Phase 1: PVC/NFS storage performance and correctness smoke test.
if ! is_enabled "$RUN_STORAGE"; then
	echo "Storage test SKIPPED (disabled by config)."
	GCRRESULT1=incomplete
elif bash "$CVAL_VALIDATION_TESTS_DIR/storage/storage.sh" | tee "$STORAGE_LOG_FILE"; then
	echo "Storage test is complete. Log file: $STORAGE_LOG_FILE Summary file: $STORAGE_SUMMARY_FILE"
	GCRRESULT1=pass
else
	echo "Storage test FAILED. Log file: $STORAGE_LOG_FILE Summary file: $STORAGE_SUMMARY_FILE"
	GCRRESULT1=fail
fi
write_result_state


# Phase 2: single-node NCCL all-reduce over the requested GPU set.
NCCL_SCRIPT="$CVAL_VALIDATION_TESTS_DIR/nccl/single-node-allreduce.py"
NCCL_IBBW_LOG_FILE=${NCCL_IBBW_LOG_FILE:-$NCCL_OUTPUT_DIR/ibbw-$GCRNODE-$GCRTIME.log}
NCCL_ARGS="--result-file $NCCL_SUMMARY_FILE --ibbw-log-file $NCCL_IBBW_LOG_FILE --iterations $CVAL_NCCL_ITERATIONS --data-size-gb $CVAL_NCCL_DATA_SIZE_GB"
IBBW_PID=""

start_ibbw_monitor() {
	local ibbw_script="$CVAL_VALIDATION_TESTS_DIR/nccl/ibbw.sh"
	if ! is_enabled "$CVAL_IBBW_ENABLED"; then
		echo "IBBW monitor disabled by config"
		return 0
	fi
	if [ ! -f "$ibbw_script" ]; then
		echo "Warning: IBBW monitor script not found: $ibbw_script"
		return 0
	fi
	mkdir -p "$(dirname "$NCCL_IBBW_LOG_FILE")"
	if [ -n "$CVAL_IBBW_START_DEVICE" ] && [ -n "$CVAL_IBBW_END_DEVICE" ]; then
		echo "Starting IBBW monitor: $ibbw_script $CVAL_IBBW_START_DEVICE $CVAL_IBBW_END_DEVICE -> $NCCL_IBBW_LOG_FILE"
		bash "$ibbw_script" "$CVAL_IBBW_START_DEVICE" "$CVAL_IBBW_END_DEVICE" > "$NCCL_IBBW_LOG_FILE" 2>&1 &
	else
		echo "Starting IBBW monitor (auto-detect all ports) -> $NCCL_IBBW_LOG_FILE"
		bash "$ibbw_script" > "$NCCL_IBBW_LOG_FILE" 2>&1 &
	fi
	IBBW_PID=$!
}

stop_ibbw_monitor() {
	if [ -n "$IBBW_PID" ] && kill -0 "$IBBW_PID" 2>/dev/null; then
		kill "$IBBW_PID" 2>/dev/null || true
		wait "$IBBW_PID" 2>/dev/null || true
	fi
	IBBW_PID=""
}

append_ibbw_log_to_nccl_log() {
	if [ -f "$NCCL_IBBW_LOG_FILE" ]; then
		{
			echo
			echo "###################### IBBW Monitor Log #############################"
			cat "$NCCL_IBBW_LOG_FILE"
			echo "#########################################################################"
		} >> "$NCCL_LOG_FILE"
	fi
}

if ! is_enabled "$RUN_NCCL"; then
	echo "NCCL test SKIPPED (disabled by config)."
	GCRRESULT2=incomplete
elif {
	echo "Running NCCL Test..."
	start_ibbw_monitor
	NCCL_NET="$CVAL_NCCL_NET" \
	NCCL_P2P_DISABLE="$(bool_to_int "$CVAL_NCCL_P2P_DISABLE")" \
	NCCL_SHM_DISABLE="$(bool_to_int "$CVAL_NCCL_SHM_DISABLE")" \
	NCCL_DEBUG="$CVAL_NCCL_DEBUG" \
	torchrun --nproc_per_node="$CVAL_NCCL_GPU_COUNT" "$NCCL_SCRIPT" $NCCL_ARGS | tee "$NCCL_LOG_FILE"
}; then
	stop_ibbw_monitor
	append_ibbw_log_to_nccl_log
	echo "NCCL test is complete. Log file: $NCCL_LOG_FILE Summary file: $NCCL_SUMMARY_FILE"
	GCRRESULT2=pass
else
	stop_ibbw_monitor
	append_ibbw_log_to_nccl_log
	echo "NCCL test FAILED. Log file: $NCCL_LOG_FILE Summary file: $NCCL_SUMMARY_FILE"
	GCRRESULT2=fail
fi
write_result_state

# Phase 3: deep learning unit test workload and numerical checks.
if ! is_enabled "$RUN_DLTEST"; then
	echo "DL Test SKIPPED (disabled by config)."
	GCRRESULT3=incomplete
else
	echo "Running DL Test..."
	if bash "$CVAL_VALIDATION_TESTS_DIR/dltest/dltest.sh" "$CVAL_DL_GPU_COUNT"; then
		GCRRESULT3=pass
	else
		GCRRESULT3=fail
	fi
fi
write_result_state

echo "Final c-val test results: storage=$GCRRESULT1 nccl=$GCRRESULT2 dltest=$GCRRESULT3"
