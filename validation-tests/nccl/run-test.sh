#!/usr/bin/env bash
set -u -o pipefail

# Run single-node NCCL all-reduce while sampling every selected InfiniBand port.
# This script owns NCCL-specific execution and log assembly; the top-level c-val
# runner owns only phase ordering and aggregate status.

is_enabled() {
    case "${1,,}" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

bool_to_int() {
    if is_enabled "$1"; then echo 1; else echo 0; fi
}

CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}
CVAL_NCCL_GPU_COUNT=${CVAL_NCCL_GPU_COUNT:-8}
CVAL_NCCL_ITERATIONS=${CVAL_NCCL_ITERATIONS:-20}
CVAL_NCCL_DATA_SIZE_GB=${CVAL_NCCL_DATA_SIZE_GB:-8}
CVAL_IBBW_ENABLED=${CVAL_IBBW_ENABLED:-true}
CVAL_IBBW_START_DEVICE=${CVAL_IBBW_START_DEVICE:-}
CVAL_IBBW_END_DEVICE=${CVAL_IBBW_END_DEVICE:-}
CVAL_NCCL_NET=${CVAL_NCCL_NET:-IB}
CVAL_NCCL_P2P_DISABLE=${CVAL_NCCL_P2P_DISABLE:-true}
CVAL_NCCL_SHM_DISABLE=${CVAL_NCCL_SHM_DISABLE:-true}
CVAL_NCCL_DEBUG=${CVAL_NCCL_DEBUG:-INFO}
CVAL_NCCL_EVALUATION_ENABLED=${CVAL_NCCL_EVALUATION_ENABLED:-false}

GCRNODE=${GCRNODE:-unknown}
GCRTIME=${GCRTIME:-unknown}
CVAL_RUN_ID=${CVAL_RUN_ID:-$GCRNODE-$GCRTIME}
NCCL_RUN_DIR=${NCCL_RUN_DIR:-\
$CVAL_VALIDATION_ROOT/validation_tests/nccl/runs/$GCRNODE/$CVAL_RUN_ID}
NCCL_OUTPUT_DIR=${NCCL_OUTPUT_DIR:-${CVAL_TEST_OUTPUT_DIR:-$NCCL_RUN_DIR/artifacts}}
NCCL_LOG_FILE=${NCCL_LOG_FILE:-${CVAL_TEST_LOG_DIR:-$NCCL_RUN_DIR}/workload.log}
NCCL_SUMMARY_FILE=${NCCL_SUMMARY_FILE:-${CVAL_TEST_SUMMARY_FILE:-$NCCL_RUN_DIR/summary.json}}
NCCL_IBBW_LOG_FILE=${NCCL_IBBW_LOG_FILE:-$NCCL_OUTPUT_DIR/ibbw-$GCRNODE-$GCRTIME.log}
NCCL_RUNTIME_EVIDENCE_FILE=${NCCL_RUNTIME_EVIDENCE_FILE:-$NCCL_OUTPUT_DIR/runtime-evidence.json}
NCCL_RUNTIME_EVIDENCE_POST_FILE=${NCCL_RUNTIME_EVIDENCE_POST_FILE:-${NCCL_RUNTIME_EVIDENCE_FILE%.json}.post.json}
NCCL_SCRIPT="$CVAL_VALIDATION_TESTS_DIR/nccl/single-node-allreduce.py"
IBBW_PID=""
NCCL_SUMMARY_STAGE_DIR=""
NCCL_SUMMARY_STAGE_FILE=""
NCCL_METRICS_STAGE_FILE=""

mkdir -p "$NCCL_OUTPUT_DIR"

prepare_summary_stage() {
    NCCL_SUMMARY_STAGE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cval-nccl-summary.XXXXXX")
    chmod 700 "$NCCL_SUMMARY_STAGE_DIR"
    NCCL_SUMMARY_STAGE_FILE="$NCCL_SUMMARY_STAGE_DIR/summary.json"
    NCCL_METRICS_STAGE_FILE="$NCCL_SUMMARY_STAGE_DIR/metrics.json"
}

cleanup_summary_stage() {
    if [[ -n "$NCCL_SUMMARY_STAGE_DIR" && -d "$NCCL_SUMMARY_STAGE_DIR" ]]; then
        rm -rf -- "$NCCL_SUMMARY_STAGE_DIR"
    fi
    NCCL_SUMMARY_STAGE_DIR=""
    NCCL_SUMMARY_STAGE_FILE=""
    NCCL_METRICS_STAGE_FILE=""
}

start_ibbw_monitor() {
    local ibbw_script="$CVAL_VALIDATION_TESTS_DIR/nccl/ibbw.sh"
    if ! is_enabled "$CVAL_IBBW_ENABLED"; then
        echo "IBBW monitor disabled by config"
        return 0
    fi
    if [[ ! -f "$ibbw_script" ]]; then
        echo "Warning: IBBW monitor script not found: $ibbw_script"
        return 0
    fi
    if [[ -n "$CVAL_IBBW_START_DEVICE" && -n "$CVAL_IBBW_END_DEVICE" ]]; then
        echo "Starting IBBW monitor: $ibbw_script $CVAL_IBBW_START_DEVICE "\
"$CVAL_IBBW_END_DEVICE -> $NCCL_IBBW_LOG_FILE"
        bash "$ibbw_script" "$CVAL_IBBW_START_DEVICE" "$CVAL_IBBW_END_DEVICE" \
            > "$NCCL_IBBW_LOG_FILE" 2>&1 &
    else
        echo "Starting IBBW monitor (auto-detect all ports) -> $NCCL_IBBW_LOG_FILE"
        bash "$ibbw_script" > "$NCCL_IBBW_LOG_FILE" 2>&1 &
    fi
    IBBW_PID=$!
}

stop_ibbw_monitor() {
    if [[ -n "$IBBW_PID" ]] && kill -0 "$IBBW_PID" 2>/dev/null; then
        kill "$IBBW_PID" 2>/dev/null || true
        wait "$IBBW_PID" 2>/dev/null || true
    fi
    IBBW_PID=""
}

append_ibbw_log_to_nccl_log() {
    if [[ -f "$NCCL_IBBW_LOG_FILE" ]]; then
        echo
        echo "###################### IBBW Monitor Log #############################"
        cat "$NCCL_IBBW_LOG_FILE"
        echo "#########################################################################"
    fi
}

on_exit() {
    stop_ibbw_monitor
    cleanup_summary_stage
}

trap on_exit EXIT INT TERM

prepare_summary_stage

args=(
    --result-file "$NCCL_METRICS_STAGE_FILE"
    --iterations "$CVAL_NCCL_ITERATIONS"
    --data-size-gb "$CVAL_NCCL_DATA_SIZE_GB"
)

echo "Running NCCL Test..."
if is_enabled "$CVAL_NCCL_EVALUATION_ENABLED"; then
    echo "Collecting NCCL runtime evidence -> $NCCL_RUNTIME_EVIDENCE_FILE"
    if ! PYTHONPATH="$CVAL_REPO_DIR${PYTHONPATH:+:$PYTHONPATH}" \
        python3 -m cval.nccl_eval.runtime_evidence --output "$NCCL_RUNTIME_EVIDENCE_FILE"; then
        echo "NCCL pre-workload runtime evidence collection FAILED" >&2
        exit 1
    fi
    if ! PYTHONPATH="$CVAL_REPO_DIR${PYTHONPATH:+:$PYTHONPATH}" \
        python3 -m cval.nccl_eval.runtime_evidence --validate "$NCCL_RUNTIME_EVIDENCE_FILE"; then
        echo "NCCL pre-workload runtime evidence validation FAILED" >&2
        exit 1
    fi
else
    echo "NCCL PostgreSQL evaluation evidence disabled by config"
fi
start_ibbw_monitor
NCCL_NET="$CVAL_NCCL_NET" \
NCCL_P2P_DISABLE="$(bool_to_int "$CVAL_NCCL_P2P_DISABLE")" \
NCCL_SHM_DISABLE="$(bool_to_int "$CVAL_NCCL_SHM_DISABLE")" \
NCCL_DEBUG="$CVAL_NCCL_DEBUG" \
    torchrun --nproc_per_node="$CVAL_NCCL_GPU_COUNT" "$NCCL_SCRIPT" "${args[@]}"
rc=$?

stop_ibbw_monitor
append_ibbw_log_to_nccl_log
if [[ $rc -ne 0 ]]; then
    exit "$rc"
fi

finalize_args=(
    --metrics "$NCCL_METRICS_STAGE_FILE"
    --ibbw-log "$NCCL_IBBW_LOG_FILE"
    --output "$NCCL_SUMMARY_STAGE_FILE"
    --ibbw-log-reference "${CVAL_CANONICAL_NCCL_IBBW_LOG_FILE:-$NCCL_IBBW_LOG_FILE}"
)
if is_enabled "$CVAL_IBBW_ENABLED"; then
    finalize_args+=(--require-hca-samples)
fi
if ! PYTHONPATH="$CVAL_REPO_DIR${PYTHONPATH:+:$PYTHONPATH}" \
    python3 -m cval.validation.nccl_summary "${finalize_args[@]}"; then
    echo "NCCL summary finalization FAILED" >&2
    exit 1
fi

if is_enabled "$CVAL_NCCL_EVALUATION_ENABLED"; then
    if ! PYTHONPATH="$CVAL_REPO_DIR${PYTHONPATH:+:$PYTHONPATH}" \
        python3 -m cval.nccl_eval.runtime_evidence --output "$NCCL_RUNTIME_EVIDENCE_POST_FILE"; then
        echo "NCCL post-workload runtime evidence collection FAILED" >&2
        exit 1
    fi
    if ! cmp -s -- "$NCCL_RUNTIME_EVIDENCE_FILE" "$NCCL_RUNTIME_EVIDENCE_POST_FILE"; then
        echo "NCCL runtime evidence changed across the workload" >&2
        exit 1
    fi
fi

if ! python3 - "$NCCL_SUMMARY_STAGE_FILE" "$CVAL_NCCL_ITERATIONS" "$CVAL_NCCL_DATA_SIZE_GB" <<'PY'
import json
import math
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected_iterations = int(sys.argv[2])
expected_data_size_gb = int(sys.argv[3])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"invalid NCCL summary: {exc}")

required = (
    "GCR_BUSBW", "GCR_ALGBW", "GCR_LATENCY", "GCR_ITERATIONS",
    "GCR_DATA_SIZE_GB",
)
for key in required:
    if key not in payload:
        raise SystemExit(f"NCCL summary missing {key}")
for key in ("GCR_BUSBW", "GCR_ALGBW", "GCR_LATENCY"):
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SystemExit(f"NCCL summary {key} is not numeric")
    if not math.isfinite(float(value)) or float(value) <= 0.0:
        raise SystemExit(f"NCCL summary {key} must be finite and positive")
if int(payload["GCR_ITERATIONS"]) != expected_iterations:
    raise SystemExit("NCCL summary iteration count does not match config")
if int(payload["GCR_DATA_SIZE_GB"]) != expected_data_size_gb:
    raise SystemExit("NCCL summary data size does not match config")
ports = payload.get("GCR_IB_PORT_BW_GBPS", {})
if not isinstance(ports, dict):
    raise SystemExit("NCCL summary HCA port metrics must be an object")
PY
then
    echo "NCCL summary validation FAILED: $NCCL_SUMMARY_STAGE_FILE" >&2
    exit 1
fi

if ! PYTHONPATH="$CVAL_REPO_DIR${PYTHONPATH:+:$PYTHONPATH}" \
    python3 -m cval.validation.secure_fs \
        --staged "$NCCL_SUMMARY_STAGE_FILE" \
        --destination "$NCCL_SUMMARY_FILE" \
        --test-id nccl \
        --summary-name summary.json; then
    echo "NCCL summary publication FAILED: $NCCL_SUMMARY_FILE" >&2
    exit 1
fi
echo "NCCL summary published to: $NCCL_SUMMARY_FILE"

trap - EXIT INT TERM
cleanup_summary_stage

exit 0
