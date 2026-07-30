#!/bin/bash
set -euo pipefail

# Ingest one validation run into metadata databases.
# The structured result JSON is authoritative when available; env files are a
# compatibility fallback for older runtime artifacts.

# main db update
echo "Updating main db with all test results"
CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_CONFIG_PATH=${CVAL_CONFIG_PATH:-$CVAL_REPO_DIR/config/cval.toml}
CVAL_VALIDATION_DB_PATH=${CVAL_VALIDATION_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/validation.db}
CVAL_RUN_HISTORY_ENABLED=${CVAL_RUN_HISTORY_ENABLED:-false}
CVAL_PER_TEST_INGESTION_ENABLED=${CVAL_PER_TEST_INGESTION_ENABLED:-false}
CVAL_RUN_HISTORY_DB_PATH=${CVAL_RUN_HISTORY_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/node-run-history.db}
CVAL_STORAGE_DB_PATH=${CVAL_STORAGE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-storage.db}
CVAL_NCCL_DB_PATH=${CVAL_NCCL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-nccl.db}
CVAL_DL_NUMERICAL_DB_PATH=${CVAL_DL_NUMERICAL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_numerical_correctness.db}
CVAL_DL_COMPUTE_DB_PATH=${CVAL_DL_COMPUTE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_compute_performance.db}
CVAL_DL_COLLECTIVE_DB_PATH=${CVAL_DL_COLLECTIVE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_collective_performance.db}
CVAL_DL_OVERLAP_DB_PATH=${CVAL_DL_OVERLAP_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_overlap_performance.db}
GCRNODE=${GCRNODE:-unknown}
GCRTIME=${GCRTIME:-unknown}
CVAL_RUN_ID=${CVAL_RUN_ID:-${GCRNODE:-unknown}-${GCRTIME:-unknown}}
CVAL_JOB_LOG_DIR=${CVAL_JOB_LOG_DIR:-$CVAL_VALIDATION_ROOT/logs/job_logs/${GCRNODE:-unknown}/$CVAL_RUN_ID}
export CVAL_RUN_ID CVAL_JOB_LOG_DIR

emit_cval_event() {
    local event_name="$1"
    local status="$2"
    local message="${3:-}"
    CVAL_EVENT_NAME="$event_name" CVAL_EVENT_STATUS="$status" \
        CVAL_EVENT_MESSAGE="$message" python3 - <<'PY'
import datetime
import json
import os
from pathlib import Path

payload = {
    "schema_version": "cval.event.v1",
    "event": os.environ["CVAL_EVENT_NAME"],
    "run_id": os.environ.get("CVAL_RUN_ID", "unknown"),
    "test": None,
    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "status": os.environ["CVAL_EVENT_STATUS"],
    "message": os.environ.get("CVAL_EVENT_MESSAGE", ""),
}
serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
log_dir = os.environ.get("CVAL_JOB_LOG_DIR")
if log_dir:
    path = Path(log_dir) / "events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(serialized + "\n")
print("CVAL_EVENT " + serialized)
PY
}

INGESTION_FINISHED=false
on_ingestion_exit() {
    local rc=$?
    trap - EXIT
    if [[ "$INGESTION_FINISHED" != true ]]; then
        emit_cval_event "ingestion_finished" "fail" "db-update.sh exited with code $rc" || true
    fi
    exit "$rc"
}

is_enabled() {
    case "${1,,}" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

valid_status() {
    case "$1" in
        pass|fail|incomplete) return 0 ;;
        *) return 1 ;;
    esac
}

valid_boolean() {
    case "${1,,}" in
        true|false|1|0|yes|no|on|off) return 0 ;;
        *) return 1 ;;
    esac
}

strict_boolean() {
    case "$1" in
        true|false) return 0 ;;
        *) return 1 ;;
    esac
}

assert_snapshot_runtime() {
    if [[ -z "${CVAL_CONFIG_SNAPSHOT_B64:-}" ]]; then
        echo "CVAL_CONFIG_SNAPSHOT_B64 is required for v2 database writes" >&2
        return 1
    fi
    RESULT_GLOBAL_CONFIG_DIGEST="$result_global_config_digest" \
        RESULT_DIGEST="$result_digest" \
        RESULT_SCHEMA_VERSION="$result_schema_version" \
        PYTHONPATH="$CVAL_REPO_DIR" python3 - <<'PY'
import os
from pathlib import Path

from cval.config import load_config_snapshot
from cval.validation.results import (
    ValidationResultV2,
    load_validation_result,
    validation_result_digest,
    validation_result_v2_digest,
)
from cval.validation.runtime import effective_config_digest

repo_root = os.environ.get("CVAL_TEST_REPO_ROOT") or os.environ.get("CVAL_REPO_DIR")
config = load_config_snapshot(
    os.environ["CVAL_CONFIG_SNAPSHOT_B64"],
    repo_root=None if not repo_root else Path(repo_root),
)
storage = config.storage
runtime = config.runtime
expected = {
    "CVAL_VALIDATION_ROOT": runtime.validation_root,
    "CVAL_VALIDATION_DB_PATH": storage.validation_db_path,
    "CVAL_RUN_HISTORY_DB_PATH": storage.run_history_db_path,
    "CVAL_STORAGE_DB_PATH": storage.storage_db_path,
    "CVAL_NCCL_DB_PATH": storage.nccl_db_path,
    "CVAL_DL_NUMERICAL_DB_PATH": storage.dl_numerical_db_path,
    "CVAL_DL_COMPUTE_DB_PATH": storage.dl_compute_db_path,
    "CVAL_DL_COLLECTIVE_DB_PATH": storage.dl_collective_db_path,
    "CVAL_DL_OVERLAP_DB_PATH": storage.dl_overlap_db_path,
    "CVAL_RUN_HISTORY_ENABLED": str(storage.run_history_enabled).lower(),
    "CVAL_PER_TEST_INGESTION_ENABLED": str(
        storage.per_test_ingestion_enabled
    ).lower(),
}
expected_digest = effective_config_digest(config)
expected["CVAL_CONFIG_DIGEST"] = expected_digest
nccl = config.tests.registry.get("nccl")
if nccl is not None:
    expected["CVAL_NCCL_ITERATIONS"] = str(nccl.definition.settings["iterations"])
    expected["CVAL_IBBW_ENABLED"] = str(
        nccl.definition.settings["ibbw_enabled"]
    ).lower()
for name, value in expected.items():
    if os.environ.get(name) != str(value):
        raise SystemExit(
            f"Runtime value {name} does not match the effective configuration snapshot"
        )
if (
    os.environ.get("RESULT_SCHEMA_VERSION") == "cval.results.v2"
    and os.environ.get("RESULT_GLOBAL_CONFIG_DIGEST") != expected_digest
):
    raise SystemExit(
        "Result global_config_digest does not match the effective configuration snapshot"
    )
result = load_validation_result(Path(os.environ["CVAL_RESULT_JSON_FILE"]))
actual_result_digest = (
    validation_result_v2_digest(result)
    if isinstance(result, ValidationResultV2)
    else validation_result_digest(result)
)
if os.environ.get("RESULT_DIGEST") != actual_result_digest:
    raise SystemExit("Result digest changed after structured validation")
PY
}

bind_result_digest() {
    RESULT_DIGEST="$result_digest" python3 - <<'PY'
import os
from pathlib import Path

log_dir = Path(os.environ["CVAL_JOB_LOG_DIR"])
marker = log_dir / ".ingestion-result-digest"
digest = os.environ["RESULT_DIGEST"] + "\n"
if marker.is_symlink():
    raise SystemExit(f"Ingestion digest marker is a symlink: {marker}")
try:
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(marker, flags, 0o600)
except FileExistsError:
    if marker.read_text(encoding="utf-8") != digest:
        raise SystemExit("Run was already ingested with a different result digest")
else:
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(digest)
        handle.flush()
        os.fsync(handle.fileno())
PY
}

STORAGE_OUTPUT_DIR=${STORAGE_OUTPUT_DIR:-$CVAL_VALIDATION_ROOT/validation_tests/storage/runs/$GCRNODE/$CVAL_RUN_ID/artifacts}
echo "Storage Output dir: $STORAGE_OUTPUT_DIR"
NCCL_OUTPUT_DIR=${NCCL_OUTPUT_DIR:-$CVAL_VALIDATION_ROOT/validation_tests/nccl/runs/$GCRNODE/$CVAL_RUN_ID/artifacts}
echo "NCCL Output dir: $NCCL_OUTPUT_DIR"

GCRRESULT1=${GCRRESULT1:-fail}
GCRRESULT2=${GCRRESULT2:-fail}
GCRRESULT3=${GCRRESULT3:-fail}
RUN_STORAGE=${RUN_STORAGE:-true}
RUN_NCCL=${RUN_NCCL:-true}
RUN_DLTEST=${RUN_DLTEST:-true}
CVAL_IMAGE_NAME=${CVAL_IMAGE_NAME:-}
CVAL_PYTORCH_VERSION=${CVAL_PYTORCH_VERSION:-}
CVAL_CUDA_VERSION=${CVAL_CUDA_VERSION:-}
CVAL_NCCL_ITERATIONS=${CVAL_NCCL_ITERATIONS:-20}
STRUCTURED_RESULT_LOADED=false
result_node=""
result_timestamp=""
result_run_id=""
result_schema_version=""
result_global_config_digest=""
result_digest=""
result_storage_artifacts=""
result_nccl_summary=""

if [ -n "${CVAL_RESULT_JSON_FILE:-}" ] && [ -f "$CVAL_RESULT_JSON_FILE" ]; then
    echo "Loading structured test result state from $CVAL_RESULT_JSON_FILE"
    if ! result_projection=$(
        PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli result \
            --result-json "$CVAL_RESULT_JSON_FILE"
    ); then
        echo "Structured result validation failed; refusing all DB writes." >&2
        exit 1
    fi
    while IFS='=' read -r key value; do
        case "$key" in
            GCRRESULT1) GCRRESULT1="$value" ;;
            GCRRESULT2) GCRRESULT2="$value" ;;
            GCRRESULT3) GCRRESULT3="$value" ;;
            RUN_STORAGE) RUN_STORAGE="$value" ;;
            RUN_NCCL) RUN_NCCL="$value" ;;
            RUN_DLTEST) RUN_DLTEST="$value" ;;
            overall_result) overall_result="$value" ;;
            image_name) CVAL_IMAGE_NAME="$value" ;;
            pytorch_version) CVAL_PYTORCH_VERSION="$value" ;;
            cuda_version) CVAL_CUDA_VERSION="$value" ;;
            result_node) result_node="$value" ;;
            result_timestamp) result_timestamp="$value" ;;
            result_run_id) result_run_id="$value" ;;
            result_schema_version) result_schema_version="$value" ;;
            result_global_config_digest) result_global_config_digest="$value" ;;
            result_digest) result_digest="$value" ;;
            result_storage_artifacts) result_storage_artifacts="$value" ;;
            result_nccl_summary) result_nccl_summary="$value" ;;
        esac
    done <<< "$result_projection"
    if [[ "$result_node" != "$GCRNODE" ]]; then
        echo "Result node mismatch: expected $GCRNODE, got $result_node" >&2
        exit 1
    fi
    if [[ "$result_timestamp" != "$GCRTIME" ]]; then
        echo "Result timestamp mismatch: expected $GCRTIME, got $result_timestamp" >&2
        exit 1
    fi
    if [[ "$result_run_id" != "$CVAL_RUN_ID" ]]; then
        echo "Result run_id mismatch: expected $CVAL_RUN_ID, got $result_run_id" >&2
        exit 1
    fi
    STRUCTURED_RESULT_LOADED=true
elif [ -n "${CVAL_RESULT_ENV_FILE:-}" ] && [ -f "$CVAL_RESULT_ENV_FILE" ]; then
    # Legacy fallback accepts only fixed status/activation assignments. Never
    # source result files as shell code.
    echo "Loading test result state from $CVAL_RESULT_ENV_FILE"
    while IFS='=' read -r key value; do
        case "$key" in
            GCRRESULT1) GCRRESULT1="$value" ;;
            GCRRESULT2) GCRRESULT2="$value" ;;
            GCRRESULT3) GCRRESULT3="$value" ;;
            RUN_STORAGE) RUN_STORAGE="$value" ;;
            RUN_NCCL) RUN_NCCL="$value" ;;
            RUN_DLTEST) RUN_DLTEST="$value" ;;
            overall_result) overall_result="$value" ;;
            result_node) result_node="$value" ;;
            result_timestamp) result_timestamp="$value" ;;
            result_run_id) result_run_id="$value" ;;
            ""|'#'*) ;;
            *) echo "Ignoring unknown legacy result key: $key" >&2 ;;
        esac
    done < "$CVAL_RESULT_ENV_FILE"
    if ! is_enabled "${CVAL_ALLOW_LEGACY_RESULT_ENV:-false}"; then
        echo "Legacy result env fallback requires CVAL_ALLOW_LEGACY_RESULT_ENV=true" >&2
        exit 1
    fi
    if [[ "$result_node" != "$GCRNODE" || \
          "$result_timestamp" != "$GCRTIME" || \
          "$result_run_id" != "$CVAL_RUN_ID" ]]; then
        echo "Legacy result identity does not match the current run" >&2
        exit 1
    fi
else
    echo "c-val result state file not found; refusing all DB writes." >&2
    exit 1
fi

for status_value in "$GCRRESULT1" "$GCRRESULT2" "$GCRRESULT3"; do
    valid_status "$status_value" || {
        echo "Invalid test status in result state: $status_value" >&2
        exit 1
    }
done
for enabled_value in "$RUN_STORAGE" "$RUN_NCCL" "$RUN_DLTEST"; do
    valid_boolean "$enabled_value" || {
        echo "Invalid test activation in result state: $enabled_value" >&2
        exit 1
    }
done

# Structured JSON has already passed aggregate consistency validation and may
# contain dynamically registered tests. Recompute only for legacy env fallback.
if [[ "$STRUCTURED_RESULT_LOADED" != true ]]; then
    overall_result=pass
    enabled_count=0
    for pair in \
        "$RUN_STORAGE:$GCRRESULT1" \
        "$RUN_NCCL:$GCRRESULT2" \
        "$RUN_DLTEST:$GCRRESULT3"; do
        enabled=${pair%%:*}
        result=${pair#*:}
        if is_enabled "$enabled"; then
            enabled_count=$((enabled_count + 1))
            [ "$result" = "pass" ] || overall_result=fail
        fi
    done
    [ "$enabled_count" -gt 0 ] || overall_result=incomplete
fi
valid_status "$overall_result" || {
    echo "Invalid overall status in result state: $overall_result" >&2
    exit 1
}
if [[ "$STRUCTURED_RESULT_LOADED" == true ]]; then
        expected_job_log_dir="$CVAL_VALIDATION_ROOT/logs/job_logs/$GCRNODE/$CVAL_RUN_ID"
        if [[ "$CVAL_JOB_LOG_DIR" != "$expected_job_log_dir" || \
                    "$CVAL_RESULT_JSON_FILE" != "$expected_job_log_dir/result.json" ]]; then
                echo "Runtime global evidence paths do not match the canonical v2 run" >&2
                exit 1
        fi
        if [[ -n "$result_storage_artifacts" && \
                    "$STORAGE_OUTPUT_DIR" != "$result_storage_artifacts" ]]; then
                echo "STORAGE_OUTPUT_DIR does not match the validated v2 result" >&2
                exit 1
        fi
        if [[ -n "$result_nccl_summary" && \
                    "${NCCL_SUMMARY_FILE:-}" != "$result_nccl_summary" ]]; then
                echo "NCCL_SUMMARY_FILE does not match the validated v2 result" >&2
                exit 1
        fi
    assert_snapshot_runtime
    strict_boolean "$CVAL_RUN_HISTORY_ENABLED" || {
        echo "CVAL_RUN_HISTORY_ENABLED must be true or false" >&2
        exit 1
    }
    strict_boolean "$CVAL_PER_TEST_INGESTION_ENABLED" || {
        echo "CVAL_PER_TEST_INGESTION_ENABLED must be true or false" >&2
        exit 1
    }
fi

# Validate result/config provenance and every compatibility target before events.
if [[ "$STRUCTURED_RESULT_LOADED" == true ]]; then
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli \
        db-preflight-compatibility-result \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest"
fi

# Validate every test/config/evidence path and every configured legacy,
# run-history, DL-rebuild, and canonical target before the first v2 DB write.
if [[ "$result_schema_version" == "cval.results.v2" ]]; then
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli \
        db-preflight-test-results \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest"
    bind_result_digest
fi

trap on_ingestion_exit EXIT
emit_cval_event "ingestion_started" "incomplete"

# Record every completed v2 execution before test-specific metric ingestion.
# This write is idempotent by run_id and does not imply metric ingestion passed.
if [[ "$result_schema_version" == "cval.results.v2" ]] && \
   is_enabled "$CVAL_RUN_HISTORY_ENABLED"; then
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-upsert-run-history \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest" \
        --db-path "$CVAL_RUN_HISTORY_DB_PATH"
elif [[ "$result_schema_version" == "cval.results.v2" ]]; then
    echo "Node run-history write skipped (run_history_enabled=false)."
fi

# Storage metrics are valid only when the storage phase itself passed.
if is_enabled "$RUN_STORAGE" && [ "$GCRRESULT1" = "pass" ]; then
    echo "Updating storage db with test results"
    storage_run_id_args=()
    if is_enabled "$CVAL_PER_TEST_INGESTION_ENABLED"; then
        storage_run_id_args=(--run-id "$CVAL_RUN_ID")
    fi
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-add-storage-result \
        "$GCRNODE" \
        "$GCRTIME" \
        "$STORAGE_OUTPUT_DIR" \
        --image-name "$CVAL_IMAGE_NAME" \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest" \
        --immutable \
        "${storage_run_id_args[@]}" \
        --db-path "$CVAL_STORAGE_DB_PATH"
    echo "Storage DB update completed."
else
    echo "Skipping storage metrics DB update because storage result is $GCRRESULT1."
fi


# NCCL metric ingestion depends on the all-reduce summary JSON.

echo "Updating nccl db with test results"
NCCL_LOG_FILE=${NCCL_LOG_FILE:-$NCCL_OUTPUT_DIR/nccl-$GCRNODE-$GCRTIME.log}
echo "NCCL Log file: $NCCL_LOG_FILE"


if is_enabled "$RUN_NCCL" && [ "$GCRRESULT2" = "pass" ]; then
    if [[ ! -f "$NCCL_SUMMARY_FILE" ]]; then
        echo "Passing NCCL result is missing required summary: $NCCL_SUMMARY_FILE" >&2
        exit 1
    fi
    # Persist one consolidated row: aggregate all-reduce BUS_BW/LATENCY plus
    # each HCA port's maximum observed bandwidth (mlx5_0..mlx5_13).
    nccl_run_id_args=()
    nccl_hca_args=()
    if is_enabled "$CVAL_PER_TEST_INGESTION_ENABLED"; then
        nccl_run_id_args=(--run-id "$CVAL_RUN_ID")
    fi
    if is_enabled "$CVAL_IBBW_ENABLED"; then
        nccl_hca_args=(--require-hca-samples)
    fi
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-add-nccl-health \
        "$GCRNODE" \
        "$GCRTIME" \
        "$NCCL_SUMMARY_FILE" \
        --iterations "$CVAL_NCCL_ITERATIONS" \
        --image-name "$CVAL_IMAGE_NAME" \
        --cuda-version "$CVAL_CUDA_VERSION" \
        --pytorch-version "$CVAL_PYTORCH_VERSION" \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest" \
        --immutable \
        "${nccl_hca_args[@]}" \
        "${nccl_run_id_args[@]}" \
        --db-path "$CVAL_NCCL_DB_PATH"

    echo "NCCL IB_HEALTH DB update completed."
else
    echo "Skipping NCCL metrics DB update because result is $GCRRESULT2."
fi

# Commit fixed compatibility status rows only after all required metric
# artifacts validate and their DB writes succeed. This is one SQLite transaction.
echo "Updating main db with test results"
PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-add-run-results \
    "$GCRNODE" \
    "$GCRTIME" \
    --storage-result "$GCRRESULT1" \
    --nccl-result "$GCRRESULT2" \
    --dltest-result "$GCRRESULT3" \
    --overall-result "$overall_result" \
    --image-name "$CVAL_IMAGE_NAME" \
    --pytorch-version "$CVAL_PYTORCH_VERSION" \
    --cuda-version "$CVAL_CUDA_VERSION" \
    --result-json "$CVAL_RESULT_JSON_FILE" \
    --result-digest "$result_digest" \
    --db-path "$CVAL_VALIDATION_DB_PATH"
echo "Main DB update completed."

# U7 canonical per-test persistence is an independently activated dual-write.
# Compatibility raw status is already durable, so one isolated adapter failure
# cannot leave current readers showing stale execution state.
if [[ "$result_schema_version" == "cval.results.v2" ]] && \
   is_enabled "$CVAL_PER_TEST_INGESTION_ENABLED"; then
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli \
        db-ingest-test-results \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest"
elif [[ "$result_schema_version" == "cval.results.v2" ]]; then
    echo "Canonical per-test DB writes skipped (per_test_ingestion_enabled=false)."
fi

emit_cval_event "ingestion_finished" "pass"
INGESTION_FINISHED=true
trap - EXIT