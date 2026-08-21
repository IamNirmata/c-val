#!/bin/bash
set -euo pipefail

# Ingest one validation run into metadata databases.
# The structured result JSON is authoritative when available; env files remain
# a historical fallback for older runtime artifacts.

# main db update
echo "Updating main db with all test results"
CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_CONFIG_PATH=${CVAL_CONFIG_PATH:-$CVAL_REPO_DIR/config/cval.toml}
CVAL_VALIDATION_DB_PATH=${CVAL_VALIDATION_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/validation.db}
CVAL_STORAGE_DB_PATH=${CVAL_STORAGE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-storage.db}
CVAL_NCCL_DB_PATH=${CVAL_NCCL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/test-nccl.db}
CVAL_NCCL_OUTBOX_ROOT=${CVAL_NCCL_OUTBOX_ROOT:-$CVAL_VALIDATION_ROOT/nccl_eval/outbox}
CVAL_NCCL_EVALUATION_ENABLED=${CVAL_NCCL_EVALUATION_ENABLED:-false}
CVAL_DL_NUMERICAL_DB_PATH=${CVAL_DL_NUMERICAL_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_numerical_correctness.db}
CVAL_DL_COMPUTE_DB_PATH=${CVAL_DL_COMPUTE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_compute_performance.db}
CVAL_DL_COLLECTIVE_DB_PATH=${CVAL_DL_COLLECTIVE_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_collective_performance.db}
CVAL_DL_OVERLAP_DB_PATH=${CVAL_DL_OVERLAP_DB_PATH:-$CVAL_VALIDATION_ROOT/metadata/dltest_overlap_performance.db}
CVAL_DL_METRIC_LOCK_FILE=${CVAL_DL_METRIC_LOCK_FILE:-$CVAL_VALIDATION_ROOT/baselines/.dl-metric-refresh.lock}
CVAL_DL_METRIC_LOCK_HELPER=${CVAL_DL_METRIC_LOCK_HELPER:-$CVAL_REPO_DIR/scripts/dl-metric-lock.py}
GCRNODE=${GCRNODE:-unknown}
GCRTIME=${GCRTIME:-unknown}
CVAL_RUN_ID=${CVAL_RUN_ID:-${GCRNODE:-unknown}-${GCRTIME:-unknown}}
CVAL_JOB_LOG_DIR=${CVAL_JOB_LOG_DIR:-$CVAL_VALIDATION_ROOT/logs/job_logs/${GCRNODE:-unknown}/$CVAL_RUN_ID}
DLTEST_RUN_DIR=${DLTEST_RUN_DIR:-$CVAL_VALIDATION_ROOT/validation_tests/dltest/runs/$GCRNODE/$CVAL_RUN_ID}
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

assert_snapshot_runtime() {
    if [[ -z "${CVAL_CONFIG_SNAPSHOT_B64:-}" ]]; then
        echo "CVAL_CONFIG_SNAPSHOT_B64 is required for v2 database writes" >&2
        return 1
    fi
    RESULT_GLOBAL_CONFIG_DIGEST="$result_global_config_digest" \
        RESULT_DIGEST="$result_digest" \
        RESULT_SCHEMA_VERSION="$result_schema_version" \
        PYTHONPATH="$CVAL_REPO_DIR" python3 - <<'PY'
import json
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
    "CVAL_STORAGE_DB_PATH": storage.storage_db_path,
    "CVAL_NCCL_DB_PATH": storage.nccl_db_path,
    "CVAL_DL_NUMERICAL_DB_PATH": storage.dl_numerical_db_path,
    "CVAL_DL_COMPUTE_DB_PATH": storage.dl_compute_db_path,
    "CVAL_DL_COLLECTIVE_DB_PATH": storage.dl_collective_db_path,
    "CVAL_DL_OVERLAP_DB_PATH": storage.dl_overlap_db_path,
    "CVAL_DL_METRIC_LOCK_FILE": str(
        Path(config.baseline.baseline_root_path) / ".dl-metric-refresh.lock"
    ),
}
expected_digest = effective_config_digest(config)
expected["CVAL_CONFIG_DIGEST"] = expected_digest
nccl = config.tests.registry.get("nccl")
if nccl is not None:
    settings = nccl.definition.settings
    expected["CVAL_NCCL_ITERATIONS"] = str(settings["iterations"])
    expected["CVAL_IBBW_ENABLED"] = str(
        settings["ibbw_enabled"]
    ).lower()
    expected.update(
        {
            "CVAL_NCCL_OUTBOX_ROOT": f"{runtime.validation_root.rstrip('/')}/nccl_eval/outbox",
            "CVAL_NCCL_EVALUATION_ENABLED": str(settings["evaluation_enabled"]).lower(),
            "CVAL_NCCL_EVALUATION_TEST_NAME": str(settings["evaluation_test_name"]),
            "CVAL_NCCL_EVALUATION_TEST_DEFINITION_VERSION": str(
                settings["evaluation_test_definition_version"]
            ),
            "CVAL_NCCL_EVALUATION_COLLECTIVE": str(settings["evaluation_collective"]),
            "CVAL_NCCL_EVALUATION_DATATYPE": str(settings["evaluation_datatype"]),
            "CVAL_NCCL_EVALUATION_REDUCTION": str(settings["evaluation_reduction"]),
            "CVAL_NCCL_EVALUATION_MESSAGE_SIZE_BYTES": str(
                settings["evaluation_message_size_bytes"]
            ),
            "CVAL_NCCL_EVALUATION_WARMUP_ITERATIONS": str(
                settings["evaluation_warmup_iterations"]
            ),
            "CVAL_NCCL_EVALUATION_SAMPLES_PER_RESULT": str(
                settings["evaluation_samples_per_result"]
            ),
            "CVAL_NCCL_EVALUATION_ITERATION_SEMANTICS": str(
                settings["evaluation_iteration_semantics"]
            ),
            "CVAL_NCCL_EVALUATION_SAMPLE_SEMANTICS": str(
                settings["evaluation_sample_semantics"]
            ),
            "CVAL_NCCL_EVALUATION_LATENCY_UNIT": str(
                settings["evaluation_latency_unit"]
            ),
            "CVAL_NCCL_EVALUATION_LATENCY_SOURCE_UNIT": str(
                settings["evaluation_latency_source_unit"]
            ),
            "CVAL_NCCL_EVALUATION_LATENCY_CONVERSION": str(
                settings["evaluation_latency_conversion"]
            ),
            "CVAL_NCCL_EVALUATION_DRIVER_GROUP_SOURCE": str(
                settings["evaluation_driver_group_source"]
            ),
            "CVAL_NCCL_EVALUATION_TOPOLOGY_CLASS_SOURCE": str(
                settings["evaluation_topology_class_source"]
            ),
        }
    )
for name, value in expected.items():
    if os.environ.get(name) != str(value):
        raise SystemExit(
            f"Runtime value {name} does not match the effective configuration snapshot"
        )
if (
    os.environ.get("RESULT_SCHEMA_VERSION") in {"cval.results", "cval.results.v2"}
    and os.environ.get("RESULT_GLOBAL_CONFIG_DIGEST") != expected_digest
):
    raise SystemExit(
        "Result global_config_digest does not match the effective configuration snapshot"
    )
raw_layout = os.environ.get("CVAL_SECURE_RUN_LAYOUT_JSON")
if raw_layout:
    layout = json.loads(raw_layout)
    result_file_fd = layout.get("result_file_fd")
    if isinstance(result_file_fd, bool) or not isinstance(result_file_fd, int):
        raise SystemExit("Secure result file descriptor is required for current ingestion")
    result_path = Path(f"/proc/self/fd/{result_file_fd}")
else:
    result_path = Path(os.environ["CVAL_RESULT_JSON_FILE"])
result = load_validation_result(result_path)
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
import json
import os
import stat
from pathlib import Path

digest = os.environ["RESULT_DIGEST"] + "\n"
raw_layout = os.environ.get("CVAL_SECURE_RUN_LAYOUT_JSON")
if raw_layout:
    try:
        layout = json.loads(raw_layout)
        run_dir_fd = layout["run_dir_fd"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise SystemExit("Secure run descriptor is required for digest binding") from exc
    if isinstance(run_dir_fd, bool) or not isinstance(run_dir_fd, int):
        raise SystemExit("Secure run descriptor is invalid")
    try:
        inherited = {int(value) for value in os.environ["CVAL_SECURE_RUN_FDS"].split(",")}
    except (KeyError, ValueError) as exc:
        raise SystemExit("Secure inherited descriptor list is invalid") from exc
    if run_dir_fd not in inherited:
        raise SystemExit("Secure run descriptor was not inherited")
    marker = ".ingestion-result-digest"
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(marker, flags, 0o600, dir_fd=run_dir_fd)
    except FileExistsError:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(marker, flags, dir_fd=run_dir_fd)
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            marker_stat = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(marker_stat.st_mode)
                or stat.S_IMODE(marker_stat.st_mode) != 0o600
                or marker_stat.st_uid != os.geteuid()
                or marker_stat.st_nlink != 1
                or marker_stat.st_size != len(digest)
            ):
                raise SystemExit("Existing ingestion digest marker is unsafe")
            existing = handle.read()
        if existing != digest:
            raise SystemExit("Run was already ingested with a different result digest")
    else:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(digest)
            handle.flush()
            os.fsync(handle.fileno())
        os.fsync(run_dir_fd)
else:
    marker = Path(os.environ["CVAL_JOB_LOG_DIR"]) / ".ingestion-result-digest"
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
NCCL_RUN_DIR=${NCCL_RUN_DIR:-$(dirname "$NCCL_OUTPUT_DIR")}
NCCL_SUMMARY_FILE=${NCCL_SUMMARY_FILE:-$NCCL_RUN_DIR/summary.json}
NCCL_RUNTIME_EVIDENCE_FILE=${NCCL_RUNTIME_EVIDENCE_FILE:-$NCCL_OUTPUT_DIR/runtime-evidence.json}

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
    canonical_job_log_dir="${CVAL_CANONICAL_JOB_LOG_DIR:-$CVAL_JOB_LOG_DIR}"
    if [[ "$canonical_job_log_dir" != "$expected_job_log_dir" || \
            "${CVAL_CANONICAL_RESULT_JSON_FILE:-$canonical_job_log_dir/result.json}" != "$expected_job_log_dir/result.json" ]]; then
                echo "Runtime global evidence paths do not match the canonical v2 run" >&2
                exit 1
        fi
        if [[ -n "$result_storage_artifacts" && \
            "${CVAL_CANONICAL_STORAGE_OUTPUT_DIR:-$STORAGE_OUTPUT_DIR}" != "$result_storage_artifacts" ]]; then
                echo "STORAGE_OUTPUT_DIR does not match the validated v2 result" >&2
                exit 1
        fi
        expected_dltest_run_dir="$CVAL_VALIDATION_ROOT/validation_tests/dltest/runs/$GCRNODE/$CVAL_RUN_ID"
        if [[ "${CVAL_CANONICAL_DLTEST_RUN_DIR:-$DLTEST_RUN_DIR}" != "$expected_dltest_run_dir" ]]; then
                echo "DLTEST_RUN_DIR does not match the canonical v2 run" >&2
                exit 1
        fi
        if [[ -n "$result_nccl_summary" && \
            "${CVAL_CANONICAL_NCCL_SUMMARY_FILE:-${NCCL_SUMMARY_FILE:-}}" != "$result_nccl_summary" ]]; then
                echo "NCCL_SUMMARY_FILE does not match the validated v2 result" >&2
                exit 1
        fi
        if [[ -n "${CVAL_CONFIG_SNAPSHOT_B64:-}" && -n "$result_nccl_summary" ]]; then
            expected_runtime_evidence="$(dirname "$result_nccl_summary")/artifacts/runtime-evidence.json"
            if [[ "${CVAL_CANONICAL_NCCL_RUNTIME_EVIDENCE_FILE:-${NCCL_RUNTIME_EVIDENCE_FILE:-}}" != "$expected_runtime_evidence" ]]; then
                echo "NCCL_RUNTIME_EVIDENCE_FILE does not match the validated v2 result" >&2
                exit 1
            fi
            NCCL_SUMMARY_FILE="$result_nccl_summary"
            NCCL_RUNTIME_EVIDENCE_FILE="$expected_runtime_evidence"
        fi
    assert_snapshot_runtime
fi

# Validate result/config provenance and every current raw DB target before events.
if [[ "$STRUCTURED_RESULT_LOADED" == true ]]; then
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli \
        db-preflight-result \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest"
fi

# Bind the complete result digest before the first current DB write.
if [[ "$result_schema_version" == "cval.results" || "$result_schema_version" == "cval.results.v2" ]]; then
    bind_result_digest
fi

trap on_ingestion_exit EXIT
emit_cval_event "ingestion_started" "incomplete"

# Two-phase NCCL outbox: write the complete immutable pending batch before any
# authoritative raw SQLite mutation. No PostgreSQL credentials enter this process.
NCCL_OUTBOX_PENDING_FILE="$CVAL_NCCL_OUTBOX_ROOT/pending/$CVAL_RUN_ID.json"
NCCL_OUTBOX_PENDING_EMITTED=false
if is_enabled "$CVAL_NCCL_EVALUATION_ENABLED" && is_enabled "$RUN_NCCL"; then
    if [[ -f "$NCCL_RUNTIME_EVIDENCE_FILE" ]]; then
        echo "Emitting immutable NCCL pending outbox before raw SQLite writes."
        if ! PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli nccl-eval emit-outbox \
            --result-json "${CVAL_CANONICAL_RESULT_JSON_FILE:-$CVAL_RESULT_JSON_FILE}" \
            --result-digest "$result_digest" \
            --summary "$NCCL_SUMMARY_FILE" \
            --runtime-evidence "$NCCL_RUNTIME_EVIDENCE_FILE" \
            --outbox-root "$CVAL_NCCL_OUTBOX_ROOT" \
            --apply --confirm emit-outbox --output json; then
            echo "NCCL pending outbox emission failed; refusing raw SQLite writes." >&2
            emit_cval_event "nccl_outbox_pending" "fail" "no raw SQLite writes attempted" || true
            exit 1
        fi
        NCCL_OUTBOX_PENDING_EMITTED=true
        emit_cval_event "nccl_outbox_pending" "pass"
    elif [[ "$GCRRESULT2" == "pass" ]]; then
        echo "Passing NCCL result is missing required runtime evidence; refusing raw SQLite writes." >&2
        emit_cval_event "nccl_outbox_pending" "fail" "no raw SQLite writes attempted" || true
        exit 1
    else
        echo "Skipping NCCL PostgreSQL outbox because the failed test produced no runtime evidence."
        emit_cval_event "nccl_outbox_skipped" "incomplete" "failure occurred before runtime evidence collection"
    fi
fi

# Storage metrics are valid only when the storage phase itself passed.
if is_enabled "$RUN_STORAGE" && [ "$GCRRESULT1" = "pass" ]; then
    echo "Updating storage db with test results"
    storage_ingest_dir=${CVAL_CANONICAL_STORAGE_OUTPUT_DIR:-$STORAGE_OUTPUT_DIR}
    PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli db-add-storage-result \
        "$GCRNODE" \
        "$GCRTIME" \
        "$storage_ingest_dir" \
        --image-name "$CVAL_IMAGE_NAME" \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest" \
        --immutable \
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
    nccl_hca_args=()
    if is_enabled "$CVAL_IBBW_ENABLED"; then
        nccl_ibbw_ingest_log=${CVAL_CANONICAL_NCCL_IBBW_LOG_FILE:-$NCCL_IBBW_LOG_FILE}
        nccl_hca_args=(--ibbw-log "$nccl_ibbw_ingest_log" --require-hca-samples)
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
        --db-path "$CVAL_NCCL_DB_PATH"

    echo "NCCL IB_HEALTH DB update completed."
else
    echo "Skipping NCCL metrics DB update because result is $GCRRESULT2."
fi

# DL metric ingestion is owned by the validation Job. The evaluator may rebuild
# these DBs for reconciliation, but it is not required for current-run ingestion.
if is_enabled "$RUN_DLTEST" && [ "$GCRRESULT3" = "pass" ]; then
    dltest_ingest_dir=${CVAL_CANONICAL_DLTEST_RUN_DIR:-$DLTEST_RUN_DIR}
    if [[ ! -d "$dltest_ingest_dir" ]]; then
        echo "Passing DL result is missing required run directory: $dltest_ingest_dir" >&2
        exit 1
    fi
    if [[ ! -f "$CVAL_DL_METRIC_LOCK_HELPER" ]]; then
        echo "DL metric lock helper is missing: $CVAL_DL_METRIC_LOCK_HELPER" >&2
        exit 1
    fi
    mkdir -p "$(dirname "$CVAL_DL_METRIC_LOCK_FILE")"
    echo "Updating DL metric DBs from current run evidence"
    PYTHONPATH="$CVAL_REPO_DIR" python3 "$CVAL_DL_METRIC_LOCK_HELPER" \
        "$CVAL_DL_METRIC_LOCK_FILE" -- \
        python3 -m cval.cli \
        db-add-dltest-run "$GCRNODE" "$GCRTIME" "$dltest_ingest_dir" \
        --result-json "$CVAL_RESULT_JSON_FILE" \
        --result-digest "$result_digest" \
        --output json
    echo "DL metric DB update completed."
else
    echo "Skipping DL metrics DB update because result is $GCRRESULT3."
fi

# Commit fixed built-in status rows only after all required metric
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

# After all authoritative raw writes are durable, expose the pending batch by
# creating its immutable commit marker. Marker creation is byte-idempotent and
# may be retried with nccl-eval commit-outbox after a partial durable failure.
if [[ "$NCCL_OUTBOX_PENDING_EMITTED" == true ]]; then
    echo "Committing NCCL evaluation outbox after durable raw SQLite writes."
    if ! PYTHONPATH="$CVAL_REPO_DIR" python3 -m cval.cli nccl-eval commit-outbox \
        --outbox-root "$CVAL_NCCL_OUTBOX_ROOT" \
        --pending "$NCCL_OUTBOX_PENDING_FILE" \
        --result-digest "$result_digest" \
        --apply --confirm commit-outbox --output json; then
        echo "NCCL commit marker failed after raw SQLite writes; retry commit-outbox with the same pending file and result digest." >&2
        emit_cval_event "nccl_outbox_committed" "fail" "partial durable raw evidence; pending retained for retry" || true
        exit 1
    fi
    emit_cval_event "nccl_outbox_committed" "pass"
elif ! is_enabled "$CVAL_NCCL_EVALUATION_ENABLED" || ! is_enabled "$RUN_NCCL"; then
    echo "NCCL evaluation outbox disabled; no outbox directories or files created."
else
    echo "NCCL evaluation outbox skipped for a pre-evidence failure."
fi

emit_cval_event "ingestion_finished" "pass"
INGESTION_FINISHED=true
trap - EXIT