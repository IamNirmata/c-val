# Validate the supervisor-projected environment before raw ingestion.

for setting in CVAL_CONFIG_SNAPSHOT_B64 CVAL_VALIDATION_ROOT CVAL_REPO_DIR \
	CVAL_VALIDATION_TESTS_DIR CVAL_DL_UNIT_TEST_DIR CVAL_VALIDATION_DB_PATH \
	CVAL_STORAGE_DB_PATH CVAL_NCCL_DB_PATH CVAL_DL_NUMERICAL_DB_PATH \
	CVAL_DL_COMPUTE_DB_PATH CVAL_DL_COLLECTIVE_DB_PATH CVAL_DL_OVERLAP_DB_PATH \
	CVAL_RUN_ID GCRNODE GCRTIME RUN_STORAGE RUN_NCCL RUN_DLTEST \
	CVAL_JOB_LOG_DIR CVAL_RESULT_DIR CVAL_RESULT_JSON_FILE; do
	if [[ ! -v "$setting" || -z "${!setting}" ]]; then
		echo "Missing configured runtime value: $setting" >&2
		return 2
	fi
done

export CVAL_VALIDATION_ROOT CVAL_REPO_DIR CVAL_VALIDATION_TESTS_DIR CVAL_DL_UNIT_TEST_DIR
export CVAL_VALIDATION_DB_PATH
export CVAL_STORAGE_DB_PATH CVAL_NCCL_DB_PATH
export CVAL_DL_NUMERICAL_DB_PATH CVAL_DL_COMPUTE_DB_PATH
export CVAL_DL_COLLECTIVE_DB_PATH CVAL_DL_OVERLAP_DB_PATH
export CVAL_RUN_ID RUN_STORAGE RUN_NCCL RUN_DLTEST

# Detect framework versions from the running image so each result records the
# exact PyTorch/CUDA build that produced it, alongside image_name. Detection is
# best-effort: a missing or CPU-only torch leaves the value empty.
CVAL_PYTORCH_VERSION=${CVAL_PYTORCH_VERSION:-$(python3 -c 'import torch; print(torch.__version__)' 2>/dev/null || echo "")}
CVAL_CUDA_VERSION=${CVAL_CUDA_VERSION:-$(python3 -c 'import torch; print(torch.version.cuda or "")' 2>/dev/null || echo "")}
export CVAL_PYTORCH_VERSION CVAL_CUDA_VERSION


for test in STORAGE NCCL DLTEST; do
	enabled="RUN_$test"
	[[ "${!enabled}" == "true" ]] || continue
	for suffix in RUN_DIR OUTPUT_DIR LOG_FILE SUMMARY_FILE; do
		setting="${test}_$suffix"
		if [[ ! -v "$setting" || -z "${!setting}" ]]; then
			echo "Missing projected evidence path: $setting" >&2
			return 2
		fi
	done
done
if [[ "$RUN_NCCL" == "true" && -z "${NCCL_IBBW_LOG_FILE:-}" ]]; then
	echo "Missing projected evidence path: NCCL_IBBW_LOG_FILE" >&2
	return 2
fi

# Default every phase to fail; tests must opt into pass after successful completion.
export GCRRESULT1=fail
export GCRRESULT2=fail
export GCRRESULT3=fail