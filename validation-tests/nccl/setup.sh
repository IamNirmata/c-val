#!/usr/bin/env bash
set -euo pipefail

# Validate NCCL runtime dependencies without installing or mutating the image.

CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
CVAL_VALIDATION_TESTS_DIR=${CVAL_VALIDATION_TESTS_DIR:-$CVAL_REPO_DIR/validation-tests}

for command_name in python3 torchrun; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		echo "nccl setup: required command not found: $command_name" >&2
		exit 1
	fi
done

for required_file in \
	"$CVAL_VALIDATION_TESTS_DIR/nccl/single-node-allreduce.py" \
	"$CVAL_VALIDATION_TESTS_DIR/nccl/ibbw.sh"; do
	if [[ ! -f "$required_file" ]]; then
		echo "nccl setup: required file not found: $required_file" >&2
		exit 1
	fi
done

echo "nccl setup: dependencies are available"
