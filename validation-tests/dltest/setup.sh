#!/usr/bin/env bash
set -euo pipefail

# Validate DL unit-test dependencies without modifying the shared source tree.

CVAL_VALIDATION_ROOT=${CVAL_VALIDATION_ROOT:-/data/continuous_validation}
CVAL_DL_UNIT_TEST_DIR=${CVAL_DL_UNIT_TEST_DIR:-$CVAL_VALIDATION_ROOT/deep-learning-unit-test-main}
CVAL_DL_TEST_PLAN=${CVAL_DL_TEST_PLAN:-80gb-example}

for command_name in python3 torchrun realpath; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		echo "dltest setup: required command not found: $command_name" >&2
		exit 1
	fi
done

if [[ ! "$CVAL_DL_TEST_PLAN" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
	echo "dltest setup: unsafe test plan name: $CVAL_DL_TEST_PLAN" >&2
	exit 1
fi

if [[ ! -d "$CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test" ]]; then
	echo "dltest setup: package not found under $CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test" >&2
	exit 1
fi

for plan_dir in \
	"$CVAL_DL_UNIT_TEST_DIR/test_plans/$CVAL_DL_TEST_PLAN" \
	"$CVAL_DL_UNIT_TEST_DIR/src/dl_unit_test/test_plans/$CVAL_DL_TEST_PLAN"; do
	if [[ -f "$plan_dir/test_plan.json" ]]; then
		echo "dltest setup: dependencies and test plan are available"
		exit 0
	fi
done

echo "dltest setup: test plan '$CVAL_DL_TEST_PLAN' not found" >&2
exit 1
