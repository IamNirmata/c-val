#!/usr/bin/env bash
set -euo pipefail

# Compatibility entrypoint used by the Volcano template. Execution, logging,
# timeouts, structured events, and result state are owned by the Python runner.
CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
export PYTHONPATH="$CVAL_REPO_DIR${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -m cval.validation.runner
