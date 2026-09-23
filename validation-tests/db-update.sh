#!/usr/bin/env bash
set -euo pipefail

CVAL_REPO_DIR=${CVAL_REPO_DIR:-/workspace/c-val}
export PYTHONPATH="$CVAL_REPO_DIR${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -m cval.storage.run_ingest