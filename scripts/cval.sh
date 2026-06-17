#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

export PYTHONPATH="$REPO_DIR${PYTHONPATH:+:$PYTHONPATH}"
exec python -m cval.cli --config "$REPO_DIR/config/cval.toml" "$@"
