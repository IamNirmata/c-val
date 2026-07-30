#!/usr/bin/env bash
set -euo pipefail

# Compatibility entrypoint retained for pinned jobs and operator commands.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
exec bash "$SCRIPT_DIR/run-test.sh" "$@"