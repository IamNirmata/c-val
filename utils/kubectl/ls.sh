#!/usr/bin/env bash
set -euo pipefail

POD="${POD:-gcr-admin-pvc-access}"
NAMESPACE="${NAMESPACE:-gcr-admin}"
TARGET_DIR="${1:-/data/continuous_validation}"

echo "Listing ${TARGET_DIR} remotely on pod ${POD} (ns: ${NAMESPACE})..."
kubectl -n "$NAMESPACE" exec "$POD" -- ls -F "$TARGET_DIR"