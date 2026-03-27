#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <node_id>"
  echo "Example: $0 slc01-cl02-hgx-0453"
  exit 1
fi

NODE="$1"
POD="${POD:-gcr-admin-pvc-access}"
NAMESPACE="${NAMESPACE:-gcr-admin}"
DB_PATH="${DB_PATH:-/data/continuous_validation/metadata/validation.db}"

echo "Fetching status for node ${NODE} remotely from pod ${POD} (ns: ${NAMESPACE})..."

kubectl -n "$NAMESPACE" exec -i "$POD" -- python3 -c "
import sqlite3, datetime, sys
db_path = '${DB_PATH}'
node_filter = sys.argv[1]
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    q = 'SELECT node, test, latest_timestamp, result FROM latest_status WHERE node = ? ORDER BY node, test'
    rows = conn.execute(q, (node_filter,)).fetchall()
    print('node\ttest\tlatest_timestamp\tresult')
    for r in rows:
        ts = datetime.datetime.fromtimestamp(r['latest_timestamp'], tz=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        print(f\"{r['node']}\t{r['test']}\t{ts}\t{r['result']}\")
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
" "$NODE"
