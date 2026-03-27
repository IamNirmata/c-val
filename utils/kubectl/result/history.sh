#!/usr/bin/env bash
set -euo pipefail

POD="${POD:-gcr-admin-pvc-access}"
NAMESPACE="${NAMESPACE:-gcr-admin}"
DB_PATH="${DB_PATH:-/data/continuous_validation/metadata/validation.db}"
TAIL="${1:-20}"

echo "Fetching history remotely from pod ${POD} (ns: ${NAMESPACE}) (tail=${TAIL})..."

kubectl -n "$NAMESPACE" exec -i "$POD" -- python3 -c "
import sqlite3, datetime, sys
db_path = '${DB_PATH}'
limit = int(sys.argv[1])
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute('SELECT node, test, timestamp, result FROM runs ORDER BY timestamp DESC LIMIT ?', (limit,)).fetchall()
    print('node\ttest\ttimestamp\tresult')
    for r in rows:
        ts = datetime.datetime.fromtimestamp(r['timestamp'], tz=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        print(f\"{r['node']}\t{r['test']}\t{ts}\t{r['result']}\")
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
" "$TAIL"
