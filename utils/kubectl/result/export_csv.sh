#!/usr/bin/env bash
set -euo pipefail

POD="${POD:-gcr-admin-pvc-access}"
NAMESPACE="${NAMESPACE:-gcr-admin}"
DB_PATH="${DB_PATH:-/data/continuous_validation/metadata/validation.db}"
OUT="${1:-./status.csv}"

echo "Exporting CSV remotely from pod ${POD} (ns: ${NAMESPACE}) to ${OUT}..."

kubectl -n "$NAMESPACE" exec -i "$POD" -- python3 -c "
import sqlite3, datetime, csv, sys
db_path = '${DB_PATH}'
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute('SELECT node, test, latest_timestamp, result FROM latest_status ORDER BY node, test').fetchall()
    w = csv.writer(sys.stdout)
    w.writerow(['node', 'test', 'latest_timestamp', 'result'])
    for r in rows:
        ts = datetime.datetime.fromtimestamp(r['latest_timestamp'], tz=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        w.writerow([r['node'], r['test'], ts, r['result']])
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
" > "$OUT"
