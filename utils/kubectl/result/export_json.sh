#!/usr/bin/env bash
set -euo pipefail

POD="${POD:-gcr-admin-pvc-access}"
NAMESPACE="${NAMESPACE:-gcr-admin}"
DB_PATH="${DB_PATH:-/data/continuous_validation/metadata/validation.db}"
OUT="${1:-./status.json}"

echo "Exporting JSON remotely from pod ${POD} (ns: ${NAMESPACE}) to ${OUT}..."

kubectl -n "$NAMESPACE" exec -i "$POD" -- python3 -c "
import sqlite3, datetime, json, sys
db_path = '${DB_PATH}'
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute('SELECT node, test, latest_timestamp, result FROM latest_status ORDER BY node, test').fetchall()
    data = {
        'generated_at': datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z'),
        'latest': []
    }
    for r in rows:
        ts = datetime.datetime.fromtimestamp(r['latest_timestamp'], tz=datetime.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        data['latest'].append({'node': r['node'], 'test': r['test'], 'latest_timestamp': ts, 'result': r['result']})
    json.dump(data, sys.stdout, indent=2)
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
" > "$OUT"
