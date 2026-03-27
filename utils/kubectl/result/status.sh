

#!/usr/bin/env bash
set -euo pipefail

POD="${POD:-gcr-admin-pvc-access}"
NAMESPACE="${NAMESPACE:-gcr-admin}"
DB_PATH="${DB_PATH:-/data/continuous_validation/metadata/validation.db}"

echo "Fetching status remotely from pod ${POD} (ns: ${NAMESPACE})..."

kubectl -n "$NAMESPACE" exec -i "$POD" -- python3 -c "
import sqlite3, datetime, sys
db_path = '${DB_PATH}'
try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute('SELECT node, test, latest_timestamp, result FROM latest_status ORDER BY node, test').fetchall()

    print('node\ttest\tlatest_timestamp_num\tlatest_timestamp\tresult')
    for r in rows:
        ts_num = int(r['latest_timestamp']) if r['latest_timestamp'] is not None else ''
        ts_iso = ''
        if r['latest_timestamp'] is not None:
            ts_iso = datetime.datetime.fromtimestamp(
                r['latest_timestamp'],
                tz=datetime.timezone.utc
            ).replace(microsecond=0).isoformat().replace('+00:00', 'Z')

        print(f\"{r['node']}\t{r['test']}\t{ts_num}\t{ts_iso}\t{r['result']}\")
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
"
