#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <node> <test> <result> [timestamp]"
  echo "Example: $0 slc01-cl02-hgx-0453 dl_test pass 1696543200"
  exit 1
fi

NODE="$1"
TEST="$2"
RESULT="$3"
TIMESTAMP="${4:-$(date +%s)}"
DB_PATH="${DB_PATH:-/data/continuous_validation/metadata/validation.db}"

echo "Adding result locally (in-pod)..."

python3 -c "
import sqlite3, os, sys, datetime
db_path = '${DB_PATH}'
node = sys.argv[1]
test = sys.argv[2]
res = sys.argv[3]
ts_str = sys.argv[4]

try:
    if ts_str.isdigit():
        ts = int(ts_str)
    else:
        t = ts_str
        if t.endswith('Z'): t = t[:-1] + '+00:00'
        d = datetime.datetime.fromisoformat(t)
        if d.tzinfo is None: d = d.replace(tzinfo=datetime.timezone.utc)
        ts = int(d.timestamp())

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    # Ensure tables exist just in case
    conn.execute(\"CREATE TABLE IF NOT EXISTS runs (node TEXT NOT NULL, test TEXT NOT NULL, timestamp INTEGER NOT NULL, result TEXT NOT NULL CHECK (result IN ('pass','fail','incomplete')));\")
    conn.execute(\"INSERT INTO runs(node, test, timestamp, result) VALUES (?,?,?,?)\", (node, test, ts, res))
    conn.commit()
    print(f'Added: {node} {test} {res} {ts}')
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
" "$NODE" "$TEST" "$RESULT" "$TIMESTAMP"
