set -euo pipefail
if [[ -n "${CVAL_SOURCE_SHA256:-}" ]]; then
  python3 - <<'PY'
import hashlib
import io
import os
from pathlib import Path
import re
import tarfile

revision = os.environ["CVAL_GIT_REF"]
expected = os.environ["CVAL_SOURCE_SHA256"]
if not re.fullmatch(r"[0-9a-f]{40}", revision) or revision == "0" * 40 or not re.fullmatch(r"[0-9a-f]{64}", expected):
    raise ValueError("exact source revision and SHA-256 required")
payload = Path("/cval-source/source.tar.gz").read_bytes()
if hashlib.sha256(payload).hexdigest() != expected:
    raise ValueError("offline source SHA-256 mismatch")
with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
    if archive.pax_headers.get("comment") != revision:
        raise ValueError("offline archive commit mismatch")
    destination = Path(os.environ["CVAL_REPO_DIR"])
    destination.mkdir(parents=True, exist_ok=False)
    archive.extractall(destination, filter="data")
PY
else
  git init "$CVAL_REPO_DIR"
  git -C "$CVAL_REPO_DIR" remote add origin "$CVAL_GIT_REPO"
  git -C "$CVAL_REPO_DIR" fetch --depth=1 origin "$CVAL_GIT_REF"
  git -C "$CVAL_REPO_DIR" checkout --detach FETCH_HEAD
  test "$(git -C "$CVAL_REPO_DIR" rev-parse HEAD)" = "$CVAL_GIT_REF"
fi
cd "$CVAL_REPO_DIR"
printf '%s' "$CVAL_RUNTIME_ENV_B64" | base64 -d > /tmp/cval-runtime.env
source /tmp/cval-runtime.env
exec python3 -m cval.validation.supervisor
