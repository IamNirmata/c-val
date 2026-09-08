"""Unpack a verified immutable source bundle and install its offline dependency."""

import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile


revision = os.environ["CVAL_EVALUATOR_SHA"]
if not re.fullmatch(r"[0-9a-f]{40}", revision) or revision == "0" * 40:
    raise ValueError("exact evaluator revision is required")
source = Path("/source/source.tar.gz")
wheel = Path("/dependency") / os.environ["CVAL_DEPENDENCY_FILE"]
for path, variable in ((source, "CVAL_SOURCE_SHA256"), (wheel, "CVAL_DEPENDENCY_SHA256")):
    if hashlib.sha256(path.read_bytes()).hexdigest() != os.environ[variable]:
        raise ValueError(f"bundle digest mismatch: {path.name}")
with tarfile.open(source, "r:gz") as archive:
    archive.extractall("/app", filter="data")
subprocess.run([sys.executable, "-m", "pip", "--disable-pip-version-check", "install", "--no-index", "--no-deps", "--no-cache-dir", "--target", "/app/.deps", str(wheel)], check=True)
subprocess.run([sys.executable, "-m", "evaluation_engine", "--config", "/app/config/cval.toml", "--check-config"], cwd="/app", check=True)