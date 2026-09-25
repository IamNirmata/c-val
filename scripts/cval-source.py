"""Render one immutable source ConfigMap for exact-commit validation Jobs."""

import argparse
import base64
import hashlib
import io
from pathlib import Path
import re
import subprocess
import tarfile

import yaml


REPO = Path(__file__).resolve().parents[1]
SOURCE_PATHS = ("cval", "config", "validation-tests", "ymls", "scripts", "pyproject.toml")


def source_manifest(revision: str, bundle: bytes, namespace: str) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or revision == "0" * 40:
        raise ValueError("exact nonzero source revision required")
    if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", namespace):
        raise ValueError("namespace must be a DNS label")
    if len(bundle) > 900000:
        raise ValueError("bundle exceeds ConfigMap limit")
    with tarfile.open(fileobj=io.BytesIO(bundle), mode="r:gz") as archive:
        if archive.pax_headers.get("comment") != revision:
            raise ValueError("source archive commit mismatch")
    digest = hashlib.sha256(bundle).hexdigest()
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cval-source-" + revision[:16],
            "namespace": namespace,
            "annotations": {"cval/source-commit": revision, "cval/source-sha256": digest},
        },
        "immutable": True,
        "binaryData": {"source.tar.gz": base64.b64encode(bundle).decode()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision) or args.revision == "0" * 40:
        parser.error("--revision must be an exact nonzero commit")
    revision = subprocess.run(
        ["git", "rev-parse", "--verify", args.revision + "^{commit}"],
        cwd=REPO, check=True, capture_output=True, text=True, timeout=30,
    ).stdout.strip()
    if revision != args.revision:
        parser.error("--revision must resolve to the exact commit")
    bundle = subprocess.run(
        ["git", "archive", "--format=tar.gz", revision, *SOURCE_PATHS],
        cwd=REPO, check=True, capture_output=True, timeout=30,
    ).stdout
    manifest = source_manifest(revision, bundle, args.namespace)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / "source-configmap.yaml"
    with output.open("x", encoding="utf-8") as handle:
        yaml.safe_dump(manifest, handle, sort_keys=False)
    print(f"{output}: ConfigMap/{manifest['metadata']['name']}")
    print(f"source_sha256={manifest['metadata']['annotations']['cval/source-sha256']}")


if __name__ == "__main__":
    main()
