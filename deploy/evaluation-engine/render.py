"""Render offline, exact-revision ConfigMaps and the fixed-name evaluator Pod."""

import argparse
import base64
import hashlib
from pathlib import Path
import re
import subprocess

import yaml


REPO = Path(__file__).resolve().parents[2]


def manifests(revision, bundle, bootstrap, wheel_name, wheel, pod):
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or revision == "0" * 40:
        raise ValueError("exact nonzero source revision required")
    if len(bundle) + len(bootstrap.encode()) > 900000 or len(wheel) > 1000000:
        raise ValueError("bundle exceeds ConfigMap limit")
    source_hash = hashlib.sha256(bundle).hexdigest()
    wheel_hash = hashlib.sha256(wheel).hexdigest()
    source_name = "cval-eval-source-" + revision[:16]
    dependency_name = "cval-eval-deps-" + wheel_hash[:16]
    namespace = pod["metadata"]["namespace"]
    source = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": source_name, "namespace": namespace}, "immutable": True, "data": {"bootstrap.py": bootstrap}, "binaryData": {"source.tar.gz": base64.b64encode(bundle).decode()}}
    dependency = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": dependency_name, "namespace": namespace}, "immutable": True, "binaryData": {wheel_name: base64.b64encode(wheel).decode()}}
    environment = {"CVAL_EVALUATOR_SHA": revision, "CVAL_SOURCE_SHA256": source_hash, "CVAL_DEPENDENCY_SHA256": wheel_hash, "CVAL_DEPENDENCY_FILE": wheel_name, "PYTHONPATH": "/app:/app/.deps", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}
    for container in pod["spec"]["initContainers"] + pod["spec"]["containers"]:
        container["env"] = [{"name": name, "value": value} for name, value in environment.items()]
    for volume in pod["spec"]["volumes"]:
        if volume["name"] in {"source", "dependency"}:
            volume["configMap"]["name"] = source_name if volume["name"] == "source" else dependency_name
    pod["metadata"]["annotations"] = {"cval/source-commit": revision, "cval/source-sha256": source_hash, "cval/dependency-sha256": wheel_hash}
    return source, dependency, pod


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    revision = subprocess.run(["git", "rev-parse", "--verify", args.revision + "^{commit}"], cwd=REPO, check=True, capture_output=True, text=True).stdout.strip()
    if revision != args.revision:
        parser.error("--revision must be the exact commit")
    bundle = subprocess.run(["git", "archive", "--format=tar.gz", revision, "cval", "evaluation_engine", "config", "validation-tests", "ymls"], cwd=REPO, check=True, capture_output=True).stdout
    bootstrap = subprocess.run(["git", "show", revision + ":deploy/evaluation-engine/bootstrap.py"], cwd=REPO, check=True, capture_output=True, text=True).stdout
    template = subprocess.run(["git", "show", revision + ":deploy/evaluation-engine/pod.yaml"], cwd=REPO, check=True, capture_output=True, text=True).stdout
    documents = manifests(revision, bundle, bootstrap, args.wheel.name, args.wheel.read_bytes(), yaml.safe_load(template))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for filename, document in zip(("source-configmap.yaml", "dependency-configmap.yaml", "pod.yaml"), documents, strict=True):
        path = args.output_dir / filename
        with path.open("x", encoding="utf-8") as handle:
            yaml.safe_dump(document, handle, sort_keys=False)
        print(f"{path}: {document['kind']}/{document['metadata']['name']}")


if __name__ == "__main__":
    main()