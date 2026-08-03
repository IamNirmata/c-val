"""Deterministic NCCL baseline profile identities."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID, uuid5

from cval.nccl_eval.models import TestRun, json_ready


PROFILE_NAMESPACE = UUID("f4c5299d-2d6a-55de-85ea-a737882ed746")
_SAFE_COMPONENT = re.compile(r"[^a-z0-9._+-]+")


@dataclass(frozen=True)
class BaselineProfileIdentity:
    """Exact profile payload used for concurrency-safe upserts."""

    profile_id: UUID
    profile_key: str
    test_name: str
    test_definition_version: str
    gpu_model: str
    gpus_per_node: int
    cuda_version: str
    pytorch_version: str
    compiled_nccl_version: str
    runtime_nccl_package_version: str
    driver_version_group: str
    topology_class: str
    source_commit: str
    image_digest: str
    implementation_identity: str
    test_config_fingerprint: str
    test_config: Mapping[str, Any]

    def to_dict(self) -> dict[str, object]:
        return {
            "profile_id": str(self.profile_id),
            "profile_key": self.profile_key,
            "test_name": self.test_name,
            "test_definition_version": self.test_definition_version,
            "gpu_model": self.gpu_model,
            "gpus_per_node": self.gpus_per_node,
            "cuda_version": self.cuda_version,
            "pytorch_version": self.pytorch_version,
            "compiled_nccl_version": self.compiled_nccl_version,
            "runtime_nccl_package_version": self.runtime_nccl_package_version,
            "driver_version_group": self.driver_version_group,
            "topology_class": self.topology_class,
            "source_commit": self.source_commit,
            "image_digest": self.image_digest,
            "implementation_identity": self.implementation_identity,
            "test_config_fingerprint": self.test_config_fingerprint,
            "test_config": json_ready(self.test_config),
        }


def canonical_test_config(test_config: Mapping[str, Any]) -> str:
    """Return stable canonical JSON for one already validated test config."""

    if not isinstance(test_config, Mapping):
        raise TypeError("test_config must be a mapping")
    return json.dumps(
        json_ready(test_config),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def test_config_fingerprint(test_config: Mapping[str, Any]) -> str:
    """Return the canonical JSON SHA-256 fingerprint."""

    digest = hashlib.sha256(canonical_test_config(test_config).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def build_profile_identity(run: TestRun) -> BaselineProfileIdentity:
    """Build a deterministic readable key and UUID from all material fields."""

    if not isinstance(run, TestRun):
        raise TypeError("run must be a TestRun")
    fingerprint = test_config_fingerprint(run.test_config)
    config = run.test_config
    material = {
        "test_name": run.test_name,
        "test_definition_version": run.test_definition_version,
        "gpu_model": run.gpu_model,
        "gpus_per_node": run.gpus_per_node,
        "cuda_version": run.cuda_version,
        "pytorch_version": run.pytorch_version,
        "compiled_nccl_version": run.compiled_nccl_version,
        "runtime_nccl_package_version": run.runtime_nccl_package_version,
        "driver_version_group": run.driver_version_group,
        "topology_class": run.topology_class,
        "source_commit": run.source_commit,
        "image_digest": run.image_digest,
        "implementation_identity": run.implementation_identity,
        "test_config_fingerprint": fingerprint,
        "test_config": json_ready(config),
    }
    canonical_material = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    identity_digest = hashlib.sha256(canonical_material.encode("utf-8")).hexdigest()
    components = (
        run.test_name,
        run.gpu_model,
        f"{run.gpus_per_node}gpu",
        f"cuda-{run.cuda_version}",
        f"pt-{run.pytorch_version}",
        f"nccl-compiled-{run.compiled_nccl_version}",
        f"nccl-package-{run.runtime_nccl_package_version}",
        run.test_definition_version,
        run.driver_version_group,
        run.topology_class,
        run.source_commit,
        run.image_digest,
        run.implementation_identity,
        str(config["collective"]),
        str(config["datatype"]),
        str(config["reduction"]),
        str(config["message_size"]),
        f"{run.iterations}iter",
        f"{'null' if run.samples is None else run.samples}samples",
        f"{config['warmup_iterations']}warmup",
        f"latency-{config['latency_unit']}",
    )
    readable = ":".join(_slug(item) for item in components)
    # Every material field is represented directly above or through the full
    # canonical digest.  The suffix makes truncation collision-resistant.
    profile_key = f"{readable[:210].rstrip(':')}:{identity_digest[:24]}"
    return BaselineProfileIdentity(
        profile_id=uuid5(PROFILE_NAMESPACE, canonical_material),
        profile_key=profile_key,
        test_name=run.test_name,
        test_definition_version=run.test_definition_version,
        gpu_model=run.gpu_model,
        gpus_per_node=run.gpus_per_node,
        cuda_version=run.cuda_version,
        pytorch_version=run.pytorch_version,
        compiled_nccl_version=run.compiled_nccl_version,
        runtime_nccl_package_version=run.runtime_nccl_package_version,
        driver_version_group=run.driver_version_group,
        topology_class=run.topology_class,
        source_commit=run.source_commit,
        image_digest=run.image_digest,
        implementation_identity=run.implementation_identity,
        test_config_fingerprint=fingerprint,
        test_config=run.test_config,
    )


def _slug(value: object) -> str:
    text = str(value).strip().lower()
    text = _SAFE_COMPONENT.sub("-", text).strip("-.")
    return (text or "unknown")[:32]
