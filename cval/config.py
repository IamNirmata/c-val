"""Configuration loading for c-val.

c-val uses TOML for repository configuration because Python 3.11 can parse it
with stdlib `tomllib`, while operators still get comments, typed values, and
clear nested sections.
"""

from __future__ import annotations

import base64
import json
import math
import os
import re
import stat
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

from cval.validation.registry import (
    ValidationTestRegistry,
    load_test_registry,
    parse_resource_quantity,
)
from cval.validation.operational_targets import build_operational_target_catalog

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "cval.toml"


@dataclass(frozen=True)
class ClusterConfig:
    """Cluster-facing defaults used by discovery, status, and submission."""

    namespace: str = "gcr-admin"
    pvc_access_pod: str = "gcr-admin-pvc-access"
    node_filter: str = "hgx"
    tolerated_no_schedule_taints: tuple[str, ...] = ("nvidia.com/gpu", "rdma")


@dataclass(frozen=True)
class SchedulingConfig:
    """Default prioritization and batching controls."""

    days_threshold: float = 7
    batch_size: int = 5


@dataclass(frozen=True)
class JobConfig:
    """Defaults for rendered validation jobs and runtime checkout."""

    template_path: Path = field(
        default_factory=lambda: REPO_ROOT / "ymls" / "specific-node-job.yml"
    )
    job_prefix: str = "cval"
    image_name: str = ""
    git_repo: str = "https://github.com/IamNirmata/c-val.git"
    git_ref: str = "main"


@dataclass(frozen=True)
class PolicyConfig:
    """Safety policy defaults for real Kubernetes job creation."""

    namespace_allowlist: tuple[str, ...] = ("gcr-admin",)
    max_batch_size: int = 5
    confirmation_phrase: str = "submit"


@dataclass(frozen=True)
class MonitoringConfig:
    """Polling defaults for read-only job monitoring."""

    timeout_seconds: float = 6000
    poll_interval_seconds: float = 60


@dataclass(frozen=True)
class StorageConfig:
    """SQLite metadata paths on the shared validation PVC."""

    validation_db_path: str = "/data/continuous_validation/metadata/validation.db"
    run_history_enabled: bool = False
    per_test_ingestion_enabled: bool = False
    run_history_db_path: str = (
        "/data/continuous_validation/metadata/node-run-history.db"
    )
    storage_db_path: str = "/data/continuous_validation/metadata/test-storage.db"
    nccl_db_path: str = "/data/continuous_validation/metadata/test-nccl.db"
    dl_numerical_db_path: str = (
        "/data/continuous_validation/metadata/dltest_numerical_correctness.db"
    )
    dl_compute_db_path: str = (
        "/data/continuous_validation/metadata/dltest_compute_performance.db"
    )
    dl_collective_db_path: str = (
        "/data/continuous_validation/metadata/dltest_collective_performance.db"
    )
    dl_overlap_db_path: str = (
        "/data/continuous_validation/metadata/dltest_overlap_performance.db"
    )


@dataclass(frozen=True)
class RuntimeConfig:
    """In-pod runtime paths used by validation scripts."""

    repo_dir: str = "/workspace/c-val"
    validation_root: str = "/data/continuous_validation"
    validation_tests_dir: str = "/workspace/c-val/validation-tests"
    dl_unit_test_dir: str = "/data/continuous_validation/deep-learning-unit-test-main"
    dl_results_root_path: str = (
        "/data/continuous_validation/validation_tests/dltest/runs"
    )


@dataclass(frozen=True)
class StorageTestConfig:
    """Storage/FIO phase controls."""

    enabled: bool = True
    install_fio: bool = True


@dataclass(frozen=True)
class NcclTestConfig:
    """NCCL all-reduce and HCA monitoring controls."""

    enabled: bool = True
    gpu_count: int = 8
    iterations: int = 20
    data_size_gb: int = 8
    ibbw_enabled: bool = True
    ibbw_start_device: int | None = None
    ibbw_end_device: int | None = None
    net: str = "IB"
    p2p_disable: bool = True
    shm_disable: bool = True
    debug: str = "INFO"


@dataclass(frozen=True)
class DlTestConfig:
    """Deep-learning unit-test phase controls."""

    enabled: bool = True
    gpu_count: int = 8
    test_plan: str = "80gb-example"
    iterations: int = 100


@dataclass(frozen=True)
class TestsConfig:
    """Dynamic registry plus compatibility views for current consumers."""

    storage: StorageTestConfig = field(default_factory=StorageTestConfig)
    nccl: NcclTestConfig = field(default_factory=NcclTestConfig)
    dltest: DlTestConfig = field(default_factory=DlTestConfig)
    registry: ValidationTestRegistry = field(default_factory=ValidationTestRegistry)


@dataclass(frozen=True)
class JobTemplateConfig:
    """Values injected into the Volcano job template."""

    namespace: str = "gcr-admin"
    queue: str = "gcr-admin"
    app_label: str = "hari-gcr-bonete-test"
    pvc_claim: str = "pvc-vast-gcr-admin"
    container_image: str = "nvcr.io/nvidia/pytorch:25.11-py3"
    shared_memory_size: str = "256Gi"
    gpu_resource_name: str = "nvidia.com/gpu"
    gpu_count: str = "8"
    cpu: str = "100"
    memory: str = "1500Gi"
    rdma_resource_name: str = "rdma/rdma_shared_device_a"
    rdma_count: str = "1"
    rdma_toleration_key: str = "rdma"
    gpu_toleration_key: str = "nvidia.com/gpu"


@dataclass(frozen=True)
class BaselineClassificationConfig:
    """Baseline and peer-comparison tolerance rules."""

    baseline_root_path: str = "/data/continuous_validation/baselines"
    nccl_peer_tolerance_pct: float = 5.0
    storage_peer_tolerance_pct: float = 10.0
    dl_compute_tolerance_pct: float = 3.0
    dl_numerical_tolerance_pct: float = 0.1
    dl_overlap_tolerance_pct: float = 20.0
    # Robust z-score cutoff (Iglewicz & Hoaglin recommend 3.5) used when
    # building dynamic baselines and flagging outliers.
    robust_z_threshold: float = 3.5
    # Minimum clean samples before a metric baseline is trustworthy.
    min_samples: int = 8
    # Rolling window (days) of recent runs used to build a baseline.
    window_days: int = 30
    # DL aggregation: a DL component/node is degraded only when enough severe
    # metric deviations accumulate, avoiding any-one-metric fleet noise.
    dl_degraded_metric_fraction: float = 0.02
    dl_min_degraded_metrics: int = 10
    dl_degraded_severity_pct: float = 10.0
    # How often the background baseline builder should build candidates.
    build_interval_seconds: int = 86400
    # How often the background classifier should evaluate nodes.
    classify_interval_seconds: int = 300


@dataclass(frozen=True)
class HealthEvaluatorConfig:
    """Independent safety and bounded-work controls for the U9 evaluator."""

    write_enabled: bool = False
    lock_timeout_seconds: int = 30
    max_classifications_per_test: int = 250
    validation_root_mode: str = "0700"


@dataclass(frozen=True)
class CvalConfig:
    """Complete c-val configuration tree."""

    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    job: JobConfig = field(default_factory=JobConfig)
    policy: PolicyConfig = field(default_factory=PolicyConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    tests: TestsConfig = field(default_factory=TestsConfig)
    job_template: JobTemplateConfig = field(default_factory=JobTemplateConfig)
    baseline: BaselineClassificationConfig = field(default_factory=BaselineClassificationConfig)
    health_evaluator: HealthEvaluatorConfig = field(default_factory=HealthEvaluatorConfig)


def default_config() -> CvalConfig:
    """Return built-in defaults used when no config file is available."""

    return CvalConfig()


def load_config(path: Path | str | None = None) -> CvalConfig:
    """Load c-val config from TOML, falling back to built-in defaults."""

    config_path = _config_path(path)
    if not config_path.exists():
        if path is not None or os.environ.get("CVAL_CONFIG"):
            raise FileNotFoundError(f"c-val config file not found: {config_path}")
        return _build_config({})

    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("c-val config must be a TOML table")
    return _build_config(data)


def config_to_dict(config: CvalConfig) -> dict[str, Any]:
    """Convert config dataclasses to JSON-serializable dictionaries."""

    return {
        "cluster": asdict(config.cluster),
        "scheduling": asdict(config.scheduling),
        "job": asdict(config.job) | {"template_path": str(config.job.template_path)},
        "policy": asdict(config.policy),
        "monitoring": asdict(config.monitoring),
        "storage": asdict(config.storage),
        "runtime": asdict(config.runtime),
        "tests": config.tests.registry.to_dict(),
        "job_template": asdict(config.job_template),
        "baseline": asdict(config.baseline),
        "health_evaluator": asdict(config.health_evaluator),
    }


def encode_config_snapshot(config: CvalConfig) -> str:
    """Encode the exact effective runtime inputs as a deterministic JSON snapshot."""

    template_path = config.job.template_path.expanduser().resolve()
    try:
        template_value = template_path.relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        template_value = str(template_path)
    data = {
        "cluster": asdict(config.cluster),
        "scheduling": asdict(config.scheduling),
        "job": asdict(config.job) | {"template_path": template_value},
        "policy": asdict(config.policy),
        "monitoring": asdict(config.monitoring),
        "storage": asdict(config.storage),
        "runtime": asdict(config.runtime),
        "tests": {
            test.id: {
                "enabled": test.enabled,
                "config_path": test.config_path,
            }
            for test in config.tests.registry.tests
        },
        "test_definitions": config.tests.registry.to_dict(),
        "job_template": asdict(config.job_template),
        "baseline": asdict(config.baseline),
        "health_evaluator": asdict(config.health_evaluator),
    }
    payload = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def load_config_snapshot(
    payload: str,
    *,
    repo_root: Path | None = None,
) -> CvalConfig:
    """Decode and validate one renderer-generated effective configuration snapshot."""

    try:
        raw = base64.b64decode(payload, validate=True)
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid c-val effective configuration snapshot") from exc
    if not isinstance(data, dict):
        raise ValueError("c-val effective configuration snapshot must be an object")
    allowed = {
        "cluster",
        "scheduling",
        "job",
        "policy",
        "monitoring",
        "storage",
        "runtime",
        "tests",
        "test_definitions",
        "job_template",
        "baseline",
        "health_evaluator",
    }
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ValueError(
            "Unknown effective configuration snapshot section(s): "
            f"{', '.join(unknown)}"
        )
    expected_definitions = data.pop("test_definitions", None)
    if not isinstance(expected_definitions, dict):
        raise ValueError(
            "c-val effective configuration snapshot lacks complete test_definitions"
        )
    config = _build_config(
        data,
        repo_root=repo_root,
        include_test_defaults=False,
    )
    actual_definitions_json = json.dumps(
        config.tests.registry.to_dict(), sort_keys=True, separators=(",", ":")
    )
    expected_definitions_json = json.dumps(
        expected_definitions, sort_keys=True, separators=(",", ":")
    )
    if actual_definitions_json != expected_definitions_json:
        raise ValueError(
            "Effective test descriptors do not match the immutable configuration snapshot"
        )
    return config


def _config_path(path: Path | str | None) -> Path:
    if path is not None:
        return Path(path).expanduser().resolve()
    env_path = os.environ.get("CVAL_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()
    return DEFAULT_CONFIG_PATH


def _build_config(
    data: dict[str, Any],
    *,
    repo_root: Path | None = None,
    include_test_defaults: bool = True,
) -> CvalConfig:
    defaults = default_config()
    cluster = _section(data, "cluster")
    scheduling = _section(data, "scheduling")
    job = _section(data, "job")
    policy = _section(data, "policy")
    monitoring = _section(data, "monitoring")
    storage = _section(data, "storage")
    runtime = _section(data, "runtime")
    tests = _section(data, "tests")
    test_registry = load_test_registry(
        tests,
        repo_root=repo_root or REPO_ROOT,
        include_defaults=include_test_defaults,
    )
    storage_registration = test_registry.get("storage")
    nccl_registration = test_registry.get("nccl")
    dltest_registration = test_registry.get("dltest")
    storage_test = (
        storage_registration.definition.settings if storage_registration else {}
    )
    nccl_test = nccl_registration.definition.settings if nccl_registration else {}
    dltest = dltest_registration.definition.settings if dltest_registration else {}
    dl_health_aggregation = _section(dict(dltest), "health_aggregation")
    job_template = _section(data, "job_template")
    baseline = _section(data, "baseline")
    health_evaluator = _section(data, "health_evaluator")
    _reject_section_keys(
        health_evaluator,
        {
            "write_enabled",
            "lock_timeout_seconds",
            "max_classifications_per_test",
            "validation_root_mode",
        },
        "health_evaluator",
    )

    config = CvalConfig(
        cluster=ClusterConfig(
            namespace=_str(cluster, "namespace", defaults.cluster.namespace),
            pvc_access_pod=_str(cluster, "pvc_access_pod", defaults.cluster.pvc_access_pod),
            node_filter=_str(cluster, "node_filter", defaults.cluster.node_filter),
            tolerated_no_schedule_taints=_str_tuple(
                cluster,
                "tolerated_no_schedule_taints",
                defaults.cluster.tolerated_no_schedule_taints,
            ),
        ),
        scheduling=SchedulingConfig(
            days_threshold=_float(scheduling, "days_threshold", defaults.scheduling.days_threshold),
            batch_size=_int(scheduling, "batch_size", defaults.scheduling.batch_size),
        ),
        job=JobConfig(
            template_path=_repo_path(job, "template_path", defaults.job.template_path),
            job_prefix=_str(job, "job_prefix", defaults.job.job_prefix),
            git_repo=_str(job, "git_repo", defaults.job.git_repo),
            git_ref=_str(job, "git_ref", defaults.job.git_ref),
        ),
        policy=PolicyConfig(
            namespace_allowlist=_str_tuple(
                policy,
                "namespace_allowlist",
                defaults.policy.namespace_allowlist,
            ),
            max_batch_size=_int(policy, "max_batch_size", defaults.policy.max_batch_size),
            confirmation_phrase=_str(
                policy,
                "confirmation_phrase",
                defaults.policy.confirmation_phrase,
            ),
        ),
        monitoring=MonitoringConfig(
            timeout_seconds=_float(
                monitoring,
                "timeout_seconds",
                defaults.monitoring.timeout_seconds,
            ),
            poll_interval_seconds=_float(
                monitoring,
                "poll_interval_seconds",
                defaults.monitoring.poll_interval_seconds,
            ),
        ),
        storage=StorageConfig(
            validation_db_path=_str(storage, "validation_db_path", defaults.storage.validation_db_path),
            run_history_enabled=_strict_config_bool(
                storage,
                "run_history_enabled",
                defaults.storage.run_history_enabled,
            ),
            per_test_ingestion_enabled=_strict_config_bool(
                storage,
                "per_test_ingestion_enabled",
                defaults.storage.per_test_ingestion_enabled,
            ),
            run_history_db_path=_str(
                storage,
                "run_history_db_path",
                defaults.storage.run_history_db_path,
            ),
            storage_db_path=_str(storage, "storage_db_path", defaults.storage.storage_db_path),
            nccl_db_path=_str(storage, "nccl_db_path", defaults.storage.nccl_db_path),
            dl_numerical_db_path=_str(
                storage, "dl_numerical_db_path", defaults.storage.dl_numerical_db_path
            ),
            dl_compute_db_path=_str(
                storage, "dl_compute_db_path", defaults.storage.dl_compute_db_path
            ),
            dl_collective_db_path=_str(
                storage, "dl_collective_db_path", defaults.storage.dl_collective_db_path
            ),
            dl_overlap_db_path=_str(
                storage, "dl_overlap_db_path", defaults.storage.dl_overlap_db_path
            ),
        ),
        runtime=RuntimeConfig(
            repo_dir=_str(runtime, "repo_dir", defaults.runtime.repo_dir),
            validation_root=_str(runtime, "validation_root", defaults.runtime.validation_root),
            validation_tests_dir=_str(
                runtime,
                "validation_tests_dir",
                defaults.runtime.validation_tests_dir,
            ),
            dl_unit_test_dir=_str(
                runtime,
                "dl_unit_test_dir",
                defaults.runtime.dl_unit_test_dir,
            ),
            dl_results_root_path=_str(
                runtime,
                "dl_results_root_path",
                defaults.runtime.dl_results_root_path,
            ),
        ),
        tests=TestsConfig(
            storage=StorageTestConfig(
                enabled=bool(storage_registration and storage_registration.enabled),
                install_fio=_bool(
                    storage_test, "install_fio", defaults.tests.storage.install_fio
                ),
            ),
            nccl=NcclTestConfig(
                enabled=bool(nccl_registration and nccl_registration.enabled),
                gpu_count=_int(nccl_test, "gpu_count", defaults.tests.nccl.gpu_count),
                iterations=_int(nccl_test, "iterations", defaults.tests.nccl.iterations),
                data_size_gb=_int(
                    nccl_test, "data_size_gb", defaults.tests.nccl.data_size_gb
                ),
                ibbw_enabled=_bool(
                    nccl_test, "ibbw_enabled", defaults.tests.nccl.ibbw_enabled
                ),
                ibbw_start_device=_optional_int_value(
                    nccl_test,
                    "ibbw_start_device",
                    defaults.tests.nccl.ibbw_start_device,
                ),
                ibbw_end_device=_optional_int_value(
                    nccl_test,
                    "ibbw_end_device",
                    defaults.tests.nccl.ibbw_end_device,
                ),
                net=_str(nccl_test, "net", defaults.tests.nccl.net),
                p2p_disable=_bool(
                    nccl_test, "p2p_disable", defaults.tests.nccl.p2p_disable
                ),
                shm_disable=_bool(
                    nccl_test, "shm_disable", defaults.tests.nccl.shm_disable
                ),
                debug=_str(nccl_test, "debug", defaults.tests.nccl.debug),
            ),
            dltest=DlTestConfig(
                enabled=bool(dltest_registration and dltest_registration.enabled),
                gpu_count=_int(dltest, "gpu_count", defaults.tests.dltest.gpu_count),
                test_plan=_str(dltest, "test_plan", defaults.tests.dltest.test_plan),
                iterations=_int(dltest, "iterations", defaults.tests.dltest.iterations),
            ),
            registry=test_registry,
        ),
        job_template=JobTemplateConfig(
            namespace=_str(job_template, "namespace", defaults.job_template.namespace),
            queue=_str(job_template, "queue", defaults.job_template.queue),
            app_label=_str(job_template, "app_label", defaults.job_template.app_label),
            pvc_claim=_str(job_template, "pvc_claim", defaults.job_template.pvc_claim),
            container_image=_str(
                job_template,
                "container_image",
                defaults.job_template.container_image,
            ),
            shared_memory_size=_str(
                job_template,
                "shared_memory_size",
                defaults.job_template.shared_memory_size,
            ),
            gpu_resource_name=_str(
                job_template,
                "gpu_resource_name",
                defaults.job_template.gpu_resource_name,
            ),
            gpu_count=_str(job_template, "gpu_count", defaults.job_template.gpu_count),
            cpu=_str(job_template, "cpu", defaults.job_template.cpu),
            memory=_str(job_template, "memory", defaults.job_template.memory),
            rdma_resource_name=_str(
                job_template,
                "rdma_resource_name",
                defaults.job_template.rdma_resource_name,
            ),
            rdma_count=_str(job_template, "rdma_count", defaults.job_template.rdma_count),
            rdma_toleration_key=_str(
                job_template,
                "rdma_toleration_key",
                defaults.job_template.rdma_toleration_key,
            ),
            gpu_toleration_key=_str(
                job_template,
                "gpu_toleration_key",
                defaults.job_template.gpu_toleration_key,
            ),
        ),
        baseline=BaselineClassificationConfig(
            baseline_root_path=_str(
                baseline, "baseline_root_path", defaults.baseline.baseline_root_path
            ),
            nccl_peer_tolerance_pct=_float(
                baseline,
                "nccl_peer_tolerance_pct",
                _health_metric_tolerance(
                    test_registry,
                    "nccl",
                    "busbw",
                    defaults.baseline.nccl_peer_tolerance_pct,
                ),
            ),
            storage_peer_tolerance_pct=_float(
                baseline,
                "storage_peer_tolerance_pct",
                _health_metric_tolerance(
                    test_registry,
                    "storage",
                    "storage_performance",
                    defaults.baseline.storage_peer_tolerance_pct,
                ),
            ),
            dl_compute_tolerance_pct=_float(
                baseline,
                "dl_compute_tolerance_pct",
                _health_metric_tolerance(
                    test_registry,
                    "dltest",
                    "compute_performance",
                    defaults.baseline.dl_compute_tolerance_pct,
                ),
            ),
            dl_numerical_tolerance_pct=_float(
                baseline,
                "dl_numerical_tolerance_pct",
                _health_metric_tolerance(
                    test_registry,
                    "dltest",
                    "numerical_correctness",
                    defaults.baseline.dl_numerical_tolerance_pct,
                ),
            ),
            dl_overlap_tolerance_pct=_float(
                baseline,
                "dl_overlap_tolerance_pct",
                _health_metric_tolerance(
                    test_registry,
                    "dltest",
                    "overlap_performance",
                    defaults.baseline.dl_overlap_tolerance_pct,
                ),
            ),
            robust_z_threshold=_float(
                baseline, "robust_z_threshold", defaults.baseline.robust_z_threshold
            ),
            min_samples=_int(baseline, "min_samples", defaults.baseline.min_samples),
            window_days=_int(baseline, "window_days", defaults.baseline.window_days),
            dl_degraded_metric_fraction=_float(
                baseline,
                "dl_degraded_metric_fraction",
                _float(
                    dl_health_aggregation,
                    "degraded_metric_fraction",
                    defaults.baseline.dl_degraded_metric_fraction,
                ),
            ),
            dl_min_degraded_metrics=_int(
                baseline,
                "dl_min_degraded_metrics",
                _int(
                    dl_health_aggregation,
                    "min_degraded_metrics",
                    defaults.baseline.dl_min_degraded_metrics,
                ),
            ),
            dl_degraded_severity_pct=_float(
                baseline,
                "dl_degraded_severity_pct",
                _float(
                    dl_health_aggregation,
                    "degraded_severity_pct",
                    defaults.baseline.dl_degraded_severity_pct,
                ),
            ),
            build_interval_seconds=_int(
                baseline,
                "build_interval_seconds",
                defaults.baseline.build_interval_seconds,
            ),
            classify_interval_seconds=_int(
                baseline,
                "classify_interval_seconds",
                defaults.baseline.classify_interval_seconds,
            ),
        ),
        health_evaluator=HealthEvaluatorConfig(
            write_enabled=_strict_config_bool(
                health_evaluator,
                "write_enabled",
                defaults.health_evaluator.write_enabled,
            ),
            lock_timeout_seconds=_int(
                health_evaluator,
                "lock_timeout_seconds",
                defaults.health_evaluator.lock_timeout_seconds,
            ),
            max_classifications_per_test=_int(
                health_evaluator,
                "max_classifications_per_test",
                defaults.health_evaluator.max_classifications_per_test,
            ),
            validation_root_mode=_str(
                health_evaluator,
                "validation_root_mode",
                defaults.health_evaluator.validation_root_mode,
            ),
        ),
    )
    _validate_config(config)
    return config


def _validate_config(config: CvalConfig) -> None:
    """Reject invalid test settings before rendering or submitting jobs."""

    tests = config.tests
    if not tests.registry.enabled:
        raise ValueError("At least one test must be enabled under [tests.*]")
    # Reject reserved/colliding operator-facing names during config loading,
    # before argparse or a background loop can observe an ambiguous catalog.
    build_operational_target_catalog(tests.registry)
    if tests.nccl.gpu_count <= 0 or tests.dltest.gpu_count <= 0:
        raise ValueError("NCCL and DL GPU counts must be positive")
    if tests.nccl.iterations <= 0 or tests.dltest.iterations <= 0:
        raise ValueError("NCCL and DL iterations must be positive")
    if tests.nccl.data_size_gb <= 0:
        raise ValueError("NCCL data_size_gb must be positive")
    start_device = tests.nccl.ibbw_start_device
    end_device = tests.nccl.ibbw_end_device
    if (start_device is None) != (end_device is None):
        raise ValueError("NCCL IBBW device override requires both start and end")
    if start_device is not None and end_device is not None:
        if start_device < 0:
            raise ValueError("NCCL ibbw_start_device must be non-negative")
        if end_device < start_device:
            raise ValueError("NCCL ibbw_end_device must be >= ibbw_start_device")
    if not tests.nccl.net.strip() or not tests.nccl.debug.strip():
        raise ValueError("NCCL net and debug values must not be empty")
    if not tests.dltest.test_plan.strip():
        raise ValueError("DL test_plan must not be empty")
    baseline = config.baseline
    non_negative_values = {
        "nccl_peer_tolerance_pct": baseline.nccl_peer_tolerance_pct,
        "storage_peer_tolerance_pct": baseline.storage_peer_tolerance_pct,
        "dl_compute_tolerance_pct": baseline.dl_compute_tolerance_pct,
        "dl_numerical_tolerance_pct": baseline.dl_numerical_tolerance_pct,
        "dl_overlap_tolerance_pct": baseline.dl_overlap_tolerance_pct,
        "dl_degraded_severity_pct": baseline.dl_degraded_severity_pct,
    }
    for name, value in non_negative_values.items():
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"baseline.{name} must be finite and non-negative")
    if not math.isfinite(baseline.robust_z_threshold) or baseline.robust_z_threshold <= 0:
        raise ValueError("baseline.robust_z_threshold must be finite and positive")
    if (
        not math.isfinite(baseline.dl_degraded_metric_fraction)
        or not 0 <= baseline.dl_degraded_metric_fraction <= 1
    ):
        raise ValueError("baseline.dl_degraded_metric_fraction must be in [0,1]")
    for name, value in {
        "min_samples": baseline.min_samples,
        "window_days": baseline.window_days,
        "dl_min_degraded_metrics": baseline.dl_min_degraded_metrics,
        "build_interval_seconds": baseline.build_interval_seconds,
        "classify_interval_seconds": baseline.classify_interval_seconds,
    }.items():
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"baseline.{name} must be a positive integer")
    for name, value in {
        "lock_timeout_seconds": config.health_evaluator.lock_timeout_seconds,
        "max_classifications_per_test": config.health_evaluator.max_classifications_per_test,
    }.items():
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"health_evaluator.{name} must be a positive integer")
    root_mode = config.health_evaluator.validation_root_mode
    if not re.fullmatch(r"0[0-7]{3}", root_mode):
        raise ValueError(
            "health_evaluator.validation_root_mode must be an exact four-digit octal mode"
        )
    parsed_root_mode = int(root_mode, 8)
    if parsed_root_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ValueError(
            "health_evaluator.validation_root_mode must not be group/world writable"
        )
    if parsed_root_mode & (stat.S_IRUSR | stat.S_IXUSR) != (
        stat.S_IRUSR | stat.S_IXUSR
    ):
        raise ValueError(
            "health_evaluator.validation_root_mode must grant owner read/search"
        )
    try:
        reserved_gpus = int(config.job_template.gpu_count)
        reserved_rdma = int(config.job_template.rdma_count)
    except ValueError as exc:
        raise ValueError("job_template GPU/RDMA counts must be integers") from exc
    reserved_quantities = {
        "cpu": parse_resource_quantity(config.job_template.cpu),
        "memory": parse_resource_quantity(config.job_template.memory),
        "shared_memory": parse_resource_quantity(config.job_template.shared_memory_size),
    }
    for registered_test in tests.registry.enabled:
        requirements = registered_test.definition.requirements
        if reserved_gpus < requirements.gpu_count:
            raise ValueError(
                f"job_template.gpu_count does not cover enabled test "
                f"{registered_test.id!r} requirement ({requirements.gpu_count})"
            )
        if reserved_rdma < requirements.rdma_count:
            raise ValueError(
                f"job_template.rdma_count does not cover enabled test "
                f"{registered_test.id!r} requirement ({requirements.rdma_count})"
            )
        required_quantities = {
            "cpu": parse_resource_quantity(requirements.cpu),
            "memory": parse_resource_quantity(requirements.memory),
            "shared_memory": parse_resource_quantity(requirements.shared_memory),
        }
        for resource_name, required in required_quantities.items():
            if reserved_quantities[resource_name] < required:
                raise ValueError(
                    f"job_template {resource_name} does not cover enabled test "
                    f"{registered_test.id!r} requirement"
                )
        if registered_test.definition.metadata.timeout_seconds > config.monitoring.timeout_seconds:
            raise ValueError(
                f"Enabled test {registered_test.id!r} timeout exceeds "
                "monitoring.timeout_seconds"
            )
    sequential_timeout = sum(
        test.definition.metadata.timeout_seconds for test in tests.registry.enabled
    )
    if config.monitoring.timeout_seconds < sequential_timeout + 300:
        raise ValueError(
            "monitoring.timeout_seconds must cover enabled sequential test "
            "timeouts plus 300 seconds of ingestion grace"
        )


def _section(data: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name, {})
    if not isinstance(value, Mapping):
        raise ValueError(f"Config section [{name}] must be a table")
    return dict(value)


def _reject_section_keys(
    section: Mapping[str, Any],
    allowed: set[str],
    name: str,
) -> None:
    unknown = sorted(set(section) - allowed)
    if unknown:
        raise ValueError(
            f"Unknown value(s) under [{name}]: {', '.join(unknown)}"
        )


def _str(section: dict[str, Any], key: str, default: str) -> str:
    return str(section.get(key, default))


def _int(section: dict[str, Any], key: str, default: int) -> int:
    value = section.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _optional_int_value(
    section: Mapping[str, Any],
    key: str,
    default: int | None,
) -> int | None:
    value = section.get(key, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{key} must be an integer")
    return value


def _float(section: dict[str, Any], key: str, default: float) -> float:
    value = section.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{key} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{key} must be finite")
    return result


def _bool(section: dict[str, Any], key: str, default: bool) -> bool:
    value = section.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _strict_config_bool(
    section: dict[str, Any],
    key: str,
    default: bool,
) -> bool:
    value = section.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"Config value {key!r} must be a TOML boolean")
    return value


def _str_tuple(section: dict[str, Any], key: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = section.get(key, default)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list | tuple):
        return tuple(str(item) for item in value)
    raise ValueError(f"Config value {key!r} must be a string or list of strings")


def _repo_path(section: dict[str, Any], key: str, default: Path) -> Path:
    value = section.get(key)
    if value is None:
        return default
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else REPO_ROOT / path


def _health_metric_tolerance(
    registry: ValidationTestRegistry,
    test_id: str,
    source: str,
    default: float,
) -> float:
    """Read one compatibility tolerance from a test-owned health descriptor."""

    registered = registry.get(test_id)
    if registered is None:
        return default
    health = registered.definition.health
    if health is None:
        return default
    for metric in health.metrics:
        if metric.source == source:
            return metric.tolerance_pct
    return default