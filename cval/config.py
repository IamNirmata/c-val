"""Configuration loading for c-val.

c-val uses TOML for repository configuration because Python 3.11 can parse it
with stdlib `tomllib`, while operators still get comments, typed values, and
clear nested sections.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

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

    timeout_seconds: float = 180
    poll_interval_seconds: float = 60


@dataclass(frozen=True)
class StorageConfig:
    """SQLite metadata paths on the shared validation PVC."""

    validation_db_path: str = "/data/continuous_validation/metadata/validation.db"
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
    dl_results_root_path: str = "/data/continuous_validation/dltest"


@dataclass(frozen=True)
class ValidationConfig:
    """Validation workload defaults used inside pods."""

    gpu_count: int = 8
    nccl_iterations: int = 20
    nccl_data_size_gb: int = 8
    ibbw_start_device: int = 0
    ibbw_end_device: int = 13
    dl_test_plan: str = "80gb-example"
    dl_baseline_test_id: str = "b200-pt2.8.0-cuda12.9"
    dl_iterations: int = 20


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
    classify_outliers: bool = True
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
class CvalConfig:
    """Complete c-val configuration tree."""

    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    job: JobConfig = field(default_factory=JobConfig)
    policy: PolicyConfig = field(default_factory=PolicyConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    job_template: JobTemplateConfig = field(default_factory=JobTemplateConfig)
    baseline: BaselineClassificationConfig = field(default_factory=BaselineClassificationConfig)


def default_config() -> CvalConfig:
    """Return built-in defaults used when no config file is available."""

    return CvalConfig()


def load_config(path: Path | str | None = None) -> CvalConfig:
    """Load c-val config from TOML, falling back to built-in defaults."""

    config_path = _config_path(path)
    if not config_path.exists():
        if path is not None or os.environ.get("CVAL_CONFIG"):
            raise FileNotFoundError(f"c-val config file not found: {config_path}")
        return default_config()

    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("c-val config must be a TOML table")
    return _build_config(data)


def config_to_dict(config: CvalConfig) -> dict[str, Any]:
    """Convert config dataclasses to JSON-serializable dictionaries."""

    data = asdict(config)
    data["job"]["template_path"] = str(config.job.template_path)
    return data


def _config_path(path: Path | str | None) -> Path:
    if path is not None:
        return Path(path).expanduser().resolve()
    env_path = os.environ.get("CVAL_CONFIG")
    if env_path:
        return Path(env_path).expanduser().resolve()
    return DEFAULT_CONFIG_PATH


def _build_config(data: dict[str, Any]) -> CvalConfig:
    defaults = default_config()
    cluster = _section(data, "cluster")
    scheduling = _section(data, "scheduling")
    job = _section(data, "job")
    policy = _section(data, "policy")
    monitoring = _section(data, "monitoring")
    storage = _section(data, "storage")
    runtime = _section(data, "runtime")
    validation = _section(data, "validation")
    job_template = _section(data, "job_template")
    baseline = _section(data, "baseline")

    return CvalConfig(
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
        validation=ValidationConfig(
            gpu_count=_int(validation, "gpu_count", defaults.validation.gpu_count),
            nccl_iterations=_int(
                validation,
                "nccl_iterations",
                defaults.validation.nccl_iterations,
            ),
            nccl_data_size_gb=_int(
                validation,
                "nccl_data_size_gb",
                defaults.validation.nccl_data_size_gb,
            ),
            ibbw_start_device=_int(
                validation,
                "ibbw_start_device",
                defaults.validation.ibbw_start_device,
            ),
            ibbw_end_device=_int(
                validation,
                "ibbw_end_device",
                defaults.validation.ibbw_end_device,
            ),
            dl_test_plan=_str(validation, "dl_test_plan", defaults.validation.dl_test_plan),
            dl_baseline_test_id=_str(
                validation,
                "dl_baseline_test_id",
                defaults.validation.dl_baseline_test_id,
            ),
            dl_iterations=_int(validation, "dl_iterations", defaults.validation.dl_iterations),
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
                baseline, "nccl_peer_tolerance_pct", defaults.baseline.nccl_peer_tolerance_pct
            ),
            storage_peer_tolerance_pct=_float(
                baseline, "storage_peer_tolerance_pct", defaults.baseline.storage_peer_tolerance_pct
            ),
            dl_compute_tolerance_pct=_float(
                baseline, "dl_compute_tolerance_pct", defaults.baseline.dl_compute_tolerance_pct
            ),
            dl_numerical_tolerance_pct=_float(
                baseline, "dl_numerical_tolerance_pct", defaults.baseline.dl_numerical_tolerance_pct
            ),
            dl_overlap_tolerance_pct=_float(
                baseline, "dl_overlap_tolerance_pct", defaults.baseline.dl_overlap_tolerance_pct
            ),
            classify_outliers=_bool(
                baseline, "classify_outliers", defaults.baseline.classify_outliers
            ),
            robust_z_threshold=_float(
                baseline, "robust_z_threshold", defaults.baseline.robust_z_threshold
            ),
            min_samples=_int(baseline, "min_samples", defaults.baseline.min_samples),
            window_days=_int(baseline, "window_days", defaults.baseline.window_days),
            dl_degraded_metric_fraction=_float(
                baseline,
                "dl_degraded_metric_fraction",
                defaults.baseline.dl_degraded_metric_fraction,
            ),
            dl_min_degraded_metrics=_int(
                baseline,
                "dl_min_degraded_metrics",
                defaults.baseline.dl_min_degraded_metrics,
            ),
            dl_degraded_severity_pct=_float(
                baseline,
                "dl_degraded_severity_pct",
                defaults.baseline.dl_degraded_severity_pct,
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
    )


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"Config section [{name}] must be a table")
    return value


def _str(section: dict[str, Any], key: str, default: str) -> str:
    return str(section.get(key, default))


def _int(section: dict[str, Any], key: str, default: int) -> int:
    return int(section.get(key, default))


def _float(section: dict[str, Any], key: str, default: float) -> float:
    return float(section.get(key, default))


def _bool(section: dict[str, Any], key: str, default: bool) -> bool:
    value = section.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


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