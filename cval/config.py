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
import tomllib
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Mapping, get_type_hints

from cval.storage.retry import RetryPolicy

from cval.validation.registry import (
    ValidationTestRegistry,
    load_test_registry,
    parse_resource_quantity,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "cval.toml"
_EXACT_COMMIT = re.compile(r"^[0-9a-f]{40}$")


def is_exact_commit(value: str) -> bool:
    """Return whether value is an immutable lowercase Git commit ID."""

    return bool(_EXACT_COMMIT.fullmatch(value)) and value != "0" * 40


@dataclass(frozen=True)
class ClusterConfig:
    """Explicit cluster settings used by discovery, status, and submission."""

    namespace: str
    pvc_access_pod: str
    node_filter: str
    tolerated_no_schedule_taints: tuple[str, ...]
    kubectl_timeout_seconds: float


@dataclass(frozen=True)
class SchedulingConfig:
    """Configured prioritization and batching controls."""

    days_threshold: float
    batch_size: int
    node_cooldown_seconds: int
    coverage_max_runs_per_node: int


@dataclass(frozen=True)
class JobConfig:
    """Configured validation Job and exact source identity.

    Empty legacy bundle fields remain in snapshots for historical decoding;
    new Jobs obtain source from the shared PVC Git checkout.
    """

    template_path: Path
    job_prefix: str
    image_name: str
    git_repo: str
    git_ref: str
    source_configmap: str
    source_sha256: str


@dataclass(frozen=True)
class PolicyConfig:
    """Safety policy defaults for real Kubernetes job creation."""

    namespace_allowlist: tuple[str, ...]
    max_batch_size: int
    confirmation_phrase: str


@dataclass(frozen=True)
class MonitoringConfig:
    """Configured deadlines for monitoring and ingestion."""

    timeout_seconds: float
    poll_interval_seconds: float
    pending_start_timeout_seconds: int
    ingestion_grace_seconds: float
    validation_poll_seconds: float
    validation_pending_seconds: float
    log_tail_lines: int
    verification_max_bytes: int
    verification_max_files: int


@dataclass(frozen=True)
class LiveConfig:
    """Main-TOML settings for the persistent controller."""

    session_name: str
    log_dir: str
    runner_worktree: str
    loop_sleep_seconds: int
    plan_limit: str
    git_branch: str
    control_host: str
    cluster_server: str
    status_dir: str
    status_refresh_seconds: int


@dataclass(frozen=True)
class MaintenanceConfig:
    """Explicit bounds for separately approved maintenance commands."""

    backup_safety_margin_percent: float
    inventory_entry_limit: int
    inventory_seconds_per_entry: float
    journal_backup_timeout_seconds: int
    staging_directory: str


@dataclass(frozen=True)
class StorageConfig:
    """SQLite metadata paths on the shared validation PVC."""

    validation_db_path: str
    storage_db_path: str
    nccl_db_path: str
    dl_numerical_db_path: str
    dl_compute_db_path: str
    dl_collective_db_path: str
    dl_overlap_db_path: str


@dataclass(frozen=True)
class RuntimeConfig:
    """In-pod runtime paths and outer child deadlines."""

    repo_dir: str
    validation_root: str
    validation_tests_dir: str
    dl_unit_test_dir: str
    dl_results_root_path: str
    runner_timeout_seconds: int
    ingestion_timeout_seconds: int


@dataclass(frozen=True)
class TestsConfig:
    """Dynamic repository-local validation test registry."""

    registry: ValidationTestRegistry


@dataclass(frozen=True)
class JobTemplateConfig:
    """Values injected into the Volcano job template."""

    namespace: str
    queue: str
    app_label: str
    pvc_claim: str
    container_image: str
    shared_memory_size: str
    gpu_resource_name: str
    gpu_count: str
    cpu: str
    memory: str
    rdma_resource_name: str
    rdma_count: str
    rdma_toleration_key: str
    gpu_toleration_key: str
    pod_restart_policy: str
    job_max_retry: int
    task_max_retry: int
    failure_action: str
    failure_events: tuple[str, ...]


@dataclass(frozen=True)
class CvalConfig:
    """Complete c-val configuration tree."""

    cluster: ClusterConfig
    scheduling: SchedulingConfig
    job: JobConfig
    policy: PolicyConfig
    monitoring: MonitoringConfig
    storage: StorageConfig
    runtime: RuntimeConfig
    tests: TestsConfig
    job_template: JobTemplateConfig
    sqlite_retry: RetryPolicy
    live: LiveConfig
    maintenance: MaintenanceConfig


def load_config(
    path: Path | str | None = None,
    *,
    validate_plugins: bool = True,
) -> CvalConfig:
    """Load a complete c-val TOML configuration without setting defaults."""

    snapshot = os.environ.get("CVAL_CONFIG_SNAPSHOT_B64")
    if snapshot:
        runtime_root = os.environ.get("CVAL_TEST_REPO_ROOT") or os.environ.get("CVAL_REPO_DIR")
        return load_config_snapshot(snapshot, repo_root=Path(runtime_root) if runtime_root else None)
    config_path = _config_path(path)
    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("c-val config must be a TOML table")
    return _build_config(data, validate_plugins=validate_plugins)


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
        "sqlite_retry": asdict(config.sqlite_retry),
        "live": _live_snapshot(config.live),
        "maintenance": asdict(config.maintenance),
    }


def _live_snapshot(config: LiveConfig) -> dict[str, Any]:
    values = asdict(config)
    if not config.status_dir and config.status_refresh_seconds == 0:
        values.pop("status_dir")
        values.pop("status_refresh_seconds")
    return values


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
        "sqlite_retry": asdict(config.sqlite_retry),
        "live": _live_snapshot(config.live),
        "maintenance": asdict(config.maintenance),
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
        "sqlite_retry",
        "live",
        "maintenance",
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
    if isinstance(data.get("live"), dict) and not {"status_dir", "status_refresh_seconds"}.intersection(data["live"]):
        data["live"].update(status_dir="", status_refresh_seconds=0)
    config = _build_config(
        data,
        repo_root=repo_root,
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


def _absolute_lexical_config_path(value: str, field_name: str) -> Path:
    """Require one absolute, normalized, concrete path without expansion."""

    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{field_name} must be a non-empty filesystem path")
    if not os.path.isabs(value):
        raise ValueError(f"{field_name} must be an absolute path")
    if os.path.normpath(value) != value or value == os.path.sep:
        raise ValueError(f"{field_name} must be lexical-canonical")
    path = Path(value)
    if any(part in {"", ".", ".."} for part in path.parts[1:]):
        raise ValueError(f"{field_name} must not contain traversal components")
    return path


def _build_config(
    data: dict[str, Any],
    *,
    repo_root: Path | None = None,
    validate_plugins: bool = True,
) -> CvalConfig:
    root = repo_root or REPO_ROOT
    section_types = get_type_hints(CvalConfig)
    _require_keys(data, set(section_types), "configuration")
    values = {
        name: _typed_section(data[name], section_type, name, root)
        for name, section_type in section_types.items()
        if name != "tests"
    }
    values["tests"] = TestsConfig(load_test_registry(
        _section(data, "tests"), repo_root=root, include_defaults=False,
    ))
    config = CvalConfig(**values)
    _validate_config(config, validate_plugins=validate_plugins)
    return config


def validate_single_attempt_config(config: JobTemplateConfig) -> None:
    """Reject settings that permit workload replay or Volcano's zero defaults."""

    if config.pod_restart_policy != "Never":
        raise ValueError("job_template.pod_restart_policy must be Never for single-attempt validation")
    if any(type(value) is not int or value != 1 for value in (config.job_max_retry, config.task_max_retry)):
        raise ValueError("Single-attempt validation requires job_max_retry=1 and task_max_retry=1; zero is defaulted by Volcano")
    if config.failure_action != "TerminateJob":
        raise ValueError("job_template.failure_action must be TerminateJob")
    if len(config.failure_events) != 4 or set(config.failure_events) != {"PodFailed", "PodEvicted", "TaskFailed", "Unknown"}:
        raise ValueError("job_template.failure_events must contain PodFailed, PodEvicted, TaskFailed and Unknown exactly once")


def _validate_config(config: CvalConfig, *, validate_plugins: bool = True) -> None:
    """Reject invalid test settings before rendering or submitting jobs."""

    tests = config.tests
    for section_name in ("cluster", "job", "policy", "storage", "runtime", "job_template", "live", "maintenance"):
        section = getattr(config, section_name)
        for item in fields(section):
            value = getattr(section, item.name)
            if isinstance(value, str) and not value and (section_name, item.name) not in {
                ("cluster", "node_filter"), ("job", "source_configmap"), ("job", "source_sha256"),
                ("live", "status_dir"),
            }:
                raise ValueError(f"{section_name}.{item.name} must not be empty")
    for name, value in vars(config.storage).items():
        _absolute_lexical_config_path(value, f"storage.{name}")
    for name, value in vars(config.runtime).items():
        if isinstance(value, str):
            _absolute_lexical_config_path(value, f"runtime.{name}")
    _absolute_lexical_config_path(config.live.runner_worktree, "live.runner_worktree")
    _absolute_lexical_config_path(config.live.log_dir, "live.log_dir")
    if config.live.status_dir:
        status_path = _absolute_lexical_config_path(config.live.status_dir, "live.status_dir")
        state_path = Path(config.live.log_dir)
        if status_path.is_relative_to(state_path) or state_path.is_relative_to(status_path):
            raise ValueError("live.status_dir and live.log_dir must be separate directories")
        if config.live.status_refresh_seconds <= 0:
            raise ValueError("live.status_refresh_seconds must be positive")
    elif config.live.status_refresh_seconds != 0:
        raise ValueError("Disabled historical status transport requires a zero refresh interval")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", config.live.control_host):
        raise ValueError("live.control_host must be an explicit hostname")
    from urllib.parse import urlsplit

    server = urlsplit(config.live.cluster_server)
    if server.scheme != "https" or not server.hostname or server.username or server.password or server.query or server.fragment:
        raise ValueError("live.cluster_server must be an HTTPS API endpoint without credentials")
    _absolute_lexical_config_path(config.maintenance.staging_directory, "maintenance.staging_directory")
    if config.maintenance.backup_safety_margin_percent < 0:
        raise ValueError("maintenance.backup_safety_margin_percent must be non-negative")
    if not 1 <= config.maintenance.inventory_entry_limit <= 100000 or not 0 < config.maintenance.inventory_seconds_per_entry <= 60:
        raise ValueError("Invalid maintenance inventory bounds")
    if config.maintenance.journal_backup_timeout_seconds <= 0:
        raise ValueError("maintenance.journal_backup_timeout_seconds must be positive")
    for name, value in (
        ("cluster.kubectl_timeout_seconds", config.cluster.kubectl_timeout_seconds),
        ("runtime.runner_timeout_seconds", config.runtime.runner_timeout_seconds),
        ("runtime.ingestion_timeout_seconds", config.runtime.ingestion_timeout_seconds),
        ("monitoring.timeout_seconds", config.monitoring.timeout_seconds),
        ("monitoring.poll_interval_seconds", config.monitoring.poll_interval_seconds),
        ("monitoring.validation_poll_seconds", config.monitoring.validation_poll_seconds),
        ("monitoring.validation_pending_seconds", config.monitoring.validation_pending_seconds),
        ("monitoring.log_tail_lines", config.monitoring.log_tail_lines),
        ("monitoring.verification_max_bytes", config.monitoring.verification_max_bytes),
        ("monitoring.verification_max_files", config.monitoring.verification_max_files),
        ("live.loop_sleep_seconds", config.live.loop_sleep_seconds),
        ("scheduling.batch_size", config.scheduling.batch_size),
        ("scheduling.coverage_max_runs_per_node", config.scheduling.coverage_max_runs_per_node),
        ("policy.max_batch_size", config.policy.max_batch_size),
    ):
        if value <= 0:
            raise ValueError(f"{name} must be positive")
    if config.scheduling.days_threshold < 0:
        raise ValueError("scheduling.days_threshold must be non-negative")
    if config.cluster.namespace != config.job_template.namespace:
        raise ValueError("cluster.namespace must match job_template.namespace")
    validate_single_attempt_config(config.job_template)
    if config.live.plan_limit != "all" and (not config.live.plan_limit.isdigit() or int(config.live.plan_limit) <= 0):
        raise ValueError("live.plan_limit must be all or a positive integer string")
    if not tests.registry.enabled:
        raise ValueError("At least one test must be enabled under [tests.*]")
    if (
        isinstance(config.scheduling.node_cooldown_seconds, bool)
        or config.scheduling.node_cooldown_seconds < 0
    ):
        raise ValueError("scheduling.node_cooldown_seconds must be a non-negative integer")
    if (
        isinstance(config.monitoring.pending_start_timeout_seconds, bool)
        or config.monitoring.pending_start_timeout_seconds <= 0
    ):
        raise ValueError(
            "monitoring.pending_start_timeout_seconds must be a positive integer"
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
    if config.monitoring.ingestion_grace_seconds <= 0:
        raise ValueError("monitoring.ingestion_grace_seconds must be positive")
    if config.monitoring.timeout_seconds < sequential_timeout + config.monitoring.ingestion_grace_seconds:
        raise ValueError(
            "monitoring.timeout_seconds must cover enabled sequential test "
            "timeouts plus configured ingestion grace"
        )

    # Import lazily to avoid the plugins module's TYPE_CHECKING-only config
    # dependency becoming a runtime cycle. Validate disabled declarations too:
    # enabling a test must never reveal a previously hidden invalid adapter.
    if validate_plugins:
        from cval.validation.plugins import validate_registry_plugins

        validate_registry_plugins(tests.registry.tests)


def _section(data: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = data[name]
    if not isinstance(value, Mapping):
        raise ValueError(f"Config section [{name}] must be a table")
    return dict(value)


def _require_keys(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        raise ValueError(f"{name}: missing keys {missing}; unknown keys {unknown}")


def _typed_section(value: Any, section_type: type, name: str, root: Path):
    if not isinstance(value, Mapping):
        raise ValueError(f"Config section [{name}] must be a table")
    types = get_type_hints(section_type)
    _require_keys(value, {item.name for item in fields(section_type)}, name)
    converted = {}
    for key, kind in types.items():
        item = value[key]
        if kind is Path:
            if not isinstance(item, str) or not item:
                raise ValueError(f"{name}.{key} must be a nonempty path string")
            path = Path(item).expanduser()
            item = path if path.is_absolute() else root / path
        elif kind == tuple[str, ...]:
            if not isinstance(item, (list, tuple)) or not all(isinstance(entry, str) for entry in item):
                raise ValueError(f"{name}.{key} must be a list of strings")
            item = tuple(item)
        elif kind is float:
            if type(item) not in (int, float) or not math.isfinite(item):
                raise ValueError(f"{name}.{key} must be a finite number")
            item = float(item)
        elif type(item) is not kind:
            raise ValueError(f"{name}.{key} must be {kind.__name__}")
        converted[key] = item
    return section_type(**converted)
