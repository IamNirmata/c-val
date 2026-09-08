"""Evaluator-only settings; deliberately excluded from raw-run snapshots."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import tomllib
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from cval.config import CvalConfig, load_config
from cval.storage.retry import RetryPolicy
from cval.storage.sqlite_uri import canonical_sqlite_path


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def positive(value, name: str, maximum=10**9):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= maximum:
        raise ValueError(f"{name} must be positive and <= {maximum}")
    return value


@dataclass(frozen=True)
class TestPolicy:
    name: str
    directory: Path
    settings: dict
    baseline: ModuleType
    classification: ModuleType
    policy_digest: str


@dataclass(frozen=True)
class Settings:
    raw: CvalConfig
    output: Path
    retry: RetryPolicy
    shared_raw_storage: bool
    poll_seconds: float
    baseline_check_seconds: float
    batch_size: int
    max_training_runs: int
    query_timeout_seconds: float
    max_metric_rows_per_run: int
    tests: tuple[TestPolicy, ...]


def load_module(path: Path):
    spec = importlib.util.spec_from_file_location("eval_" + digest(str(path)), path)
    if spec is None or spec.loader is None:
        raise ValueError(f"cannot load evaluator: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_settings(path: Path) -> Settings:
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    options = document.get("evaluation", {})
    allowed = {"enabled", "output_db", "shared_raw_storage", "poll_seconds", "baseline_check_seconds", "batch_size", "max_training_runs", "query_timeout_seconds", "max_metric_rows_per_run"}
    if set(options) - allowed:
        raise ValueError("unknown global evaluation setting")
    if options.get("enabled") is not True:
        raise ValueError("evaluation.enabled must be true")
    if type(options.get("shared_raw_storage", True)) is not bool:
        raise ValueError("shared_raw_storage must be boolean")
    raw = load_config(path)
    output = canonical_sqlite_path(options["output_db"], must_exist=False)
    raw_root = Path(raw.runtime.validation_root).resolve()
    if output.is_relative_to(raw_root):
        raise ValueError("evaluation output must be outside the raw validation root")
    if output in {Path(value).resolve() for value in vars(raw.storage).values()}:
        raise ValueError("evaluation output must not alias a raw DB")
    if output.exists() and any(Path(value).exists() and output.samefile(value) for value in vars(raw.storage).values()):
        raise ValueError("evaluation output must not hard-link a raw DB")
    tests = []
    test_allowed = {"enabled", "baseline_script", "classification_script", "refresh_seconds", "window_seconds", "holdout_seconds", "expires_seconds", "min_nodes", "quantile", "min_relative_gap", "atol", "rtol", "min_mode_fraction", "expected_ranks", "require_hardware", "metrics"}
    for registered in raw.tests.registry.tests:
        descriptor = tomllib.loads(registered.resolved_config_path.read_text(encoding="utf-8"))
        policy = descriptor.get("evaluation", {})
        if not policy.get("enabled", False):
            continue
        if set(policy) - test_allowed:
            raise ValueError(f"unknown evaluator setting for {registered.id}")
        for field in ("refresh_seconds", "window_seconds", "holdout_seconds", "expires_seconds", "min_nodes", "expected_ranks"):
            positive(policy[field], field)
        if not 0.5 < policy["quantile"] < 1 or not 0.5 < policy["min_mode_fraction"] <= 1:
            raise ValueError("invalid quantile or numerical mode fraction")
        for field in ("atol", "rtol", "min_relative_gap"):
            positive(policy[field], field, 1)
        for field in ("min_nodes", "expected_ranks"):
            if type(policy[field]) is not int:
                raise ValueError(f"{field} must be an integer")
        if type(policy.get("require_hardware")) is not bool:
            raise ValueError("require_hardware must be boolean")
        if not isinstance(policy.get("metrics"), dict) or not policy["metrics"]:
            raise ValueError("explicit metric policy required")
        for names in policy["metrics"].values():
            if not isinstance(names, list) or not names or any(not isinstance(name, str) or not name for name in names) or len(set(names)) != len(names):
                raise ValueError("metrics must contain unique nonempty name lists")
        modules = []
        for field in ("baseline_script", "classification_script"):
            script = registered.test_dir / policy[field]
            if script.resolve().parent != registered.test_dir.resolve() or script.is_symlink():
                raise ValueError("evaluation scripts must be owned by their test directory")
            modules.append(load_module(script))
        tests.append(TestPolicy(registered.id, registered.test_dir, policy, *modules, digest(policy)))
    if not tests:
        raise ValueError("no enabled test evaluators")
    limits = {}
    for field, default, maximum in (("poll_seconds", 60, 86400), ("baseline_check_seconds", 3600, 86400), ("batch_size", 20, 1000), ("max_training_runs", 100, 1000), ("query_timeout_seconds", 20, 300), ("max_metric_rows_per_run", 50000, 1000000)):
        limits[field] = positive(options.get(field, default), field, maximum)
    for field in ("batch_size", "max_training_runs", "max_metric_rows_per_run"):
        if type(limits[field]) is not int:
            raise ValueError(f"{field} must be an integer")
    return Settings(raw, output, RetryPolicy(**document.get("sqlite_retry", {})), options.get("shared_raw_storage", True), **limits, tests=tuple(tests))