"""Strict environment configuration for the NCCL PostgreSQL evaluator."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Mapping
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class NcclEvaluationConfig:
    """Validated evaluator settings.

    ``database_url`` is deliberately excluded from repr/equality and from the
    public dictionary.  Callers must never include it in logs or receipts.
    """

    database_url: str | None = field(default=None, repr=False, compare=False)
    evaluator_batch_size: int = 25
    evaluator_poll_interval_seconds: float = 5.0
    evaluator_max_attempts: int = 8
    evaluator_stale_claim_seconds: int = 300
    evaluator_retry_base_seconds: float = 2.0
    evaluator_retry_max_seconds: float = 300.0
    baseline_minimum_results: int = 40
    baseline_update_increment: int = 10
    baseline_builder_interval_seconds: float = 300.0
    evaluator_version: str = "nccl-evaluator-v1"
    derivation_method_version: str = "nccl-median-bands-v2"
    pool_min_size: int = 1
    pool_max_size: int = 4
    pool_timeout_seconds: float = 10.0
    pool_startup_timeout_seconds: float = 15.0

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        require_database: bool = False,
    ) -> "NcclEvaluationConfig":
        """Build configuration from documented environment variables."""

        source = os.environ if environ is None else environ
        database_url = _optional_text(source, "DATABASE_URL")
        if require_database and database_url is None:
            raise ValueError("DATABASE_URL is required for NCCL database commands")
        if database_url is not None and not database_url.lower().startswith(
            ("postgresql://", "postgres://")
        ):
            raise ValueError("DATABASE_URL must be a PostgreSQL URL")
        if database_url is not None:
            database_name = unquote(urlparse(database_url).path.lstrip("/"))
            if database_name != "cval":
                raise ValueError("DATABASE_URL must select the PostgreSQL database named 'cval'")

        config = cls(
            database_url=database_url,
            evaluator_batch_size=_integer(source, "EVALUATOR_BATCH_SIZE", 25, 1, 1000),
            evaluator_poll_interval_seconds=_number(
                source, "EVALUATOR_POLL_INTERVAL_SECONDS", 5.0, 0.1, 3600.0
            ),
            evaluator_max_attempts=_integer(
                source, "EVALUATOR_MAX_ATTEMPTS", 8, 1, 100
            ),
            evaluator_stale_claim_seconds=_integer(
                source, "EVALUATOR_STALE_CLAIM_SECONDS", 300, 1, 86400
            ),
            evaluator_retry_base_seconds=_number(
                source, "EVALUATOR_RETRY_BASE_SECONDS", 2.0, 0.1, 3600.0
            ),
            evaluator_retry_max_seconds=_number(
                source, "EVALUATOR_RETRY_MAX_SECONDS", 300.0, 0.1, 86400.0
            ),
            baseline_minimum_results=_integer(
                source, "BASELINE_MINIMUM_RESULTS", 40, 40, 40
            ),
            baseline_update_increment=_integer(
                source, "BASELINE_UPDATE_INCREMENT", 10, 10, 10
            ),
            baseline_builder_interval_seconds=_number(
                source, "BASELINE_BUILDER_INTERVAL_SECONDS", 300.0, 1.0, 86400.0
            ),
            evaluator_version=_text(
                source, "EVALUATOR_VERSION", "nccl-evaluator-v1", maximum=128
            ),
            derivation_method_version=_text(
                source,
                "DERIVATION_METHOD_VERSION",
                "nccl-median-bands-v2",
                maximum=128,
            ),
            pool_min_size=_integer(source, "DATABASE_POOL_MIN_SIZE", 1, 1, 32),
            pool_max_size=_integer(source, "DATABASE_POOL_MAX_SIZE", 4, 1, 32),
            pool_timeout_seconds=_number(
                source, "DATABASE_POOL_TIMEOUT_SECONDS", 10.0, 0.1, 300.0
            ),
            pool_startup_timeout_seconds=_number(
                source, "DATABASE_POOL_STARTUP_TIMEOUT_SECONDS", 15.0, 0.1, 300.0
            ),
        )
        if config.pool_min_size > config.pool_max_size:
            raise ValueError("DATABASE_POOL_MIN_SIZE must not exceed DATABASE_POOL_MAX_SIZE")
        if config.evaluator_retry_base_seconds > config.evaluator_retry_max_seconds:
            raise ValueError(
                "EVALUATOR_RETRY_BASE_SECONDS must not exceed "
                "EVALUATOR_RETRY_MAX_SECONDS"
            )
        return config

    def require_database_url(self) -> str:
        """Return the private connection string or fail without revealing it."""

        if self.database_url is None:
            raise ValueError("DATABASE_URL is required for NCCL database commands")
        return self.database_url

    def public_dict(self) -> dict[str, object]:
        """Return safe structured configuration for receipts and diagnostics."""

        return {
            "database_configured": self.database_url is not None,
            "evaluator_batch_size": self.evaluator_batch_size,
            "evaluator_poll_interval_seconds": self.evaluator_poll_interval_seconds,
            "evaluator_max_attempts": self.evaluator_max_attempts,
            "evaluator_stale_claim_seconds": self.evaluator_stale_claim_seconds,
            "evaluator_retry_base_seconds": self.evaluator_retry_base_seconds,
            "evaluator_retry_max_seconds": self.evaluator_retry_max_seconds,
            "baseline_minimum_results": self.baseline_minimum_results,
            "baseline_update_increment": self.baseline_update_increment,
            "baseline_builder_interval_seconds": self.baseline_builder_interval_seconds,
            "evaluator_version": self.evaluator_version,
            "derivation_method_version": self.derivation_method_version,
            "pool_min_size": self.pool_min_size,
            "pool_max_size": self.pool_max_size,
            "pool_timeout_seconds": self.pool_timeout_seconds,
            "pool_startup_timeout_seconds": self.pool_startup_timeout_seconds,
        }


def _optional_text(environ: Mapping[str, str], name: str) -> str | None:
    value = environ.get(name)
    if value is None:
        return None
    value = value.strip()
    if not value:
        raise ValueError(f"{name} must not be empty when set")
    if "\n" in value or "\r" in value:
        raise ValueError(f"{name} must be a single-line value")
    return value


def _text(
    environ: Mapping[str, str], name: str, default: str, *, maximum: int
) -> str:
    value = environ.get(name, default)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    value = value.strip()
    if len(value) > maximum or "\n" in value or "\r" in value:
        raise ValueError(f"{name} must be a single-line string of at most {maximum} characters")
    return value


def _integer(
    environ: Mapping[str, str], name: str, default: int, minimum: int, maximum: int
) -> int:
    raw = environ.get(name)
    try:
        value = default if raw is None else int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        if minimum == maximum:
            raise ValueError(f"{name} must be exactly {minimum}")
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _number(
    environ: Mapping[str, str], name: str, default: float, minimum: float, maximum: float
) -> float:
    raw = environ.get(name)
    try:
        value = default if raw is None else float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value
