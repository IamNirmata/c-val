"""Opt-in helpers for disposable PostgreSQL integration tests."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse


_DISPOSABLE_NAME = re.compile(r"^cval_test_[a-z0-9_]+$")
CLEANUP_CONFIRMATION = "drop-cval-nccl-test-schemas"


def require_disposable_test_url(database_url: str) -> str:
    """Reject production-looking URLs before any integration connection."""

    if not isinstance(database_url, str) or not database_url.strip():
        raise ValueError("CVAL_NCCL_TEST_DATABASE_URL must be non-empty")
    parsed = urlparse(database_url)
    database = parsed.path.lstrip("/").split("?", 1)[0]
    if parsed.scheme not in {"postgres", "postgresql"} or not database:
        raise ValueError("CVAL_NCCL_TEST_DATABASE_URL must be a PostgreSQL URL with a database")
    if not _DISPOSABLE_NAME.fullmatch(database):
        raise ValueError(
            "CVAL_NCCL_TEST_DATABASE_URL database name must match cval_test_[a-z0-9_]+"
        )
    return database


def clean_nccl_schemas(pool: Any, *, confirm: str) -> None:
    """Drop only subsystem-owned schemas in an already validated disposable DB."""

    if confirm != CLEANUP_CONFIRMATION:
        raise ValueError(
            f"NCCL test cleanup requires confirm={CLEANUP_CONFIRMATION!r}"
        )
    with pool.connection() as connection:
        with connection.transaction():
            row = connection.execute("SELECT current_database()").fetchone()
            database = "" if row is None else str(row[0])
            if not _DISPOSABLE_NAME.fullmatch(database):
                raise ValueError("refusing NCCL cleanup outside a cval_test_* database")
            connection.execute("DROP SCHEMA IF EXISTS nccl_validation CASCADE")
            connection.execute("DROP SCHEMA IF EXISTS nccl_baseline CASCADE")
            connection.execute("DROP SCHEMA IF EXISTS nccl_raw CASCADE")
