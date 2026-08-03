"""Packaged SQL migration discovery and application."""

from __future__ import annotations

import hashlib
import importlib
import re
from dataclasses import dataclass
from importlib import resources
from typing import Any


_DISPOSABLE_DATABASE = re.compile(r"^cval_test_[a-z0-9_]+$")
_RUNTIME_ROLE = re.compile(r"^[a-z_][a-z0-9_-]{0,62}$")


@dataclass(frozen=True)
class Migration:
    migration_id: str
    sha256: str
    sql: str

    def public_dict(self) -> dict[str, object]:
        return {
            "migration_id": self.migration_id,
            "sha256": self.sha256,
            "bytes": len(self.sql.encode("utf-8")),
        }


def migrations() -> tuple[Migration, ...]:
    """Load packaged migrations in lexical identity order."""

    root = resources.files("cval.nccl_eval").joinpath("migrations")
    loaded: list[Migration] = []
    for item in sorted(root.iterdir(), key=lambda path: path.name):
        if not item.name.endswith(".sql"):
            continue
        sql = item.read_text(encoding="utf-8")
        loaded.append(
            Migration(
                migration_id=item.name,
                sha256=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                sql=sql,
            )
        )
    if not loaded:
        raise RuntimeError("No packaged NCCL PostgreSQL migrations were found")
    return tuple(loaded)


def migration_plan() -> dict[str, object]:
    """Return a database-free dry-run migration receipt."""

    items = migrations()
    return {
        "mode": "dry-run",
        "database_expected": "cval",
        "schemas": ["nccl_raw", "nccl_baseline", "nccl_validation"],
        "migration_count": len(items),
        "migrations": [item.public_dict() for item in items],
    }


def apply_migrations(pool: Any, *, allow_disposable_test_database: bool = False) -> dict[str, object]:
    """Apply idempotent migrations through explicit transactions."""

    applied: list[str] = []
    existing: list[str] = []
    database_name = ""
    with pool.connection() as connection:
        with connection.transaction():
            row = connection.execute("SELECT current_database()").fetchone()
            if row is None:
                raise RuntimeError("Could not determine PostgreSQL database name")
            database_name = str(row[0])
            if database_name != "cval" and not (
                allow_disposable_test_database and _DISPOSABLE_DATABASE.fullmatch(database_name)
            ):
                raise ValueError(
                    "NCCL schema may only be applied to database 'cval' "
                    "or an explicitly allowed disposable test database"
                )
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                ("cval.nccl_eval.schema_migrations",),
            )
            for migration in migrations():
                ledger_exists = connection.execute(
                    "SELECT to_regclass('nccl_raw.schema_migration')"
                ).fetchone()
                stored = None
                if ledger_exists and ledger_exists[0] is not None:
                    stored = connection.execute(
                        "SELECT sha256 FROM nccl_raw.schema_migration WHERE migration_id = %s",
                        (migration.migration_id,),
                    ).fetchone()
                if stored is not None:
                    if stored[0] != migration.sha256:
                        raise RuntimeError(
                            f"Applied migration checksum mismatch: {migration.migration_id}"
                        )
                    existing.append(migration.migration_id)
                    continue
                connection.execute(migration.sql)
                connection.execute(
                    "INSERT INTO nccl_raw.schema_migration (migration_id, sha256) "
                    "VALUES (%s, %s)",
                    (migration.migration_id, migration.sha256),
                )
                applied.append(migration.migration_id)
    return {
        "mode": "apply",
        "database": database_name,
        "applied": applied,
        "already_applied": existing,
        "migration_count": len(applied) + len(existing),
    }


def provision_runtime_role(
    pool: Any,
    *,
    username: str,
    password: str,
    allow_disposable_test_database: bool = False,
) -> dict[str, object]:
    """Create/rotate the non-owner runtime login and grant only subsystem DML."""

    if not isinstance(username, str) or not _RUNTIME_ROLE.fullmatch(username):
        raise ValueError("runtime username must be a bounded lowercase PostgreSQL role name")
    if (
        not isinstance(password, str)
        or not password
        or len(password) > 1024
        or "\n" in password
        or "\r" in password
    ):
        raise ValueError("runtime password must be a non-empty bounded single-line secret")
    try:
        sql = importlib.import_module("psycopg.sql")
    except ImportError as exc:
        raise RuntimeError("runtime role provisioning requires psycopg") from exc
    role = sql.Identifier(username)
    password_literal = sql.Literal(password)
    created = False
    with pool.connection() as connection:
        with connection.transaction():
            database = connection.execute("SELECT current_database()").fetchone()
            database_name = "" if database is None else str(database[0])
            if database_name != "cval" and not (
                allow_disposable_test_database
                and _DISPOSABLE_DATABASE.fullmatch(database_name)
            ):
                raise ValueError("runtime role may only be provisioned in database 'cval'")
            attestation = _attest_runtime_role_reuse(connection, username)
            if attestation is None:
                credential_statement = sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
                ).format(role, password_literal)
                created = True
            else:
                credential_statement = sql.SQL(
                    "ALTER ROLE {} WITH LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
                ).format(role, password_literal)
            try:
                connection.execute(credential_statement)
            except Exception:
                raise RuntimeError(
                    "runtime role credential statement failed"
                ) from None
            connection.execute(
                sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(
                    sql.Identifier(database_name)
                )
            )
            connection.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
            connection.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database_name), role
                )
            )
            connection.execute(
                sql.SQL(
                    "GRANT USAGE ON SCHEMA nccl_raw, nccl_baseline, nccl_validation TO {}"
                ).format(role)
            )
            grants = (
                "GRANT SELECT, INSERT ON nccl_raw.test_run, nccl_raw.node_result, "
                "nccl_raw.nic_result, nccl_raw.outbox_receipt TO {}",
                "GRANT SELECT, UPDATE ON nccl_raw.outbox_scan_cursor TO {}",
                "GRANT SELECT, INSERT, UPDATE ON nccl_baseline.baseline_profile, "
                "nccl_baseline.baseline_version, nccl_baseline.baseline_version_sample, "
                "nccl_baseline.metric_threshold TO {}",
                "REVOKE INSERT ON nccl_baseline.calibration_decision FROM {}",
                "GRANT SELECT ON nccl_baseline.calibration_decision TO {}",
                "GRANT EXECUTE ON FUNCTION nccl_baseline.apply_calibration_decision("
                "UUID, BIGINT, TEXT, TEXT, TEXT, JSONB) TO {}",
                "GRANT SELECT ON nccl_validation.health_class TO {}",
                "GRANT SELECT, INSERT, UPDATE ON nccl_validation.evaluation_job TO {}",
                "GRANT SELECT, INSERT ON nccl_validation.evaluation TO {}",
                "GRANT SELECT ON nccl_validation.raw_result_status_view, "
                "nccl_validation.latest_result_view TO {}",
                "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA nccl_raw, "
                "nccl_baseline, nccl_validation TO {}",
            )
            for grant in grants:
                connection.execute(sql.SQL(grant).format(role))
    return {
        "mode": "apply",
        "database": database_name,
        "runtime_role": username,
        "created": created,
        "reuse_attested": not created,
        "unsafe_role_attributes": False,
        "role_memberships": False,
        "owned_database_or_nccl_objects": False,
        "ownership_granted": False,
        "schema_migration_granted": False,
        "truncate_granted": False,
    }


def _attest_runtime_role_reuse(connection: Any, username: str) -> dict[str, object] | None:
    """Fail before credential/grant mutation when a preexisting role is privileged."""

    role = connection.execute(
        """
        SELECT oid, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
        FROM pg_roles WHERE rolname = %s
        """,
        (username,),
    ).fetchone()
    if role is None:
        return None
    role_oid = role[0]
    if any(bool(value) for value in role[1:]):
        raise ValueError(
            "preexisting runtime role has unsafe superuser/create/replication/bypass attributes"
        )
    membership = connection.execute(
        "SELECT 1 FROM pg_auth_members WHERE member = %s OR roleid = %s LIMIT 1",
        (role_oid, role_oid),
    ).fetchone()
    if membership is not None:
        raise ValueError("preexisting runtime role has role memberships")
    database_owner = connection.execute(
        "SELECT datname FROM pg_database WHERE datdba = %s LIMIT 1",
        (role_oid,),
    ).fetchone()
    if database_owner is not None:
        raise ValueError("preexisting runtime role owns a database")
    schema_owner = connection.execute(
        """
        SELECT nspname FROM pg_namespace
        WHERE nspowner = %s
        LIMIT 1
        """,
        (role_oid,),
    ).fetchone()
    if schema_owner is not None:
        raise ValueError("preexisting runtime role owns a schema")
    relation_owner = connection.execute(
        """
        SELECT namespace.nspname, relation.relname
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE relation.relowner = %s
        LIMIT 1
        """,
        (role_oid,),
    ).fetchone()
    if relation_owner is not None:
        raise ValueError("preexisting runtime role owns a relation")
    function_owner = connection.execute(
        """
        SELECT namespace.nspname, routine.proname
        FROM pg_proc AS routine
        JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace
        WHERE routine.proowner = %s
        LIMIT 1
        """,
        (role_oid,),
    ).fetchone()
    if function_owner is not None:
        raise ValueError("preexisting runtime role owns a function")
    type_owner = connection.execute(
        """
        SELECT namespace.nspname, type.typname
        FROM pg_type AS type
        JOIN pg_namespace AS namespace ON namespace.oid = type.typnamespace
        WHERE type.typowner = %s
        LIMIT 1
        """,
        (role_oid,),
    ).fetchone()
    if type_owner is not None:
        raise ValueError("preexisting runtime role owns a type")
    default_acl = connection.execute(
        """
        SELECT 1
        FROM pg_default_acl AS defaults
        WHERE defaults.defaclrole = %s
           OR EXISTS (
                SELECT 1 FROM aclexplode(defaults.defaclacl) AS acl
                WHERE acl.grantee = %s
           )
        LIMIT 1
        """,
        (role_oid, role_oid),
    ).fetchone()
    if default_acl is not None:
        raise ValueError("preexisting runtime role owns or receives default privileges")
    unexpected_acl = _unexpected_runtime_role_acl(connection, role_oid)
    if unexpected_acl is not None:
        raise ValueError(
            "preexisting runtime role has an unexpected direct privilege: "
            f"{unexpected_acl}"
        )
    return {
        "rolsuper": False,
        "rolcreatedb": False,
        "rolcreaterole": False,
        "rolreplication": False,
        "rolbypassrls": False,
        "memberships": 0,
        "owned_objects": 0,
        "unexpected_direct_privileges": 0,
    }


def _unexpected_runtime_role_acl(connection: Any, role_oid: int) -> str | None:
    """Return the first direct ACL outside the exact runtime-role allowlist."""

    database_rows = connection.execute(
        """
        SELECT datname, acl.privilege_type, acl.is_grantable
        FROM pg_database
        CROSS JOIN LATERAL aclexplode(datacl) AS acl
        WHERE acl.grantee = %s
        ORDER BY datname, acl.privilege_type
        """,
        (role_oid,),
    ).fetchall()
    current_database = connection.execute("SELECT current_database()").fetchone()[0]
    for database, privilege, grantable in database_rows:
        if grantable or (database, privilege) != (current_database, "CONNECT"):
            return f"database {database} {privilege}"

    schema_rows = connection.execute(
        """
        SELECT namespace.nspname, acl.privilege_type, acl.is_grantable
        FROM pg_namespace AS namespace
        CROSS JOIN LATERAL aclexplode(namespace.nspacl) AS acl
        WHERE acl.grantee = %s
        ORDER BY namespace.nspname, acl.privilege_type
        """,
        (role_oid,),
    ).fetchall()
    runtime_schemas = {"nccl_raw", "nccl_baseline", "nccl_validation"}
    for schema, privilege, grantable in schema_rows:
        if grantable or schema not in runtime_schemas or privilege != "USAGE":
            return f"schema {schema} {privilege}"

    relation_rows = connection.execute(
        """
        SELECT namespace.nspname, relation.relname, relation.relkind,
               acl.privilege_type, acl.is_grantable
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        CROSS JOIN LATERAL aclexplode(relation.relacl) AS acl
        WHERE acl.grantee = %s
        ORDER BY namespace.nspname, relation.relname, acl.privilege_type
        """,
        (role_oid,),
    ).fetchall()
    relation_privileges = {
        ("nccl_raw", "test_run"): {"SELECT", "INSERT"},
        ("nccl_raw", "node_result"): {"SELECT", "INSERT"},
        ("nccl_raw", "nic_result"): {"SELECT", "INSERT"},
        ("nccl_raw", "outbox_receipt"): {"SELECT", "INSERT"},
        ("nccl_raw", "outbox_scan_cursor"): {"SELECT", "UPDATE"},
        ("nccl_baseline", "baseline_profile"): {"SELECT", "INSERT", "UPDATE"},
        ("nccl_baseline", "baseline_version"): {"SELECT", "INSERT", "UPDATE"},
        ("nccl_baseline", "baseline_version_sample"): {"SELECT", "INSERT", "UPDATE"},
        ("nccl_baseline", "metric_threshold"): {"SELECT", "INSERT", "UPDATE"},
        ("nccl_baseline", "calibration_decision"): {"SELECT"},
        ("nccl_validation", "health_class"): {"SELECT"},
        ("nccl_validation", "evaluation_job"): {"SELECT", "INSERT", "UPDATE"},
        ("nccl_validation", "evaluation"): {"SELECT", "INSERT"},
        ("nccl_validation", "raw_result_status_view"): {"SELECT"},
        ("nccl_validation", "latest_result_view"): {"SELECT"},
    }
    for schema, relation, kind, privilege, grantable in relation_rows:
        allowed = (
            {"USAGE", "SELECT"}
            if kind == "S" and schema in runtime_schemas
            else relation_privileges.get((schema, relation), set())
        )
        if grantable or privilege not in allowed:
            return f"relation {schema}.{relation} {privilege}"

    function_rows = connection.execute(
        """
         SELECT namespace.nspname, routine.proname,
             oidvectortypes(routine.proargtypes),
               acl.privilege_type, acl.is_grantable
         FROM pg_proc AS routine
         JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace
         CROSS JOIN LATERAL aclexplode(routine.proacl) AS acl
        WHERE acl.grantee = %s
         ORDER BY namespace.nspname, routine.proname
        """,
        (role_oid,),
    ).fetchall()
    expected_function = (
        "nccl_baseline",
        "apply_calibration_decision",
        "uuid, bigint, text, text, text, jsonb",
        "EXECUTE",
    )
    for schema, function, arguments, privilege, grantable in function_rows:
        if grantable or (schema, function, arguments, privilege) != expected_function:
            return f"function {schema}.{function}({arguments}) {privilege}"
    return None
