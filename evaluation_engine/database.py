"""Separate derived DB with short transactions and bounded lock retries."""

from contextlib import closing
import json

from cval.storage.retry import retry_sqlite
from cval.storage.sqlite_uri import connect_sqlite_file


SCHEMA = """
CREATE TABLE IF NOT EXISTS baseline_version (
 baseline_id TEXT PRIMARY KEY, test TEXT NOT NULL, cohort_key TEXT NOT NULL,
 policy_digest TEXT NOT NULL, evaluator_sha TEXT NOT NULL, cohort_json TEXT NOT NULL,
 policy_json TEXT NOT NULL, training_manifest_json TEXT NOT NULL,
 built_at INTEGER NOT NULL, cutoff INTEGER NOT NULL, expires_at INTEGER NOT NULL,
 active INTEGER NOT NULL CHECK(active IN (0,1))
);
CREATE UNIQUE INDEX IF NOT EXISTS active_baseline ON baseline_version(test,cohort_key) WHERE active=1;
CREATE TABLE IF NOT EXISTS threshold (
 baseline_id TEXT NOT NULL REFERENCES baseline_version(baseline_id), metric_key TEXT NOT NULL,
 rule_json TEXT NOT NULL, PRIMARY KEY(baseline_id,metric_key)
);
CREATE TABLE IF NOT EXISTS evaluation (
 evaluation_id TEXT PRIMARY KEY, node TEXT NOT NULL, raw_timestamp INTEGER NOT NULL,
 test TEXT NOT NULL, source_digest TEXT NOT NULL, baseline_id TEXT REFERENCES baseline_version(baseline_id),
 policy_digest TEXT NOT NULL, evaluator_sha TEXT NOT NULL, status TEXT NOT NULL,
 reason TEXT NOT NULL, evaluated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS evaluation_run ON evaluation(node,raw_timestamp,test,evaluated_at);
CREATE TABLE IF NOT EXISTS metric_classification (
 evaluation_id TEXT NOT NULL REFERENCES evaluation(evaluation_id), metric_key TEXT NOT NULL,
 rank INTEGER NOT NULL, value REAL, health_class TEXT, reason TEXT NOT NULL,
 PRIMARY KEY(evaluation_id,metric_key,rank)
);
CREATE TABLE IF NOT EXISTS test_classification (
 evaluation_id TEXT PRIMARY KEY REFERENCES evaluation(evaluation_id), raw_status TEXT NOT NULL,
 health_class TEXT, expected INTEGER NOT NULL, seen INTEGER NOT NULL,
 classified INTEGER NOT NULL, bad INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS worker_state (
 task TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at INTEGER NOT NULL
);
CREATE VIEW IF NOT EXISTS latest_test_class AS
WITH ordered AS (
 SELECT evaluation.*, ROW_NUMBER() OVER (
 PARTITION BY node,test ORDER BY raw_timestamp DESC,evaluated_at DESC,rowid DESC) position
 FROM evaluation
)
SELECT ordered.node,ordered.test,ordered.raw_timestamp,ordered.status,ordered.reason,
 ordered.evaluation_id,ordered.baseline_id,ordered.evaluated_at,
 summary.raw_status,summary.health_class,summary.expected,summary.seen,summary.classified,summary.bad,
 baseline.expires_at,baseline.expires_at < CAST(strftime('%s','now') AS INTEGER) AS baseline_expired
FROM ordered JOIN test_classification summary USING(evaluation_id)
LEFT JOIN baseline_version baseline USING(baseline_id) WHERE position=1;
"""


class Database:
    def __init__(self, settings):
        self.settings = settings

    def transaction(self, callback):
        def operation():
            with closing(connect_sqlite_file(self.settings.output, mode="rwc", timeout=self.settings.retry.timeout_seconds)) as connection:
                connection.execute("PRAGMA foreign_keys=ON")
                with connection:
                    connection.execute("BEGIN IMMEDIATE")
                    return callback(connection)
        return retry_sqlite(operation, self.settings.retry)

    def initialize(self):
        self.settings.output.parent.mkdir(parents=True, exist_ok=True)

        def operation():
            with closing(connect_sqlite_file(self.settings.output, mode="rwc", timeout=self.settings.retry.timeout_seconds)) as connection:
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.executescript(SCHEMA)
        retry_sqlite(operation, self.settings.retry)

    def read(self, sql, params=()):
        def operation():
            with closing(connect_sqlite_file(self.settings.output, mode="ro", timeout=self.settings.retry.timeout_seconds)) as connection:
                connection.execute("PRAGMA query_only=ON")
                return connection.execute(sql, params).fetchall()
        return retry_sqlite(operation, self.settings.retry)

    def state(self, task, payload, timestamp):
        return self.transaction(lambda connection: connection.execute(
            "INSERT INTO worker_state VALUES (?,?,?) ON CONFLICT(task) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
            (task, json.dumps(payload, sort_keys=True), timestamp),
        ))