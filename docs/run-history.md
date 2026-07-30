# Node Run History

**Implementation status:** U6 implemented and validated locally. Live PVC creation or migration is not authorized by local implementation approval.

Node run history records one normalized row per c-val execution and one row per registered test. It complements the current fixed compatibility `validation.db` surface; it does not replace or mutate that database.

## Canonical database

```text
/data/continuous_validation/metadata/node-run-history.db
```

Configured by:

```toml
[storage]
run_history_enabled = false
run_history_db_path = "/data/continuous_validation/metadata/node-run-history.db"
```

The writer is default-off. Deploying U6 code does not create or write the live
database until `run_history_enabled=true` is separately approved and deployed.

## Ownership and meaning

- `runs` contains run identity, aggregate raw status, selected test IDs, image/framework versions, Git ref, config digest, result path, and timestamps.
- `run_tests` contains one normalized execution row for every registered test, including disabled/not-selected tests.
- `node_run_history` is a human-readable one-row-per-run view with ordered `tests_ran`.
- `schema_migrations` records additive schema version 1.
- Health classes and baseline verdicts do not belong in this database.

The source is a strictly validated `cval.results.v2` artifact. Historical v1 artifacts are not synthesized into this database automatically.

## Write behavior

The compatibility ingestion phase invokes the hidden internal command only for v2 results:

```text
cval db-upsert-run-history --result-json <canonical-result> --db-path <history-db>
```

Properties:

- The run and all test rows are written in one `BEGIN IMMEDIATE` transaction.
- `run_id` is the idempotency key.
- Repeating an identical ingestion updates `updated_at` without duplicating rows.
- A reused run ID with a different node, start timestamp, test set, aggregate status, or test terminal state is rejected and rolled back.
- Foreign keys are enabled and destructive cascades are disabled.
- Existing `validation.db` writes remain a separate compatibility transaction.

## Read-only CLI

```bash
cval history
cval history --node <node> --limit 20
cval history --test nccl --status fail --output json
cval history --run-id <run-id> --output json
```

The command resolves the PVC access pod, passes the query script over stdin, and opens SQLite with `mode=ro`. Missing databases return an empty result and are not created.

With `--test`, `--status` filters that selected test's raw status. Without
`--test`, it filters the aggregate run status.

Table output shows start epoch, node, aggregate status, ordered selected tests, and run ID. JSON includes all run metadata.

## Local validation

Use a temporary database only:

```bash
python -m cval.cli db-upsert-run-history \
  --result-json /tmp/cval-result-v2.json \
  --db-path /tmp/node-run-history.db

python -m unittest tests.test_run_history
```

The public `history` command normally reads through the PVC pod. Direct local reader behavior is covered by unit tests.

## Production activation runbook

The following stages are intentionally separate from local implementation.

### 1. Read-only preflight

- Verify the deployed result writer emits `cval.results.v2`.
- Verify the configured path and PVC free space.
- Confirm no database already exists at the target with an unsupported schema.
- Record current Git commit and runtime configuration digest.

These checks must not create the target file.

### 2. Backup approval

If a target DB already exists, obtain explicit approval before copying it. The approved command must use a timestamped sibling path and preserve mode/ownership. Record source size and SHA-256 before and after the copy.

Do not rename, truncate, or delete the original database.

### 3. Dry-run conversion

Before enabling writes, select representative v2 result artifacts and ingest them into a temporary sibling or `/tmp` database. Verify:

- One `runs` row per unique run ID.
- One `run_tests` row per registered test.
- Repeated ingestion changes no row counts.
- Filters and the `node_run_history` view return expected results.
- Invalid/mismatched artifacts leave row counts unchanged.

### 4. Explicit live-write approval

Creating or writing the production database requires separate approval with:

- Exact command/deployment commit.
- Target namespace, pod, and database path.
- Backup evidence when applicable.
- Dry-run counts and validation output.
- Rollback plan.

Local U6 approval does not grant this permission.

The approved activation change is the explicit switch:

```toml
[storage]
run_history_enabled = true
```

### 5. Observe and rollback

After approved activation:

- Compare v2 result count to distinct history run IDs.
- Check ingestion events for failures.
- Run `cval history --limit 10` read-only.
- Rollback by disabling the new writer or reverting the pinned code ref. Keep the additive database for evidence; do not delete it.

## Compatibility boundary

`metadata/validation.db` remains the source for existing freshness scheduling and fixed latest status during U6. Later work may migrate readers only after parity is measured and separately approved.
