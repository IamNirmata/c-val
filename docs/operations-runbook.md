# Operations Runbook

## Safe Preflight

```bash
python -m cval.cli status --output table
python -m cval.cli nodes --output table
python -m cval.cli run --live-status --threshold-days 4 --batch-size 1 --output json
```

Confirm the dry-run output before submitting. Look for:

- `dry_run: true`
- `submitted_count: 0`
- job names include the image segment, such as `pytorch-26-05-py3`
- selected node is not cordoned
- selected node is expected by the operator

## Continuous Live Runner

The tmux live runner is explicitly audit-first. `CVAL_LIVE_MODE` is immutable
for the session and accepts only `audit` or `submit`; the default is `audit`.
Each audit cycle performs discovery, reads compatibility
`validation.db/latest_status`, prioritizes due nodes, renders jobs, selects up
to the configured slots, writes `audit-plan.json` plus
`audit-summary.json`, and then exits the cycle without submission, submitted-job
resume/monitoring, or pruning. Logs distinguish discovery/status/plan failures
from successful `no-free-nodes` and `no-due-candidates` states.

Normalized run history is optional and independently default-off. A missing
`node-run-history.db` does not affect live scheduling; the loop continues to use
the read-only compatibility `latest_status` view.

Start audit mode, inspect, attach, and stop:

```bash
CVAL_LIVE_MODE=audit CVAL_KUBECTL_TIMEOUT_SECONDS=120 scripts/cval-live.sh start
scripts/cval-live.sh status
scripts/cval-live.sh attach
scripts/cval-live.sh stop
```

Without an explicit `CVAL_GIT_REF`, every cycle fetches and resolves the current
`origin/main` to an exact 40-character commit. An explicit ref is resolved to a
full commit and is propagated only when set on the new `start` invocation.

Submit mode has an independent exact startup gate in addition to the CLI's
submission gate:

```bash
CVAL_LIVE_MODE=submit CVAL_LIVE_CONFIRM=submit \
  CVAL_KUBECTL_TIMEOUT_SECONDS=120 scripts/cval-live.sh start
```

Submit mode may resume and monitor jobs recorded by an earlier submit cycle. It
keeps at most the configured batch size active and rebuilds the ranked list for
each open slot. It still passes `--submit --confirm submit` to the CLI; mode
alone never authorizes a submission.

Pending-job pruning is disabled by default, including in submit mode. To retain
the legacy prefix-bounded pruning behavior, separately set the exact destructive
gate `CVAL_PRUNE_CONFIRM=delete-pending`. Audit mode ignores that value and never
deletes. Use a dedicated prefix and inspect matching jobs before enabling it.
Every direct script-level kubectl operation has both a process timeout and
`--request-timeout`, derived from `CVAL_KUBECTL_TIMEOUT_SECONDS`.

## One-Node Validation

1. Push code and capture the commit SHA.
2. Build a pinned dry-run plan:

   ```bash
   python -m cval.cli run \
     --live-status \
     --threshold-days 4 \
     --batch-size 1 \
     --git-ref <commit-sha> \
     --output json
   ```

3. Ask for operator approval.
4. Submit exactly one job:

   ```bash
   python -m cval.cli run \
     --free-nodes <node> \
     --live-status \
     --threshold-days 4 \
     --batch-size 1 \
     --git-ref <commit-sha> \
     --timestamp <timestamp> \
     --submit \
     --confirm submit \
     --output json
   ```

5. Monitor read-only:

   ```bash
   python -m cval.cli jobs \
     --jobs <job-name> \
     --watch \
     --timeout-seconds 1200 \
     --poll-interval-seconds 30 \
     --output json
   ```

6. Verify result JSON and DB rows:

   ```bash
   python -m cval.cli status --output json
   python -m cval.cli result --result-json <result-json>
   ```

## Validated Example

Pinned run:

```text
commit: c9a762a65bf9ae2989d71a01395d86dbc5c96af5
node: slc01-cl02-hgx-0204
job: cval-slc01-cl02-hgx-0204-pytorch-26-05-py3-1781134840
phase: Completed
result: storage=pass, nccl=pass, dltest=pass, overall=pass
```

## Baselines and Node Classification

### Local U9 evaluator preflight (not a live activation procedure)

U9 has no background service or Kubernetes manifest. First run only against a
local fixture or separately prepared PVC copy:

```bash
python -m cval.cli --config <local-copy-config.toml> \
  health evaluate --output json
python -m cval.cli --config <local-copy-config.toml> \
  health activate <test-id> <candidate-id> --output json
```

Confirm `mode=dry-run`, per-test schema/catalog results, candidate IDs, DNR
reasons, candidate source counts, classification selected/backlog/remaining/
truncation, migration state, candidate/history counts, and
`partial_durable_writes=false`. Confirm that no `.health-evaluator.lock`, health
DB/key, migration, history row, `-wal`, or `-shm` appeared and no source mtime
changed. A source must be checkpointed with
absent WAL/SHM/rollback-journal sidecars; dry-run fails closed rather than deleting sidecars.
On Linux, the evaluator uses `O_NOATIME|O_NOFOLLOW` and therefore must own the
source DB (or have equivalent privilege); it fails closed instead of accepting
an access-time mutation.
Before any apply, back up each U7 DB and each existing
U8 DB together with its owner-only `.activation.key`, enable only
`[health_evaluator].write_enabled`, and obtain explicit write approval. Apply
uses exact confirmations `evaluate` and `activate`. Never copy, print, or
restore an activation key separately. Live rollout/scheduling belongs to U11.
Apply preflights all source/plugin/build/classification reads before writing.
Immediately before history append, U9 revalidates the catalog through the
already-open U7 write transaction and gives adapters an in-memory projection of
that connection. A checkpointed WAL-mode U7 DB therefore does not need to be
reopened while the transaction's temporary WAL/SHM sidecars exist.
The report remains stage-aware if a later operation fails: each SQLite file is
transactional, but U7 migration/history and U8 candidate commits are explicitly
cross-database non-atomic and must be retried from immutable evidence.

All evaluator-owned DBs, keys, locks, and state live under the configured
dedicated state root. Never chmod/chown the shared `runtime.validation_root`.
Provisioning `/data/continuous_validation/evaluator_state` as UID/GID
`65532:65532` mode `0700`, deploying fixed-UID/GID U7 ingestion, creating live
DBs, or mounting/unsuspending the evaluator are still unapproved live steps.
Because the current validation workload identity is unspecified, enabling U7
before that fixed-identity ingestion path exists fails closed.

### U11 offline rollout and rollback preparation

Use only local/disposable copies until the live blockers are closed:

```bash
python -m cval.cli --config <local-copy-config.toml> \
  evaluator-preflight --state-root <copy-root> --access ro
python -m cval.cli evaluator-parity --u8-json <u8.json> \
  --compatibility-db <copied-classification-results.db>
python -m cval.cli evaluator-backup --source-root <copy-root> \
  --destination <new-backup-directory>
python -m cval.cli evaluator-backup --source-root <copy-root> \
  --destination <new-backup-directory> --apply --confirm backup
```

The local backup command requires both source and destination outside the
configured live shared and state roots, rejects symlink/traversal/overlap and SQLite
WAL/SHM/journal sidecars, and does not substitute for a reviewed live backup.
Its destination parent must already exist; dry-run creates no directories.
Kubernetes manifests remain
suspended and contain image/commit/PVC placeholders. The checked-in image
recipe is build-time offline (`--network=none`, local exact-version/hash-checked
wheelhouse), embeds the exact commit, and produces a distroless non-root final
stage; it is not a published or approved image. Build/sign/SBOM/provenance steps
and the full ancestry/owner/parity checks are documented in the rollout guide.
Do not apply, unsuspend,
or create a live Job until U7 availability, PVC owner/mode/sidecars, Kubernetes
and NetworkPolicy behavior, image digest/embedded commit/SBOM, backup approval,
shadow criteria, apply, cutover, and rollback commands are separately reviewed.
The detailed phase plan, risks, and non-destructive rollback are in
[U11 evaluator rollout preparation](u11-evaluator-rollout.md).

### U12A test lifecycle and compatibility evidence

Scaffold is dry-run-first and local. It neither edits global config nor creates
a plugin or health policy. Apply uses exact owner modes, same-parent complete
tree staging, fsync durability, and atomic no-overwrite publication; a failure
or race leaves no partial scaffold:

```bash
python -m cval.cli tests scaffold <id> --order <N> --output json
python -m cval.cli tests scaffold <id> --order <N> \
  --apply --confirm scaffold
```

Keep the printed stanza disabled through review and offline acceptance. Use
`compatibility inventory` for the source catalog and `compatibility audit` only
against explicitly copied regular files. Neither command may inspect the live
cluster/PVC or write state. Audit inputs must have no symlink ancestor, be
current-user-owned and not group/world writable; FIFO/device and unstable files
fail, while binary/decoding/unsupported formats are explicitly unscannable.
Treat `internal-current-protocol` observations as required supervisor/ingestion
controls, not legacy removal candidates. Every compatibility removal remains
blocked; do not delete wrappers, readers, DBs, logs, or historical artifacts. Follow
[Operator Test Lifecycle](test-lifecycle.md).

Build a baseline from recent results, review it, then promote it to active:

```bash
# Dry-run: print the robust per-metric baseline (no write)
python -m cval.cli baseline build --test-type storage --window-days 30

# Store as a candidate and review
python -m cval.cli baseline build --test-type storage --window-days 30 \
  --baseline-id storage-2026Q2 --store
python -m cval.cli baseline show storage-2026Q2 storage

# Promote to active (changes what "normal" means going forward)
python -m cval.cli baseline activate storage-2026Q2 storage
```

Classify nodes against the active baseline and act on degraded nodes:

```bash
python -m cval.cli baseline classify --test-type storage --output table
python -m cval.cli baseline classify --test-type nccl --node <node> --output json
python -m cval.cli baseline classify --test-type dltest --store-results --output json
python -m cval.cli baseline classify --test-type dltest-compute --store-results
python -m cval.cli classifications --test all --type csv
python -m cval.cli results --test dltest-compute --type csv
```

Classification itself is read-only against raw metric DBs. With `--store-results`,
the derived verdict is written to
`/data/continuous_validation/baselines/classification-results.db`; raw validation
`pass/fail/incomplete` rows remain untouched. A `degraded` verdict is a signal to
investigate, not an automated action; c-val does not cordon or drain nodes.

DL has component-level classifications: `dltest-numerical`, `dltest-compute`,
`dltest-collective`, and `dltest-overlap`. The periodic classifier stores all of
them by default. DL verdicts use configurable count/fraction/severity thresholds
so one noisy metric does not degrade an otherwise healthy node.

### Background Baseline Services

The scripts enumerate enabled registry targets by capability on every cycle;
they do not maintain storage/NCCL/DL enable variables. An environment value
cannot re-enable a disabled registry test. `CVAL_BASELINE_CLASSIFY_TESTS`, when
set, remains a strict allowlist over the catalog. DL aggregate/component targets
share one lock and one freshness/rebuild decision per cycle. Failures are
isolated per target and the cycle returns nonzero after all selected targets
have been attempted.

Run these where `/data/continuous_validation` is visible. The intended location
is the `gcr-admin` PVC access pod, in a tmux session:

```bash
# One-shot dry operational checks before enabling loops
scripts/cval-baseline-build.sh run-once
scripts/cval-baseline-classify.sh run-once

# Long-running tmux loops
scripts/cval-baseline-build.sh start
scripts/cval-baseline-classify.sh start

# Inspect/attach/stop
scripts/cval-baseline-build.sh status
scripts/cval-baseline-build.sh attach
scripts/cval-baseline-build.sh stop

scripts/cval-baseline-classify.sh status
scripts/cval-baseline-classify.sh attach
scripts/cval-baseline-classify.sh stop
```

To start them inside the PVC pod, first exec into the running access pod and run
the same commands from the c-val checkout. Do not paste credentials into docs or
commands; use the kubeconfig already present on the operator machine.

Code or local U10 test completion does **not** authorize restarting either live
loop. A pod checkout update and restart remain separate ship/deploy actions.
U10 also does not move compatibility readers from `metadata/`/`baselines/` to
U7/U8/U9 databases.

For DL tests, the scripts first rebuild the four DL metric DBs from rank JSON
files under `/data/continuous_validation/validation_tests/dltest/runs`. To run
that rebuild manually:

```bash
python -m cval.cli db-rebuild-dltest-metrics \
  --results-root /data/continuous_validation/validation_tests/dltest/runs \
  --output-dir /data/continuous_validation/metadata
```

## Cleanup

The `jobs --watch` command, audit-mode live loop, and `cval-live.sh stop` never
delete jobs. A submit-mode loop prunes matching stale `Pending` jobs only when
started with the separate exact `CVAL_PRUNE_CONFIRM=delete-pending` gate. Any
other cleanup requires explicit approval and an exact delete command for the
intended validation job.