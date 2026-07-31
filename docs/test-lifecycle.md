# Operator test lifecycle

This guide is the U12A contract for adding, updating, disabling, and eventually removing a validation test. U12A is local-only. U11 live acceptance and the agreed compatibility period are still open blockers, so no compatibility producer, consumer, wrapper, CLI, database reader, default registration, DL alias, or historical reader may be removed.

## Safety and approval matrix

| Change | Local preparation | Required review/approval before live use |
|---|---|---|
| Scaffold a disabled pass/fail test | Dry-run by default; exact `--apply --confirm scaffold` creates only a new directory | Normal code review; no workload runs while disabled |
| Update test implementation or descriptor | Validate registry, shell, unit, compile, and offline render | Review resource, timeout, evidence, and backward-compatibility changes |
| Enable a new test | Change only its global `enabled` value after all local evidence passes | Explicit workload/cluster approval; first live submission uses existing c-val submission gates |
| Add `ingest`, `health`, `baseline`, or `export` capability | Separate plugin/schema/policy change | Database backup/dry-run and surface-specific approval; U7/U8 gates remain independent |
| Disable a test | Set `enabled = false`; retain descriptor, artifacts, readers, and history | Operational review because coverage changes |
| Remove registration/code | Not authorized in U12A | U11 live acceptance, completed compatibility period, clean copied-input audit, owner approval, rollback plan |
| Delete historical artifacts or databases | Forbidden | Separate destructive-data approval; ordinary removal never deletes history |

Disabling is the safe rollback. Removing a registration is not a data-retention operation: historical `cval.results.v1`, `cval.results.v2`, run-history, U7, compatibility DB, baseline, export, and DL artifact readers remain able to read retained evidence.

## Add a pass/fail-only test

1. Preview the exact files and disabled stanza. This writes nothing:

   ```bash
   python -m cval.cli tests scaffold smoke --order 40 --output json
   ```

2. Create the new directory only after review:

   ```bash
   python -m cval.cli tests scaffold smoke --order 40 --apply --confirm scaffold
   ```

   The command refuses unsafe IDs, negative/oversized orders, collisions with
   explicit or compatibility-default registrations, every symlinked lexical
   ancestor, and every existing/racing target. Apply stages the complete tree
   under the retained `validation-tests/` descriptor, fsyncs every file and
   directory, and publishes once with atomic no-replace rename. Any failure
   removes the unpublished tree. Directories are exactly `0700`, shell files
   `0755`, and descriptor/documentation files `0600`, independent of umask. It
   never edits global config and never creates a plugin or health policy.

3. Implement deterministic checks in `validation-tests/smoke/tests/test.sh`. The scaffold is deliberately fail-closed. It creates:

   - `README.md`
   - `test_config.toml`
   - `setup.sh`
   - `run-test.sh`
   - `tests/README.md`
   - `tests/test.sh`

   The setup and workload share the descriptor deadline. The workload returns zero for pass and non-zero for fail and writes its canonical summary to `CVAL_TEST_SUMMARY_FILE`.

4. Copy the printed stanza into `config/cval.toml` without changing `enabled = false`:

   ```toml
   [tests.smoke]
   enabled = false
   config_path = "validation-tests/smoke/test_config.toml"
   ```

5. Run local acceptance:

   ```bash
   python -m cval.cli tests validate --output json
   python -m cval.cli plan --free-nodes <node> --db-status-json <offline-status.json> --output json
   find scripts validation-tests -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
   PYTHONWARNINGS=error python -m unittest discover -s tests -p 'test_*.py'
   python -m compileall -q cval tests skills/c-val-hpc-engineer/scripts
   ```

6. Review resource requirements, order uniqueness among enabled tests, timeout coverage, logs, `cval.event.v1`, `cval.results.v2`, run history, and U7 raw persistence. A pass/fail-only test has no classification or export target.

   Runtime directory creation is registry driven. The Python supervisor reserves
   only enabled-test run directories through retained descriptors; adding a
   fourth test requires no shell `mkdir`, redirection, or template path list.

7. Enable it only in a separately reviewed change. Enabling does not authorize a live job; normal `run --submit --confirm submit` or targeted-validation approval still applies.

## Add capabilities later

Pass/fail is the minimum lifecycle. Add `plugin.py` and `[plugin]` only when the test needs one or more explicit capabilities:

- `config` — adapter-owned descriptor validation.
- `ingest` — test metrics in its U7 database; `per_test_ingestion_enabled` remains an independent default-off write gate.
- `health` — U8 observations and policy; candidate construction and persistence remain framework-owned.
- `baseline` — established compatibility baseline lifecycle and classification target.
- `export` — established compatibility results export target.

An adapter that imports test-local helper files must declare each confined
regular path explicitly in `plugin.support_files`. Evaluator catalog assembly
copies only the descriptor, adapter, declared support files, and the descriptor
setup/entrypoint needed to validate the installed registry. The final evaluator
image makes all catalog files read-only; the setup and entrypoint are present
for descriptor validation only and are never evaluator workload entrypoints.
Catalog publication validates both the staged snapshot and the installed paths.

A health-capable test also needs a versioned `[health]` policy and a health DB path. Keep `auto_activate = false`. Do not combine first enablement with live database activation, baseline activation, or U11 rollout.

## Update an existing test

1. Keep the test ID and historical artifact paths stable.
2. Change the descriptor, scripts, tests, and docs together.
3. Increasing resource requests must stay within the shared job reservation; increasing timeout must keep the sequential timeout plus ingestion grace below monitoring timeout.
4. A metric/schema change is additive and must preserve exact older readers. A health-policy change gets a new `policy_version` and does not rewrite prior candidates/history.
5. Verify built-in byte parity where a compatibility producer is touched.
6. Deploy or restart nothing without separate approval.

## Disable and rollback

Set only the registration's `enabled` value to false. Keep its descriptor loadable so immutable snapshots and historical tooling retain meaning. Disabled tests remain in v2 as `enabled=false`, `selected=false`, `status=incomplete`, and `phase=not_selected`; they do not run or ingest.

Do not use loop environment variables to re-enable a disabled registration. Do not delete its run directories, result DB, health DB/key pair, compatibility metadata, baselines, logs, or exports.

Rollback for a newly enabled test is therefore:

1. Disable the registration.
2. Validate the composed registry and offline render.
3. Review active jobs separately; do not delete them automatically.
4. Preserve all generated evidence.

## Removal preflight — blocked in U12A

The inventory is source-only and read-only:

```bash
python -m cval.cli compatibility inventory --output json
```

The report separates `compatibility-legacy` surfaces from the
`internal-current-protocol` names used by descriptor-anchored supervision and
canonical ingestion path guards. Internal protocol observations are not legacy
removal candidates. Historical readers are likewise retained permanently;
other legacy surfaces remain blocked as described below. The immutable legacy
CLI/DB surface includes every hidden `db-add-*` compatibility writer, the
hidden `db-rebuild-dltest-metrics` maintenance hook, and the exact configured
`validation.db`, storage, NCCL, and four `dltest_*` metric database filenames.
Catalog tests derive the hidden compatibility command set from the CLI parser
and the DL filenames from `StorageConfig`, then require exact equality.

Inventory/audit use a dedicated pre-parser before global configuration loading
or adapter validation. A broken or unavailable operator config therefore cannot
cause these explicit-input lifecycle tools to read the repository or import a
plugin.

Audit only explicit local copied files; the command does not discover directories, contact Kubernetes/PVC/network services, or write output/state:

```bash
python -m cval.cli compatibility audit \
  --input /copy/job.log \
  --input /copy/pinned-manifest.yaml \
  --output json
```

The audit accepts at most 64 current-user-owned regular inputs, 8 MiB each, and
32 MiB total. It opens lexical ancestors with
`O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC|O_NONBLOCK`; those descriptors are used only
for `openat` traversal and `fstat` identity checks, never directory-entry
reads, so directory atime is unchanged. The leaf evidence file additionally
requires `O_NOATIME|O_NONBLOCK|O_NOFOLLOW|O_CLOEXEC`. The audit rejects
group/world-writable inputs and never retries the leaf read without
`O_NOATIME` when the filesystem or caller cannot provide
metadata-side-effect-free access. It requires unchanged
device/inode/size/mtime/ctime/owner/mode around the bounded read, then reopens
the complete lexical parent chain and requires it to still identify the
retained parent descriptor. FIFO/device inputs fail without blocking. Tests
always certify unchanged inode/size/mtime/ctime and certify unchanged atime
only after the local filesystem passes an `O_NOATIME` open/`fstat` repeated-read
capability probe; mocked permission-denial coverage independently proves there
is no fallback open. Matching uses complete token boundaries, treats `/` and
`\\` path separators as boundaries, preserves embedded-identifier near-miss
rejection, and parses JSON/JSONL/TOML before matching; binary, malformed,
decoding-invalid, and unsupported inputs are reported as `unscannable` and
never treated as absence evidence. Every report remains
`removal_eligible=false` while U11 live acceptance or the compatibility period
is open. An observed token adds `observed-explicit-input`. Historical-reader
surfaces are permanently blocked by `historical-reader-retained`.

Command and path matching is exact: copied inputs containing
`db-rebuild-dltest-metrics` or one of the four complete DL metric DB filenames
are positive observations, while embedded prefixes, suffixes, backup names,
and SQLite sidecar-style near misses are not.

A future removal proposal must include copied-input evidence, owner inventory, no pinned producer/consumer use, U11 acceptance, completed compatibility period, a non-destructive rollback, and explicit approval. It may remove code only; historical data deletion is a separate forbidden-by-default operation.
