# c-val HPC Engineer

Use this skill when operating or improving c-val continuous validation for GPU clusters through Hermes Agent.

## When to Use

- Discovering free GPU nodes.
- Reading latest validation status.
- Building a dry-run validation plan.
- Reviewing rendered validation jobs before submission.
- Monitoring Volcano validation jobs.
- Summarizing validation history and result artifacts.
- Improving c-val orchestration, result ingestion, safety policy, or reporting.

## Safety Rules

Start with read-only commands:

```bash
python -m cval.cli status --output table
python -m cval.cli nodes --output table
python -m cval.cli run --live-status --threshold-days 4 --batch-size 3 --output json
```

Do not run real submission unless the operator explicitly approves it. Real submission requires:

```bash
python -m cval.cli run --live-status --threshold-days 4 --batch-size 1 --submit --confirm submit
```

Never run destructive cluster commands without explicit approval. This includes `kubectl delete`, `kubectl drain`, `kubectl cordon`, `kubectl taint`, `kubectl patch node`, `kubectl scale`, PVC/log/database deletion, driver restarts, kubelet/containerd restarts, and cluster-wide RBAC changes.

## Setup Checks

Run these from the c-val repo root:

```bash
python -m cval.cli status --output table
python -m cval.cli nodes --output table
python -m unittest discover -s tests -p 'test_*.py'
```

If `kubectl` authentication fails, stop and ask the operator to refresh credentials directly on the machine. Do not ask for or print tokens.

## Dry-Run Validation Workflow

1. Read latest DB status:

   ```bash
   python -m cval.cli status --output table
   ```

2. Discover fully free GPU nodes:

   ```bash
   python -m cval.cli nodes --output table
   ```

3. Build a dry-run plan:

   ```bash
   python -m cval.cli run --live-status --threshold-days 4 --batch-size 3 --output json
   ```

4. Preview submit actions without creating Kubernetes resources:

   ```bash
   python -m cval.cli run --live-status --threshold-days 4 --batch-size 3 --output json
   ```

## Batch Execution Workflow

Use only after explicit operator approval.

1. Reduce blast radius: use `--batch-size 1` for the first run.
2. Keep namespace scoped to `gcr-admin` unless the operator gives another namespace.
3. Submit with the confirmation phrase:

   ```bash
   python -m cval.cli run --live-status --threshold-days 4 --batch-size 1 --submit --confirm submit
   ```

4. Monitor job phases with read-only polling:

   ```bash
   python -m cval.cli jobs --jobs <job-name> --watch --timeout-seconds 180 --poll-interval-seconds 30 --output json
   ```

## Log Collection Workflow

- Prefer global results/logs under
   `/data/continuous_validation/logs/job_logs/<node>/<run-id>/` and per-test
   evidence under `logs/<test-id>/<node>/<run-id>/` plus
   `validation_tests/<test-id>/runs/<node>/<run-id>/`.
- Use `python -m cval.cli result --result-json <result.json>` to inspect structured result status.
- Use the test-specific artifact paths from result JSON for logs and summaries.
- Job startup is owned by the descriptor-anchored Python supervisor. It retains
   validation-root/run directory fds, reserves owner-only evidence atomically,
   and passes `/proc/self/fd/<fd>` paths to the generic runner and compatibility
   ingestion while keeping canonical `/data/...` paths in result JSON. Do not
   reintroduce shell `mkdir`, redirection, or `tee` for canonical run evidence.

## Failure Triage Workflow

1. Check job phase with `jobs` or `jobs --watch`.
2. If the job completed, inspect structured result JSON first.
3. If a test failed, inspect that test's log and summary path from JSON.
4. Do not assume Kubernetes `Ready` means workload health is good.
5. Record whether the failure is storage, NCCL, DL correctness, scheduling, image/bootstrap, or DB ingestion.

## Baseline and Outlier Classification Workflow

The commands in this section operate the existing compatibility baseline DBs
under `baselines/`. U9 now exposes local dry-run `health evaluate` and
`health activate` over U8, but they are not wired to compatibility loops or a
live service. Do not create, migrate, activate, or assume live
`validation_tests/<test>/<test>_health_classes.db` files. Apply requires the
independent evaluator gate and exact confirmation; live rollout is U11.
Dry-run requires checkpointed canonical copies with absent WAL/SHM/journal sidecars;
it reads one in-memory snapshot shared with adapters and never deletes or
creates source sidecars. Review candidate-source completeness plus
classification selected/backlog/remaining/truncation, migration,
candidate/history counts, and partial durable writes. Routine catalogs use
bounded result keyset pages and indexed exact-target history batches; a separate
streamed joined audit handles full history integrity. On apply errors, inspect
the reported stage and partial-write facts: U7 migration/history and U8
candidate transactions are individually atomic but cross-database commits are
not. History precommit revalidation uses the already-open U7 write transaction
for its catalog and an in-memory connection projection for adapter evidence; it
does not reopen a WAL source after `BEGIN IMMEDIATE`.
Eligible candidate rebuilds use the selected-result guard's active U7
transaction projection for both the complete catalog and adapter observations.
Review the separate deferred count and bounded reasons: deferred rows remain in
`classification_remaining` even after all actionable history is stored. U7/U8
writers reject DB, activation-key, and evaluator-lock path replacement at
transaction-open, precommit, postcommit, and exact-retry boundaries. U7
ingestion, U9 apply/activation, and backup apply share one descriptor-relative
per-test lock and retain exact state-root/ancestry/parent/file bindings. The
held lock is a callable identity guard; its canonical inode, fixed UID/GID,
owner-only `0600` mode, and single-link state must remain valid for the complete
operation. Initial U8 database/key staging and publication stay relative to the
retained parent; existing U8 uses its captured file identity. Missing ancestry
is also bound, and appearance of its first missing component or target fails.
Persistent evaluator/backup creators defer only main-thread `SIGINT` and
`SIGTERM` through the create syscall and immediate no-follow open/`fstat`
identity registration, restore handlers, then re-deliver the first pending
signal. Cleanup is exact device/inode-scoped and preserves racers; a currently
named replacement is never adopted. U8 write helpers are internal, and
evaluator persistence requires retained DB/key bindings plus this shared lock.
Immutable U11 snapshots and key reads use retained descriptors with private
`pread` semantics, so parent substitution cannot redirect a read.

```bash
python -m cval.cli health evaluate --output json
python -m cval.cli health activate <test-id> <candidate-id> --output json
```

U11 local preparation adds hidden strict-JSON `evaluator-preflight`,
`evaluator-parity`, `evaluator-backup`, and `evaluator-service` entry points plus
suspended Kustomize shadow/apply variants under `deploy/cval-evaluator/`.
Preflight descriptor-traverses foreign outer ancestors, then validates the
fixed-owner state root/exact descendant `0700` and `0600` files, revalidates
identities after reads, and enforces registered U7 row/adapter/receipt owners.
Parity requires exact JSON/SQLite identity, class, DNR, baseline, and timestamp
types and checks U7 ownership even without history. Both accept only
local/copied inputs. Backup
rejects equal, ancestor, or descendant overlap with both configured live
shared/state roots (siblings are allowed), is dry-run by default,
and requires `--apply --confirm backup` for disposable copies. The
destination's no-follow ancestry is retained continuously from canonical
validation through exclusive reservation, restore validation, and cleanup.
ServiceAccount has no bindings/token and deny-all network policy is required.
The checked-in offline, hash-bound, distroless image recipe packages the commit
marker, config, and descriptors/plugins, but base/image digests, embedded-commit
manifest value, and PVC values are fail-closed placeholders; never apply or
unsuspend them. U11 remains blocked on live U7 availability, PVC ownership and
sidecars, real Kubernetes/CNI/admission facts, a verified image digest/commit/
SBOM, approved live backup/restore, accepted shadow evidence, and explicit
apply/cutover/rollback approval. Follow `docs/u11-evaluator-rollout.md`.
The evaluator mounts only the pre-existing state subPath. Never chmod/chown the
shared evidence root. State provisioning and fixed-UID/GID U7 ingestion remain
unapproved; current validation workload identity is unspecified.

- Start with `status` to identify stale or missing results.
- Resolve compatibility targets from the enabled registry catalog. The
   `baseline` plugin capability controls build/lifecycle/classification choices;
   `export` controls result-export choices. The built-in DL component aliases
   are one overlay owned by enabled `dltest`, not separate registered tests.
- Build a baseline from recent results (dry-run prints the robust metrics;
  `--store` saves a candidate, `--activate` promotes it):

  ```bash
  python -m cval.cli baseline build --test-type nccl --window-days 30 --output json
  python -m cval.cli baseline build --test-type storage --image-name <image> --store
  ```

- Classify nodes against the active baseline and act on `degraded` nodes:

  ```bash
  python -m cval.cli baseline classify --test-type nccl --output json
  python -m cval.cli baseline classify --test-type storage --node <node> --output json
   python -m cval.cli baseline classify --test-type dltest --store-results --output json
  ```

- For continuous operation, use the tmux-managed loops in the environment that
   can see `/data/continuous_validation`:

   ```bash
   scripts/cval-baseline-build.sh start
   scripts/cval-baseline-classify.sh start
   scripts/cval-baseline-build.sh status
   scripts/cval-baseline-classify.sh status
   ```

   The loops enumerate targets each cycle. `CVAL_BASELINE_CLASSIFY_TESTS` is an
   allowlist and cannot re-enable disabled tests; empty/malformed enumeration or
   an empty allowlist intersection fails the cycle. DL targets take one shared lock
   and refresh once per group. The helper opens the existing canonical baseline
   directory with `O_DIRECTORY|O_NOFOLLOW`, requires effective-user ownership and
   no group/other write permission, flocks that stable directory inode while
   supervising the child, and revalidates pathname/device/inode before, during,
   and after child execution. The configured child lock pathname is only a
   compatibility marker and cannot split locking when replaced. Any validation/
   acquisition failure stops DL work. One target failure does not skip later targets, but the
   cycle returns nonzero. Never infer a live restart from a local U10 change.

- Promote a new baseline to active only with operator awareness; it redefines
  what "normal" means for future classification.
- Classification reads raw metric DBs and never cordons or drains nodes; with
   `--store-results` it writes derived decisions to
   `/data/continuous_validation/baselines/classification-results.db`.
- Prefer the c-val baseline modules (robust median/MAD logic) over ad hoc shell
  parsing of the metric DBs.
- Keep built-in compatibility readers on the current `metadata/` DBs and
   compatibility outputs under `baselines/`; U10 is not a U7/U8/U9 cutover.

## Verification Steps

Before and after code changes, run:

```bash
find scripts validation-tests -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q cval tests
```

## Known Pitfalls

- The repository is named `c-val`, but the importable package is `cval`; do not treat this as a duplicate checkout.
- Runtime jobs clone c-val and check out the explicit `CVAL_GIT_REF`; deploy
   pinned commit IDs rather than a moving branch.
- `run` dry-run does not submit resources. Real submission requires both `--submit` and `--confirm submit`.
- `jobs --watch` is read-only and does not cancel timed-out jobs.
- New result JSON uses dynamic `cval.results.v2` and structured
   `cval.event.v1`; historical v1 remains readable. The env file is only a
   fixed storage/NCCL/DL ingestion compatibility projection. U7 canonical
   per-test adapter writes are implemented but remain independently disabled by
   default until live dual-write approval.
- Never assume `validation_tests/<test>/<test>_results.db` exists live. The
   parent framework owns adapter SQL transactions/receipts; repository adapters
   run through spawned SQL RPC and must never receive the parent's raw SQLite
   connection.

## U12A test lifecycle and compatibility audit

Preview a pass/fail-only test with `cval tests scaffold <id> --order N`.
Creation requires exact `--apply --confirm scaffold`, creates only a new test
directory, and prints a disabled stanza; it does not edit global config or add
plugin/health behavior. Apply is no-follow, same-parent staged, exact-mode,
fsynced, atomic-no-replace, and rollback-on-failure. Follow
`docs/test-lifecycle.md` before enabling.

`cval compatibility inventory` reads the immutable source catalog.
`cval compatibility audit --input <copied-file>` reads only explicitly named
local regular files under fixed bounds. It performs no Kubernetes/PVC/network
discovery and writes nothing. Inputs require safe lexical ancestors/current
ownership/mode and stable identity; binary/decoding/unsupported inputs are
unscannable rather than absence evidence. Path separators bound catalog tokens
without allowing embedded identifier near misses. The report separates current
supervisor/canonical-ingestion protocol names from legacy compatibility
surfaces; current protocol is not a cleanup candidate. Treat every removal as
blocked until U11 live acceptance and the compatibility period close; never
delete historical data.