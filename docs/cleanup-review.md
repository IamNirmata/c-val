# Framework Simplification Review

Review date: 2026-09-22. Baseline:
`7464e4fc55ec88691d015afdb3ec08ffa246f6c1`.

**Published and accepted on 2026-09-23:** batches A-B, planning finding 1,
ingestion finding 2, and the CLI settings cleanup shipped in
`1abde89ff3d615d0d064b33131542315dc4169c6` on internal `main`. One-node
acceptance passed before the live loop was restarted at that commit.

The 2026-09-23 settings cleanup also removes single-choice format switches,
legacy TSV, optional validation ZIP export and duplicate public CLI overrides
for config-owned targets, job settings and policy limits. The
[remaining control inventory](components_design/cli-design.md#mode-inventory)
records replacements. TOML settings, raw writers, canonical evidence and the
independent evaluator are retained.

**Implemented follow-up:** retire batch C's all-files publish wrapper and
consolidate documentation (finding 5), followed by shared bootstrap extraction
(finding 3). The bootstrap refactor still requires its own exact-commit cluster
acceptance before deployment. Finding 4 and compatibility-alias retirement remain deferred. Absence
from the continuous loop alone is not proof of non-use.

## Maintainability Re-audit

The A-B removals reduce stale material, not active framework complexity. The
next goal is fewer places to understand or change one behavior, not merely fewer
files. Findings 1-2 are implemented and accepted; finding 3 is implemented,
and finding 4 remains a proposal.
The earlier retain inventory protected responsibilities and evidence; it did
not mean every current implementation or abstraction should remain unchanged.

### 1. Separate Candidate Selection From Job Rendering

**Published and accepted in `1abde89`.** Previously the loop rendered
every due candidate before selecting an available node, then rendered its Job
again at submission. Preflight repeated that rendering as well.

Plain `plan` now returns ordered candidates with an exact commit and counts,
using the existing pure priority function. Both
[live-loop](../scripts/cval-live.sh) inspection paths use this single default;
only confirmed submission invokes the [Job planner](../cval/orchestrator/workflow.py).
The unshipped `--candidates-only` flag and old `--include-yaml` mode are removed,
along with rendering-only plan arguments and the unused rendered-plan serializer.
No internal render toggle, new command, scheduler or configuration was added.

Local regressions verify zero renders during candidate inspection, identical
priority order, one render for selected-node submission, and failure before
creation for invalid templates. Existing preflight, fresh eligibility, cooldown,
resume, pruning and worktree/source guards remain. Malformed candidate JSON and
a returned commit differing from the requested commit fail closed. See the
[planning contract](operations-runbook.md#candidate-inspection) for the default
candidate JSON shape and the [CLI inventory](components_design/cli-design.md#mode-inventory)
for retained controls. Callers must migrate from `planned_jobs`
to `candidates`; this is an intentional planning-interface change.

### 2. Give Ingestion One Orchestration Path

**Published and accepted in `1abde89`.** The former 537-line
[db-update.sh](../validation-tests/db-update.sh) is now a six-line launcher for
[one Python coordinator](../cval/storage/run_ingest.py). JSON-to-shell projection,
argument reconstruction and separate writer CLI processes are gone. The existing
raw writers and schemas are unchanged; no generic DB or new plugin layer was added.

The coordinator authorizes the result/config snapshot, binds the immutable
digest, and preserves storage -> NCCL -> locked DL -> atomic status ordering.
The provenance owner now owns digest binding; each low-level writer still
revalidates its authorization. The existing POSIX lock helper exposes its secure
acquisition checks for direct use, while its command wrapper retains child
cleanup behavior. Events and SQLite retries use the existing implementations.

Local tests verify seven actual temporary stores, unchanged receipts and rows
after a lock retry, suppressed status on missing evidence, failure/interruption
events, released locks and descriptor-bound supervisor ingestion. Historical
structured v1/v2 readers remain; environment-only results cannot authorize writes.
Individual CLI writer/rebuild hooks remain compatibility/maintenance interfaces,
not part of normal orchestration. Exact-commit cluster acceptance also verified
canonical evidence and matching rows in all seven raw databases; see below.

### 3. Maintain One Job Bootstrap Program

**Implemented; exact-commit cluster acceptance pending.** Both Job
templates now contain one `runtime-bootstrap-placeholder` in the server's Bash
args block. The [renderer](../cval/jobs/renderer.py) embeds the packaged
[shared shell payload](../cval/jobs/bootstrap.sh) before its existing semantic,
duplicate-key and executable-order validation. No template engine, CLI mode,
configuration switch or extra pod dependency was added.

The rendered Job remains self-contained: it does not need to fetch source before
obtaining its bootstrap. Node-specific placement and scheduler-selected placement
remain separate and unchanged, as do resource and environment fields. Valid
existing inline bootstrap templates still pass the same safety checks; malformed,
mixed or duplicate shared markers are rejected.

The 38 focused renderer/workflow/job-manager tests pass, including both source
transports, archive commit/hash rejection, shell syntax and hostile-template
regressions. A structured comparison with `1abde89` verified unchanged placement,
resources and environment, and identical executable bootstrap lines. This local
verification does not replace exact-commit cluster acceptance for the refactor.

### 4. Share DL Transaction Bookkeeping

**Deferred proposal; not implemented.**
[write_standard_db](../cval/storage/dltest_ingest.py#L738) and
[write_overlap_db](../cval/storage/dltest_ingest.py#L804) occupy 64 and 67 lines.
They repeat receipt setup, scoped replacement, insertion, generation writes and
commit handling; their columns and indexes genuinely differ.

Extract only common transaction mechanics into a private helper, leaving fixed
schemas/columns explicit. Keep the existing databases and public writer APIs;
do not replace them with a configurable database framework or combine the four
stores. Moving historical rebuild code to another file alone is not a reduction.

Gate: unchanged rows, schemas, receipts and idempotency;
[generation stays incomplete until finalization](../tests/test_dltest_ingest.py#L1089)
and divergent receipts still block publication. Retain symlink/identity checks.

### 5. Give Current Behavior One Documentation Home

**Completed.** The
[documentation map](architecture.md#documentation-map) assigns current runtime,
configuration, operations, evidence and evaluator contracts to existing references.
No additional design document was created.

The evaluator design note now contains rationale and deferred decisions, with
links to its operating procedure and schema reference instead of duplicated
deployment instructions and fictional schema examples. The database note links
filesystem permissions to the current Pod contract instead of claiming a
read-only raw mount.

The runbook owns the candidate JSON contract. The CLI note retains the mode
inventory and migration guidance, but replaces its stale local-only roadmap and
speculative line-count targets with explicitly unapproved interface decisions.
The published `1abde89` acceptance is recorded separately from these later
follow-ups. Dated rollout evidence is not a current-state
claim. Local link/reference checks and the customization validator cover this
documentation-only change; retained operational records are not rewritten.

## Simple Ownership Model

Keep the existing responsibilities; simplify the hand-offs, not the safety model:

| Owner | Responsibility |
| --- | --- |
| Live controller and existing scheduler/job APIs | Exact source, candidate priority, fresh eligibility, bounded slots, submission/resume state |
| Supervisor and registered test runner | Descriptor-owned paths, test setup/execution, deadlines, canonical results and events |
| Test-owned raw writers | Validated metrics first, final status last, unchanged raw schemas and history |
| Independent consumers and maintenance | Read-only audit/export, separate evaluator, backup and recovery outside the per-cycle path |

No new orchestration framework, generic persistence layer, or wholesale directory
reorganization is proposed. A refactor should remove a duplicate implementation
or conversion path, not just rename, relocate or compress it.

## File Cleanup Status

| Batch | Status | Baseline reduction |
| --- | --- | --- |
| A: retired evaluation prototypes | Published in `1abde89` | 3 files, 554 text lines, 23,924 bytes |
| B: historical planning bundle | Published in `1abde89` | 7 files, 427 text lines plus PNG, 147,067 bytes |
| C: all-files publish wrapper | Approved and removed in this follow-up | 1 file, 61 lines, 1,422 bytes |
| D: shell compatibility aliases | Deferred unless explicitly retired | 4 files, only 26 lines total |

A-B total: 10 files, 981 text lines plus one PNG, 170,991 bytes. Gross source
removals exclude documentation edits. Exact removed paths remain recoverable at
the baseline commit; no duplicate archive was created:

```text
docs/evals/README.md
docs/evals/classify.py
docs/evals/threshold.py
docs/project_managemnt_docs/README.md
docs/project_managemnt_docs/cval-components.md
docs/project_managemnt_docs/cval-versions.md
docs/project_managemnt_docs/cval2.0-audited.md
docs/project_managemnt_docs/cval2.0-initial-scope.md
docs/project_managemnt_docs/cval3.0.md
docs/project_managemnt_docs/image.png
```

C retired the all-files publish wrapper: it staged everything and pushed the
current branch to origin, not an isolated reviewed change to an explicitly
chosen target. The workspace shipping skill now requires explicit reviewed
paths, staged-diff inspection and destination verification. No replacement
wrapper was added. C is safety cleanup, not the main runtime simplification.

D's exact paths remain:

- [validation-tests/run-test.sh](../validation-tests/run-test.sh)
- [validation-tests/storage/storage.sh](../validation-tests/storage/storage.sh)
- [validation-tests/nccl/run-nccl-allreduce.sh](../validation-tests/nccl/run-nccl-allreduce.sh)
- [validation-tests/dltest/dltest.sh](../validation-tests/dltest/dltest.sh)

These are tested operator interfaces, not unreferenced code. The supervisor
already bypasses the top-level alias, and descriptors select canonical per-test
runners. Removal needs an external-usage decision and retained behavior coverage.

## Keep These Guarantees

- Retain exact-commit/mutation gates, cooldown/resume records, bounded commands,
	descriptor/path/symlink protections, immutable result/config checks, locking,
	status-last publication, and historical result/path readers. Reuse their code
	where justified; do not remove checks solely to lower line counts.
- Keep registered tests/plugins, their regression suites, backup/layout/journal
	recovery, and the supported evaluator. Their independent entry points or
	dynamic loading do not make them dead files. Splitting services/packages is
	a separate product decision, not necessary to obtain the reductions above.
- Protect raw DBs, PVC contents, backups, run logs, active source/runner worktrees,
	governance and user-owned untracked files. Conversation archives and the
	protected `dl_unit_test_evaluation` directory were not read.

## Verification and Remaining Scope

The runtime changes were accepted on `slc01-cl02-hgx-0250`, run timestamp
`1790132796`, using the published exact commit. The Job completed with exit 0
and no container restarts. Storage, NCCL and DL passed; ingestion finished with
`pass` in 37.395 seconds. The audit verified 38 canonical evidence files,
result/config digests, four passing status rows, one storage row, one NCCL row,
and 7,128 DL metrics across eight ranks with complete generations and receipts.

The live loop restarted at `2026-09-23T03:35:34Z`. Its first cycle verified
candidate-only plans and checked 282 due candidates, all ineligible. The
`03:41:06Z` observation showed a running, idle loop; two fresh automated runs
were still unverified for lack of eligible capacity. This is dated rollout
evidence, not a claim about the current live state. The detailed acceptance
and restart record is retained locally under the release run logs.

The documentation/wrapper follow-up uses link/reference checks and the workspace
customization validator. The later approved bootstrap extraction has focused
renderer/caller regressions; no live mutation was performed for either change.
Publication alone does not deploy the shared bootstrap. Its exact published
commit still needs separately approved cluster acceptance before rollout.
DL transaction consolidation remains a proposal, and tested shell aliases remain
supported. A future runtime refactor still needs its own approval and acceptance.
Source cleanup never authorizes live deletion or evidence removal.