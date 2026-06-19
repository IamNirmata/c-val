# c-val Command Cheatsheet

Quick reference for the `cval` CLI and operational scripts.
Full reference: [cli-reference.md](cli-reference.md) · [operations-runbook.md](operations-runbook.md) · [baselines.md](baselines.md)

---

## Invoke

```bash
# From the repo (no install): wrapper sets config + PYTHONPATH
bash scripts/cval.sh <args>

# Handy alias (add to ~/.bashrc)
alias cval='bash /home/v-srsreenath/hari/claw/c-val/scripts/cval.sh'

# Or directly
python -m cval.cli --config config/cval.toml <args>

# Non-default config
cval --config /path/to/cval.toml <args>
```

> Most commands accept `--output table|json` (default `table`). Add `--help` to any command.

---

## Observe (read-only)

```bash
cval overview                       # one screen: free nodes, freshness, queue, jobs
cval overview --watch --interval 5  # auto-refresh dashboard (Ctrl-C to stop)
cval overview --no-jobs             # skip Volcano job listing (faster)
cval overview --output json         # machine-readable

cval nodes                          # schedulable GPU nodes + free capacity ('*' = fully free)
cval status                         # latest validation status per node/test
cval status --output tsv            # legacy TSV
cval config                         # print effective TOML config (JSON)
```

---

## Results & exports

```bash
# Inspect one structured result JSON
cval result --result-json <cval-results-*.json>            # env lines
cval result --result-json <file> --output json

# Export latest per-node results to a local CSV (LA-time filename)
cval results --test overall  --type csv
cval results --test storage  --type csv
cval results --test nccl     --type csv
cval results --test dltest   --type csv
cval results --test dltest-compute --type csv
cval results --test overall  --type csv --output-dir exports
# -> cval_<test>_<YYYYMMDD_HHMMSS_TZ>.csv

# Export derived baseline health verdicts directly
cval classifications --test all --type csv
cval classifications --test storage --type csv
cval classifications --test dltest-compute --type csv
```

`results` keeps the raw pass/fail `result` column and adds baseline health
columns when available: `classification_status`, `n_degraded`,
`degraded_metric_percent`, and `worst_pct_diff`.

---

## Plan & submit

```bash
# Priority queue + reasons (never-tested / stale), live discovery
cval plan --live-status --threshold-days 7

# Dry-run a batch (DEFAULT: submits nothing)
cval run --live-status --threshold-days 7 --batch-size 1

# Plan specific nodes
cval run --free-nodes <node1>,<node2> --batch-size 1 --timestamp 12345

# REAL submission — double-gated
cval run --live-status --batch-size 1 \
  --git-ref <commit-or-tag> --submit --confirm submit
```

| Gate | Requirement |
|---|---|
| Confirm | `--submit --confirm submit` |
| Namespace | must be in policy allowlist |
| Batch size | must not exceed `max_batch_size` |

---

## Monitor jobs (read-only)

```bash
cval jobs --jobs <job-name>                       # phase once
cval jobs --jobs <job-name> --watch \
  --timeout-seconds 1200 --poll-interval-seconds 30
```

Phases: `Pending → Running → Completed` (or `Failed`/`Aborted`). `--watch` never deletes jobs.

---

## Baselines (robust statistical "normal")

```bash
# Build (dry-run prints metrics; --store=candidate; --activate=promote)
cval baseline build --test-type storage --activate
cval baseline build --test-type nccl    --window-days 30 --store
cval baseline build --test-type dltest  --test-plan 80gb-example --activate
cval baseline build --test-type storage --image-name pytorch:26.05-py3 \
  --baseline-id storage-2026Q2 --activate

# Manage
cval baseline list  [--test-type storage]
cval baseline show  <baseline-id> <nccl|storage|dltest>
cval baseline activate <baseline-id> <nccl|storage|dltest>
```

> Default storage: `/data/continuous_validation/baselines/*-baselines.db` (override with `--db-path`).
> Key flags: `--window-days`, `--min-samples`, `--node`, `--source-db`, `--baseline-id`.

---

## Classify nodes (degraded / normal / improved)

```bash
# All nodes vs active baseline
cval baseline classify --test-type storage
cval baseline classify --test-type nccl

# One node, against an explicit baseline, persist verdicts
cval baseline classify --test-type dltest --node <node> \
  --baseline-id <id> --store-results --output json

# DL components are first-class classification tests
cval baseline classify --test-type dltest-numerical --store-results
cval baseline classify --test-type dltest-compute --store-results
cval baseline classify --test-type dltest-collective --store-results
cval baseline classify --test-type dltest-overlap --store-results
```

`--store-results` writes to `/data/continuous_validation/baselines/classification-results.db`
(raw pass/fail in `validation.db` stays untouched).

DL verdicts use three config knobs to avoid false positives from a few noisy
metrics: `dl_degraded_metric_fraction`, `dl_min_degraded_metrics`, and
`dl_degraded_severity_pct`.

---

## DL metric DB maintenance

```bash
# Rebuild the 4 DL metric DBs from rank JSON (run where the PVC is mounted)
python -m cval.cli db-rebuild-dltest-metrics \
  --results-root /data/continuous_validation/dltest \
  --output-dir   /data/continuous_validation/metadata \
  --output json
```

Source of truth:
`/data/continuous_validation/dltest/<node>/dltest-<node>-<ts>/workdir/test_plans/<plan>/runs/*.json`

---

## Background services (tmux) — run on the PVC pod

```bash
# Continuous validation runner (discover → prioritize → submit → monitor)
scripts/cval-live.sh start | status | attach | stop | run-once

# Daily baseline builder (rebuilds DL DBs, builds + activates baselines)
scripts/cval-baseline-build.sh start | status | attach | stop | run-once

# Periodic classifier (refreshes DL DBs, classifies aggregate + DL components)
scripts/cval-baseline-classify.sh start | status | attach | stop | run-once
```

Local-test override (no PVC): `CVAL_BASELINE_ROOT=/tmp/cval-baselines scripts/cval-baseline-build.sh run-once`

---

## Git helper

```bash
./push.sh -m "message"     # stage all, commit, push current branch to origin
```

---

## Daily operator flow

```bash
cval overview                          # 1. fleet health at a glance
cval status | head                     # 2. fresh vs stale results
cval plan --live-status                # 3. who's queued and why
cval run  --live-status --batch-size 1 # 4. dry-run the next batch
# ...approve, then: --submit --confirm submit
cval baseline classify --test-type storage   # 5. find degraded nodes
cval classifications --test all --type csv   # 6. export health verdicts
cval results --test overall --type csv       # 7. export raw status + classification columns
```

---

## Safety reminders

- `run` is **dry-run by default**; real submit needs `--submit --confirm submit`.
- `status`/`jobs`/`overview`/`results` are **read-only**.
- Baseline `activate` redefines "normal" for future classification — promote deliberately.
- Run baseline/DL-rebuild commands **where `/data/continuous_validation` is mounted** (the `gcr-admin-pvc-access` pod), not on a dev box.
