# Hermes Integration

Hermes should operate c-val through the same CLI that humans use. The agent is
an orchestrator and summarizer; deterministic validation remains in c-val jobs
and scripts.

## Safe Operating Model

Use this progression:

1. Read state with `status`, `nodes`, and `jobs`.
2. Select one eligible, fully free node and an exact published commit.
3. Submit one real validation with explicit operator approval using
	`validate --submit --confirm submit`.
4. Summarize structured result JSON and exact-timestamp raw DB rows after it finishes.

## Commands Hermes Should Prefer

```bash
python -m cval.cli status --output json
python -m cval.cli nodes --output json
python -m cval.cli validate --node <node> --git-ref <40-hex-commit> --submit --confirm submit
python -m cval.cli jobs --jobs <job-name> --output json
python -m cval.cli result --result-json <result-json>
```

## Baselines and Node Classification

Hermes can build storage/DL baselines and classify nodes. NCCL uses the
separate PostgreSQL evaluator and its exact apply confirmations.

```bash
python -m cval.cli baseline build --test-type storage --window-days 30 --output json
python -m cval.cli baseline classify --test-type storage --output json
python -m cval.cli baseline list --output json
python -m cval.cli nccl-eval status --output json
```

Promote a baseline to active only with operator awareness, since it changes what
"normal" means for future classification:

```bash
python -m cval.cli baseline build --test-type storage --baseline-id <id> --activate
```

## Guardrails

- Do not hard-code kubeconfigs, tokens, cluster names, or private paths.
- Do not run destructive Kubernetes commands automatically.
- Do not delete timed-out jobs as part of monitoring.
- Require an exact lowercase 40-hex commit for `--git-ref`.
- Keep generated logs, result JSON, and SQLite metadata intact for auditability.

## Local Setup Notes

Host-specific Hermes installation notes, kubeconfig paths, and workstation facts
belong outside this repository. Keep this repo focused on portable c-val code,
docs, and the `skills/c-val-hpc-engineer` skill package.