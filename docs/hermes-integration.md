# Hermes Integration

Hermes should operate c-val through the same CLI that humans use. The agent is
an orchestrator and summarizer; deterministic validation remains in c-val jobs
and scripts.

## Safe Operating Model

Use this progression:

1. Read state with `status`, `discover-free-nodes`, `job-status`, and
   `monitor-jobs`.
2. Build plans with `plan` or `submit-plan` in dry-run mode.
3. Review the target namespace, nodes, batch size, and `CVAL_GIT_REF`.
4. Submit only with explicit operator approval using `--submit --confirm submit`.
5. Summarize structured result JSON and DB rows after jobs finish.

## Commands Hermes Should Prefer

```bash
python -m cval.cli status --output json
python -m cval.cli discover-free-nodes --output json
python -m cval.cli submit-plan --live-status --threshold-days 4 --batch-size 1 --output json
python -m cval.cli monitor-jobs --jobs <job-name> --output json
python -m cval.cli result-env --result-json <result-json>
```

## Guardrails

- Do not hard-code kubeconfigs, tokens, cluster names, or private paths.
- Do not run destructive Kubernetes commands automatically.
- Do not delete timed-out jobs as part of monitoring.
- Prefer commit or tag pins over moving branch names for `--git-ref`.
- Keep generated logs, result JSON, and SQLite metadata intact for auditability.

## Local Setup Notes

Host-specific Hermes installation notes, kubeconfig paths, and workstation facts
belong outside this repository. Keep this repo focused on portable c-val code,
docs, and the `skills/c-val-hpc-engineer` skill package.