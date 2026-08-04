# c-val documentation

c-val has one validation path and one evaluator path:

- validation jobs run registered checks and write current raw DBs;
- baseline build/classify uses median/MAD and writes per-target baseline and
  classification DBs.

## Current documents

- [Architecture](architecture.md)
- [CLI reference](cli-reference.md)
- [Configuration](configuration.md)
- [Operations runbook](operations-runbook.md)
- [Baselines and classification](baselines.md)
- [Result schema](result-schema.md)
- [Test lifecycle](test-lifecycle.md)
- [Workflow](workflow.md)
- [Troubleshooting](troubleshooting.md)
- [Operator cheatsheet](cval-cheatsheet.md)
- [Current status and remaining live actions](todo/cval-update.md)
- [NCCL PostgreSQL evaluator specification](evals/nccl-eval-process.md)
- [NCCL PostgreSQL rollout and rollback](evals/nccl-rollout.md)
- [Hermes integration](hermes-integration.md)

Historical result artifacts remain readable. The repository no longer contains
the rejected normalized-history, alternate per-test persistence, health-class,
shadow-parity, or compatibility-audit stacks.
