# Cluster Safety Policy

## Read-only observation

The nonmutating command set is:

```bash
python -m cval.cli status --output table
python -m cval.cli nodes --output table
python -m cval.cli plan --live-status --git-ref <40-hex-commit> --threshold-days 4 --batch-size 3 --output json
python -m cval.cli jobs --jobs <job-name> --output json
python -m cval.cli jobs --jobs <job-name> --watch --timeout-seconds 180 --poll-interval-seconds 30 --output json
```

## Approval Required

Real validation job submission requires explicit operator approval and this command shape:

```bash
python -m cval.cli validate --node <node> --git-ref <40-hex-commit> --submit --confirm submit
```

## Never Run Without Approval

- `kubectl delete`
- `kubectl drain`
- `kubectl cordon`
- `kubectl taint`
- `kubectl patch node`
- `kubectl scale`
- PVC, NFS, log, or SQLite DB deletion
- driver, fabric-manager, kubelet, or containerd restart
- cluster-wide RBAC, scheduler, queue, or admission changes

## Credentials

Never print kubeconfigs, tokens, API keys, or secret values. If auth fails, ask the operator to refresh credentials directly on the machine.