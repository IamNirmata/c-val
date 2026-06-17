# Troubleshooting

## Job Stays Pending

Check phase and events:

```bash
python -m cval.cli jobs --jobs <job-name> --output json
kubectl describe vcjob -n gcr-admin <job-name>
kubectl describe pod -n gcr-admin <job-name>-server-0
```

Common causes:

- targeted node is cordoned or tainted with non-tolerated `NoSchedule`
- insufficient free GPUs
- insufficient CPU or memory for the template
- insufficient `rdma/rdma_shared_device_a`
- node selector mismatch

c-val discovery excludes cordoned and non-tolerated `NoSchedule` nodes.

## PVC Access Pod Exec Fails

Sometimes `kubectl exec` to the PVC access pod can fail through the cluster proxy. Use pod logs or retry later. Do not paste kubeconfig or tokens into chat.

## No Structured Result JSON

Confirm the job checked out a c-val 2.0 commit:

```bash
kubectl logs -n gcr-admin <pod> | grep 'HEAD is now at'
```

Jobs that cloned old `main` before c-val 2.0 will not emit `CVAL_RESULT_JSON_FILE`.

## DL Test Concerns

Use the DL README guidance. Look for:

- successful `DL Test completed successfully` line
- task tables with `norm_output`, CPU timing, and GPU timing
- no `FAILED`, `Traceback`, `Exception`, `NaN`, `Inf`, or mismatch markers

The DL test validates layer and collective numerical consistency through output norms, plus CPU/GPU timing behavior.

## NCCL Concerns

Inspect the NCCL summary JSON:

```bash
cat /data/continuous_validation/nccl/<node>/nccl-<node>-<ts>/nccl-summary-<node>-<ts>.json
```

Important fields:

- `GCR_LATENCY`
- `GCR_ALGBW`
- `GCR_BUSBW`
- `GCR_IB_PORT_BW_GBPS`, per-`mlx5_*` average, max, last, and sample count from the in-pod IBBW monitor
- `GCR_IBBW_LOG_FILE`, the raw one-second IBBW monitor log appended to the NCCL log

## DB Rows Missing

Check result JSON first:

```bash
python -m cval.cli result --result-json <result-json>
```

Then check latest status:

```bash
python -m cval.cli status --output table
```