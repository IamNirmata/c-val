# Validation test lifecycle

## Add a pass/fail test

Preview a scaffold:

```text
cval tests scaffold <id> --order <number>
```

Create it only with exact `--apply --confirm scaffold`. Scaffolding creates a
disabled registry stanza and a test directory; it never edits global config.

A descriptor declares:

- execution metadata and deterministic timeout;
- Kubernetes resource requirements;
- test-owned settings;
- artifact summary filename;
- optional plugin.

A new test does not need a result DB. If it owns a specialized metric DB, it may
declare test-owned persistence settings and implement its own validated
writer/reader. c-val never creates a generic common DB automatically.

## Plugin capabilities

- `config` — deterministic descriptor validation;
- `baseline` — build and classify using the canonical evaluator contract;
- `export` — return validated rectangular rows to core CSV writing.

Enablement of a new writer or DB remains a separate backed-up, inspected, and
reviewed change. Plugin code receives no Kubernetes client or arbitrary output path.

## Validate changes on the cluster

```text
cval nodes --output json
cval validate --node <eligible-node> --git-ref <published-40-hex-commit> \
	--submit --confirm submit
```

The real job is the development acceptance test. Confirm its terminal phase,
canonical `cval.results`, exact-timestamp raw DB rows, and retained logs. Local
unit, compile, registry, and offline-render suites are no longer mandatory
before publishing a candidate.

## Disable or remove

Disabling a registration is the first rollback. It removes the test from new
runs and operational target enumeration but does not delete evidence.

Code removal requires proof that no current producer/consumer needs it and an
explicit rollback plan. Historical result and DL artifact readers remain
read-only where tests prove they are required. Data deletion is always a
separate, forbidden-by-default operation.
