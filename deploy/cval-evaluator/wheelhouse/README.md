# Offline evaluator wheelhouse

Place the independently downloaded and hash-verified `PyYAML==6.0.2` platform
wheel here before building. Dependency wheels are release inputs and are not
committed. The Docker build uses `--no-index` and must run with `--network=none`.
