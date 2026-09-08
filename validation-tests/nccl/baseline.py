"""NCCL-owned bandwidth, latency and selected HCA baseline inputs."""


def read(context, run, test):
    metrics = test.settings["metrics"]["nccl"]
    directions = {name: "lower" if name == "LATENCY" else "higher" for name in metrics}
    observations = context.read_wide(run, "nccl", "nccl_db_path", "IB_HEALTH", "Node", directions)
    rows = context.read(context.settings.raw.storage.nccl_db_path, "SELECT iterations,data_size_gb,samples FROM IB_HEALTH WHERE Node=? AND timestamp=?", (run["node"], run["timestamp"]))
    if rows[0]["samples"] is None or rows[0]["samples"] <= 0:
        from evaluation_engine.runtime import EvidenceError
        raise EvidenceError("missing_nccl_samples")
    return observations, {"iterations": rows[0]["iterations"], "data_size_gb": rows[0]["data_size_gb"]}


def build(context, test, runs):
    return context.build_baselines(test, runs)


if __name__ == "__main__":
    from evaluation_engine.__main__ import main
    raise SystemExit(main("nccl", "baseline"))