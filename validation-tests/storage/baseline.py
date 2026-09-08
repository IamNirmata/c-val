"""Storage-owned FIO baseline inputs and build entry point."""


def read(context, run, test):
    metrics = test.settings["metrics"]["storage"]
    observations = context.read_wide(run, "storage", "storage_db_path", "storage_performance", "node", {name: "higher" for name in metrics})
    return observations, {"source": "storage_performance", "units": "fio-iops-and-KiB-per-second"}


def build(context, test, runs):
    return context.build_baselines(test, runs)


if __name__ == "__main__":
    from evaluation_engine.__main__ import main
    raise SystemExit(main("storage", "baseline"))