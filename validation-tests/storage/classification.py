"""Storage-owned raw FIO classification and derived persistence entry point."""


def classify(context, test, run):
    return context.classify_run(run, test)


if __name__ == "__main__":
    from evaluation_engine.__main__ import main
    raise SystemExit(main("storage", "classify"))