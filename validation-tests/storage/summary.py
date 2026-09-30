"""Publish the storage test's summary from its retained FIO artifacts."""

import json
import os
from pathlib import Path

from cval.validation.plugins import load_test_support

parse_storage_metrics = load_test_support(Path(__file__).with_name("ingest.py")).parse_storage_metrics


def main() -> None:
    summary = {
        "schema_version": "cval.storage.summary.v1",
        "node": os.environ["GCRNODE"],
        "timestamp": int(os.environ["GCRTIME"]),
        "metrics": parse_storage_metrics(os.environ["STORAGE_OUTPUT_DIR"]),
    }
    destination = Path(os.environ["STORAGE_SUMMARY_FILE"])
    with destination.open("x", encoding="utf-8") as handle:
        json.dump(summary, handle, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(f"Storage summary retained: {destination}")


if __name__ == "__main__":
    main()