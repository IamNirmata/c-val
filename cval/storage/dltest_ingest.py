"""Compatibility import for the DL test's raw parser and SQLite writer."""

from pathlib import Path
import sys

from cval.validation.plugins import load_test_support

_implementation = load_test_support(
    Path(__file__).resolve().parents[2] / "validation-tests/dltest/ingest.py"
)
sys.modules[__name__] = _implementation