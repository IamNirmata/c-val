"""Compatibility entry point for the test-owned summary."""

from pathlib import Path
import runpy

globals().update({name: value for name, value in runpy.run_path(
    str(Path(__file__).with_name("summary.py"))
).items() if not name.startswith("__")})

if __name__ == "__main__":
    raise SystemExit(main())