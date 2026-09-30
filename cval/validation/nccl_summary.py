"""Compatibility imports for the test-owned NCCL summary implementation."""

from pathlib import Path
import runpy

globals().update({name: value for name, value in runpy.run_path(
    str(Path(__file__).resolve().parents[2] / "validation-tests/nccl/summary.py")
).items() if not name.startswith("__")})

if __name__ == "__main__":
    raise SystemExit(main())