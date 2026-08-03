"""Focused PostgreSQL-backed NCCL evaluation subsystem.

The package root intentionally imports no PostgreSQL driver.  Ordinary c-val
commands therefore remain usable when the optional ``postgresql`` dependency
extra is not installed.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "1.0.0"
