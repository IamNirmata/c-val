"""Narrow process-signal deferral for persistent creation registration."""

from __future__ import annotations

import signal
import threading
from contextlib import contextmanager
from types import FrameType
from typing import Iterator

_DEFERRED_SIGNALS = (signal.SIGINT, signal.SIGTERM)


@contextmanager
def defer_creation_signals() -> Iterator[None]:
    """Defer the first SIGINT/SIGTERM until a creation identity is registered.

    Python dispatches process-signal handlers on the main thread between bytecode
    instructions, including immediately after a successful creation syscall and
    before its return value is assigned.  Persistent creators use this context
    only around the syscall, immediate no-follow open/fstat, and insertion into
    their cleanup registry.  Direct ``BaseException`` instances raised by code
    are not intercepted.  Non-main threads are unchanged because only the main
    thread may install Python signal handlers.
    """

    if threading.current_thread() is not threading.main_thread():
        yield
        return

    previous: dict[int, signal.Handlers] = {}
    pending: list[int] = []

    def remember_first(signum: int, _frame: FrameType | None) -> None:
        if not pending:
            pending.append(signum)

    installed: list[int] = []
    try:
        for signum in _DEFERRED_SIGNALS:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, remember_first)
            installed.append(signum)
    except BaseException:
        for signum in reversed(installed):
            signal.signal(signum, previous[signum])
        raise

    try:
        yield
    finally:
        for signum in reversed(installed):
            signal.signal(signum, previous[signum])
        if pending:
            signal.raise_signal(pending[0])


__all__ = ["defer_creation_signals"]
