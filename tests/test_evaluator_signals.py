from __future__ import annotations

import os
import signal
import threading
import unittest

from cval.evaluator.signals import defer_creation_signals


class CreationSignalDeferralTests(unittest.TestCase):
    def test_sigint_is_redelivered_after_critical_section_and_handlers_restore(self) -> None:
        previous = {
            signum: signal.getsignal(signum)
            for signum in (signal.SIGINT, signal.SIGTERM)
        }
        events: list[str] = []
        with self.assertRaises(KeyboardInterrupt):
            with defer_creation_signals():
                os.kill(os.getpid(), signal.SIGINT)
                events.append("registered")
        self.assertEqual(events, ["registered"])
        for signum, handler in previous.items():
            self.assertEqual(signal.getsignal(signum), handler)

    def test_first_pending_signal_only_is_redelivered(self) -> None:
        delivered: list[int] = []
        previous_int = signal.getsignal(signal.SIGINT)
        previous_term = signal.getsignal(signal.SIGTERM)

        def record(signum, _frame):
            delivered.append(signum)

        signal.signal(signal.SIGTERM, record)
        try:
            with defer_creation_signals():
                os.kill(os.getpid(), signal.SIGTERM)
                os.kill(os.getpid(), signal.SIGINT)
                self.assertEqual(delivered, [])
            self.assertEqual(delivered, [signal.SIGTERM])
        finally:
            signal.signal(signal.SIGINT, previous_int)
            signal.signal(signal.SIGTERM, previous_term)

    def test_direct_baseexceptions_are_not_swallowed(self) -> None:
        for primary in (KeyboardInterrupt("direct"), SystemExit("direct")):
            with self.subTest(exception=type(primary).__name__), self.assertRaises(
                type(primary)
            ) as raised:
                with defer_creation_signals():
                    raise primary
            self.assertIs(raised.exception, primary)

    def test_non_main_thread_does_not_install_or_defer_handlers(self) -> None:
        previous = {
            signum: signal.getsignal(signum)
            for signum in (signal.SIGINT, signal.SIGTERM)
        }
        completed: list[bool] = []
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                with defer_creation_signals():
                    completed.append(True)
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(completed, [True])
        for signum, handler in previous.items():
            self.assertEqual(signal.getsignal(signum), handler)


if __name__ == "__main__":
    unittest.main()