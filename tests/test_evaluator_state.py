from __future__ import annotations

import multiprocessing
import fcntl
import os
import shutil
import signal
import sqlite3
import stat
import tempfile
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from cval.config import load_config
from cval.evaluator.state import (
    StateLockError,
    bind_state_directory,
    bind_state_target,
    configured_state_root,
    inspect_state_ancestry,
    inspect_state_target,
    state_test_lock,
)
from cval.storage.per_test_results import (
    PerTestResultRecord,
    framework_metric_ingestion_session,
    write_per_test_result,
)
from cval.storage.sqlite_snapshot import immutable_sqlite_snapshot
from cval.storage.sqlite_uri import SQLiteFileIdentity


def _state_config(root: Path):
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    base = load_config()
    return replace(
        base,
        runtime=replace(base.runtime, validation_root=str(root.parent / "shared")),
        health_evaluator=replace(
            base.health_evaluator,
            state_root=str(root),
            state_owner_uid=os.geteuid(),
            state_owner_gid=os.getegid(),
            lock_timeout_seconds=1,
        ),
    )


def _record(run_id: str) -> PerTestResultRecord:
    return PerTestResultRecord(
        run_id=run_id,
        test_id="storage",
        node="node-a",
        run_timestamp=1,
        started_timestamp=1,
        completed_timestamp=2,
        status="fail",
        exit_code=1,
        image_name="image",
        pytorch_version="2.8",
        cuda_version="12.9",
        test_config_digest="sha256:" + "1" * 64,
        result_path="/evidence/result.json",
        summary_path="",
        artifacts_path="/evidence/artifacts",
        raw_result_json='{"schema_version":"cval.test-result.v1","test_id":"storage"}',
        result_digest="sha256:" + "2" * 64,
    )


def _first_creator(root_text: str, queue) -> None:
    root = Path(root_text)
    config = _state_config(root)
    path = root / "validation_tests/storage/storage_results.db"
    with state_test_lock(config, path):
        with bind_state_target(
            config,
            path,
            create=True,
            allow_missing=False,
            writable=True,
            require_writable=True,
        ) as binding:
            assert binding.identity is not None
            queue.put((binding.identity.device, binding.identity.inode))


def _hold_shared_lock(root_text: str, ready, release) -> None:
    root = Path(root_text)
    config = _state_config(root)
    path = root / "validation_tests/storage/storage_results.db"
    with state_test_lock(config, path):
        ready.set()
        if not release.wait(10):
            raise RuntimeError("shared-lock test release timed out")


def _lock_split_creator(root_text: str, registered, proceed, queue) -> None:
    from cval.evaluator import state as state_module

    root = Path(root_text)
    config = _state_config(root)
    path = root / "validation_tests/storage/storage_results.db"

    def pause(stage, guard):
        if stage == "registered":
            queue.put(("a-registered", guard.identity.device, guard.identity.inode))
            registered.set()
            if not proceed.wait(10):
                raise RuntimeError("creator pause timed out")

    try:
        with patch.object(state_module, "_state_lock_checkpoint", side_effect=pause):
            with state_test_lock(config, path, timeout_seconds=1):
                queue.put(("a-acquired",))
    except StateLockError:
        lock_path = path.parent / ".storage_results.health-evaluator.lock"
        metadata = lock_path.stat()
        queue.put(("a-timeout", metadata.st_dev, metadata.st_ino))


def _hold_named_lock(root_text: str, ready, release, queue) -> None:
    root = Path(root_text)
    lock_path = root / "validation_tests/storage/.storage_results.health-evaluator.lock"
    descriptor = os.open(lock_path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        metadata = os.fstat(descriptor)
        queue.put(("b-acquired", metadata.st_dev, metadata.st_ino))
        ready.set()
        if not release.wait(10):
            raise RuntimeError("named-lock holder release timed out")
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class EvaluatorStateBindingTests(unittest.TestCase):
    @staticmethod
    def _fd_count() -> int:
        return len(list(Path("/proc/self/fd").iterdir()))

    def test_repeated_missing_invalid_and_symlink_inspection_leaks_no_fds(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            missing = root / "validation_tests/storage/missing/results.db"
            before = self._fd_count()
            for _ in range(100):
                self.assertIsNone(
                    inspect_state_target(
                        config,
                        missing,
                        allow_missing=True,
                        require_writable=False,
                    )
                )
                inspect_state_ancestry(
                    config,
                    missing,
                    allow_missing=True,
                    require_writable=False,
                )
            self.assertEqual(self._fd_count(), before)

            wrong = root / "validation_tests"
            wrong.mkdir(mode=0o700)
            os.chmod(wrong, 0o770)
            for _ in range(50):
                with self.assertRaises(PermissionError):
                    inspect_state_ancestry(
                        config,
                        missing,
                        allow_missing=True,
                        require_writable=False,
                    )
            self.assertEqual(self._fd_count(), before)
            os.chmod(wrong, 0o700)
            outside = root.parent / "outside"
            outside.mkdir()
            (wrong / "storage").symlink_to(outside, target_is_directory=True)
            for _ in range(50):
                with self.assertRaises((ValueError, OSError)):
                    inspect_state_target(
                        config,
                        missing,
                        allow_missing=True,
                        require_writable=False,
                    )
            self.assertEqual(self._fd_count(), before)

    def test_absolute_and_state_parent_transfer_interruptions_leak_no_fds(self) -> None:
        from cval.evaluator import state as state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            nested = root / "validation_tests/storage"
            nested.mkdir(parents=True, mode=0o700)
            for directory in (root / "validation_tests", nested):
                os.chmod(directory, 0o700)

            before = self._fd_count()
            absolute_primary = KeyboardInterrupt("absolute transfer interrupted")
            with patch.object(
                state_module.os,
                "dup",
                side_effect=absolute_primary,
            ), self.assertRaises(KeyboardInterrupt) as absolute_raised:
                state_module._open_absolute_directory(nested)
            self.assertIs(absolute_raised.exception, absolute_primary)
            self.assertEqual(self._fd_count(), before)

            parent_primary = SystemExit("state parent transfer interrupted")
            with patch.object(
                state_module,
                "_assert_owned_directory",
                side_effect=parent_primary,
            ), self.assertRaises(SystemExit) as parent_raised:
                inspect_state_target(
                    config,
                    nested / "missing.db",
                    allow_missing=True,
                    require_writable=False,
                )
            self.assertIs(parent_raised.exception, parent_primary)
            self.assertEqual(self._fd_count(), before)

    def test_binding_rejects_root_intermediate_move_mode_and_hardlink_drift(self) -> None:
        mutations = ("root", "intermediate", "move", "mode", "hardlink")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                parent = Path(tmpdir)
                root = parent / "state"
                config = _state_config(root)
                path = root / "validation_tests/storage/storage_results.db"
                with self.assertRaises((RuntimeError, ValueError, OSError)):
                    with bind_state_target(
                        config,
                        path,
                        create=True,
                        allow_missing=False,
                        writable=True,
                        require_writable=True,
                    ) as binding:
                        if mutation == "root":
                            parked = parent / "parked-root"
                            root.rename(parked)
                            root.mkdir(mode=0o700)
                        elif mutation == "intermediate":
                            current = root / "validation_tests"
                            parked = root / "parked-validation-tests"
                            current.rename(parked)
                            current.mkdir(mode=0o700)
                        elif mutation == "move":
                            path.rename(path.with_name("moved.db"))
                        elif mutation == "mode":
                            os.chmod(path, 0o640)
                        else:
                            os.link(path, path.with_name("alias.db"))
                        binding.assert_path_binding()

    def test_missing_binding_rejects_first_component_subtree_and_target_appearance(self) -> None:
        mutations = ("first", "subtree", "target")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir) / "state"
                config = _state_config(root)
                target = root / "validation_tests/storage/deep/results.db"
                if mutation == "target":
                    target.parent.mkdir(parents=True, mode=0o700)
                    for directory in (
                        root / "validation_tests",
                        root / "validation_tests/storage",
                        target.parent,
                    ):
                        os.chmod(directory, 0o700)
                with self.assertRaisesRegex(RuntimeError, "appeared"):
                    with bind_state_target(
                        config,
                        target,
                        create=False,
                        allow_missing=True,
                        writable=False,
                        require_writable=False,
                    ) as binding:
                        if mutation == "first":
                            (root / "validation_tests").mkdir(mode=0o700)
                        elif mutation == "subtree":
                            (root / "validation_tests/storage/deep").mkdir(
                                parents=True,
                                mode=0o700,
                            )
                        else:
                            target.touch(mode=0o600)
                        binding.assert_path_binding()

    def test_fileexists_race_never_repairs_unsafe_directory(self) -> None:
        from cval.evaluator import secure_state as secure_state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            target = root / "validation_tests/storage/results.db"
            original_publish = secure_state_module.rename_noreplace_at
            raced = False

            def race(source_parent_fd, source_name, destination_parent_fd, destination_name):
                nonlocal raced
                if destination_name == "validation_tests" and not raced:
                    raced = True
                    os.mkdir(destination_name, 0o770, dir_fd=destination_parent_fd)
                    os.chmod(root / "validation_tests", 0o770)
                return original_publish(
                    source_parent_fd,
                    source_name,
                    destination_parent_fd,
                    destination_name,
                )

            with patch.object(
                secure_state_module,
                "rename_noreplace_at",
                side_effect=race,
            ):
                with self.assertRaises((PermissionError, FileExistsError)):
                    with bind_state_directory(
                        config,
                        target,
                        create=True,
                        allow_missing=False,
                        require_writable=True,
                    ):
                        pass
            self.assertEqual(
                stat.S_IMODE((root / "validation_tests").stat().st_mode),
                0o770,
            )
            self.assertEqual(
                list(root.glob(".cval-dir-stage-*")),
                [],
            )

    def test_noatime_is_required_for_read_writable_and_create_bindings(self) -> None:
        from cval.evaluator import state as state_module

        if not hasattr(os, "O_NOATIME"):
            self.skipTest("O_NOATIME is unavailable")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            parent_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            original_open = os.open
            observed_flags: list[int] = []

            def record_flags(path, flags, mode=0o777, *, dir_fd=None):
                observed_flags.append(flags)
                return original_open(path, flags, mode, dir_fd=dir_fd)

            def deny_noatime(path, flags, mode=0o777, *, dir_fd=None):
                if flags & os.O_NOATIME:
                    raise PermissionError("O_NOATIME denied")
                return original_open(path, flags, mode, dir_fd=dir_fd)

            try:
                with patch.object(state_module.os, "open", side_effect=record_flags):
                    descriptor = state_module._open_file_at(
                        parent_fd,
                        "created.db",
                        writable=True,
                        create_exclusive=True,
                    )
                    os.close(descriptor)
                    descriptor = state_module._open_file_at(
                        parent_fd,
                        "created.db",
                        writable=True,
                    )
                    os.close(descriptor)
                    descriptor = state_module._open_file_at(
                        parent_fd,
                        "created.db",
                        writable=False,
                    )
                    os.close(descriptor)
                with patch.object(state_module.os, "open", side_effect=deny_noatime):
                    for writable in (False, True):
                        with self.assertRaisesRegex(
                            PermissionError,
                            "O_NOATIME denied",
                        ):
                            state_module._open_file_at(
                                parent_fd,
                                "created.db",
                                writable=writable,
                            )
            finally:
                os.close(parent_fd)
            self.assertEqual(len(observed_flags), 3)
            self.assertTrue(all(flags & os.O_NOATIME for flags in observed_flags))

    def test_pre_yield_u7_creation_interruptions_remove_only_created_inode(self) -> None:
        from cval.evaluator import state as state_module

        cases = (
            ("open", KeyboardInterrupt("open interrupted")),
            ("fchmod", SystemExit("fchmod interrupted")),
            ("file_fsync", KeyboardInterrupt("fsync interrupted")),
            ("identity", SystemExit("identity interrupted")),
        )
        for stage, primary in cases:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir) / "state"
                config = _state_config(root)
                path = root / "validation_tests/storage/storage_results.db"
                with state_test_lock(config, path):
                    before = self._fd_count()

                    def interrupt(current: str) -> None:
                        if current == stage:
                            raise primary

                    with patch.object(
                        state_module,
                        "_state_target_creation_checkpoint",
                        side_effect=interrupt,
                    ), self.assertRaises(type(primary)) as raised:
                        with bind_state_target(
                            config,
                            path,
                            create=True,
                            allow_missing=False,
                            writable=True,
                            require_writable=True,
                        ):
                            self.fail("interrupted creation must not yield")
                    self.assertIs(raised.exception, primary)
                    self.assertFalse(path.exists())
                    self.assertEqual(self._fd_count(), before)

    def test_u7_exclusive_open_fstat_interruption_removes_registered_inode(self) -> None:
        from cval.evaluator import state as state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            path = root / "validation_tests/storage/storage_results.db"
            with state_test_lock(config, path):
                before = self._fd_count()
                original_fstat = state_module.os.fstat
                interrupted = False

                def interrupt_created(descriptor):
                    nonlocal interrupted
                    metadata = original_fstat(descriptor)
                    if not interrupted and stat.S_ISREG(metadata.st_mode):
                        interrupted = True
                        raise KeyboardInterrupt("U7 creation fstat interrupted")
                    return metadata

                with patch.object(
                    state_module.os,
                    "fstat",
                    side_effect=interrupt_created,
                ), self.assertRaises(KeyboardInterrupt):
                    with bind_state_target(
                        config,
                        path,
                        create=True,
                        allow_missing=False,
                        writable=True,
                        require_writable=True,
                    ):
                        self.fail("interrupted creation must not yield")
                self.assertTrue(interrupted)
                self.assertFalse(path.exists())
                self.assertEqual(self._fd_count(), before)

    def test_u7_sigint_after_open_is_delivered_only_after_identity_registration(self) -> None:
        from cval.evaluator import state as state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            path = root / "validation_tests/storage/storage_results.db"
            with state_test_lock(config, path):
                before = self._fd_count()
                original_open = state_module._open_file_at
                sent = False

                def open_then_signal(*args, **kwargs):
                    nonlocal sent
                    descriptor = original_open(*args, **kwargs)
                    if kwargs.get("create_exclusive") and not sent:
                        sent = True
                        os.kill(os.getpid(), signal.SIGINT)
                    return descriptor

                with patch.object(
                    state_module,
                    "_open_file_at",
                    side_effect=open_then_signal,
                ), self.assertRaises(KeyboardInterrupt):
                    with bind_state_target(
                        config,
                        path,
                        create=True,
                        allow_missing=False,
                        writable=True,
                        require_writable=True,
                    ):
                        self.fail("signal-interrupted creation must not yield")
                self.assertTrue(sent)
                self.assertFalse(path.exists())
                self.assertEqual(self._fd_count(), before)

    def test_u7_ancestry_final_name_racer_is_preserved_and_stage_is_cleaned(self) -> None:
        from cval.evaluator import secure_state as secure_state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            target = root / "validation_tests/storage/results.db"
            racer = root / "validation_tests"
            original_publish = secure_state_module.rename_noreplace_at
            raced = False

            def publish_after_racer(source_parent_fd, source_name, destination_parent_fd, destination_name):
                nonlocal raced
                if destination_name == "validation_tests" and not raced:
                    raced = True
                    os.mkdir(destination_name, 0o700, dir_fd=destination_parent_fd)
                    (racer / "racer-owned").write_text("keep", encoding="utf-8")
                return original_publish(
                    source_parent_fd,
                    source_name,
                    destination_parent_fd,
                    destination_name,
                )

            before = self._fd_count()
            with patch.object(
                secure_state_module,
                "rename_noreplace_at",
                side_effect=publish_after_racer,
            ), self.assertRaises((PermissionError, FileExistsError, RuntimeError)):
                with bind_state_directory(
                    config,
                    target,
                    create=True,
                    allow_missing=False,
                    require_writable=True,
                ):
                    self.fail("malformed raced ancestry creation must not yield")
            self.assertTrue(raced)
            self.assertEqual((racer / "racer-owned").read_text(encoding="utf-8"), "keep")
            self.assertEqual(list(root.glob(".cval-dir-stage-*")), [])
            self.assertEqual(self._fd_count(), before)

    def test_quarantine_cleanup_preserves_public_racer_after_relocation(self) -> None:
        from cval.evaluator import secure_state as secure_state_module

        for is_directory in (False, True):
            with self.subTest(directory=is_directory), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir)
                name = "created"
                target = root / name
                if is_directory:
                    target.mkdir(mode=0o700)
                else:
                    target.write_bytes(b"created")
                metadata = target.stat()
                expected = (metadata.st_dev, metadata.st_ino)
                parent_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)

                def create_racer(stage, *_args):
                    if stage != "relocated":
                        return
                    if is_directory:
                        target.mkdir(mode=0o700)
                        (target / "racer").write_text("keep", encoding="utf-8")
                    else:
                        target.write_bytes(b"racer")

                try:
                    with patch.object(
                        secure_state_module,
                        "_quarantine_cleanup_checkpoint",
                        side_effect=create_racer,
                    ):
                        self.assertTrue(
                            secure_state_module.remove_entry_if_identity_at(
                                parent_fd,
                                name,
                                expected,
                                is_directory=is_directory,
                                description="test cleanup target",
                            )
                        )
                finally:
                    os.close(parent_fd)
                if is_directory:
                    self.assertEqual(
                        (target / "racer").read_text(encoding="utf-8"),
                        "keep",
                    )
                else:
                    self.assertEqual(target.read_bytes(), b"racer")
                self.assertEqual(list(root.glob(".cval-cleanup-*")), [])

    def test_quarantine_cleanup_mismatch_fails_explicitly_without_deletion(self) -> None:
        from cval.evaluator import secure_state as secure_state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            target = root / "created"
            target.write_bytes(b"replacement")
            parent_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaisesRegex(RuntimeError, "replacement preserved"):
                    secure_state_module.remove_entry_if_identity_at(
                        parent_fd,
                        target.name,
                        (-1, -1),
                        is_directory=False,
                        description="test cleanup target",
                    )
            finally:
                os.close(parent_fd)
            self.assertEqual(target.read_bytes(), b"replacement")
            self.assertEqual(list(root.glob(".cval-cleanup-*")), [])

    def test_pre_yield_u7_creation_preserves_racer_and_removes_relocated_created_file(self) -> None:
        from cval.evaluator import state as state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            path = root / "validation_tests/storage/storage_results.db"
            relocated = path.with_name("relocated-created.db")
            racer = b"racer-owned"
            primary = KeyboardInterrupt("post-open racer")

            def replace_then_interrupt(stage: str) -> None:
                if stage != "open":
                    return
                path.rename(relocated)
                path.write_bytes(racer)
                os.chmod(path, 0o600)
                raise primary

            with state_test_lock(config, path):
                with patch.object(
                    state_module,
                    "_state_target_creation_checkpoint",
                    side_effect=replace_then_interrupt,
                ), self.assertRaises(KeyboardInterrupt) as raised:
                    with bind_state_target(
                        config,
                        path,
                        create=True,
                        allow_missing=False,
                        writable=True,
                        require_writable=True,
                    ):
                        self.fail("interrupted creation must not yield")
            self.assertIs(raised.exception, primary)
            self.assertEqual(path.read_bytes(), racer)
            self.assertTrue(relocated.exists())

    def test_state_lock_signal_after_exclusive_open_publishes_0600_and_is_reacquirable(self) -> None:
        from cval.evaluator import state as state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            result = root / "validation_tests/storage/storage_results.db"
            lock_path = result.parent / ".storage_results.health-evaluator.lock"
            result.parent.mkdir(parents=True, mode=0o700)
            for directory in (root / "validation_tests", result.parent):
                os.chmod(directory, 0o700)
            original_open = state_module._open_file_at
            previous_umask = os.umask(0o777)
            sent = False

            def open_then_signal(*args, **kwargs):
                nonlocal sent
                descriptor = original_open(*args, **kwargs)
                if kwargs.get("create_exclusive") and not sent:
                    sent = True
                    os.kill(os.getpid(), signal.SIGINT)
                return descriptor

            try:
                with patch.object(
                    state_module,
                    "_open_file_at",
                    side_effect=open_then_signal,
                ), self.assertRaises(KeyboardInterrupt):
                    with state_test_lock(config, result, timeout_seconds=1):
                        self.fail("pending signal must be redelivered before lock acquisition")
            finally:
                os.umask(previous_umask)

            self.assertTrue(sent)
            metadata = lock_path.stat()
            self.assertTrue(stat.S_ISREG(metadata.st_mode))
            self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
            self.assertEqual((metadata.st_uid, metadata.st_gid), (os.geteuid(), os.getegid()))
            self.assertEqual(metadata.st_nlink, 1)
            with state_test_lock(config, result, timeout_seconds=1) as guard:
                guard()

    def test_state_lock_unlock_exceptions_close_fd_and_preserve_primary(self) -> None:
        from cval.evaluator import state as state_module

        for unlock_error in (
            KeyboardInterrupt("unlock interrupted"),
            OSError("unlock failed"),
        ):
            for primary in (None, RuntimeError("primary failure")):
                with self.subTest(
                    unlock=type(unlock_error).__name__,
                    primary=primary is not None,
                ), tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir) / "state"
                    config = _state_config(root)
                    result = root / "validation_tests/storage/storage_results.db"
                    result.parent.mkdir(parents=True, mode=0o700)
                    for directory in (root / "validation_tests", result.parent):
                        os.chmod(directory, 0o700)
                    original_flock = state_module.fcntl.flock

                    def fail_unlock(descriptor, operation):
                        if operation == fcntl.LOCK_UN:
                            raise unlock_error
                        return original_flock(descriptor, operation)

                    before = self._fd_count()
                    with patch.object(
                        state_module.fcntl,
                        "flock",
                        side_effect=fail_unlock,
                    ), self.assertRaises(
                        type(primary) if primary is not None else type(unlock_error)
                    ) as raised:
                        with state_test_lock(config, result, timeout_seconds=1):
                            if primary is not None:
                                raise primary
                    self.assertIs(raised.exception, primary or unlock_error)
                    if primary is not None:
                        self.assertTrue(
                            any(
                                "shared-lock unlock failed" in note
                                for note in getattr(primary, "__notes__", ())
                            )
                        )
                    self.assertEqual(self._fd_count(), before)

    def test_ancestry_unlock_exceptions_close_untracked_descriptor(self) -> None:
        from cval.evaluator import state as state_module

        for unlock_error in (
            KeyboardInterrupt("ancestry unlock interrupted"),
            OSError("ancestry unlock failed"),
        ):
            for primary in (None, RuntimeError("ancestry primary failure")):
                with self.subTest(
                    unlock=type(unlock_error).__name__,
                    primary=primary is not None,
                ), tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir) / "state"
                    config = _state_config(root)
                    target = root / "validation_tests/storage/results.db"
                    original_flock = state_module.fcntl.flock
                    original_creator = state_module.create_published_directory_at

                    def create_then_fail(*args, **kwargs):
                        published = original_creator(*args, **kwargs)
                        if primary is None:
                            return published

                        class InterruptedPublication:
                            descriptor = published.descriptor

                            @property
                            def identity(self):
                                raise primary

                        return InterruptedPublication()

                    def fail_unlock(descriptor, operation):
                        if operation == fcntl.LOCK_UN:
                            raise unlock_error
                        return original_flock(descriptor, operation)

                    before = self._fd_count()
                    with patch.object(
                        state_module.fcntl,
                        "flock",
                        side_effect=fail_unlock,
                    ), patch.object(
                        state_module,
                        "create_published_directory_at",
                        side_effect=create_then_fail,
                    ), self.assertRaises(
                        type(primary) if primary is not None else type(unlock_error)
                    ) as raised:
                        with bind_state_directory(
                            config,
                            target,
                            create=True,
                            allow_missing=False,
                            require_writable=True,
                        ):
                            self.fail("unlock failure must prevent ancestry handoff")
                    self.assertIs(raised.exception, primary or unlock_error)
                    if primary is not None:
                        self.assertTrue(
                            any(
                                "serialization unlock failed" in note
                                for note in getattr(primary, "__notes__", ())
                            )
                        )
                    self.assertEqual(self._fd_count(), before)

    def test_writable_state_snapshot_preserves_source_atime(self) -> None:
        if not hasattr(os, "O_NOATIME"):
            self.skipTest("O_NOATIME is unavailable")
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            # A non-.db leaf avoids host endpoint scanners racing this metadata
            # assertion; the file contents and binding/snapshot path remain
            # real SQLite.
            path = root / "validation_tests/storage/snapshot-source.bin"
            path.parent.mkdir(parents=True, mode=0o700)
            for directory in (root / "validation_tests", path.parent):
                os.chmod(directory, 0o700)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("CREATE TABLE t(value INTEGER)")
                connection.execute("INSERT INTO t VALUES(1)")
                connection.commit()
            os.chmod(path, 0o600)
            metadata = path.stat()
            os.utime(
                path,
                ns=(
                    metadata.st_mtime_ns + 7 * 24 * 60 * 60 * 1_000_000_000,
                    metadata.st_mtime_ns,
                ),
            )
            before = path.stat().st_atime_ns

            with bind_state_target(
                config,
                path,
                create=False,
                allow_missing=False,
                writable=True,
                require_writable=True,
            ) as binding:
                identity = binding.sqlite_identity
                assert identity is not None and binding.descriptor is not None
                with immutable_sqlite_snapshot(
                    path,
                    expected_identity=identity,
                    source_fd=binding.descriptor,
                    source_parent_fd=binding.directory.parent_fd,
                    source_name=binding.name,
                    binding_guard=binding.assert_path_binding,
                ) as snapshot:
                    with closing(sqlite3.connect(snapshot.uri, uri=True)) as connection:
                        self.assertEqual(
                            connection.execute("SELECT value FROM t").fetchone(),
                            (1,),
                        )
            self.assertEqual(path.stat().st_atime_ns, before)

    def test_open_state_root_uses_descriptor_capability_and_rejects_replacement(self) -> None:
        from cval.evaluator import state as state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            parent = Path(tmpdir)
            root = parent / "state"
            config = _state_config(root)
            self.assertEqual(configured_state_root(config), root)
            with patch.object(
                state_module.os,
                "access",
                side_effect=AssertionError("path access must not be used"),
            ):
                inspect_state_ancestry(
                    config,
                    root / "missing/results.db",
                    allow_missing=True,
                    require_writable=True,
                )

            original_statvfs = state_module.os.statvfs
            parked = parent / "parked"

            def replace_during_capability(descriptor):
                result = original_statvfs(descriptor)
                root.rename(parked)
                root.mkdir(mode=0o700)
                return result

            with patch.object(
                state_module.os,
                "statvfs",
                side_effect=replace_during_capability,
            ):
                with self.assertRaises(RuntimeError):
                    inspect_state_ancestry(
                        config,
                        root / "missing/results.db",
                        allow_missing=True,
                        require_writable=True,
                    )

    def test_two_process_first_creators_share_one_inode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            _state_config(root)
            context = multiprocessing.get_context("fork")
            queue = context.Queue()
            processes = [
                context.Process(target=_first_creator, args=(str(root), queue))
                for _ in range(2)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(10)
                self.assertEqual(process.exitcode, 0)
            identities = [queue.get(timeout=1) for _ in processes]
            self.assertEqual(identities[0], identities[1])

    def test_shared_primitive_blocks_a_second_process(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            result = root / "validation_tests/storage/storage_results.db"
            context = multiprocessing.get_context("fork")
            ready = context.Event()
            release = context.Event()
            holder = context.Process(
                target=_hold_shared_lock,
                args=(str(root), ready, release),
            )
            holder.start()
            self.assertTrue(ready.wait(5))
            try:
                with self.assertRaisesRegex(StateLockError, "Timed out"):
                    with state_test_lock(config, result, timeout_seconds=1):
                        pass
            finally:
                release.set()
            holder.join(5)
            self.assertEqual(holder.exitcode, 0)

    def test_three_process_lock_timeout_preserves_same_path_and_inode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            result = root / "validation_tests/storage/storage_results.db"
            lock_path = result.parent / ".storage_results.health-evaluator.lock"
            context = multiprocessing.get_context("fork")
            a_registered = context.Event()
            a_proceed = context.Event()
            b_ready = context.Event()
            b_release = context.Event()
            queue = context.Queue()
            creator = context.Process(
                target=_lock_split_creator,
                args=(str(root), a_registered, a_proceed, queue),
            )
            creator.start()
            self.assertTrue(a_registered.wait(5))
            registered = queue.get(timeout=1)
            holder = context.Process(
                target=_hold_named_lock,
                args=(str(root), b_ready, b_release, queue),
            )
            holder.start()
            self.assertTrue(b_ready.wait(5))
            held = queue.get(timeout=1)
            self.assertEqual(registered[1:], held[1:])
            a_proceed.set()
            creator.join(5)
            self.assertEqual(creator.exitcode, 0)
            timed_out = queue.get(timeout=1)
            self.assertEqual(timed_out[0], "a-timeout")
            self.assertEqual(timed_out[1:], held[1:])
            self.assertEqual((lock_path.stat().st_dev, lock_path.stat().st_ino), held[1:])
            try:
                with self.assertRaisesRegex(StateLockError, "Timed out"):
                    with state_test_lock(config, result, timeout_seconds=1):
                        pass
                self.assertEqual(
                    (lock_path.stat().st_dev, lock_path.stat().st_ino),
                    held[1:],
                )
            finally:
                b_release.set()
            holder.join(5)
            self.assertEqual(holder.exitcode, 0)

    def test_malformed_and_raced_lock_entries_remain_fail_closed(self) -> None:
        for malformed in ("directory", "mode"):
            with self.subTest(malformed=malformed), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir) / "state"
                config = _state_config(root)
                result = root / "validation_tests/storage/storage_results.db"
                result.parent.mkdir(parents=True, mode=0o700)
                for directory in (root / "validation_tests", result.parent):
                    os.chmod(directory, 0o700)
                lock_path = result.parent / ".storage_results.health-evaluator.lock"
                if malformed == "directory":
                    lock_path.mkdir(mode=0o700)
                else:
                    lock_path.touch(mode=0o640)
                    os.chmod(lock_path, 0o640)
                identity = (lock_path.stat().st_dev, lock_path.stat().st_ino)
                with self.assertRaises((PermissionError, OSError, StateLockError)):
                    with state_test_lock(config, result, timeout_seconds=1):
                        pass
                self.assertEqual(
                    (lock_path.stat().st_dev, lock_path.stat().st_ino),
                    identity,
                )

    def test_lock_identity_registration_failure_leaves_owner_0600_inode(self) -> None:
        from cval.evaluator import state as state_module

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            result = root / "validation_tests/storage/storage_results.db"
            lock_path = result.parent / ".storage_results.health-evaluator.lock"
            original_identity = state_module._state_file_identity

            def reject_lock_identity(descriptor, path, *, config):
                if path == lock_path:
                    raise KeyboardInterrupt("lock identity registration failed")
                return original_identity(descriptor, path, config=config)

            with patch.object(
                state_module,
                "_state_file_identity",
                side_effect=reject_lock_identity,
            ), self.assertRaises(KeyboardInterrupt):
                with state_test_lock(config, result, timeout_seconds=1):
                    self.fail("identity registration failure must not acquire")
            metadata = lock_path.stat()
            self.assertTrue(stat.S_ISREG(metadata.st_mode))
            self.assertEqual((metadata.st_uid, metadata.st_gid), (os.geteuid(), os.getegid()))
            self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
            self.assertEqual(metadata.st_nlink, 1)

    def test_shared_lock_rejects_symlink_hardlink_unlink_and_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            result = root / "validation_tests/storage/storage_results.db"
            with state_test_lock(config, result) as guard:
                alias = guard.path.with_name("lock-alias")
                os.link(guard.path, alias)
                with self.assertRaises(StateLockError):
                    guard()
                alias.unlink()
            lock_path = result.parent / ".storage_results.health-evaluator.lock"
            lock_path.unlink()
            outside = root / "outside-lock"
            outside.touch(mode=0o600)
            lock_path.symlink_to(outside)
            with self.assertRaises((StateLockError, ValueError, OSError)):
                with state_test_lock(config, result):
                    pass
            lock_path.unlink()
            with self.assertRaises(StateLockError):
                with state_test_lock(config, result) as guard:
                    guard.path.unlink()
                    guard()
            replacement = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(replacement)

    def test_snapshot_rejects_transient_substitute_even_when_original_restored(self) -> None:
        from cval.storage import sqlite_snapshot as snapshot_module

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "source.db"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("CREATE TABLE t(value INTEGER)")
                connection.execute("INSERT INTO t VALUES(1)")
                connection.commit()
            expected = SQLiteFileIdentity.capture(path)
            substitute = path.with_name("substitute.db")
            shutil.copy2(path, substitute)
            parked = path.with_name("parked.db")
            original_read = snapshot_module._read_without_atime

            def transient(value, metadata):
                path.rename(parked)
                substitute.rename(path)
                try:
                    return original_read(value, metadata)
                finally:
                    path.rename(substitute)
                    parked.rename(path)

            with patch.object(snapshot_module, "_read_without_atime", transient):
                with self.assertRaises(RuntimeError):
                    with immutable_sqlite_snapshot(path, expected_identity=expected):
                        pass
            self.assertEqual(SQLiteFileIdentity.capture(path), expected)

    def test_bound_snapshot_reads_original_then_rejects_parent_substitution_without_touching_substitute(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            path = root / "validation_tests/storage/source.db"
            path.parent.mkdir(parents=True, mode=0o700)
            for directory in (root / "validation_tests", path.parent):
                os.chmod(directory, 0o700)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("CREATE TABLE t(value INTEGER)")
                connection.execute("INSERT INTO t VALUES(1)")
                connection.commit()
            os.chmod(path, 0o600)
            parked = root / "validation_tests/parked-storage"
            with self.assertRaises(RuntimeError):
                with bind_state_target(
                    config,
                    path,
                    create=False,
                    allow_missing=False,
                    writable=False,
                    require_writable=False,
                ) as binding:
                    identity = binding.sqlite_identity
                    assert identity is not None and binding.descriptor is not None
                    path.parent.rename(parked)
                    path.parent.mkdir(mode=0o700)
                    substitute = path
                    shutil.copy2(parked / path.name, substitute)
                    os.chmod(substitute, 0o600)
                    before = substitute.stat().st_atime_ns
                    with immutable_sqlite_snapshot(
                        path,
                        expected_identity=identity,
                        source_fd=binding.descriptor,
                        source_parent_fd=binding.directory.parent_fd,
                        source_name=binding.name,
                        binding_guard=binding.assert_path_binding,
                    ):
                        pass
                    self.assertEqual(substitute.stat().st_atime_ns, before)

    def test_u7_writer_guards_block_mode_and_hardlink_drift_and_never_use_rwc(self) -> None:
        from cval.storage import per_test_results as storage_module

        for mutation in ("mode", "hardlink"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmpdir:
                root = Path(tmpdir) / "state"
                config = _state_config(root)
                path = root / "validation_tests/storage/storage_results.db"
                with state_test_lock(config, path) as lock_guard, bind_state_target(
                    config,
                    path,
                    create=True,
                    allow_missing=False,
                    writable=True,
                    require_writable=True,
                ) as binding:
                    identity = binding.sqlite_identity
                    assert identity is not None
                    write_per_test_result(
                        _record("first"),
                        db_path=path,
                        expected_identity=identity,
                        state_guard=lambda: (lock_guard(), binding.assert_path_binding()),
                    )
                    calls = 0

                    def mutate_guard() -> None:
                        nonlocal calls
                        calls += 1
                        if calls == 2:
                            if mutation == "mode":
                                os.chmod(path, 0o640)
                            else:
                                os.link(path, path.with_name("writer-alias.db"))
                        lock_guard()
                        binding.assert_path_binding()

                    with self.assertRaises(RuntimeError):
                        write_per_test_result(
                            _record("second"),
                            db_path=path,
                            expected_identity=identity,
                            state_guard=mutate_guard,
                        )
                    if mutation == "mode":
                        os.chmod(path, 0o600)
                    else:
                        path.with_name("writer-alias.db").unlink()
                    with closing(sqlite3.connect(path)) as connection:
                        self.assertEqual(
                            connection.execute(
                                "SELECT run_id FROM test_results ORDER BY result_id"
                            ).fetchall(),
                            [("first",)],
                        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "state"
            config = _state_config(root)
            path = root / "validation_tests/storage/storage_results.db"
            modes: list[str] = []
            original_connect = storage_module.connect_sqlite_file

            def recording_connect(*args, **kwargs):
                modes.append(kwargs["mode"])
                return original_connect(*args, **kwargs)

            with state_test_lock(config, path) as lock_guard, bind_state_target(
                config,
                path,
                create=True,
                allow_missing=False,
                writable=True,
                require_writable=True,
            ) as binding, patch.object(
                storage_module,
                "connect_sqlite_file",
                side_effect=recording_connect,
            ):
                identity = binding.sqlite_identity
                assert identity is not None
                guard = lambda: (lock_guard(), binding.assert_path_binding())
                write_per_test_result(
                    _record("mode-check"),
                    db_path=path,
                    expected_identity=identity,
                    state_guard=guard,
                )
                with framework_metric_ingestion_session(
                    path,
                    expected_identity=identity,
                    state_guard=guard,
                ):
                    pass
            self.assertEqual(modes, ["rw", "rw"])
            self.assertNotIn("rwc", modes)


if __name__ == "__main__":
    unittest.main()
