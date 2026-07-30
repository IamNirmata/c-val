"""Lexical path safety checks for explicit SQLite write targets."""

from __future__ import annotations

from pathlib import Path


def safe_writable_file_path(
    value: str | Path,
    *,
    allowed_root: str | Path | None = None,
    description: str = "SQLite database",
) -> Path:
    """Reject symlinked/escaping targets before a writer creates parent paths."""

    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    path = Path(*path.parts)
    if any(part in {"", ".", ".."} for part in path.parts[1:]):
        raise ValueError(f"{description} path contains an invalid segment: {path}")

    if allowed_root is not None:
        root = Path(allowed_root).expanduser()
        if not root.is_absolute():
            root = Path.cwd() / root
        root = Path(*root.parts)
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"{description} path escapes allowed root {root}: {path}") from exc
        _reject_existing_symlinks(root, description)
    else:
        root = Path(path.anchor)

    _reject_existing_symlinks(path, description)
    if path.exists() and not path.is_file():
        raise ValueError(f"{description} target is not a regular file: {path}")
    return path


def safe_existing_evidence_path(
    value: str | Path,
    *,
    expected_path: str | Path,
    allowed_root: str | Path,
    expect_directory: bool,
    description: str = "validation evidence",
    allow_missing: bool = False,
) -> Path:
    """Require one canonical existing evidence path without symlink traversal."""

    path = _absolute_lexical_path(value)
    expected = _absolute_lexical_path(expected_path)
    root = _absolute_lexical_path(allowed_root)
    if path != expected:
        raise ValueError(f"{description} path is not canonical: {path}")
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{description} path escapes allowed root {root}: {path}") from exc
    _reject_existing_symlinks(root, description)
    _reject_existing_symlinks(path, description)
    if not path.exists():
        if not allow_missing:
            raise ValueError(f"{description} path does not exist: {path}")
        resolved_root = root.resolve(strict=False)
        resolved = path.resolve(strict=False)
        try:
            resolved.relative_to(resolved_root)
        except ValueError as exc:
            raise ValueError(
                f"{description} resolved path escapes root {root}: {path}"
            ) from exc
        if resolved != path:
            raise ValueError(f"{description} path is not lexically canonical: {path}")
        return path
    if expect_directory:
        if not path.is_dir():
            raise ValueError(f"{description} path is not a directory: {path}")
    elif not path.is_file():
        raise ValueError(f"{description} path is not a regular file: {path}")
    try:
        resolved_root = root.resolve(strict=True)
        resolved = path.resolve(strict=True)
        resolved.relative_to(resolved_root)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        raise ValueError(
            f"{description} resolved path escapes or invalidates root {root}: {path}"
        ) from exc
    if resolved != path:
        raise ValueError(f"{description} path is not lexically canonical: {path}")
    return path


def _absolute_lexical_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return Path(*path.parts)


def _reject_existing_symlinks(path: Path, description: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if current.is_symlink():
            raise ValueError(
                f"{description} must be a non-symlink path; found symlink: {current}"
            )
