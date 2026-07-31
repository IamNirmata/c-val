"""Release identity helpers shared by U11 preflight and service execution."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from cval.config import CvalConfig, config_to_dict


COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
IMAGE_PATTERN = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
PLACEHOLDER_COMMIT = "0" * 40
PLACEHOLDER_IMAGE_DIGEST = "0" * 64
DEFAULT_BUILD_MARKER = Path(__file__).with_name("BUILD_COMMIT")


def canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def effective_config_digest(config: CvalConfig) -> str:
    return canonical_digest(config_to_dict(config))


def read_verified_release_identity(
    *,
    expected_commit: str | None = None,
    image_ref: str | None = None,
    marker_path: Path = DEFAULT_BUILD_MARKER,
) -> dict[str, str]:
    """Verify the immutable image marker and digest-pinned image identity."""

    expected = expected_commit or os.environ.get("CVAL_EXPECTED_COMMIT", "")
    image = image_ref or os.environ.get("CVAL_IMAGE_REF", "")
    if not COMMIT_PATTERN.fullmatch(expected) or expected == PLACEHOLDER_COMMIT:
        raise ValueError("CVAL_EXPECTED_COMMIT must be a non-placeholder 40-character commit")
    try:
        embedded = marker_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError) as exc:
        raise RuntimeError("Embedded release commit marker is unreadable") from exc
    if not COMMIT_PATTERN.fullmatch(embedded) or embedded == PLACEHOLDER_COMMIT:
        raise RuntimeError("Embedded release commit marker is not a built release identity")
    if embedded != expected:
        raise RuntimeError("Embedded release commit does not match CVAL_EXPECTED_COMMIT")
    if not IMAGE_PATTERN.fullmatch(image):
        raise ValueError("CVAL_IMAGE_REF must be pinned by sha256 digest")
    if image.endswith(PLACEHOLDER_IMAGE_DIGEST):
        raise ValueError("CVAL_IMAGE_REF still contains the release digest placeholder")
    return {"commit": embedded, "image": image}
