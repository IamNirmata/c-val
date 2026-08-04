"""Finalize NCCL metrics with parent-owned InfiniBand monitor evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
from pathlib import Path


IBBW_SAMPLE_PATTERN = re.compile(
    r"(mlx5_\d+(?:\.\d+)?):\s*([0-9]+(?:\.[0-9]+)?)\s*([MG]B/s)"
)
_MAX_METRICS_BYTES = 2 * 1024 * 1024
_MAX_IBBW_BYTES = 16 * 1024 * 1024


def _read_private_regular_file(path: Path, *, max_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or before.st_size < 0
            or before.st_size > max_bytes
        ):
            raise ValueError(f"NCCL input is not a safe private regular file: {path}")
        payload = bytearray()
        while len(payload) <= max_bytes:
            chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(descriptor)
        identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if len(payload) > max_bytes or tuple(
            getattr(before, field) for field in identity
        ) != tuple(getattr(after, field) for field in identity):
            raise RuntimeError(f"NCCL input changed while reading: {path}")
        return bytes(payload)
    finally:
        os.close(descriptor)


def summarize_ibbw(payload: str) -> dict[str, dict[str, float | int]]:
    samples: dict[str, list[float]] = {}
    for line in payload.splitlines():
        for device, value, unit in IBBW_SAMPLE_PATTERN.findall(line):
            gbps = float(value) / 1024 if unit == "MB/s" else float(value)
            samples.setdefault(device, []).append(gbps)
    return {
        device: {
            "avg_gbps": sum(values) / len(values),
            "max_gbps": max(values),
            "last_gbps": values[-1],
            "samples": len(values),
        }
        for device, values in sorted(samples.items(), key=lambda item: _mlx_sort_key(item[0]))
        if values
    }


def _mlx_sort_key(device: str) -> tuple[int, int, str]:
    suffix = device.rsplit("_", 1)[-1]
    dev_part, _, port_part = suffix.partition(".")
    dev_index = int(dev_part) if dev_part.isdigit() else 10_000
    port_index = int(port_part) if port_part.isdigit() else 1
    return (dev_index, port_index, device)


def finalize_summary(
    metrics_path: Path,
    ibbw_log_path: Path,
    output_path: Path,
    *,
    ibbw_log_reference: str,
    require_hca_samples: bool,
) -> None:
    metrics = json.loads(
        _read_private_regular_file(metrics_path, max_bytes=_MAX_METRICS_BYTES)
    )
    if not isinstance(metrics, dict):
        raise ValueError("NCCL staged metrics must be a JSON object")
    if require_hca_samples or ibbw_log_path.exists():
        ibbw_payload = _read_private_regular_file(
            ibbw_log_path, max_bytes=_MAX_IBBW_BYTES
        ).decode("utf-8", errors="replace")
    else:
        ibbw_payload = ""
    ports = summarize_ibbw(ibbw_payload)
    if require_hca_samples and not any(
        re.fullmatch(r"mlx5_(?:[0-9]|1[0-3])", port) for port in ports
    ):
        raise ValueError("NCCL IBBW log has no sampled mlx5_0..mlx5_13 HCA port")
    metrics["GCR_IB_PORT_BW_GBPS"] = ports
    metrics["GCR_IBBW_LOG_FILE"] = ibbw_log_reference
    payload = (json.dumps(metrics, indent=4, sort_keys=True) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(output_path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write while finalizing NCCL summary")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--ibbw-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ibbw-log-reference", required=True)
    parser.add_argument("--require-hca-samples", action="store_true")
    args = parser.parse_args(argv)
    finalize_summary(
        args.metrics,
        args.ibbw_log,
        args.output,
        ibbw_log_reference=args.ibbw_log_reference,
        require_hca_samples=args.require_hca_samples,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())