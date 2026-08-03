"""GPU-pod runtime evidence for NCCL baseline profile identity."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4


RUNTIME_EVIDENCE_SCHEMA = "cval.nccl-runtime-evidence.v1"
_RUNTIME_EVIDENCE_MODE = 0o600
_COMMAND_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class RuntimeEvidence:
    """Immutable hardware/runtime facts collected inside the NCCL GPU pod."""

    schema_version: str
    gpu_model: str
    compiled_nccl_version: str
    runtime_nccl_package_version: str
    driver_version: str
    driver_version_group: str
    topology_class: str

    def __post_init__(self) -> None:
        if self.schema_version != RUNTIME_EVIDENCE_SCHEMA:
            raise ValueError(
                f"schema_version must be {RUNTIME_EVIDENCE_SCHEMA!r}"
            )
        for field_name in (
            "gpu_model",
            "compiled_nccl_version",
            "runtime_nccl_package_version",
            "driver_version",
            "driver_version_group",
            "topology_class",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or not value.strip()
                or len(value) > 256
                or "\n" in value
                or "\r" in value
            ):
                raise ValueError(
                    f"{field_name} must be a non-empty single-line string of at most 256 characters"
                )
        if self.driver_version_group != self.driver_version:
            raise ValueError(
                "driver_version_group must default to the exact driver_version"
            )
        if not self.topology_class.startswith("nvidia-topo-sha256:"):
            raise ValueError("topology_class must be a normalized NVIDIA topology SHA-256 label")
        digest = self.topology_class.removeprefix("nvidia-topo-sha256:")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("topology_class must end in a lowercase SHA-256 digest")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimeEvidence":
        if not isinstance(value, Mapping):
            raise ValueError("NCCL runtime evidence must be an object")
        allowed = {
            "schema_version",
            "gpu_model",
            "compiled_nccl_version",
            "runtime_nccl_package_version",
            "driver_version",
            "driver_version_group",
            "topology_class",
        }
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValueError(
                "NCCL runtime evidence has unknown field(s): " + ", ".join(unknown)
            )
        fields: dict[str, str] = {}
        for field_name in allowed:
            item = value.get(field_name)
            if not isinstance(item, str):
                raise ValueError(f"NCCL runtime evidence {field_name} must be a string")
            fields[field_name] = item
        return cls(**fields)

    def to_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "gpu_model": self.gpu_model,
            "compiled_nccl_version": self.compiled_nccl_version,
            "runtime_nccl_package_version": self.runtime_nccl_package_version,
            "driver_version": self.driver_version,
            "driver_version_group": self.driver_version_group,
            "topology_class": self.topology_class,
        }


CommandRunner = Callable[..., Any]


def collect_runtime_evidence(
    *,
    torch_module: Any | None = None,
    command_runner: CommandRunner = subprocess.run,
    package_version_resolver: Callable[[str], str] = importlib.metadata.version,
) -> RuntimeEvidence:
    """Collect required facts without importing torch outside the GPU runtime path."""

    torch = torch_module
    if torch is None:
        try:
            torch = importlib.import_module("torch")
        except (ImportError, OSError) as exc:
            raise RuntimeError("PyTorch is required to collect NCCL runtime evidence") from exc

    cuda = getattr(torch, "cuda", None)
    if cuda is None or not bool(cuda.is_available()):
        raise RuntimeError("CUDA is unavailable; NCCL runtime evidence cannot be collected")
    device_count = cuda.device_count()
    if isinstance(device_count, bool) or not isinstance(device_count, int) or device_count <= 0:
        raise RuntimeError("CUDA reported no GPU devices")
    models = tuple(_required_fact(cuda.get_device_name(index), "GPU model") for index in range(device_count))
    if len(set(models)) != 1:
        raise RuntimeError("NCCL runtime evidence requires one exact GPU model per test run")

    nccl = getattr(cuda, "nccl", None)
    if nccl is None or not callable(getattr(nccl, "version", None)):
        raise RuntimeError("torch.cuda.nccl.version is unavailable")
    compiled_nccl_version = normalize_nccl_version(nccl.version())

    driver_output = _run_nvidia_smi(
        (
            "nvidia-smi",
            "--query-gpu=driver_version",
            "--format=csv,noheader,nounits",
        ),
        command_runner,
    )
    driver_versions = tuple(
        line.strip() for line in driver_output.splitlines() if line.strip()
    )
    if not driver_versions:
        raise RuntimeError("nvidia-smi returned no driver version")
    for value in driver_versions:
        _required_fact(value, "driver version")
    if len(set(driver_versions)) != 1:
        raise RuntimeError("NCCL runtime evidence requires one exact driver version per test run")
    driver_version = driver_versions[0]

    topology_output = _run_nvidia_smi(("nvidia-smi", "topo", "-m"), command_runner)
    topology_class = topology_class_from_output(topology_output)
    runtime_nccl_package_version = _runtime_nccl_package_version(
        package_version_resolver
    )
    return RuntimeEvidence(
        schema_version=RUNTIME_EVIDENCE_SCHEMA,
        gpu_model=models[0],
        compiled_nccl_version=compiled_nccl_version,
        runtime_nccl_package_version=runtime_nccl_package_version,
        driver_version=driver_version,
        driver_version_group=driver_version,
        topology_class=topology_class,
    )


def normalize_nccl_version(value: object) -> str:
    """Normalize the documented tuple or integer torch NCCL version forms."""

    if isinstance(value, bool):
        raise RuntimeError("torch.cuda.nccl.version returned an invalid value")
    if isinstance(value, int):
        if value <= 0:
            raise RuntimeError("torch.cuda.nccl.version returned an invalid value")
        parts = (
            (value // 1000, (value % 1000) // 100, value % 100)
            if value < 10000
            else (value // 10000, (value % 10000) // 100, value % 100)
        )
        suffix = None
    elif isinstance(value, list | tuple) and len(value) in {3, 4}:
        numeric = value[:3]
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in numeric
        ):
            raise RuntimeError("torch.cuda.nccl.version returned an invalid value")
        parts = tuple(numeric)
        suffix = value[3] if len(value) == 4 else None
        if suffix is not None and (
            not isinstance(suffix, str | int)
            or isinstance(suffix, bool)
            or not str(suffix).strip()
            or not re.fullmatch(r"[A-Za-z0-9._+-]+", str(suffix))
        ):
            raise RuntimeError("torch.cuda.nccl.version returned an invalid suffix")
    else:
        raise RuntimeError("torch.cuda.nccl.version returned an invalid value")
    normalized = ".".join(str(item) for item in parts)
    if suffix is not None:
        normalized += f"+{str(suffix).strip()}"
    return _required_fact(normalized, "NCCL version")


def normalize_topology_output(value: str) -> str:
    """Canonicalize a supported NVIDIA GPU/NIC graph independent of labels/order."""

    node_types, affinities, edges = _parse_topology(value)
    colors = {
        node: f"{node_types[node]}|cpu={affinities.get(node, 'unknown')}"
        for node in node_types
    }
    for _ in range(len(node_types) + 1):
        refined = {}
        for node in node_types:
            neighbors = sorted(
                f"{edge_type}:{colors[other]}"
                for left, other, edge_type in edges
                if left == node
            )
            material = colors[node] + "|" + "|".join(neighbors)
            refined[node] = hashlib.sha256(material.encode("utf-8")).hexdigest()
        if all(refined[node] == colors[node] for node in node_types):
            break
        colors = refined
    vertices = sorted(
        f"{node_types[node]}|cpu={affinities.get(node, 'unknown')}|color={colors[node]}"
        for node in node_types
    )
    undirected = sorted(
        f"{edge_type}|{min(colors[left], colors[right])}|{max(colors[left], colors[right])}"
        for left, right, edge_type in edges
        if left < right
    )
    return "vertices\n" + "\n".join(vertices) + "\nedges\n" + "\n".join(undirected) + "\n"


def topology_class_from_output(value: str) -> str:
    normalized = normalize_topology_output(value)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"nvidia-topo-sha256:{digest}"


def write_runtime_evidence(path: Path, evidence: RuntimeEvidence) -> dict[str, object]:
    """Create immutable evidence atomically, accepting only a byte-equal retry."""

    if not isinstance(evidence, RuntimeEvidence):
        raise TypeError("evidence must be RuntimeEvidence")
    payload = (
        json.dumps(
            evidence.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    created = atomic_write_once(path, payload, mode=_RUNTIME_EVIDENCE_MODE)
    return {
        "schema_version": evidence.schema_version,
        "path": str(path),
        "created": created,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
    }


def load_runtime_evidence(path: Path) -> RuntimeEvidence:
    payload = read_exact_regular_file(path, mode=_RUNTIME_EVIDENCE_MODE, maximum_bytes=16384)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("NCCL runtime evidence is not valid UTF-8 JSON") from exc
    return RuntimeEvidence.from_dict(value)


def atomic_write_once(path: Path, payload: bytes, *, mode: int) -> bool:
    """Atomically link a complete file into place without replacement."""

    path = Path(path)
    if not path.is_absolute():
        raise ValueError("immutable output path must be absolute")
    if path.name in {"", ".", ".."} or "/" in path.name:
        raise ValueError("immutable output path must end in a safe basename")
    parent_fd = _open_output_directory(path.parent)
    temporary = f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            mode,
            dir_fd=parent_fd,
        )
        os.fchmod(descriptor, mode)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write while creating immutable output")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        try:
            os.link(
                temporary,
                path.name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            existing = _read_regular_at(
                parent_fd,
                path.name,
                mode=mode,
                maximum_bytes=max(len(payload), 16 * 1024 * 1024),
            )
            if existing != payload:
                raise FileExistsError(
                    f"immutable output conflict for existing path: {path}"
                )
            return False
        os.fsync(parent_fd)
        return True
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        os.close(parent_fd)


def read_exact_regular_file(path: Path, *, mode: int, maximum_bytes: int) -> bytes:
    path = Path(path)
    parent_fd = _open_output_directory(path.parent)
    try:
        return _read_regular_at(parent_fd, path.name, mode=mode, maximum_bytes=maximum_bytes)
    finally:
        os.close(parent_fd)


def _open_output_directory(path: Path) -> int:
    """Open a normal directory or duplicate an inherited supervisor directory FD."""

    path = Path(path)
    match = re.fullmatch(r"/proc/self/fd/([0-9]+)", str(path))
    if match is not None:
        descriptor = os.dup(int(match.group(1)))
        try:
            if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise NotADirectoryError(str(path))
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise
    return os.open(
        path,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )


def _read_regular_at(
    parent_fd: int,
    name: str,
    *,
    mode: int,
    maximum_bytes: int,
) -> bytes:
    descriptor = os.open(
        name,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=parent_fd,
    )
    try:
        value = os.fstat(descriptor)
        if (
            not stat.S_ISREG(value.st_mode)
            or stat.S_IMODE(value.st_mode) != mode
            or value.st_nlink != 1
            or value.st_size > maximum_bytes
        ):
            raise PermissionError(f"immutable input file is unsafe: {name}")
        before = (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_nlink,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
        chunks: list[bytes] = []
        remaining = value.st_size
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                raise OSError(f"immutable input file changed while reading: {name}")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise OSError(f"immutable input file grew while reading: {name}")
        after_value = os.fstat(descriptor)
        after = (
            after_value.st_dev,
            after_value.st_ino,
            after_value.st_mode,
            after_value.st_nlink,
            after_value.st_size,
            after_value.st_mtime_ns,
            after_value.st_ctime_ns,
        )
        path_value = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        path_after = (
            path_value.st_dev,
            path_value.st_ino,
            path_value.st_mode,
            path_value.st_nlink,
            path_value.st_size,
            path_value.st_mtime_ns,
            path_value.st_ctime_ns,
        )
        if after != before or path_after != before:
            raise OSError(f"immutable input file changed while reading: {name}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _run_nvidia_smi(command: Sequence[str], runner: CommandRunner) -> str:
    try:
        completed = runner(
            list(command),
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"bounded command failed: {' '.join(command)}") from exc
    output = getattr(completed, "stdout", None)
    if not isinstance(output, str) or not output.strip():
        raise RuntimeError(f"bounded command returned no data: {' '.join(command)}")
    return output


def _required_fact(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{name} is absent")
    normalized = value.strip()
    if len(normalized) > 256 or "\n" in normalized or "\r" in normalized:
        raise RuntimeError(f"{name} is not a bounded single-line fact")
    return normalized


def _runtime_nccl_package_version(resolver: Callable[[str], str]) -> str:
    found: list[tuple[str, str]] = []
    for package in ("nvidia-nccl-cu13", "nvidia-nccl-cu12", "nvidia-nccl-cu11", "nccl"):
        try:
            version = resolver(package)
        except importlib.metadata.PackageNotFoundError:
            continue
        except Exception as exc:  # noqa: BLE001 - package metadata boundary
            raise RuntimeError("NCCL runtime package metadata lookup failed") from exc
        found.append((package, _required_fact(version, f"{package} package version")))
    if not found:
        raise RuntimeError("NCCL runtime package metadata is absent")
    if len(found) != 1:
        raise RuntimeError("multiple NCCL runtime packages are installed")
    return f"{found[0][0]}=={found[0][1]}"


def _parse_topology(
    value: str,
) -> tuple[dict[str, str], dict[str, str], list[tuple[str, str, str]]]:
    if not isinstance(value, str):
        raise TypeError("NVIDIA topology output must be text")
    lines = [" ".join(line.strip().split()) for line in value.splitlines() if line.strip()]
    lines = lines[: next((index for index, line in enumerate(lines) if line.lower().startswith("legend")), len(lines))]
    header_index = next(
        (
            index
            for index, line in enumerate(lines)
            if len(re.findall(r"\b(?:GPU|NIC)\d+\b", line)) >= 1
            and ("CPU Affinity" in line or not re.match(r"^(?:GPU|NIC)\d+\b", line))
        ),
        None,
    )
    if header_index is None:
        raise RuntimeError("unsupported nvidia-smi topology header")
    header_nodes = re.findall(r"\b(?:GPU|NIC)\d+\b", lines[header_index])
    if not header_nodes or len(header_nodes) != len(set(header_nodes)):
        raise RuntimeError("unsupported nvidia-smi topology device header")
    rows: dict[str, tuple[list[str], str]] = {}
    for line in lines[header_index + 1 :]:
        fields = line.split()
        if not fields or not re.fullmatch(r"(?:GPU|NIC)\d+", fields[0]):
            continue
        if len(fields) < 1 + len(header_nodes):
            raise RuntimeError("unsupported nvidia-smi topology row")
        node = fields[0]
        if node in rows:
            raise RuntimeError("duplicate nvidia-smi topology row")
        relations = fields[1 : 1 + len(header_nodes)]
        if any(not re.fullmatch(r"[A-Za-z0-9._+-]+", item) for item in relations):
            raise RuntimeError("unsupported nvidia-smi topology edge")
        affinity = fields[1 + len(header_nodes)] if len(fields) > 1 + len(header_nodes) else "unknown"
        if not re.fullmatch(r"(?:N/A|unknown|[0-9,-]+)", affinity):
            raise RuntimeError("unsupported nvidia-smi CPU affinity")
        rows[node] = (relations, affinity)
    if set(rows) != set(header_nodes):
        raise RuntimeError("nvidia-smi topology rows do not match the device header")
    edges: list[tuple[str, str, str]] = []
    for left, (relations, _affinity) in rows.items():
        for right, edge_type in zip(header_nodes, relations, strict=True):
            if left == right:
                if edge_type != "X":
                    raise RuntimeError("nvidia-smi topology diagonal must be X")
                continue
            reverse = rows[right][0][header_nodes.index(left)]
            if reverse != edge_type:
                raise RuntimeError("nvidia-smi topology matrix is asymmetric")
            edges.append((left, right, edge_type))
    return (
        {node: "GPU" if node.startswith("GPU") else "NIC" for node in header_nodes},
        {node: rows[node][1] for node in header_nodes},
        edges,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect immutable NCCL runtime evidence")
    paths = parser.add_mutually_exclusive_group(required=True)
    paths.add_argument("--output", type=Path)
    paths.add_argument("--validate", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.validate is not None:
            evidence = load_runtime_evidence(args.validate)
            receipt = {
                "mode": "validate",
                "valid": True,
                "path": str(args.validate),
                "schema_version": evidence.schema_version,
            }
        else:
            receipt = write_runtime_evidence(args.output, collect_runtime_evidence())
    except Exception as exc:  # noqa: BLE001 - GPU-pod process boundary
        print(f"NCCL runtime evidence collection failed: {exc}", file=os.sys.stderr)
        return 1
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
