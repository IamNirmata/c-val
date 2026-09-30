"""Read-only exact-run evidence verification shared by admission and validation."""

from __future__ import annotations

import base64
from collections import Counter
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import shlex
import time
import zlib

from cval.storage.sqlite_uri import SQLiteFileIdentity, assert_sqlite_connection_identity, assert_sqlite_file_identity, canonical_sqlite_path, connect_sqlite_file
from cval.validation.builtins import project_builtin_statuses
from cval.validation.results import COMPACT_RESULT_SCHEMA, parse_validation_result_v2, validation_result_digest, validation_result_to_env, recording_complete


class EvidenceError(ValueError):
    def __init__(self, state: str, message: str):
        super().__init__(message)
        self.state = state


def _failure(error: Exception) -> dict:
    if isinstance(error, EvidenceError):
        state = error.state
    elif isinstance(error, FileNotFoundError):
        state = "incomplete"
    elif isinstance(error, PermissionError):
        state = "forbidden"
    elif isinstance(error, (OSError, sqlite3.Error, RuntimeError)):
        state = "unavailable"
    else:
        state = "conflicting"
    return {"state": state, "reason": str(error)}


class EvidenceReader:
    def __init__(self, request: dict):
        self.request = request
        self.deadline = time.monotonic() + request["timeout_seconds"]
        self.files = {}
        self.databases = {}
        self.bytes_read = 0

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Exact-run verification deadline expired")
        return remaining

    def read(self, path: Path) -> bytes:
        self.remaining()
        canonical_sqlite_path(path, must_exist=True)
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(descriptor)
            if path in self.files and self.files[path][0] != self._identity(before):
                raise EvidenceError("unavailable", f"Evidence changed between reads: {path}")
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                raise ValueError(f"Evidence is not a single-link regular file: {path}")
            if path not in self.files and len(self.files) >= self.request["max_files"]:
                raise EvidenceError("unavailable", "Configured evidence file limit exceeded")
            remaining = self.request["max_bytes"] - self.bytes_read
            if before.st_size > remaining:
                raise EvidenceError("unavailable", "Configured evidence byte limit exceeded")
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                content = handle.read(remaining + 1)
            if len(content) > remaining or self._identity(before) != self._identity(os.fstat(descriptor)):
                raise EvidenceError("unavailable", "Evidence changed during read")
            self.bytes_read += len(content)
            self.files[path] = (self._identity(before), hashlib.sha256(content).hexdigest())
            return content
        finally:
            os.close(descriptor)

    def json(self, path: Path):
        try:
            return json.loads(self.read(path))
        except json.JSONDecodeError as error:
            raise EvidenceError("conflicting", f"Malformed JSON evidence {path}: {error}") from error

    def unchanged(self) -> None:
        for path, (identity, _digest) in self.files.items():
            self.remaining()
            canonical_sqlite_path(path, must_exist=True)
            if self._identity(os.stat(path, follow_symlinks=False)) != identity:
                raise EvidenceError("unavailable", f"Evidence changed during verification: {path}")
        for identity in self.databases.values():
            assert_sqlite_file_identity(identity)

    @staticmethod
    def _identity(metadata):
        return (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns)

    def rows(self, path: str, sql: str, parameters: tuple) -> list[dict]:
        if path not in self.databases:
            self.databases[path] = SQLiteFileIdentity.capture(path)
        identity = self.databases[path]
        connection = connect_sqlite_file(path, mode="ro", expected_identity=identity, timeout=min(self.remaining(), self.request["sqlite_timeout_seconds"]))
        try:
            connection.execute("PRAGMA query_only=ON")
            connection.row_factory = sqlite3.Row
            connection.set_progress_handler(lambda: int(time.monotonic() >= self.deadline), 1000)
            rows = [dict(row) for row in connection.execute(sql, parameters)]
            assert_sqlite_connection_identity(connection, path, expected_identity=identity)
            return rows
        finally:
            connection.close()


def _same_rows(expected: list[dict], observed: list[dict]) -> None:
    encode = lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False)
    wanted = Counter(encode(row) for row in expected)
    actual = Counter(encode(row) for row in observed)
    if wanted == actual:
        return
    if not observed or (not (actual - wanted) and wanted - actual):
        raise EvidenceError("incomplete", f"Expected {len(expected)} rows, found {len(observed)}")
    raise EvidenceError("conflicting", "Exact-run rows differ from canonical artifact values")


def _canonical(reader: EvidenceReader):
    request = reader.request
    root = Path(request["root"])
    run_dir = root / "logs/job_logs" / request["node"] / request["run_id"]
    result = parse_validation_result_v2(reader.json(run_dir / "result.json"))
    for field, expected in (
        ("node", request["node"]), ("timestamp", request["timestamp"]), ("run_id", request["run_id"]),
        ("git_ref", request["git_ref"]), ("global_config_digest", request["config_digest"]),
        ("image_name", request["image_name"]),
    ):
        if getattr(result, field) != expected:
            raise EvidenceError("conflicting", f"Canonical {field} differs from the expected request")
    if set(result.tests) != set(request["tests"]):
        raise EvidenceError("conflicting", "Canonical test set differs from the expected request")
    if not result.completed_at:
        raise EvidenceError("incomplete", "Canonical run has not completed")
    digest = validation_result_digest(result)
    compact = result.schema_version == COMPACT_RESULT_SCHEMA
    ingestion = {"state": "complete"}
    try:
        if compact:
            if not recording_complete(result):
                raise EvidenceError("incomplete", "Final result lacks complete file/log/DB acknowledgements")
        elif reader.read(run_dir / ".ingestion-result-digest") != (digest + "\n").encode():
            raise EvidenceError("conflicting", "Bound ingestion digest differs from canonical result")
    except Exception as error:
        ingestion = _failure(error)
    for test_id, test in result.tests.items():
        expected = request["tests"][test_id]
        if (test.enabled, test.selected, test.config_path, test.config_digest, test.order) != (
            expected["enabled"], expected["enabled"], expected["config_path"], expected["config_digest"], expected["order"],
        ):
            raise EvidenceError("conflicting", f"Canonical test definition differs: {test_id}")
        if not test.selected:
            continue
        log_dir = root / "logs" / test_id / request["node"] / request["run_id"]
        test_dir = root / "logs/validation_tests" / test_id / "runs" / request["node"] / request["run_id"]
        paths = {"stdout": log_dir / "stdout.log", "stderr": log_dir / "stderr.log", "log": log_dir / "events.jsonl",
                 "result": test_dir / "result.json", "summary": test_dir / expected["summary_filename"], "artifacts": test_dir / "artifacts"}
        if compact:
            paths = {"log": log_dir / "job.log", "summary": log_dir / expected["summary_filename"], "artifacts": log_dir}
        for field, path in paths.items():
            if getattr(test, field) != str(path):
                raise EvidenceError("conflicting", f"Noncanonical {test_id}.{field}")
            if field not in {"summary", "artifacts"}:
                reader.read(path)
        if test.status == "pass" and not reader.read(paths["summary"]).strip():
            raise EvidenceError("incomplete", f"Passing test summary is empty: {test_id}")
        if not compact:
            test_result = reader.json(paths["result"])
            expected_result = {"schema_version": "cval.test-result.v1", "test_id": test_id} | {
                field: getattr(test, field) for field in ("status", "phase", "started_at", "completed_at", "duration_ms", "exit_code", "summary", "artifacts", "message")
            }
            if test_result != expected_result:
                raise EvidenceError("conflicting", f"Per-test result differs from canonical envelope: {test_id}")
    if compact:
        events = [json.loads(line.removeprefix(b"CVAL_EVENT ")) for line in reader.read(run_dir / "job.log").splitlines() if line.startswith(b"CVAL_EVENT ")]
    else:
        for filename in ("stdout.log", "stderr.log", "job.log"):
            reader.read(run_dir / filename)
        events = [json.loads(line) for line in reader.read(run_dir / "events.jsonl").splitlines() if line.strip()]
    if any(event.get("schema_version") != "cval.event.v1" or event.get("run_id") != result.run_id for event in events):
        raise EvidenceError("conflicting", "Canonical event identity or schema differs")
    finished = [index for index, event in enumerate(events) if event.get("event") == "run_finished"]
    starts = [index for index, event in enumerate(events) if event.get("event") == "ingestion_started"]
    ends = [index for index, event in enumerate(events) if event.get("event") == "ingestion_finished"]
    if not finished:
        raise EvidenceError("incomplete", "Terminal execution event is missing")
    if not starts or not ends:
        ingestion = ingestion if ingestion["state"] != "complete" else {"state": "incomplete", "reason": "Terminal ingestion events are missing"}
    elif not finished[-1] < starts[-1] < ends[-1]:
        raise EvidenceError("conflicting", "Execution and ingestion event ordering conflicts")
    if events[finished[-1]].get("status") != result.overall:
        raise EvidenceError("conflicting", "Run-finished event differs from canonical outcome")
    if ends and events[ends[-1]].get("status") != "pass":
        ingestion = ingestion if ingestion["state"] != "complete" else {"state": "incomplete", "reason": "Ingestion did not finish successfully"}
    if compact and ends:
        acknowledgement = events[ends[-1]]
        if (acknowledgement.get("producer"), acknowledgement.get("node_name"), acknowledgement.get("result_digest"), acknowledgement.get("config_digest")) != ("ingestion", result.node, digest, result.global_config_digest):
            raise EvidenceError("conflicting", "Ingestion event identity differs from the final result")
        if result.completion and acknowledgement.get("stores") != result.completion["stores"]:
            raise EvidenceError("conflicting", "Final result stores differ from ingestion acknowledgement")
        terminal = [index for index, event in enumerate(events) if event.get("event") == "supervisor_finished"]
        if not terminal or terminal[-1] <= ends[-1] or events[terminal[-1]].get("logs_state") != "retained":
            raise EvidenceError("incomplete", "Supervisor log finalization is not confirmed")
    for name, test in result.tests.items():
        if test.selected and not any(event.get("test") == name and event.get("status") == test.status and event.get("phase") == test.phase for event in events[:finished[-1]]):
            raise EvidenceError("incomplete", f"Terminal test diagnostic event missing: {name}")
    return result, digest, ingestion


def _storage(reader: EvidenceReader, result) -> dict:
    from cval.storage.ingest import STORAGE_FILE_PREFIXES, parse_storage_metrics

    path = Path(result.tests["storage"].artifacts)
    for name in STORAGE_FILE_PREFIXES:
        reader.read(path / name)
    summary_name = reader.request["tests"]["storage"]["summary_filename"] if result.schema_version == COMPACT_RESULT_SCHEMA else None
    metrics = parse_storage_metrics(path, summary_filename=summary_name)
    expected = {"node": result.node, "timestamp": result.timestamp, "image_name": result.image_name, **metrics}
    rows = reader.rows(reader.request["db_paths"]["storage"],
                       f"SELECT {','.join(expected)} FROM storage_performance WHERE node=? AND timestamp=?",
                       (result.node, result.timestamp))
    _same_rows([expected], rows)
    return {"state": "complete", "row_count": len(rows), "metrics_compared": len(metrics)}


def _nccl(reader: EvidenceReader, result) -> dict:
    from cval.storage.ingest import NCCL_IB_PORT_COLUMNS, parse_nccl_health_summary
    from cval.validation.nccl_summary import summarize_ibbw

    test = result.tests["nccl"]
    settings = reader.request["tests"]["nccl"]["settings"]
    summary = reader.json(Path(test.summary))
    metrics = parse_nccl_health_summary(test.summary, iterations=settings["iterations"],
                                       data_size_gb=settings["data_size_gb"], require_hca_samples=settings["ibbw_enabled"])
    if settings["ibbw_enabled"]:
        log_path = Path(summary["GCR_IBBW_LOG_FILE"])
        allowed = {Path(test.artifacts) / "ibbw.log", Path(test.artifacts) / f"ibbw-{result.node}-{result.timestamp}.log"}
        if log_path not in allowed:
            raise EvidenceError("conflicting", "NCCL sampler path is not canonical")
        ports = summarize_ibbw(reader.read(log_path).decode())
        if ports != summary["GCR_IB_PORT_BW_GBPS"]:
            raise EvidenceError("conflicting", "NCCL summary differs from retained HCA samples")
    expected = {"Node": result.node, "timestamp": result.timestamp, "iterations": metrics.iterations,
                "data_size_gb": metrics.data_size_gb, "image_name": result.image_name, "cuda": result.cuda_version,
                "pytorch": result.pytorch_version, "samples": metrics.samples, "BUS_BW": metrics.bus_bw,
                "LATENCY": metrics.latency, **{name: metrics.port_max_gbps.get(name) for name in NCCL_IB_PORT_COLUMNS}}
    rows = reader.rows(reader.request["db_paths"]["nccl"],
                       f"SELECT {','.join(expected)} FROM IB_HEALTH WHERE Node=? AND timestamp=?", (result.node, result.timestamp))
    _same_rows([expected], rows)
    return {"state": "complete", "row_count": len(rows), "hca_ports_compared": len(metrics.port_max_gbps)}


_DL_COMPONENTS = ("numerical_correctness", "compute_performance", "collective_performance", "overlap_performance")


def _generations(reader: EvidenceReader) -> dict:
    values = {}
    for name in _DL_COMPONENTS:
        path = reader.request["db_paths"][name]
        if reader.rows(path, "PRAGMA journal_mode", ())[0]["journal_mode"] != "delete":
            raise EvidenceError("unavailable", "DL journal mode is not DELETE")
        rows = reader.rows(path, "SELECT generation_id,state,updated_at FROM cval_ingest_metadata WHERE id=1", ())
        if len(rows) != 1 or not rows[0]["generation_id"] or rows[0]["state"] != "complete":
            raise EvidenceError("unavailable", "DL generation is missing or in progress")
        values[name] = rows[0]
    if len({row["generation_id"] for row in values.values()}) != 1:
        raise EvidenceError("unavailable", "DL generation publication is not consistent")
    return values


def _dl(reader: EvidenceReader, result) -> dict:
    from cval.storage.dltest_ingest import classify_rank_files, load_rank_files

    test = result.tests["dltest"]
    run_dir = Path(test.summary).parent
    summary = reader.json(Path(test.summary))
    expected = reader.request["tests"]["dltest"]
    gpu_count = expected["settings"]["gpu_count"]
    if summary.get("status") != "pass" or summary.get("iterations") != expected["settings"]["iterations"] or summary.get("gpu_count") != gpu_count:
        raise EvidenceError("conflicting", "DL summary disagrees with requested workload")
    rank_paths = sorted({path for pattern in ("workdir/test_plans/*/runs/*.json", "artifacts/workdir/test_plans/*/runs/*.json") for path in run_dir.glob(pattern)})
    for path in rank_paths:
        reader.read(path)
    rank_files = list(load_rank_files(run_dir))
    if len(rank_files) != gpu_count or {rank.rank for rank in rank_files} != set(range(gpu_count)):
        raise EvidenceError("incomplete", "DL rank coverage is incomplete")
    if {rank.test_plan for rank in rank_files} != {expected["settings"]["test_plan"]}:
        raise EvidenceError("conflicting", "DL rank artifacts differ from the requested test plan")
    if {rank.path for rank in rank_files} != set(rank_paths) or any((rank.node, rank.cval_timestamp) != (result.node, result.timestamp) for rank in rank_files):
        raise EvidenceError("conflicting", "DL artifact identity differs from the requested run")
    rows_by_component = dict(zip(_DL_COMPONENTS, classify_rank_files(rank_files)))
    for attempt in range(reader.request["generation_attempts"]):
        try:
            before = _generations(reader)
            components = {}
            for name, expected_rows in rows_by_component.items():
                try:
                    if not expected_rows:
                        raise EvidenceError("incomplete", f"No expected DL rows for {name}")
                    expected_values = [asdict(row) for row in expected_rows]
                    columns = list(expected_values[0])
                    path = reader.request["db_paths"][name]
                    rows = reader.rows(path, f"SELECT {','.join(columns)} FROM {name} WHERE node=? AND cval_timestamp=?",
                                       (result.node, result.timestamp))
                    keys = {row["run_key"] for row in rows}
                    if not rows:
                        raise EvidenceError("incomplete", f"No exact-run DL rows for {name}")
                    if len(keys) != 1:
                        raise EvidenceError("conflicting", f"Multiple stored DL run keys for {name}")
                    stored_key = next(iter(keys))
                    for row in expected_values:
                        row["run_key"] = stored_key
                    _same_rows(expected_values, rows)
                    receipts = reader.rows(path, "SELECT run_key,sample_dir FROM cval_ingested_runs WHERE run_key=?", (stored_key,))
                    _same_rows([{"run_key": stored_key, "sample_dir": str(run_dir)}], receipts)
                    components[name] = {"state": "complete", "row_count": len(rows), "ranks": len(rank_files), "run_key": stored_key, "receipts": len(receipts)}
                except Exception as error:
                    components[name] = _failure(error)
            after = _generations(reader)
            if before != after:
                raise EvidenceError("unavailable", "DL generation changed during verification")
            return {"state": _combined_state(components), "components": components,
                    "generation": next(iter(before.values()))["generation_id"], "attempts": attempt + 1}
        except EvidenceError as error:
            if error.state != "unavailable" or attempt + 1 == reader.request["generation_attempts"]:
                raise
            time.sleep(min(reader.remaining(), reader.request["generation_delay_seconds"]))
    raise EvidenceError("unavailable", "DL verification retry budget exhausted")


def _combined_state(sources: dict) -> str:
    for state in ("forbidden", "unavailable", "conflicting", "incomplete"):
        if any(source["state"] == state for source in sources.values()):
            return state
    return "complete"


def verify_local_evidence(request: dict) -> dict:
    reader = EvidenceReader(request)
    report = {"state": "unavailable", "accepted": False, "complete": False, "request": {
        key: request[key] for key in ("node", "timestamp", "run_id", "git_ref", "config_digest")
    }, "sources": {}, "raw_results": {}}
    try:
        result, digest, ingestion = _canonical(reader)
        report["result_digest"] = digest
        report["raw_results"] = {name: test.status for name, test in result.tests.items()} | {"all": result.overall}
        report["sources"]["canonical"] = {"state": "complete"}
        report["sources"]["ingestion"] = ingestion
    except Exception as error:
        report["sources"]["canonical"] = _failure(error)
        report["state"] = report["sources"]["canonical"]["state"]
        return report
    statuses = project_builtin_statuses(validation_result_to_env(result))
    expected = [{"test": name, "result": value, "image_name": result.image_name,
                 "pytorch_version": result.pytorch_version, "cuda_version": result.cuda_version} for name, value in statuses.items()]
    try:
        rows = reader.rows(request["db_paths"]["validation"],
                           "SELECT test,result,image_name,pytorch_version,cuda_version FROM runs WHERE node=? AND timestamp=?",
                           (request["node"], request["timestamp"]))
        _same_rows(expected, [dict(items) for items in {tuple(row.items()) for row in rows}])
        report["sources"]["validation"] = {"state": "complete", "row_count": len(rows)}
    except Exception as error:
        report["sources"]["validation"] = _failure(error)
    for name, verify in (("storage", _storage), ("nccl", _nccl), ("dltest", _dl)):
        if name not in result.tests or not result.tests[name].selected or result.tests[name].status != "pass":
            report["sources"][name] = {"state": "not_required", "reason": "No passing selected outcome"}
            continue
        try:
            report["sources"][name] = verify(reader, result)
        except Exception as error:
            report["sources"][name] = _failure(error)
    try:
        reader.unchanged()
    except Exception as error:
        report["sources"]["stability"] = _failure(error)
    report["state"] = _combined_state(report["sources"])
    report["complete"] = report["state"] == "complete"
    selected = [test for test in result.tests.values() if test.enabled and test.selected]
    report["tests_passed"] = bool(selected) and not result.errors and all(test.status == "pass" and test.phase == "finished" for test in selected)
    report["files"] = {str(path): {"sha256": digest, "bytes": identity[2]} for path, (identity, digest) in reader.files.items()}
    return report


_MEMORY_READER = r'''
import base64, hashlib, importlib.abc, importlib.util, json, signal, sys, zlib
sys.dont_write_bytecode = True
payload = json.load(sys.stdin)
sources = json.loads(zlib.decompress(base64.b64decode(payload["sources"])))
class SourceLoader(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in sources:
            filename, source, package = sources[fullname]
            return importlib.util.spec_from_loader(fullname, self, origin="/__cval_readonly__/" + filename, is_package=package)
    def create_module(self, spec):
        return None
    def get_data(self, path):
        for filename, source, package in sources.values():
            if path == "/__cval_readonly__/" + filename:
                return source.encode("utf-8")
        raise FileNotFoundError(path)
    def exec_module(self, module):
        filename, source, package = sources[module.__name__]
        module.__file__ = "/__cval_readonly__/" + filename
        exec(compile(source, module.__file__, "exec"), module.__dict__)
sys.meta_path.insert(0, SourceLoader())
def expired(signum, frame):
    raise TimeoutError("Exact-run verification deadline expired")
signal.signal(signal.SIGALRM, expired)
signal.setitimer(signal.ITIMER_REAL, payload["request"]["timeout_seconds"])
if payload["operation"] == "coverage":
    from cval.storage.status import read_local_coverage as collect
elif payload["operation"] == "verification":
    from cval.storage.verification import verify_local_evidence as collect
else:
    raise ValueError("Unknown read-only collection operation")
report = json.dumps(collect(payload["request"]), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
print(json.dumps({"encoding": "zlib+base64", "sha256": hashlib.sha256(report).hexdigest(),
                  "data": base64.b64encode(zlib.compress(report)).decode()}, separators=(",", ":")), flush=True)
'''


def evidence_request(intent: dict, config, timeout: float) -> dict:
    from cval.validation.registry import validation_test_config_digest

    return {
        "node": intent["node_name"], "timestamp": intent["timestamp"], "run_id": intent["run_id"],
        "git_ref": intent["git_ref"], "config_digest": intent["config_digest"], "image_name": config.job.image_name,
        "root": config.runtime.validation_root, "timeout_seconds": timeout,
        "sqlite_timeout_seconds": config.sqlite_retry.timeout_seconds,
        "generation_attempts": config.sqlite_retry.attempts, "generation_delay_seconds": config.sqlite_retry.delay_seconds,
        "max_bytes": config.monitoring.verification_max_bytes, "max_files": config.monitoring.verification_max_files,
        "tests": {test.id: {"enabled": test.enabled, "order": test.definition.metadata.order,
                            "config_path": test.config_path, "config_digest": validation_test_config_digest(test),
                            "summary_filename": test.definition.artifacts.summary_filename,
                            "settings": test.to_dict()["settings"], "gpu_count": test.definition.requirements.gpu_count}
                  for test in config.tests.registry.tests},
        "db_paths": {"validation": config.storage.validation_db_path, "storage": config.storage.storage_db_path,
                     "nccl": config.storage.nccl_db_path, "numerical_correctness": config.storage.dl_numerical_db_path,
                     "compute_performance": config.storage.dl_compute_db_path, "collective_performance": config.storage.dl_collective_db_path,
                     "overlap_performance": config.storage.dl_overlap_db_path},
    }


def collect_exact_evidence(request: dict, config, client, *, operation: str = "verification") -> dict:
    sources = {}
    package_root = Path(__file__).resolve().parents[1]
    for path in package_root.rglob("*.py"):
        relative = path.relative_to(package_root.parent)
        package = path.name == "__init__.py"
        name = ".".join(relative.parts[:-1] if package else relative.with_suffix("").parts)
        sources[name] = (relative.as_posix(), path.read_text(encoding="utf-8"), package)
    summary = package_root.parent / "validation-tests/nccl/summary.py"
    sources["cval.validation.nccl_summary"] = ("validation-tests/nccl/summary.py", summary.read_text(encoding="utf-8"), False)
    for registered in config.tests.registry.tests:
        declaration = registered.definition.plugin
        if declaration is None:
            continue
        for filename in declaration.support_files:
            if not filename.endswith(".py"):
                continue
            path = registered.test_dir / filename
            relative = (Path(registered.config_path).parent / filename).as_posix()
            sources[relative] = (relative, path.read_text(encoding="utf-8"), False)
    encoded = base64.b64encode(zlib.compress(json.dumps(sources).encode())).decode()
    try:
        response = client.run(["exec", "-i", "-n", config.cluster.namespace, config.cluster.pvc_access_pod,
                               "--", "python3", "-c", _MEMORY_READER],
                              input_text=json.dumps({"sources": encoded, "request": request, "operation": operation}),
                              timeout=request["timeout_seconds"], check=False)
        if response.returncode:
            return {"state": "forbidden" if "forbidden" in response.stderr.lower() else "unavailable",
                    "accepted": False, "complete": False, "reason": response.stderr, "sources": {}}
        transport = json.loads(response.stdout)
        if not isinstance(transport, dict) or set(transport) != {"encoding", "sha256", "data"} or transport["encoding"] != "zlib+base64":
            raise ValueError("Invalid evidence transport envelope")
        decoder = zlib.decompressobj()
        decoded = decoder.decompress(base64.b64decode(transport["data"], validate=True), request["max_bytes"] + 1)
        if len(decoded) > request["max_bytes"] or not decoder.eof or decoder.unused_data:
            raise ValueError("Incomplete or oversized evidence transport payload")
        if hashlib.sha256(decoded).hexdigest() != transport["sha256"]:
            raise ValueError("Evidence transport checksum mismatch")
        report = json.loads(decoded)
        if not isinstance(report, dict):
            raise ValueError("Evidence transport report must be an object")
        report["reader_source_sha256"] = hashlib.sha256(zlib.decompress(base64.b64decode(encoded))).hexdigest()
        report["transport_stderr"] = response.stderr
        return report
    except Exception as error:
        return {"state": "unavailable", "reason": str(error), "accepted": False, "complete": False, "sources": {}}


def verify_request(intent: dict, request: dict, config, client) -> dict:
    """Bind a read-only evidence report to the caller's exact Job and workload."""

    from cval.jobs.manager import _requested_fields_match, verify_submission_identity
    from cval.jobs.monitor import TERMINAL_PHASES

    deadline = time.monotonic() + request["timeout_seconds"]
    binding = {key: request[key] for key in ("node", "timestamp", "run_id", "git_ref", "config_digest")}
    binding.update(job_name=intent["job_name"], manifest_sha256=intent["manifest_sha256"])
    report = {"schema_version": "cval.verification.v1", "state": "unavailable", "accepted": False,
              "complete": False, "release": False, "identity_verified": False, "request": binding, "sources": {}}

    def remaining():
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("Exact-run verification deadline expired")
        return min(value, config.cluster.kubectl_timeout_seconds)

    def get_json(args):
        response = client.run([*args, "-o", "json"], timeout=remaining(), check=False)
        if response.returncode:
            raise EvidenceError("forbidden" if "forbidden" in response.stderr.lower() else "unavailable", response.stderr)
        return json.loads(response.stdout)

    try:
        if (request["node"], request["timestamp"], request["run_id"], request["git_ref"], request["config_digest"]) != (
            intent["node_name"], intent["timestamp"], intent["run_id"], intent["git_ref"], intent["config_digest"],
        ):
            raise ValueError("Verification request differs from retained intent")
        endpoint = client.run(["config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"], timeout=remaining(), check=False)
        if endpoint.returncode:
            raise EvidenceError("forbidden" if "forbidden" in endpoint.stderr.lower() else "unavailable", endpoint.stderr)
        if endpoint.stdout.strip() != config.live.cluster_server:
            raise ValueError("Verification cluster differs from the expected scope")
        job_args = ["get", "vcjob", intent["job_name"], "-n", config.cluster.namespace]
        job = get_json(job_args)
        uid = verify_submission_identity(intent, job, config.cluster.namespace)
        binding["job_uid"] = uid
        phase = job.get("status", {}).get("state", {}).get("phase", "Unknown")
        report["job_phase"] = phase
        if phase not in TERMINAL_PHASES - {"Missing"}:
            report["reason"] = "Job execution is not terminal"
            return report
        containers = job["spec"]["tasks"][0]["template"]["spec"]["containers"]
        fixed = {item["name"]: item.get("value") for item in containers[0]["env"]}
        runtime = {}
        for line in base64.b64decode(fixed["CVAL_RUNTIME_ENV_B64"], validate=True).decode().splitlines():
            tokens = shlex.split(line)
            if len(tokens) != 2 or tokens[0] != "export":
                raise ValueError("Invalid submitted runtime snapshot transport")
            name, separator, value = tokens[1].partition("=")
            if not separator or name in runtime:
                raise ValueError("Invalid or duplicate submitted runtime value")
            runtime[name] = value
        if runtime["CVAL_CONFIG_DIGEST"] != request["config_digest"] or fixed["CVAL_GIT_REF"] != request["git_ref"] or int(fixed["GCRTIME"]) != request["timestamp"]:
            raise ValueError("Submitted source/config/run identity differs from request")
        snapshot = json.loads(base64.b64decode(runtime["CVAL_CONFIG_SNAPSHOT_B64"], validate=True))
        definitions = snapshot["test_definitions"]
        resolved = {key: value for key, value in snapshot.items() if key != "test_definitions"}
        resolved["tests"] = definitions
        if "sha256:" + hashlib.sha256(json.dumps(resolved, sort_keys=True, separators=(",", ":")).encode()).hexdigest() != request["config_digest"]:
            raise ValueError("Submitted configuration snapshot does not match its expected digest")
        if set(definitions) != set(request["tests"]) or snapshot["runtime"]["validation_root"] != request["root"]:
            raise ValueError("Submitted evidence root or test set differs from request")
        storage_keys = {"validation": "validation_db_path", "storage": "storage_db_path", "nccl": "nccl_db_path",
                        "numerical_correctness": "dl_numerical_db_path", "compute_performance": "dl_compute_db_path",
                        "collective_performance": "dl_collective_db_path", "overlap_performance": "dl_overlap_db_path"}
        if any(request["db_paths"][name] != snapshot["storage"][field] for name, field in storage_keys.items()) or request["image_name"] != fixed["CVAL_IMAGE_NAME"]:
            raise ValueError("Expected raw destinations or image differ from submitted configuration")
        for name, definition in definitions.items():
            expected = request["tests"][name]
            raw = {key: value for key, value in definition.items() if key not in {"enabled", "config_path"}}
            digest = "sha256:" + hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            if digest != expected["config_digest"] or definition["enabled"] != expected["enabled"] or definition["config_path"] != expected["config_path"]:
                raise ValueError(f"Submitted test definition differs: {name}")
            if (definition["settings"], definition["requirements"]["gpu_count"], definition["artifacts"]["summary_filename"], definition["metadata"]["order"]) != (expected["settings"], expected["gpu_count"], expected["summary_filename"], expected["order"]):
                raise ValueError(f"Verification requirements differ from submitted test: {name}")
        pods = get_json(["get", "pods", "-n", config.cluster.namespace, "-l", f"volcano.sh/job-name={intent['job_name']}"])
        if pods.get("metadata", {}).get("continue") or len(pods["items"]) != 1:
            raise EvidenceError("unavailable", "Cannot identify exactly one owned validation pod")
        pod = pods["items"][0]
        if not any(owner.get("uid") == uid and owner.get("name") == intent["job_name"] for owner in pod["metadata"].get("ownerReferences", [])) or pod["spec"].get("nodeName") != request["node"]:
            raise ValueError("Observed validation pod ownership or assigned node differs")
        if pod.get("status", {}).get("phase") not in {"Succeeded", "Failed"}:
            raise EvidenceError("unavailable", "Owned validation pod is not terminal")
        for expected in containers:
            matches = [container for container in pod["spec"]["containers"] if container["name"] == expected["name"]]
            if len(matches) != 1 or any(matches[0].get(key) != expected.get(key) for key in ("image", "command", "args")):
                raise ValueError("Observed pod command or image differs from the submitted workload")
            for entry in expected.get("env", []):
                values = [value for value in matches[0].get("env", []) if value["name"] == entry["name"]]
                if len(values) != 1 or not _requested_fields_match(entry, values[0]):
                    raise ValueError("Observed pod runtime identity differs from the submitted workload")
        statuses = pod["status"].get("containerStatuses", [])
        pod_success = pod["status"]["phase"] == "Succeeded" and {item["name"] for item in statuses} == {item["name"] for item in containers} and all(item.get("restartCount") == 0 and item.get("state", {}).get("terminated", {}).get("exitCode") == 0 for item in statuses)
        identity = {"state": "complete", "uid": uid, "phase": phase, "pod": pod["metadata"]["name"],
                    "pod_uid": pod["metadata"]["uid"], "pod_phase": pod["status"]["phase"], "pod_success": pod_success}
        report.update(collect_exact_evidence(request | {"timeout_seconds": remaining()}, config, client))
        report["request"] = binding
        if not report["sources"]:
            report["sources"]["evidence"] = {"state": report["state"], "reason": report.get("reason", "Evidence reader unavailable")}
        observed = get_json(job_args)
        if verify_submission_identity(intent, observed, config.cluster.namespace) != uid or observed.get("status", {}).get("state", {}).get("phase") != phase:
            raise EvidenceError("unavailable", "Job identity or terminal phase changed during evidence read")
        report["identity_verified"] = True
        report["sources"]["job"] = identity
        report["state"] = _combined_state(report["sources"])
        report["complete"] = report["state"] == "complete"
        report["release"] = report["state"] in {"complete", "incomplete", "conflicting"} and not any(source["state"] in {"forbidden", "unavailable"} for source in report["sources"].values())
        report["accepted"] = report["complete"] and report.get("tests_passed") is True and pod_success and phase in {"Completed", "Succeeded"}
    except Exception as error:
        report.update(_failure(error), accepted=False, complete=False, release=False, identity_verified=False)
    return report


def verify_run_evidence(intent: dict, config, client, timeout: float | None = None) -> dict:
    from dataclasses import replace
    from cval.validation.runtime import effective_config_digest

    config = replace(config, job=replace(config.job, git_ref=intent["git_ref"]))
    if effective_config_digest(config) != intent["config_digest"]:
        return {"schema_version": "cval.verification.v1", "state": "conflicting", "accepted": False,
                "complete": False, "release": False, "identity_verified": False, "reason": "Expected configuration differs from retained intent", "sources": {}}
    limit = config.monitoring.ingestion_grace_seconds if timeout is None else timeout
    return verify_request(intent, evidence_request(intent, config, limit), config, client)