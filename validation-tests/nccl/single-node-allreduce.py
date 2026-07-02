import argparse
import json
import os
import re
import time
from pathlib import Path

import torch
import torch.distributed as dist


GB_UNIT = 1024 * 1024 * 1024
# Match ibbw.sh sample tokens like "mlx5_4: 46.231 GB/s" or the multi-port
# "mlx5_5.2: 12.0 GB/s" label (port 1 uses the bare mlx5_<n> device name).
IBBW_SAMPLE_PATTERN = re.compile(r"(mlx5_\d+(?:\.\d+)?):\s*([0-9]+(?:\.[0-9]+)?)\s*([MG]B/s)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-file", type=str, help="Path to save the metrics JSON file")
    parser.add_argument(
        "--ibbw-log-file",
        type=str,
        help="Optional ibbw.sh log to summarize per-mlx port bandwidth",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=int(os.environ.get("CVAL_NCCL_ITERATIONS", "20")),
        help="Number of all-reduce iterations to average",
    )
    parser.add_argument(
        "--data-size-gb",
        type=int,
        default=int(os.environ.get("CVAL_NCCL_DATA_SIZE_GB", "8")),
        help="Base Gi elements used for the BF16 all-reduce tensor",
    )
    return parser.parse_args()


def summarize_ibbw_log(path: str | None) -> dict[str, dict[str, float | int]]:
    if not path:
        return {}
    log_path = Path(path)
    if not log_path.exists():
        return {}

    samples: dict[str, list[float]] = {}
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        for device, value, unit in IBBW_SAMPLE_PATTERN.findall(line):
            gbps = float(value) / 1024 if unit == "MB/s" else float(value)
            samples.setdefault(device, []).append(gbps)

    summary: dict[str, dict[str, float | int]] = {}
    for device in sorted(samples, key=_mlx_sort_key):
        values = samples[device]
        if not values:
            continue
        summary[device] = {
            "avg_gbps": sum(values) / len(values),
            "max_gbps": max(values),
            "last_gbps": values[-1],
            "samples": len(values),
        }
    return summary


def _mlx_sort_key(device: str) -> tuple[int, int, str]:
    # Labels look like "mlx5_4" (port 1) or "mlx5_5.2" (device 5, port 2).
    suffix = device.rsplit("_", 1)[-1]
    dev_part, _, port_part = suffix.partition(".")
    dev_index = int(dev_part) if dev_part.isdigit() else 10_000
    port_index = int(port_part) if port_part.isdigit() else 1
    return (dev_index, port_index, device)


def main() -> None:
    args = parse_args()

    local_rank = int(os.environ.get("LOCAL_RANK", 0))
    world_rank = int(os.environ.get("RANK", 0))
    world_size = int(os.environ.get("WORLD_SIZE", 1))

    dist.init_process_group("nccl", init_method="env://", rank=world_rank, world_size=world_size)
    torch.cuda.set_device(local_rank)

    data_size_elements = args.data_size_gb * GB_UNIT
    total_size_bytes = data_size_elements * 2
    npair_data = torch.zeros(data_size_elements, dtype=torch.bfloat16, device="cuda")

    dist.all_reduce(npair_data)
    torch.cuda.synchronize()

    pre = time.perf_counter()
    for _ in range(args.iterations):
        dist.all_reduce(npair_data)
    torch.cuda.synchronize()
    duration = (time.perf_counter() - pre) / args.iterations

    correction_factor = 2 * (world_size - 1) / world_size
    alg_bw = (total_size_bytes / GB_UNIT) / duration
    bus_bw = alg_bw * correction_factor

    if world_rank == 0:
        print(f"World Size: {world_size}")
        print(f"Latency: {duration * 1000:.4f} ms")
        print(f"AlgBW: {alg_bw:.4f} GB/s")
        print(f"BusBW: {bus_bw:.4f} GB/s")

        if args.result_file:
            metrics = {
                "GCR_LATENCY": duration * 1000,
                "GCR_ALGBW": alg_bw,
                "GCR_BUSBW": bus_bw,
                "GCR_IB_PORT_BW_GBPS": summarize_ibbw_log(args.ibbw_log_file),
                "GCR_IBBW_LOG_FILE": args.ibbw_log_file or "",
            }

            os.makedirs(os.path.dirname(os.path.abspath(args.result_file)), exist_ok=True)

            with open(args.result_file, "w", encoding="utf-8") as handle:
                json.dump(metrics, handle, indent=4, sort_keys=True)
                handle.write("\n")

            print(f"Metrics saved to: {args.result_file}")

    dist.destroy_process_group()


if __name__ == "__main__":
    main()
