import argparse
import json
import os
import time
from pathlib import Path

import torch
import torch.distributed as dist


GB_UNIT = 1024 * 1024 * 1024
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-file", type=str, help="Path to save the metrics JSON file")
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
                "GCR_ITERATIONS": args.iterations,
                "GCR_DATA_SIZE_GB": args.data_size_gb,
                "GCR_LATENCY": duration * 1000,
                "GCR_ALGBW": alg_bw,
                "GCR_BUSBW": bus_bw,
            }

            result_path = Path(args.result_file)
            if not result_path.parent.is_dir():
                raise FileNotFoundError(
                    f"NCCL staged result directory does not exist: {result_path.parent}"
                )
            descriptor = os.open(
                result_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o600)
                json.dump(metrics, handle, indent=4, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())

            print(f"Metrics saved to: {args.result_file}")

    dist.destroy_process_group()


if __name__ == "__main__":
    main()
