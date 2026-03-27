import torch.distributed as dist
import os
import time
import torch
import argparse
import json

# --- Argument Parsing ---
parser = argparse.ArgumentParser()
parser.add_argument("--result-file", type=str, help="Path to save the metrics JSON file")
args = parser.parse_args()

# --- Environment Variable Setup ---
local_rank = int(os.environ.get("LOCAL_RANK", 0))
world_rank = int(os.environ.get("RANK", 0))
world_size = int(os.environ.get("WORLD_SIZE", 1))

# --- Initialization ---
dist.init_process_group("nccl", init_method="env://", rank=world_rank, world_size=world_size)
torch.cuda.set_device(local_rank)

# --- Configuration ---
ITERATIONS = 20
GB_UNIT = 1024 * 1024 * 1024
data_size_elements = 8 * GB_UNIT 
total_size_bytes = data_size_elements * 2 

# Allocate memory
npair_data = torch.zeros(data_size_elements, dtype=torch.bfloat16, device='cuda')

# --- Warmup ---
dist.all_reduce(npair_data)
torch.cuda.synchronize()

# --- Benchmark ---
pre = time.perf_counter()
for _ in range(ITERATIONS):
    dist.all_reduce(npair_data)
torch.cuda.synchronize()
duration = (time.perf_counter() - pre) / ITERATIONS

# --- Bandwidth Calculation ---
correction_factor = 2 * (world_size - 1) / world_size
alg_bw = (total_size_bytes / GB_UNIT) / duration
bus_bw = alg_bw * correction_factor

# --- Output and Saving ---
if world_rank == 0:
    # 1. Print to console (for logs)
    print(f"World Size: {world_size}")
    print(f"Latency: {duration * 1000:.4f} ms")
    print(f"AlgBW: {alg_bw:.4f} GB/s")
    print(f"BusBW: {bus_bw:.4f} GB/s")

    # 2. Save to JSON file if argument is provided
    if args.result_file:
        metrics = {
            "GCR_LATENCY": duration * 1000,
            "GCR_ALGBW": alg_bw,
            "GCR_BUSBW": bus_bw
        }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(args.result_file)), exist_ok=True)
        
        with open(args.result_file, 'w') as f:
            json.dump(metrics, f, indent=4)
        
        print(f"Metrics saved to: {args.result_file}")

dist.destroy_process_group()