import torch.distributed as dist
import os
import time
import torch

# --- Environment Variable Setup for torchrun ---
# torchrun automatically sets these variables
local_rank = int(os.environ.get("LOCAL_RANK", 0))
world_rank = int(os.environ.get("RANK", 0))
world_size = int(os.environ.get("WORLD_SIZE", 1))

# --- Initialization ---
# init_method="env://" tells NCCL to look for MASTER_ADDR/PORT vars set by torchrun
dist.init_process_group("nccl", init_method="env://", rank=world_rank, world_size=world_size)
torch.cuda.set_device(local_rank)

# --- Configuration ---
ITERATIONS = 20
GB_UNIT = 1024 * 1024 * 1024

# 8GB * 2 (bfloat16) = 16GB per GPU
# Note: Ensure you have enough memory. B200s have plenty, but reduce '8' if needed.
data_size_elements = 8 * GB_UNIT 
total_size_bytes = data_size_elements * 2  # bfloat16 = 2 bytes

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
# Bus BW Formula: (DataSize / Time) * (2 * (N - 1) / N)
correction_factor = 2 * (world_size - 1) / world_size
alg_bw = (total_size_bytes / GB_UNIT) / duration
bus_bw = alg_bw * correction_factor

if world_rank == 0:
    print(f"World Size: {world_size}")
    print(f"Latency: {duration * 1000:.4f} ms")
    print(f"AlgBW: {alg_bw:.4f} GB/s")
    print(f"BusBW: {bus_bw:.4f} GB/s")

dist.destroy_process_group()


# ... [Inside your Python script, near the end] ...

if world_rank == 0:
    # 1. Print to Standard Out (For your human-readable logs)
    print(f"World Size: {world_size}")
    print(f"Latency: {duration * 1000:.4f} ms")
    print(f"AlgBW: {alg_bw:.4f} GB/s")
    print(f"BusBW: {bus_bw:.4f} GB/s")

    # 2. [NEW] Write to Metrics File (For the Bash script)
    # We check if the environment variable exists first
    metrics_file_path = os.environ.get("METRICS_OUTPUT_FILE")
    
    if metrics_file_path:
        with open(metrics_file_path, "w") as f:
            # We write these as bash commands
            f.write(f"export GCR_LATENCY={duration * 1000:.4f}\n")
            f.write(f"export GCR_ALGBW={alg_bw:.4f}\n")
            f.write(f"export GCR_BUSBW={bus_bw:.4f}\n")