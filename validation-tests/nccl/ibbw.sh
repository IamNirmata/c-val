#!/usr/bin/env bash
# Real-time InfiniBand Bandwidth Monitoring Script
# Usage: ./ibbw.sh <start_device> <end_device>
# Example: ./ibbw.sh 5 12    # Monitor mlx5_5 through mlx5_12 (8 GPU devices)
# Example: ./ibbw.sh 0 12    # Monitor all devices mlx5_0 through mlx5_12

space='    '

# Initialize arrays for storing counter values
declare -a old
declare -a new

echo " Press Ctrl+C to stop"

# Main monitoring loop
while :
do
    # Read initial counter values for all devices
    for i in $(seq $1 $2); do
        old[${i}]=$(cat /sys/class/infiniband/mlx5_${i}/ports/1/counters/port_xmit_data 2>/dev/null || echo 0)
    done

    # Wait 1 second
    sleep 1

    # Read new counter values for all devices
    for i in $(seq $1 $2); do
        new[${i}]=$(cat /sys/class/infiniband/mlx5_${i}/ports/1/counters/port_xmit_data 2>/dev/null || echo 0)
    done

    # Calculate and display bandwidth for each device
    echo -n "$(date +%H:%M:%S) | "
    for i in $(seq $1 $2); do
        # Counter is in 4-byte (32-bit) units
        # Multiply by 4 to get bytes, then divide by 1048576 (1024^2) to get MB/s
        bw=$(( (new[${i}] - old[${i}]) * 4 / 1048576 ))
        printf "mlx5_%d: %5d MB/s${space}" $i $bw
        
        # Store current value for next iteration
        old[${i}]=${new[${i}]}
    done
    echo
done
