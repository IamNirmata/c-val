#!/usr/bin/env bash
# Real-time InfiniBand Bandwidth Monitoring Script
# Usage: ./ibbw.sh [start_device] [end_device] [interval_seconds]
# Example: ./ibbw.sh 5 12    # Monitor mlx5_5 through mlx5_12 (8 GPU devices)
# Example: ./ibbw.sh 0 12    # Monitor all devices mlx5_0 through mlx5_12

space='    '
start_device=${1:-0}
end_device=${2:-12}
interval_seconds=${3:-1}

if ! [[ "$start_device" =~ ^[0-9]+$ && "$end_device" =~ ^[0-9]+$ && "$interval_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "Usage: $0 [start_device] [end_device] [interval_seconds]" >&2
    exit 2
fi
if (( start_device > end_device )); then
    echo "start_device must be less than or equal to end_device" >&2
    exit 2
fi

read_counter() {
    local device="$1"
    local counter_path="/sys/class/infiniband/${device}/ports/1/counters/port_xmit_data"
    cat "$counter_path" 2>/dev/null || echo 0
}

# Initialize arrays for storing counter values
declare -a old
declare -a new

echo "Monitoring mlx5_${start_device} through mlx5_${end_device}; press Ctrl+C to stop"

# Main monitoring loop
while :
do
    # Read initial counter values for all devices
    for i in $(seq "$start_device" "$end_device"); do
        device="mlx5_${i}"
        old[${i}]=$(read_counter "$device")
    done

    sleep "$interval_seconds"

    # Read new counter values for all devices
    for i in $(seq "$start_device" "$end_device"); do
        device="mlx5_${i}"
        new[${i}]=$(read_counter "$device")
    done

    # Calculate and display bandwidth for each device
    echo -n "$(date +%H:%M:%S) | "
    for i in $(seq "$start_device" "$end_device"); do
        device="mlx5_${i}"
        # Counter is in 4-byte (32-bit) units
        # Multiply by 4 to get bytes, then divide by 1024^3 to get GB/s.
        delta=$((new[${i}] - old[${i}]))
        if (( delta < 0 )); then
            delta=0
        fi
        bw=$(awk -v delta="$delta" -v interval="$interval_seconds" 'BEGIN { printf "%.3f", (delta * 4) / 1073741824 / interval }')
        printf "%s: %7s GB/s${space}" "$device" "$bw"
        
        # Store current value for next iteration
        old[${i}]=${new[${i}]}
    done
    echo
done
