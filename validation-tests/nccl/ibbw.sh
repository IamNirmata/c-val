#!/usr/bin/env bash
# Real-time InfiniBand / HCA per-port bandwidth monitoring.
#
# By default this auto-detects every IB device and port under
# /sys/class/infiniband and samples per-port transmit bandwidth. An optional
# numeric device range can still be supplied to restrict monitoring to a
# contiguous mlx5_<n> window (backward compatible with the old invocation).
#
# Usage:
#   ./ibbw.sh                         # auto-detect ALL devices/ports (default)
#   ./ibbw.sh '' '' 2                 # auto-detect, 2s sampling interval
#   ./ibbw.sh 4 13                    # only mlx5_4..mlx5_13 (override)
#   ./ibbw.sh 0 13 1                  # mlx5_0..mlx5_13, 1s interval
#
# Output (one block per interval), parsed by single-node-allreduce.py:
#   HH:MM:SS | mlx5_0:   0.000 GB/s    mlx5_4:  46.231 GB/s    mlx5_4.2:  12.0 GB/s
# Port 1 uses the bare device label (mlx5_N); additional ports use mlx5_N.P.

space='    '
start_device=${1:-}
end_device=${2:-}
interval_seconds=${3:-1}

IB_SYS_ROOT=${IB_SYS_ROOT:-/sys/class/infiniband}

# The parent NCCL runner stops this monitor with SIGTERM and waits for it. Bash
# otherwise exits immediately while a foreground sleep/counter helper can remain
# in the validation process group, causing the generic runner to reject an
# otherwise successful workload as having leftover descendants. A trap defers
# shell exit until the foreground helper is reaped.
trap 'exit 0' INT TERM HUP

if ! [[ "$interval_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "Usage: $0 [start_device] [end_device] [interval_seconds]" >&2
    exit 2
fi

# Validate the optional numeric range only when both bounds are supplied.
if [[ -n "$start_device" || -n "$end_device" ]]; then
    if ! [[ "$start_device" =~ ^[0-9]+$ && "$end_device" =~ ^[0-9]+$ ]]; then
        echo "start_device and end_device must both be integers when a range is given" >&2
        exit 2
    fi
    if (( start_device > end_device )); then
        echo "start_device must be less than or equal to end_device" >&2
        exit 2
    fi
fi

# Discover (label, counter_path) pairs for every IB port. When a numeric range
# is provided, keep only mlx5_<n> devices whose suffix falls inside [start,end].
declare -a LABELS
declare -a COUNTER_PATHS

discover_ports() {
    LABELS=()
    COUNTER_PATHS=()
    [[ -d "$IB_SYS_ROOT" ]] || return 0

    local device_path device port_path port label suffix counter
    local devices=()
    for device_path in "$IB_SYS_ROOT"/*; do
        [[ -e "$device_path" ]] || continue
        devices+=("$(basename "$device_path")")
    done
    (( ${#devices[@]} == 0 )) && return 0
    # Sort devices by trailing numeric suffix so output order is stable.
    IFS=$'\n' devices=($(printf '%s\n' "${devices[@]}" | sort -t_ -k2 -n 2>/dev/null)); unset IFS

    for device in "${devices[@]}"; do
        suffix=${device##*_}
        # Apply the optional numeric range filter to mlx5_<n>-style names only.
        if [[ -n "$start_device" && "$suffix" =~ ^[0-9]+$ ]]; then
            if (( suffix < start_device || suffix > end_device )); then
                continue
            fi
        fi
        for port_path in "$IB_SYS_ROOT/$device"/ports/*; do
            [[ -e "$port_path" ]] || continue
            port=$(basename "$port_path")
            counter="$port_path/counters/port_xmit_data"
            [[ -e "$counter" ]] || continue
            if [[ "$port" == "1" ]]; then
                label="$device"
            else
                label="$device.$port"
            fi
            LABELS+=("$label")
            COUNTER_PATHS+=("$counter")
        done
    done
}

read_counter() {
    cat "$1" 2>/dev/null || echo 0
}

discover_ports

if (( ${#LABELS[@]} == 0 )); then
    echo "No InfiniBand ports found under $IB_SYS_ROOT" >&2
    # Emit nothing but do not hard-fail the NCCL phase.
    exit 0
fi

echo "Monitoring ${#LABELS[@]} IB port(s): ${LABELS[*]}; press Ctrl+C to stop"

declare -a old
declare -a new

while :
do
    for i in "${!COUNTER_PATHS[@]}"; do
        old[$i]=$(read_counter "${COUNTER_PATHS[$i]}")
    done

    sleep "$interval_seconds"

    for i in "${!COUNTER_PATHS[@]}"; do
        new[$i]=$(read_counter "${COUNTER_PATHS[$i]}")
    done

    echo -n "$(date +%H:%M:%S) | "
    for i in "${!COUNTER_PATHS[@]}"; do
        # port_xmit_data is in 4-byte (32-bit) units: multiply by 4 for bytes,
        # divide by 1024^3 for GB, divide by the interval for GB/s.
        delta=$((new[$i] - old[$i]))
        if (( delta < 0 )); then
            delta=0
        fi
        bw=$(awk -v delta="$delta" -v interval="$interval_seconds" 'BEGIN { printf "%.3f", (delta * 4) / 1073741824 / interval }')
        printf "%s: %7s GB/s${space}" "${LABELS[$i]}" "$bw"
        old[$i]=${new[$i]}
    done
    echo
done
