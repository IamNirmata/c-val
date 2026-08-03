#!/usr/bin/env bash
set -euo pipefail

# Local/read-only by default. This script never contacts Kubernetes and does
# not claim that a reported filesystem supports PostgreSQL semantics.

mount_path=/var/lib/postgresql/data
pgdata_path=/var/lib/postgresql/data/pgdata
minimum_free_gib=50
apply=false
confirm=

usage() {
    cat <<'EOF'
Usage: cval-nccl-postgres-preflight.sh [options]
    --mount-path PATH          Dedicated PostgreSQL PVC mount (default: /var/lib/postgresql/data)
    --pgdata-path PATH         Planned PGDATA path (default: /var/lib/postgresql/data/pgdata)
  --minimum-free-gib N       Required reported free GiB (default: 50)
  --apply                    Run disposable fsync/flock/rename probes
  --confirm storage-preflight
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mount-path) mount_path=${2:?missing --mount-path value}; shift 2 ;;
        --pgdata-path) pgdata_path=${2:?missing --pgdata-path value}; shift 2 ;;
        --minimum-free-gib) minimum_free_gib=${2:?missing --minimum-free-gib value}; shift 2 ;;
        --apply) apply=true; shift ;;
        --confirm) confirm=${2:?missing --confirm value}; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ "$mount_path" == /* ]] || { echo "--mount-path must be absolute" >&2; exit 2; }
[[ "$mount_path" != *"//"* && "$mount_path" != */. && "$mount_path" != *"/../"* && "$mount_path" != */.. ]] || {
    echo "--mount-path must be lexical-canonical without . or .." >&2
    exit 2
}
[[ "$pgdata_path" != *"//"* && "$pgdata_path" != */. && "$pgdata_path" != *"/../"* && "$pgdata_path" != */.. ]] || {
    echo "--pgdata-path must be lexical-canonical without . or .." >&2
    exit 2
}
[[ "$(readlink -f -- "$mount_path")" == "$mount_path" ]] || {
    echo "--mount-path must be canonical and contain no symlink components" >&2
    exit 2
}
[[ "$pgdata_path" == "$mount_path"/* ]] || {
    echo "--pgdata-path must be below --mount-path" >&2
    exit 2
}
relative_pgdata=${pgdata_path#"$mount_path"/}
current_component=$mount_path
IFS=/ read -r -a pgdata_components <<<"$relative_pgdata"
for component in "${pgdata_components[@]}"; do
    current_component="$current_component/$component"
    [[ ! -L "$current_component" ]] || {
        echo "--pgdata-path contains a symlink component: $current_component" >&2
        exit 2
    }
    [[ ! -e "$current_component" || -d "$current_component" ]] || {
        echo "--pgdata-path contains a non-directory component: $current_component" >&2
        exit 2
    }
done
[[ "$minimum_free_gib" =~ ^[0-9]+$ ]] || {
    echo "--minimum-free-gib must be a non-negative integer" >&2
    exit 2
}
[[ -d "$mount_path" && ! -L "$mount_path" ]] || {
    echo "Mount path is absent, not a directory, or a symlink: $mount_path" >&2
    exit 1
}
[[ ! -e "$pgdata_path" && ! -L "$pgdata_path" ]] || {
    echo "Planned PGDATA already exists; refusing preflight: $pgdata_path" >&2
    exit 1
}

mount_info=$(findmnt -T "$mount_path" -n -o FSTYPE,OPTIONS)
[[ -n "$mount_info" ]] || { echo "Could not determine filesystem type/options" >&2; exit 1; }
read -r filesystem_type mount_options <<<"$mount_info"
free_kib=$(df -Pk "$mount_path" | awk 'NR == 2 {print $4}')
[[ "$free_kib" =~ ^[0-9]+$ ]] || { echo "Could not determine free capacity" >&2; exit 1; }
required_kib=$((minimum_free_gib * 1024 * 1024))
(( free_kib >= required_kib )) || {
    echo "Insufficient reported free capacity: ${free_kib}KiB < ${required_kib}KiB" >&2
    exit 1
}

printf 'mode=%s\n' "$([[ "$apply" == true ]] && echo apply || echo dry-run)"
printf 'mount_path=%s\n' "$mount_path"
printf 'pgdata_path=%s\n' "$pgdata_path"
printf 'filesystem_type=%s\n' "$filesystem_type"
printf 'mount_options=%s\n' "$mount_options"
printf 'free_kib=%s\n' "$free_kib"
printf 'pgdata_absent=true\n'
printf 'postgresql_storage_supported=UNDETERMINED\n'

if [[ "$apply" != true ]]; then
    printf 'disposable_write_probe=NOT_RUN\n'
    printf 'next=rerun only after approval with --apply --confirm storage-preflight\n'
    exit 0
fi

[[ "$confirm" == storage-preflight ]] || {
    echo "Write probes require --apply --confirm storage-preflight" >&2
    exit 2
}

probe_dir=$(mktemp -d "$mount_path/.cval-nccl-postgres-preflight.XXXXXX")
probe_dev=$(stat -c %d "$probe_dir")
probe_ino=$(stat -c %i "$probe_dir")
cleanup() {
    if [[ -d "$probe_dir" && ! -L "$probe_dir" && \
          "$(stat -c %d "$probe_dir")" == "$probe_dev" && \
          "$(stat -c %i "$probe_dir")" == "$probe_ino" ]]; then
        rm -f -- "$probe_dir/fsync-source" "$probe_dir/fsync-renamed" "$probe_dir/advisory.lock"
        rmdir -- "$probe_dir"
    fi
}
trap cleanup EXIT INT TERM
chmod 0700 "$probe_dir"

dd if=/dev/zero of="$probe_dir/fsync-source" bs=4096 count=1 conv=fsync status=none
exec 9>"$probe_dir/advisory.lock"
flock -x 9
mv "$probe_dir/fsync-source" "$probe_dir/fsync-renamed"
python3 - "$probe_dir/fsync-renamed" "$probe_dir" <<'PY'
import os
import sys

file_descriptor = os.open(sys.argv[1], os.O_RDONLY)
directory_descriptor = os.open(sys.argv[2], os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(file_descriptor)
    os.fsync(directory_descriptor)
finally:
    os.close(file_descriptor)
    os.close(directory_descriptor)
PY
[[ -f "$probe_dir/fsync-renamed" ]]
flock -u 9
exec 9>&-
rm -f -- "$probe_dir/fsync-renamed" "$probe_dir/advisory.lock"
rmdir -- "$probe_dir"
trap - EXIT INT TERM
printf 'disposable_write_probe=PASS\n'
printf 'probe_cleanup=PASS\n'
printf 'postgresql_storage_supported=UNDETERMINED\n'
