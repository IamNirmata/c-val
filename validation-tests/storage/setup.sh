#!/usr/bin/env bash
set -euo pipefail

# Prepare storage-test dependencies. This is idempotent when fio is already
# present and honors the current compatibility setting injected by the renderer.

CVAL_STORAGE_INSTALL_FIO=${CVAL_STORAGE_INSTALL_FIO:-true}

is_enabled() {
	case "${1,,}" in
		1|true|yes|on) return 0 ;;
		*) return 1 ;;
	esac
}

if command -v fio >/dev/null 2>&1; then
	echo "storage setup: fio is available"
	exit 0
fi

if ! is_enabled "$CVAL_STORAGE_INSTALL_FIO"; then
	echo "storage setup: fio is missing and install_fio is disabled" >&2
	exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
	echo "storage setup: fio is missing and apt-get is unavailable" >&2
	exit 1
fi

echo "storage setup: installing fio"
apt-get update >/dev/null 2>&1
apt-get install -y fio >/dev/null 2>&1
command -v fio >/dev/null 2>&1
