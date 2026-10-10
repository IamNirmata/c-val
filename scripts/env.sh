#!/usr/bin/env bash
set -euo pipefail

export KUBECONFIG=/data/continuous_validation/secrets/config
export DEBIAN_FRONTEND=noninteractive
export PYTHONDONTWRITEBYTECODE=1
export GH_PROMPT_DISABLED=1
REPO=/data/continuous_validation/c-val
REMOTE=https://github.com/msr-central/c-val.git

[[ -r "$KUBECONFIG" ]] || { echo 'PVC kubeconfig is missing or unreadable' >&2; exit 2; }
[[ -n "${GH_TOKEN:-}" ]] || { echo 'GH_TOKEN Secret is required' >&2; exit 2; }
case "${1:-}" in
    ''|--check) ;;
    *) echo 'Usage: env.sh [--check]' >&2; exit 2 ;;
esac

if [[ "${1:-}" != --check ]]; then
    apt-get update
    apt-get install -y --no-install-recommends ca-certificates git gh kubectl tmux util-linux
    gh auth setup-git --hostname github.com
    if [[ ! -e "$REPO" ]]; then
        git clone --branch main "$REMOTE" "$REPO"
    fi
    [[ -d "$REPO/.git" && ! -L "$REPO" ]] || { echo 'Expected independent PVC Git checkout' >&2; exit 2; }
    [[ "$(git -C "$REPO" remote get-url origin)" == "$REMOTE" ]] || { echo 'Checkout origin differs from approved remote' >&2; exit 2; }
    [[ -z "$(git -C "$REPO" status --porcelain)" ]] || { echo 'Checkout has local changes; refusing automatic update' >&2; exit 2; }
    git -C "$REPO" fetch origin main
    git -C "$REPO" merge-base --is-ancestor HEAD origin/main || { echo 'Checkout has divergent commits; refusing update' >&2; exit 2; }
    git -C "$REPO" checkout --detach origin/main
    python3 -m pip install -e "$REPO"
fi

cd "$REPO"
export CVAL_SOURCE_REPO="$REPO"
export CVAL_CONFIG="$REPO/config/cval.toml"
unset CVAL_CONFIG_SNAPSHOT_B64 CVAL_SESSION_DIR CVAL_GIT_REF
settings=$(python3 - <<'PY'
import shlex
import socket
import subprocess
from pathlib import Path
from dataclasses import replace
from cval.config import load_config, load_config_snapshot
from cval.jobs.manager import read_session_state, admission_scope
from cval.validation.runtime import effective_config_digest

config = load_config()
root = Path(config.live.log_dir)
expected = Path('/data/continuous_validation/cval-controller/state')
if root != expected or config.live.control_host != socket.gethostname():
    raise SystemExit('Controller paths/hostname are not migrated; do not reuse host configuration')
pointer = root / 'current-session'
if not pointer.is_file() or pointer.is_symlink():
    raise SystemExit('Migrated admission session is required before activation')
name = pointer.read_text().strip()
if not name or Path(name).name != name or name in {'.', '..'}:
    raise SystemExit('Invalid session pointer')
session = root / name
state = read_session_state(session)
revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
pinned = load_config_snapshot(state['config_snapshot_b64'], repo_root=Path.cwd())
if state['git_ref'] != revision or pinned.job.git_ref != revision:
    raise SystemExit('Latest checkout requires reviewed session/source turnover before activation')
if pinned.job.source_configmap or pinned.job.source_sha256:
    raise SystemExit('Session uses retired source ConfigMap transport; initialize a PVC-source session')
if state['admission_scope'] != admission_scope(config):
    raise SystemExit('Session admission identity differs from controller configuration')
bound = replace(config, job=replace(config.job, git_ref=pinned.job.git_ref))
if effective_config_digest(bound) != effective_config_digest(pinned):
    raise SystemExit('Session settings differ from final TOMLs; reviewed turnover required')
for key, value in {'CVAL_SESSION_DIR': str(session), 'CVAL_GIT_REF': revision,
                   'TMUX_NAME': pinned.live.session_name}.items():
    print(f'export {key}={shlex.quote(value)}')
PY
)
eval "$settings"
if [[ "${1:-}" == --check ]]; then
    echo 'Controller bootstrap prerequisites verified; no loop started'
    exit 0
fi

export CVAL_LIVE_CONFIRM=submit
unset CVAL_PRUNE_CONFIRM
tmux has-session -t "$TMUX_NAME" 2>/dev/null && { echo 'Controller tmux session already exists' >&2; exit 2; }
stop_controller() {
    bash "$REPO/scripts/cval-live.sh" stop
}
trap stop_controller EXIT
trap 'exit 0' TERM INT
printf -v loop_command 'bash %q run-loop >>%q 2>&1' "$REPO/scripts/cval-live.sh" "$CVAL_SESSION_DIR/loop.log"
tmux new-session -d -s "$TMUX_NAME" "$loop_command"
echo "Controller started; attach with: tmux attach -t $TMUX_NAME"
tmux wait-for controller-exited &
waiter=$!
tmux set-hook -g session-closed 'run-shell "tmux wait-for -S controller-exited"'
if ! tmux has-session -t "$TMUX_NAME" 2>/dev/null; then
    kill "$waiter" 2>/dev/null || true
    exit 1
fi
wait "$waiter"
exit 1