#!/usr/bin/env bash
# Commit all changes and push to the current branch on origin.
#
# Usage:
#   ./push.sh -m "commit message"
#   ./push.sh --m "commit message"
#   ./push.sh --message "commit message"
#
# Behavior:
#   - Stages all changes (git add -A).
#   - Commits with the provided message (skipped if there is nothing to commit).
#   - Pushes the current branch to origin.
set -euo pipefail

usage() {
    echo "Usage: $(basename "$0") -m \"commit message\"" >&2
    exit 1
}

MESSAGE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -m|--m|--message)
            MESSAGE="${2:-}"
            shift 2 || true
            ;;
        -m=*|--m=*|--message=*)
            MESSAGE="${1#*=}"
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            ;;
    esac
done

if [[ -z "$MESSAGE" ]]; then
    echo "Error: a commit message is required (-m \"message\")." >&2
    usage
fi

# Operate on the repository that contains this script, regardless of caller cwd.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR"

BRANCH=$(git rev-parse --abbrev-ref HEAD)

git add -A

if git diff --cached --quiet; then
    echo "No staged changes to commit; pushing existing commits on '$BRANCH'."
else
    git commit -m "$MESSAGE"
fi

git push origin "$BRANCH"
echo "Pushed '$BRANCH' to origin."
