#!/usr/bin/env bash
set -Eeuo pipefail

show_usage() {
    cat <<'USAGE'
Usage: ./git_push_all.sh [commit message]

Stages all changes in this repository, creates a commit when staged changes exist,
and pushes the current branch to its upstream remote.

Environment variables:
  COMMIT_MESSAGE  Commit message to use when no positional message is provided
  REMOTE          Remote name to use when no upstream is configured (default: origin)
  BRANCH          Branch name override (default: current branch)
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    show_usage
    exit 0
fi

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_dir"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: $repo_dir is not inside a git repository." >&2
    exit 1
fi

branch="${BRANCH:-$(git branch --show-current)}"
if [[ -z "$branch" ]]; then
    echo "Error: git is in detached HEAD state. Set BRANCH or check out a branch first." >&2
    exit 1
fi

remote="${REMOTE:-origin}"
commit_message="${COMMIT_MESSAGE:-}"
if [[ -z "$commit_message" && "$#" -gt 0 ]]; then
    commit_message="$*"
fi
if [[ -z "$commit_message" ]]; then
    commit_message="chore: update c-val $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
fi

echo "Repository: $repo_dir"
echo "Branch:     $branch"
echo "Remote:     $remote"

git add -A

if git diff --cached --quiet; then
    echo "No local changes to commit."
else
    echo "Creating commit: $commit_message"
    git commit -m "$commit_message"
fi

upstream="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || true)"
if [[ -n "$upstream" ]]; then
    echo "Pushing to upstream: $upstream"
    git push
else
    if ! git remote get-url "$remote" >/dev/null 2>&1; then
        echo "Error: remote '$remote' is not configured." >&2
        exit 1
    fi

    echo "No upstream configured. Pushing and setting upstream to $remote/$branch"
    git push -u "$remote" "$branch"
fi

echo "Done."