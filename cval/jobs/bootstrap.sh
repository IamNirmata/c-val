set -euo pipefail
revision="${CVAL_GIT_REF:?}"
source_checkout="${CVAL_SOURCE_CHECKOUT:-/data/continuous_validation/c-val}"
destination="${CVAL_REPO_DIR:?}"
if [[ ! "$revision" =~ ^[0-9a-f]{40}$ || "$revision" == 0000000000000000000000000000000000000000 ]]; then
  echo "An exact nonzero Git commit is required" >&2
  exit 2
fi
if [[ "$destination" != /* || "$destination" == / || -e "$destination" || -L "$destination" ]]; then
  echo "Repository destination must be a new absolute path" >&2
  exit 2
fi
resolved_revision="$(git -C "$source_checkout" rev-parse --verify "${revision}^{commit}")"
if [[ "$resolved_revision" != "$revision" ]]; then
  echo "PVC checkout does not contain the exact requested commit" >&2
  exit 2
fi
destination_parent="$(dirname -- "$destination")"
if [[ ! -d "$destination_parent" ]]; then
  echo "Repository workspace parent is missing" >&2
  exit 2
fi
staging="$(mktemp -d "${destination_parent}/.cval-source.XXXXXXXX")"
trap '[[ -z "${staging:-}" ]] || rm -rf -- "$staging"' EXIT
git -C "$source_checkout" archive --format=tar "$revision" | tar -xf - -C "$staging"
mv -- "$staging" "$destination"
staging=""
trap - EXIT
cd "$CVAL_REPO_DIR"
printf '%s' "$CVAL_RUNTIME_ENV_B64" | base64 -d > /tmp/cval-runtime.env
source /tmp/cval-runtime.env
exec python3 -m cval.validation.supervisor
