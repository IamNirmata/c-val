#!/usr/bin/env bash
set -euo pipefail

# Build: node -> used GPUs (sum of per-pod effective GPU request)
USED_JSON="$(
  kubectl get pods -A -o json | jq -c '
    reduce (
      .items[]
      | select(.spec.nodeName? != null)
      | select(.status.phase != "Succeeded" and .status.phase != "Failed")
      | {
          node: .spec.nodeName,
          app: (
            ([ .spec.containers[]? | (.resources.requests["nvidia.com/gpu"] // "0") ] | map(tonumber) | add) // 0
          ),
          init: (
            ([ .spec.initContainers[]? | (.resources.requests["nvidia.com/gpu"] // "0") ] | map(tonumber) | max) // 0
          )
        }
      | .g = (if .app > .init then .app else .init end)
    ) as $p ({}; .[$p.node] = ((.[$p.node] // 0) + $p.g))
  '
)"

tcap=0; talloc=0; tused=0; tfree=0

while read -r n cap alloc; do
  cap=${cap:-0};   [[ "$cap"   == "<none>" ]] && cap=0
  alloc=${alloc:-0}; [[ "$alloc" == "<none>" ]] && alloc=0

  used="$(jq -r --arg n "$n" '.[$n] // 0' <<<"$USED_JSON")"
  free=$(( alloc - used ))

  printf "%-45s  cap=%3d  alloc=%3d  used=%3d  free=%3d\n" "$n" "$cap" "$alloc" "$used" "$free"

  tcap=$((tcap+cap)); talloc=$((talloc+alloc)); tused=$((tused+used)); tfree=$((tfree+free))
done < <(
  kubectl get nodes --no-headers \
    -o custom-columns="NAME:.metadata.name,CAP:.status.capacity.nvidia\.com/gpu,ALLOC:.status.allocatable.nvidia\.com/gpu" \
  | grep hgx
)

echo
echo "TOTAL: cap=$tcap  alloc=$talloc  used=$tused  free=$tfree"
