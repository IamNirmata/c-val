#!/usr/bin/env bash
set -euo pipefail

# gpu_pods_age_and_util.sh
# Requires: kubectl, jq, awk, sort, head, cut, wc, column, date, mktemp
#
# Outputs:
#  - RUNNING GPU pods only (nvidia.com/gpu > 0)
#  - Age stats + percentiles + histogram
#  - Top N oldest with per-pod GPU util (%)
#  - Overall avg GPU util across pods where util is available

KUBECTL="${KUBECTL:-kubectl}"
RESOURCE_KEY="${RESOURCE_KEY:-nvidia.com/gpu}"
TOP_N="${TOP_N:-50}"

# Util collection knobs
GET_UTIL="${GET_UTIL:-1}"          # 1 to collect per-pod util via kubectl exec, 0 to skip
UTIL_SAMPLES="${UTIL_SAMPLES:-2}"  # number of samples per pod
UTIL_SLEEP="${UTIL_SLEEP:-1}"      # seconds between samples
EXEC_TIMEOUT="${EXEC_TIMEOUT:-5}"  # seconds for kubectl exec
CONTAINER="${CONTAINER:-}"         # optional: set to a specific container name
PARALLEL="${PARALLEL:-8}"          # parallelism for util collection

now_epoch="$(date -u +%s)"

tmp_tsv="$(mktemp -t gpu_pods_age.XXXXXX.tsv)"
ages_sorted="$(mktemp -t gpu_pods_age.XXXXXX.ages)"
util_tsv="$(mktemp -t gpu_pods_util.XXXXXX.tsv)"
top_tsv="$(mktemp -t gpu_pods_top.XXXXXX.tsv)"
trap 'rm -f "$tmp_tsv" "$ages_sorted" "$util_tsv" "$top_tsv"' EXIT

# -----------------------------
# 1) Build base TSV (Running GPU pods)
# age_sec \t ns \t pod \t node \t phase \t gpu_total \t created_ts
# -----------------------------
"$KUBECTL" get pods -A -o json | jq -r --arg rk "$RESOURCE_KEY" --argjson now "$now_epoch" '
  def to_num(x): (x // "0") | (if type=="number" then . else (tostring|tonumber) end);
  def c_gpu(c):
    (to_num(c.resources.requests[$rk]) // 0) as $req |
    (to_num(c.resources.limits[$rk])   // 0) as $lim |
    (if $req > $lim then $req else $lim end);
  def pod_gpu(p):
    (
      ([ p.spec.containers[]?     | c_gpu(.) ] | add) // 0
    ) as $app |
    (
      ([ p.spec.initContainers[]? | c_gpu(.) ] | max) // 0
    ) as $init |
    # effective GPU: app sum + init max
    (if ($app > 0) or ($init > 0) then ($app + $init) else 0 end);

  .items[]
  | select(.status.phase == "Running")
  | . as $p
  | (pod_gpu($p)) as $g
  | select($g > 0)
  | ($p.metadata.creationTimestamp // "") as $cts
  | ($cts | fromdateiso8601) as $created_epoch
  | ($now - $created_epoch) as $age
  | [
      $age,
      ($p.metadata.namespace // "-"),
      ($p.metadata.name // "-"),
      ($p.spec.nodeName // "-"),
      ($p.status.phase // "-"),
      $g,
      $cts
    ]
  | @tsv
' > "$tmp_tsv"

count="$(wc -l < "$tmp_tsv" | tr -d ' ')"
if [[ "$count" == "0" ]]; then
  echo "No RUNNING GPU pods found (resource key: $RESOURCE_KEY)."
  exit 0
fi

echo "RUNNING GPU pods found: $count  (resource key: $RESOURCE_KEY)"
echo

# -----------------------------
# Helpers
# -----------------------------
humanize_sec() {
  awk -v sec="$1" 'BEGIN{
    d=int(sec/86400); sec%=86400;
    h=int(sec/3600);  sec%=3600;
    m=int(sec/60);    s=sec%60;
    out="";
    if (d>0) out=out d "d ";
    if (h>0 || d>0) out=out h "h ";
    if (m>0 || h>0 || d>0) out=out m "m ";
    out=out s "s";
    print out;
  }'
}

# -----------------------------
# 2) Stats (avg/stddev/min/max)
# -----------------------------
awk -F'\t' '
  function human(sec,  d,h,m,s,out) {
    d=int(sec/86400); sec%=86400;
    h=int(sec/3600);  sec%=3600;
    m=int(sec/60);    s=sec%60;
    out="";
    if (d>0) out=out d "d ";
    if (h>0 || d>0) out=out h "h ";
    if (m>0 || h>0 || d>0) out=out m "m ";
    out=out s "s";
    return out;
  }
  BEGIN { n=0; mean=0; m2=0; min=1e18; max=0; }
  {
    x=$1+0;
    n++;
    if (x<min) min=x;
    if (x>max) max=x;
    delta = x - mean;
    mean += delta / n;
    m2   += delta * (x - mean);
  }
  END {
    var = (n>1) ? (m2/(n-1)) : 0;
    sd  = sqrt(var);
    printf("Age stats:\n");
    printf("  avg     = %.2f s  (%s)\n", mean, human(mean));
    printf("  stddev  = %.2f s  (%s)\n", sd,   human(sd));
    printf("  min     = %d s    (%s)\n", min,  human(min));
    printf("  max     = %d s    (%s)\n", max,  human(max));
  }
' "$tmp_tsv"
echo

# -----------------------------
# 3) Percentiles
# -----------------------------
cut -f1 "$tmp_tsv" | sort -n > "$ages_sorted"

get_pct() {
  local pct="$1"
  local n rank
  n="$(wc -l < "$ages_sorted" | tr -d ' ')"
  rank="$(awk -v p="$pct" -v n="$n" 'BEGIN{
    r=int((p/100.0)*n);
    if ((p/100.0)*n > r) r++;   # ceil
    if (r<1) r=1;
    if (r>n) r=n;
    print r;
  }')"
  awk -v r="$rank" 'NR==r{print $1; exit}' "$ages_sorted"
}

p50="$(get_pct 50)"
p90="$(get_pct 90)"
p99="$(get_pct 99)"

echo "Percentiles (nearest-rank):"
echo "  median (p50) = ${p50}s  ($(humanize_sec "$p50"))"
echo "  p90          = ${p90}s  ($(humanize_sec "$p90"))"
echo "  p99          = ${p99}s  ($(humanize_sec "$p99"))"
echo

# -----------------------------
# 4) Histogram
# -----------------------------
echo "Age histogram:"
awk -F'\t' '
  function bucket(sec){
    if (sec < 3600) return "<1h";
    if (sec < 21600) return "1-6h";
    if (sec < 86400) return "6-24h";
    if (sec < 259200) return "1-3d";
    if (sec < 604800) return "3-7d";
    return ">=7d";
  }
  { b=bucket($1+0); c[b]++; total++; }
  END{
    order[1]="<1h"; order[2]="1-6h"; order[3]="6-24h"; order[4]="1-3d"; order[5]="3-7d"; order[6]=">=7d";
    for(i=1;i<=6;i++){
      k=order[i];
      v=(k in c)?c[k]:0;
      pct=(total>0)?(100.0*v/total):0;
      printf("  %-6s : %6d (%.1f%%)\n", k, v, pct);
    }
  }
' "$tmp_tsv"
echo

# -----------------------------
# 5) Top N oldest (base)
# -----------------------------
sort -t$'\t' -k1,1nr "$tmp_tsv" | head -n "$TOP_N" > "$top_tsv"

# -----------------------------
# 6) GPU util collection (optional)
# Produces TSV: ns \t pod \t util_pct_or_NA
# Util = avg of per-GPU utilization.gpu across GPUs visible inside the pod
# (averaged across UTIL_SAMPLES samples)
# -----------------------------
: > "$util_tsv"

if [[ "$GET_UTIL" == "1" ]]; then
  echo "Collecting GPU utilization for top ${TOP_N} pods (samples=${UTIL_SAMPLES}, sleep=${UTIL_SLEEP}s, timeout=${EXEC_TIMEOUT}s, parallel=${PARALLEL})..."
  echo

  # Build job list: ns \t pod
  cut -f2,3 "$top_tsv" | while IFS=$'\t' read -r ns pod; do
    printf "%s\t%s\n" "$ns" "$pod"
  done \
  | xargs -P "$PARALLEL" -n 2 bash -lc '
      set -euo pipefail
      ns="$1"; pod="$2"

      # Build kubectl exec args
      k=( "'"$KUBECTL"'" -n "$ns" exec "$pod" )
      if [[ -n "'"$CONTAINER"'" ]]; then
        k+=( -c "'"$CONTAINER"'" )
      fi
      k+=( -- )

      # Collect UTIL_SAMPLES samples
      sum=0
      n=0
      ok=0

      for ((i=0; i<'"$UTIL_SAMPLES"'; i++)); do
        # Try to read GPU utilization from nvidia-smi
        # Output is one integer per GPU; we average them.
        out="$(
          timeout '"$EXEC_TIMEOUT"' "${k[@]}" sh -lc \
            "command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits" 2>/dev/null || true
        )"

        if [[ -n "$out" ]]; then
          # Average over GPUs for this sample
          sample_avg="$(echo "$out" | awk '"'"'BEGIN{c=0;s=0}{g=$1+0;s+=g;c++}END{if(c>0)printf "%.2f", s/c;}'"'"')"
          if [[ -n "$sample_avg" ]]; then
            sum="$(awk -v a="$sum" -v b="$sample_avg" '"'"'BEGIN{printf "%.6f", a+b}'"'"')"
            n=$((n+1))
            ok=1
          fi
        fi

        if [[ $i -lt $(( '"$UTIL_SAMPLES"' - 1 )) ]]; then
          sleep '"$UTIL_SLEEP"'
        fi
      done

      if [[ "$ok" == "1" && "$n" -gt 0 ]]; then
        util="$(awk -v s="$sum" -v n="$n" '"'"'BEGIN{printf "%.2f", s/n}'"'"')"
        printf "%s\t%s\t%s\n" "$ns" "$pod" "$util"
      else
        printf "%s\t%s\tNA\n" "$ns" "$pod"
      fi
    ' _ \
  >> "$util_tsv"

  # Overall util avg (excluding NA)
  overall_avg="$(
    awk -F'\t' '$3 != "NA" {s+=$3; c++} END{ if(c>0) printf "%.2f", s/c; else print "NA"; }' "$util_tsv"
  )"
  util_ok_count="$(awk -F'\t' '$3 != "NA" {c++} END{print c+0}' "$util_tsv")"

  echo "GPU util coverage: ${util_ok_count}/${TOP_N} pods"
  echo "Overall avg GPU util (top ${TOP_N}, available only): ${overall_avg}%"
  echo
else
  echo "Skipping GPU utilization (GET_UTIL=0)."
  echo
fi

# -----------------------------
# 7) Pretty output table (Top N oldest)
# Add UTIL column and align with column -t
# -----------------------------
echo "Top ${TOP_N} oldest RUNNING GPU pods (with utilization):"

# Header
printf "AGE\tUTIL(%%)\tNAMESPACE\tPOD\tNODE\tPHASE\tGPU\tCREATED\n"

# Join top list with util table on (ns,pod)
# top_tsv fields: age ns pod node phase gpu created
awk -F'\t' '
  NR==FNR { key=$1"\t"$2; util[key]=$3; next }
  {
    age=$1+0; ns=$2; pod=$3; node=$4; phase=$5; gpu=$6; created=$7;
    key=ns"\t"pod;
    u=(key in util)?util[key]:"NA";

    # humanize age
    d=int(age/86400); age%=86400;
    h=int(age/3600);  age%=3600;
    m=int(age/60);    s=age%60;
    out="";
    if (d>0) out=out d "d ";
    if (h>0 || d>0) out=out h "h ";
    if (m>0 || h>0 || d>0) out=out m "m ";
    out=out s "s";

    print out "\t" u "\t" ns "\t" pod "\t" node "\t" phase "\t" gpu "\t" created;
  }
' "$util_tsv" "$top_tsv" | column -t -s $'\t'
