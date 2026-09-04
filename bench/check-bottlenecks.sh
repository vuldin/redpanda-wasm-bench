#!/usr/bin/env bash
# Automated verification for the two mistakes found the hard way while
# investigating the consumer-fanout benchmark (2026-07-28):
#
# 1. Shard placement must be read from `GET /v1/cluster/partitions` (what
#    `rpk cluster partitions list` uses). The older per-partition endpoint,
#    `GET /v1/partitions/{ns}/{topic}/{partition}`, reports `.replicas[].core`
#    as a stale/unmaintained field that does not reflect node-local core
#    assignment - it read back `0` for every partition regardless of where
#    shard_balancer actually placed it, and produced a wrong "everything
#    collapses onto shard 0" conclusion that a `rpk cluster partitions list -a`
#    cross-check immediately contradicted.
# 2. `readers_cache_target_max_size` (default 200) caps the per-partition
#    reader-object cache independently of the batch (data) cache. A fan-out
#    of concurrent consumer groups above that cap thrashes reader objects -
#    visible as a near-100% *reader*-cache miss ratio even when the *batch*
#    cache is a clean 100% hit - which is a real, substantial, config-fixable
#    cost that has nothing to do with WASM/transforms.
#
# Usage: check-bottlenecks.sh <admin-url> <namespace> <topic> [<topic> ...]
# Prints a summary and writes bench/results/<topic>.diag.json for each topic.
set -euo pipefail

ADMIN_URL="$1"; NS="$2"; shift 2
RESULTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/results"
mkdir -p "$RESULTS_DIR"

ALL_PARTITIONS_JSON="$(curl -s "${ADMIN_URL}/v1/cluster/partitions")"
METRICS="$(curl -s "${ADMIN_URL}/metrics")"

for topic in "$@"; do
  replicas="$(echo "$ALL_PARTITIONS_JSON" | jq -c --arg ns "$NS" --arg t "$topic" \
    '[.[] | select(.ns==$ns and .topic==$t)] | .[0].replicas // []')"

  # vectorized_storage_log_cache_{hits,misses} - reader cache, keyed by
  # namespace/partition/shard/topic labels on the internal (not public)
  # metrics endpoint.
  hits=$(echo "$METRICS" | grep "^vectorized_storage_log_cache_hits{" | grep "topic=\"${topic}\"" | awk '{s+=$NF} END {print s+0}')
  misses=$(echo "$METRICS" | grep "^vectorized_storage_log_cache_misses{" | grep "topic=\"${topic}\"" | awk '{s+=$NF} END {print s+0}')
  batch_read=$(echo "$METRICS" | grep "^vectorized_storage_log_read_bytes{" | grep "topic=\"${topic}\"" | awk '{s+=$NF} END {print s+0}')
  batch_cached=$(echo "$METRICS" | grep "^vectorized_storage_log_cached_read_bytes{" | grep "topic=\"${topic}\"" | awk '{s+=$NF} END {print s+0}')

  total=$((hits + misses))
  if [ "$total" -gt 0 ]; then
    reader_miss_pct=$(awk -v m="$misses" -v t="$total" 'BEGIN{printf "%.1f", (m/t)*100}')
  else
    reader_miss_pct="n/a"
  fi
  if [ "$batch_read" -gt 0 ]; then
    batch_hit_pct=$(awk -v c="$batch_cached" -v r="$batch_read" 'BEGIN{printf "%.1f", (c/r)*100}')
  else
    batch_hit_pct="n/a"
  fi

  echo "--- ${topic} ---"
  echo "  shard placement: $(echo "$replicas" | jq -c .)"
  echo "  reader-cache miss%: ${reader_miss_pct} (hits=${hits} misses=${misses})"
  echo "  batch-cache hit%:   ${batch_hit_pct}"

  if [ "$reader_miss_pct" != "n/a" ] && awk -v p="$reader_miss_pct" 'BEGIN{exit !(p>50)}'; then
    echo "  WARNING: reader-cache miss% > 50 - likely thrashing (fan-out above readers_cache_target_max_size?)." >&2
  fi

  jq -n --arg topic "$topic" --argjson replicas "$replicas" \
    --arg reader_hits "$hits" --arg reader_misses "$misses" --arg reader_miss_pct "$reader_miss_pct" \
    --arg batch_read_bytes "$batch_read" --arg batch_cached_bytes "$batch_cached" --arg batch_hit_pct "$batch_hit_pct" \
    '{topic: $topic, replicas: $replicas, reader_cache: {hits: ($reader_hits|tonumber), misses: ($reader_misses|tonumber), miss_pct: $reader_miss_pct}, batch_cache: {read_bytes: ($batch_read_bytes|tonumber), cached_read_bytes: ($batch_cached_bytes|tonumber), hit_pct: $batch_hit_pct}}' \
    > "$RESULTS_DIR/${topic}.diag.json"
done
