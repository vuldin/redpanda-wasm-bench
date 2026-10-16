#!/usr/bin/env bash
# Server-side filtering for FIX gateways (josh-391): what each gateway pays in
# bytes and CPU to get its own events, with and without the in-broker
# gateway-demux transform.
#
# Every gateway's events share one "mega events" topic. Arms, per gateway
# count N:
#
#   firehose  no transform: each of the N gateways reads the whole events
#             topic and discards what is not its own (today's shape)
#   topics    gateway-demux MODE=topics: one output topic per gateway; each
#             reads only its own (exact; at most 8 - the broker's output-topic
#             limit)
#   keyed     gateway-demux MODE=keyed: one output topic with N x
#             KEYED_PARTITIONS_PER_GATEWAY partitions keyed by gateway id; each
#             reads the partition its id hashes to, plus whatever other
#             gateways' ids land there
#
# Per level it records, from cmd/gateways' own reports: records and bytes the
# gateways received, the fraction they kept, consumer CPU per kept event,
# end-to-end latency (producer send -> gateway receive, one clock), and broker
# egress (vectorized_kafka_handler_sent_bytes_total{handler="fetch"} summed
# over the brokers, before vs after). The win is egress and gateway CPU, not
# latency: a demux sends each event to one gateway, so total output volume
# equals input volume.
#
# The leader balancer is paused for the sweep, and every level records how many
# raft leadership changes the brokers saw during its window (leader_changes).
# A move of the events partition stalls the producer and the firehose readers
# for ~5s and restarts the demux on the new leader, which replays from its last
# offset commit: on 2026-10-16 that alone put a ~4.5s p99 on 7 of 10 levels and
# duplicates on 3 of them.
#
# Runs on client0 from /opt/wasm-bench, through `wb run gateways KEY=VALUE...`.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/lib-cluster.sh"

RUN_ID="${RUN_ID:-$(date +%s)}"
ARMS="${ARMS:-firehose topics keyed}"
GATEWAY_LEVELS="${GATEWAY_LEVELS:-2 4 8}"
# Extra gateway counts for the keyed arm only (topics stops at 8, and firehose
# at N=20 is 20x the topic in egress - add it explicitly if wanted).
KEYED_EXTRA_LEVELS="${KEYED_EXTRA_LEVELS:-20}"
KEYED_PARTITIONS_PER_GATEWAY="${KEYED_PARTITIONS_PER_GATEWAY:-4}"
RATE="${RATE:-30000}"
SIZE="${SIZE:-650}"
DURATION="${DURATION:-60s}"
EVENTS_PARTITIONS="${EVENTS_PARTITIONS:-1}"
RF="${RF:-3}"
WRITE_CACHING="${WRITE_CACHING:-true}"
ACKS="${ACKS:--1}"
SETTLE="${SETTLE:-10}"
WASM="${WASM:-$BIN/gateway-demux.wasm}"
RESULTS_DIR="${RESULTS_DIR:-./results/gateways-$RUN_ID}"
mkdir -p "$RESULTS_DIR"
SUMMARY="$RESULTS_DIR/summary.txt"

dur_s() { # Go duration -> whole seconds (s, m only)
  local d="$1" s=0
  [[ "$d" =~ ([0-9]+)m ]] && s=$(( s + ${BASH_REMATCH[1]} * 60 ))
  [[ "$d" =~ ([0-9]+)s$ ]] && s=$(( s + ${BASH_REMATCH[1]} ))
  echo "$s"
}

# Broker egress for Kafka fetches, summed over every broker's internal metrics.
fetch_bytes() {
  local total=0 u v
  for u in ${ADMIN_URLS//,/ }; do
    v="$(curl -sf --max-time 10 "$u/metrics" | awk '/^vectorized_kafka_handler_sent_bytes_total\{/ && /handler="fetch"/ {s += $NF} END {printf "%.0f", s}')"
    total=$(( total + ${v:-0} ))
  done
  echo "$total"
}

delete_level() {
  local t
  for t in $(rpkc transform list 2>/dev/null | awk -v r="$RUN_ID" 'NR>1 && $1 ~ "^gwdemux-" r {print $1}'); do
    rpkc transform delete "$t" --no-confirm >/dev/null 2>&1 || true
  done
  sleep 2
  for t in $(rpkc topic list 2>/dev/null | awk -v r="$RUN_ID" 'NR>1 && ($1 ~ "^gwevents-" r || $1 ~ "^gw-" r "-" || $1 ~ "^gwk-" r) {print $1}'); do
    rpkc topic delete "$t" >/dev/null 2>&1 || true
  done
}
# Raft leadership changes of this run's own topics, summed over every broker so
# far. Only these: the first idempotent produce of a run creates and elects the
# internal id_allocator partition, which is no move.
leader_changes() {
  local total=0 u v
  for u in ${ADMIN_URLS//,/ }; do
    v="$(curl -sf --max-time 10 "$u/metrics" | awk -v t="topic=\"(gwevents|gw|gwk)-$RUN_ID" '/^vectorized_raft_leadership_changes\{/ && $0 ~ t {s += $NF} END {printf "%.0f", s}')"
    total=$(( total + ${v:-0} ))
  done
  echo "$total"
}

trap 'delete_level; cfg_restore_all' EXIT
# Restored by cfg_restore_all on exit.
cfg_set enable_leader_balancer false

{
  echo "# fingerprint: MODE=gateways RF=$RF write.caching=$WRITE_CACHING"
  echo "# rate=$RATE size=$SIZE duration=$DURATION events_partitions=$EVENTS_PARTITIONS acks=$ACKS arms=$ARMS gateway_levels=$GATEWAY_LEVELS keyed_extra=$KEYED_EXTRA_LEVELS keyed_partitions_per_gateway=$KEYED_PARTITIONS_PER_GATEWAY guest_sha256=$(sha256sum "$WASM" 2>/dev/null | cut -c1-64)"
  echo "level arm gateways produced records bytes kept kept_fraction bytes_per_gateway egress_bytes egress_per_event cpu_s cpu_us_per_kept p50_us p99_us p999_us clean leader_changes"
} > "$SUMMARY"

d_s="$(dur_s "$DURATION")"
for arm in $ARMS; do
  levels="$GATEWAY_LEVELS"
  [ "$arm" = keyed ] && levels="$levels $KEYED_EXTRA_LEVELS"
  # gw_n, not n: lib-cluster.sh's wait_for_transforms_running loops over a
  # global `n` and overwrote this one with a transform name.
  for gw_n in $levels; do
    if [ "$arm" = topics ] && [ "$gw_n" -gt 8 ]; then
      echo "--- skipping topics at $gw_n gateways: a transform has at most 8 output topics"
      continue
    fi
    tag="$arm-$gw_n"
    # A transform name per level, never reused: redeploying a name with MORE
    # output topics hits a broker bug (the per-name metrics probe is cached
    # and keeps the first deployment's output count) that crashed a broker
    # with SIGILL on 2026-10-16, or makes the transform fail with
    # std::out_of_range (josh-416).
    tname="gwdemux-$RUN_ID-$tag"
    echo "############ $tag : arm=$arm gateways=$gw_n rate=$RATE size=$SIZE ############"
    delete_level
    events="gwevents-$RUN_ID"
    rpkc topic create "$events" -p "$EVENTS_PARTITIONS" -r "$RF" -c "write.caching=$WRITE_CACHING" >/dev/null \
      || { echo "FAIL: create $events"; exit 1; }
    source="$events"
    case "$arm" in
      topics)
        outs=""
        for ((i = 0; i < gw_n; i++)); do
          rpkc topic create "gw-$RUN_ID-$i" -p 1 -r "$RF" -c "write.caching=$WRITE_CACHING" >/dev/null || { echo "FAIL: create gw-$RUN_ID-$i"; exit 1; }
          outs="${outs:+$outs,}gw-$RUN_ID-$i"
        done
        bash "$BIN/deploy-transform.sh" "$ADMIN_URLS" "$tname" "$events" "$outs" "$WASM" \
          --env MODE=topics --env "TOPIC_PREFIX=gw-$RUN_ID-" || { echo "FAIL: deploy gateway-demux (topics)"; exit 1; }
        source="gw-$RUN_ID-" ;;
      keyed)
        p=$(( gw_n * KEYED_PARTITIONS_PER_GATEWAY ))
        rpkc topic create "gwk-$RUN_ID" -p "$p" -r "$RF" -c "write.caching=$WRITE_CACHING" >/dev/null || { echo "FAIL: create gwk-$RUN_ID"; exit 1; }
        bash "$BIN/deploy-transform.sh" "$ADMIN_URLS" "$tname" "$events" "gwk-$RUN_ID" "$WASM" \
          --env MODE=keyed || { echo "FAIL: deploy gateway-demux (keyed)"; exit 1; }
        source="gwk-$RUN_ID" ;;
    esac
    if [ "$arm" != firehose ]; then
      wait_for_transforms_running "$tname" || { echo "FAIL: gateway-demux never ran"; exit 1; }
    fi
    sleep "$SETTLE"

    e0="$(fetch_bytes)"; l0="$(leader_changes)"
    ./gateways consume -brokers "$BROKERS" -arm "$arm" -gateways "$gw_n" -source "$source" \
      -duration "$(( d_s + 120 ))s" -idle 15s -warmup 5s -out "$RESULTS_DIR/$tag.consume.json" \
      > "$RESULTS_DIR/$tag.consume.log" 2>&1 &
    cpid=$!
    sleep 3
    ./gateways produce -brokers "$BROKERS" -topic "$events" -gateways "$gw_n" -rate "$RATE" -size "$SIZE" \
      -duration "$DURATION" -acks "$ACKS" -out "$RESULTS_DIR/$tag.produce.json" \
      > "$RESULTS_DIR/$tag.produce.log" 2>&1 || echo "WARN: producer exited non-zero (see $tag.produce.log)"
    wait "$cpid" || echo "WARN: consumer exited non-zero (see $tag.consume.log)"
    e1="$(fetch_bytes)"; l1="$(leader_changes)"

    python3 - "$RESULTS_DIR/$tag.produce.json" "$RESULTS_DIR/$tag.consume.json" "$tag" "$arm" "$gw_n" "$(( e1 - e0 ))" "$(( l1 - l0 ))" >> "$SUMMARY" <<'PY'
import json, sys
pf, cf, tag, arm, n, egress, moves = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]), int(sys.argv[6]), sys.argv[7]
try:
    p, c = json.load(open(pf)), json.load(open(cf))
except (OSError, ValueError) as e:
    print(f"{tag} {arm} {n} - - - - - - - - - - - - - false {moves}")
    sys.exit(0)
lat = c.get("latency_micros") or {}
produced = p["acked"]
# Clean: every produced event reached its gateway exactly once.
clean = c["kept"] == produced and p["errors"] == 0
print(" ".join(str(x) for x in [
    tag, arm, n, produced, c["records"], c["bytes"], c["kept"], f"{c['kept_fraction']:.4f}",
    f"{c['bytes_per_gateway']:.0f}", egress, f"{egress / produced:.1f}" if produced else "-",
    f"{c['cpu_seconds']:.2f}", f"{c['cpu_micros_per_kept_event']:.2f}",
    f"{lat.get('p50', 0):.0f}", f"{lat.get('p99', 0):.0f}", f"{lat.get('p999', 0):.0f}",
    str(clean).lower(), moves]))
PY
    tail -1 "$SUMMARY"
    [ "$(( l1 - l0 ))" -gt 0 ] && echo "WARN: $(( l1 - l0 )) leadership change(s) during $tag - its latency and clean columns include them"
    delete_level
  done
done
echo "=== gateways summary: $SUMMARY ==="
cat "$SUMMARY"
