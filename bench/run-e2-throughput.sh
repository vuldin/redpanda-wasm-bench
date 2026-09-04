#!/usr/bin/env bash
# Phase 2 of bench/SCALING-TEST-PLAN.md: what one wasm client costs at a known
# throughput. This is the plan's highest-value experiment - it produces
# CPU-microseconds per record per client and the saturation knee, which is what
# every capacity claim downstream is computed from.
#
#   E2.1 (MODE=rate, default) sweep offered rate, everything else pinned
#   E2.2 (MODE=payload)       sweep payload size at a fixed rate
#   E2.3 (MODE=guest)         matcher vs passthrough at a fixed rate
#
# One variable per mode; the others stay at their defaults and are recorded in
# every report. Deliberately ONE producer and ONE relay consumer throughout -
# client count is Phase 3's variable, not this one.
#
# Placement is pinned, not observed: the producer's topics go on
# PIN_NODE/PIN_CORE and the single relay consumer goes on PROBE_CORE, a
# different shard. That is a fixed choice, not a measurement - co-locating the
# consumer with the producer would fold Phase 3's shard-contention effect into
# this rate sweep and make the knee unreadable.
#
# Works against the local sandbox (BROKERS=127.0.0.1:19093,
# ADMIN_URL=http://127.0.0.1:9645, BIN=.) or a real cluster (env pointed at it,
# BIN=/opt/wasm-bench) - same script either way. Note the local sandbox runs
# --smp 1, so PROBE_CORE cannot differ from PIN_CORE there and the run is a
# smoke test of the harness rather than a capacity measurement.
set -euo pipefail

BIN="${BIN:-.}"
RPK="${RPK:-$BIN/rpk}"
BROKERS="${BROKERS:?set to comma-separated host:port list}"
ADMIN_URL="${ADMIN_URL:?set to http://<seed-or-localhost>:<admin-port> (9644 cloud, 9645 local sandbox)}"
# EVERY broker's admin endpoint, comma-separated, for METRIC scraping.
# Transform/wasm-engine metrics are per-node and follow partition leadership, so
# a single-endpoint scrape silently omits the transform or reports an idle
# RELAY_TARGET_SHARD duplicate. Defaults to ADMIN_URL so a local single-node run
# still works, but on a cluster wb exports the full list.
ADMIN_URLS="${ADMIN_URLS:-$ADMIN_URL}"
RUN_ID="${RUN_ID:?set to a unique id, e.g. date +%s}"

MODE="${MODE:-rate}"
RATE_LEVELS="${RATE_LEVELS:-1000 5000 10000 50000 100000}"
PAYLOAD_LEVELS="${PAYLOAD_LEVELS:-0 64 512 4096}"
GUEST_LEVELS="${GUEST_LEVELS:-matcher passthrough}"
# E2.4: does wasm-client throughput scale with input partition count? One
# processor exists per (transform, partition) and each lives on the shard
# leading its partition, so more partitions should mean more processors on more
# cores. This is the test for whether wasm clients can use the headroom on
# other cores at all.
PARTITION_LEVELS="${PARTITION_LEVELS:-1 2 4 8 13}"
# E3.1: how many in-broker relay consumers subscribe to the producer's output.
# The target this was built for is 800-1000 consumers on ONE partition, and the
# in-broker relay has never been measured past 100, so this is the dimension
# with the largest gap between what we have measured and what matters.
# Retuned after Round 2 (2026-08-29). The old ladder was 1 10 50 100 250 500
# 1000; with the filler consumers' write amplification removed the interesting
# transition sits well below 100, and the 500/1000 levels cost many minutes of
# sequential `rpk transform deploy` calls each. Extend upward deliberately once
# a clean run shows where the knee actually is.
FANOUT_LEVELS="${FANOUT_LEVELS:-1 5 10 25 50}"
FANOUT="${FANOUT:-1}"
# How many DISTINCT shards the fanout consumers are spread over. Defaults to
# every shard (round-robin), which is the behaviour the cross-shard relay fix
# was built for. Setting it to 1 puts every consumer on ONE shard, so
# relay::service::push() issues ONE cross-shard invoke_on per record instead of
# one per occupied shard - same consumer count, same guest work, ~Nx fewer
# cross-shard submissions.
#
# That is the discriminator for the 2026-09-01 hypothesis: hot-shard CPU is flat
# across fanout 5->20 and the clean level uses MORE of it than the saturated
# ones, so the ceiling is not CPU. If it is instead per-record cross-shard
# submission volume, the ceiling should track SHARDS OCCUPIED, not consumer
# count - and collapsing the spread should move it.
#
# Note the confound this deliberately reintroduces: co-locating consumers on one
# shard is what made relay_consume grow with fanout before the cross-shard fix,
# so consume_delay is expected to RISE. Read the ceiling (clean/lag), not the
# per-consumer latency, from this arm.
FANOUT_SHARDS="${FANOUT_SHARDS:-}"
# Which shard the FANOUT_SHARDS window starts at. This matters and is easy to get
# wrong: with FANOUT_SHARDS=1 every consumer lands on shard FANOUT_SHARD_BASE, and
# if that is the MATCHER's shard then relay delivery is purely local - no
# invoke_on at all - which is a third experiment (co-location), not the
# one-remote-shard arm intended. The matcher's shard follows partition leadership
# and is not fixed, so the caller must pick a base it has confirmed is not the
# matcher's. scheduler_runtime_micros_by_shard in the resources JSON identifies
# the matcher's shard after the fact.
# Set to "auto" to have the runner pick a base that is provably NOT the
# matcher's shard, once leader placement is actually known. Prefer that at RF>1,
# where PIN_CORE is only advisory (the leader's shard is wherever that node's
# replica lands), so no base chosen in advance can be guaranteed safe.
FANOUT_SHARD_BASE="${FANOUT_SHARD_BASE:-0}"
# Round 3, the in-broker-vs-external comparison. One variable: WHO consumes the
# matcher's output.
#   inbroker - N relay consumers inside the broker (consumer 0 measured)
#   external - N ordinary Kafka consumer groups outside it, and loadgen itself is
#              the measured one, consuming the fills topic directly
# Both arms measure `total` on loadgen's OWN clock (send -> receipt observed), so
# the headline comparison is single-clock with no skew residual. Note the
# inbroker arm's total carries an extra produce+fetch hop (probe -> probe_out ->
# loadgen) that the external arm does not, so the comparison HANDICAPS in-broker.
CONSUMER_ARM="${CONSUMER_ARM:-inbroker}"
# data_transforms_read_linger_us. Swept as a MODE rather than set out
# of band so the value lands in the fingerprint and the summary table - an
# untracked config change is what makes runs silently incomparable.
# Microseconds. The property is data_transforms_read_linger_us (integer us),
# re-cut from milliseconds after the 2026-08-30 sweep showed essentially ALL of
# the 21% CPU benefit was captured by 1000us - so the entire useful range sat
# inside one millisecond step and the knee could not be located.
READ_LINGER_LEVELS="${READ_LINGER_LEVELS:-0 50 100 250 500 1000}"
READ_LINGER_US="${READ_LINGER_US:-0}"

# Held fixed except in the mode that sweeps them.
RATE="${RATE:-10000}"
PAYLOAD="${PAYLOAD:-0}"
GUEST="${GUEST:-matcher}"

DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-10s}"
PACING="${PACING:-fixed}"
# 0 = latency mode (METHODOLOGY #5). Set 1-5 for throughput runs at RF>1, where
# linger=0 makes the CLIENT the bottleneck rather than the broker.
LINGER_MS="${LINGER_MS:-0}"
# Produce acknowledgement level, passed straight to loadgen -acks.
# "all" (quorum) is the default and the only durable choice. "leader" is a
# DURABILITY TRADE that is only defensible for in-broker relay consumers,
# which already read before replication completes - see PRODUCE-LEG.md. It is
# fingerprinted because it is the dominant e2e term: the produce leg measured
# 641-1010us against 78us for the whole relay path, so a run at acks=leader is
# not comparable with one at acks=all.
ACKS="${ACKS:-all}"
# Independent producer clients. One client cannot saturate a single partition at
# RF=3 (franz-go caps in-flight produce per broker under idempotency; measured a
# hard ~2,739 orders/sec regardless of offered rate). More clients raise the
# achievable offered rate, and match how the target accounts drive a hot
# partition: many producers, one partition.
PRODUCERS="${PRODUCERS:-1}"
SAMPLE_EVERY="${SAMPLE_EVERY:-10}"
# A FIXED drain silently truncates the metric window at higher rates: at 5,000
# orders/sec a 10s drain gave 59,948 invocations for 200,000 records sent (30%
# coverage) and still reported clean, while 40s gave exactly 200,000. loadgen now
# fails a run whose coverage is short, but the default should not provoke it.
DRAIN="${DRAIN:-40s}"
SETTLE="${SETTLE:-15}"   # seconds between levels, so one level's backlog never lands in the next

# Recreate topics and redeploy the transforms before EVERY level.
#
# Default 0, which shares topics and a long-running transform across levels -
# cheaper, and correct for a sweep whose variable is purely client-side (rate,
# payload). But it is NOT strict isolation: level N then reads an `orders` log
# containing every prior level's records, against a warm batch/readers cache,
# with a matcher whose guest order book has been live since level 1. Any of
# those could bias a later level.
#
# Set 1 when the levels must be independent - comparing a first level against a
# fourth, or investigating something the read path could plausibly affect. Costs
# roughly 30-45s per level. `partitions` mode always recreates, since the
# partition count is a property of the topic.
FRESH_TOPICS_PER_LEVEL="${FRESH_TOPICS_PER_LEVEL:-0}"

PIN_NODE="${PIN_NODE:-0}"
PIN_CORE="${PIN_CORE:-1}"

# Production shape: latency-sensitive deployments of this kind run RF=3 with
# write caching on. RF matters more than it looks: the transform's output write
# is replicated at quorum_ack and AWAITED before the next read
# (transform/rpc/service.cc:50), so RF=3 puts a follower round trip inside every
# per-batch cycle. RF=1 measurements are an optimistic floor.
RF="${RF:-1}"
WRITE_CACHING="${WRITE_CACHING:-false}"
FLUSH_MS="${FLUSH_MS:-}"

RESULTS_DIR="${RESULTS_DIR:-$BIN/results/e2-throughput-$MODE-$RUN_ID}"
mkdir -p "$RESULTS_DIR"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib-cluster.sh
source "$HERE/lib-cluster.sh"
SCRAPE="$HERE/scrape-metrics.sh"

case "$MODE" in
  rate)       SWEEP="$RATE_LEVELS" ;;
  payload)    SWEEP="$PAYLOAD_LEVELS" ;;
  guest)      SWEEP="$GUEST_LEVELS" ;;
  partitions) SWEEP="$PARTITION_LEVELS" ;;
  fanout)     SWEEP="$FANOUT_LEVELS" ;;
  readlinger) SWEEP="$READ_LINGER_LEVELS" ;;
  *) echo "MODE must be rate, payload, guest, partitions, fanout or readlinger (got $MODE)" >&2; exit 1 ;;
esac

case "$CONSUMER_ARM" in
  inbroker|external) ;;
  *) echo "CONSUMER_ARM must be inbroker or external (got $CONSUMER_ARM)" >&2; exit 1 ;;
esac
if [ "$CONSUMER_ARM" = "external" ] && [ "$MODE" != "fanout" ]; then
  echo "CONSUMER_ARM=external only applies to MODE=fanout (got MODE=$MODE)" >&2; exit 1
fi

# Idempotent: the signal traps below call this and then exit, which fires the
# EXIT trap as well.
_ON_EXIT_DONE=0
on_exit() {
  [ "$_ON_EXIT_DONE" = 1 ] && return 0
  _ON_EXIT_DONE=1
  stop_external_fanout 2>/dev/null || true
  cleanup_all_test_artifacts
  cfg_restore_all
}

# EXIT alone is not enough. A bash EXIT trap does NOT run when the shell is
# killed by an untrapped signal - the process dies immediately and its
# transforms stay deployed on the cluster.
#
# That is exactly what happened on 2026-09-02: a `timeout 90 ./wb run e2 ...`
# self-test sent SIGTERM, this script died with no cleanup, and its matcher plus
# five probes were left behind. The next three runs were then refused by
# assert_no_concurrent_run - correctly, but for a reason that looked like a
# concurrent run rather than the debris of a killed one. Diagnosing that cost
# more than the self-test saved.
trap on_exit EXIT
trap 'echo "run-e2-throughput: SIGTERM - cleaning up run $RUN_ID before exit" >&2; on_exit; exit 143' TERM
trap 'echo "run-e2-throughput: SIGINT - cleaning up run $RUN_ID before exit" >&2;  on_exit; exit 130' INT
trap 'echo "run-e2-throughput: SIGHUP - cleaning up run $RUN_ID before exit" >&2;  on_exit; exit 129' HUP
# Refuse to start alongside another run: unscoped setup/teardown between two
# concurrent runs is what produced the phantom 2,739 orders/sec ceiling.
assert_no_concurrent_run "$RUN_ID" || exit 1
cleanup_all_test_artifacts

NUM_SHARDS="${NUM_SHARDS:-$(node_num_cores "$PIN_NODE")}"
PROBE_CORE="${PROBE_CORE:-}"
if [ -z "$PROBE_CORE" ]; then
  if [ "$NUM_SHARDS" -gt 2 ]; then
    PROBE_CORE=$(( (PIN_CORE + 1) % NUM_SHARDS ))
    [ "$PROBE_CORE" = "0" ] && PROBE_CORE=$(( PIN_CORE + 1 < NUM_SHARDS ? PIN_CORE + 1 : 1 ))
  else
    PROBE_CORE="$PIN_CORE"
    echo "WARNING: PIN_NODE has only $NUM_SHARDS shard(s); the consumer must share the producer's shard."
    echo "         Shard contention is folded into every number below. Treat this as a harness smoke"
    echo "         test, not a capacity measurement (SCALING-TEST-PLAN.md P0.3)."
  fi
fi
echo "=== node $PIN_NODE has $NUM_SHARDS shards; producer on core $PIN_CORE, consumer on core $PROBE_CORE ==="

echo "=== disabling core balancing so the pins hold for this run ==="
cfg_set core_balancing_continuous false
cfg_set core_balancing_on_core_count_change false

# NOTE: data_transforms_max_instances_per_core and
# data_transforms_per_function_memory_limit are needs_restart:yes. Setting them
# live lifts plugin_frontend's admission check but NOT wasm::heap_allocator's
# pool, which is carved at boot - so a deploy can be admitted and then fail at
# processor start with "unable to allocate memory within requested bounds".
# Phase 2 needs only two instances, so this script deliberately does not touch
# them; run-e1-ceiling.sh handles them properly, with a restart.

CLOCK_RMS_MICROS=0
record_clock_sync "$RESULTS_DIR/clock-sync.json"

orders="orders-e2-${RUN_ID}"
fills="fills-e2-${RUN_ID}"
probe_out="probe-out-e2-${RUN_ID}"
# Sink for the unmeasured fan-out consumers (MODE=fanout). Nothing reads it; it
# exists so consumers 1..N-1 have somewhere to write without every order
# arriving at loadgen N times. See deploy_fanout_consumers.
probe_sink="probe-sink-e2-${RUN_ID}"

# In partitions mode the input topic is recreated per level with a different
# partition count, and placement is deliberately NOT pinned: the point is to see
# where shard_balancer puts the partitions and whether the processors follow
# onto other cores. Pinning would defeat the experiment.
# write.caching is a per-topic property and a real durability tradeoff
# (METHODOLOGY.md's own section applies). Applied explicitly rather than
# inherited from a cluster default, so every run's fingerprint is unambiguous.
apply_topic_durability() {
  for t in "$@"; do
    rpkc topic alter-config "$t" --set "write.caching=$WRITE_CACHING" >/dev/null 2>&1 || true
    [ -n "$FLUSH_MS" ] && rpkc topic alter-config "$t" --set "flush.ms=$FLUSH_MS" >/dev/null 2>&1 || true
  done
}

echo "=== durability/replication for this run: RF=$RF write.caching=$WRITE_CACHING${FLUSH_MS:+ flush.ms=$FLUSH_MS} ==="

if [ "$MODE" != "partitions" ]; then
  rpkc topic create "$orders" "$fills" "$probe_out" "$probe_sink" -p 1 -r "$RF"
  apply_topic_durability "$orders" "$fills" "$probe_out"
  echo "=== waiting for placement to settle, then co-locating for relay locality ==="
  for t in "$orders" "$fills" "$probe_out"; do wait_for_stable_placement "$t" 0; done
  if [ "$RF" -gt 1 ]; then
    # Replicas already exist on every node; relay co-location is a leadership
    # question. Pinning a specific CORE is not possible this way - the leader's
    # shard is wherever that node's replica lives - so PIN_CORE is advisory at
    # RF>1 and the producer/consumer may share or differ in core. Cross-shard
    # relay delivery handles either.
    # Co-locate fills (the relay ntp) with orders (the matcher's input), which
    # is the only constraint that matters. probe_out is written by ordinary
    # Kafka produce, not the relay, so it needs no co-location.
    colocate_leadership "$fills" "$orders" 1
  else
    for t in "$orders" "$fills" "$probe_out"; do pin_to_shard "$t" 0 "$PIN_NODE" "$PIN_CORE"; done
  fi
  echo "--- leader placement (node/core) ---"
  for t in "$orders" "$fills"; do
    read -r n c <<< "$(partition_placement "$t" 0)"
    echo "    $t p0 -> node $n core $c"
  done

  # Resolve FANOUT_SHARD_BASE=auto now that the matcher's shard is KNOWN rather
  # than assumed. This matters only for a pinned fan-out arm (FANOUT_SHARDS=1):
  # if every consumer landed on the matcher's own shard, relay delivery would be
  # purely local with no invoke_on at all, which is a co-location experiment
  # rather than the one-remote-shard arm intended - and the whole arm would be
  # wasted. At RF>1 PIN_CORE is advisory, so this cannot be decided up front.
  if [ "$FANOUT_SHARD_BASE" = auto ]; then
    read -r _an _ac <<< "$(partition_placement "$orders" 0)"
    if [ -n "$_ac" ] && [ "$_ac" -ge 0 ] 2>/dev/null; then
      FANOUT_SHARD_BASE=$(( (_ac + 1) % NUM_SHARDS ))
      echo "    FANOUT_SHARD_BASE=auto resolved to shard $FANOUT_SHARD_BASE (matcher is on shard $_ac)"
    else
      FANOUT_SHARD_BASE=0
      echo "    FANOUT_SHARD_BASE=auto could not read placement; falling back to 0" >&2
    fi
  fi
fi

# A base of "auto" that never got resolved (e.g. MODE=partitions skips the block
# above) would be used as a number and silently break the arithmetic.
if [ "$FANOUT_SHARD_BASE" = auto ]; then
  echo "run-e2-throughput.sh: FANOUT_SHARD_BASE=auto was not resolved in this mode; using 0" >&2
  FANOUT_SHARD_BASE=0
fi

# Where did each partition's leader actually land? This is the observation that
# makes E2.4 interpretable - throughput that does not scale with partitions
# means nothing if the partitions never left one core.
report_placement() {
  local topic="$1"
  echo "--- leader placement for $topic (node/core per partition) ---"
  # LEADER placement - replicas[] order is arbitrary and replicas[0] is not the
  # leader (the response carries leader_id). Reading replicas[0] is correct only
  # at RF=1 and silently reports a random follower at RF>1.
  curl -sf "$ADMIN_URL/v1/cluster/partitions" 2>/dev/null | \
    jq -r --arg t "$topic" '[.[] | select(.topic==$t)] | sort_by(.partition_id) | .[]
      | .partition_id as $p | .leader_id as $l
      | (.replicas[] | select(.node_id==$l) | "    p\($p) -> node \($l) core \(.core)")' 2>/dev/null
  echo "    distinct leader cores used: $(curl -sf "$ADMIN_URL/v1/cluster/partitions" 2>/dev/null | jq -r --arg t "$topic" '[.[] | select(.topic==$t) | .leader_id as $l | (.replicas[] | select(.node_id==$l) | "\($l)/\(.core)")] | unique | length' 2>/dev/null)"
}

deploy_producer() {
  local guest="$1" name="$2"
  local wasm
  case "$guest" in
    matcher)     wasm="$BIN/wasm-matcher.wasm" ;;
    passthrough) wasm="$BIN/passthrough.wasm" ;;
    *) echo "unknown guest $guest" >&2; return 1 ;;
  esac
  [ -f "$wasm" ] || { echo "ERROR: $wasm not found - run make build-wasm / make build-passthrough" >&2; return 1; }
  echo "--- deploying producer $name ($guest) ---"
  rpkc transform deploy --name "$name" \
    --input-topic "$orders" --output-topic "$fills" \
    --var RISK_LIMIT=0 --file "$wasm" >/dev/null
  wait_for_transforms_running "$name"
}

deploy_consumer() {
  local name="$1"
  if [ "$MODE" = "partitions" ]; then
    # No RELAY_TARGET_SHARD: let the consumer land wherever its input
    # partition's leadership puts it, which is the behaviour under test. A
    # static pin would force every consumer onto one core and hide the effect.
    echo "--- deploying relay consumer $name (unpinned; follows partition leadership) ---"
    rpkc transform deploy --name "$name" \
      --input-topic "$fills" --output-topic "$probe_out" \
      --var RELAY_SOURCE=1 --file "$BIN/relay-probe.wasm" >/dev/null
    wait_for_transforms_running "$name"
    return
  fi
  echo "--- deploying relay consumer $name on shard $PROBE_CORE ---"
  rpkc transform deploy --name "$name" \
    --input-topic "$fills" --output-topic "$probe_out" \
    --var RELAY_SOURCE=1 --var "RELAY_TARGET_SHARD=$PROBE_CORE" \
    --file "$BIN/relay-probe.wasm" >/dev/null
  wait_for_transforms_running "$name"
  wait_for_relay_subscriptions 1
}

# MODE=fanout: N relay consumers on ONE partition, which is the customer shape.
#
# Only consumer 0 writes to $probe_out; consumers 1..N-1 write to $probe_sink,
# which nothing reads. That split is deliberate and load-bearing: loadgen
# correlates one receipt per order and treats a second as a duplicate, so if
# all N consumers wrote to $probe_out every order would arrive N times and
# `max_receipts_for_any_sampled_order` would report N - a real verdict failure
# caused entirely by the harness. Measuring one representative consumer while
# the other N-1 supply the fan-out is the standard shape for this experiment.
#
# Known confound, recorded rather than hidden: the N-1 unmeasured consumers
# still *produce* to $probe_sink, so their write load is inside the numbers.
# That is arguably realistic (real consumers do work), but it means this sweep
# measures "fan-out with N working consumers", not the dispatch loop in
# isolation. A zero-emit guest variant would isolate the loop; that is a
# follow-up, not this sweep.
deploy_fanout_consumers() {
  local n="$1" i shard name
  # Fail here, not on consumer 2 of 50. Both guests are required: the measured
  # one emits receipts, the fillers must NOT, and silently substituting
  # relay-probe for relay-sink is what invalidated Round 2.
  for w in relay-probe relay-sink; do
    [ -f "$BIN/$w.wasm" ] || {
      echo "ERROR: $BIN/$w.wasm not found - run 'make build-$w' and redeploy." >&2
      return 1
    }
  done
  if [ -n "$FANOUT_SHARDS" ]; then
    echo "--- deploying $n relay consumers, round-robin across $FANOUT_SHARDS shard(s) from base $FANOUT_SHARD_BASE (of $NUM_SHARDS) ---"
  else
    echo "--- deploying $n relay consumers, round-robin across all $NUM_SHARDS shards from base $FANOUT_SHARD_BASE ---"
  fi
  echo "    consumer 0 = relay-probe (measured, emits receipts to $probe_out)"
  echo "    consumers 1..$((n - 1)) = relay-sink (zero-emit fillers)"
  for i in $(seq 0 $((n - 1))); do
    # Spread across the producer node's shards. Leaving every consumer on the
    # producer's shard is what made relay_consume grow with fanout before the
    # cross-shard fix; a sweep that re-creates that measures contention, not
    # fan-out.
    shard=$(( (FANOUT_SHARD_BASE + (i % ${FANOUT_SHARDS:-$NUM_SHARDS})) % NUM_SHARDS ))
    name="probe-e2-${RUN_ID}-${i}"
    if [ "$i" -eq 0 ]; then
      # The ONE measured consumer: relay-probe emits a receipt per fill, which
      # is what loadgen correlates to produce the latency distribution.
      rpkc transform deploy --name "$name" \
        --input-topic "$fills" --output-topic "$probe_out" \
        --var RELAY_SOURCE=1 --var "RELAY_TARGET_SHARD=$shard" \
        --file "$BIN/relay-probe.wasm" >/dev/null
    else
      # The N-1 fillers: relay-sink RECEIVES AND PARSES every pushed record and
      # writes nothing. Using relay-probe here is what invalidated Round 2 - at
      # fanout 50 the fillers produced ~33M extra records in a 30s window and
      # the cluster saturated on the benchmark's own output instead of on relay
      # dispatch. They still own a VM, a pending queue and a scheduling slot,
      # which are the costs the ladder is meant to measure.
      rpkc transform deploy --name "$name" \
        --input-topic "$fills" --output-topic "$probe_sink" \
        --var RELAY_SOURCE=1 --var "RELAY_TARGET_SHARD=$shard" \
        --file "$BIN/relay-sink.wasm" >/dev/null
    fi
  done
  # Gate on the relay's own subscription gauge on the PRODUCER's node, not on
  # `rpk transform list`: RELAY_TARGET_SHARD pins shard N on every node, and an
  # idle duplicate on a non-producer node reports "running" before the real
  # subscription exists. Pushes sent before a subscription exists are dropped
  # silently and permanently - push() treats "no subscriber" as a no-op.
  wait_for_relay_subscriptions "$n"
  # Subscriptions being up is not enough - see the comment on this function.
  # Without it, retried processor starts land inside the window and report the
  # level unclean even though nothing was lost.
  wait_for_transform_failures_quiescent
}

# The external arm's fan-out load: N-1 ordinary Kafka consumer groups fetching
# the matcher's output topic, which is what the in-broker relay consumers are
# being compared against. loadgen itself is the Nth (measured) consumer, so the
# two arms have the same total consumer count.
#
# Started per level and stopped after it. cmd/fanout-load exists precisely for
# this question - it was written to ask whether heavy fetch load on an output
# topic degrades the in-broker path differently than the external one.
start_external_fanout() {
  local n="$1"
  local load=$(( n - 1 ))
  [ -f "$BIN/fanout-load" ] || {
    echo "ERROR: $BIN/fanout-load not found - run 'make build-fanout' and redeploy." >&2
    return 1
  }
  if [ "$load" -lt 1 ]; then
    echo "--- external arm: fanout $n needs no background load consumers ---"
    : > "$RESULTS_DIR/${tag}.fanoutload.pid"
    return 0
  fi
  echo "--- external arm: starting $load external Kafka consumer groups on $fills ---"
  "$BIN/fanout-load" -brokers "$BROKERS" -topic "$fills" -n "$load" \
    -group-prefix "extfan-$RUN_ID-$level" \
    > "$RESULTS_DIR/${tag}.fanoutload.log" 2>&1 &
  echo $! > "$RESULTS_DIR/${tag}.fanoutload.pid"
  # Give the groups time to join and start fetching, or the level measures a
  # ramp-up rather than steady-state fan-out.
  sleep 10
}

stop_external_fanout() {
  local pf="$RESULTS_DIR/${tag}.fanoutload.pid"
  [ -s "$pf" ] || return 0
  local pid; pid="$(cat "$pf")"
  # Kill by RECORDED PID, never by pattern - a pkill pattern here has three
  # times matched this harness's own command line and killed the controlling
  # shell.
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$pf"
}

# Abort before the sweep, not mid-ladder, if the per-core instance cap cannot
# hold the largest level. RELAY_TARGET_SHARD=N creates a processor on EVERY node
# that has shard N, so each consumer costs one instance slot per node, and the
# admission check scales by the deciding node's own core count.
#
# This check exists because the cap is `needs_restart::yes` and the old fanout
# runner raised it with a live `cluster config set` that cannot take effect
# (METHODOLOGY #13). That went unnoticed only because fanout <= 100 fits under
# the default. At 250+ it fails, and it fails several levels into a long run.
preflight_instance_capacity() {
  local max_level=0 lvl need cap have_capacity
  for lvl in $SWEEP; do
    [ "$lvl" -gt "$max_level" ] 2>/dev/null && max_level="$lvl"
  done
  need=$(( max_level + 1 ))   # N consumers + the producer
  cap="$(rpkc cluster config get data_transforms_max_instances_per_core 2>/dev/null | tr -d '[:space:]')"
  case "$cap" in ''|*[!0-9]*) echo "WARNING: could not read data_transforms_max_instances_per_core; skipping capacity pre-flight" >&2; return 0 ;; esac
  have_capacity=$(( cap * NUM_SHARDS ))
  echo "--- instance capacity pre-flight: need >= $need per node, cap ${cap}/core x ${NUM_SHARDS} cores = $have_capacity ---"
  if [ "$have_capacity" -lt "$need" ]; then
    cat >&2 <<EOF
ERROR: max fanout level $max_level needs >= $need partition-instances per node,
       but data_transforms_max_instances_per_core=$cap on $NUM_SHARDS cores gives
       only $have_capacity. The sweep would fail partway through.

       Fix, and note this property is needs_restart::yes so a live set does
       NOTHING until the brokers are rolled:
         rpk cluster config set data_transforms_max_instances_per_core $(( (need / NUM_SHARDS) + 1 ))
         <roll every broker>   # e.g. ./wb restart, which rolls and waits

       The reported value is the DESIRED value; a running broker keeps its boot
       value (METHODOLOGY #13). If you set it without rolling, this check passes
       and the deploy still fails.
EOF
    return 1
  fi
}

{
  echo "# fingerprint: MODE=$MODE RF=$RF write.caching=$WRITE_CACHING${FLUSH_MS:+ flush.ms=$FLUSH_MS}"
  echo "# rate=$RATE payload=$PAYLOAD guest=$GUEST duration=$DURATION warmup=$WARMUP pacing=$PACING linger_ms=$LINGER_MS producers=$PRODUCERS fanout=$FANOUT"
  echo "# pin_node=$PIN_NODE probe_core=$PROBE_CORE num_shards=$NUM_SHARDS run_id=$RUN_ID read_linger_us=$READ_LINGER_US relay_stage_metrics=${RELAY_STAGE_METRICS:-unset} consumer_arm=$CONSUMER_ARM"
  echo "# fanout_shards=${FANOUT_SHARDS:-all($NUM_SHARDS)} fanout_shard_base=$FANOUT_SHARD_BASE"
  echo "# producer_acks=$ACKS"
  echo "# fresh_topics_per_level=$FRESH_TOPICS_PER_LEVEL settle=${SETTLE}s"
  echo "# admin_endpoints_scraped=$(echo "$ADMIN_URLS" | tr ',' '\n' | grep -c .)"
  echo "level offered/s achieved/s cores relay_p50us relay_p99us total_p50us match_us/inv match_us/order probe_us/inv dropped clean"
} > "$RESULTS_DIR/summary.txt"

if [ "$MODE" = "fanout" ]; then
  preflight_instance_capacity || exit 1
  # Record the boot reservation as a capacity cost of the approach. It is taken
  # on every core whether or not anything is deployed, and a customer will ask.
  _cap="$(rpkc cluster config get data_transforms_max_instances_per_core 2>/dev/null | tr -d '[:space:]')"
  _fnmem="$(rpkc cluster config get data_transforms_per_function_memory_limit 2>/dev/null | tr -d '[:space:]')"
  case "$_cap$_fnmem" in
    ''|*[!0-9]*) echo "# boot_reservation=unknown (could not read cap/per-function limit)" >> "$RESULTS_DIR/summary.txt" ;;
    *) echo "# boot_reservation_per_core_bytes=$(( _cap * _fnmem )) per_core_instances=$_cap per_function_bytes=$_fnmem cores=$NUM_SHARDS total_per_broker_bytes=$(( _cap * _fnmem * NUM_SHARDS ))" >> "$RESULTS_DIR/summary.txt" ;;
  esac
fi

# RELAY_STAGE_METRICS was, like READ_LINGER_US, only ever PRINTED in the
# fingerprint line while nothing applied it - so the run record could state a
# value the cluster never had. Applied here through cfg_set, which verifies the
# write and registers it for cfg_restore_all, so callers never need to touch rpk
# themselves (two wrappers tried, with a bare `rpk` that is not on PATH on the
# client: "run-josh236.sh: line 66: rpk: command not found").
if [ -n "${RELAY_STAGE_METRICS:-}" ]; then
  echo "--- relay stage metrics for this sweep: $RELAY_STAGE_METRICS ---"
  cfg_set relay_stage_metrics_enabled "$RELAY_STAGE_METRICS"
fi

# READ_LINGER_US used to be applied ONLY in MODE=readlinger, while every mode
# printed it in the per-level header. So `READ_LINGER_US=250 MODE=fanout`
# announced read_linger_us=250 and ran with whatever the cluster already had -
# a run record that states its own configuration incorrectly. Apply it here so
# it holds for every mode; readlinger still overrides per level below.
if [ "$MODE" != "readlinger" ]; then
  echo "--- read linger for this sweep: ${READ_LINGER_US}us ---"
  cfg_set data_transforms_read_linger_us "$READ_LINGER_US"
fi

producer_name=""
for level in $SWEEP; do
  this_rate="$RATE"; this_payload="$PAYLOAD"; this_guest="$GUEST"; this_fanout="$FANOUT"; this_read_linger="$READ_LINGER_US"
  case "$MODE" in
    rate)    this_rate="$level" ;;
    payload) this_payload="$level" ;;
    guest)   this_guest="$level" ;;
    fanout)  this_fanout="$level" ;;
    readlinger)
      this_read_linger="$level"
      # needs_restart::no, so this takes effect live between levels.
      cfg_set data_transforms_read_linger_us "$level" ;;
  esac
  tag="${MODE}-${level}"
  echo ""
  echo "############ $tag : rate=$this_rate payload=$this_payload guest=$this_guest fanout=$this_fanout read_linger_us=$this_read_linger ############"

  # Guard against defect 19's class: a topic variable silently reassigned to
  # something that is not a topic name. Cheap, and it converts a four-hour
  # phantom-ceiling investigation into an immediate, named failure.
  for tv in orders fills probe_out probe_sink; do
    case "${!tv}" in
      *-e2-"$RUN_ID") ;;
      *) echo "ERROR: \$$tv is '${!tv}', which is not this run's topic name (expected *-e2-$RUN_ID)." >&2
         echo "       A topic variable has been clobbered - see defect 19. Refusing to run." >&2
         exit 1 ;;
    esac
  done

  # In guest mode the producer changes per level, so redeploy both and let the
  # relay subscription re-register against the new fills producer. In the other
  # modes the deploy is stable across levels and only the load changes.
  if [ "$MODE" = "partitions" ] || [ "$FRESH_TOPICS_PER_LEVEL" = "1" ]; then
    # Fresh topics. Transforms must go first: a topic delete with a transform
    # still attached fails with a misleading CLUSTER_AUTHORIZATION_FAILED.
    # In partitions mode the count comes from $level; otherwise keep 1.
    this_parts=1
    [ "$MODE" = "partitions" ] && this_parts="$level"
    rpkc transform list 2>/dev/null | awk 'NR>1{print $1}' | \
      xargs -r -n1 "$RPK" "${RPK_ARGS[@]}" transform delete --no-confirm >/dev/null 2>&1 || true
    for _ in $(seq 1 15); do
      [ -z "$(rpkc transform list 2>/dev/null | awk 'NR>1{print $1}')" ] && break
      sleep 1
    done
    # $probe_sink MUST be in this list. It was omitted when probe_sink was added
    # for the zero-emit relay-sink fillers, and since the create below covers all
    # four, the leftover probe_sink made `topic create` fail and set -e killed the
    # whole sweep after level 1 - observed 2026-09-02 on the first run that used
    # FRESH_TOPICS_PER_LEVEL=1 since probe_sink existed. A delete list and a
    # create list that disagree is a silent trap: it only fires on the code path
    # nobody exercises.
    rpkc topic delete "$orders" "$fills" "$probe_out" "$probe_sink" >/dev/null 2>&1 || true
    sleep 3
    echo "--- creating $orders with $this_parts partition(s) (fills/probe-out matched) ---"
    rpkc topic create "$orders" -p "$this_parts" -r "$RF" >/dev/null
    rpkc topic create "$fills" -p "$this_parts" -r "$RF" >/dev/null
    rpkc topic create "$probe_out" -p "$this_parts" -r "$RF" >/dev/null
    rpkc topic create "$probe_sink" -p "$this_parts" -r "$RF" >/dev/null
    apply_topic_durability "$orders" "$fills" "$probe_out"
    sleep 8
    # The relay has no cross-node hop, so the matcher's input topic and the
    # relay ntp (its output topic) must be node-co-located partition-for-
    # partition or the consumer receives nothing at all. Pin both onto PIN_NODE
    # and let the balancer spread them across that node's CORES - which is
    # exactly the variable under test. probe_out is written by ordinary Kafka
    # produce, not the relay, so it needs no pinning.
    if [ "$RF" -gt 1 ]; then
      colocate_leadership "$fills" "$orders" "$this_parts"
    else
      pin_topic_to_node "$orders" "$PIN_NODE" "$this_parts"
      pin_topic_to_node "$fills" "$PIN_NODE" "$this_parts"
    fi
    report_placement "$orders"
    report_placement "$fills"
    producer_name=""   # force redeploy against the new topics
  fi

  want_producer="wasm-${this_guest}-e2-${RUN_ID}"
  if [ "$MODE" = "fanout" ]; then
    # The consumer COUNT is the variable, so consumers are torn down and
    # redeployed at every level. Delete by listing what is actually there
    # rather than by iterating the previous level's count: a partially failed
    # deploy would otherwise leave orphans that silently inflate the next
    # level's fanout, which is the confound this whole sweep is about.
    rpkc transform list 2>/dev/null | awk -v r="probe-e2-${RUN_ID}-" 'NR>1 && index($1,r)==1{print $1}' | \
      xargs -r -n1 "$RPK" "${RPK_ARGS[@]}" transform delete --no-confirm >/dev/null 2>&1 || true
    if [ "$producer_name" != "$want_producer" ]; then
      [ -n "$producer_name" ] && rpkc transform delete "$producer_name" --no-confirm >/dev/null 2>&1 || true
      sleep 3
      deploy_producer "$this_guest" "$want_producer"
      producer_name="$want_producer"
    fi
    # Wait for the relay to actually forget the old subscriptions before
    # counting the new ones, or wait_for_relay_subscriptions can be satisfied
    # by the PREVIOUS level's stragglers at a lower target.
    sleep 5
    if [ "$CONSUMER_ARM" = "external" ]; then
      # No relay consumers at all in this arm - the matcher's output is consumed
      # from outside, which is the comparison.
      start_external_fanout "$this_fanout" || { echo "external fan-out load failed to start" >&2; exit 1; }
    else
      deploy_fanout_consumers "$this_fanout"
    fi
  elif [ "$producer_name" != "$want_producer" ] || [ "$MODE" = "partitions" ]; then
    [ -n "$producer_name" ] && rpkc transform delete "$producer_name" --no-confirm >/dev/null 2>&1 || true
    consumer_name="relay-probe-e2-${RUN_ID}"
    rpkc transform delete "$consumer_name" --no-confirm >/dev/null 2>&1 || true
    sleep 3
    deploy_producer "$this_guest" "$want_producer"
    deploy_consumer "$consumer_name"
    producer_name="$want_producer"
  fi

  # NOTE on which resource figures to trust. This outer snapshot pair brackets
  # loadgen's WHOLE invocation - warmup included - so per-order figures derived
  # from it are inflated by the warmup's CPU (the warmup runs at the same rate,
  # so a 10s warmup on a 30s window overstates per-order cost by ~33%).
  # loadgen's OWN internal scrape brackets the timed window only, so
  # `.resources.per_function` in its report is the clean one, and the summary
  # table below reads from there. This pair is kept for the wider per-shard
  # scheduling-group view loadgen does not collect, where warmup inclusion is
  # harmless because it is read as a distribution across shards rather than as
  # a per-record cost.
  # Deliverability pre-check. Ten orders at a trivial rate, asserting receipts
  # come back, before spending a full level. Both zero-receipt failures this
  # harness has produced (relay node-locality, and the topic-name clobber of
  # defect 19) would have been caught here in seconds instead of after a
  # multi-minute level - or, in the clobber's case, after a five-level sweep.
  # Receipt plumbing differs per arm, so build the flags once and use the same
  # ones in the pre-check and the timed run - a pre-check that measures a
  # different path than the run is worse than no pre-check.
  recv_args=(-probe-topic "$probe_out")
  if [ "$CONSUMER_ARM" = "external" ]; then
    recv_args=(-receipt-source fills -fills-topic "$fills")
  fi

  echo "--- pre-check: 10 orders must produce receipts ---"
  if ! "$BIN/loadgen" -brokers "$BROKERS" -input-topic "$orders" "${recv_args[@]}" \
    -clock-rms-micros "${CLOCK_RMS_MICROS:-0}" \
       -num-probes 1 -rate 20 -duration 1s -warmup 0s -drain 15s -sample-every 1 \
       -producers 1 -linger-ms "$LINGER_MS" -acks "$ACKS" -label precheck \
       -out "$RESULTS_DIR/${tag}.precheck.json" >/dev/null 2>&1; then
    echo "    pre-check invocation failed" >&2
  fi
  pre_recv=$(jq -r '.received_receipts // 0' "$RESULTS_DIR/${tag}.precheck.json" 2>/dev/null || echo 0)
  if [ "${pre_recv:-0}" -lt 1 ]; then
    echo "!!! PRE-CHECK FAILED for $tag: 0 receipts from 10 orders." >&2
    echo "    The pipeline is not delivering. Check, in this order: the echoed" >&2
    echo "    -input-topic below matches $orders; producer input and relay ntp are" >&2
    echo "    node-co-located (the relay has no cross-node hop); both transforms" >&2
    echo "    report running. Skipping this level rather than recording noise." >&2
    printf "%s %s PRECHECK-FAILED - - - - - - - - false\n" "$level" "$this_rate" >> "$RESULTS_DIR/summary.txt"
    sleep "$SETTLE"
    continue
  fi
  echo "    ok: $pre_recv receipts"

  bash "$SCRAPE" snap "$ADMIN_URLS" "$RESULTS_DIR/${tag}.before.json"

  echo "--- loadgen: arm=$CONSUMER_ARM -input-topic $orders ${recv_args[*]} -rate $this_rate -producers $PRODUCERS -linger-ms $LINGER_MS -acks $ACKS ---"
  "$BIN/loadgen" \
    -clock-rms-micros "${CLOCK_RMS_MICROS:-0}" \
    -brokers "$BROKERS" -input-topic "$orders" "${recv_args[@]}" \
    -num-probes 1 -rate "$this_rate" -duration "$DURATION" -warmup "$WARMUP" \
    -pacing "$PACING" -payload-bytes "$this_payload" -sample-every "$SAMPLE_EVERY" \
    -linger-ms "$LINGER_MS" -acks "$ACKS" -producers "$PRODUCERS" \
    -drain "$DRAIN" -admin-url "$ADMIN_URLS" -transform-name "$want_producer" \
    -label "$tag" -out "$RESULTS_DIR/${tag}.json" || true

  bash "$SCRAPE" snap "$ADMIN_URLS" "$RESULTS_DIR/${tag}.after.json"
  [ "$CONSUMER_ARM" = "external" ] && stop_external_fanout

  # A level that dies (loadgen crash, broker wedge) must not take the rest of
  # the sweep with it - the surviving levels are still a usable curve, and an
  # aborted sweep costs a whole cluster session to redo.
  if [ ! -s "$RESULTS_DIR/${tag}.json" ]; then
    echo "!!! level $tag produced no report; recording it as failed and continuing"
    printf "%s %s FAILED - - - - - - - - false\n" "$level" "$this_rate" >> "$RESULTS_DIR/summary.txt"
    sleep "$SETTLE"
    continue
  fi

  # Orders, not receipts. scrape-metrics reads each guest's own invocation count
  # from redpanda_transform_execution_latency_sec_count for the per-record
  # figure; orders are only used for the per-order figure, which is the unit
  # -rate is expressed in.
  # NOTE the variable name. This was `orders` once, which silently overwrote the
  # INPUT TOPIC NAME (`orders="orders-e2-$RUN_ID"`) with an order count. Level 1
  # ran fine; every level after it produced to a topic literally named "30000",
  # got UNKNOWN_TOPIC_OR_PARTITION for 100% of records from its first warmup
  # record, and reported zero receipts. That was defect 19, and it cost roughly
  # four hours and seven wrong hypotheses (fsync, linger, client in-flight caps,
  # leadership churn, processor restarts, stale binary, unscoped cleanup) before
  # the cause turned out to be a name collision introduced while improving unit
  # clarity elsewhere in this very block.
  # Deliberately NO order count passed. This snapshot pair brackets loadgen's
  # WHOLE invocation, warmup included, so its invocation delta covers more
  # records than `orders_sent` counts - at 1,000 orders/sec that was 80,000
  # invocations against 30,000 timed orders, and a per-order figure computed
  # from those two mismatched windows read 6.94us where the true value was
  # 5.20us (ratio 2.67 instead of the 2.00 an order-is-two-records workload
  # must give). Omitting the count makes scrape-metrics print only
  # per-invocation, over its own internally consistent window.
  #
  # The authoritative per-order and per-invocation figures come from loadgen's
  # own report, whose scrape brackets the timed window only (verified:
  # invocations == records_sent, window_coverage == 1.0), and that is what the
  # summary table below reads.
  bash "$SCRAPE" delta "$RESULTS_DIR/${tag}.before.json" "$RESULTS_DIR/${tag}.after.json" \
    "" "$RESULTS_DIR/${tag}.resources.json" >/dev/null

  achieved=$(jq -r '.rates.achieved_orders_per_sec | floor' "$RESULTS_DIR/${tag}.json")
  rp50=$(jq -r '.relay_consume.p50_micros' "$RESULTS_DIR/${tag}.json")
  rp99=$(jq -r '.relay_consume.p99_micros' "$RESULTS_DIR/${tag}.json")
  tp50=$(jq -r '.total.p50_micros' "$RESULTS_DIR/${tag}.json")
  dropped=$(jq -r '.resources.relay_dropped_delta' "$RESULTS_DIR/${tag}.json")
  clean=$(jq -r '.clean' "$RESULTS_DIR/${tag}.json")
  # From loadgen's own report: its internal scrape brackets the timed window
  # only, so these are free of warmup contamination (see the note above).
  minv=$(jq -r --arg n "$want_producer" '.resources.per_function[$n].cpu_micros_per_invocation // "n/a"' "$RESULTS_DIR/${tag}.json")
  mord=$(jq -r --arg n "$want_producer" '.resources.per_function[$n].cpu_micros_per_order // "n/a"' "$RESULTS_DIR/${tag}.json")
  # Consumer CPU. In fanout mode there are N consumers named
  # probe-e2-$RUN_ID-<i>, so a single-name lookup reports n/a at every level -
  # which is exactly what Round 2 (2026-08-29) did while the data sat in
  # per_function all along. Aggregate instead: weight per-invocation cost by
  # each consumer's own invocation count, so the figure is the true mean cost
  # per invocation across the fan-out rather than an unweighted average of
  # averages (which would let an idle duplicate processor drag it down).
  if [ "$MODE" = "fanout" ]; then
    pinv=$(jq -r --arg pfx "probe-e2-${RUN_ID}-" '
      [.resources.per_function | to_entries[]
       | select(.key | startswith($pfx))
       | select(.value.invocations > 0)
       | {c: .value.cpu_seconds_delta, i: .value.invocations}]
      | if length == 0 then "n/a"
        else ((map(.c) | add) * 1000000) / (map(.i) | add)
        end' "$RESULTS_DIR/${tag}.json")
  else
    pinv=$(jq -r --arg n "relay-probe-e2-${RUN_ID}" '.resources.per_function[$n].cpu_micros_per_invocation // "n/a"' "$RESULTS_DIR/${tag}.json")
  fi

  # How many distinct cores the input partitions' leaders actually occupy. In
  # partitions mode this is the whole point: throughput that fails to scale
  # means nothing if the partitions never spread off one core.
  # Distinct LEADER cores the input partitions occupy. Two bugs lived here:
  # it read `.replicas[0]` (arbitrary order - the leader is named by
  # `leader_id`, so this reported a random follower at RF>1), and it ran after
  # defect 19's clobber, so the topic filter matched nothing and the column
  # printed 0 on every rate sweep. Both fixed; a missing value now prints "?"
  # rather than a plausible-looking number.
  cores_used=$(curl -sf "$ADMIN_URL/v1/cluster/partitions" 2>/dev/null | \
    jq -r --arg t "$orders" '[.[] | select(.topic==$t) | .leader_id as $l
       | (.replicas[] | select(.node_id==$l) | "\($l)/\(.core)")] | unique | length' 2>/dev/null)
  # 0 means the topic was not found, which is a bug, not a measurement.
  [ "${cores_used:-0}" = "0" ] && cores_used="?"
  printf "%s %s %s %s %s %s %s %s %s %s %s %s\n" \
    "$level" "$this_rate" "$achieved" "${cores_used:-?}" "$rp50" "$rp99" "$tp50" "$minv" "$mord" "$pinv" "$dropped" "$clean" \
    >> "$RESULTS_DIR/summary.txt"

  if [ "$clean" != "true" ]; then
    echo "--- level $tag NOT CLEAN:"
    jq -r '(.unclean_reasons // [])[] | "      " + .' "$RESULTS_DIR/${tag}.json"
  fi

  echo "--- settling ${SETTLE}s before the next level ---"
  sleep "$SETTLE"
done

echo ""
echo "=== E2 ($MODE) summary ==="
column -t "$RESULTS_DIR/summary.txt"
echo ""
echo "Reading this table:"
echo "  * The knee is the first level where achieved_per_sec stops tracking offered_per_sec."
echo "    Latency at and past that point is a queue depth, not a latency - clean=false says so."
echo "  * match_us/inv is CPU per GUEST INVOCATION (the real per-record cost);"
echo "    match_us/order is per ORDER, which is a crossing pair = 2 invocations, and is"
echo "    the figure a capacity claim stated in orders/sec must use. Both are reported"
echo "    because confusing them is a clean 2x error - invocations are measured from"
echo "    redpanda_transform_execution_latency_sec_count, never inferred."
echo "  * total_p50_us is CROSS-CLOCK (client clock minus broker guest clock). Quote it only"
echo "    with clock-sync.json's residual attached; relay_p50_us is single-clock and safe."
echo ""
echo "Results: $RESULTS_DIR"
