#!/usr/bin/env bash
# Cloud/real-hardware counterpart to run-fanout-comparison.sh. Runs on a
# client instance against a real multi-broker RF=3 cluster (no
# --overprovisioned, SMT disabled, one OS-reserved core, real dedicated
# NVMe/network) instead of the local single-process sandbox.
#
# Two differences from the local script, both deliberate:
#   1. No `make build` - the Go/Rust binaries are pre-shipped to
#      /opt/wasm-bench by the ansible playbook (cross-compiled locally,
#      not built on the client).
#   2. The "external, non-wasm-engine-path" comparison leg runs
#      cmd/wasm-client (hosting the SAME wasm-matcher.wasm the in-broker
#      leg deploys) instead of cmd/kafka-matcher's hand-written Go
#      matching logic - this is the standardized external client
#      decided on for testing wasm-engine improvements, and it makes the
#      in-broker vs. external comparison apples-to-apples (identical
#      matching logic, only the execution location differs) rather than
#      comparing two independently-implemented matchers.
set -euo pipefail

BIN="${BIN:-/opt/wasm-bench}"
RPK="$BIN/rpk"
BROKERS="${BROKERS:?set to comma-separated private_ip:9092 list}"
ADMIN_URL="${ADMIN_URL:?set to http://<seed_private_ip>:9644}"
N="${N:-300}"
# When true, the external leg's matcher is already running on a SEPARATE
# instance (started by the caller) rather than co-located with bench here.
EXTERNAL_MATCHER_PRESTARTED="${EXTERNAL_MATCHER_PRESTARTED:-false}"
WARMUP="${WARMUP:-20}"
FANOUT_N="${FANOUT_N:-100}"
RESULTS_DIR="${RESULTS_DIR:-$BIN/results}"
mkdir -p "$RESULTS_DIR"
RUN_ID="${RUN_ID:?set to a unique id, e.g. date +%s - Date.now() is not available in the caller}"

# This cluster is dedicated solely to this benchmark - no other tenants -
# so a full sweep is simpler and more robust than trying to enumerate
# every prefix this harness's own tools create (missed cmd/bench's own
# "bench-<label>-<runid>" consumer group the first time through). Confirmed
# the actual cost of skipping this: 4000+ leftover fanout-load consumer
# groups from earlier runs made a broker restart take 10+ minutes replaying
# group-manager recovery, instead of the usual ~15 seconds - never skip
# this, on success OR failure.
cleanup_all_test_artifacts() {
  echo "--- cleanup: removing all groups/topics/transforms before/after this run ---"
  "$RPK" -X "brokers=$BROKERS" transform list 2>/dev/null | awk 'NR>1{print $1}' | \
    xargs -r -n 20 "$RPK" -X "brokers=$BROKERS" transform delete --no-confirm 2>/dev/null || true
  "$RPK" -X "brokers=$BROKERS" group list 2>/dev/null | awk 'NR>1{print $2}' | \
    xargs -r -n 200 "$RPK" -X "brokers=$BROKERS" group delete 2>/dev/null || true
  "$RPK" -X "brokers=$BROKERS" topic list 2>/dev/null | awk 'NR>1 && $1 !~ /^_/{print $1}' | \
    xargs -r -n 50 "$RPK" -X "brokers=$BROKERS" topic delete 2>/dev/null || true
  echo "--- cleanup: done (groups left: $("$RPK" -X "brokers=$BROKERS" group list 2>/dev/null | tail -n +2 | wc -l), topics left: $("$RPK" -X "brokers=$BROKERS" topic list 2>/dev/null | tail -n +2 | wc -l)) ---"
}

# EXIT alone does not fire when the shell is killed by an untrapped signal, so
# a `timeout`/Ctrl-C would skip this sweep entirely and strand every fanout-load
# consumer group. That is not a cosmetic leak: this script's own header records
# 4000+ leftover groups turning a broker restart into a 10+ minute
# group-manager replay. Idempotent, so running twice is harmless.
_CLEANUP_DONE=0
cleanup_once() {
  [ "$_CLEANUP_DONE" = 1 ] && return 0
  _CLEANUP_DONE=1
  cleanup_all_test_artifacts
}
trap cleanup_once EXIT
trap 'echo "run-cloud-fanout-comparison: SIGTERM - sweeping groups/topics before exit" >&2; cleanup_once; exit 143' TERM
trap 'echo "run-cloud-fanout-comparison: SIGINT - sweeping groups/topics before exit" >&2;  cleanup_once; exit 130' INT
trap 'cleanup_once; exit 129' HUP
cleanup_all_test_artifacts # also sweep anything left behind by a prior interrupted run

run_leg() {
  local kind="$1" out_prefix="$2" skip_own_cleanup="${3:-false}"
  local orders="orders-${kind}-${RUN_ID}" fills="fills-${kind}-${RUN_ID}"
  # Tolerant of pre-existing topics: in EXTERNAL_MATCHER_PRESTARTED mode the
  # orchestrator creates them first, so the remote matcher can join its
  # consumer group before any orders flow. Without `|| true` set -e aborts the
  # leg on "topic already exists".
  "$RPK" -X "brokers=$BROKERS" topic create "$orders" "$fills" -p 1 -r 3 || true
  "$RPK" -X "brokers=$BROKERS" topic alter-config "$orders" --set write.caching=true
  "$RPK" -X "brokers=$BROKERS" topic alter-config "$fills" --set write.caching=true

  local admin_args=()
  local pid=""
  if [ "$kind" = "wasm" ]; then
    local name="wasm-matcher-cloud-${RUN_ID}"
    "$RPK" -X "brokers=$BROKERS" transform deploy --name "$name" \
      --input-topic "$orders" --output-topic "$fills" \
      --var RISK_LIMIT=0 --file "$BIN/wasm-matcher.wasm"
    for i in $(seq 1 30); do
      "$RPK" -X "brokers=$BROKERS" transform list 2>/dev/null | grep -q "$name.*ACTIVE" && break
      sleep 1
    done
    admin_args=(-admin-url "$ADMIN_URL" -transform-name "$name")
  else
    if [ "$EXTERNAL_MATCHER_PRESTARTED" = "true" ]; then
      # The external matcher runs on its OWN instance, started by the
      # orchestrator, reaching the brokers over the network - which is the
      # whole point of the external leg and cannot be shown by co-locating it
      # with the measuring process.
      #
      # Running it here with `&` instead put wasm-client on the same host as
      # bench AND the fanout-load consumer groups, so the external leg carried
      # CPU on the measuring host that the wasm leg did not. That asymmetry
      # penalises exactly the leg being characterised.
      echo "--- external: matcher is PRESTARTED on a separate instance, consuming $orders -> $fills ---"
      # Assert it is really attached before measuring. A silently dead remote
      # matcher yields an all-zero leg that looks like a latency result.
      local joined=false
      for i in $(seq 1 30); do
        if "$RPK" -X "brokers=$BROKERS" group describe "wasm-client-external-${RUN_ID}" 2>/dev/null \
             | grep -qE "$orders"; then
          joined=true; break
        fi
        sleep 2
      done
      if [ "$joined" != "true" ]; then
        echo "ERROR: prestarted external matcher never joined group wasm-client-external-${RUN_ID} on $orders." >&2
        echo "  Refusing to measure a leg whose matcher is not attached - that produces" >&2
        echo "  zeros indistinguishable from a latency result. Check the matcher host's log." >&2
        return 1
      fi
      echo "    ok: external matcher attached to $orders"
    else
      KAFKA_BROKERS="$BROKERS" INPUT_TOPIC="$orders" OUTPUT_TOPIC="$fills" \
        CONSUMER_GROUP="wasm-client-external-${RUN_ID}" WASM_FILE="$BIN/wasm-matcher.wasm" \
        PRODUCER_LINGER_MS=0 "$BIN/wasm-client" &
      pid=$!
      sleep 3
    fi
  fi

  echo "--- ${kind}: baseline (no fanout) ---"
  "$BIN/bench" -brokers "$BROKERS" -input-topic "$orders" -output-topic "$fills" \
    -n "$N" -warmup "$WARMUP" -label "${kind}-baseline" "${admin_args[@]}" \
    -out "$RESULTS_DIR/${out_prefix}-baseline.json"
  echo "--- ${kind}: baseline bottleneck check ---"
  bash "$BIN/check-bottlenecks.sh" "$ADMIN_URL" kafka "$orders" "$fills" || true

  echo "--- ${kind}: starting $FANOUT_N fanout consumer groups on $fills ---"
  "$BIN/fanout-load" -brokers "$BROKERS" -topic "$fills" -n "$FANOUT_N" \
    -group-prefix "fanout-${kind}-${RUN_ID}" &
  local fanout_pid=$!
  sleep 3

  echo "--- ${kind}: with $FANOUT_N-way fanout active ---"
  "$BIN/bench" -brokers "$BROKERS" -input-topic "$orders" -output-topic "$fills" \
    -n "$N" -warmup "$WARMUP" -label "${kind}-fanout" "${admin_args[@]}" \
    -out "$RESULTS_DIR/${out_prefix}-fanout.json"
  echo "--- ${kind}: fanout bottleneck check ---"
  bash "$BIN/check-bottlenecks.sh" "$ADMIN_URL" kafka "$orders" "$fills" || true
  mv "$RESULTS_DIR/${fills}.diag.json" "$RESULTS_DIR/${out_prefix}-fanout-output.diag.json" 2>/dev/null || true

  kill "$fanout_pid" 2>/dev/null || true
  wait "$fanout_pid" 2>/dev/null || true
  if [ "$kind" = "wasm" ]; then
    "$RPK" -X "brokers=$BROKERS" transform delete "$name" --no-confirm || true
  elif [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
  fi
  # Give fanout-load's ~500 killed consumers time to actually leave their
  # groups (SIGTERM doesn't send LeaveGroup instantly for that many at
  # once) before the NEXT leg starts - otherwise leg 2 can inherit leg 1's
  # still-active reader-cache pressure and/or leg 2's own cleanup attempt
  # hits NON_EMPTY_GROUP errors. This is exactly the isolation gap that
  # made the wasm-vs-external asymmetry ambiguous the first time through.
  #
  # Skipped entirely in CONCURRENT_LEGS mode: cleanup_all_test_artifacts
  # does a blanket sweep of every topic/group/transform on the cluster,
  # not just this leg's own RUN_ID-scoped ones - calling it while the
  # OTHER leg is still mid-flight would delete its topics out from under
  # it. Concurrent mode relies solely on the start/end sweeps below.
  if [ "$skip_own_cleanup" != "true" ]; then
    sleep 10
    cleanup_all_test_artifacts
  fi
}

# LEG_ORDER lets the caller swap which leg runs first (to test whether an
# observed asymmetry tracks the LEG vs. the SLOT), or run just ONE leg
# (e.g. "wasm" alone) when the other leg isn't needed for this pass.
#
# CONCURRENT_LEGS=true runs both legs at once instead of sequentially -
# the ONLY variable this changes vs. the default sequential mode (same N,
# WARMUP, FANOUT_N, phase structure, cleanup-at-start/end). Use this to
# test whether an observed asymmetry depends on sequential/isolated
# execution specifically, without also changing N or phase structure the
# way an ad hoc one-off diagnostic script would.
LEG_ORDER="${LEG_ORDER:-wasm,external}"
IFS=',' read -r FIRST_LEG SECOND_LEG <<< "$LEG_ORDER"
if [ -n "$SECOND_LEG" ] && [ "${CONCURRENT_LEGS:-false}" = "true" ]; then
  run_leg "$FIRST_LEG" "$FIRST_LEG" true &
  LEG1_PID=$!
  run_leg "$SECOND_LEG" "$SECOND_LEG" true &
  LEG2_PID=$!
  wait "$LEG1_PID" "$LEG2_PID"
else
  run_leg "$FIRST_LEG" "$FIRST_LEG"
  [ -n "$SECOND_LEG" ] && run_leg "$SECOND_LEG" "$SECOND_LEG"
fi

echo ""
echo "=== p50/p99 summary (us) ==="
for f in "${FIRST_LEG}-baseline" "${FIRST_LEG}-fanout" ${SECOND_LEG:+"${SECOND_LEG}-baseline" "${SECOND_LEG}-fanout"}; do
  p50=$(jq '.p50_micros' "$RESULTS_DIR/$f.json" 2>/dev/null)
  p99=$(jq '.p99_micros' "$RESULTS_DIR/$f.json" 2>/dev/null)
  clean=$(jq '.clean' "$RESULTS_DIR/$f.json" 2>/dev/null)
  echo "$f: p50=${p50}us p99=${p99}us clean=${clean}"
done
