#!/usr/bin/env bash
# OPTIONAL series: does an in-broker wasm deployment survive leadership moving,
# what does latency do while it happens, and how long until it is normal again?
#
# Not part of `wb reproduce`. The published tables measure steady state, and
# mixing a disturbance into them would make them incomparable. Run this
# separately, on a cluster that already has the wasm arm deployed and healthy.
#
# WHAT IT MEASURES, per action:
#
#   baseline  a quiet window, to establish what "normal" is for THIS cluster
#             (absolute numbers move between cluster instantiations, so the
#              baseline has to be measured, not assumed)
#   during    the same window with the action injected part-way through
#   recovery  repeated short windows until latency is back inside the baseline
#             band AND STAYS there - reported as time-to-baseline
#
# KNOWN LIMITATION OF THE RECOVERY VERDICT, and it bites the FIRST arm of every
# invocation. All arms share one set of topics (orders-res-$RUN_ID, created once
# below), so the log grows monotonically underneath the whole series and never
# resets. Arm 1's baseline is therefore measured on an almost empty log while
# its recovery windows run against a much larger one, and p99 is sensitive to
# that: measured 2026-09-09, baselines drifted 740 -> 1067 -> 1114 -> 1195us
# across four arms as the shared topic grew to 1.52M records, and arm 1 reported
# DID NOT RECOVER while sitting at 1066-1478us - i.e. inside the range that
# every LATER arm measured as its own healthy baseline. A separate invocation
# with fresh topics baselined 824us and recovered normally.
# So: a first-arm "DID NOT RECOVER" is probably this, not the cluster. Compare
# it against the later arms' baselines before believing it. The real fix is to
# stop comparing an empty-log baseline against a full-log recovery - either
# prime the log before the first baseline, or make "recovered" mean "p99 has
# stabilised" rather than "p99 is back under a number measured earlier".
#
# CORRECTNESS IS MEASURED ALONGSIDE LATENCY, and matters more. Transforms are
# at-least-once: a leadership move discards work that was read but not
# committed, and the next owner reprocesses it. So
#   missing receipts   must be ZERO - that would be data loss
#   duplicate receipts are EXPECTED to be non-zero without a graceful drain
# Reporting only latency would miss the entire point.
#
# THE A/B THAT MAKES THIS WORTH RUNNING: with DRAIN_AB=1 each action runs twice,
# once with data_transforms_graceful_transfer_timeout_ms unset (today's
# behaviour) and once with it set. Duplicates should fall toward zero when set,
# at the cost of some added drain time. Without that contrast this script only
# establishes that nothing crashed, which the ducktape suite already covers.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${RPK:?set RPK to an rpk binary}"
: "${BROKERS:?set BROKERS}"
: "${ADMIN_URL:?set ADMIN_URL}"
# shellcheck source=lib-cluster.sh
. "$HERE/lib-cluster.sh"

# Same BIN convention as the other runners: "." on a client instance where wb
# ships the tools, or an absolute path for a local sandbox run.
BIN="${BIN:-.}"
RUN_ID="${RUN_ID:-$(date +%s)}"
OUT_DIR="${OUT_DIR:-$BIN/results/resilience-$RUN_ID}"
LOADGEN="${LOADGEN:-$BIN/loadgen}"

# Which disturbances to run. Space separated, in order.
#   maintenance  enable maintenance on the input partition's leader, then
#                disable it - the operator action Josh asked about directly
#   transfer     an explicit admin leadership transfer, i.e. leadership moving
#                for a reason that has nothing to do with maintenance
#   restart      stop and start the broker leading the input partition
ACTIONS="${ACTIONS:-maintenance transfer}"

RATE="${RATE:-1000}"
PAYLOAD="${PAYLOAD:-650}"
ACKS="${ACKS:-all}"
RF="${RF:-3}"
WRITE_CACHING="${WRITE_CACHING:-true}"
LINGER_MS="${LINGER_MS:-0}"

# Per-run topic and transform names, like the other runners. NOT fixed names:
# a previous run's `orders` still holding data is how a level ends up measuring
# a log it did not write, and reusing a transform name across runs makes the
# deploy offset - which the wasm factory cache is keyed on - ambiguous.
ORDERS_TOPIC="orders-res-${RUN_ID}"
FILLS_TOPIC="fills-res-${RUN_ID}"
MATCHER_NAME="matcher-res-${RUN_ID}"

# 60s, not 30s. A short baseline catches an unrepresentatively quiet stretch,
# and then a perfectly healthy cluster reads as never having recovered.
# Measured 2026-09-09: baselines taken over 30s came in at 770-890us while the
# same cluster's post-disturbance steady state sat at 1025-1180us, so several
# arms reported "DID NOT RECOVER" while flat and healthy. Session baselines also
# drifted upward monotonically (839 -> 1316us) as the cluster accumulated
# state, so a baseline measured at the start of an arm can legitimately sit
# below the same arm's later steady state.
BASELINE_SECS="${BASELINE_SECS:-60}"
DURING_SECS="${DURING_SECS:-30}"
# When to fire the action inside the "during" window. Far enough in that the
# window has a stable stretch before it, far enough from the end that the
# disturbance is actually inside the measured period.
INJECT_AT_SECS="${INJECT_AT_SECS:-10}"
RECOVERY_WINDOW_SECS="${RECOVERY_WINDOW_SECS:-10}"
RECOVERY_MAX_WINDOWS="${RECOVERY_MAX_WINDOWS:-18}"
# Recovered means p99 within this factor of the baseline p99...
#
# 1.35, not 1.20. Even with a longer baseline the drift above accounts for
# ~25%, and 1.20 produced false negatives on arms that had plainly recovered -
# a false "did not recover" is worse than a slightly loose band, because it
# makes the feature under test look broken when the harness is at fault.
RECOVERY_BAND="${RECOVERY_BAND:-1.35}"
# ...and holding for this many consecutive windows. Without the second
# condition a single lucky window reads as recovered, which would understate
# time-to-baseline exactly when it matters most.
RECOVERY_SUSTAIN="${RECOVERY_SUSTAIN:-2}"

# Client-side leader-rediscovery tuning. Defaults are TUNED here, not at
# franz-go's defaults, and that is deliberate for this series: at the defaults
# the produce tail during a leadership move is 4.7s while the broker's own
# handling stays at 0.18ms, so an untuned client measures its own metadata
# rate-limit rather than anything about the cluster or the transform.
#
# Set METADATA_MIN_AGE=5s RETRY_BACKOFF_MAX=5s to reproduce the untuned
# behaviour for comparison. Both values are recorded in every report's config
# fingerprint, so a tuned run can never be mistaken for an untuned one.
METADATA_MIN_AGE="${METADATA_MIN_AGE:-100ms}"
RETRY_BACKOFF_MAX="${RETRY_BACKOFF_MAX:-500ms}"

# Per-interval slices of each window. An aggregate cannot say WHEN a stall
# began or how long it lasted, and those are the facts that identify the
# mechanism: a stall starting one drain-budget after the injection is a second
# drain, one starting at the injection is the transfer itself, and both produce
# the same window aggregate. 500ms resolves a sub-second stall without making
# the report unreadable.
TIMELINE_MS="${TIMELINE_MS:-500}"

# One pacing interval in microseconds, from the offered rate. render-tables
# rejects a level whose send_lateness p99 exceeds this; the series warns.
PACING_INTERVAL_US=$(awk -v r="$RATE" 'BEGIN{printf "%.0f", (r>0 ? 1000000/r : 0)}')

DRAIN_AB="${DRAIN_AB:-1}"
# 200ms. This was 5000ms, then 1000ms on a "keep 5x headroom over the measured
# requirement" argument, and that argument was WRONG - headroom here is not
# free, it is paid 1:1 in e2e latency on every maintenance drain.
#
# Measured on AWS (opt, 3 brokers, 1000 orders/s), same cluster, same action:
#   budget    duplicates    e2e p99 during
#   none      30            1,536us
#   200ms     0             114,464us
#   1000ms    0             875,620us
# Both budgets eliminate duplicates completely; the cost scales with the
# budget. The mechanism is that a drain stops the CONSUMER immediately and the
# transfer then waits out the budget, so records arriving in that window are
# not transformed until the new owner starts. Under maintenance the drain
# reliably runs to its full budget, because maintenance moves the OUTPUT
# topic's leadership too and the transform's commits cannot land until it
# settles - so the full budget is the cost, not a worst case.
DRAIN_TIMEOUT_MS="${DRAIN_TIMEOUT_MS:-200}"

mkdir -p "$OUT_DIR"
SUMMARY="$OUT_DIR/summary.txt"
: > "$SUMMARY"
say() { echo "$@" | tee -a "$SUMMARY"; }

# --- one measurement window ------------------------------------------------
# Prints "p50 p90 p99 missing duplicates clean samples" or "SKIP" if the window
# produced no report at all. An unclean window is not a slow result, it is no
# result - and `samples` is reported because a window with ZERO samples has a
# p99 of 0, which trivially satisfies any "is it back under threshold" test.
window() {
  local label="$1" secs="$2"
  local out="$OUT_DIR/$label.json"
  # Receipts come from an ordinary Kafka fetch of the fills topic, NOT from an
  # in-broker relay consumer. That is a deliberate reversal of what the
  # steady-state runners do, and the reason is the experiment itself: an
  # unpinned relay consumer follows its own input partition's leadership, and
  # the relay has no cross-node hop, so the moment a leadership move separates
  # the consumer from the matcher writing to fills, receipts stop entirely.
  # Measured 2026-09-09: the arm survived one maintenance drain and then
  # delivered NOTHING, with loadgen reporting "relay drops, or a shard-locality
  # mismatch". Measuring through the relay means the harness breaks exactly
  # when the disturbance under test begins.
  #
  # A fills fetch is leadership-agnostic - a consumer simply re-fetches from
  # the new leader - and still measures order-in to fill-visible, which is the
  # transform's dark time. It costs absolute comparability with the published
  # in-broker figures, since it includes an external fetch, but this series
  # measures a DELTA against a baseline taken on the same cluster minutes
  # earlier, so the absolute offset cancels.
  "$LOADGEN" \
    -brokers "$BROKERS" -admin-url "$ADMIN_URL" \
    -input-topic "$ORDERS_TOPIC" \
    -receipt-source fills -fills-topic "$FILLS_TOPIC" \
    -transform-name "$MATCHER_NAME" \
    -num-probes 1 -rate "$RATE" -payload-bytes "$PAYLOAD" -acks "$ACKS" \
    -linger-ms "$LINGER_MS" \
    -metadata-min-age "$METADATA_MIN_AGE" \
    -retry-backoff-max "$RETRY_BACKOFF_MAX" \
    -pacing fixed -duration "${secs}s" -warmup 5s -drain 10s \
    -timeline-ms "$TIMELINE_MS" \
    -label "$label" -out "$out" > "$OUT_DIR/$label.log" 2>&1
  if [ ! -s "$out" ]; then
    echo "SKIP"
    return
  fi
  python3 - "$out" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
t = d.get("total", {})
# Same benign exception scripts/render-tables.py already applies: a missing
# -admin-url means latency without resource attribution, which is a gap in what
# was collected rather than a reason to distrust the latency. Every other
# unclean reason is disqualifying. Without this the series would hard-skip on a
# report the rest of the repo considers usable, and produce nothing.
reasons = d.get("unclean_reasons") or []
clean = d.get("clean") or (bool(reasons) and all("no -admin-url" in r for r in reasons))
# The same per-stage quantiles the published tables carry, plus the ones a
# DISTURBANCE needs that a steady-state table does not:
#   max      - a leadership move's cost can leave p99 entirely and survive only
#              in max; that already happened once in this doc's own figures, so
#              reporting p99 alone can show a disturbance as free when it isn't
#   over_*   - how MANY records were affected, not just how bad for the worst
#              few. One 900ms record and three hundred of them give nearly the
#              same p99 and mean completely different things.
#   lateness - render-tables rejects a level whose send_lateness p99 exceeds a
#              pacing interval, because a starved generator is indistinguishable
#              from system latency. A disturbance is exactly when the client is
#              most likely to fall behind, so the same gate has to apply here.
pr = d.get("produce", {})
lat = d.get("send_lateness", {})
over = t.get("over_micros", {}) or {}
print("%.0f %.0f %.0f %d %d %s %d %.0f %.0f %.0f %.0f %.0f %d %d %.0f" % (
    t.get("p50_micros", 0), t.get("p90_micros", 0), t.get("p99_micros", 0),
    d.get("missing_receipts", 0), d.get("duplicate_receipts_sampled", 0),
    "yes" if clean else "no", t.get("samples", 0),
    t.get("p999_micros", 0), t.get("max_micros", 0), t.get("mean_micros", 0),
    pr.get("p50_micros", 0), pr.get("p99_micros", 0),
    int(over.get("10000", 0)), int(over.get("100000", 0)),
    lat.get("p99_micros", 0)))
PY
}

# --- build the in-broker wasm arm -----------------------------------------
# The series needs a live pipeline before it can disturb one:
#   orders --[matcher]--> fills --[relay consumer]--> probe-out
# loadgen produces to orders and reads receipts from probe-out, so a receipt
# proves the whole in-broker path ran. Nothing else in the harness leaves such
# a pipeline behind - the other runners create and then delete their own - so
# this has to build it rather than assume it.
setup_arm() {
  say "--- building the in-broker arm (RF=$RF write.caching=$WRITE_CACHING) ---"
  rpkc topic create "$ORDERS_TOPIC" "$FILLS_TOPIC" \
    -p 1 -r "$RF" >/dev/null || { say "topic create failed"; return 1; }
  local t
  for t in "$ORDERS_TOPIC" "$FILLS_TOPIC"; do
    rpkc topic alter-config "$t" --set "write.caching=$WRITE_CACHING" >/dev/null 2>&1 || true
    wait_for_stable_placement "$t" 0 >/dev/null 2>&1 || true
  done

  # No co-location step. It would be pointless here: the disturbance under test
  # moves leadership, so any co-location arranged at setup is destroyed by the
  # first action - which is precisely how the relay-probe version of this
  # script broke.
  [ -f "$BIN/wasm-matcher.wasm" ] || { say "no $BIN/wasm-matcher.wasm"; return 1; }

  rpkc transform deploy --name "$MATCHER_NAME" \
    --input-topic "$ORDERS_TOPIC" --output-topic "$FILLS_TOPIC" \
    --var RISK_LIMIT=0 --file "$BIN/wasm-matcher.wasm" >/dev/null \
    || { say "matcher deploy failed"; return 1; }
  wait_for_transforms_running "$MATCHER_NAME" || return 1

  # One transform only. A second (relay) transform would add its own placement
  # as a failure mode of the measurement rather than of the thing measured.

  # Pre-check, same discipline as the other runners: if ten orders produce no
  # receipts the pipeline is not delivering, and every window after this would
  # measure nothing while looking like a result.
  local pre="$OUT_DIR/precheck.json"
  "$LOADGEN" -brokers "$BROKERS" -admin-url "$ADMIN_URL" \
    -input-topic "$ORDERS_TOPIC" \
    -receipt-source fills -fills-topic "$FILLS_TOPIC" \
    -transform-name "$MATCHER_NAME" -num-probes 1 \
    -rate 20 -duration 1s -warmup 0s -drain 15s -sample-every 1 \
    -producers 1 -linger-ms "$LINGER_MS" -acks "$ACKS" \
    -metadata-min-age "$METADATA_MIN_AGE" \
    -retry-backoff-max "$RETRY_BACKOFF_MAX" \
    -label precheck -out "$pre" >/dev/null 2>&1 || true
  local got; got=$(jq -r '.received_receipts // 0' "$pre" 2>/dev/null || echo 0)
  if [ "${got:-0}" -lt 1 ]; then
    say "PRE-CHECK FAILED: 0 receipts from 10 orders - the pipeline is not"
    say "  delivering, so there is nothing to disturb. Check that"
    say "  $MATCHER_NAME reports running and that $FILLS_TOPIC exists."
    return 1
  fi
  say "  pre-check ok: $got receipts"
}

teardown_arm() {
  rpkc transform delete "$MATCHER_NAME" >/dev/null 2>&1 || true
  rpkc topic delete "$ORDERS_TOPIC" "$FILLS_TOPIC" >/dev/null 2>&1 || true
}

# --- the disturbances ------------------------------------------------------
# Each runs in the BACKGROUND so it lands inside the "during" window. Each
# leaves the cluster in the state it found it.
inject() {
  local action="$1" victim="$2"
  case "$action" in
    maintenance)
      say "    [inject] maintenance enable on node $victim"
      maintenance_enable "$victim" >> "$SUMMARY" 2>&1
      # Disabled immediately: the point is the leadership move, and leaving a
      # node muted would change what the recovery windows are measuring.
      maintenance_disable "$victim" >> "$SUMMARY" 2>&1
      ;;
    transfer)
      # Any node but the current leader. Leadership moving for an ordinary
      # reason, not an operator draining a broker.
      local target
      target=$(curl -sf "$ADMIN_URL/v1/brokers" | jq -r --argjson v "$victim" \
                 '[.[] | select(.node_id != $v) | .node_id][0]')
      say "    [inject] transfer $ORDERS_TOPIC/0 leadership $victim -> $target"
      transfer_leadership_to_node "$ORDERS_TOPIC" 0 "$target" >> "$SUMMARY" 2>&1
      ;;
    restart)
      say "    [inject] restart broker $victim  (NOT IMPLEMENTED - needs the"
      say "             ansible/ssh path; maintenance and transfer cover the"
      say "             leadership-move case without stopping a process)"
      return 1
      ;;
    *) say "    [inject] unknown action: $action"; return 1 ;;
  esac
}

# --- one action, one drain setting ----------------------------------------
run_action() {
  # Separate statements on purpose: a compound `local a=$1 b=$2 c="$a-$b"`
  # depends on the shell making earlier names visible mid-statement, which is
  # not portable and failed under set -u with "drain_label: unbound variable".
  local action="$1"
  local drain_label="$2"
  local tag="$action-$drain_label"

  local victim
  victim=$(leader_of "$ORDERS_TOPIC" 0)
  if [ -z "$victim" ]; then
    say "  SKIP $tag: could not determine the leader of $ORDERS_TOPIC/0"
    return
  fi

  say ""
  say "--- $tag (input leader = node $victim) ---"

  local base; base=$(window "$tag-baseline" "$BASELINE_SECS")
  if [ "$base" = "SKIP" ]; then say "  SKIP $tag: baseline produced no report"; return; fi
  read -r b50 b90 b99 bmiss bdup bclean bsamp b999 bmax bmean bp50 bp99 bo10 bo100 blate <<< "$base"
  say "  baseline   total p50=${b50} p90=${b90} p99=${b99} p999=${b999} max=${bmax} mean=${bmean} (us)"
  say "             produce p50=${bp50} p99=${bp99} | over 10ms=$bo10 over 100ms=$bo100 of $bsamp"
  say "             missing=$bmiss dup=$bdup clean=$bclean send_lateness_p99=${blate}us"
  if [ "$bclean" != "yes" ]; then
    say "  SKIP $tag: baseline was not clean, so there is nothing to compare against"
    return
  fi

  # The action fires part-way through the during-window.
  ( sleep "$INJECT_AT_SECS"; inject "$action" "$victim" ) &
  local injector=$!
  local dur; dur=$(window "$tag-during" "$DURING_SECS")
  wait "$injector" 2>/dev/null
  if [ "$dur" = "SKIP" ]; then say "  $tag: during-window produced no report"; return; fi
  read -r d50 d90 d99 dmiss ddup dclean dsamp d999 dmax dmean dp50 dp99 do10 do100 dlate <<< "$dur"
  say "  during     total p50=${d50} p90=${d90} p99=${d99} p999=${d999} max=${dmax} mean=${dmean} (us)"
  # produce vs total is the decomposition that says WHERE the cost landed.
  # Reporting total alone once made a drain look like a produce regression when
  # produce was in fact 689us inside an 875ms total - the whole cost was the
  # transform not consuming, which is a different problem with a different fix.
  say "             produce p50=${dp50} p99=${dp99} | over 10ms=$do10 over 100ms=$do100 of $dsamp"
  say "             missing=$dmiss dup=$ddup clean=$dclean send_lateness_p99=${dlate}us"
  # A generator that fell behind reports its own queueing as system latency.
  # render-tables rejects a level for this; here it is a warning, because the
  # disturbance is the thing under test and discarding the window would discard
  # the measurement.
  if [ "${dlate%.*}" -gt "$PACING_INTERVAL_US" ] 2>/dev/null; then
    say "             !! send_lateness p99 ${dlate}us exceeds the ${PACING_INTERVAL_US}us pacing interval -"
    say "                the generator was late, so part of this latency is client-side queueing"
  fi
  if [ "$dmiss" -gt 0 ]; then
    say "  *** $dmiss MISSING receipts during $tag - that is data loss, not a slowdown ***"
  fi

  # Recovery: short windows until p99 is inside the band and stays.
  local threshold
  threshold=$(awk -v b="$b99" -v f="$RECOVERY_BAND" 'BEGIN{printf "%.0f", b*f}')
  say "  recovery   target: p99 <= ${threshold}us (${RECOVERY_BAND}x baseline) for $RECOVERY_SUSTAIN consecutive ${RECOVERY_WINDOW_SECS}s windows"
  local consec=0 elapsed=0 i r50 r90 r99 rmiss rdup rclean recovered=no
  for i in $(seq 1 "$RECOVERY_MAX_WINDOWS"); do
    local w; w=$(window "$tag-recovery-$i" "$RECOVERY_WINDOW_SECS")
    [ "$w" = "SKIP" ] && { say "    window $i: no report"; continue; }
    read -r r50 r90 r99 rmiss rdup rclean rsamp r999 rmax rmean rp50 rp99 ro10 ro100 rlate <<< "$w"
    elapsed=$((elapsed + RECOVERY_WINDOW_SECS))
    # A window only counts as in-band if it actually MEASURED something and is
    # clean. Without both conditions a window that delivered nothing reports
    # p99=0, satisfies "p99 <= threshold", and the series declares recovery
    # while the pipeline is dead - which is exactly what happened on
    # 2026-09-09 before this guard existed.
    if [ "${rsamp:-0}" -gt 0 ] && [ "$rclean" = "yes" ] \
       && awk -v a="$r99" -v t="$threshold" 'BEGIN{exit !(a<=t)}'; then
      consec=$((consec + 1))
    else
      consec=0
    fi
    # max and over10ms alongside p99: a window can sit inside the band on p99
    # while still carrying multi-hundred-millisecond outliers, which is not
    # recovered in any sense an operator cares about.
    say "    window $i (+${elapsed}s): p99=${r99}us max=${rmax}us over10ms=$ro10 samples=$rsamp dup=$rdup clean=$rclean  in-band-streak=$consec"
    if [ "$consec" -ge "$RECOVERY_SUSTAIN" ]; then recovered=yes; break; fi
  done
  if [ "$recovered" = yes ]; then
    say "  RECOVERED after ~${elapsed}s (p99 back within ${RECOVERY_BAND}x baseline and held)"
  else
    say "  DID NOT RECOVER within $((RECOVERY_MAX_WINDOWS * RECOVERY_WINDOW_SECS))s - reporting no recovery time rather than a floor"
  fi

  say "  RESULT $tag: total p99 ${b99}->${d99} max ${bmax}->${dmax} | produce p99 ${bp99}->${dp99} | over10ms $bo10->$do10 | duplicates=$ddup missing=$dmiss | recovery=${recovered}:${elapsed}s"
}

# --- main -----------------------------------------------------------------
say "resilience series  run_id=$RUN_ID"
say "actions: $ACTIONS   rate=${RATE}/s payload=${PAYLOAD}B acks=$ACKS"
say "drain A/B: $([ "$DRAIN_AB" = 1 ] && echo "yes (unset vs ${DRAIN_TIMEOUT_MS}ms)" || echo no)"
say "client: metadata-min-age=$METADATA_MIN_AGE retry-backoff-max=$RETRY_BACKOFF_MAX"
[ -x "$LOADGEN" ] || { say "no loadgen at $LOADGEN"; exit 1; }

# Whatever happens, do not leave a node muted or the property set.
cleanup() {
  say ""
  say "restoring cluster state"
  maintenance_clear_all
  cfg_set data_transforms_graceful_transfer_timeout_ms null >/dev/null 2>&1 || true
  teardown_arm
}
trap cleanup EXIT INT TERM

setup_arm || { say "could not build the in-broker arm - nothing measured"; exit 1; }

for action in $ACTIONS; do
  if [ "$DRAIN_AB" = 1 ]; then
    cfg_set data_transforms_graceful_transfer_timeout_ms null >/dev/null 2>&1
    run_action "$action" "nodrain"
    cfg_set data_transforms_graceful_transfer_timeout_ms "$DRAIN_TIMEOUT_MS" >/dev/null 2>&1
    run_action "$action" "drain${DRAIN_TIMEOUT_MS}ms"
  elif [ -n "${DRAIN_TIMEOUT_SET:-}" ]; then
    # Single arm at one explicit budget, for sweeping the timeout. The drain
    # stalls produces for as long as it runs (it is invoked after rm_stm's
    # write lock is taken), so the budget is an upper bound on the produce
    # stall - the sweep is looking for the smallest budget that still gets
    # duplicates to zero.
    cfg_set data_transforms_graceful_transfer_timeout_ms "$DRAIN_TIMEOUT_SET" >/dev/null 2>&1
    run_action "$action" "drain${DRAIN_TIMEOUT_SET}ms"
  else
    run_action "$action" "asconfigured"
  fi
done

say ""
say "results in $OUT_DIR"
