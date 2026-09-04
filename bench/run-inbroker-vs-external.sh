#!/usr/bin/env bash
# In-broker wasm transform vs. the SAME wasm module hosted by an ordinary
# external Kafka client, with the external matcher on its OWN instance.
#
# WHY THIS EXISTS, given run-cloud-fanout-comparison.sh already compares the
# two legs: that script starts cmd/wasm-client with `&`, i.e. on the same host
# as cmd/bench and the fanout-load consumer groups. The external leg therefore
# carried CPU on the measuring host that the in-broker leg did not, and the
# external matcher never actually crossed the network from a machine of its
# own. Josh's requirement (2026-09-02) is that the external clients be
# "in their own separate instances and communicate over the network".
#
# client0 has no private key, so it cannot start anything on client1. This
# script therefore runs on the OPERATOR host and drives both clients over ssh,
# which also makes the ordering explicit instead of racing the other script's
# blanket sweep against a pre-started matcher.
#
# WHAT IS HELD IDENTICAL between the legs:
#   - the same wasm-matcher.wasm binary, byte for byte
#   - acks: in-broker transform writes vs kgo.AllISRAcks() - both all-ISR
#   - producer linger 0 on every producer (METHODOLOGY 5)
#   - cmd/bench on client0 as the only measuring process, concurrency 1
#   - fanout-load consumer groups on client0, same count, in both legs
#   - RF=3, 1 partition, write.caching=true on both topics
#   - phase structure: baseline (no fanout) then FANOUT_N-way fanout
#
# THE ONE VARIABLE: where the matcher executes - inside the broker, or on
# client1 reaching the brokers over the network.
#
# NEITHER LEG USES THE RELAY. The in-broker leg is a plain transform writing to
# the fills topic, consumed over Kafka like any topic; there are no relay
# subscriptions and no in-broker probe consumers. That is deliberate - a relay
# leg would confound "in-broker execution" with "in-broker delivery".
set -euo pipefail

KEY="${KEY:-$HOME/.ssh/jlp-aws-iceberg.pem}"
SSH_OPTS=(-o IdentitiesOnly=yes -o StrictHostKeyChecking=no
          -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15
          -o ServerAliveInterval=30 -o ServerAliveCountMax=1000)
BENCH_HOST="${BENCH_HOST:?public IP of the client running bench + fanout-load}"
MATCHER_HOST="${MATCHER_HOST:?public IP of the client running the external matcher}"
BROKERS="${BROKERS:?comma-separated private_ip:9092}"
ADMIN_URL="${ADMIN_URL:?http://<seed_private_ip>:9644}"
BIN="${BIN:-/opt/wasm-bench}"
# Defaults are the EXACT config of the 824us in-broker run (1788399100), read
# out of its own report's config block, because comparability with that number
# is the entire point of this script. Change one and the comparison is void.
RATE="${RATE:-1000}"              # orders/sec; each order is a crossing pair = 2 records
DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-10s}"
DRAIN="${DRAIN:-30s}"
PAYLOAD="${PAYLOAD:-650}"
SAMPLE_EVERY="${SAMPLE_EVERY:-10}"
ACKS="${ACKS:-all}"
LINGER_MS="${LINGER_MS:-0}"
# Total consumers of the fills topic, INCLUDING loadgen itself as the measured
# one - so FANOUT_N-1 background groups. 500 matches the 824us run's consumer
# count; there they were in-broker relay consumers, here they are all ordinary
# external Kafka consumer groups, which is the comparison.
FANOUT_N="${FANOUT_N:-500}"
LEG_ORDER="${LEG_ORDER:-wasm,external}"
RUN_ID="${RUN_ID:-$(date +%s)}"
OUT="${OUT:-$BIN/results/inbroker-vs-external-$RUN_ID}"

b() { ssh -i "$KEY" "${SSH_OPTS[@]}" "ubuntu@$BENCH_HOST" "$@"; }
m() { ssh -i "$KEY" "${SSH_OPTS[@]}" "ubuntu@$MATCHER_HOST" "$@"; }
rpk() { b "cd $BIN && ./rpk -X brokers=$BROKERS $*"; }

sweep() {
  echo "--- sweep: transforms, groups, topics (dedicated cluster) ---"
  # Blanket, not RUN_ID-scoped, for the reason recorded in
  # run-cloud-fanout-comparison.sh: 4000+ leftover fanout-load groups once
  # turned a broker restart into a 10+ minute group-manager replay.
  b "cd $BIN && ./rpk -X brokers=$BROKERS transform list 2>/dev/null | awk 'NR>1{print \$1}' | xargs -r -n 20 ./rpk -X brokers=$BROKERS transform delete --no-confirm" >/dev/null 2>&1 || true
  b "cd $BIN && ./rpk -X brokers=$BROKERS group list 2>/dev/null | awk 'NR>1{print \$2}' | xargs -r -n 200 ./rpk -X brokers=$BROKERS group delete" >/dev/null 2>&1 || true
  b "cd $BIN && ./rpk -X brokers=$BROKERS topic list 2>/dev/null | awk 'NR>1 && \$1 !~ /^_/{print \$1}' | xargs -r -n 50 ./rpk -X brokers=$BROKERS topic delete" >/dev/null 2>&1 || true
}

# [w]asm-client, not wasm-client: the pattern is sent over ssh, so the remote
# shell's own command line contains the literal string and a plain `pkill -f
# wasm-client` can match and kill that wrapper instead of the matcher - while
# still exiting 0, so it looks like it worked. The bracket makes the pattern
# not match itself. Verified the same way: a `pgrep -f wasm-client` over ssh
# reported a process when none was running.
stop_matcher() { m "pkill -f '[w]asm-client'" >/dev/null 2>&1 || true; }

# Signal traps, not just EXIT: a killed shell skips an EXIT trap entirely and
# would strand FANOUT_N consumer groups plus a running remote matcher.
_DONE=0
finish() { [ "$_DONE" = 1 ] && return 0; _DONE=1; stop_matcher; sweep; }
trap finish EXIT
trap 'echo "SIGTERM - cleaning up" >&2; finish; exit 143' TERM
trap 'echo "SIGINT - cleaning up"  >&2; finish; exit 130' INT
trap 'finish; exit 129' HUP

b "mkdir -p $OUT"
# Measured host clock offset, so loadgen can tell a genuine event ordering from
# clock error rather than guessing (see judgeNegativeMatch).
CLOCK_RMS="$(b "chronyc tracking 2>/dev/null | awk -F': *' '/RMS offset/{print \$2}' | awk '{printf \"%.1f\", \$1*1e6}'" 2>/dev/null || echo 0)"
[ -z "$CLOCK_RMS" ] && CLOCK_RMS=0
echo "clock offset on bench host: ${CLOCK_RMS}us"
sweep

run_leg() {
  local kind="$1"
  local orders="orders-$kind-$RUN_ID" fills="fills-$kind-$RUN_ID"
  local admin_args="" name="wasm-matcher-cmp-$RUN_ID"

  echo ""
  echo "############ leg: $kind ############"
  rpk "topic create $orders $fills -p 1 -r 3" >/dev/null
  rpk "topic alter-config $orders --set write.caching=true" >/dev/null
  rpk "topic alter-config $fills  --set write.caching=true" >/dev/null

  if [ "$kind" = "wasm" ]; then
    rpk "transform deploy --name $name --input-topic $orders --output-topic $fills --var RISK_LIMIT=0 --file $BIN/wasm-matcher.wasm" >/dev/null
    local ok=false
    for _ in $(seq 1 30); do
      rpk "transform list" 2>/dev/null | grep -q "$name" && { ok=true; break; }
      sleep 1
    done
    [ "$ok" = true ] || { echo "ERROR: transform $name never appeared" >&2; return 1; }
    # METHODOLOGY: always pass these for the wasm leg so a VM restart during the
    # window marks the report unclean instead of silently inflating the tail.
    admin_args="-admin-url $ADMIN_URL -transform-name $name"
    echo "    in-broker transform active"
  else
    # Separate instance, over the network. nohup+setsid so it survives the ssh
    # session closing.
    # RISK_LIMIT=0 set explicitly to match the transform's --var RISK_LIMIT=0.
    # The module defaults to 0 when unset (rust/src/main.rs unwrap_or(0)), so
    # this changes nothing today - but it makes the legs identical by
    # construction rather than by two defaults happening to agree, which is the
    # kind of coincidence that breaks silently when a default moves.
    # wasm-client passes it through via wasiConfig.InheritEnv().
    m "cd $BIN && KAFKA_BROKERS=$BROKERS INPUT_TOPIC=$orders OUTPUT_TOPIC=$fills \
        CONSUMER_GROUP=wasm-client-external-$RUN_ID WASM_FILE=$BIN/wasm-matcher.wasm \
        RISK_LIMIT=0 PRODUCER_LINGER_MS=0 nohup setsid ./wasm-client > /tmp/wasm-client-$RUN_ID.log 2>&1 & echo started"
    # Assert attachment. A dead remote matcher produces an all-zero leg that
    # looks like a latency result rather than a failure.
    local joined=false
    for _ in $(seq 1 30); do
      if rpk "group describe wasm-client-external-$RUN_ID" 2>/dev/null | grep -q "$orders"; then
        joined=true; break
      fi
      sleep 2
    done
    if [ "$joined" != true ]; then
      echo "ERROR: external matcher on $MATCHER_HOST never joined its group on $orders." >&2
      m "tail -20 /tmp/wasm-client-$RUN_ID.log" >&2 2>/dev/null || true
      return 1
    fi
    echo "    external matcher attached from $MATCHER_HOST (separate instance, over the network)"
  fi

  # loadgen, not cmd/bench. bench is concurrency-1 by design (one order in
  # flight, so the number is a true zero-contention per-record latency), which
  # is a different regime from the rate-paced 824us run and not comparable to
  # it. loadgen with -receipt-source fills is the SAME tool and the SAME
  # measurement definition as that run, just consuming the matcher's output
  # topic as an ordinary external Kafka consumer instead of a probe topic.
  local lg="./loadgen -brokers $BROKERS -input-topic $orders \
      -receipt-source fills -fills-topic $fills -num-probes 1 \
      -rate $RATE -duration $DURATION -warmup $WARMUP -drain $DRAIN \
      -pacing fixed -payload-bytes $PAYLOAD -sample-every $SAMPLE_EVERY \
      -linger-ms $LINGER_MS -acks $ACKS -producers 1 \
      -clock-rms-micros $CLOCK_RMS"

  echo "--- $kind: baseline, no background fanout ---"
  b "cd $BIN && $lg $admin_args -label $kind-baseline -out $OUT/$kind-baseline.json" || true

  local load=$(( FANOUT_N - 1 ))
  if [ "$load" -ge 1 ]; then
    echo "--- $kind: starting $load background external consumer groups on $fills (+loadgen = $FANOUT_N) ---"
    b "cd $BIN && nohup setsid ./fanout-load -brokers $BROKERS -topic $fills -n $load \
        -group-prefix fanout-$kind-$RUN_ID > $OUT/$kind.fanoutload.log 2>&1 & echo started" >/dev/null
    sleep 15
  fi

  echo "--- $kind: with $FANOUT_N-way external fanout ---"
  b "cd $BIN && $lg $admin_args -label $kind-fanout -out $OUT/$kind-fanout.json" || true

  b "pkill -f '[f]anout-load'" >/dev/null 2>&1 || true
  if [ "$kind" = "wasm" ]; then
    rpk "transform delete $name --no-confirm" >/dev/null 2>&1 || true
  else
    stop_matcher
  fi
  # Let the killed consumers actually leave their groups before the next leg,
  # or leg 2 inherits leg 1's reader-cache pressure - the isolation gap that
  # made this asymmetry ambiguous the first time it was measured.
  sleep 10
  sweep
}

IFS=',' read -r FIRST SECOND <<< "$LEG_ORDER"
run_leg "$FIRST"
# `[ -n "$SECOND" ] && run_leg ...` would make the whole && list fail when
# SECOND is empty, and under set -e that exits the script before the results
# summary below - so a single-leg run would silently print nothing.
if [ -n "${SECOND:-}" ]; then
  run_leg "$SECOND"
fi

echo ""
echo "############ results ############"
b "cd $OUT && for f in *.json; do
     printf '%-22s ' \"\${f%.json}\"
     jq -r '\"clean=\(.clean)  recv=\(.received_receipts)/\(.expected_receipts)  miss=\(.missing_receipts)  e2e_p50=\(.total.p50_micros|floor)us  p90=\(.total.p90_micros|floor)us  p99=\(.total.p99_micros|floor)us  produce_p50=\(.produce.p50_micros|floor)us  lag_grew=\(.lag.grew_during_run)  client_bound=\(.client_bound)\"' \"\$f\" 2>/dev/null || echo '(unparseable)'
   done"
echo ""
echo "NOTE: total here is loadgen's own clock at BOTH ends (send -> fill observed"
echo "      over the network), so it is single-clock and directly quotable. The"
echo "      824us reference is the same tool but stamped by an IN-BROKER probe at"
echo "      delivery, so it stops before the fetch out to a client. Compare the"
echo "      two new legs to each other for execution location; compare either to"
echo "      824us for the value of in-broker DELIVERY."
echo ""
echo "results dir on $BENCH_HOST: $OUT"
