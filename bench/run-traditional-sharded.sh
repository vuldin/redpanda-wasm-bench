#!/usr/bin/env bash
# Traditional deployment (no in-broker wasm, no relay) with the consumer load
# SHARDED across dedicated instances, so the load generator is never the
# bottleneck.
#
# Why this exists: the single-client version starved loadgen. 499 consumer
# groups plus the generator on one c5n.9xlarge drove send_lateness p99 to
# 2,486us against a 1,000us pacing interval, so the reported latency included
# client-side queueing and could not be attributed to the architecture
# (METHODOLOGY 29). Measured budget on this instance type: 49 groups + loadgen
# held pacing at p99 544us; 99 groups + loadgen did not, at p99 1,003us.
#
# Role assignment, from that budget:
#   client0        loadgen ONLY - nothing co-located, so pacing stays clean
#   client1        the external matcher (cmd/wasm-client), its own instance
#   client2..N     background consumer groups, evenly sharded
#
# The measured consumer is loadgen itself, so total consumers of the fills
# topic = 1 + (background groups).
set -euo pipefail

KEY="${KEY:-$HOME/.ssh/jlp-aws-iceberg.pem}"
SSH_OPTS=(-o IdentitiesOnly=yes -o StrictHostKeyChecking=no
          -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15
          -o ServerAliveInterval=30 -o ServerAliveCountMax=1000)
# Space-separated public IPs, in role order: loadgen, matcher, then fanout hosts.
CLIENTS="${CLIENTS:?space-separated client public IPs (>=3): loadgen matcher fanout...}"
BROKERS="${BROKERS:?comma-separated private_ip:9092}"
BIN="${BIN:-/opt/wasm-bench}"
FANOUT_N="${FANOUT_N:-500}"        # total consumers incl. loadgen
RATE="${RATE:-1000}"
DURATION="${DURATION:-30s}"; WARMUP="${WARMUP:-10s}"; DRAIN="${DRAIN:-30s}"
PAYLOAD="${PAYLOAD:-650}"; SAMPLE_EVERY="${SAMPLE_EVERY:-10}"
ACKS="${ACKS:-all}"; LINGER_MS="${LINGER_MS:-0}"
RUN_ID="${RUN_ID:-$(date +%s)}"

read -r -a HOSTS <<< "$CLIENTS"
[ "${#HOSTS[@]}" -ge 3 ] || { echo "need at least 3 clients (loadgen, matcher, >=1 fanout)" >&2; exit 1; }
LG="${HOSTS[0]}"; MATCH="${HOSTS[1]}"
FANOUT_HOSTS=("${HOSTS[@]:2}")
NF="${#FANOUT_HOSTS[@]}"

ORDERS="orders-trad-$RUN_ID"; FILLS="fills-trad-$RUN_ID"
OUT="$BIN/results/traditional-sharded-$RUN_ID"

sh_on() { local h="$1"; shift; ssh -i "$KEY" "${SSH_OPTS[@]}" "ubuntu@$h" "$@"; }

# Start a long-lived process on a remote host WITHOUT depending on ssh
# returning.
#
# `ssh host "cmd &"` reliably hangs here even with the child's stdin, stdout and
# stderr all redirected and setsid applied - the invoking bash -c keeps the
# channel open. It cost three separate stalls in one session, and the third time
# an attempt to unblock it by killing the ssh tripped `set -e` and took the
# whole run down with the EXIT trap sweeping the topics.
#
# So: ship a script, run it, and treat the call as BEST EFFORT under a timeout.
# Whether ssh returns is not evidence either way, so the caller must verify the
# process actually started by observing its effect (a consumer group appearing),
# never by this function's exit status.
remote_start() { # remote_start <host> <tag> <command line...>
  local h="$1" tag="$2"; shift 2
  local rs="/tmp/start-$tag.sh"
  printf '#!/usr/bin/env bash\ncd %s\nsetsid %s > /tmp/%s.log 2>&1 < /dev/null &\nexit 0\n' \
    "$BIN" "$*" "$tag" \
    | sh_on "$h" "cat > $rs && chmod +x $rs" || true
  timeout 25 ssh -i "$KEY" "${SSH_OPTS[@]}" "ubuntu@$h" "bash $rs" >/dev/null 2>&1 || true
}
rpk()   { sh_on "$LG" "cd $BIN && ./rpk -X brokers=$BROKERS $*"; }

echo "roles: loadgen=$LG  matcher=$MATCH  fanout=${FANOUT_HOSTS[*]} ($NF hosts)"

cleanup() {
  for h in "${FANOUT_HOSTS[@]}"; do sh_on "$h" "pkill -f '[f]anout-load'" >/dev/null 2>&1 || true; done
  sh_on "$MATCH" "pkill -f '[w]asm-client'" >/dev/null 2>&1 || true
  sleep 12
  rpk "group list" 2>/dev/null | awk 'NR>1{print $2}' | grep -E "trad-|wasm-client-" \
    | xargs -r -n 200 -I{} sh -c "true" >/dev/null 2>&1 || true
  sh_on "$LG" "cd $BIN && ./rpk -X brokers=$BROKERS group list 2>/dev/null | awk 'NR>1{print \$2}' | xargs -r -n 200 ./rpk -X brokers=$BROKERS group delete" >/dev/null 2>&1 || true
  sh_on "$LG" "cd $BIN && ./rpk -X brokers=$BROKERS topic list 2>/dev/null | awk 'NR>1 && \$1 !~ /^_/{print \$1}' | xargs -r -n 50 ./rpk -X brokers=$BROKERS topic delete" >/dev/null 2>&1 || true
}
_DONE=0
finish() { [ "$_DONE" = 1 ] && return 0; _DONE=1; cleanup; }
trap finish EXIT
trap 'echo "SIGTERM - cleaning up" >&2; finish; exit 143' TERM
trap 'echo "SIGINT - cleaning up"  >&2; finish; exit 130' INT
trap 'finish; exit 129' HUP

sh_on "$LG" "mkdir -p $OUT"
cleanup

rpk "topic create $ORDERS $FILLS -p 1 -r 3" >/dev/null
rpk "topic alter-config $ORDERS --set write.caching=true" >/dev/null
rpk "topic alter-config $FILLS  --set write.caching=true" >/dev/null

# The external matcher, on its own instance. </dev/null matters: a backgrounded
# process that inherits the ssh channel's stdin keeps the channel open and the
# ssh call never returns - which hung this exact step once already.
remote_start "$MATCH" "matcher-$RUN_ID" \
  "env KAFKA_BROKERS=$BROKERS INPUT_TOPIC=$ORDERS OUTPUT_TOPIC=$FILLS \
   CONSUMER_GROUP=wasm-client-trad-$RUN_ID WASM_FILE=$BIN/wasm-matcher.wasm \
   RISK_LIMIT=0 PRODUCER_LINGER_MS=0 ./wasm-client"

joined=false
for _ in $(seq 1 30); do
  if rpk "group describe wasm-client-trad-$RUN_ID" 2>/dev/null | grep -q "$ORDERS"; then joined=true; break; fi
  sleep 2
done
[ "$joined" = true ] || { echo "ERROR: external matcher never joined its group" >&2; sh_on "$MATCH" "tail -20 /tmp/matcher-$RUN_ID.log" >&2 || true; exit 1; }
echo "external matcher attached from $MATCH"

# Shard the background groups. loadgen is one of the FANOUT_N consumers.
load=$(( FANOUT_N - 1 ))
if [ "$load" -ge 1 ]; then
  per=$(( (load + NF - 1) / NF ))
  remaining=$load; i=0
  for h in "${FANOUT_HOSTS[@]}"; do
    [ "$remaining" -le 0 ] && break
    n=$per; [ "$n" -gt "$remaining" ] && n=$remaining
    remote_start "$h" "fanout-$RUN_ID-h$i" \
      "./fanout-load -brokers $BROKERS -topic $FILLS -n $n -group-prefix trad-$RUN_ID-h$i"
    echo "  $h: $n groups"
    remaining=$(( remaining - n )); i=$(( i + 1 ))
  done
  sleep 25
fi
echo -n "consumer groups now: "; rpk "group list" 2>/dev/null | tail -n +2 | wc -l

echo "--- loadgen on $LG (alone), matched to run 1788399100 ---"
RMS="$(sh_on "$LG" "chronyc tracking 2>/dev/null | awk -F': *' '/RMS offset/{print \$2}' | awk '{printf \"%.1f\", \$1*1e6}'" || echo 0)"
[ -z "$RMS" ] && RMS=0
sh_on "$LG" "cd $BIN && ./loadgen -brokers $BROKERS -input-topic $ORDERS \
  -receipt-source fills -fills-topic $FILLS -num-probes 1 \
  -rate $RATE -duration $DURATION -warmup $WARMUP -drain $DRAIN \
  -pacing fixed -payload-bytes $PAYLOAD -sample-every $SAMPLE_EVERY \
  -linger-ms $LINGER_MS -acks $ACKS -producers 1 -clock-rms-micros $RMS \
  -label trad-sharded-$FANOUT_N -out $OUT/trad-$FANOUT_N.json" 2>&1 | grep -E 'wrote report|NOT CLEAN' || true

sh_on "$LG" "cd $OUT && jq -r '\"clean=\(.clean)  recv=\(.received_receipts)/\(.expected_receipts)  miss=\(.missing_receipts)  e2e_p50=\(.total.p50_micros|floor)us  p90=\(.total.p90_micros|floor)us  p99=\(.total.p99_micros|floor)us  lateness_p99=\(.send_lateness.p99_micros|floor)us\"' trad-$FANOUT_N.json"
echo "SHARDED_DONE $OUT"
