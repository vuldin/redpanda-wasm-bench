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
# Live Prometheus metrics from loadgen, off by default. Set by `wb run` when a
# monitoring host exists. This arm is the one where client saturation actually
# bit (METHODOLOGY 29), so it is the arm where watching send_lateness live is
# worth the most.
LOADGEN_METRICS_LISTEN="${LOADGEN_METRICS_LISTEN:-}"
# WHERE the external matcher runs. This is the third arm of the comparison:
# co-location over loopback, which needs no broker change and is the obvious
# cheaper alternative to running the matcher in-broker. The hypothesis it tests
# is that most of the latency is the network hop, and that a local transport
# (user -> kernel -> user roundtrip, expected well under 50us) captures most of
# the win without in-broker execution. It is also the prerequisite for judging
# a Unix-domain-socket listener, which would only remove what loopback leaves.
#
#   external - the matcher on its own instance, reaching brokers over the VPC.
#              This is the published traditional arm.
#   loopback - the matcher ON a broker host, dialling that broker's own address
#              (routed via `lo`, never the NIC - see below). Removes the
#              network hop while keeping every other thing about the external
#              path, including a real TCP stack and a real consumer group.
#
# What this arm settles: if co-located loopback still loses clearly to
# in-broker execution, then a Unix-socket listener cannot close that gap
# either - loopback's 64 KiB MTU already removes most packetisation cost - and
# nobody needs to build one to find out. If it does NOT lose, that is the most
# important negative result available to this project, and it is better to
# find it ourselves than to have it raised at us.
MATCHER_PLACEMENT="${MATCHER_PLACEMENT:-external}"
# Broker public IPs (for ssh) and private IPs (what the co-located matcher
# dials), needed only for MATCHER_PLACEMENT=loopback. The matcher lands on the
# FIRST broker, and must dial THAT broker's own address.
BROKER_PUB="${BROKER_PUB:-}"
BROKER_PRIV_LIST="${BROKER_PRIV_LIST:-}"
# Optional cpuset for the co-located matcher, e.g. "11,12".
#
# Left EMPTY on purpose by default. The textbook setup pins Redpanda to cores
# the engine does not use; the fallback is that an unpinned engine is placed
# by the kernel scheduler. Restricting Redpanda
# to fewer cores for this arm alone would give it a different broker
# configuration from the other two arms, which is a worse confound than
# contention: the whole point is that only the matcher's LOCATION changes.
#
# So by default Redpanda keeps every core and the scheduler arbitrates, and the
# resulting contention is reported as part of what co-location costs, because
# that is what it is. Set this when you specifically want the isolated variant,
# and say so when quoting the number.
MATCHER_CPUS="${MATCHER_CPUS:-}"
RATE="${RATE:-1000}"
DURATION="${DURATION:-30s}"; WARMUP="${WARMUP:-10s}"; DRAIN="${DRAIN:-30s}"
PAYLOAD="${PAYLOAD:-650}"; SAMPLE_EVERY="${SAMPLE_EVERY:-10}"
# Orders per record, so this arm can run the mass series too - a comparison
# where only one arm batches is not a comparison.
ORDERS_PER_RECORD="${ORDERS_PER_RECORD:-1}"
# A latency sample is one ENVELOPE, so the sampled population shrinks by N.
# Left at SAMPLE_EVERY the mass levels would yield tens of samples per window
# instead of thousands, which cannot support the tail figures this round is
# for.
EFFECTIVE_SAMPLE_EVERY="$SAMPLE_EVERY"
# The module this arm's matcher hosts MUST be able to decode what loadgen
# sends. wasm-matcher handles wire type 0 only and rejects an envelope
# outright, so an orders-per-record>1 run against it would drop every record
# and report zero receipts - after the cluster is already billing. Switch to
# the envelope-capable module, which also handles type 0.
MATCHER_MODULE="wasm-matcher.wasm"
if [ "$ORDERS_PER_RECORD" -gt 1 ]; then
  EFFECTIVE_SAMPLE_EVERY=1
  MATCHER_MODULE="mass-matcher.wasm"
fi
# An explicit module wins, e.g. book-matcher.wasm for the production-shaped
# book (it handles types 0, 1 and 2; the host stubs its shared-memory import,
# and it runs with no state, exactly as the in-broker arm's STATE=off).
MATCHER_MODULE="${GUEST_MODULE:-$MATCHER_MODULE}"
# The same order flow as the in-broker runs: a preloaded book and the
# insert+cancel side stream (loadgen -book-depth / -cancel-fraction).
BOOK_DEPTH="${BOOK_DEPTH:-0}"
CANCEL_FRACTION="${CANCEL_FRACTION:-0}"
ACKS="${ACKS:-all}"; LINGER_MS="${LINGER_MS:-0}"
RUN_ID="${RUN_ID:-$(date +%s)}"

read -r -a HOSTS <<< "$CLIENTS"
[ "${#HOSTS[@]}" -ge 3 ] || { echo "need at least 3 clients (loadgen, matcher, >=1 fanout)" >&2; exit 1; }
LG="${HOSTS[0]}"; MATCH="${HOSTS[1]}"
FANOUT_HOSTS=("${HOSTS[@]:2}")

# The matcher's own view of the cluster. Everything else - loadgen, the
# background groups - keeps using $BROKERS over the VPC, so the ONLY thing
# that changes between the two placements is the matcher's own transport.
MATCHER_BROKERS="$BROKERS"
MATCHER_PREFIX=""
case "$MATCHER_PLACEMENT" in
  external) ;;
  loopback)
    read -r -a _bpub <<< "$BROKER_PUB"
    [ "${#_bpub[@]}" -ge 1 ] || { echo "MATCHER_PLACEMENT=loopback needs BROKER_PUB" >&2; exit 1; }
    MATCH="${_bpub[0]}"
    read -r -a _bpriv <<< "$BROKER_PRIV_LIST"
    [ "${#_bpriv[@]}" -ge 1 ] || { echo "MATCHER_PLACEMENT=loopback needs BROKER_PRIV_LIST" >&2; exit 1; }
    # The broker's OWN private address, not 127.0.0.1.
    #
    # An earlier version of this used 127.0.0.1 on the stated reasoning that
    # the private address "still traverses the NIC path". That reasoning was
    # wrong, and the arm failed on every level with "external matcher never
    # joined its group": Redpanda binds kafka_api to the configured private
    # address only (ss shows LISTEN 10.0.0.x:9092, not 0.0.0.0), so
    # 127.0.0.1:9092 is simply refused.
    #
    # Dialing the broker's own address from the broker itself IS the loopback
    # path. Linux installs a `local` route for every address the host owns:
    #   $ ip route get 10.0.0.226
    #   local 10.0.0.226 dev lo src 10.0.0.226
    # so the packets go via `lo` (MTU 65536) and never reach the NIC - which
    # is exactly the 64 KiB-MTU, no-network condition this arm needs. Using
    # the real address also avoids reconfiguring the broker to bind 0.0.0.0,
    # which would give this arm a different broker config from the other two
    # and confound the only variable it is meant to change.
    MATCHER_BROKERS="${_bpriv[0]}:9092"
    # MATCHER_CPUS=auto: the cpus on the matcher's broker whose PHYSICAL core
    # Redpanda's --cpuset does not include (and not core 0, which the deploy
    # reserves for the OS). That is the isolated variant Travis specified -
    # "ensure that Redpanda is pinned to cores different than your engine
    # uses" - without changing the broker's own configuration, since the
    # deploy already leaves cores outside Redpanda's cpuset.
    if [ "$MATCHER_CPUS" = auto ]; then
      MATCHER_CPUS="$(ssh -i "$KEY" "${SSH_OPTS[@]}" "ubuntu@$MATCH" 'bash -s' <<'FREE' 2>/dev/null
set -euo pipefail
flags="$(rpk redpanda config get rpk.additional_start_flags 2>/dev/null || true)"
rp="$(echo "$flags" | grep -o -- '--cpuset=[0-9,-]*' | cut -d= -f2)"
[ -n "$rp" ] || exit 0
expand() { echo "$1" | tr ',' '\n' | while IFS= read -r r; do [ -n "$r" ] || continue; case "$r" in *-*) seq "${r%%-*}" "${r##*-}";; *) echo "$r";; esac; done; }
used=" $(expand "$rp" | tr '\n' ' ') "
free=""
for c in $(expand "$(cat /sys/devices/system/cpu/online)"); do
  tsl=/sys/devices/system/cpu/cpu$c/topology/thread_siblings_list
  [ -r "$tsl" ] || continue
  phys="$(cut -d, -f1 "$tsl" | cut -d- -f1)"
  [ "$phys" = 0 ] && continue
  case "$used" in *" $phys "*) continue ;; esac
  free="$free${free:+,}$c"
done
echo "$free"
FREE
)"
      [ -n "$MATCHER_CPUS" ] || { echo "ERROR: MATCHER_CPUS=auto found no cores outside Redpanda's cpuset on $MATCH" >&2; exit 1; }
    fi
    if [ -n "$MATCHER_CPUS" ]; then
      MATCHER_PREFIX="taskset -c $MATCHER_CPUS "
    fi
    echo "matcher placement: LOOPBACK on broker $MATCH (cpus=${MATCHER_CPUS:-unpinned, scheduler arbitrates})"
    ;;
  *) echo "MATCHER_PLACEMENT must be external or loopback (got $MATCHER_PLACEMENT)" >&2; exit 1 ;;
esac
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
# Admin API, called FROM the loadgen host (the private addresses are not
# reachable from where this script runs).
SEED_ADMIN="http://${BROKERS%%:*}:9644"
admin() { sh_on "$LG" "curl -sfL $*"; }

echo "roles: loadgen=$LG  matcher=$MATCH  fanout=${FANOUT_HOSTS[*]} ($NF hosts)"

# The leader balancer is paused for the measured window and restored on any
# exit. The in-broker arm has always disabled balancing so its pins hold; these
# two arms never did, so a freshly created partition's leadership could move
# mid-window. On 2026-10-15 the loopback fan-out-1 level had 8,203 of 60,000
# samples above ONE SECOND (max 5.0s) while its produce leg stayed clean and
# the next two levels showed nothing similar. A leadership move is one
# candidate the data could not rule out, and it is cheap to rule out next
# time - which is the point of pausing it here.
BALANCER_PREV=""
restore_balancer() {
  [ -n "$BALANCER_PREV" ] || return 0
  rpk "cluster config set enable_leader_balancer $BALANCER_PREV -X admin.hosts=${BROKERS%%:*}:9644" >/dev/null 2>&1 || true
  BALANCER_PREV=""
}
cleanup() {
  restore_balancer
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

BALANCER_PREV=$(rpk "cluster config get enable_leader_balancer -X admin.hosts=${BROKERS%%:*}:9644" 2>/dev/null | tr -d '[:space:]')
[ -n "$BALANCER_PREV" ] || BALANCER_PREV=true
rpk "cluster config set enable_leader_balancer false -X admin.hosts=${BROKERS%%:*}:9644" >/dev/null 2>&1 \
  || echo "WARNING: could not pause the leader balancer; leadership may move mid-window" >&2

# In the co-located arm the matcher only skips the network if the brokers it
# talks to ARE the one it runs on. It bootstraps from its own broker, but then
# follows metadata to each partition's LEADER - so with six brokers and nothing
# pinned, its fetch from $ORDERS and its produce to $FILLS could each still
# cross the network. That made every loopback figure from 2026-10-15 an upper
# bound on co-located latency rather than a measurement of it. Pin both
# leaders onto the matcher's broker, moving replicas there first if that broker
# is not in a replica set (the topics are empty, so the move is near-instant).
pin_to_node() { # pin_to_node <topic> <node>
  local t="$1" node="$2" reps leader
  reps=$(admin "$SEED_ADMIN/v1/partitions/kafka/$t/0" | sh_on "$LG" "jq -r '[.replicas[].node_id]|join(\" \")'" 2>/dev/null)
  if [[ " $reps " != *" $node "* ]]; then
    local keep; keep=$(printf '%s\n' $reps | grep -vx "$node" | head -2 | paste -sd,)
    rpk "cluster partitions move $t -p 0:$node,$keep -X admin.hosts=${BROKERS%%:*}:9644" >/dev/null 2>&1 || return 1
    for _ in $(seq 1 30); do
      reps=$(admin "$SEED_ADMIN/v1/partitions/kafka/$t/0" | sh_on "$LG" "jq -r '[.replicas[].node_id]|join(\" \")'" 2>/dev/null)
      [[ " $reps " == *" $node "* ]] && break; sleep 2
    done
  fi
  admin "-X POST '$SEED_ADMIN/v1/partitions/kafka/$t/0/transfer_leadership?target=$node'" >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    leader=$(admin "$SEED_ADMIN/v1/partitions/kafka/$t/0" | sh_on "$LG" "jq -r .leader_id" 2>/dev/null)
    [ "$leader" = "$node" ] && return 0; sleep 2
  done
  return 1
}
MATCHER_NODE=""
if [ "$MATCHER_PLACEMENT" = loopback ]; then
  MATCHER_NODE=$(admin "http://${MATCHER_BROKERS%%:*}:9644/v1/node_config" | sh_on "$LG" "jq -r .node_id" 2>/dev/null)
  [ -n "$MATCHER_NODE" ] || { echo "ERROR: could not read the matcher broker's node id" >&2; exit 1; }
  for t in "$ORDERS" "$FILLS"; do
    pin_to_node "$t" "$MATCHER_NODE" || { echo "ERROR: could not pin $t/0 leadership to node $MATCHER_NODE - the co-located arm would not be co-located" >&2; exit 1; }
  done
  echo "pinned $ORDERS/0 and $FILLS/0 leadership to node $MATCHER_NODE (the matcher's broker)"
fi

# Record where everything actually was, beside the report, so placement is a
# checkable fact afterwards rather than an assumption.
record_placement() { # record_placement <when>
  local o f
  o=$(admin "$SEED_ADMIN/v1/partitions/kafka/$ORDERS/0" 2>/dev/null)
  f=$(admin "$SEED_ADMIN/v1/partitions/kafka/$FILLS/0" 2>/dev/null)
  sh_on "$LG" "jq -n --arg when '$1' --arg placement '$MATCHER_PLACEMENT' --arg mnode '$MATCHER_NODE' \
    --argjson o '${o:-null}' --argjson f '${f:-null}' \
    --arg mcpus '${MATCHER_CPUS:-}' --arg module '$MATCHER_MODULE' \
    '{when:\$when, matcher_placement:\$placement, matcher_node:\$mnode, matcher_cpus:\$mcpus, module:\$module,
      orders:{leader:\$o.leader_id, replicas:[\$o.replicas[]?.node_id]},
      fills:{leader:\$f.leader_id, replicas:[\$f.replicas[]?.node_id]}}' \
    > $OUT/placement-$FANOUT_N-$1.json 2>/dev/null" || true
}
record_placement before

# The external matcher, on its own instance. </dev/null matters: a backgrounded
# process that inherits the ssh channel's stdin keeps the channel open and the
# ssh call never returns - which hung this exact step once already.
remote_start "$MATCH" "matcher-$RUN_ID" \
  "${MATCHER_PREFIX}env KAFKA_BROKERS=$MATCHER_BROKERS INPUT_TOPIC=$ORDERS OUTPUT_TOPIC=$FILLS \
   CONSUMER_GROUP=wasm-client-trad-$RUN_ID WASM_FILE=$BIN/$MATCHER_MODULE \
   RISK_LIMIT=0 PRODUCER_LINGER_MS=0 ./wasm-client"

joined=false
for _ in $(seq 1 30); do
  if rpk "group describe wasm-client-trad-$RUN_ID" 2>/dev/null | grep -q "$ORDERS"; then joined=true; break; fi
  sleep 2
done
[ "$joined" = true ] || { echo "ERROR: external matcher never joined its group" >&2; sh_on "$MATCH" "tail -20 /tmp/matcher-$RUN_ID.log" >&2 || true; exit 1; }
echo "external matcher attached from $MATCH (placement=$MATCHER_PLACEMENT module=$MATCHER_MODULE brokers=$MATCHER_BROKERS)"

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
  -pacing fixed -payload-bytes $PAYLOAD -sample-every $EFFECTIVE_SAMPLE_EVERY \
  -orders-per-record $ORDERS_PER_RECORD -timeline-ms 500 \
  -book-depth $BOOK_DEPTH -cancel-fraction $CANCEL_FRACTION -guest ${MATCHER_MODULE%.wasm} \
  -linger-ms $LINGER_MS -acks $ACKS -producers 1 -clock-rms-micros $RMS \
  -metrics-listen '$LOADGEN_METRICS_LISTEN' \
  -label trad-$MATCHER_PLACEMENT${MATCHER_CPUS:+-isolated}-$FANOUT_N -out $OUT/trad-$FANOUT_N.json" 2>&1 | grep -E 'wrote report|NOT CLEAN' || true

sh_on "$LG" "cd $OUT && jq -r '\"clean=\(.clean)  recv=\(.received_receipts)/\(.expected_receipts)  miss=\(.missing_receipts)  e2e_p50=\(.total.p50_micros|floor)us  p90=\(.total.p90_micros|floor)us  p99=\(.total.p99_micros|floor)us  lateness_p99=\(.send_lateness.p99_micros|floor)us\"' trad-$FANOUT_N.json"
# Placement AFTER the window too: a leader that moved during the run is the
# thing this is here to catch, and "before" alone cannot show it.
record_placement after
sh_on "$LG" "cd $OUT && jq -r '\"placement: orders leader \(.orders.leader) fills leader \(.fills.leader) matcher node \(.matcher_node // \"-\")\"' placement-$FANOUT_N-after.json" 2>/dev/null || true
echo "SHARDED_DONE $OUT"
