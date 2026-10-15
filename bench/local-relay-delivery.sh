#!/usr/bin/env bash
# Local check of the broker's RELAY_DELIVERY option on ONE broker built from
# your branch: no cloud, ~6 minutes. Runs the e2 harness (book-matcher, a
# 1,000-deep book, 30% cancels, one relay consumer) at 3,000 records/sec,
# alternating RELAY_DELIVERY=precommit and committed.
#
# Passes when every level is clean with nothing dropped, each report's
# fingerprint carries the mode it was run with, and committed is measurably
# slower on the relay hop than precommit (median relay p50). The last check is
# what proves the option reached the processor: a broker without it ignores
# the variable and both modes measure the same.
#
# One broker, RF=1, one shard: a functional test of the plumbing, NOT a
# measurement. The cost of committed delivery on a real cluster comes from
# plans/relay-modes.plan.
#
#   REDPANDA_BIN=... WASM_DIR=... bench/local-relay-delivery.sh
#   PAIRS=1 bench/local-relay-delivery.sh     (one precommit + one committed)
#
# Ports: KAFKA_PORT (19093), ADMIN_PORT (9645), RPC_PORT (33146) - must be free.
set -uo pipefail
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=../scripts/lib-env.sh
. "$ROOT/scripts/lib-env.sh"
REDPANDA_BIN="${REDPANDA_BIN:-$REDPANDA_SRC/bazel-bin/src/v/redpanda/redpanda}"
WASM_DIR="${WASM_DIR:-$WASM_CLIENTS_SRC/bin}"
KAFKA_PORT="${KAFKA_PORT:-19093}" ADMIN_PORT="${ADMIN_PORT:-9645}" RPC_PORT="${RPC_PORT:-33146}"
RPK_CLI="${RPK_CLI:-$(command -v rpk)}"
PAIRS="${PAIRS:-2}"
[ -x "$REDPANDA_BIN" ] || { echo "relay-delivery: no broker binary at $REDPANDA_BIN" >&2; exit 1; }
for w in book-matcher relay-probe relay-sink; do
  [ -f "$WASM_DIR/$w.wasm" ] || { echo "relay-delivery: no $WASM_DIR/$w.wasm (make build-wasm)" >&2; exit 1; }
done
[ -n "$RPK_CLI" ] || { echo "relay-delivery: rpk not on PATH" >&2; exit 1; }
for p in "$KAFKA_PORT" "$ADMIN_PORT" "$RPC_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then echo "relay-delivery: port $p is in use" >&2; exit 1; fi
done

W="$(mktemp -d)"
B="$W/bin"
mkdir -p "$B"
ADMIN="http://127.0.0.1:$ADMIN_PORT"
cat > "$W/redpanda.yaml" <<YAML
redpanda:
  data_directory: $W/data
  node_id: 0
  developer_mode: true
  seed_servers: []
  data_transforms_enabled: true
  data_transforms_per_function_memory_limit: 16777216
  data_transforms_max_instances_per_core: 10
  rpc_server: {address: 127.0.0.1, port: $RPC_PORT}
  kafka_api: [{address: 127.0.0.1, port: $KAFKA_PORT}]
  admin: [{address: 127.0.0.1, port: $ADMIN_PORT}]
rpk:
  admin_api: {addresses: [127.0.0.1:$ADMIN_PORT]}
  kafka_api: {brokers: [127.0.0.1:$KAFKA_PORT]}
YAML
cleanup() {
  [ -f "$W/pid" ] && kill -TERM "$(cat "$W/pid")" 2>/dev/null
  sleep 3
  [ -f "$W/pid" ] && kill -KILL "$(cat "$W/pid")" 2>/dev/null
  if [ "${KEEP_WORKDIR:-0}" = 1 ] || [ "${rc:-1}" != 0 ]; then echo "relay-delivery: work dir kept: $W"; else rm -rf -- "${W:?}"; fi
}
trap cleanup EXIT
rc=1

echo "--- building loadgen + fanout-load"
go -C "$ROOT" build -o "$B/loadgen" ./cmd/loadgen && go -C "$ROOT" build -o "$B/fanout-load" ./cmd/fanout-load || exit 1
# The e2 harness calls $BIN/rpk. --config, so a local rpk profile (SASL,
# cloud) cannot leak into the run - without it topic creation fails with
# ILLEGAL_SASL_STATE on a machine whose default profile has SASL set.
printf '#!/usr/bin/env bash\nexec %q --config %q "$@"\n' "$RPK_CLI" "$W/redpanda.yaml" > "$B/rpk"
chmod +x "$B/rpk"
ln -s "$ROOT/scripts/deploy-transform.sh" "$B/deploy-transform.sh"
for f in "$WASM_DIR"/*.wasm; do ln -s "$f" "$B/$(basename "$f")"; done

echo "--- starting broker $REDPANDA_BIN"
# One shard: the harness's shard pinning cannot move partitions between
# shards on a developer-mode single node, so producer and consumer share it.
setsid nohup "$REDPANDA_BIN" --redpanda-cfg "$W/redpanda.yaml" --smp 1 --memory 2G --overprovisioned \
  --unsafe-bypass-fsync=true >> "$W/broker.log" 2>&1 < /dev/null &
echo $! > "$W/pid"
for i in $(seq 1 60); do curl -sf "$ADMIN/v1/status/ready" >/dev/null && break; sleep 1; done
curl -sf "$ADMIN/v1/status/ready" >/dev/null || { echo "relay-delivery: broker did not become ready (see $W/broker.log)" >&2; exit 1; }

modes=()
for i in $(seq 1 "$PAIRS"); do modes+=(precommit committed); done
printf '%s\n' "mode relay_p50us relay_p99us total_p50us dropped clean" > "$W/table.txt"
n=0
for mode in "${modes[@]}"; do
  n=$((n + 1))
  log="$W/e2-$n-$mode.log"
  echo "--- run $n/${#modes[@]}: RELAY_DELIVERY=$mode"
  ( cd "$W" && BROKERS="127.0.0.1:$KAFKA_PORT" ADMIN_URL="$ADMIN" BIN="$B" RPK="$B/rpk" RUN_ID="lrd$(date +%s)$n" \
      MODE=rate RATE_LEVELS=3000 FANOUT=1 GUEST=book-matcher STATE=off ORDERS_PER_RECORD=15 \
      CANCEL_FRACTION=0.3 BOOK_DEPTH=1000 PIN_NODE=0 PIN_CORE=0 PROBE_CORE=0 RF=1 \
      WARMUP=5s DURATION=20s DRAIN=10s SAMPLE_EVERY=1 RELAY_STAGE_METRICS=true \
      RELAY_DELIVERY="$mode" bash "$ROOT/bench/run-e2-throughput.sh" ) > "$log" 2>&1 \
    || { echo "relay-delivery: e2 run $n ($mode) failed - tail of $log:" >&2; tail -8 "$log" >&2; exit 1; }
  grep -q "relay_delivery=$mode" "$log" || { echo "relay-delivery: run $n report does not record relay_delivery=$mode" >&2; exit 1; }
  # The data row under the report's column header: level offered achieved
  # cores relay_p50 relay_p99 total_p50 ... dropped clean.
  row=$(grep -A1 '^level ' "$log" | tail -1)
  [ -n "$row" ] || { echo "relay-delivery: run $n produced no report row (see $log)" >&2; exit 1; }
  echo "$row" | awk -v m="$mode" '{print m, $5, $6, $7, $11, $12}' >> "$W/table.txt"
done

echo "--- results (one broker, RF=1: functional, not a measurement)"
column -t "$W/table.txt"
median() { sort -n | awk '{a[NR]=$1} END {print (NR%2 ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2)}'; }
pre=$(awk '$1=="precommit"{print $2}' "$W/table.txt" | median)
com=$(awk '$1=="committed"{print $2}' "$W/table.txt" | median)
unclean=$(awk 'NR>1 && ($5!=0 || $6!="true")' "$W/table.txt" | wc -l)
echo "median relay p50: precommit ${pre}us, committed ${com}us"
if [ "$unclean" -ne 0 ]; then
  echo "RELAY-DELIVERY CHECK FAILED: $unclean run(s) dropped records or were not clean (logs: $W)" >&2
elif awk -v p="$pre" -v c="$com" 'BEGIN{exit !(c > p)}'; then
  echo "RELAY-DELIVERY CHECK PASSED: both modes deliver every record; committed waits for the output write"
  rc=0
else
  echo "RELAY-DELIVERY CHECK FAILED: committed (${com}us) is not slower than precommit (${pre}us) - the option is not reaching the processor (logs: $W)" >&2
fi
exit "$rc"
