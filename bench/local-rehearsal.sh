#!/usr/bin/env bash
# Local durability rehearsal on ONE broker built from your branch: no cloud,
# ~3 minutes. Deploys book-matcher with snapshots, runs insert+cancel traffic
# over a 1,000-deep book, places sentinel waves, SIGKILLs the broker at T,
# restarts it, and checks the book came back.
#
# Passes when the restarted guest logged `STATE restored` and every sentinel
# placed >= 6s before the kill survived (older than one 5s checkpoint
# interval). Later waves may be lost or kept depending on how the snapshot and
# the committed offset fall - that window is what the cluster series measures,
# not what this checks.
#
# This is the test that found the shared_engine forwarding bug (snapshots were
# never taken in a real broker while unit tests passed). Run it after any
# broker change touching transforms, wasm or state.
#
#   REDPANDA_BIN=... WASM_DIR=... bench/local-rehearsal.sh
#
# Ports: KAFKA_PORT (19093), ADMIN_PORT (9645), RPC_PORT (33146) - must be free.
set -uo pipefail
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REDPANDA_SRC="${REDPANDA_SRC:-$HOME/redpanda/redpanda}"
REDPANDA_BIN="${REDPANDA_BIN:-$REDPANDA_SRC/bazel-bin/src/v/redpanda/redpanda}"
WASM_DIR="${WASM_DIR:-${WASM_CLIENTS_SRC:-$HOME/redpanda/projects/redpanda-wasm-clients}/bin}"
KAFKA_PORT="${KAFKA_PORT:-19093}" ADMIN_PORT="${ADMIN_PORT:-9645}" RPC_PORT="${RPC_PORT:-33146}"
RPK_CLI="${RPK_CLI:-$(command -v rpk)}"
[ -x "$REDPANDA_BIN" ] || { echo "rehearsal: no broker binary at $REDPANDA_BIN" >&2; exit 1; }
[ -f "$WASM_DIR/book-matcher.wasm" ] || { echo "rehearsal: no $WASM_DIR/book-matcher.wasm (make build-wasm)" >&2; exit 1; }
[ -n "$RPK_CLI" ] || { echo "rehearsal: rpk not on PATH" >&2; exit 1; }
for p in "$KAFKA_PORT" "$ADMIN_PORT" "$RPC_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then echo "rehearsal: port $p is in use" >&2; exit 1; fi
done

W="$(mktemp -d)"
ADMIN="http://127.0.0.1:$ADMIN_PORT"
BROKERS="127.0.0.1:$KAFKA_PORT"
cat > "$W/redpanda.yaml" <<YAML
redpanda:
  data_directory: $W/data
  node_id: 0
  developer_mode: true
  seed_servers: []
  data_transforms_enabled: true
  data_transforms_per_function_memory_limit: 16777216
  data_transforms_max_instances_per_core: 10
  data_transforms_state_snapshot_max_size: 8388608
  rpc_server: {address: 127.0.0.1, port: $RPC_PORT}
  kafka_api: [{address: 127.0.0.1, port: $KAFKA_PORT}]
  admin: [{address: 127.0.0.1, port: $ADMIN_PORT}]
rpk:
  admin_api: {addresses: [127.0.0.1:$ADMIN_PORT]}
  kafka_api: {brokers: [127.0.0.1:$KAFKA_PORT]}
YAML
cat > "$W/start.sh" <<SH
#!/usr/bin/env bash
exec "$REDPANDA_BIN" --redpanda-cfg "$W/redpanda.yaml" --smp 1 --memory 2G --overprovisioned --unsafe-bypass-fsync=true >> "$W/broker.log" 2>&1
SH
chmod +x "$W/start.sh"
# --config, so a local rpk profile (SASL, cloud) cannot leak into the run.
rpkw() { "$RPK_CLI" --config "$W/redpanda.yaml" "$@"; }
start_broker() {
  setsid nohup "$W/start.sh" >/dev/null 2>&1 < /dev/null &
  echo $! > "$W/pid"
  disown
  local i; for i in $(seq 1 60); do curl -sf "$ADMIN/v1/status/ready" >/dev/null && return 0; sleep 1; done
  echo "rehearsal: broker did not become ready (see $W/broker.log)" >&2; return 1
}
cleanup() {
  [ -f "$W/pid" ] && kill -TERM "$(cat "$W/pid")" 2>/dev/null
  sleep 3
  [ -f "$W/pid" ] && kill -KILL "$(cat "$W/pid")" 2>/dev/null
  if [ "${KEEP_WORKDIR:-0}" = 1 ] || [ "${rc:-1}" != 0 ]; then echo "rehearsal: work dir kept: $W"; else rm -rf -- "${W:?}"; fi
}
trap cleanup EXIT
rc=1

echo "--- building loadgen + statecheck"
go -C "$ROOT" build -o "$W/loadgen" ./cmd/loadgen && go -C "$ROOT" build -o "$W/statecheck" ./cmd/statecheck || exit 1
echo "--- starting broker $REDPANDA_BIN"
start_broker || exit 1
rpkw topic create orders fills -p 1 -r 1 >/dev/null || exit 1
bash "$ROOT/scripts/deploy-transform.sh" "$ADMIN" bm orders fills "$WASM_DIR/book-matcher.wasm" --require-state-recovery \
  --env STATE=snapshot --env SEQ_LANES=1 || exit 1
for i in $(seq 1 30); do rpkw transform list 2>/dev/null | awk '$1=="bm"{print $4}' | grep -q '^1$' && break; sleep 1; done

echo "--- traffic: 1,000 orders/sec, depth 1,000, 30% cancels"
"$W/loadgen" -brokers "$BROKERS" -input-topic orders -receipt-source fills -fills-topic fills \
  -rate 1000 -warmup 2s -duration 25s -drain 5s -book-depth 1000 -cancel-fraction 0.3 \
  -out "$W/loadgen.json" > "$W/loadgen.log" 2>&1 &
lg=$!
sleep 8
echo "--- sentinels in waves, SIGKILL at T, restart 5s later"
"$W/statecheck" place -brokers "$BROKERS" -topic orders -per-wave 10 -waves 10,6,3,1,0.3 -lead 11s \
  -exec "kill -9 \$(cat $W/pid); sleep 5; setsid nohup $W/start.sh >/dev/null 2>&1 < /dev/null & echo \$! > $W/pid" \
  -out "$W/placed.json" || exit 1
for i in $(seq 1 60); do curl -sf "$ADMIN/v1/status/ready" >/dev/null && break; sleep 1; done
wait "$lg" || true
for i in $(seq 1 30); do rpkw transform list 2>/dev/null | awk '$1=="bm"{print $4}' | grep -q '^1$' && break; sleep 1; done
sleep 5
"$W/statecheck" verify -brokers "$BROKERS" -placed "$W/placed.json" -fills fills -label rehearsal -out "$W/verdict.json" || exit 1
rpkw transform logs bm 2>/dev/null | grep STATE > "$W/state.log"

restored=$(grep -c "STATE restored" "$W/state.log")
old_ok=$(jq '[.waves[] | select(.wave_seconds_before_t >= 6) | (.recovered == .placed)] | all' "$W/verdict.json")
echo "--- verdict"
jq -r '"placed=\(.placed) recovered=\(.recovered) lost=\(.lost) duplicated=\(.duplicated)", (.waves[] | "  wave T-\(.wave_seconds_before_t)s: \(.recovered)/\(.placed)")' "$W/verdict.json"
grep "STATE restored" "$W/state.log" | head -1
if [ "$restored" -ge 1 ] && [ "$old_ok" = true ]; then
  echo "REHEARSAL PASSED: snapshot restored, every sentinel older than one checkpoint interval survived"
  rc=0
else
  echo "REHEARSAL FAILED: restored_lines=$restored older_waves_survived=$old_ok (logs: $W)" >&2
fi
exit "$rc"
