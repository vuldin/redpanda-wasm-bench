#!/usr/bin/env bash
# Run a plan's e2 phases, scaled down, on ONE local broker built from your
# branch: the cheapest way to find out that a phase's settings do not work
# together before a cluster is paid for. ~1-2 minutes per distinct e2 phase.
#
#   bench/local-plan-smoke.sh plans/state-and-jitter.plan
#
# Each `run e2` phase runs with the plan's own settings, then these overrides
# so it fits one local node: RATE 3,000 (RATE_LEVELS "3000 6000"), 10s windows,
# RF=1, one producer, fan-out at most 2, books at most 200 deep, everything
# pinned to the one shard. Phases that differ only in their trial number run
# once. `run traditional` and `run durability` phases need cluster hosts and
# are listed as not covered. Passes when every e2 phase exits 0 with at least
# one completed level; prints each level's clean flag and p50.
#
# One broker, RF=1: a functional check of the plan, not a measurement.
# Ports: KAFKA_PORT (19093), ADMIN_PORT (9645), RPC_PORT (33146) - must be free.
set -uo pipefail
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=../scripts/lib-env.sh
. "$ROOT/scripts/lib-env.sh"
PLAN="${1:?usage: local-plan-smoke.sh <plan>}"
[ -f "$PLAN" ] || { echo "plan-smoke: no such plan: $PLAN" >&2; exit 1; }
REDPANDA_BIN="${REDPANDA_BIN:-$REDPANDA_SRC/bazel-bin/src/v/redpanda/redpanda}"
WASM_DIR="${WASM_DIR:-$WASM_CLIENTS_SRC/bin}"
KAFKA_PORT="${KAFKA_PORT:-19093}" ADMIN_PORT="${ADMIN_PORT:-9645}" RPC_PORT="${RPC_PORT:-33146}"
RPK_CLI="${RPK_CLI:-$(command -v rpk)}"
[ -x "$REDPANDA_BIN" ] || { echo "plan-smoke: no broker binary at $REDPANDA_BIN" >&2; exit 1; }
[ -n "$RPK_CLI" ] || { echo "plan-smoke: rpk not on PATH" >&2; exit 1; }
for p in "$KAFKA_PORT" "$ADMIN_PORT" "$RPC_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then echo "plan-smoke: port $p is in use" >&2; exit 1; fi
done

# --- read the plan's phases --------------------------------------------------
NAMES=() ARGS=()
phase() {
  local name="$1"; shift 2
  NAMES+=("$name")
  local a="" x
  for x in "$@"; do a+="$(printf '%q' "$x") "; done
  ARGS+=("$a")
}
RUN_DIR="<run-dir>"
# shellcheck disable=SC1090
. "$PLAN"

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
  data_transforms_state_snapshot_max_size: 8388608
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
  if [ "${KEEP_WORKDIR:-0}" = 1 ] || [ "${rc:-1}" != 0 ]; then echo "plan-smoke: work dir kept: $W"; else rm -rf -- "${W:?}"; fi
}
trap cleanup EXIT
rc=1

echo "--- building loadgen + fanout-load"
go -C "$ROOT" build -o "$B/loadgen" ./cmd/loadgen && go -C "$ROOT" build -o "$B/fanout-load" ./cmd/fanout-load || exit 1
# --config, so a local rpk profile (SASL, cloud) cannot leak into the run.
printf '#!/usr/bin/env bash\nexec %q --config %q "$@"\n' "$RPK_CLI" "$W/redpanda.yaml" > "$B/rpk"
chmod +x "$B/rpk"
ln -s "$ROOT/scripts/deploy-transform.sh" "$B/deploy-transform.sh"
for f in "$WASM_DIR"/*.wasm; do ln -s "$f" "$B/$(basename "$f")"; done

echo "--- starting broker $REDPANDA_BIN"
setsid nohup "$REDPANDA_BIN" --redpanda-cfg "$W/redpanda.yaml" --smp 1 --memory 2G --overprovisioned \
  --unsafe-bypass-fsync=true >> "$W/broker.log" 2>&1 < /dev/null &
echo $! > "$W/pid"
for i in $(seq 1 60); do curl -sf "$ADMIN/v1/status/ready" >/dev/null && break; sleep 1; done
curl -sf "$ADMIN/v1/status/ready" >/dev/null || { echo "plan-smoke: broker did not become ready (see $W/broker.log)" >&2; exit 1; }

# Scale one KEY=VALUE to the local node; anything else passes through.
scale() {
  local kv="$1" k="${1%%=*}" v="${1#*=}" out="" x
  case "$k" in
    RATE) echo "RATE=3000" ;;
    RATE_LEVELS) echo "RATE_LEVELS=3000 6000" ;;
    DURATION) echo "DURATION=10s" ;;
    WARMUP) echo "WARMUP=3s" ;;
    DRAIN) echo "DRAIN=5s" ;;
    RF) echo "RF=1" ;;
    PRODUCERS) echo "PRODUCERS=1" ;;
    SAMPLE_EVERY) echo "SAMPLE_EVERY=1" ;;
    FANOUT) echo "FANOUT=$(( v > 2 ? 2 : v ))" ;;
    FANOUT_LEVELS) echo "FANOUT_LEVELS=1 2" ;;
    BOOK_DEPTH) echo "BOOK_DEPTH=$(( v > 100 ? 100 : v ))" ;;
    BOOKDEPTH_LEVELS) for x in $v; do out+="$(( x > 200 ? 200 : x )) "; done; echo "BOOKDEPTH_LEVELS=${out% }" ;;
    *) echo "$kv" ;;
  esac
}

printf '%s\n' "phase level clean total_p50us" > "$W/table.txt"
seen="" failed=0 ran=0 skipped=()
for i in "${!NAMES[@]}"; do
  eval "set -- ${ARGS[$i]}"
  if [ "${1:-}" != run ] || [ "${2:-}" != e2 ]; then
    skipped+=("${NAMES[$i]} (${1:-} ${2:-})"); continue
  fi
  shift 2
  envs=() key=""
  for kv in "$@"; do
    case "$kv" in *=*) envs+=("$(scale "$kv")") ;; esac
  done
  key="$(printf '%s|' "${envs[@]}")"
  case "$seen" in *"<$key>"*) echo "--- ${NAMES[$i]}: same settings as an earlier phase, skipped"; continue ;; esac
  seen+="<$key>"
  ran=$((ran + 1))
  log="$W/${NAMES[$i]}.log"
  echo "--- ${NAMES[$i]}"
  ( cd "$W" && env "${envs[@]}" BROKERS="127.0.0.1:$KAFKA_PORT" ADMIN_URL="$ADMIN" BIN="$B" RPK="$B/rpk" \
      RUN_ID="lps$(date +%s)$i" PIN_NODE=0 PIN_CORE=0 PROBE_CORE=0 \
      bash "$ROOT/bench/run-e2-throughput.sh" ) > "$log" 2>&1
  e=$?
  # The data rows under the LAST summary header (level offered/s ...).
  # Comment lines inside the table (fanout mode prints its boot reservation
  # there) are skipped, not taken as the end of it.
  rows=$(awk '/^level /{n=0; f=1; next} f && /^#/ {next} f && $1 ~ /^[0-9]/ {r[n++]=$0; next} f {f=0} END{for (i=0; i<n; i++) print r[i]}' "$log")
  if [ "$e" -ne 0 ] || [ -z "$rows" ]; then
    echo "    FAILED (exit $e) - tail of $log:"; tail -6 "$log" | sed 's/^/      /'; failed=$((failed + 1)); continue
  fi
  echo "$rows" | awk -v p="${NAMES[$i]}" '{print p, $1, $12, $7}' >> "$W/table.txt"
done

echo "--- results (one broker, RF=1: functional, not a measurement)"
column -t "$W/table.txt"
[ ${#skipped[@]} -gt 0 ] && { echo "not covered locally (need cluster hosts):"; printf '  %s\n' "${skipped[@]}"; }
if [ "$failed" -eq 0 ] && [ "$ran" -gt 0 ]; then
  echo "PLAN SMOKE PASSED: $ran distinct e2 phase(s) of $PLAN ran on a local broker"
  rc=0
else
  echo "PLAN SMOKE FAILED: $failed of $ran e2 phase(s) failed (logs: $W)" >&2
fi
exit "$rc"
