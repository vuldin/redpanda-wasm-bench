#!/usr/bin/env bash
# Local check that a caught-up transform sleeps until new input arrives, on
# ONE broker built from your branch: no cloud, ~1 minute. Two failure modes,
# one check each:
#
#   spinning: the broker branch's wait for new data was always already
#   satisfied (josh-431), so an idle transform re-read ~278,000 times a second
#   and used ~90% of its core. Here an idle gateway-demux must use under
#   IDLE_MAX_MS_PER_S (50) of transforms-group CPU per second.
#   sleeping through input: the fix must still wake on new data at once, not
#   on a fallback timer (upstream's was ~1-1.5s). Records sent one at a time
#   after idle gaps must come through with a mean broker e2e latency
#   (redpanda_transform_e2e_latency_seconds) under WAKE_MAX_MS (100).
#
#   REDPANDA_BIN=... WASM_DIR=... bench/local-idle-transform.sh
#
# Ports: KAFKA_PORT (19094), ADMIN_PORT (9646), RPC_PORT (33147) - must be free.
set -uo pipefail
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=../scripts/lib-env.sh
. "$ROOT/scripts/lib-env.sh"
REDPANDA_BIN="${REDPANDA_BIN:-$REDPANDA_SRC/bazel-bin/src/v/redpanda/redpanda}"
WASM_DIR="${WASM_DIR:-$WASM_CLIENTS_SRC/bin}"
KAFKA_PORT="${KAFKA_PORT:-19094}" ADMIN_PORT="${ADMIN_PORT:-9646}" RPC_PORT="${RPC_PORT:-33147}"
IDLE_MAX_MS_PER_S="${IDLE_MAX_MS_PER_S:-50}" WAKE_MAX_MS="${WAKE_MAX_MS:-100}"
RPK_CLI="${RPK_CLI:-$(command -v rpk)}"
[ -x "$REDPANDA_BIN" ] || { echo "idle-check: no broker binary at $REDPANDA_BIN" >&2; exit 1; }
[ -f "$WASM_DIR/gateway-demux.wasm" ] || { echo "idle-check: no $WASM_DIR/gateway-demux.wasm (make build-wasm)" >&2; exit 1; }
[ -n "$RPK_CLI" ] || { echo "idle-check: rpk not on PATH" >&2; exit 1; }
for p in "$KAFKA_PORT" "$ADMIN_PORT" "$RPC_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then echo "idle-check: port $p is in use" >&2; exit 1; fi
done

W="$(mktemp -d)"
ADMIN="http://127.0.0.1:$ADMIN_PORT"
cat > "$W/redpanda.yaml" <<YAML
redpanda:
  data_directory: $W/data
  node_id: 0
  developer_mode: true
  seed_servers: []
  data_transforms_enabled: true
  rpc_server: {address: 127.0.0.1, port: $RPC_PORT}
  kafka_api: [{address: 127.0.0.1, port: $KAFKA_PORT}]
  admin: [{address: 127.0.0.1, port: $ADMIN_PORT}]
rpk:
  admin_api: {addresses: [127.0.0.1:$ADMIN_PORT]}
  kafka_api: {brokers: [127.0.0.1:$KAFKA_PORT]}
YAML
R() { "$RPK_CLI" --config "$W/redpanda.yaml" "$@"; }
cleanup() {
  [ -f "$W/pid" ] && kill -TERM "$(cat "$W/pid")" 2>/dev/null
  sleep 3
  [ -f "$W/pid" ] && kill -KILL "$(cat "$W/pid")" 2>/dev/null
  if [ "${KEEP_WORKDIR:-0}" = 1 ] || [ "${rc:-1}" != 0 ]; then echo "idle-check: work dir kept: $W"; else rm -rf -- "${W:?}"; fi
}
trap cleanup EXIT
rc=1

echo "--- starting broker $REDPANDA_BIN"
setsid nohup "$REDPANDA_BIN" --redpanda-cfg "$W/redpanda.yaml" --smp 1 --memory 2G --overprovisioned \
  --unsafe-bypass-fsync=true >> "$W/broker.log" 2>&1 < /dev/null &
echo $! > "$W/pid"
for _ in $(seq 1 60); do curl -sf "$ADMIN/v1/status/ready" >/dev/null && break; sleep 1; done
curl -sf "$ADMIN/v1/status/ready" >/dev/null || { echo "idle-check: broker never ready" >&2; exit 1; }

R topic create ev -p 1 >/dev/null || exit 1
outs=""
for i in 0 1 2 3 4 5 6 7; do R topic create "gw-$i" -p 1 >/dev/null || exit 1; outs="${outs:+$outs,}gw-$i"; done
R transform deploy --file "$WASM_DIR/gateway-demux.wasm" --name idle-demux --input-topic ev \
  --output-topic "$outs" --var MODE=topics --var TOPIC_PREFIX=gw- >/dev/null || { echo "idle-check: deploy failed" >&2; exit 1; }
for _ in $(seq 1 60); do R transform list 2>/dev/null | grep -qE 'idle-demux .* 1 / 1' && break; sleep 1; done

# Transforms-group CPU, ms summed over shards.
transforms_ms() {
  curl -sf "$ADMIN/metrics" | awk '/^vectorized_scheduler_runtime_ms\{/ && /group="transforms"/ {s += $NF} END {printf "%.0f", s}'
}
sleep 5
a="$(transforms_ms)"; sleep 10; b="$(transforms_ms)"
idle=$(( (b - a) / 10 ))

# One record at a time after idle gaps: each must wake the transform.
for i in 1 2 3 4 5; do
  sleep 2
  echo "{\"gateway\":$((i % 8)),\"n\":$i}" | R topic produce ev >/dev/null || { echo "idle-check: produce failed" >&2; exit 1; }
done
sleep 2
read -r count sum < <(curl -sf "$ADMIN/public_metrics" | awk '
  /^redpanda_transform_e2e_latency_seconds_count\{/ && /idle-demux/ {c += $NF}
  /^redpanda_transform_e2e_latency_seconds_sum\{/ && /idle-demux/ {s += $NF}
  END {printf "%d %.6f\n", c, s}')

fail=0
if [ "$idle" -lt "$IDLE_MAX_MS_PER_S" ]; then
  echo "  ok    an idle transform used ${idle} ms/s of transforms-group CPU (limit $IDLE_MAX_MS_PER_S)"
else
  echo "  FAIL  an idle transform used ${idle} ms/s of transforms-group CPU (limit $IDLE_MAX_MS_PER_S): it is spinning"
  fail=1
fi
if [ "${count:-0}" -lt 1 ]; then
  echo "  FAIL  no transform e2e latency samples: the records never came through"
  fail=1
else
  mean_ms=$(awk -v s="$sum" -v c="$count" 'BEGIN {printf "%.1f", s * 1000 / c}')
  if awk -v m="$mean_ms" -v t="$WAKE_MAX_MS" 'BEGIN {exit !(m < t)}'; then
    echo "  ok    records after idle gaps came through in ${mean_ms} ms mean e2e ($count samples, limit $WAKE_MAX_MS ms)"
  else
    echo "  FAIL  records after idle gaps took ${mean_ms} ms mean e2e ($count samples, limit $WAKE_MAX_MS ms): the transform slept through new input"
    fail=1
  fi
fi
if [ "$fail" = 0 ]; then echo "IDLE-TRANSFORM CHECK PASSED"; rc=0; else echo "IDLE-TRANSFORM CHECK FAILED"; fi
exit "$rc"
