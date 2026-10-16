#!/usr/bin/env bash
# Local check of the gateway-filtering series on ONE broker built from your
# branch: no cloud, ~2 minutes. Runs the real bench/run-gateways.sh, scaled
# down (RF=1, 2,000 events/sec, 8s per level), over all three arms at 4
# gateways plus keyed at 8, and checks what each arm must show:
#
#   every level clean: each produced event reached its gateway exactly once
#   firehose: gateways keep 1/N of what they read
#   topics:   they keep everything and read nothing foreign
#   keyed:    they keep all their own events, plus whatever collides
#   egress per event (broker fetch bytes): firehose > 2x topics
#
#   REDPANDA_BIN=... WASM_DIR=... bench/local-gateways.sh
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
[ -x "$REDPANDA_BIN" ] || { echo "gateways-check: no broker binary at $REDPANDA_BIN" >&2; exit 1; }
[ -f "$WASM_DIR/gateway-demux.wasm" ] || { echo "gateways-check: no $WASM_DIR/gateway-demux.wasm (make build-wasm)" >&2; exit 1; }
[ -n "$RPK_CLI" ] || { echo "gateways-check: rpk not on PATH" >&2; exit 1; }
for p in "$KAFKA_PORT" "$ADMIN_PORT" "$RPC_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then echo "gateways-check: port $p is in use" >&2; exit 1; fi
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
  data_transforms_per_function_memory_limit: 16777216
  data_transforms_max_instances_per_core: 10
  rpc_server: {address: 127.0.0.1, port: $RPC_PORT}
  kafka_api: [{address: 127.0.0.1, port: $KAFKA_PORT}]
  admin: [{address: 127.0.0.1, port: $ADMIN_PORT}]
rpk:
  admin_api: {addresses: [127.0.0.1:$ADMIN_PORT]}
  kafka_api: {brokers: [127.0.0.1:$KAFKA_PORT]}
YAML
# rpk with --config, so a local rpk profile (SASL, cloud) cannot leak into the
# run - the runner calls "$RPK" for every topic and transform operation.
cat > "$W/rpk" <<SH
#!/usr/bin/env bash
exec "$RPK_CLI" --config "$W/redpanda.yaml" "\$@"
SH
chmod +x "$W/rpk"
cleanup() {
  [ -f "$W/pid" ] && kill -TERM "$(cat "$W/pid")" 2>/dev/null
  sleep 3
  [ -f "$W/pid" ] && kill -KILL "$(cat "$W/pid")" 2>/dev/null
  if [ "${KEEP_WORKDIR:-0}" = 1 ] || [ "${rc:-1}" != 0 ]; then echo "gateways-check: work dir kept: $W"; else rm -rf -- "${W:?}"; fi
}
trap cleanup EXIT
rc=1

echo "--- building gateways"
mkdir -p "$W/tools" "$W/results"
go -C "$ROOT" build -o "$W/tools/gateways" ./cmd/gateways || exit 1
cp "$ROOT/scripts/deploy-transform.sh" "$WASM_DIR/gateway-demux.wasm" "$W/tools/"
echo "--- starting broker $REDPANDA_BIN"
setsid nohup "$REDPANDA_BIN" --redpanda-cfg "$W/redpanda.yaml" --smp 1 --memory 2G --overprovisioned \
  --unsafe-bypass-fsync=true >> "$W/broker.log" 2>&1 < /dev/null &
echo $! > "$W/pid"
for _ in $(seq 1 60); do curl -sf "$ADMIN/v1/status/ready" >/dev/null && break; sleep 1; done
curl -sf "$ADMIN/v1/status/ready" >/dev/null || { echo "gateways-check: broker never ready" >&2; exit 1; }

echo "--- bench/run-gateways.sh, scaled down"
( cd "$W/tools" && RPK="$W/rpk" BROKERS="127.0.0.1:$KAFKA_PORT" ADMIN_URL="$ADMIN" ADMIN_URLS="$ADMIN" \
    BIN="$W/tools" RF=1 RATE=2000 DURATION=8s SETTLE=2 GATEWAY_LEVELS=4 KEYED_EXTRA_LEVELS=8 \
    RESULTS_DIR="$W/results" bash "$HERE/run-gateways.sh" ) > "$W/run.log" 2>&1
tail -8 "$W/run.log" | sed 's/^/  /'

python3 - "$W/results/summary.txt" <<'PY' && rc=0
import sys
rows = {}
for line in open(sys.argv[1]):
    if line.startswith(("#", "level ")) or not line.strip():
        continue
    f = line.split()
    rows[f[0]] = dict(zip(["level", "arm", "gateways", "produced", "records", "bytes", "kept", "kept_fraction",
                           "bytes_per_gateway", "egress_bytes", "egress_per_event", "cpu_s", "cpu_us_per_kept",
                           "p50_us", "p99_us", "p999_us", "clean"], f))
bad = []
def need(ok, what):
    print(("  ok    " if ok else "  FAIL  ") + what)
    ok or bad.append(what)
for lvl in ("firehose-4", "topics-4", "keyed-4", "keyed-8"):
    r = rows.get(lvl)
    need(r is not None and r["clean"] == "true", f"{lvl}: every event reached its gateway once")
if len(bad) == 0:
    fh, tp, k4 = rows["firehose-4"], rows["topics-4"], rows["keyed-4"]
    need(abs(float(fh["kept_fraction"]) - 0.25) < 0.01, f"firehose keeps 1/4 (kept_fraction {fh['kept_fraction']})")
    need(tp["records"] == tp["kept"], "topics: nothing foreign")
    need(int(k4["records"]) >= int(k4["kept"]), "keyed: own events plus collisions")
    e_fh, e_tp = float(fh["egress_per_event"]), float(tp["egress_per_event"])
    need(e_fh > 2 * e_tp, f"egress per event: firehose {e_fh:.0f} B > 2x topics {e_tp:.0f} B")
if bad:
    print("GATEWAYS CHECK FAILED"); sys.exit(1)
print("GATEWAYS CHECK PASSED")
PY
exit "$rc"
