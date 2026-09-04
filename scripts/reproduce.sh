#!/usr/bin/env bash
# One command from nothing to both published tables.
#
# This exists because the alternative is a person who knows which of eight
# scripts to run with which environment variables, in which order - which is
# not a reproduction, it is an apprenticeship. Everything here is scripted and
# configurable; nothing needs an operator's judgment mid-run.
#
# It deliberately does NOT tear down at the end. Teardown is destructive and
# the results directory is on the cluster, so it is a separate, explicit step -
# but the cost of forgetting is real money, so the final line says so loudly.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

RATE="${RATE:-1000}"
RF="${RF:-3}"
PAYLOAD="${PAYLOAD:-650}"
ACKS="${ACKS:-all}"
INBROKER_CONSUMERS="${INBROKER_CONSUMERS:-500}"
EXTERNAL_LADDER="${EXTERNAL_LADDER:-1 10 50 500}"
TRIALS="${TRIALS:-3}"
DURATION="${DURATION:-30s}"; WARMUP="${WARMUP:-10s}"; DRAIN="${DRAIN:-30s}"
REGIONS="${REGIONS:-us-east-2 us-east-1 us-west-2 eu-west-1}"
SKIP_PROVISION="${SKIP_PROVISION:-false}"
SKIP_DEPLOY="${SKIP_DEPLOY:-$SKIP_PROVISION}"
RUN_ID="${RUN_ID:-$(date +%s)}"
OUT_LOCAL="${OUT_LOCAL:-$ROOT/results/reproduce-$RUN_ID}"

say() { printf '\n=== %s ===\n' "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

# Wait for the cluster to report healthy. Deleting a few hundred consumer
# groups leaves __consumer_offsets partitions briefly leaderless while
# leadership is re-elected, which is normal and self-heals in seconds - but the
# deploy playbook's pre-flight treats any is_healthy:false as fatal and its
# advice ("wipe /var/lib/redpanda/data") is wildly disproportionate for it.
# Waiting here turns a spurious hard failure into a pause.
wait_healthy() {
  local budget="${1:-180}" seed cp waited=0
  eval "$(bash "$HERE/wb" env 2>/dev/null | grep -E '^(BROKER_PRIV|CLIENT_PUB)=')"
  seed=$(echo "$BROKER_PRIV" | cut -d' ' -f1)
  cp=$(echo "$CLIENT_PUB" | cut -d' ' -f1)
  [ -n "$seed" ] && [ -n "$cp" ] || { echo "  cannot resolve cluster addresses; skipping health wait"; return 0; }
  while [ "$waited" -lt "$budget" ]; do
    if ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
         -i "${BENCH_SSH_KEY:-$HOME/.ssh/id_rsa}" "ubuntu@$cp" \
         "curl -sf --max-time 5 http://$seed:9644/v1/cluster/health_overview" 2>/dev/null \
         | grep -q '"is_healthy":[[:space:]]*true'; then
      echo "  cluster healthy after ${waited}s"
      return 0
    fi
    sleep 10; waited=$((waited+10))
  done
  echo "  WARNING: cluster still not healthy after ${budget}s - continuing anyway" >&2
  return 0
}

# ---------------------------------------------------------------------------
# 1. prerequisites - fail before spending money, not after
# ---------------------------------------------------------------------------
say "prerequisites"
# Collect every failure and report them together. Dying at the first one means
# somebody with an expired AWS session never finds out their wasm build is also
# missing, and re-runs three times to learn three things.
FAILS=()

for t in aws terraform ansible-playbook jq python3 go ssh md5sum; do
  if command -v "$t" >/dev/null 2>&1; then echo "  ok    $t"
  else echo "  MISS  $t"; FAILS+=("install $t"); fi
done

if command -v aws >/dev/null 2>&1 && aws sts get-caller-identity >/dev/null 2>&1; then
  echo "  ok    aws credentials ($(aws sts get-caller-identity --query Account --output text 2>/dev/null))"
else
  echo "  MISS  aws credentials"
  FAILS+=("authenticate aws - SSO login is interactive, so run it yourself")
fi

if [ -z "${REDPANDA_BIN:-}" ]; then
  echo "  MISS  REDPANDA_BIN"
  FAILS+=("set REDPANDA_BIN to a redpanda built from transform-latency-instrumentation (README prerequisites)")
elif [ ! -x "$REDPANDA_BIN" ]; then
  echo "  MISS  REDPANDA_BIN=$REDPANDA_BIN is not executable"
  FAILS+=("point REDPANDA_BIN at an executable")
else
  echo "  ok    redpanda binary ($(du -h "$REDPANDA_BIN" | cut -f1))"
fi

MATCHER_MD5=""
if [ -z "${WASM_DIR:-}" ]; then
  echo "  MISS  WASM_DIR"
  FAILS+=("set WASM_DIR to redpanda-wasm-clients/bin, after running 'make' there")
else
  wasm_missing=""
  for w in wasm-matcher.wasm relay-probe.wasm passthrough.wasm relay-sink.wasm; do
    [ -f "$WASM_DIR/$w" ] || wasm_missing="$wasm_missing $w"
  done
  if [ -n "$wasm_missing" ]; then
    echo "  MISS  wasm guests:$wasm_missing"
    FAILS+=("run 'make build-wasm' in redpanda-wasm-clients")
  else
    MATCHER_MD5=$(md5sum "$WASM_DIR/wasm-matcher.wasm" | cut -d' ' -f1)
    echo "  ok    wasm guests (matcher md5 $MATCHER_MD5)"
    if [ "$MATCHER_MD5" != "94c3d0177a754b9f0c1df80c10dea298" ]; then
      echo "  NOTE  not the module the published numbers used"
      echo "        (94c3d0177a754b9f0c1df80c10dea298). Results stay valid, but they"
      echo "        are a different experiment - record this md5 with them."
    fi
  fi
  if [ -x "$WASM_DIR/wasm-client" ] || [ -x "$WASM_DIR/../bin/wasm-client" ]; then
    echo "  ok    external host binary"
  else
    echo "  MISS  external host binary"
    FAILS+=("run 'make build-host' in redpanda-wasm-clients")
  fi
fi

if [ "${#FAILS[@]}" -gt 0 ]; then
  echo ""
  echo "ERROR: ${#FAILS[@]} prerequisite(s) unmet - nothing has been provisioned:" >&2
  for f in "${FAILS[@]}"; do echo "  - $f" >&2; done
  exit 1
fi

# ---------------------------------------------------------------------------
# 2. size the fleet, then find a region that can actually hold it
# ---------------------------------------------------------------------------
if [ "$SKIP_PROVISION" != "true" ]; then
  say "sizing the client fleet from EXTERNAL_LADDER"
  EXTERNAL_LADDER="$EXTERNAL_LADDER" bash "$HERE/size-clients.sh" || die "sizing failed"

  say "selecting a region with capacity"
  REGIONS="$REGIONS" bash "$HERE/select-region.sh" || die "no region has capacity for this topology"

  say "provisioning"
  bash "$HERE/wb" up || die "provisioning failed"

else
  echo "  SKIP_PROVISION=true - using the existing cluster as-is"
fi

if [ "$SKIP_DEPLOY" != "true" ]; then
  say "waiting for cluster health before deploying"
  wait_healthy 180
  say "deploying broker binary and client tools"
  REDPANDA_BIN="$REDPANDA_BIN" WASM_DIR="$WASM_DIR" bash "$HERE/wb" deploy \
    || die "deploy failed"
else
  echo "  SKIP_DEPLOY=true - not reshipping binaries"
fi

# ---------------------------------------------------------------------------
# 2b. start from a pristine cluster
# ---------------------------------------------------------------------------
# Not optional. The runner defaults to fresh_topics_per_level=0, so a level
# REUSES its topics - and a cluster carrying data from an earlier run makes 500
# relay consumers churn that backlog instead of the offered stream. First
# attempt at this: an earlier sweep had left large topics behind, and the
# in-broker arm came back unclean with transform lag peaking at 1011 records
# and transform_failures still climbing past 2312 after 240s. Nothing was wrong
# with the code; the cluster was dirty.
say "clearing cluster state before measuring"
bash "$HERE/wb" cleanup-orphans --force 2>&1 | tail -3 || true
# clearing groups is exactly what makes __consumer_offsets briefly leaderless
wait_healthy 180

# ---------------------------------------------------------------------------
# 3. arm 1: in-broker wasm
# ---------------------------------------------------------------------------
say "arm 1: in-broker wasm, $INBROKER_CONSUMERS consumers, $TRIALS trials"
# `wb run` forwards KEY=VALUE ARGUMENTS to the remote runner as exports. It does
# NOT forward its own environment - passing these as env silently ran the
# runner's defaults instead (MODE=rate, RF=1, write.caching=false), i.e. an
# entirely different experiment that still produced clean-looking reports.
for t in $(seq 1 "$TRIALS"); do
  echo "--- trial $t/$TRIALS ---"
  bash "$HERE/wb" run e2 \
    MODE=fanout "FANOUT_LEVELS=$INBROKER_CONSUMERS" "RATE=$RATE" \
    "PAYLOAD=$PAYLOAD" "ACKS=$ACKS" "RF=$RF" \
    "DURATION=$DURATION" "WARMUP=$WARMUP" "DRAIN=$DRAIN" \
    RELAY_STAGE_METRICS=true WRITE_CACHING=true LINGER_MS=0 \
    || echo "  trial $t did not complete cleanly" >&2
done

# ---------------------------------------------------------------------------
# 4. arm 2: traditional, one level at a time
# ---------------------------------------------------------------------------
say "arm 2: traditional deployment, ladder: $EXTERNAL_LADDER"
# run-traditional-sharded.sh drives the hosts itself over ssh, so it needs the
# actual addresses. They come from terraform via `wb env`; without them it
# aborts on its own CLIENTS:?  guard, which is how the first run silently
# skipped this whole arm.
eval "$(bash "$HERE/wb" env 2>/dev/null | grep -E '^(CLIENT_PUB|BROKER_PRIV)=')"
[ -n "${CLIENT_PUB:-}" ] || die "could not read CLIENT_PUB from wb env - is the cluster up?"
BROKERS_CSV=$(echo "$BROKER_PRIV" | tr ' ' '\n' | sed 's/$/:9092/' | paste -sd,)
n_clients=$(echo "$CLIENT_PUB" | wc -w)
[ "$n_clients" -ge 3 ] || die "traditional arm needs >=3 clients (loadgen, matcher, fanout); have $n_clients"
echo "  clients: $n_clients  brokers: $BROKERS_CSV"
for level in $EXTERNAL_LADDER; do
  echo "--- $level external consumer(s) ---"
  CLIENTS="$CLIENT_PUB" BROKERS="$BROKERS_CSV" \
  FANOUT_N="$level" RATE="$RATE" PAYLOAD="$PAYLOAD" ACKS="$ACKS" \
  DURATION="$DURATION" WARMUP="$WARMUP" DRAIN="$DRAIN" \
    bash "$ROOT/bench/run-traditional-sharded.sh" \
    || echo "  level $level did not complete cleanly" >&2
done

# ---------------------------------------------------------------------------
# 5. collect and render
# ---------------------------------------------------------------------------
say "collecting results"
mkdir -p "$OUT_LOCAL"
bash "$HERE/wb" results "$OUT_LOCAL" || echo "  could not copy results back" >&2
# Scope to THIS run's labels. The results directory on the cluster accumulates
# every run, and without this the tables silently average unrelated experiments.
python3 "$HERE/render-tables.py" "$OUT_LOCAL" \
  --inbroker-consumers "$INBROKER_CONSUMERS" --matcher-md5 "$MATCHER_MD5" \
  --labels "fanout-,trad-" \
  | tee "$OUT_LOCAL/TABLES.md"

cat <<EOF

=== the cluster is STILL RUNNING and still billing (~\$40/hr) ===
  tear it down:  ./scripts/wb teardown
  then VERIFY:   ./scripts/wb verify-clean
Do not skip verify-clean: this harness was developed in an account that both
removed infrastructure on its own and kept billing after an apparently
successful destroy.

results: $OUT_LOCAL
REPRODUCE_DONE
EOF
