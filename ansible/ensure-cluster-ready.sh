#!/usr/bin/env bash
# Preflight companion to wasm-orderbook/bench/run-wasm-vs-external.sh.
# Run this from the ansible control machine (needs SSH to the brokers for
# a safe rolling restart) before every benchmark session - it is
# idempotent, so running it when nothing needs to change is a fast no-op.
#
# Exists because run-wasm-vs-external.sh deliberately REFUSES to run
# against a misconfigured cluster rather than silently producing a
# confounded result. This script is what actually fixes the
# misconfiguration when needed; the client-side script only checks.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

REQUIRED_CACHE_CAP=5000  # must stay >= run-wasm-vs-external.sh's own
                          # (max fanout level * 2) - both scripts assume
                          # 2000 is the highest level in the fixed sweep.
BROKERS=$(cd ../terraform && terraform output -json broker_private_ips | jq -r '[.[] | .+":9092"] | join(",")')
SEED_HOST=$(cd ../terraform && terraform output -json broker_private_ips | jq -r '.[0]')

echo "=== checking cluster health ==="
# The admin API binds to the broker's private IP, not localhost/127.0.0.1
# (confirmed the hard way earlier this session - localhost just gets
# connection-refused). hostname -I resolves to the actual bound address.
health=$(ansible 'broker[0]' -i ../terraform/hosts.ini -m shell -a "curl -sf http://\$(hostname -I | awk '{print \$1}'):9644/v1/cluster/health_overview 2>&1" -o 2>/dev/null | tail -1)
echo "$health" | grep -q '"is_healthy": true' || { echo "FAIL: cluster not healthy: $health" >&2; exit 1; }
echo "OK: cluster healthy"

echo "=== checking our dev build is what's actually running (not a public release) ==="
for h in 'broker[0]' 'broker[1]' 'broker[2]'; do
  ver=$(ansible "$h" -i ../terraform/hosts.ini -m shell -a "/opt/redpanda/bin/redpanda --version 2>&1" -o 2>/dev/null | tail -1)
  echo "$ver" | grep -q "v0.0.0-dev" || { echo "FAIL: $h is not running our patched dev build: $ver" >&2; exit 1; }
done
echo "OK: all 3 brokers running the patched transform-latency-instrumentation build"

echo "=== checking config values, fixing + rolling-restarting if needed ==="
needs_restart=false

cap=$(ansible 'broker[0]' -i ../terraform/hosts.ini -m shell -a "rpk cluster config get readers_cache_target_max_size -X brokers=$BROKERS 2>&1" -o 2>/dev/null | tail -1 | grep -oP '\(stdout\) \K.*')
if [ "$cap" -lt "$REQUIRED_CACHE_CAP" ]; then
  echo "readers_cache_target_max_size=$cap < required $REQUIRED_CACHE_CAP - fixing"
  ansible 'broker[0]' -i ../terraform/hosts.ini -m shell -a "rpk cluster config set readers_cache_target_max_size $REQUIRED_CACHE_CAP -X brokers=$BROKERS 2>&1" -o 2>&1 | tail -3
  needs_restart=true
else
  echo "OK: readers_cache_target_max_size=$cap"
fi

dt=$(ansible 'broker[0]' -i ../terraform/hosts.ini -m shell -a "rpk cluster config get data_transforms_enabled -X brokers=$BROKERS 2>&1" -o 2>/dev/null | tail -1 | grep -oP '\(stdout\) \K.*')
if [ "$dt" != "true" ]; then
  echo "data_transforms_enabled=$dt - fixing"
  ansible 'broker[0]' -i ../terraform/hosts.ini -m shell -a "rpk cluster config set data_transforms_enabled true -X brokers=$BROKERS 2>&1" -o 2>&1 | tail -3
  needs_restart=true
else
  echo "OK: data_transforms_enabled=true"
fi

# needs_restart: no for this one (confirmed against its own cluster-config
# metadata) - deliberately NOT added to the needs_restart flag above, so
# this fix never waits on a restart that was never required. Previously
# had to be set by hand after every fresh cluster/region move - found the
# hard way when a rebuilt cluster silently reverted to the default
# (disabled) and nobody noticed until asked to explain a stale finding.
rc=$(ansible 'broker[0]' -i ../terraform/hosts.ini -m shell -a "rpk cluster config get kafka_fetch_read_coalescing_enabled -X brokers=$BROKERS 2>&1" -o 2>/dev/null | tail -1 | grep -oP '\(stdout\) \K.*')
if [ "$rc" != "true" ]; then
  echo "kafka_fetch_read_coalescing_enabled=$rc - fixing"
  ansible 'broker[0]' -i ../terraform/hosts.ini -m shell -a "rpk cluster config set kafka_fetch_read_coalescing_enabled true -X brokers=$BROKERS 2>&1" -o 2>&1 | tail -3
else
  echo "OK: kafka_fetch_read_coalescing_enabled=true"
fi

if [ "$needs_restart" = "true" ]; then
  echo "=== rolling restart (one broker at a time, waiting for health between each) ==="
  for h in 'broker[0]' 'broker[1]' 'broker[2]'; do
    echo "--- restarting $h ---"
    ansible "$h" -i ../terraform/hosts.ini -b -m systemd -a "name=redpanda state=restarted" -o 2>&1 | tail -1
    for i in $(seq 1 30); do
      healthy=$(ansible "$h" -i ../terraform/hosts.ini -m shell -a "curl -sf http://\$(hostname -I | awk '{print \$1}'):9644/v1/cluster/health_overview 2>/dev/null" -o 2>/dev/null | grep -c '"is_healthy": true')
      [ "$healthy" = "1" ] && break
      sleep 5
    done
    echo "$h healthy"
  done
fi

echo ""
echo "=== cluster is ready for run-wasm-vs-external.sh ==="
echo "BROKERS=$BROKERS"
echo "ADMIN_URL=http://$SEED_HOST:9644"
