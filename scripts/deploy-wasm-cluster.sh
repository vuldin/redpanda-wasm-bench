#!/usr/bin/env bash
# Formalizes what was previously done ad hoc: build every artifact
# deploy-wasm.yaml ships (redpanda binary + its shared libs, the wasm-
# orderbook Go/Rust binaries and .wasm guests), stage them
# under local_artifacts_dir, and run the playbook against whatever
# cluster terraform/hosts.ini currently points at.
#
# Run this after every redpanda or wasm-orderbook source change that
# needs to reach the live cluster (e.g. the network_module Nagle fix),
# and again any time the cluster topology changes (single-AZ vs
# multi-AZ) - it's the same artifact set either way, only hosts.ini
# differs.
#
# Requires:
#   BENCH_SSH_KEY   private key matching the public key terraform used
set -euo pipefail
# cd to the REPO ROOT, not to scripts/. This script used to live at the root,
# so every path below is root-relative ("$(pwd)/ansible/...", "cd ansible").
# Moving it into scripts/ broke three of them at once; anchoring here fixes all
# of them rather than patching each.
cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.."

: "${BENCH_SSH_KEY:?set to the private key path matching the public_key_path terraform used}"
REDPANDA_SRC="${REDPANDA_SRC:-$HOME/redpanda/redpanda}"
# WASM_DIR points at redpanda-wasm-clients/bin - the guests and the external
# host are built in THAT repo, deliberately, so the artifact under test cannot
# be silently changed by the harness measuring it. The Go measurement tools are
# built from this repo.
WASM_DIR="${WASM_DIR:?set to redpanda-wasm-clients/bin (run 'make' there first)}"
# cwd is already scripts/ by here (see the cd above), so resolve from the
# script's own absolute path rather than a relative dirname.
BENCH_SRC="$(pwd)"
STAGE_DIR="${STAGE_DIR:-/tmp/cloud-deploy}"

echo "=== staging artifacts under $STAGE_DIR ==="
rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR/bin" "$STAGE_DIR/redpanda-libs"

# Rewritten 2026-09-04 for the two-repo split. This block used to run `make` in
# a monorepo and copy eight binaries, five of which no longer exist here
# (bench, kafka-matcher, relay-consumer, relay-fanout-load,
# relay-wasm-fanout-bench). Every missing file is a hard failure at this step,
# which is cheap - but the same class of mistake has twice been discovered by
# ansible minutes into a deploy against already-billing hardware.
echo "--- go measurement tools (built from this repo) ---"
for t in loadgen fanout-load; do
  GOOS=linux GOARCH=amd64 go -C "$BENCH_SRC" build -o "$STAGE_DIR/bin/$t" "./cmd/$t" \
    || { echo "FAIL: building $t" >&2; exit 1; }
done

echo "--- wasm guests + external host (from $WASM_DIR) ---"
for w in wasm-matcher.wasm relay-probe.wasm relay-sink.wasm passthrough.wasm; do
  [ -f "$WASM_DIR/$w" ] || { echo "FAIL: missing $WASM_DIR/$w - run 'make build-wasm' in redpanda-wasm-clients" >&2; exit 1; }
  cp "$WASM_DIR/$w" "$STAGE_DIR/"
done
[ -x "$WASM_DIR/wasm-client" ] || { echo "FAIL: missing $WASM_DIR/wasm-client - run 'make build-host'" >&2; exit 1; }
cp "$WASM_DIR/wasm-client" "$STAGE_DIR/bin/"
# Record what shipped. Both arms must run the same module, and this is where
# that becomes checkable after the fact rather than assumed.
md5sum "$STAGE_DIR"/*.wasm > "$STAGE_DIR/WASM-CHECKSUMS"
echo "    matcher md5: $(md5sum "$STAGE_DIR/wasm-matcher.wasm" | cut -d' ' -f1)"

# lib-cluster.sh is sourced by the runners, so it has to land beside them or
# they fail at startup with a confusing "set RPK" error.
# NOTE: this list and the `loop:` list in ansible/deploy-wasm.yaml must agree.
# A script in the ansible list but not staged here fails the deploy with
# "Could not find or access '/tmp/cloud-deploy/<file>'".
echo "--- benchmark scripts ---"
for f in check-bottlenecks.sh scrape-metrics.sh lib-cluster.sh \
         run-e2-throughput.sh run-traditional-sharded.sh \
         run-inbroker-vs-external.sh run-cloud-fanout-comparison.sh \
         run-resilience.sh; do
  cp "$BENCH_SRC/bench/$f" "$STAGE_DIR/" \
    || { echo "FAIL: missing bench/$f" >&2; exit 1; }
done

echo "--- rpk (client CLI only) ---"
cp "$(command -v rpk)" "$STAGE_DIR/rpk"

echo "--- redpanda's shared libs (non-glibc deps our build links against) ---"
REDPANDA_BIN="$REDPANDA_SRC/bazel-bin/src/v/redpanda/redpanda"
for lib in libcom_err.so.3 libgssapi_krb5.so.2 libk5crypto.so.3 libkrb5.so.3 \
           libkrb5support.so.0 libcrypto.so.3 libssl.so.3; do
  src=$(ldd "$REDPANDA_BIN" | awk -v l="$lib" '$1==l{print $3}')
  [ -n "$src" ] || { echo "FAIL: could not resolve $lib via ldd" >&2; exit 1; }
  cp "$(readlink -f "$src")" "$STAGE_DIR/redpanda-libs/$lib"
done

# Preflight: the playbook's src: entries under {{ local_artifacts_dir }} are the
# contract for what STAGE_DIR must hold. Checking it here turns a staging gap
# into a 20ms local failure instead of one discovered by ansible minutes into a
# deploy against already-provisioned (already-billing) hardware. Two deploys
# were lost to exactly that: relay-sink.wasm, then run-round3.sh.
echo "=== preflight: staged artifacts vs playbook requirements ==="
python3 scripts/verify-staging.py "$(pwd)/ansible/deploy-wasm.yaml" "$STAGE_DIR" || {
  echo "FAIL: staging incomplete - not starting ansible" >&2; exit 1; }

echo "=== running deploy-wasm.yaml against $(pwd)/terraform/hosts.ini ==="
cd ansible
ansible-playbook deploy-wasm.yaml \
  --private-key "$BENCH_SSH_KEY" \
  -e "local_artifacts_dir=$STAGE_DIR" \
  -e "redpanda_binary_path=$REDPANDA_BIN" \
  "$@"

echo ""
echo "=== deploy complete - run ../ansible/ensure-cluster-ready.sh next ==="
