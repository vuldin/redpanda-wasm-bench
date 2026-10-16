#!/usr/bin/env bash
# The offline gate: everything that can be checked before any money is spent.
# scripts/autorun.sh refuses to provision unless this passed for the exact
# commits of all three repos (bench, clients, broker), so run it after your
# last commit and before `autorun.sh start`.
#
#   scripts/offline-gate.sh            all checks, including the two local broker tests
#   REHEARSAL=0 scripts/offline-gate.sh   skip the local broker tests (no broker changes)
#
# Records a line in results/GATE-PASSED on success.
set -uo pipefail
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck source=lib-env.sh
. "$HERE/lib-env.sh"
CLIENTS_REPO="${CLIENTS_REPO:-$WASM_CLIENTS_SRC}"
REDPANDA_BIN="${REDPANDA_BIN:-$REDPANDA_SRC/bazel-bin/src/v/redpanda/redpanda}"
GATE_FILE="${GATE_FILE:-$ROOT/results/GATE-PASSED}"
fail=0
step() { echo ""; echo "=== $*"; }
bad() { echo "GATE FAIL: $*" >&2; fail=1; }

step "wb doctor: tools, AWS session, keys, local build, guest artifacts"
"$ROOT/scripts/wb" doctor >/tmp/gate-doctor-$$.log 2>&1 && echo "  ok" || { bad "wb doctor"; grep -vE '^\s*OK|^===|^$' /tmp/gate-doctor-$$.log | head -10; }
rm -f /tmp/gate-doctor-$$.log

step "clean working trees (a gate result must describe committed code)"
[ -z "$(git -C "$ROOT" status --porcelain -- . ':!results')" ] || bad "uncommitted changes in $ROOT"
[ -z "$(git -C "$CLIENTS_REPO" status --porcelain)" ] || bad "uncommitted changes in $CLIENTS_REPO"
[ -z "$(git -C "$REDPANDA_SRC" status --porcelain -uno)" ] || bad "uncommitted changes in $REDPANDA_SRC"

step "broker binary was built after the last broker source edit"
# Not a commit-time comparison: a binary built from a working tree and then
# committed is current, and dev builds carry no git stamp to compare. What
# matters is whether any source file changed after the build. Files under a
# tests/ directory are not linked into the broker, so adding a unit test after
# the build does not make the binary stale.
if [ -x "$REDPANDA_BIN" ]; then
  newer="$(find "$REDPANDA_SRC/src/v" -type f -newer "$REDPANDA_BIN" -not -path '*/tests/*' \( -name '*.cc' -o -name '*.h' -o -name 'BUILD' -o -name '*.bzl' \) 2>/dev/null | sed -n '1,5p')"
  # A newer timestamp is not always a change: a file restored or re-saved
  # with the same content leaves bazel's action cache valid, and a cache-hit
  # build does not touch the binary's mtime. So ask bazel (capped like every
  # broker build here): if it builds cleanly, the binary matches the sources.
  if [ -n "$newer" ] && [ "${ALLOW_STALE_BINARY:-0}" != 1 ]; then
    echo "  sources newer than the binary; asking bazel whether it is current:"
    echo "$newer" | sed 's/^/    /'
    if (cd "$REDPANDA_SRC" && bazel build //src/v/redpanda:redpanda -c opt --jobs="${BAZEL_JOBS:-6}" \
          --local_resources=memory="${BAZEL_MEMORY_MB:-16384}" >/tmp/gate-bazel-$$.log 2>&1); then
      echo "  bazel: binary is current ($(grep -o '[0-9]* total action[s]*' /tmp/gate-bazel-$$.log | tail -1))"
    else
      bad "broker build failed - see /tmp/gate-bazel-$$.log"
    fi
  fi
  echo "  $(sha256sum "$REDPANDA_BIN" | cut -c1-16) $REDPANDA_BIN (built $(date -r "$REDPANDA_BIN" '+%F %T'))"
else
  bad "no broker binary at $REDPANDA_BIN"
fi

step "the external host (bin/wasm-client) was built after its source last changed"
hostbin="$CLIENTS_REPO/bin/wasm-client"
if [ -x "$hostbin" ]; then
  newer="$(find "$CLIENTS_REPO/host" -type f -name '*.go' -newer "$hostbin" 2>/dev/null | head -3)"
  [ -z "$newer" ] || { bad "host sources changed after $hostbin was built - run: make -C $CLIENTS_REPO build-host"; echo "$newer" | sed 's/^/    /'; }
else
  bad "no $hostbin - run: make -C $CLIENTS_REPO build-host"
fi

step "guests: rust tests, wasm build, published modules unchanged"
if (cd "$CLIENTS_REPO/rust" && cargo test --release > /tmp/gate-cargo-$$.log 2>&1); then
  grep -E "^test result" /tmp/gate-cargo-$$.log | sed 's/^/  /'
else
  bad "cargo test"; grep -E "FAILED|panicked|error" /tmp/gate-cargo-$$.log | head -10
fi
rm -f /tmp/gate-cargo-$$.log
(cd "$CLIENTS_REPO" && make build-wasm >/dev/null 2>&1 && make verify-published) || bad "make build-wasm / verify-published"

step "bench: go vet, go test, shell suites"
(cd "$ROOT" && go vet ./... && go test ./...) || bad "go vet / go test"
for t in "$ROOT"/bench/tests/*.sh; do
  bash "$t" >/tmp/gate-$$.log 2>&1 && echo "  ok   $(basename "$t")" || { bad "$(basename "$t")"; tail -5 /tmp/gate-$$.log; }
done
rm -f /tmp/gate-$$.log
for f in "$ROOT"/bench/*.sh "$ROOT"/scripts/*.sh "$ROOT/scripts/wb" "$ROOT"/plans/*.plan; do bash -n "$f" || bad "syntax: $f"; done

if [ "${REHEARSAL:-1}" = 1 ]; then
  step "local broker rehearsal (snapshot survives SIGKILL)"
  REDPANDA_BIN="$REDPANDA_BIN" WASM_DIR="$CLIENTS_REPO/bin" bash "$ROOT/bench/local-rehearsal.sh" || bad "local rehearsal"
  step "local broker rehearsal, chunked checkpoints (restore reassembles them)"
  CHECKPOINT_CHUNK_BYTES=16384 REDPANDA_BIN="$REDPANDA_BIN" WASM_DIR="$CLIENTS_REPO/bin" bash "$ROOT/bench/local-rehearsal.sh" || bad "local rehearsal (chunked)"
  step "local relay-delivery check (RELAY_DELIVERY=committed waits for the output write)"
  PAIRS=1 REDPANDA_BIN="$REDPANDA_BIN" WASM_DIR="$CLIENTS_REPO/bin" bash "$ROOT/bench/local-relay-delivery.sh" || bad "local relay-delivery check"
fi

echo ""
heads="bench=$(git -C "$ROOT" rev-parse HEAD) clients=$(git -C "$CLIENTS_REPO" rev-parse HEAD) broker=$(git -C "$REDPANDA_SRC" rev-parse HEAD)"
# A pass is only worth recording if it names three commits. A checkout that
# vanished mid-gate (a removed worktree, 2026-10-16) left rev-parse empty and
# the gate recorded "bench= clients=... broker=...".
[[ "$heads" =~ ^bench=[0-9a-f]{40}\ clients=[0-9a-f]{40}\ broker=[0-9a-f]{40}$ ]] \
  || bad "could not read all three commits ($heads)"
if [ "$fail" = 0 ]; then
  mkdir -p "$(dirname "$GATE_FILE")"
  echo "$heads" >> "$GATE_FILE"
  echo "OFFLINE GATE PASSED: $heads"
  echo "  recorded in $GATE_FILE - scripts/autorun.sh start may now provision for these commits"
else
  echo "OFFLINE GATE FAILED - nothing recorded" >&2
  exit 1
fi
