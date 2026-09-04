#!/usr/bin/env bash
# Verify that EVERY commit on a branch builds standalone.
#
# Why this exists: a branch whose tip builds tells you nothing about its
# commits. On 2026-09-03 three commits on transform-latency-instrumentation had
# never compiled as committed - the declaration one of them needed lived only in
# the working tree - and the tip built fine the whole time, because the worktree
# carried the missing header. A tip-only bazel run came back with 9,364 action
# cache hits and 1 action: it compiled nothing and proved nothing.
#
# Builds oldest-first on purpose: consecutive commits differ by one feature, so
# bazel's action cache carries most of the work forward and the sweep costs far
# less than N full builds.
#
# --jobs and --local_resources are capped and NOT optional. An unconstrained
# `bazel build --config=release` froze this laptop hard enough to need a power
# cycle.
set -uo pipefail

REPO="${REPO:-$HOME/redpanda/redpanda}"
TARGET="${TARGET:-//src/v/redpanda:redpanda}"
BASE_REF="${BASE_REF:-origin/dev}"
JOBS="${JOBS:-6}"
MEM_MB="${MEM_MB:-16384}"
LOG_DIR="${LOG_DIR:-/tmp/verify-each-commit-$(date +%Y%m%d-%H%M%S)}"
STOP_ON_FAIL="${STOP_ON_FAIL:-false}"

cd "$REPO" || { echo "no repo at $REPO" >&2; exit 1; }

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$BRANCH" = "HEAD" ]; then
  echo "ERROR: repo is in detached HEAD; check out the branch to verify first" >&2
  exit 1
fi
if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: working tree is dirty. This script checks out every commit in" >&2
  echo "       turn and would either fail or silently build your uncommitted" >&2
  echo "       changes instead of the commit. Commit or stash first." >&2
  exit 1
fi

BASE="$(git merge-base HEAD "$BASE_REF")"
mapfile_compat() { git rev-list --reverse "${BASE}..HEAD"; }
COMMITS=$(mapfile_compat)
N=$(echo "$COMMITS" | grep -c .)
mkdir -p "$LOG_DIR"

echo "branch      : $BRANCH"
echo "base        : $(git rev-parse --short "$BASE") ($BASE_REF)"
echo "commits     : $N"
echo "target      : $TARGET"
echo "resources   : --jobs=$JOBS --local_resources=memory=$MEM_MB"
echo "logs        : $LOG_DIR"
echo ""

# Always put the branch back, even on signal - a detached HEAD left behind is
# how a later 'git status' misleads someone into thinking work was lost.
restore() { git checkout -q "$BRANCH" 2>/dev/null || true; }
trap restore EXIT
trap 'echo "SIGINT - restoring $BRANCH" >&2; restore; exit 130' INT
trap 'echo "SIGTERM - restoring $BRANCH" >&2; restore; exit 143' TERM

PASS=0; FAIL=0; FIRST_FAIL=""
i=0
for c in $COMMITS; do
  i=$((i+1))
  short=$(git rev-parse --short "$c")
  subj=$(git log -1 --format=%s "$c" | cut -c1-58)
  printf "[%2d/%2d] %s %-58s " "$i" "$N" "$short" "$subj"
  git checkout -q "$c" || { echo "CHECKOUT-FAIL"; FAIL=$((FAIL+1)); continue; }
  log="$LOG_DIR/$(printf '%02d' "$i")-$short.log"
  start=$SECONDS
  if bazel build "$TARGET" -c opt \
        --jobs="$JOBS" --local_resources=memory="$MEM_MB" >"$log" 2>&1; then
    printf "OK   (%4ds)\n" "$((SECONDS-start))"
    PASS=$((PASS+1))
  else
    printf "FAIL (%4ds)\n" "$((SECONDS-start))"
    FAIL=$((FAIL+1))
    [ -z "$FIRST_FAIL" ] && FIRST_FAIL="$short"
    # The first few compiler errors are what identify the missing declaration
    # or dependency; the rest are cascade.
    grep -E '^(src|external)/.*(error|fatal error):' "$log" | head -5 | sed 's/^/          /'
    if [ "$STOP_ON_FAIL" = "true" ]; then
      echo "STOP_ON_FAIL set - stopping at first failure" >&2
      break
    fi
  fi
done

echo ""
echo "=== $PASS/$N built, $FAIL failed ==="
[ -n "$FIRST_FAIL" ] && echo "first failure: $FIRST_FAIL (see $LOG_DIR)"
echo "VERIFY_DONE"
[ "$FAIL" -eq 0 ]
