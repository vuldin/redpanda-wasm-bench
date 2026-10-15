#!/usr/bin/env bash
# The traditional (non-in-broker) arms over a fan-out ladder, one
# run-traditional-sharded.sh invocation per (placement, fan-out) level.
# Runs on the WORKSTATION (that script drives every host over ssh); invoked as
# `wb run traditional KEY=VALUE...`, which supplies the cluster's addresses.
#
#   PLACEMENTS     external | loopback | loopback-isolated  (default: all three)
#                  external           matcher on its own client instance
#                  loopback           matcher on the partition leader's broker,
#                                     over loopback; cores shared with Redpanda
#                  loopback-isolated  the same, pinned to cores outside
#                                     Redpanda's cpuset (Travis's method)
#   FANOUT_LADDER  total consumers incl. loadgen, e.g. "1 10 20 50"
#
# Everything else (RATE, ORDERS_PER_RECORD, GUEST_MODULE, BOOK_DEPTH,
# CANCEL_FRACTION, DURATION, ...) passes through to run-traditional-sharded.sh.
# Results stay on client0 under results/traditional-sharded-<id>/, which
# `wb results` fetches.
set -uo pipefail
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
: "${CLIENTS:?wb run traditional supplies CLIENTS}" "${BROKERS:?}" "${KEY:?}"
PLACEMENTS="${PLACEMENTS:-external loopback loopback-isolated}"
FANOUT_LADDER="${FANOUT_LADDER:-1 10 20 50}"
fails=0 total=0
for p in $PLACEMENTS; do
  for f in $FANOUT_LADDER; do
    total=$((total + 1))
    case "$p" in
      external)          place=external cpus="" ;;
      loopback)          place=loopback cpus="" ;;
      loopback-isolated) place=loopback cpus=auto ;;
      *) echo "unknown placement $p" >&2; fails=$((fails + 1)); continue ;;
    esac
    echo ""
    echo "############ traditional placement=$p fanout=$f ############"
    if ! FANOUT_N="$f" MATCHER_PLACEMENT="$place" MATCHER_CPUS="$cpus" \
         RUN_ID="$(date +%s)" bash "$HERE/run-traditional-sharded.sh"; then
      echo "!!! level placement=$p fanout=$f failed; continuing"
      fails=$((fails + 1))
    fi
    sleep 5
  done
done
echo "# traditional ladder done: failed_levels=$fails/$total"
[ "$fails" -lt "$total" ]
