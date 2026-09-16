#!/usr/bin/env bash
# Report any benchmark cluster still running, and what it has cost so far.
#
# WHY THIS IS A SCRIPT AND NOT A HABIT: this cluster costs about $40/hr and
# teardown is deliberately manual, because the results live on the cluster and
# an automatic destroy would throw them away. That combination means the
# failure mode is forgetting, and forgetting has happened before - clusters in
# this account have been left running for days after a "tear it down when I'm
# done" intention.
#
# Designed for cron. It needs no arguments, holds no state, prints nothing when
# the account is clean, and exits non-zero when something is billing so cron
# mails you only when there is something to act on:
#
#   # every 30 minutes, mail me only while a bench cluster is up
#   */30 * * * * /path/to/redpanda-wasm-bench/scripts/cost-guard.sh --quiet
#
# It never destroys anything. Deciding to tear down a cluster mid-run is not a
# decision a cron job should make - the whole point of the manual teardown is
# that the results are still on it.
set -uo pipefail

HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
export AWS_PROFILE="${AWS_PROFILE:-sandbox-cse}"
OWNER="${OWNER:-rp-percore-benchmark}"
# Every region the harness can land in, since select-region.sh falls back on
# capacity and a forgotten cluster is most likely in the one you did not expect.
REGIONS="${REGIONS:-us-east-1 us-east-2 us-west-2 eu-west-1}"
QUIET=0
[ "${1:-}" = "--quiet" ] && QUIET=1

# Approximate on-demand USD/hr. Deliberately a static table rather than the
# Pricing API: this is a nag about money, not an invoice, and a wrong-by-10%
# number still answers "is this worth walking over to the terminal for". Update
# if the topology changes instance types.
rate_for() {
  case "$1" in
    r8id.8xlarge)  echo 8.00 ;;
    c5n.9xlarge)   echo 1.95 ;;
    m6i.2xlarge)   echo 0.38 ;;
    *)             echo 0    ;;
  esac
}

found=0
total_hourly=0
total_spend=0
report=""

for region in $REGIONS; do
  rows="$(aws ec2 describe-instances --region "$region" \
    --filters "Name=tag:owner,Values=$OWNER" \
              "Name=instance-state-name,Values=running,pending" \
    --query 'Reservations[].Instances[].[InstanceType,LaunchTime,InstanceId]' \
    --output text 2>/dev/null)" || continue
  [ -z "$rows" ] && continue

  while read -r itype launched iid; do
    [ -z "$itype" ] && continue
    found=$((found + 1))
    rate="$(rate_for "$itype")"
    # LaunchTime is ISO8601; date -d parses it. Fall back to 0 hours rather
    # than producing a nonsense figure if it ever does not.
    start_epoch="$(date -d "$launched" +%s 2>/dev/null || echo 0)"
    if [ "$start_epoch" -gt 0 ]; then
      hours="$(awk -v s="$start_epoch" -v n="$(date +%s)" 'BEGIN{printf "%.2f", (n-s)/3600}')"
    else
      hours=0
    fi
    spend="$(awk -v r="$rate" -v h="$hours" 'BEGIN{printf "%.2f", r*h}')"
    total_hourly="$(awk -v a="$total_hourly" -v b="$rate" 'BEGIN{printf "%.2f", a+b}')"
    total_spend="$(awk -v a="$total_spend" -v b="$spend" 'BEGIN{printf "%.2f", a+b}')"
    report="$report$(printf '    %-14s %-20s %6sh  ~$%s\n' "$itype" "$region" "$hours" "$spend")"$'\n'
  done <<< "$rows"
done

if [ "$found" -eq 0 ]; then
  [ "$QUIET" -eq 1 ] || echo "cost-guard: nothing tagged $OWNER is running in: $REGIONS"
  exit 0
fi

cat <<MSG
cost-guard: $found benchmark instance(s) STILL RUNNING (owner=$OWNER)

$report
    running at ~\$$total_hourly/hr, ~\$$total_spend spent so far

Tear down when the run is finished and the results are copied back:
    cd $(cd "$HERE/.." && pwd)
    ./scripts/wb results          # copy results off the cluster FIRST
    ./scripts/wb teardown --yes
    ./scripts/wb verify-clean
MSG
exit 1
