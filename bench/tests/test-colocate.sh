#!/usr/bin/env bash
# Offline test of colocate_leadership's decision logic against stubbed cluster
# state - no cluster, no spend. Exists because the function was only correct
# while broker_count == RF, and that broke the first six-broker run
# (2026-10-15): the bug was only discoverable on a provisioned, billing fleet.
#
# The stubs model the admin API's view as two maps (leader, replica set) and
# make transfer/move mutate them, so each case checks the END STATE - both
# topics led from one node - rather than which calls were made.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RPK=/bin/true BROKERS=x:9092 ADMIN_URL=http://x:9644
export RPK BROKERS ADMIN_URL
# shellcheck source=../lib-cluster.sh
source "$HERE/../lib-cluster.sh"

declare -A LEADER REPS
partition_placement() { echo "${LEADER[$1/$2]} 0"; }
partition_replicas()  { printf '%s\n' ${REPS[$1/$2]} | sort | tr '\n' ' ' | sed 's/ $//'; }
# A transfer only succeeds onto a node that holds a replica - the real rule,
# and the one the original bug ignored.
curl() {
  local url="${*: -1}"
  if [[ "$url" =~ /partitions/kafka/([^/]+)/([0-9]+)/transfer_leadership\?target=([0-9]+) ]]; then
    local k="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}" t="${BASH_REMATCH[3]}"
    [[ " ${REPS[$k]} " == *" $t "* ]] && LEADER[$k]="$t"
  fi
  return 0
}
rpkc() {
  # cluster partitions move <topic> -p <part>:<n1>,<n2>,...
  local topic="$4" spec="$6"
  local part="${spec%%:*}" nodes="${spec#*:}"
  REPS[$topic/$part]="${nodes//,/ }"
  # A moved partition's leader lands on one of its new replicas.
  LEADER[$topic/$part]="${nodes%%,*}"
}
sleep() { :; }

pass=0 fail=0
check() {
  local name="$1"
  if colocate_leadership fills orders 1 >/dev/null 2>&1 \
     && [ "${LEADER[orders/0]}" = "${LEADER[fills/0]}" ]; then
    echo "PASS  $name  (both led from node ${LEADER[orders/0]})"; pass=$((pass+1))
  else
    echo "FAIL  $name  (orders@${LEADER[orders/0]} fills@${LEADER[fills/0]})"; fail=$((fail+1))
  fi
}

# broker_count == RF: every node holds every replica (all pre-2026-10-15 runs).
LEADER=([orders/0]=0 [fills/0]=2); REPS=([orders/0]="0 1 2" [fills/0]="0 1 2")
check "broker_count == RF"

# Six brokers, overlapping sets, orders' leader is in the overlap.
LEADER=([orders/0]=2 [fills/0]=4); REPS=([orders/0]="0 1 2" [fills/0]="2 3 4")
check "overlap, reference leader shared"

# Overlap exists but NEITHER current leader is in it: both must move. This is
# the case the original one-transfer design could never satisfy.
LEADER=([orders/0]=0 [fills/0]=4); REPS=([orders/0]="0 1 2" [fills/0]="2 3 4")
check "overlap, neither leader shared"

# Disjoint sets: the ~1-in-20 case that used to abort the sweep outright.
LEADER=([orders/0]=0 [fills/0]=3); REPS=([orders/0]="0 1 2" [fills/0]="3 4 5")
check "disjoint replica sets"

# Already co-located: must be a no-op success.
LEADER=([orders/0]=1 [fills/0]=1); REPS=([orders/0]="0 1 2" [fills/0]="1 3 5")
check "already co-located"

echo "--- $pass passed, $fail failed"
[ "$fail" -eq 0 ]
