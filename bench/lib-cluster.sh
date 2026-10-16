#!/usr/bin/env bash
# Shared cluster-manipulation helpers for the bench runner scripts.
#
# These functions were written and hardened inside run-relay-wasm-fanout.sh and
# its cross-shard variant, each one paying for a real failure that cost a run
# (see the comments on individual functions). The new SCALING-TEST-PLAN.md
# runners source this instead of carrying a fourth copy. The existing
# run-relay-*.sh scripts deliberately keep their own inline copies - they work,
# they are the record of how these numbers were produced, and rewriting them to
# source this would put a working measurement path at risk for no gain.
#
# Required in the environment before sourcing: RPK, BROKERS, ADMIN_URL.
# Sets: RPK_ARGS, ADMIN_HOST.

: "${RPK:?lib-cluster.sh: set RPK to an rpk binary path}"
: "${BROKERS:?lib-cluster.sh: set BROKERS to a comma-separated host:port list}"
: "${ADMIN_URL:?lib-cluster.sh: set ADMIN_URL to http://<host>:<admin-port> (9644 cloud, 9645 local sandbox)}"

ADMIN_HOST="${ADMIN_URL#http://}"
ADMIN_HOST="${ADMIN_HOST#https://}"
RPK_ARGS=(-X "brokers=$BROKERS" -X "admin.hosts=$ADMIN_HOST")

rpkc() { "$RPK" "${RPK_ARGS[@]}" "$@"; }

# --- cluster config save/restore -------------------------------------------

# CFG_SAVED holds "name<TAB>original-value" lines for everything cfg_set has
# touched, so cfg_restore_all can put the cluster back exactly as found even if
# the run aborts partway through a sweep.
CFG_SAVED=""

cfg_set() {
  local name="$1" value="$2"
  if ! printf '%s' "$CFG_SAVED" | grep -q "^${name}	"; then
    local orig
    orig="$(rpkc cluster config get "$name" 2>/dev/null || echo "")"
    CFG_SAVED="${CFG_SAVED}${name}	${orig}
"
  fi
  rpkc cluster config set "$name" "$value" >/dev/null

  # Verify, because a silently-ignored set is indistinguishable from a
  # successful one until the results make no sense - and by then the cluster is
  # usually gone. relay_stage_metrics_enabled was false for a whole Round 2
  # sweep this way (METHODOLOGY #19). Every cluster property the harness touches
  # goes through here, so this is the one place worth checking.
  if [ "${CFG_VERIFY:-on}" = "on" ]; then
    local got
    got="$(rpkc cluster config get "$name" 2>/dev/null | tr -d '[:space:]')"
    local want
    want="$(printf '%s' "$value" | tr -d '[:space:]')"
    if [ -n "$got" ] && [ "$got" != "$want" ]; then
      echo "FAIL: cluster config $name reads back '$got' after setting '$want'." >&2
      echo "      Not continuing - a run whose config did not apply is not a measurement." >&2
      echo "      (set CFG_VERIFY=off to bypass if a property legitimately normalises)" >&2
      return 1
    fi
  fi
}

cfg_restore_all() {
  [ -z "$CFG_SAVED" ] && return 0
  echo "--- restoring cluster config ---"
  while IFS=$'\t' read -r name orig; do
    [ -z "$name" ] && continue
    if [ -n "$orig" ]; then
      rpkc cluster config set "$name" "$orig" >/dev/null 2>&1 || true
    fi
  done <<< "$CFG_SAVED"
}

# Raft leadership changes of the topics whose name matches the extended regex
# $1, summed over every broker (ADMIN_URLS) so far. A runner samples it before
# and after a measured window: anything but 0 means a partition moved inside
# the window, and its latency and delivery include the move.
raft_leader_changes() {
  local pat="$1" total=0 u v
  for u in ${ADMIN_URLS//,/ }; do
    v="$(curl -sf --max-time 10 "$u/metrics" | awk -v t="topic=\"$pat" '/^vectorized_raft_leadership_changes\{/ && $0 ~ t {s += $NF} END {printf "%.0f", s}')"
    total=$(( total + ${v:-0} ))
  done
  echo "$total"
}

# --- teardown ---------------------------------------------------------------

# Remove artifacts belonging to THIS run only.
#
# This used to delete every non-underscore topic on the cluster, and that caused
# the single most expensive measurement failure of 2026-08-29. Runners are
# killed and relaunched constantly during development; an orphaned runner's EXIT
# trap fires minutes later and, being unscoped, deleted the topics out from under
# whatever run was live at that moment. Confirmed from the broker log: a failing
# sweep's own topics were deleted three times each inside its run window.
#
# Downstream, that looked nothing like a harness fault. The client's produces
# began failing UNKNOWN_TOPIC_OR_PARTITION, franz-go retried, the buffer filled,
# Produce blocked for tens of seconds, the send window overran 30s -> 73s, and
# `attempted = sent/elapsed` collapsed to a suspiciously stable ~2,739
# orders/sec - identical whether 10k or 100k was offered, because it is
# blocked-Produce arithmetic rather than any rate. Meanwhile transform lag sat at
# 1 and failures at 0, since nothing ever reached the broker. Three separate
# hypotheses (fsync, producer linger, franz-go in-flight caps) were chased and
# discarded before the cause turned out to be self-inflicted.
#
# So: scope by RUN_ID. A caller that genuinely wants a full wipe can call
# cleanup_every_test_artifact explicitly.
cleanup_run_artifacts() {
  local run_id="${1:?cleanup_run_artifacts needs a RUN_ID}"
  echo "--- cleanup: removing transforms/topics for run $run_id ---"
  # -n 1: unlike `topic delete`, `transform delete` takes one name per call.
  rpkc transform list 2>/dev/null | awk 'NR>1{print $1}' | grep -- "$run_id" | \
    xargs -r -n 1 "$RPK" "${RPK_ARGS[@]}" transform delete --no-confirm 2>/dev/null || true
  # A transform delete's API call returns success before the transform has fully
  # detached from its topics server-side, and deleting the topic immediately
  # after fails with a misleading CLUSTER_AUTHORIZATION_FAILED every time. Poll
  # until this run's transforms are gone first.
  for _ in $(seq 1 15); do
    [ -z "$(rpkc transform list 2>/dev/null | awk 'NR>1{print $1}' | grep -- "$run_id")" ] && break
    sleep 1
  done
  rpkc topic list 2>/dev/null | awk 'NR>1 && $1 !~ /^_/{print $1}' | grep -- "$run_id" | \
    xargs -r -n 50 "$RPK" "${RPK_ARGS[@]}" topic delete 2>/dev/null || true
}

# Full wipe. Only safe when no other run is active - it will destroy a
# concurrent run's topics, which is exactly the failure described above.
cleanup_every_test_artifact() {
  echo "--- cleanup: removing ALL transforms/topics (unscoped - no other run may be active) ---"
  rpkc transform list 2>/dev/null | awk 'NR>1{print $1}' | \
    xargs -r -n 1 "$RPK" "${RPK_ARGS[@]}" transform delete --no-confirm 2>/dev/null || true
  for _ in $(seq 1 15); do
    [ -z "$(rpkc transform list 2>/dev/null | awk 'NR>1{print $1}')" ] && break
    sleep 1
  done
  rpkc topic list 2>/dev/null | awk 'NR>1 && $1 !~ /^_/{print $1}' | \
    xargs -r -n 50 "$RPK" "${RPK_ARGS[@]}" topic delete 2>/dev/null || true
}

# Refuse to start when another runner is already going - the second run's setup
# would delete the first's artifacts even with scoped cleanup, since both create
# transforms against the same cluster.
assert_no_concurrent_run() {
  local mine="${1:?}"
  local others
  others="$(rpkc transform list 2>/dev/null | awk 'NR>1{print $1}' | grep -v -- "$mine" | grep -E 'e2-|e1-' || true)"
  if [ -n "$others" ]; then
    echo "ERROR: another benchmark run appears active - transforms present that are not this run's ($mine):" >&2
    echo "$others" | sed 's/^/    /' >&2
    echo "  Refusing to start: concurrent runs corrupt each other." >&2
    echo "" >&2
    echo "  If a run really is in flight, wait for it." >&2
    echo "  If these are debris from a killed run (check: is any wb/loadgen process" >&2
    echo "  actually alive?), clear them from the operator host with:" >&2
    echo "" >&2
    echo "      ./wb cleanup-orphans" >&2
    echo "" >&2
    echo "  Naming cleanup_every_test_artifact here was useless advice: it is a" >&2
    echo "  function inside this library on the remote host, not something an" >&2
    echo "  operator can invoke. Hence the wb subcommand." >&2
    return 1
  fi
}

# Back-compat shim so existing callers keep working, now scoped when RUN_ID is
# available.
cleanup_all_test_artifacts() {
  if [ -n "${RUN_ID:-}" ]; then
    cleanup_run_artifacts "$RUN_ID"
  else
    cleanup_every_test_artifact
  fi
}

# --- placement --------------------------------------------------------------

# Always GET /v1/cluster/partitions. METHODOLOGY.md #11: the per-partition
# endpoint's .replicas[].core is stale/unmaintained and reads back 0 for every
# partition regardless of real placement, which once produced a completely wrong
# "everything collapsed onto shard 0" conclusion.
#
# Returns the LEADER's node and core, which is what matters: a transform
# processor runs on the shard leading its input partition.
#
# `replicas[]` order is arbitrary and `replicas[0]` is NOT the leader - the
# response carries an explicit `leader_id`. Reading replicas[0] happened to be
# correct at RF=1 (only one replica) and silently reports a random follower at
# RF>1, which would misattribute every placement observation on a
# production-shaped cluster.
partition_placement() {
  curl -sf "$ADMIN_URL/v1/cluster/partitions" | \
    jq -r --arg t "$1" --argjson p "$2" \
      '.[] | select(.topic==$t and .partition_id==$p)
        | .leader_id as $l
        | (.replicas[] | select(.node_id==$l) | "\($l) \(.core)")' 2>/dev/null
}

# Transfer LEADERSHIP (not replica placement) for one partition onto a node.
# At RF>1 the replicas already exist on every node, so relay co-location is a
# leadership question, not a movement question - and `rpk cluster partitions
# move` would rewrite the replica set instead, which is not what we want.
# NOTE the -L. The admin handler does
#   `shard_for(ntp)` -> if this node is not in the raft group, redirect to leader
# so a request to an arbitrary broker answers with a redirect. `curl -sf` treats
# the 3xx as a failure and does NOT follow it, so the transfer silently never
# happened and leadership stayed put - which is exactly how the first RF=3 run
# failed. -L follows to the leader's own admin address.
transfer_leadership_to_node() {
  local topic="$1" partition="$2" node="$3"
  local cur_node
  read -r cur_node _ <<< "$(partition_placement "$topic" "$partition")"
  [ "$cur_node" = "$node" ] && return 0
  # Re-issued every 10s rather than sent once: a partition created moments
  # ago can ignore a transfer while its raft group settles, and a single
  # dropped request used to fail the whole sweep after 60s (2026-10-16,
  # MODE=fanout with fresh topics per level: "leadership never moved to
  # node 5 (still 0)" at the third level of four).
  local i
  for i in $(seq 0 44); do
    if [ $((i % 5)) -eq 0 ]; then
      curl -sfL -X POST \
        "$ADMIN_URL/v1/partitions/kafka/$topic/$partition/transfer_leadership?target=$node" \
        >/dev/null 2>&1 || true
    fi
    sleep 2
    read -r cur_node _ <<< "$(partition_placement "$topic" "$partition")"
    [ "$cur_node" = "$node" ] && return 0
  done
  echo "    WARNING: $topic/$partition leadership never moved to node $node (still ${cur_node:-unknown})" >&2
  return 1
}

# Every node id holding a replica of one partition, space separated.
partition_replicas() {
  curl -sf "$ADMIN_URL/v1/cluster/partitions" | \
    jq -r --arg t "$1" --argjson p "$2" \
      '.[] | select(.topic==$t and .partition_id==$p)
        | [.replicas[].node_id] | sort | join(" ")' 2>/dev/null
}

# Move one partition's replicas onto an exact node set and wait until the
# cluster reports that set. Only meant for an EMPTY partition (just created):
# on a partition carrying data this would trigger a real rebalance.
realign_replicas() {
  local topic="$1" partition="$2" nodes="$3"
  local want; want="$(printf '%s\n' $nodes | sort | tr '\n' ' ' | sed 's/ $//')"
  rpkc cluster partitions move "$topic" -p "$partition:$(printf '%s\n' $nodes | paste -sd,)" >/dev/null 2>&1 || return 1
  for _ in $(seq 1 60); do
    [ "$(partition_replicas "$topic" "$partition")" = "$want" ] && return 0
    sleep 2
  done
  return 1
}

# Co-locate one topic's leadership with the reference topic's, per partition.
#
# This is the actual requirement - the relay has no cross-node hop, so the
# producer's input topic and the relay ntp must share a node per partition.
# Which node wins does not matter for these experiments; that they match does.
#
# It used to simply transfer $topic's leadership onto whoever led $reference,
# on the stated assumption that "at RF>1 the replicas already exist on every
# node". That is true only while broker_count == RF, which was the case for
# every run before 2026-10-14. On a SIX broker cluster at RF=3 the two topics
# get different three-node replica sets, leadership cannot be transferred to a
# node that holds no replica, and the whole sweep aborted with "leadership
# never moved to node 3".
#
# So: intersect the two replica sets and move BOTH topics onto a node in the
# intersection. Prefer a node that already leads one of them, which keeps this
# to a single transfer in the common case and avoids fighting the balancer.
colocate_leadership() {
  local topic="$1" reference="$2" nparts="$3"
  echo "  co-locating $topic leadership with $reference ($nparts partitions)"
  local failed=0
  for i in $(seq 0 $((nparts - 1))); do
    local ref_node top_node ref_reps top_reps common target
    read -r ref_node _ <<< "$(partition_placement "$reference" "$i")"
    read -r top_node _ <<< "$(partition_placement "$topic" "$i")"
    ref_reps="$(partition_replicas "$reference" "$i")"
    top_reps="$(partition_replicas "$topic" "$i")"
    if [ -z "$ref_reps" ] || [ -z "$top_reps" ]; then
      echo "    WARNING: could not read replica sets for $reference/$i or $topic/$i" >&2
      failed=$((failed + 1)); continue
    fi
    # Nodes holding a replica of BOTH partitions.
    common="$(comm -12 <(printf '%s\n' $ref_reps | sort) <(printf '%s\n' $top_reps | sort) | tr '\n' ' ')"
    if [ -z "${common// /}" ]; then
      # Disjoint replica sets: no leadership transfer can help, so move
      # $topic's REPLICAS onto $reference's nodes instead. With broker_count >
      # RF this happens on roughly 1 run in C(brokers,RF) - 1 in 20 at six
      # brokers and RF=3 - and before this branch existed it aborted the
      # whole sweep. Safe here because colocation runs right after topic
      # creation, before anything is produced, so the move carries no data
      # and completes in seconds.
      echo "    $reference/$i (nodes: $ref_reps) and $topic/$i (nodes: $top_reps) share no node;"
      echo "    moving $topic/$i replicas onto $ref_reps"
      if ! realign_replicas "$topic" "$i" "$ref_reps"; then
        echo "    ERROR: could not move $topic/$i replicas onto $ref_reps" >&2
        failed=$((failed + 1)); continue
      fi
      top_reps="$(partition_replicas "$topic" "$i")"
      read -r top_node _ <<< "$(partition_placement "$topic" "$i")"
      common="$ref_reps"
    fi
    # Fewest transfers: if a current leader is already in the intersection,
    # keep it and move only the other topic.
    if [[ " $common " == *" $ref_node "* ]]; then
      target="$ref_node"
    elif [[ " $common " == *" $top_node "* ]]; then
      target="$top_node"
    else
      target="$(printf '%s\n' $common | head -1)"
    fi
    transfer_leadership_to_node "$reference" "$i" "$target" || { failed=$((failed + 1)); continue; }
    transfer_leadership_to_node "$topic" "$i" "$target" || { failed=$((failed + 1)); continue; }
  done
  if [ "$failed" -gt 0 ]; then
    echo "    ERROR: $failed partition(s) not co-located - the relay would deliver NOTHING" >&2
    return 1
  fi
  echo "    all $nparts partitions co-located"
}

# Kept for callers that genuinely need a specific node.
pin_leadership_to_node() {
  local topic="$1" node="$2" nparts="$3"
  echo "  transferring $topic leadership ($nparts partitions) onto node $node"
  local failed=0
  for i in $(seq 0 $((nparts - 1))); do
    transfer_leadership_to_node "$topic" "$i" "$node" || failed=$((failed + 1))
  done
  if [ "$failed" -gt 0 ]; then
    echo "    WARNING: $failed partition(s) not on node $node" >&2
    return 1
  fi
  echo "    all $nparts leaders on node $node"
}

node_num_cores() {
  curl -sf "$ADMIN_URL/v1/brokers" | jq -r --argjson n "$1" \
    '.[] | select(.node_id==$n) | .num_cores'
}

# --- maintenance mode -------------------------------------------------------
#
# Maintenance is a LEADERSHIP DRAIN, not a data move: it blocks new leadership
# for the node and transfers away every partition it leads. Replicas stay put.
# That is why it is the cheapest way to make a transform move - and why the
# transform follows without anything in the cluster layer knowing transforms
# exist.

maintenance_enable() {
  local node="$1"
  # --wait blocks until the node reports drained, so the caller knows the
  # leadership move has actually happened rather than merely been requested.
  rpkc cluster maintenance enable "$node" --wait 2>&1 | tail -2
}

maintenance_disable() {
  local node="$1"
  rpkc cluster maintenance disable "$node" 2>&1 | tail -1
}

# Leaves no node in maintenance. Safe to call when none is - disable on an
# inactive node is a no-op - so it works as an unconditional cleanup step.
maintenance_clear_all() {
  local ids
  ids=$(curl -sf "$ADMIN_URL/v1/brokers" 2>/dev/null | jq -r '.[].node_id' 2>/dev/null)
  local n
  for n in $ids; do
    rpkc cluster maintenance disable "$n" >/dev/null 2>&1 || true
  done
}

# The node currently leading a partition, for choosing a victim to drain.
leader_of() {
  local topic="$1" partition="${2:-0}"
  curl -sf "$ADMIN_URL/v1/partitions/kafka/$topic/$partition" 2>/dev/null \
    | jq -r '.leader_id // empty'
}

wait_for_move() {
  local topic="$1" partition="$2" want_node="$3" want_core="$4"
  local now_node now_core
  for i in $(seq 1 30); do
    read -r now_node now_core <<< "$(partition_placement "$topic" "$partition")"
    if [ "$now_node" = "$want_node" ] && { [ -z "$want_core" ] || [ "$now_core" = "$want_core" ]; }; then
      echo "  confirmed after ${i}s"
      return
    fi
    sleep 1
  done
  echo "ERROR: $topic/$partition never landed on node=$want_node${want_core:+ core=$want_core} (still node=${now_node:-?} core=${now_core:-?})" >&2
  return 1
}

# A freshly created partition reports a transient placeholder placement (core 0)
# for a few seconds before shard_balancer settles it onto its real target, and
# that settling can complete faster than a naive "two reads 2s apart" check can
# distinguish from genuine stability. Reading and trusting whatever shard a
# topic organically settles on is fundamentally racy against that process, which
# is why every runner pins explicitly instead.
wait_for_stable_placement() {
  local topic="$1" partition="$2"
  local prev="" cur=""
  for _ in $(seq 1 30); do
    cur="$(partition_placement "$topic" "$partition")"
    if [ -n "$cur" ] && [ "$cur" = "$prev" ]; then
      return
    fi
    prev="$cur"
    sleep 2
  done
  echo "ERROR: $topic/$partition placement never stabilized" >&2
  return 1
}

pin_to_shard() {
  local topic="$1" partition="$2" pin_node="$3" pin_core="$4"
  local cur_node cur_core
  read -r cur_node cur_core <<< "$(partition_placement "$topic" "$partition")"
  if [ "$cur_node" = "$pin_node" ] && [ "$cur_core" = "$pin_core" ]; then
    echo "  $topic/$partition already on node=$pin_node core=$pin_core"
    return
  fi
  if [ "$cur_node" != "$pin_node" ]; then
    echo "  moving $topic/$partition -> node=$pin_node (was node=$cur_node core=$cur_core)"
    rpkc cluster partitions move "$topic" -p "$partition:$pin_node" >/dev/null
    wait_for_move "$topic" "$partition" "$pin_node" ""
  fi
  echo "  moving $topic/$partition -> node=$pin_node core=$pin_core"
  rpkc cluster partitions move "$topic" -p "$partition:$pin_node-$pin_core" >/dev/null
  wait_for_move "$topic" "$partition" "$pin_node" "$pin_core"
}

# Move EVERY partition of a topic onto one node, leaving the core to the
# balancer. Needed because the relay has NO cross-node hop: delivery is
# Seastar's container().invoke_on(), which is intra-process only. A relay
# consumer therefore only receives data when it runs on the SAME NODE as the
# producing transform - and since a transform's shard follows its input
# partition's leadership, the producer's input topic and the relay ntp (its
# output topic) must be node-co-located partition-for-partition.
#
# Getting this wrong is silent: push() treats "no subscriber here" as a no-op,
# so the consumer simply receives nothing and the run reports zero receipts.
pin_topic_to_node() {
  local topic="$1" node="$2" nparts="$3"
  echo "  pinning $topic ($nparts partitions) onto node $node"
  local moved=0
  for i in $(seq 0 $((nparts - 1))); do
    local cur_node cur_core
    read -r cur_node cur_core <<< "$(partition_placement "$topic" "$i")"
    if [ "$cur_node" = "$node" ]; then continue; fi
    rpkc cluster partitions move "$topic" -p "$i:$node" >/dev/null 2>&1 || true
    moved=$((moved + 1))
  done
  [ "$moved" -gt 0 ] && echo "    requested $moved move(s); waiting for them to settle"
  # Verify rather than assume - a move that silently failed would show up later
  # as zero receipts, which is much harder to diagnose than here.
  for _ in $(seq 1 40); do
    local off=0
    for i in $(seq 0 $((nparts - 1))); do
      local n; read -r n _ <<< "$(partition_placement "$topic" "$i")"
      [ "$n" = "$node" ] || off=$((off + 1))
    done
    [ "$off" -eq 0 ] && { echo "    all $nparts partitions on node $node"; return 0; }
    sleep 3
  done
  echo "    WARNING: some partitions of $topic never landed on node $node - relay delivery will be incomplete" >&2
  return 1
}

# --- readiness --------------------------------------------------------------

# Matches on the name being present and NOT reporting all-zero, rather than an
# exact "1 / 1": RELAY_TARGET_SHARD pins shard N on every node that deploys the
# transform, so a given name can legitimately report more running processors
# than there are partitions.
wait_for_transforms_running() {
  local pending=("$@")
  local listing
  for _ in $(seq 1 60); do
    listing="$(rpkc transform list 2>/dev/null)"
    if [ -n "$listing" ]; then
      local still_pending=()
      for n in "${pending[@]}"; do
        local line
        line="$(echo "$listing" | grep "$n\b" || true)"
        if [ -z "$line" ] || echo "$line" | grep -qE '\b0 */ *[0-9]+\b'; then
          still_pending+=("$n")
        fi
      done
      pending=("${still_pending[@]}")
      [ "${#pending[@]}" -eq 0 ] && return
    fi
    sleep 1
  done
  echo "ERROR: transform(s) never reported running: ${pending[*]}" >&2
  rpkc transform list >&2 2>&1 || true
  return 1
}

# wait_for_transforms_running only confirms SOME processor for a name is running
# somewhere in the cluster, which on a multi-node setup can be an idle duplicate
# on a non-producer node - it reports "running" before the producer node's own
# subscription is registered. An initial full run lost real samples that way
# (350/500 at fanout=10, 1200/2500 at fanout=50): the benchmark started before
# every real subscriber existed, and the matcher's earliest pushes went out to
# nobody, silently and permanently (push() correctly treats "no subscriber yet"
# as a no-op, not a retryable failure).
#
# redpanda_relay_active_subscriptions is a per-node gauge already summed across
# that node's shards, so waiting on the producer node's own copy is a precise,
# node-scoped signal. Assumes ADMIN_URL points at the pinned node; if you
# override the pin to another node, point ADMIN_URL there too.
# Wait until transform_failures STOPS moving across every broker.
#
# Why this exists. relay_active_subscriptions reaching the target is necessary but
# NOT sufficient: deploying 250-1000 transforms at once produces a thundering herd
# on the wasm-binary load, some processors fail to start, and the manager retries
# them. Those retries succeed - zero drops, zero missing receipts - but they land
# INSIDE the measurement window, so transform_failures moves and the level is
# reported unclean. On 2026-09-02 that made 5 of 19 runs at 500 clients
# unquotable while nothing was actually wrong with the data path.
#
# The broker-side error is also misreported: an expired read deadline comes back
# as "Invalid request" rather than a timeout, which is why the cause took hours to
# find. Fixed separately in transform/rpc/service.cc.
#
# Gate on quiescence rather than on a fixed sleep, because the ramp length scales
# with the transform count and a sleep tuned for 100 would be wrong for 1000.
wait_for_transform_failures_quiescent() {
  local settle="${1:-10}"       # consecutive stable polls required (2s apart)
  local budget="${2:-120}"      # max polls before giving up
  local min_quiet_s="${3:-20}"  # minimum ELAPSED quiet time, not just poll count
  local stable=0 prev="" cur=""
  for _ in $(seq 1 "$budget"); do
    cur=0
    local IFS_SAVE="$IFS"; IFS=','
    for u in $ADMIN_URLS; do
      IFS="$IFS_SAVE"
      local v
      v="$(curl -sf --max-time 5 "${u%/}/public_metrics" 2>/dev/null |            awk '/^redpanda_transform_failures\{/{s+=$2} END{print s+0}')"
      cur=$(awk -v a="$cur" -v b="${v:-0}" 'BEGIN{print a+b}')
      IFS=','
    done
    IFS="$IFS_SAVE"
    if [ "$cur" = "$prev" ]; then
      stable=$((stable + 1))
      # Require a MINIMUM QUIET PERIOD, not just N consecutive equal polls.
      # On 2026-09-02 this gate reported "quiescent at 609" and then failures
      # moved another 243 INSIDE the measurement window: three equal polls two
      # seconds apart is satisfied by a lull in the deploy ramp just as well as
      # by the ramp finishing. Elapsed quiet time is the property we actually
      # want, so require both.
      if [ "$stable" -ge "$settle" ] && [ $((stable * 2)) -ge "$min_quiet_s" ]; then
        if [ "$cur" != "0" ]; then
          # A non-zero baseline is not necessarily wrong - the counter is
          # cumulative across the cluster's life - but it is worth seeing,
          # because "quiescent at 609" and "quiescent at 0" are very different
          # situations and the old message made them look identical.
          echo "  transform_failures quiescent at $cur after ${min_quiet_s}s quiet (NOTE: non-zero baseline; the level is judged on the DELTA across the window, not this value)"
        else
          echo "  transform_failures quiescent at 0 after ${min_quiet_s}s quiet"
        fi
        return 0
      fi
    else
      [ -n "$prev" ] && echo "  transform_failures still moving: $prev -> $cur"
      stable=0
    fi
    prev="$cur"
    sleep 2
  done
  # Deliberately does NOT predict the verdict. This counter is the CLUSTER-WIDE
  # sum, which is the right scope for what this gate waits on - the consumer
  # ramp's retries - but the clean/unclean verdict is scoped to the matcher
  # alone (loadgen's -transform-name), so a moving sum here says nothing about
  # whether the level will be rejected. An earlier version of this message
  # claimed "the level will likely report unclean"; on 2026-09-17 it printed
  # that for two trials, one of which came back clean and the other of which
  # was rejected for transform lag, a reason unrelated to this number.
  echo "  NOTE: cluster-wide transform_failures still moving after $((budget * 2))s (last $cur)." >&2
  echo "        That is the consumer ramp, not a verdict: the level is judged on the" >&2
  echo "        MATCHER's failure delta across the window. Retries landing inside the" >&2
  echo "        window are the risk, so check the level's own unclean_reasons." >&2
  return 0
}

# wait_for_relay_subscriptions_drained [max_secs]
# Wait for the previous trial's relay subscriptions to go away before the next
# trial's consumers are deployed.
#
# Why this is needed, measured on 2026-09-17 (run 1789658612). Three identical
# trials: trials 1 and 3 ran with relay_active_subscriptions_end exactly 1500
# and relay_delivered_per_logical_push exactly 500. Trial 2 ran with 1562 and
# 562.1 - it inherited 62 live subscriptions from trial 1, did 12.4% more
# delivery work (33,725,745 against 30,000,000), and its transform lag peaked
# at 1166 against a 1000-record threshold, so the trial was rejected. Nothing
# was wrong with the code; the previous trial had not finished letting go.
#
# wait_for_relay_subscriptions cannot catch this and is not meant to: it takes
# the CONSUMER count (500) and compares it against a subscription count that is
# consumers x partitions led (1500), so it is a loose lower bound by
# construction, and a stale-inflated 1562 satisfies it just as well as a
# correct 1500. A lower bound cannot detect a number that is too HIGH.
wait_for_relay_subscriptions_drained() {
  local max_secs="${1:-120}" have="" waited=0
  while [ "$waited" -lt "$max_secs" ]; do
    have="$(curl -sf "$ADMIN_URL/public_metrics" 2>/dev/null | \
      awk '/^redpanda_relay_active_subscriptions\{/{s+=$2} END{print s+0}')"
    if [ -n "$have" ] && awk -v h="$have" 'BEGIN{exit !(h==0)}'; then
      [ "$waited" -gt 0 ] && echo "  relay subscriptions drained to 0 after ${waited}s"
      return 0
    fi
    sleep 2; waited=$((waited + 2))
  done
  # Deliberately not fatal: a trial that starts contaminated is still worth
  # running, because its throughput and receipt counts stay valid and the
  # latency gates judge it on its own lag. Say so loudly instead, so a
  # rejected trial can be attributed rather than puzzled over.
  echo "  WARNING: relay subscriptions still at ${have:-<unknown>} after ${max_secs}s," >&2
  echo "           not 0. This trial starts with the previous one's consumers" >&2
  echo "           still attached, which adds delivery work it did not ask for." >&2
  echo "           Expect inflated relay_delivered_per_logical_push and lag." >&2
  return 0
}

wait_for_relay_subscriptions() {
  local want="$1"
  local have=""
  for _ in $(seq 1 60); do
    have="$(curl -sf "$ADMIN_URL/public_metrics" 2>/dev/null | \
      awk '/^redpanda_relay_active_subscriptions\{/{print $2}')"
    if [ -n "$have" ] && awk -v h="$have" -v w="$want" 'BEGIN{exit !(h>=w)}'; then
      echo "  relay subscriptions on the pinned node: $have (>= $want wanted)"
      return
    fi
    sleep 1
  done
  echo "ERROR: pinned node never reported >= $want active relay subscriptions (last saw: ${have:-<none>})" >&2
  return 1
}

# --- clock sync -------------------------------------------------------------

# SCALING-TEST-PLAN.md P0.5: `match` and `total` each subtract a client-clock
# reading from a broker-clock reading, so they carry the full offset between the
# two hosts. Ordinary NTP is hundreds of microseconds to milliseconds off, which
# is larger than the quantity being measured. Record the residual so a
# cross-clock number is never quoted without its error bar.
# Record the network path to every broker, as the produce leg's error bar.
#
# Same role as record_clock_sync: a per-run measurement of a condition the
# result depends on but does not control. `produce` (client to quorum ack) is
# the noisiest stage in this benchmark and it moves MORE between cluster
# instantiations than the thing being measured - 666 us and 1,126 us p50 on two
# identical single-AZ topologies on 2026-09-16/17, a 460 us spread, while
# relay_consume moved 58 us. Rack distance inside the AZ is one candidate
# explanation and this is what makes it checkable: with an RTT per run, produce
# can be correlated against it across runs instead of argued about.
#
# Measured from the loadgen host, so it is the client-to-leader hop
# specifically. Broker-to-broker raft replication is the other half of produce
# and is measured at deploy time by ansible, which is the only place with
# access to all three brokers.
record_network_baseline() {
  local out="$1" brokers="${2:-$BROKERS}"
  if ! command -v ping >/dev/null 2>&1; then
    echo '{"available": false, "note": "no ping on this host"}' > "$out"
    echo "  WARNING: no ping; the produce leg has no network error bar" >&2
    return 0
  fi
  local first=1 measured=0 unreachable=""
  { printf '{"available": true, "from": "loadgen", "targets": ['
    local hostport host line
    for hostport in $(echo "$brokers" | tr ',' ' '); do
      host="${hostport%%:*}"
      # 20 packets at 0.2s: enough for a usable mdev without adding a
      # measurable delay to level setup.
      line="$(ping -n -q -c 20 -i 0.2 -w 15 "$host" 2>/dev/null | tail -1)"
      # rtt min/avg/max/mdev = 0.123/0.234/0.456/0.078 ms
      local stats; stats="$(echo "$line" | sed -n 's|.*= \([0-9.]*\)/\([0-9.]*\)/\([0-9.]*\)/\([0-9.]*\) ms.*|\1 \2 \3 \4|p')"
      # An unreachable target is recorded as such, never as a zero. Zero is a
      # legal value in these units, so substituting it turns "not measured"
      # into "measured as adjacent" - which is how a missing ICMP rule went
      # unnoticed for a whole run. See docs/METHODOLOGY.md #31.
      [ "$first" -eq 0 ] && printf ','
      first=0
      if [ -z "$stats" ]; then
        unreachable="${unreachable}${unreachable:+ }${host}"
        printf '{"host":"%s","reachable":false,"note":"ping produced no rtt summary (100%% loss)"}' "$host"
        continue
      fi
      measured=$((measured + 1))
      set -- $stats
      printf '{"host":"%s","reachable":true,"min_us":%s,"avg_us":%s,"max_us":%s,"mdev_us":%s}' \
        "$host" \
        "$(awk -v v="$1" 'BEGIN{printf "%.1f", v*1000}')" \
        "$(awk -v v="$2" 'BEGIN{printf "%.1f", v*1000}')" \
        "$(awk -v v="$3" 'BEGIN{printf "%.1f", v*1000}')" \
        "$(awk -v v="$4" 'BEGIN{printf "%.1f", v*1000}')"
    done
    printf ']}\n'
  } > "$out"
  if [ -n "$unreachable" ]; then
    echo "  WARNING: no RTT sample for $unreachable - ping saw 100% loss." >&2
    echo "           ICMP has to be permitted inside the VPC (terraform/main.tf);" >&2
    echo "           a cluster provisioned before that rule existed needs an apply." >&2
  fi
  if [ "$measured" -eq 0 ]; then
    echo "  client-to-broker RTT: NOT MEASURED (the produce leg has no network error bar)"
    return 0
  fi
  local worst
  worst="$(python3 -c "
import json
d=json.load(open('$out'))
t=[x for x in (d.get('targets') or []) if x.get('reachable')]
print(f\"{max((x['avg_us'] for x in t), default=0):.0f}\")" 2>/dev/null || echo 0)"
  echo "  client-to-broker RTT: worst avg ${worst}us over ${measured} broker(s) (the produce leg's network floor)"
}

record_clock_sync() {
  local out="$1"
  local rms="" src=""
  if command -v chronyc >/dev/null 2>&1; then
    rms="$(chronyc tracking 2>/dev/null | awk -F': *' '/RMS offset/{print $2}' | awk '{print $1}')"
    src="chronyc"
  fi
  if [ -z "$rms" ]; then
    echo '{"available": false, "note": "no chronyc on this host - cross-clock stages (match, total) have an UNKNOWN error bar and must not be quoted as microsecond results. See SCALING-TEST-PLAN.md P0.5."}' > "$out"
    echo "  WARNING: no clock-sync residual available; cross-clock stages are unquotable" >&2
    CLOCK_RMS_MICROS=0
    return 0
  fi
  local rms_us
  rms_us="$(awk -v r="$rms" 'BEGIN{printf "%.1f", r*1e6}')"
  printf '{"available": true, "source": "%s", "rms_offset_seconds": %s, "rms_offset_micros": %s}\n' \
    "$src" "$rms" "$rms_us" > "$out"
  # Exported so loadgen can tell a negative `match` caused by clock error apart
  # from one caused by the in-broker pre-commit read. Without it the two are
  # indistinguishable, and on 2026-09-02 they were in fact confused: a 227us
  # negative match was read as proof of skew while chrony had every host inside
  # 9us, and the real cause was the matcher stamping before the quorum ack.
  CLOCK_RMS_MICROS="$rms_us"
  echo "  clock-sync RMS offset: ${rms_us}us (the error bar on any cross-clock stage)"
}
