#!/usr/bin/env bash
# EC2 capacity probing, as a sourceable library.
#
# Extracted from switch-topology.sh so select-region.sh can reuse it rather
# than carry a second copy. One implementation - two would drift, and a
# capacity probe that disagrees with itself is worse than none.
#
# WHY A CAPACITY RESERVATION AND NOT A DRY-RUN RunInstances: a plain
# `terraform apply` has no fast-fail signal for capacity exhaustion.
# RunInstances just hangs - observed for real on 2026-07-29, three separate
# stalls of 4 to 37+ minutes across three different AZ pairs, before direct API
# checks confirmed zero instances were ever appearing. A capacity reservation is
# real admission control against the same pool RunInstances draws from, and it
# resolves in seconds either way. Ask first, cancel immediately, then apply.

# capacity_available <region> <az> <instance_type> [count]
# 0 = capacity confirmed for COUNT instances, 1 = not right now. Reason on
# stderr.
#
# COUNT matters, and defaulting it to 1 was a bug rather than a simplification.
# Observed 2026-09-16: this probe confirmed c5n.9xlarge in us-east-2c, the plan
# then asked for EIGHT, three were created, and RunInstances stalled on the
# remaining five for 11 minutes with no error - the exact failure this probe
# exists to answer fast. Capacity for one instance says nothing about capacity
# for eight.
#
# A reservation for N is all-or-nothing, which is the semantics wanted: the
# question is whether N can be placed together, not whether the pool is
# non-empty. It bills from the moment it is active, so it is cancelled
# immediately - a few seconds of the fleet is cents against a multi-minute
# stall and a half-built cluster.
capacity_available() {
  local region="$1" az="$2" itype="$3" count="${4:-1}"
  local out id
  out=$(aws ec2 create-capacity-reservation --region "$region" \
        --instance-type "$itype" --instance-platform Linux/UNIX \
        --availability-zone "$az" --instance-count "$count" \
        --instance-match-criteria open \
        --end-date-type limited \
        --end-date "$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%S)" 2>&1) || true
  id=$(printf '%s' "$out" | grep -o '"CapacityReservationId": "[^"]*"' | cut -d'"' -f4 || true)
  if [ -n "$id" ]; then
    aws ec2 cancel-capacity-reservation --region "$region" \
      --capacity-reservation-id "$id" >/dev/null 2>&1 || true
    return 0
  fi
  printf '%s' "$out" | grep -oE '"Message": "[^"]*"' >&2 || true
  return 1
}

# region_azs <region> <n>
# The first n AZ names the account can actually use in this region. Queried,
# not constructed by appending a/b/c: not every region exposes those letters,
# and an account can be opted out of individual zones.
region_azs() {
  local region="$1" n="$2"
  aws ec2 describe-availability-zones --region "$region" \
    --filters Name=state,Values=available \
    --query 'AvailabilityZones[].ZoneName' --output text 2>/dev/null \
    | tr '\t' '\n' | sort | head -n "$n"
}

# needed_pairs <az-count> <broker_count> <broker_itype> <client_count> <client_itype> <az...>
# Emits "<az> <itype> <count>" for each distinct pair a plan would need, using
# main.tf's own count.index % len(azs) subnet-assignment formula. Derived from
# the formula rather than from plan JSON, which can report availability_zone as
# unknown until apply for a resource whose subnet_id is itself only known then.
#
# The monitoring host comes in through MONITOR_ITYPE rather than a seventh
# positional argument: there are already six, two callers pass them, and an
# env var cannot be silently mis-ordered. Empty means no monitoring host, so a
# caller that predates it is unchanged.
#
# The trailing COUNT is load-bearing. This used to end in `sort -u`, which
# collapsed the list to distinct pairs and threw away how many of each the plan
# needed - so an 8-instance ask was probed as 1. See capacity_available.
needed_pairs() {
  local nazs="$1" bcount="$2" bitype="$3" ccount="$4" citype="$5"; shift 5
  local azs=("$@") i
  {
    for ((i=0; i<bcount; i++)); do echo "${azs[$((i % nazs))]} $bitype"; done
    for ((i=0; i<ccount; i++)); do echo "${azs[$((i % nazs))]} $citype"; done
    # main.tf pins the monitor to subnet_ids[0] - it does not round-robin -
    # so that its scrape traffic stays inside one AZ.
    [ -n "${MONITOR_ITYPE:-}" ] && echo "${azs[0]} $MONITOR_ITYPE"
  } | sort | uniq -c | awk '{print $2, $3, $1}'
}

# capacity_probe_region <region> <broker_count> <broker_itype> <client_count> <client_itype> <az-count>
# 0 only if EVERY needed pair has capacity. Prints per-pair results.
capacity_probe_region() {
  local region="$1" bcount="$2" bitype="$3" ccount="$4" citype="$5" nazs="$6"
  local azs; mapfile -t azs < <(region_azs "$region" "$nazs") 2>/dev/null || true
  if [ "${#azs[@]}" -lt "$nazs" ]; then
    echo "    $region: only ${#azs[@]} usable AZ(s), need $nazs" >&2
    return 1
  fi
  local fail=0 az itype n
  while read -r az itype n; do
    [ -z "$az" ] && continue
    if capacity_available "$region" "$az" "$itype" "$n"; then
      echo "    OK   ${n}x $itype in $az"
    else
      echo "    FAIL ${n}x $itype in $az"
      fail=1
    fi
  done < <(needed_pairs "$nazs" "$bcount" "$bitype" "$ccount" "$citype" "${azs[@]}")
  return "$fail"
}

# tfvar <file> <name> - read a scalar from a tfvars file
tfvar() {
  grep -oP "^\\s*$2\\s*=\\s*\"?\\K[^\"]+" "$1" 2>/dev/null | head -1 | tr -d ' '
}
# tfvar_list_len <file> <name> - length of a tfvars list like azs = ["a","b"]
tfvar_list_len() {
  local raw; raw=$(grep -oP "^\\s*$2\\s*=\\s*\\K\\[.*\\]" "$1" 2>/dev/null | head -1)
  [ -z "$raw" ] && { echo 0; return; }
  printf '%s' "$raw" | tr ',' '\n' | grep -c '"'
}
