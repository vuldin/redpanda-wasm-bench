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
# A 5th argument is a placement-group ARN to reserve INSIDE. That matters:
# putting the brokers in a cluster placement group makes AWS find adjacent
# capacity, which is strictly harder than finding capacity anywhere in the AZ.
# Probing without it would answer an easier question than the apply asks and go
# back to passing while RunInstances stalls - the exact failure the count fix
# addressed. create-capacity-reservation takes --placement-group-arn for this.
capacity_available() {
  local region="$1" az="$2" itype="$3" count="${4:-1}" pg_arn="${5:-}"
  local out id
  # Build the optional flag as ARRAY ELEMENTS. `${pg_arn:+--flag "$pg_arn"}`
  # looks right and is not: the quotes inside a :+ expansion are literal
  # characters, so the ARN reaches the CLI wrapped in quote marks, the request
  # is rejected, and the refusal is indistinguishable from "no capacity". That
  # made a placement-group probe report NO while the same call by hand
  # succeeded - a false negative, which for a capacity probe means refusing to
  # provision a cluster that would have been fine.
  local pg_args=()
  [ -n "$pg_arn" ] && pg_args=(--placement-group-arn "$pg_arn")
  out=$(aws ec2 create-capacity-reservation --region "$region" \
        --instance-type "$itype" --instance-platform Linux/UNIX \
        --availability-zone "$az" --instance-count "$count" \
        --instance-match-criteria open \
        "${pg_args[@]}" \
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

# temp_placement_group <region> <strategy>
# Creates a throwaway placement group, echoes its ARN, and leaves deleting it
# to the caller via delete_placement_group.
#
# Needed because the probe runs BEFORE terraform creates the real group, so
# there is nothing to reserve inside yet. Placement groups cost nothing and are
# instant, so making one to ask the question and removing it afterwards is
# cheaper than discovering the answer during a 12-instance apply.
temp_placement_group() {
  local region="$1" strategy="${2:-cluster}"
  local name="rp-bench-probe-$$-$(date +%s)"
  aws ec2 create-placement-group --region "$region" \
    --group-name "$name" --strategy "$strategy" \
    --query 'PlacementGroup.GroupArn' --output text 2>/dev/null || return 1
}

delete_placement_group() {  # <region> <arn-or-name>
  local region="$1" ref="$2"
  local name="${ref##*/}"
  [ -n "$name" ] || return 0
  # Retried, because cancel-capacity-reservation returns before the
  # reservation has actually let go of the group: a delete issued straight
  # after the cancel fails with the group still in use. The first version
  # swallowed that with `|| true` and leaked six groups across three regions
  # in one run before anyone looked. They cost nothing, but they count
  # against the per-region placement-group limit, and a cleanup that cannot
  # fail out loud is not a cleanup.
  local err="" i
  for i in 1 2 3 4 5 6; do
    if err="$(aws ec2 delete-placement-group --region "$region" \
                --group-name "$name" 2>&1)"; then
      return 0
    fi
    sleep 5
  done
  echo "    WARNING: could not delete probe placement group $name in $region" >&2
  echo "             after 30s: ${err##*: }" >&2
  echo "             Sweep leftovers with: wb verify-clean" >&2
  return 0
}

# sweep_probe_placement_groups <region>
# Removes probe groups an earlier run leaked. Named separately from
# delete_placement_group because it deletes by PATTERN rather than by a
# reference the caller is holding - the leaked ones are exactly those whose
# caller is gone.
sweep_probe_placement_groups() {
  local region="$1" name found=0
  while read -r name; do
    [ -z "$name" ] && continue
    if aws ec2 delete-placement-group --region "$region" \
         --group-name "$name" >/dev/null 2>&1; then
      echo "    removed stale probe placement group $name in $region"
      found=$((found + 1))
    fi
  done < <(aws ec2 describe-placement-groups --region "$region" \
             --query 'PlacementGroups[?starts_with(GroupName, `rp-bench-probe-`)].GroupName' \
             --output text 2>/dev/null | tr '\t' '\n')
  return 0
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

# capacity_probe_azs <region> <bcount> <bitype> <ccount> <citype> <nazs> <az...>
# 0 only if EVERY needed (az, itype, count) triple has capacity. Prints
# per-triple results.
#
# THE loop. select-region.sh used to carry its own copy because it needs to
# probe a specific AZ list rather than whatever region_azs returns, and that
# third copy is what made a change to needed_pairs' output format fail
# silently: the copy still read two fields from a three-field line, so the
# count landed inside the instance type and every reservation was refused for
# a type that does not exist. Reported as "no capacity" in a region that had
# plenty. One loop, both callers.
capacity_probe_azs() {
  local region="$1" bcount="$2" bitype="$3" ccount="$4" citype="$5" nazs="$6"
  shift 6
  local azs=("$@")

  # When the brokers will be placed in a cluster group, probe the BROKER type
  # inside a throwaway group of the same strategy. BROKER_PLACEMENT_GROUP is
  # read from the environment so the two callers need no new argument.
  local pg_arn=""
  if [ "${BROKER_PLACEMENT_GROUP:-false}" = "true" ] && [ "$nazs" -eq 1 ]; then
    pg_arn="$(temp_placement_group "$region" cluster)" || pg_arn=""
    if [ -n "$pg_arn" ]; then
      echo "    (probing $bitype inside a throwaway cluster placement group)"
    else
      echo "    WARNING: could not create a probe placement group; the broker" >&2
      echo "             capacity answer below is for unplaced capacity and is" >&2
      echo "             therefore optimistic." >&2
    fi
  fi

  local fail=0 az itype n this_pg
  while read -r az itype n; do
    [ -z "$az" ] && continue
    # Only the brokers are placed, so only they are probed inside the group.
    this_pg=""
    [ "$itype" = "$bitype" ] && this_pg="$pg_arn"
    if capacity_available "$region" "$az" "$itype" "$n" "$this_pg"; then
      echo "    OK   ${n}x $itype in $az${this_pg:+ (placement group)}"
    else
      echo "    FAIL ${n}x $itype in $az${this_pg:+ (placement group)}"
      fail=1
    fi
  done < <(needed_pairs "$nazs" "$bcount" "$bitype" "$ccount" "$citype" "${azs[@]}")

  [ -n "$pg_arn" ] && delete_placement_group "$region" "$pg_arn"
  return "$fail"
}

# capacity_probe_region <region> <broker_count> <broker_itype> <client_count> <client_itype> <az-count>
# 0 only if EVERY needed pair has capacity. Picks the AZs itself; use
# capacity_probe_azs when the caller already knows which AZs it wants.
capacity_probe_region() {
  local region="$1" bcount="$2" bitype="$3" ccount="$4" citype="$5" nazs="$6"
  local azs; mapfile -t azs < <(region_azs "$region" "$nazs") 2>/dev/null || true
  if [ "${#azs[@]}" -lt "$nazs" ]; then
    echo "    $region: only ${#azs[@]} usable AZ(s), need $nazs" >&2
    return 1
  fi
  capacity_probe_azs "$region" "$bcount" "$bitype" "$ccount" "$citype" \
                     "$nazs" "${azs[@]}"
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
