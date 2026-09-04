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

# capacity_available <region> <az> <instance_type>
# 0 = capacity confirmed, 1 = none right now. Reason on stderr.
capacity_available() {
  local region="$1" az="$2" itype="$3"
  local out id
  out=$(aws ec2 create-capacity-reservation --region "$region" \
        --instance-type "$itype" --instance-platform Linux/UNIX \
        --availability-zone "$az" --instance-count 1 \
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
# Emits the distinct "<az> <itype>" pairs a plan would actually need, using
# main.tf's own count.index % len(azs) subnet-assignment formula. Derived from
# the formula rather than from plan JSON, which can report availability_zone as
# unknown until apply for a resource whose subnet_id is itself only known then.
needed_pairs() {
  local nazs="$1" bcount="$2" bitype="$3" ccount="$4" citype="$5"; shift 5
  local azs=("$@") i
  {
    for ((i=0; i<bcount; i++)); do echo "${azs[$((i % nazs))]} $bitype"; done
    for ((i=0; i<ccount; i++)); do echo "${azs[$((i % nazs))]} $citype"; done
  } | sort -u
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
  local fail=0 pair az itype
  while read -r az itype; do
    [ -z "$az" ] && continue
    if capacity_available "$region" "$az" "$itype"; then
      echo "    OK   $itype in $az"
    else
      echo "    FAIL $itype in $az"
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
