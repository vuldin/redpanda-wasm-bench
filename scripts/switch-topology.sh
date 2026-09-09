#!/usr/bin/env bash
# Switches the wasm-transforms fan-out cluster between multi-AZ and
# single-AZ topology (or (re)provisions it fresh in either one).
#
# Exists because a plain `terraform apply -var-file=...` against this
# project is NOT safe to run directly, for two independent reasons
# discovered the hard way on 2026-07-29:
#
#   1. variables.tf defaults broker_count=1 and client_count=3 (right
#      for percore-benchmark's own single-hot-core methodology, wrong
#      for this workload) - forgetting to override both on every
#      apply silently resizes the cluster instead of just moving it.
#      Guarded against two ways: both tfvars files below bake the
#      counts in (nothing left to forget on the command line), and
#      this script still re-verifies the PLANNED counts against those
#      same tfvars before ever applying, in case a future tfvars edit
#      or stray -var flag reintroduces the drift.
#
#   2. Moving a broker to a different AZ means terraform destroying
#      and recreating its EC2 instance - and Redpanda's own cluster
#      membership doesn't know that's a planned move, not a failure.
#      If terraform destroys 2 of 3 Raft-controller voters without
#      the cluster ever being told they're leaving for good, the
#      surviving voter can't reach quorum (needs 2 of 3) to elect a
#      controller leader, which means it can't even process a
#      decommission request afterward - the cluster gets stuck and
#      the only way out ended up being wiping all 3 brokers' data and
#      re-bootstrapping from nothing. Guarded against by cleanly
#      decommissioning every broker this apply would destroy BEFORE
#      terraform ever touches its instance, one at a time, confirming
#      cluster health after each - so the Raft group's own view of
#      membership always matches reality, and it never needs quorum
#      from a member that's already gone.
set -euo pipefail

# shellcheck source=lib-capacity.sh
. "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/lib-capacity.sh"
# terraform/ is a sibling of scripts/, not a child of it. Fourth instance of
# this same bug from moving these scripts off the repo root - resolve from
# the script's absolute path so it does not depend on the caller's cwd.
cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../terraform"

TOPOLOGY="${1:?usage: switch-topology.sh <multi-az|single-az|highcore>}"
case "$TOPOLOGY" in
  multi-az)  TFVARS="r8id-8xl-wasm-multi-az.tfvars" ;;
  single-az) TFVARS="r8id-8xl-wasm-single-az.tfvars" ;;
  # High-core-count box for the consumer-scaling / NUMA test. Different instance
  # FAMILY as well as size, so absolute latencies are not comparable with the
  # r8id topologies - the tfvars header says how to control for that.
  highcore)  TFVARS="r6id-32xl-wasm-highcore.tfvars" ;;
  *) echo "unknown topology '$TOPOLOGY' - want multi-az, single-az or highcore" >&2; exit 1 ;;
esac

# The *.auto.tfvars overlays (region.auto.tfvars from select-region.sh,
# clients.auto.tfvars from size-clients.sh) have to be passed EXPLICITLY, and
# after $TFVARS, or they do nothing.
#
# Terraform's precedence is: *.auto.tfvars first, then any -var-file given on
# the command line, then -var. So an explicit `-var-file=$TFVARS` OVERRIDES an
# auto-loaded overlay - the opposite of what those files' own headers used to
# claim. The effect was silent: both overlays were dead weight, and only looked
# like they worked because the topology file happened to name the same region,
# AZ and counts. Observed 2026-09-09, when clients.auto.tfvars asked for 1
# client and terraform provisioned 8.
#
# That mattered most for region fallback, which is select-region.sh's entire
# purpose: had it fallen back to another region, the overlay would have been
# ignored, terraform would have provisioned in the ORIGINAL region, and the
# run would have been labelled with a region it did not use.
OVERLAY_ARGS=()
for overlay in region.auto.tfvars clients.auto.tfvars; do
  [ -f "$overlay" ] && OVERLAY_ARGS+=(-var-file="$overlay")
done

# The expectation the plan is checked against must come from the SAME merged
# view terraform will use, or a legitimate overlay looks like a mismatch and
# the plan is rejected. Last file that defines the variable wins, matching
# terraform's own ordering.
merged_var() {
  local name="$1" val="" f
  for f in "$TFVARS" region.auto.tfvars clients.auto.tfvars; do
    [ -f "$f" ] || continue
    local got
    got=$(grep -oP "^${name}\s*=\s*\K[0-9]+" "$f" | tail -1)
    [ -n "$got" ] && val="$got"
  done
  echo "$val"
}
EXPECT_BROKERS=$(merged_var broker_count)
EXPECT_CLIENTS=$(merged_var client_count)
PUBKEY="${BENCH_PUB_KEY:-$HOME/.ssh/id_rsa.pub}"
PLAN_FILE=/tmp/topology-switch.plan

echo "=== planning $TOPOLOGY ($TFVARS${OVERLAY_ARGS[*]:+ + overlays}, expecting $EXPECT_BROKERS brokers / $EXPECT_CLIENTS clients) ==="
terraform plan -var-file="$TFVARS" "${OVERLAY_ARGS[@]}" \
  -var="public_key_path=$PUBKEY" -out="$PLAN_FILE"

echo "=== verifying the plan actually results in that many instances before applying ==="
read -r GOT_BROKERS GOT_CLIENTS < <(terraform show -json "$PLAN_FILE" | python3 -c "
import json, sys
resources = json.load(sys.stdin).get('planned_values', {}).get('root_module', {}).get('resources', [])
brokers = sum(1 for r in resources if r['address'].startswith('aws_instance.broker'))
clients = sum(1 for r in resources if r['address'].startswith('aws_instance.client'))
print(brokers, clients)
")

if [ "$GOT_BROKERS" != "$EXPECT_BROKERS" ] || [ "$GOT_CLIENTS" != "$EXPECT_CLIENTS" ]; then
  echo "ABORT: this plan would leave $GOT_BROKERS broker(s) / $GOT_CLIENTS client(s)," >&2
  echo "       but $TFVARS expects $EXPECT_BROKERS / $EXPECT_CLIENTS. Not applying -" >&2
  echo "       this is exactly the mistake this script exists to catch." >&2
  exit 1
fi
echo "OK: plan matches expected topology ($GOT_BROKERS brokers / $GOT_CLIENTS clients)"

# The count check above reads planned_values, which is the state AFTER apply -
# so a plan that DESTROYS and RECREATES all three brokers still shows three
# brokers and passes it. That is not a hypothetical: adding client instances to
# a live cluster (2026-09-03) meant planning against a tfvars file, and picking
# the wrong one of the three would have replaced the brokers, taking with them
# the redpanda build under test and hours of measurement.
#
# So inspect resource_changes, which records the ACTION per resource, and refuse
# anything that deletes or replaces an existing instance unless explicitly
# allowed. ALLOW_REPLACE=1 for a deliberate topology switch, where replacing
# instances is the entire point.
echo "=== checking the plan does not replace or destroy existing instances ==="
# Only aws_* resources count. local_file.inventory is rewritten as
# delete+create on every change to the host list, which is exactly what ADDING
# instances does - so counting it made the guard fire on the one operation it
# was written to permit. Caught on its first real use, 2026-09-03, adding six
# clients: the guard would have refused a plan whose only destructive action
# was regenerating a local text file.
DESTRUCTIVE="$(terraform show -json "$PLAN_FILE" | python3 -c "
import json, sys
out = []
for c in json.load(sys.stdin).get('resource_changes', []):
    if not c['address'].startswith('aws_'):
        continue
    acts = c.get('change', {}).get('actions', [])
    if 'delete' in acts:
        out.append('%s -> %s' % (c['address'], '+'.join(acts)))
print('\n'.join(out))
")"
if [ -n "$DESTRUCTIVE" ]; then
  if [ "${ALLOW_REPLACE:-0}" = "1" ]; then
    echo "WARNING: plan destroys/replaces the following, and ALLOW_REPLACE=1 was set:" >&2
    echo "$DESTRUCTIVE" | sed 's/^/    /' >&2
  else
    echo "ABORT: this plan would destroy or replace existing resources:" >&2
    echo "$DESTRUCTIVE" | sed 's/^/    /' >&2
    echo "" >&2
    echo "  If you are ADDING capacity to a live cluster, this is a wrong-tfvars" >&2
    echo "  mistake - check which file matches the running instances first:" >&2
    echo "      terraform -chdir=terraform show -json | grep instance_type" >&2
    echo "  If you INTEND a topology switch, re-run with ALLOW_REPLACE=1." >&2
    exit 1
  fi
else
  echo "OK: no existing resource is destroyed or replaced"
fi

echo "=== checking real EC2 capacity for every instance this plan would create, before touching anything ==="
# Exists because a plain `terraform apply` has no fast-fail signal for AWS
# capacity exhaustion - RunInstances just hangs (seen for real 2026-07-29:
# three separate stalls, 4-37+ minutes each, across three different AZ
# pairs, before finally confirming via direct AWS API checks that zero new
# instances were ever appearing). A capacity reservation is a real
# admission-control check against the same capacity pool RunInstances
# draws from, but resolves in seconds instead of minutes whether it
# succeeds or fails - so we ask it first, for every (AZ, instance type)
# pair the plan would actually need, and cancel the reservation
# immediately either way. Computed directly from count.index % len(azs)
# (main.tf's own subnet-assignment formula), not by trusting the plan
# JSON's availability_zone field, which Terraform may report as unknown
# until apply for a resource whose subnet_id is itself only known then.
AZS_RAW=$(grep -oP '^azs\s*=\s*\K\[.*\]' "$TFVARS")
BROKER_ITYPE=$(grep -oP '^broker_instance_type\s*=\s*"\K[^"]+' "$TFVARS")
CLIENT_ITYPE=$(grep -oP '^client_instance_type\s*=\s*"\K[^"]+' "$TFVARS")
TF_REGION=$(grep -oP '^region\s*=\s*"\K[^"]+' "$TFVARS")
CAPACITY_CHECK=$(terraform show -json "$PLAN_FILE" | AZS_RAW="$AZS_RAW" BROKER_ITYPE="$BROKER_ITYPE" CLIENT_ITYPE="$CLIENT_ITYPE" python3 -c "
import json, os, re, sys
plan = json.load(sys.stdin)
azs = json.loads(os.environ['AZS_RAW'].replace(chr(39), '\"'))
itypes = {'broker': os.environ['BROKER_ITYPE'], 'client': os.environ['CLIENT_ITYPE']}
needed = set()
for rc in plan.get('resource_changes', []):
    m = re.match(r'aws_instance\.(broker|client)\[(\d+)\]\$', rc['address'])
    if m and 'create' in rc['change']['actions']:
        kind, idx = m.group(1), int(m.group(2))
        needed.add((azs[idx % len(azs)], itypes[kind]))
for az, itype in sorted(needed):
    print(az, itype)
")
if [ -z "$CAPACITY_CHECK" ]; then
  echo "OK: this plan creates no new instances - nothing to capacity-check"
else
  # capacity_available lives in lib-capacity.sh, sourced above, so this script
  # and select-region.sh ask EC2 the same question the same way. It used to be
  # duplicated here; two probes that can disagree are worse than one.
  CAPACITY_FAIL=0
  while read -r az itype; do
    [ -z "$az" ] && continue
    echo "--- checking $itype in $az ---"
    if capacity_available "$TF_REGION" "$az" "$itype"; then
      echo "    OK: capacity confirmed"
    else
      echo "    FAIL: no capacity for $itype in $az right now" >&2
      CAPACITY_FAIL=1
    fi
  done <<< "$CAPACITY_CHECK"
  if [ "$CAPACITY_FAIL" != "0" ]; then
    echo "ABORT: at least one (AZ, instance type) pair this plan needs has no capacity right now." >&2
    echo "       Not applying - nothing has been touched (no decommission, no destroy). Try a" >&2
    echo "       different AZ (re-check with the same technique before committing) or wait and" >&2
    echo "       retry; do not just re-run terraform apply and hope, that's exactly the" >&2
    echo "       multi-minute-stall mistake this check exists to avoid." >&2
    exit 1
  fi
  echo "OK: capacity confirmed for every instance this plan would create"
fi

echo "=== checking whether this plan destroys any broker instance ==="
REPLACED_BROKER_IDS=$(terraform show -json "$PLAN_FILE" | python3 -c "
import json, re, sys
plan = json.load(sys.stdin)
ids = []
for rc in plan.get('resource_changes', []):
    m = re.match(r'aws_instance\.broker\[(\d+)\]\$', rc['address'])
    if m and 'delete' in rc['change']['actions']:
        ids.append(int(m.group(1)))
print(' '.join(str(i) for i in sorted(ids)))
")

if [ -n "$REPLACED_BROKER_IDS" ]; then
  SEED=$(terraform output -json broker_private_ips | python3 -c "import json,sys; print(json.load(sys.stdin)[0])")
  echo "=== broker(s) [$REPLACED_BROKER_IDS] will be destroyed - decommissioning them from the live cluster first ==="

  health=$(ssh -i "${BENCH_SSH_KEY:-$HOME/.ssh/id_rsa}" -o StrictHostKeyChecking=no "ubuntu@$(terraform output -json broker_public_ips | python3 -c "import json,sys; print(json.load(sys.stdin)[0])")" \
    "curl -sf http://$SEED:9644/v1/cluster/health_overview" 2>/dev/null || echo '{}')
  echo "$health" | grep -q '"is_healthy":[[:space:]]*true' || {
    echo "ABORT: cluster is not currently healthy - fix that first (see" >&2
    echo "       ensure-cluster-ready.sh) before switching topology. A" >&2
    echo "       decommission needs a healthy quorum to even be processed;" >&2
    echo "       running it against an already-degraded cluster is exactly" >&2
    echo "       how this got stuck before." >&2
    echo "       health_overview: $health" >&2
    exit 1
  }

  for id in $REPLACED_BROKER_IDS; do
    echo "--- decommissioning broker id $id ---"
    ssh -i "${BENCH_SSH_KEY:-$HOME/.ssh/id_rsa}" -o StrictHostKeyChecking=no "ubuntu@$(terraform output -json broker_public_ips | python3 -c "import json,sys; print(json.load(sys.stdin)[0])")" \
      "/opt/redpanda/bin/rpk redpanda admin brokers decommission $id --hosts $SEED:9644" 2>&1 || true
    for i in $(seq 1 30); do
      still_present=$(ssh -i "${BENCH_SSH_KEY:-$HOME/.ssh/id_rsa}" -o StrictHostKeyChecking=no "ubuntu@$(terraform output -json broker_public_ips | python3 -c "import json,sys; print(json.load(sys.stdin)[0])")" \
        "/opt/redpanda/bin/rpk redpanda admin brokers list --hosts $SEED:9644 2>/dev/null" | awk -v id="$id" '$1==id{print}')
      [ -z "$still_present" ] && break
      sleep 3
    done
    if [ -n "$still_present" ]; then
      echo "ABORT: broker $id did not leave the cluster after decommissioning - not safe to destroy its instance." >&2
      echo "" >&2
      echo "Known edge case (hit for real 2026-07-29): decommissioning a" >&2
      echo "broker that holds zero partitions (an idle/just-cleaned-up" >&2
      echo "test cluster) can get stuck at membership_status=draining" >&2
      echo "indefinitely, with 'rpk cluster partitions move-status'" >&2
      echo "showing no ongoing movements and re-decommission requests" >&2
      echo "failing with 'invalid state transition requested'. If you've" >&2
      echo "confirmed via 'rpk cluster health' that the cluster is" >&2
      echo "otherwise healthy and you don't need continuity (no real" >&2
      echo "data on this cluster worth preserving), the working fallback" >&2
      echo "is: apply the saved plan directly (terraform apply $PLAN_FILE)," >&2
      echo "then stop redpanda and wipe /var/lib/redpanda/data on ALL" >&2
      echo "brokers (including ones NOT being replaced) and let the" >&2
      echo "ansible playbook re-bootstrap the cluster from scratch - the" >&2
      echo "same recovery this project used earlier the same day for an" >&2
      echo "unrelated quorum-loss incident." >&2
      exit 1
    fi
    echo "OK: broker $id cleanly decommissioned"
  done
  echo "=== all to-be-destroyed brokers cleanly decommissioned - the cluster's own Raft config now matches what's about to happen ==="
fi

echo "=== applying ==="
terraform apply "$PLAN_FILE"

echo ""
echo "=== topology is now $TOPOLOGY - run ./deploy-wasm-cluster.sh next ==="
