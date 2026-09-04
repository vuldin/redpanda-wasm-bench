# Same broker profile as r8id-8xl.tfvars, but a single AZ instead of
# spreading across 4 - built for the wasm-transforms fan-out
# single-AZ-vs-multi-AZ comparison (deploy-wasm.yaml), not
# percore-benchmark's own single-hot-core methodology, where AZ
# placement doesn't matter.
#
# Region moved from us-east-1 to us-east-2 on 2026-07-29: us-east-1
# had a persistent, verified r8id.8xlarge capacity shortage in every
# AZ except us-east-1a (confirmed repeatedly via fast
# create-capacity-reservation probes, not just RunInstances stalls),
# which made a real multi-AZ comparison impossible there. us-east-2a
# is the multi-AZ tfvars' first AZ too, for the same broker[0]/
# client[0]/push-relay/partition-0-leader colocation reasoning this
# file always used.
#
# AZ moved from us-east-2a to us-east-2c on 2026-08-28, same reason as the
# region move above: a verified r8id.8xlarge capacity shortage. Probed with the
# same fast create-capacity-reservation technique switch-topology.sh uses -
# us-east-2a and us-east-2b both returned InsufficientInstanceCapacity for 3
# instances, us-east-2c confirmed capacity for 3x r8id.8xlarge and 2x
# c5n.9xlarge.
#
# CAVEAT this move introduces, and it is not cosmetic: the us-east-2a choice
# existed so that broker[0]/client[0] sit in the SAME AZ in both this file and
# r8id-8xl-wasm-multi-az.tfvars, which is what let a single-AZ-vs-multi-AZ
# comparison move only the other two brokers and isolate the AZ-count variable.
# Pointing this file at us-east-2c breaks that invariant. It is fine for the
# SCALING-TEST-PLAN.md phases, which are single-AZ throughput/capacity work and
# never compare the two topologies. Before running any single-vs-multi
# comparison again, either move this back to us-east-2a (re-probing capacity
# first) or move the multi-AZ file's first AZ to match - do not just run the
# comparison across a moved broker[0] and attribute the difference to AZ count.
region               = "us-east-2"
broker_instance_type = "r8id.8xlarge"
broker_arch          = "x86_64"
client_instance_type = "c5n.9xlarge"
azs                  = ["us-east-2c"]
# Explicit, not left to variables.tf's defaults (broker_count=1,
# client_count=3 - meant for percore-benchmark's own single-hot-core
# methodology, wrong for this workload). Omitting these here is
# exactly the mistake that shrank a live 3-broker cluster to 1 broker
# on 2026-07-29 - switch-topology.sh also verifies the plan's actual
# instance counts against these before applying, as a second layer.
broker_count         = 3
# 8, not 2, as of 2026-09-03. The traditional-deployment leg needs 500 EXTERNAL
# consumers, and 499 consumer groups plus loadgen on a single c5n.9xlarge
# starved the load generator: send_lateness p99 hit 2,486us against a 1,000us
# pacing interval, so the latency it reported included client-side queueing and
# could not be attributed to the architecture (METHODOLOGY 29).
#
# Measured budget on this instance type: 49 groups + loadgen held pacing
# (p99 544us); 99 groups + loadgen did not (p99 1,003us). So:
#   client0      - loadgen ONLY, nothing co-located, so pacing stays clean
#   client1      - the external matcher (cmd/wasm-client), its own instance
#   client2..7   - 499 background consumer groups, ~84 each, well under the
#                  marginal point even before removing loadgen's share
# Instance TYPE deliberately unchanged: the fanout 1/10/50 points were measured
# on c5n.9xlarge and changing it would make the 500 point incomparable.
client_count         = 8
