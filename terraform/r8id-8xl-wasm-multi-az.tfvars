# Same broker profile as r8id-8xl.tfvars, but self-contained for the
# wasm-transforms fan-out single-AZ-vs-multi-AZ comparison
# (deploy-wasm.yaml) specifically - counts baked in here rather than
# left to variables.tf's defaults (broker_count=1, client_count=3,
# which are right for percore-benchmark's own single-hot-core
# methodology and wrong for this workload). See
# r8id-8xl-wasm-single-az.tfvars for the counterpart single-AZ file -
# broker[0]/client[0] both land in the first AZ under either file, so
# switching between them only moves the OTHER two brokers.
region               = "us-east-2"
broker_instance_type = "r8id.8xlarge"
broker_arch          = "x86_64"
client_instance_type = "c5n.9xlarge"
# Moved off us-east-1 entirely on 2026-07-29 - after burning real
# time on THREE separate multi-AZ attempts there (1a+1c+1d, then
# 1a+1c+1f twice), a fast create-capacity-reservation probe (seconds,
# not a 4-37+ minute RunInstances stall) confirmed every AZ except
# us-east-1a itself was out of r8id.8xlarge capacity, region-wide, not
# just for the specific pairs tried - no AZ combination in us-east-1
# would have worked. The same probe against us-east-2 initially found
# only 2a/2c with capacity (2b briefly out), but capacity fluctuates -
# a re-probe minutes later found all 3 (2a/2b/2c) available, so this
# is the ideal genuine 3-distinct-AZ spread, one broker per AZ.
# switch-topology.sh now runs this same probe automatically before
# every apply, so a future capacity shift here gets caught in seconds,
# not by repeating this investigation.
azs                  = ["us-east-2a", "us-east-2b", "us-east-2c"]
broker_count         = 3
client_count         = 2
