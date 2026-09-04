# HIGH-CORE-COUNT topology, added 2026-09-02 to test whether in-broker consumer
# capacity scales with core count, and to measure the NUMA penalty that scaling
# would have to survive.
#
# WHY THIS INSTANCE, and what it costs us in comparability:
#
# The measurements this is testing were all taken on r8id.8xlarge - 16 physical
# cores, of which Redpanda takes 13. The prediction in
# wasm-orderbook/bench/BATCHING-AND-CORES.md is that consumer-side guest CPU is
# per-shard and embarrassingly parallel, so client capacity should scale with
# shard count. The stated risk is NUMA: relay push() hands remote shards a
# POINTER into the producer shard's memory, so on a multi-socket box every
# far-socket delivery becomes a remote read.
#
# Testing that needs a box that actually spans sockets. On 2026-09-02, 3x
# r8id.48xlarge and 3x r8id.24xlarge had NO CAPACITY in any us-east-2 AZ
# (verified with the create-capacity-reservation probe, not a RunInstances
# stall), and r7id/m7id are not offered in this region at all. Of what was
# available with local NVMe - r8id.16xlarge (32 cores, same family, but Granite
# Rapids so probably still ONE NUMA node), r6id.32xlarge (64 cores, Ice Lake,
# which caps at 40 cores/socket so 64 cores is necessarily TWO sockets) and
# i4i.32xlarge - r6id.32xlarge is the only one that puts the NUMA question on
# the table.
#
# THE CONFOUND, and how the test controls for it: r6id is Ice Lake, r8id is
# Granite Rapids, so absolute latencies are NOT comparable across the two.
# Do not compare this cluster's crossshard_transit against the 20-25us r8id
# baseline and call the difference NUMA. Re-establish the baseline ON THIS
# INSTANCE at low fanout (5) in the same run, and compare within it.
#
# 64 physical cores vs 13 usable on the baseline is ~4.9x the shards, so the
# prediction to test is a clean ceiling near 100 x 4.9 ~= 490 clients at 5,000
# orders/sec, against 100 verified on r8id.8xlarge.
region               = "us-east-2"
broker_instance_type = "r6id.32xlarge"
broker_arch          = "x86_64"
client_instance_type = "c5n.9xlarge"
azs                  = ["us-east-2c"]
# Baked in, never left to variables.tf's defaults - see switch-topology.sh's
# header for the incident that made that non-optional.
broker_count         = 3
client_count         = 2
