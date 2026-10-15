# Six brokers, single AZ: a common production exchange topology.
#
# Why this file exists rather than a -var override on single-az: both
# switch-topology.sh's capacity probe and its post-plan instance-count check
# read broker_count out of the tfvars, so overriding on the command line
# would have the probe ask about 3 brokers while the apply builds 6 - the
# exact probe/apply disagreement that file's own header documents as the
# dangerous direction. A separate topology keeps the two in agreement.
#
# The point of six: production exchange deployments commonly run a single-AZ
# six-broker cluster, and comparing against those figures with a different
# topology invites the obvious objection. Note the consequence for
# comparability in the other direction: every published figure in REPRODUCE.md
# came from THREE brokers, so a number from this topology is not
# interchangeable with one from those, and a run that wants to reproduce a
# published figure must use single-az instead.
#
# Region and AZ are the fallback only - select-region.sh writes
# region.auto.tfvars and that overrides both, after probing for capacity.
# r8id.8xlarge capacity has been short in specific AZs repeatedly (see the
# single-az file's header for the history), and asking for six rather than
# three roughly doubles the chance of a refusal, so expect the probe to move
# regions more often with this topology than with single-az.
region               = "us-east-1"
broker_instance_type = "r8id.8xlarge"
broker_arch          = "x86_64"
client_instance_type = "c5n.9xlarge"
azs                  = ["us-east-1a"]
broker_count         = 6

# THREE clients, not the eight single-az uses.
#
# Eight exists there to host 499 background consumer groups without starving
# the load generator (METHODOLOGY 29). This topology's consumer ladders top
# out around 50, and the measured budget on this instance type is that 49
# groups plus loadgen still held pacing at p99 544us - so three hosts cover
# it with margin:
#   client0  - loadgen ONLY, nothing co-located, so pacing stays clean
#   client1  - the external matcher, its own instance (unused by the
#              co-located arm, which runs the matcher ON broker[0] instead)
#   client2  - background consumer groups
#
# Instance TYPE deliberately unchanged from single-az: the fanout points were
# measured on c5n.9xlarge and changing it would make them incomparable.
# size-clients.sh may raise this from EXTERNAL_LADDER; it will not lower it.
client_count = 3
