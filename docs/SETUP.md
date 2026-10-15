# Setup: prerequisites, building, cost and configuration

What you need before running anything, in more detail than the [README](../README.md).
For running tests unattended, see [RUNNING-YOUR-OWN-TESTS.md](RUNNING-YOUR-OWN-TESTS.md).

## Cost

A full reproduction provisions 3 brokers, 8 client instances and a
monitoring host:

| | instance | count | ~on-demand |
|---|---|---|---|
| brokers | `r8id.8xlarge` | 3 | ~$24/hr |
| clients | `c5n.9xlarge` | 8 | ~$16/hr |
| monitoring | `m6i.2xlarge` | 1 | ~$0.40/hr |
| | | | ~$40/hr |

A full run takes 1-2 hours including provisioning and teardown, so budget
$40-80. Teardown is not automatic for `wb reproduce` - see
[Teardown](MANUAL-RUNS.md#teardown). Plan runs through `scripts/autorun.sh` do tear down
(and verify) on their own, with a watchdog at a hard deadline.

The monitoring host is about 1% of the bill. If you want it gone anyway, set
`monitor_count = 0` and nothing else changes.

## Prerequisites

| tool | why |
|---|---|
| `aws` CLI, authenticated | provisioning, capacity probing, teardown verification. SSO login is interactive, so do it yourself before starting |
| `terraform` | provisioning |
| `ansible` | deploying the broker binary and the client tools |
| `go` 1.25+ | builds `loadgen` and `fanout-load` |
| `jq`, `python3` | result parsing and the capacity probe |
| `ssh` keypair | the public half must match `public_key_path`; the private half is how every script reaches the instances |

### A redpanda binary built from the branch

The in-broker arm needs a broker built from the
`transform-latency-instrumentation` branch. The relay and its instrumentation
don't exist in a released Redpanda.

```bash
git clone https://github.com/vuldin/redpanda.git
cd redpanda && git checkout transform-latency-instrumentation
bazel build //src/v/redpanda:redpanda -c opt --jobs=6 --local_resources=memory=16384
```

Don't drop the `--jobs` and `--local_resources` caps. An unconstrained
`bazel build` on this tree consumed enough memory to require a power cycle.
Expect 1-2 hours cold. Every commit on that branch is verified to build
standalone (`scripts/verify-each-commit.sh`), so you can build an earlier
commit if you only want part of the feature set.

There's no prebuilt binary since the branch is changing rapidly.

### The wasm guests

```bash
git clone https://github.com/vuldin/redpanda-wasm-clients.git
cd redpanda-wasm-clients && make
make checksum   # record this, a result is only comparable to another that ran the same module
```

You'll need a Rust toolchain with `rustup target add wasm32-wasip1`, and
network access on the first build since the transform SDK is pulled from git
rather than crates.io.

### AWS capacity

The instance types should remain untouched if you want comparable numbers. The
published results are on `r8id.8xlarge` brokers and `c5n.9xlarge` clients, and
the same instance name on a different hardware generation isn't the same
machine. Capacity for them isn't guaranteed in any region, so
`scripts/select-region.sh` probes and falls back automatically.

## Configuration

| knob | default | notes |
|---|---|---|
| `RATE` | `1000` | orders/sec. Each order is a crossing pair, so 2 records/sec per order |
| `RF` | `3` | replication factor |
| `PAYLOAD` | `650` | bytes per order |
| `ACKS` | `all` | changing this changes what's being measured, not just the speed |
| `INBROKER_CONSUMERS` | `500` | in-broker relay consumers for the wasm arm |
| `EXTERNAL_LADDER` | `1 10 50 500` | external consumer counts for the traditional arm |
| `DURATION` / `WARMUP` / `DRAIN` | `30s` / `10s` / `30s` | |
| `broker_placement_group` | `true` | brokers in a cluster placement group, so rack distance stops being a variable in the produce leg. Single-AZ only, and capacity is harder to find - the probe asks inside a throwaway group so a refusal is still fast |
| `REGIONS` | `us-east-1 us-east-2 us-west-2 eu-west-1` | tried in order. `us-east-1` first because that's where the published numbers came from, and region is part of a result's fingerprint |
| `GROUPS_PER_INSTANCE` | `84` | measured safe, see `scripts/size-clients.sh` for why not 99 |

### Region selection

`select-region.sh` walks `REGIONS` and, for each, asks EC2
for a real capacity reservation for every `(AZ, instance type)` pair the plan
needs, then cancels it. It takes the first region where all pairs succeed.
This exists because a plain `terraform apply` has no fast-fail for capacity
exhaustion. It also warns if the chosen region isn't the one the
published numbers came from, since that affects comparability.

### Client-fleet sizing

`size-clients.sh` derives `client_count` from the
consumer ladder, giving loadgen an instance to itself and sharding background
consumer groups at 84 per instance. Under-provisioning here doesn't fail
loudly. It produces a plausible latency that's really the load generator
starved of CPU. See the script header for the measurement that fixed the
number at 84.
