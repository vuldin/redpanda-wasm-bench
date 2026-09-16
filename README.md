# redpanda-wasm-bench

A benchmark harness that compares in-broker wasm transforms against a
traditional external Kafka deployment on AWS.

Both arms run the byte-identical wasm module from
[redpanda-wasm-clients](https://github.com/vuldin/redpanda-wasm-clients). The
only variable is where it executes: inside the broker as a data transform, or
on its own instance as an ordinary Kafka client. Keeping the module identical
makes the result a statement about execution location rather than about two
different implementations.

At 500 consumers and 1,000 orders/sec, in-broker end-to-end p50 was 958 us
against 13,057 us for the same work done externally, so roughly an order of
magnitude. Full tables and the commands that produced them are in
[REPRODUCE.md](REPRODUCE.md).

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
$40-80. Teardown is not automatic. See [Teardown](#teardown).

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

## Quick start

```bash
# everything: prerequisites, client-fleet sizing, region selection,
# provisioning, monitoring, deploy, both arms, tables
REDPANDA_BIN=/path/to/bazel-bin/src/v/redpanda/redpanda \
WASM_DIR=/path/to/redpanda-wasm-clients/bin \
  ./scripts/wb reproduce

# the Grafana URL, once anything is provisioned
./scripts/wb monitor

# tear down, and verify it
./scripts/wb teardown --yes && ./scripts/wb verify-clean
```

`wb reproduce` runs `size-clients.sh`, `select-region.sh`, `wb up` and
`wb deploy` itself, so you don't run those separately. Doing both is worse
than redundant: `select-region.sh` rewrites
`terraform/region.auto.tfvars`, so if capacity moved between your
`wb up` and `reproduce`'s own region selection, the second apply targets a
different region than the one holding your instances.

To drive the steps yourself, or to re-run one arm against a cluster that is
already up:

```bash
EXTERNAL_LADDER="1 10 50 500" ./scripts/size-clients.sh
REGIONS="us-east-1 us-east-2 us-west-2" ./scripts/select-region.sh
./scripts/wb up          # capacity probe, provision, then the monitoring stack
./scripts/wb deploy      # broker binary + client tools
SKIP_PROVISION=true ./scripts/wb reproduce
```

## Dashboards

`wb up` brings up Prometheus and Grafana on their own instance before the
broker binary is deployed and before anything is measured, so you can watch a
run rather than read about it afterwards.

```bash
./scripts/wb monitor              # the URL, plus whether it's reachable
./scripts/wb monitor deploy       # redeploy the stack
./scripts/wb monitor dashboards   # regenerate and push panels, no restart
./scripts/wb monitor check        # validate every panel query against Prometheus
```

Grafana opens on anonymous viewer access, so the URL works without a login.
Use `admin` / `wasmbench` if you want to edit. Three dashboards ship:

| dashboard | what it answers |
|---|---|
| overview | how fast the in-broker path is, and whether the result is admissible at all |
| relay stages | which of the four relay stages the microseconds are in |
| measurement health | whether this run's numbers can be trusted |

Start with the overview. It leads on `emit -> guest`, the single span worth
quoting for the in-broker arm, and puts the correctness gates next to it as
stat panels: relay drops, transform failures and batches given up all have to
be zero, and active subscriptions has to match the fan-out you asked for. A
fast number with a non-zero drop rate is the latency of an incomplete stream.

Measurement health exists because of METHODOLOGY 29. Background consumer
groups co-located with loadgen starved it, its send lateness went past the
pacing interval, and the latency it reported included its own queueing. That
reads exactly like an architectural result and nothing broker-side showed it,
so the client host panels come first on that dashboard.

A few things worth knowing before you read anything off these:

- The scrape is not the measurement. Every published number comes from
  loadgen's own histograms and its own before/after snapshots. Prometheus is
  there to be watched, which is why the scrape interval is chosen for
  legibility rather than defended as methodology.
- The relay stage panels need `relay_stage_metrics_enabled`, which is off by
  default because each stage costs a clock read on the relay hot path.
  `wb reproduce` turns it on for the in-broker arm.
- The `loadgen` scrape target is down between runs. loadgen is a per-window
  batch process, not a service, so that's correct rather than broken.
- `wb monitor check` is the one to run if a panel looks empty. It asks
  Prometheus every dashboard query and separates "this metric doesn't exist"
  from "this feature is off", which Grafana renders identically.

More detail, including the metric names and where each comes from in the
Redpanda source, is in [docs/MONITORING.md](docs/MONITORING.md).

### If Grafana won't load

`wb monitor` tells you, and prints the fix. The cause is almost always the
security group: terraform admits the single address an IP reflector reported
for this machine at apply time. That breaks if your egress IP has changed
since, or if it isn't stable per flow, which is common behind carrier-grade
NAT where a browser can leave from a different address than the reflector
reported. Either way the symptom is a connection that times out rather than
one that's refused, while the VPC, route table and security group all look
correct, so it reads as a broken Grafana. Widen the range with
`operator_extra_cidrs` and re-apply; that changes only the security group rule
and doesn't touch the instances.

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

## Optional: resilience series

The published tables measure steady state. This series is separate, and
deliberately not part of `wb reproduce`, since folding a disturbance into
those numbers would make them incomparable.

Results and analysis are in [docs/RESILIENCE.md](docs/RESILIENCE.md). Read
that before running this.

```bash
# the four-arm series: two actions, drain unset vs 200ms
./scripts/wb run resilience ACTIONS="maintenance transfer" DRAIN_AB=1

# the eight-arm series the published figures come from, adding the
# residency A/B on top
./scripts/wb run resilience ACTIONS="maintenance transfer" DRAIN_AB=1 \
  RESIDENCY_LEVELS="0 8"
```

It answers three questions per action, against a cluster already running the
wasm arm:

- Does the deployment survive? `missing` receipts must be zero. Anything else
  is data loss, not a slowdown.
- What does latency do while it happens? A baseline window, then the same
  window with the action injected part-way through.
- How long until it's normal again? Repeated short windows until p99 is back
  within `RECOVERY_BAND` of the measured baseline and holds there for
  `RECOVERY_SUSTAIN` windows. Both conditions matter. Without the second, one
  lucky window reads as recovered and time-to-baseline is understated exactly
  when it's most interesting.

The baseline is measured rather than assumed, because absolute latency moves
between cluster instantiations.

### Duplicates are expected

Transforms are at-least-once. A leadership move discards work that was read
but not committed, and the next owner reprocesses it, which shows up
downstream as duplicate output because a transform takes a new producer id
whenever it starts. So `duplicate` receipts should be non-zero on every
planned move.

That's why `DRAIN_AB=1` (the default) runs each action twice, once with
`data_transforms_graceful_transfer_timeout_ms` unset and once set. The setting
lets a transform finish and commit in-flight work before leadership goes away,
so duplicates fall to zero at the cost of some added drain time. Without that
contrast this series only establishes that nothing crashed, which the upstream
ducktape suite already covers.

| knob | default | notes |
|---|---|---|
| `ACTIONS` | `maintenance transfer` | `restart` is stubbed, not implemented |
| `DRAIN_AB` / `DRAIN_TIMEOUT_MS` | `1` / `200` | the unset-vs-set comparison. 200 ms eliminates duplicates and the e2e cost scales with the budget, so there's no reason to go higher |
| `DRAIN_TIMEOUT_SET` | unset | run one arm at this budget instead of the A/B, for sweeping it |
| `RESIDENCY_LEVELS` | unset | set to `0 8` to A/B `data_transforms_max_resident_factories`. Unset runs one pass at whatever the cluster already has |
| `BASELINE_SECS` / `DURING_SECS` | `60` / `30` | 60 s because 30 s baselines came in below the cluster's own steady state and produced false "did not recover" verdicts |
| `INJECT_AT_SECS` | `10` | when the action fires inside the during-window |
| `RECOVERY_WINDOW_SECS` | `10` | granularity of time-to-baseline |
| `RECOVERY_BAND` / `RECOVERY_SUSTAIN` | `1.35` / `2` | what "recovered" means |
| `SETTLE_SECS` / `SETTLE_MAX_SECS` | `30` / `150` | leadership must be unchanged this long before an arm's baseline. Arms used to run back to back and one arm's residual landed in the next one's baseline |
| `METADATA_MIN_AGE` / `RETRY_BACKOFF_MAX` | `100ms` / `500ms` | tuned by default here, unlike `loadgen`'s own defaults. Set both to `5s` to reproduce the untuned tail |

This needs a broker built from a branch carrying
`data_transforms_graceful_transfer_timeout_ms` and
`data_transforms_max_resident_factories`. Without them the A/B collapses to
two identical arms, which is worth knowing before you read the output as a
result.

## Teardown

```bash
./scripts/wb teardown       # terraform destroy
./scripts/wb verify-clean   # EC2 API check, don't skip it
```

### Don't rely on remembering

Teardown is manual because the results live on the cluster and an automatic
destroy would throw them away. That makes forgetting the failure mode, and
clusters in the development account have been left running for days on a
"tear it down when I'm done" intention.

`scripts/cost-guard.sh` is the nag. It takes no arguments, holds no state,
prints nothing while the account is clean, and exits non-zero while anything
is billing, so cron mails you only when there's something to act on:

```bash
# every 30 minutes, while a bench cluster is up
*/30 * * * * /path/to/redpanda-wasm-bench/scripts/cost-guard.sh --quiet
```

It reports each instance, how long it's been up and roughly what it has cost,
across every region `select-region.sh` can land in. It never destroys
anything: deciding to tear down mid-run isn't a decision a cron job should
make, which is the whole reason teardown is manual.

### Verification

`verify-clean` queries the EC2 API rather than trusting terraform state,
across instances in any non-terminated state, `available` volumes, EIPs with a
null association, and VPCs tagged for this benchmark. That's not paranoia: the
development account both removed infrastructure on its own and kept billing
after an apparently-successful destroy.

If you're running in an account with automated cleanup, note that
`cloud-nuke`-style tooling tags resources (`cloud-nuke-first-seen`, for
example) and deletes them on its own schedule. A `terraform apply` will show
those tags as drift it wants to remove, which is expected and harmless, but it
means a long-running cluster is on a clock you don't control.

## Repo layout

```
scripts/    wb (entry point), select-region.sh, size-clients.sh,
            lib-capacity.sh, switch-topology.sh, deploy-wasm-cluster.sh,
            verify-each-commit.sh (verifies the redpanda branch commit by commit),
            make-dashboards.py and check-dashboards.py (the Grafana panels and
            a validator for their queries), check-prometheus-template.py,
            cost-guard.sh (cron-friendly reminder that a cluster is billing)
terraform/  topologies as tfvars. region.auto.tfvars and clients.auto.tfvars
            are generated by the scripts above
ansible/    broker install and client tool deployment, plus monitoring.yaml
            (prometheus + grafana, node_exporter everywhere) and the
            dashboards it ships
bench/      the measurement scripts both arms run, plus run-resilience.sh
            (the optional maintenance/leadership-move series)
cmd/        loadgen (rate-paced generator, source of every published number),
            fanout-load (background consumer groups)
wire/       Go fill decoder plus a conformance test against a Rust-generated fixture
docs/       METHODOLOGY.md, RELAY-LATENCY-STAGES.md, TESTS.md,
            RESILIENCE.md (latency during leadership movement),
            MONITORING.md (the dashboards and what they read)
```

Read [docs/METHODOLOGY.md](docs/METHODOLOGY.md) before trusting any number you
produce with this. It's a list of specific ways this harness has produced
confidently wrong results, each one found by getting a wrong number and
chasing it.
