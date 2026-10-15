# Monitoring

Prometheus and Grafana on their own instance, with node_exporter on every
broker, client and on the monitoring host itself.

`wb up` deploys this before the broker binary and before anything is measured,
so the URL exists while a run is happening rather than after it. `wb monitor`
prints it.

## Using the dashboards

`wb up` brings up Prometheus and Grafana on their own instance before the
broker binary is deployed and before anything is measured, so you can watch a
run rather than read about it afterwards.

```bash
./scripts/wb monitor              # the URL, plus whether it's reachable
./scripts/wb monitor deploy       # redeploy the stack
./scripts/wb monitor dashboards   # regenerate and push panels, no restart
./scripts/wb monitor check        # validate every panel query against Prometheus
./scripts/wb monitor export       # bundle the metrics so they outlive the cluster
```

Grafana opens on anonymous viewer access, so the URL works without a login.
Use `admin` / `wasmbench` if you want to edit. Plan runs (`scripts/autorun.sh`)
log the URL as soon as the cluster is ready (`autorun.sh status` shows it
too) and export the metrics before tearing down. Three dashboards ship:

| dashboard | what it answers |
|---|---|
| overview | how fast the in-broker path is, and whether the result is admissible at all; its last row covers stateful transforms (snapshot restore, dark time across leadership moves, recovery failures) |
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

## Purpose

To watch a run, and to tell a fast number from an admissible one.

That second part is the reason this is worth an instance. The harness has
produced confidently wrong results more than once, and the ways it does are
recorded in [METHODOLOGY.md](METHODOLOGY.md). Most of them share a shape: the
cluster looks healthy, the report looks clean, and the number is measuring
something other than what it claims. Relay drops, a fan-out that never reached
the configured subscriber count, and a starved load generator all do this. Each
has a panel.

## What is not here

The scrape is not the measurement. Every published figure comes from loadgen's
own latency histograms and its own before/after metric snapshots, written into
each run's JSON report. Nothing in `REPRODUCE.md` is read off a dashboard.

That separation is deliberate and it buys two things. The scrape interval can
be chosen for legibility instead of having to be defended as part of the
methodology. And a run whose numbers need to be beyond argument can set
`monitor_count = 0`, which takes the observer off the network entirely and
changes nothing else.

## Endpoints and intervals

Redpanda exposes two metric endpoints on the admin port, and neither is a
superset of the other:

| endpoint | family | interval | why |
|---|---|---|---|
| `/public_metrics` | `redpanda_*` | 2s | every metric this benchmark is about |
| `/metrics` | `vectorized_*` | 15s | the full per-shard Seastar set |
| node_exporter | `node_*` | 5s | host CPU, load, network |
| loadgen | `loadgen_*` | 1s | the client's own view of the window |

Everything the bench dashboards read is on the public endpoint. That's worth
knowing rather than assuming, because it's what makes a 2s scrape defensible
against a cluster under measurement: the public registry is curated and small,
while the internal one carries thousands of series per shard and is where the
scrape cost actually is. So the cheap endpoint is scraped tightly enough to see
inside a 30 second window, and the expensive one an order of magnitude more
slowly.

At `DURATION=30s`, a 2s interval gives about 15 points across a window. Enough
to see shape. Going tighter starts to be visible on the thing being measured.

## Labels

| label | meaning |
|---|---|
| `redpanda_id` | cluster identity, on every job |
| `node_id` | broker ordinal |
| `host` | `broker0`, `client3`, `monitor` |
| `role` | `broker`, `client`, `monitor` |
| `instance` | scrape target, added by Prometheus |

`host` is the one that earns its keep. A legend reading `client3` is readable;
one reading `10.0.0.147:9100` has to be cross-referenced against terraform
output every time you look at it. And the client roles are not
interchangeable: `client0` is loadgen alone, `client1` is the external matcher,
`client2` and up carry the background consumer groups. Knowing which is which
is the difference between "a client was busy" and "the measuring client was
starved".

These names follow the convention in
[redpanda-observability-lab](https://github.com/vuldin/redpanda-observability-lab),
which uses Redpanda Cloud's names so a query written against one works against
the other. If you want the broader Redpanda dashboards rather than these
bench-specific ones, that repo has them already rewritten onto these labels.

## The dashboards

Three, generated by `scripts/make-dashboards.py`:

| file | folder | opens on |
|---|---|---|
| `bench-overview.json` | wasm-bench | yes, it's the home dashboard |
| `relay-stages.json` | wasm-bench | |
| `bench-health.json` | wasm-bench | |

### overview

Latency first, then the gates that decide whether the latency means anything,
then where the CPU went.

It leads on `redpanda_relay_emit_to_guest_duration_seconds`, which is the
single span to quote for the in-broker arm: the producing transform's emit
through to the consuming guest dequeuing the record. Prefer it over adding the
four stages up. They overlap, since transit starts inside dispatch's window,
and they leave gaps between each other, so the sum is neither an upper nor a
lower bound.

Beside it is `redpanda_transform_e2e_latency_seconds`, the durable path, for
contrast. That's the honest figure for a consumer reading the output topic
instead of subscribing to the relay.

The stat panels are the gates:

- relay drops per second has to be zero. The relay is best-effort, so a
  consumer that falls behind has its oldest queued record dropped rather than
  the producer being slowed. Non-zero means the latency shown is the latency of
  an incomplete stream.
- active subscriptions has to match `INBROKER_CONSUMERS`. Lower means some
  consumers never subscribed, and the fan-out being measured is not the one in
  the run's label.
- transform failures and batches given up both have to be zero. A given-up
  batch was skipped or dead-lettered, so output is missing records. That's data
  loss, not a slowdown.

One panel is deliberately not aggregated over shards:
`redpanda_cpu_busy_seconds_total` by shard, per broker. Relay and transform
work lands on specific shards, and one saturated shard against twelve idle ones
is the shape of this workload's ceiling. `num_cores` is 13 on `r8id.8xlarge`,
not 32, since SMT is off and cores are reserved for the OS.

The last row, **stateful transforms**, is for guests that persist state
(`book-matcher STATE=snapshot`) and for the durability series:

- `redpanda_transform_state_recovery_failures` has to be zero. Non-zero means
  a persisted snapshot existed and could not be delivered into the guest on
  start - the guest ran, or refused to run under `require_state_recovery`,
  without its state.
- `redpanda_transform_state_restore_latency_seconds` is the broker copying the
  snapshot back into the guest's region. It scales with the registered region
  (the broker copies all of it), not with what the guest stored in it.
- `redpanda_transform_startup_latency_seconds` is the transform's dark time on
  each bring-up, which is paid on every leadership change; beside it, the
  phases (engine start, offset load, state restore) that make it up. The
  durability runner's failover figure is larger: it also includes the raft
  election before this starts.
- transform lag per function shows a replay (the durability runner's
  `REPLAY=1`) as a spike draining to zero.

The latency panels in this row (and "processor create latency") are quantiles
over the trailing 10 minutes, `increase(..._bucket[10m])`, not over
`$__rate_interval` like the rest of the dashboard. They record rare events - a
handful of samples per leadership move - and a rate over a few scrape
intervals is empty everywhere but the seconds after a move, which reads as a
broken panel. `wb monitor check` reports them `empty (expected)` until the
first move of a run.

### relay stages

One row per stage, in the order a record traverses them, each with its own
quantiles. This is the dashboard for "the number moved, which stage moved it",
which a combined panel can't answer.

The four stages, and what each is charged for:

| stage | charged where | scales with |
|---|---|---|
| `crossshard_dispatch` | producer shard | one payload copy plus every submission |
| `crossshard_transit` | destination shard | seastar's cross-shard path |
| `fanout_duration` | the delivering shard | subscriber count |
| `consume_delay` | destination shard | processors sharing the core |

Each stage also has a samples-per-second panel. A flat zero there with a live
workload means `relay_stage_metrics_enabled` is off, not that the stage is
free. The histograms are always registered so that flipping the property live
starts populating a series something is already graphing, rather than making a
new one appear and break whatever was pointed at it.

[RELAY-LATENCY-STAGES.md](RELAY-LATENCY-STAGES.md) has the design behind these.

### measurement health

The client host panels come first, and that ordering is the point.

METHODOLOGY 29: 499 consumer groups co-located with loadgen on one
`c5n.9xlarge` starved it. Its send lateness p99 hit 2,486us against a 1,000us
pacing interval, so the latency it reported included client-side queueing that
could not be attributed to the architecture. Every broker-side signal looked
fine. The fix was eight client instances with loadgen alone on `client0`, and
the way you confirm it held is this dashboard.

`loadgen_send_lateness_seconds` against `loadgen_pacing_interval_seconds` is
the most useful client-side number in the harness. It needs loadgen started
with `-metrics-listen`, which `wb run` passes whenever a monitoring host
exists.

## loadgen's own metrics

loadgen serves a small Prometheus endpoint when given `-metrics-listen`. Off by
default.

It's hand-rolled rather than using `prometheus/client_golang`, for two reasons.
loadgen is the instrument every published number comes from, so adding a
dependency to it means the measuring device changed, and a few dozen lines of
exposition format is easier to audit than a library's collector machinery. And
`go.mod` stays at one direct dependency.

When off, the only cost is an already-false bool check per sample. When on, the
histogram update happens inside the lock the send loop already holds for its
own lateness slice, so it adds no locking. The counters are separate from the
ones the report uses, because the report resets its send counter between the
warmup and timed phases, which is right for a per-window report and wrong for a
Prometheus counter.

| metric | type | |
|---|---|---|
| `loadgen_send_lateness_seconds` | histogram | how late each produce call was against its slot |
| `loadgen_orders_sent_total` | counter | orders offered |
| `loadgen_orders_acked_total` | counter | aggressor legs acknowledged |
| `loadgen_receipts_total` | counter | receipts observed coming back |
| `loadgen_produce_errors_total` | counter | |
| `loadgen_offered_rate_orders_per_second` | gauge | the configured rate |
| `loadgen_pacing_interval_seconds` | gauge | so a panel can draw the threshold |
| `loadgen_phase_info` | gauge | phase and arm, as labels on a constant 1 |

Bucket bounds are densest between 100us and 2.5ms. The pacing interval at the
published `RATE=1000` is 1,000us, and the question these buckets exist to
answer is whether the p99 is approaching it. Buckets an order of magnitude
apart would answer yes or no but not by how much.

## Where the metric names come from

Every metric name in the dashboards was read out of the Redpanda source rather
than guessed, and the generator records where:

| family | source |
|---|---|
| `redpanda_relay_*` | `src/v/relay/probe.cc` |
| `redpanda_transform_*` | `src/v/transform/probe.cc` |
| `redpanda_transform_execution_latency_sec` | `src/v/wasm/transform_probe.cc` |
| `redpanda_wasm_engine_*` | `src/v/wasm/engine_probe.cc` |
| `redpanda_wasm_binary_*` | `src/v/wasm/cache.cc` |
| `redpanda_cpu_busy_seconds_total` | `src/v/redpanda/application.cc` |

All of them register on the public metric registry, which is why they all
arrive on `/public_metrics`.

## Taking the metrics with you

The monitoring host is created per run and destroyed by teardown, so the
scraped metrics have exactly one copy and it dies with the cluster. `wb monitor
export` turns the measured windows into a bundle that opens in a fresh Grafana
with nothing else running:

```bash
./scripts/wb monitor export           # newest results dir, or pass one
cd results/reproduce-<id>/metrics-bundle
docker compose up
./validate.sh                         # every panel, checked
```

`reproduce.sh` does this automatically at the end of a run, and so does
`scripts/autorun.sh`, before every teardown - including the watchdog's - into
`<run-dir>/metrics-bundle`. `wb teardown` says something when no bundle newer
than the cluster exists, because forgetting is permanent. (It used to accept
the newest `reproduce-*` bundle of any age; on 2026-10-15 that was a September
one, and the day's metrics were destroyed unexported.)

For a durability trial to be in the bundle, its loadgen must have been
scraped: export finds windows by when the `loadgen` target was up.
`run-durability.sh` exposes loadgen's endpoint whenever a monitoring host
exists.

### Why it exports from Prometheus and not Grafana

Grafana stores no metrics. It is a query layer, so there is nothing in it to
export. The one exception is a Grafana snapshot, which bakes rendered query
results into the dashboard JSON and needs no datasource at all - but it freezes
the data at one resolution and one time range, so it cannot be re-queried, the
window cannot be changed, and a panel cannot be added. That is a picture, not a
dataset, and combined with METHODOLOGY 30 it is a picture someone would
eventually quote. So the bundle ships real TSDB blocks and the dashboards that
read them.

### How big it is, measured

From the 2026-09-16 three-trial in-broker run, `us-east-1a`:

| | |
|---|---|
| series in one window | 295k, 562k, 830k |
| samples in one window | 9.4M, 34.6M, 53.4M |
| intermediate OpenMetrics text | 2.0 GB, 7.4 GB, 12 GB |
| shipped blocks | 41 MB, 83 MB, 122 MB |
| bundle total, 3 windows | 245 MB |

Two things to take from that.

The series count TRIPLES across three identical trials, and so does the dump.
Each trial deploys 500 new relay consumers with new names, and the branch
retains a transform's metrics past its last processor, so the sets accumulate.
A 7-window both-arms export is therefore much more than twice a 3-window one.

And `scope` barely helps. `public` came out at 2.0/7.4/12 GB against `full`'s
2.4/7.1/12 GB - nearly identical - because the bulk is not the internal
`vectorized_*` set at all. It is the 500 consumers' own per-function
`redpanda_transform_*` and `redpanda_wasm_engine_*` series, which are on the
PUBLIC endpoint. Narrowing the scope excludes the cheap half.

If an export is too slow or too large, the lever is fewer consumers or fewer
trials, not a narrower scope.

### Why windows and not the whole run

A reproduction is a couple of hours of wall clock containing about ten minutes
of measurement. The rest is provisioning, 500 consumers being deployed, topic
cleanup and settle time. Measured on the 2026-09-16 run: seven windows totalling
11.7 minutes against roughly 130 minutes of monitoring-host uptime, an 11x
reduction. That is what makes it affordable to include the full internal
`vectorized_*` set rather than only the families the dashboards query.

Each window is warmup + duration + drain, padded 30s either side. The drain is
inside the window deliberately: receipts are still arriving during it, so a
window clipped before the drain cuts the tail off the distribution being
measured. The padding is 30s rather than 60s because the measured span is 70s
at the published settings and 60s either side nearly triples it.

Panels are empty between windows. That is construction, not breakage, and the
bundle README says so.

### Why promtool and not a copy of the data directory

Prometheus keeps the most recent two hours in the write-ahead log, uncompacted.
For a run shorter than that the blocks on disk are EMPTY, so `cp -r data`
produces a bundle containing nothing. `promtool tsdb dump-openmetrics` replays
the WAL itself - verified by dumping a 25-second-old Prometheus and getting
15,458 lines when no block existed yet.

It also reads a LIVE data directory, so no admin API has to be enabled, nothing
is restarted, and the running stack is untouched. The dump and the block build
both happen on the monitoring host, which already has `promtool`, so the bundle
ships finished blocks and whoever opens it needs no Prometheus tooling.

One dump and one import per window, each becoming its own time-disjoint block.
Do not concatenate the dumps into a single import: per-series sample ordering is
only guaranteed within a window.

### Things that were tried and were wrong

Recorded because each one produces a bundle that looks broken rather than one
that fails loudly:

- `--storage.tsdb.retention.time=0` does NOT disable retention. Zero is read as
  "not set" and Prometheus falls back to its 15d default, announcing it in the
  log: "No time or size retention was set so using the default time retention
  duration=15d". The bundle uses `100y`. (Separately, an idle Prometheus was
  observed not to evict 30-day-old blocks under a 15d retention within 150s, so
  this may never have bitten - but a recording should not depend on that.)
- The data directory must be writable by the container user. `prom/prometheus`
  runs as nobody and panics on start with "open /prometheus/queries.active:
  permission denied" if it cannot write, even though it only ever serves. The
  bundle's data dir is made world-writable at build time so no environment
  variables are needed.
- The dashboards must carry an ABSOLUTE time range. They ship defaulting to
  `now-15m`, which in a recording shows nothing.
- `check-dashboards.py` needs `--at`. A recording's newest sample is older than
  Prometheus's five-minute instant-query lookback, so every query evaluated at
  `now` returns empty. Measured on a real bundle: 51 queries returning data
  with `--at`, against 0 returning and 9 failing without it. `validate.sh`
  computes it from the manifest.
- `dump-openmetrics` is experimental and drops native histograms. Redpanda's
  are classic `_bucket` series today, so nothing is lost, but a future build
  using native histograms would lose them silently. The manifest records the
  build.
- Windows derived from `run_id` are WRONG BY ABOUT 150 SECONDS. `run_id` is
  when the runner started, and between then and the first measured sample it
  creates topics, deploys 500 consumers and runs a deliverability precheck. The
  first real export anchored on it and captured 12-17s of each 95s traffic
  period - the deploy phase, not the measurement. The windows now come from
  `up{job="loadgen"}`, which is up exactly while a measurement is in flight.
- An `ssh` inside a `while read` loop silently ends the loop. ssh reads stdin,
  so the first invocation consumes the rest of the list. Three windows were
  derived and one was dumped. The loop reads an array now.
- The remote dump SURVIVES its ssh. Killing an export leaves promtool running
  on the monitoring host, and a retry against a fixed work path then races it
  for the same file - both truncate it and the retry sits at zero bytes. The
  work directory is unique per invocation and the export refuses to start while
  a dump is in flight.
- `wb ssh` wraps every command in `timeout ${RSH_TIMEOUT:-300}`, and wb's own
  header warns that long-running work must not use that default. A full-scope
  window dump takes many minutes, so 300s kills it - and the failure arrives as
  an EMPTY error string, because the output dies with the connection. The
  export sets 7200s.
- The manifest must be verified against the blocks, not written from the
  request. A truncated export produced 60 MB of real data describing itself as
  three windows while holding one. `metrics-bundle.py` reads each block's
  `meta.json` and refuses to write a manifest it cannot substantiate.

## Validating a change

`wb monitor check` asks the live Prometheus every query in every dashboard and
reports whether each one returns data. A bundle carries its own copy plus
`validate.sh`.

Validating a RECORDING needs two things that validating a live cluster does
not, and both were learned by having a bad bundle pass:

- **NaN is not data.** `histogram_quantile` over a rate window with no samples
  returns a result whose value is NaN, and Grafana renders it as an empty
  panel. Counting it as data is how this reported "57 queries returning data"
  for a bundle whose every latency panel was blank. Excluding NaN took the same
  bundle to 29 returning and 34 empty.
- **Probe every window, not one instant.** Windows are padded and separated by
  gaps, so the midpoint of the whole recording can land after traffic stopped.
  A query passes if ANY window has its data; `validate.sh` derives one instant
  per window from the manifest.

Recording mode is also stricter about what may be empty. On a live cluster
`redpanda_relay_*` can legitimately be absent - nothing is deployed between
runs. In a recording it cannot: the recording exists because a measurement
happened, so an empty relay panel means the wrong range was captured. Only
`loadgen_*`, the residency gauges and processor-create latency are allowed
empty.

This exists because the two ways a panel breaks are both silent. A query that
doesn't parse renders an empty panel with the error behind a hover. A query
that parses but names a metric that doesn't exist renders an identical empty
panel. Neither is distinguishable, by looking, from a feature being switched
off.

So queries that are legitimately empty are listed by name in
`scripts/check-dashboards.py` with the reason, and anything else that comes
back empty is a failure. Run it after changing a dashboard, and run it if a
panel looks wrong.

```bash
./scripts/wb monitor check
```

After editing `scripts/make-dashboards.py`:

```bash
python3 scripts/make-dashboards.py    # regenerate the committed JSON
./scripts/wb monitor dashboards       # push; grafana rescans within 10s
./scripts/wb monitor check
```

The generated JSON is committed so the monitoring host needs no python and the
deploy has nothing to generate. The generator is the source of truth, so edit
it rather than the JSON, and commit both.

## Versions

Pinned in `ansible/monitoring-vars.yaml`, and every download URL built from
them was checked to resolve before being committed. An unpinned "latest" here
would mean a deploy that worked last week failing against hardware that is
already billing.

| component | version |
|---|---|
| node_exporter | 1.9.1 |
| Prometheus | 3.5.0 |
| Grafana | 11.6.1 |

## Access

Grafana runs with anonymous viewer access, so the URL opens without a login.
`admin` / `wasmbench` to edit.

That's only defensible because of where it sits: the security group admits one
address, the cluster holds generated order data, and it exists for a few hours.
It's the wrong setting for anything that outlives a benchmark run.
