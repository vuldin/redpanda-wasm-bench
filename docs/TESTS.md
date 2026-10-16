# The sweeps: what each one varies, pins, and can't isolate

Read this before running a sweep, and before quoting a number out of one.

Every sweep obeys the same contract: one variable per mode, everything else
pinned and written into the run's fingerprint header. A result whose
fingerprint differs in any other field isn't comparable.

Drive them through `./scripts/wb run e2 MODE=... K=V ...`, which supplies
brokers, admin endpoints and a run id.

## `run-e2-throughput.sh`

| MODE | varies | answers | default levels |
|---|---|---|---|
| `rate` | offered orders/sec | per-record cost and the saturation knee | 1000 5000 10000 50000 100000 |
| `payload` | bytes per record | splits per-record from per-byte cost | 0 64 512 4096 |
| `guest` | matcher vs passthrough | splits engine floor from guest work | matcher passthrough |
| `partitions` | input partition count | whether throughput scales with partitions. Currently broken, see below | 1 2 4 8 13 |
| `fanout` | in-broker relay consumers on one partition | how latency and CPU scale toward 800-1,000 consumers on a hot partition | 1 5 10 25 50 |

### `partitions` mode can't measure N greater than 1

Don't use this mode until it's fixed, and don't quote anything it has already
produced. At 2 partitions it reported zero achieved throughput with all
300,000 receipts missing, and at 4 and 8 partitions it lost 50% of receipts.
That isn't saturation.

`run-e2-throughput.sh` takes in-broker receipts through one relay probe
consumer, since `recv_args` is `-probe-topic` unless `CONSUMER_ARM=external`.
`partitions` mode spreads processors across shards by design, and the relay
has no cross-shard hop, so the probe only ever sees its own shard's output.
`run-resilience.sh` already solved this class of problem by switching to an
ordinary fills fetch (`-receipt-source fills`), which is the fix this mode
needs too.

There's a second defect stacked on top. The deliverability pre-check sends 10
orders at rate 20, which need not touch every partition, so it passes and the
level then loses 50-100% of its receipts. That's exactly the zero-receipt
class the pre-check exists to catch.

### Pinned in every mode

- One producer transform and one relay consumer, unless the mode says
  otherwise. Client count is a different phase's variable, not this script's.
- Placement is chosen rather than observed. `rate`, `payload` and `guest` put
  the producer's topics on `PIN_NODE`/`PIN_CORE` and the consumer on
  `PROBE_CORE`, a different shard, since co-locating them would fold shard
  contention into the sweep. `partitions` deliberately doesn't pin cores,
  because the spread is what it measures, but it still co-locates nodes since
  the relay has no cross-node hop.
- `RF`, `WRITE_CACHING`/`FLUSH_MS`, `LINGER_MS`, `PRODUCERS`, `PACING`,
  `SAMPLE_EVERY`, `DURATION`, `WARMUP` and `DRAIN` are all fingerprinted.

### `fanout` mode specifics

The shape this models is one hot partition with 800-1,000 consumers, so the
sweep holds partitions at 1 and moves only the consumer count. Fix `RATE` at a
level a prior sweep proved clean, so fan-out is the only variable. At the
10,000 orders/sec default the ladder goes unclean somewhere between 5 and 10
consumers, which measures the rate rather than the fan-out.

Three things about it that are easy to get wrong:

- Consumer 0 is `relay-probe` and consumers 1..N-1 are `relay-sink`. Only
  consumer 0 writes to `probe_out`, because loadgen correlates one receipt per
  order and would report `max_receipts_for_any_sampled_order = N` if they all
  did, which is a verdict failure caused purely by the harness.
- The fillers have to be zero-emit, and this isn't optional. `relay-sink`
  receives and parses every pushed record and writes nothing. An earlier round
  used `relay-probe` as the filler, so all N-1 produced a record per delivery:
  roughly 33M extra writes in a 30s window at fanout 50. The cluster saturated
  on the benchmark's own output and the ladder measured the harness rather
  than relay dispatch. The two guests differ only in the write, so
  `relay-probe` against `relay-sink` is also a clean one-variable comparison
  if the write cost itself is ever the question. The fillers still aren't
  free: each owns a wasm VM, a pending queue and a scheduling slot, which are
  the costs the ladder exists to measure.
- Consumers are round-robined across the producer node's shards via
  `RELAY_TARGET_SHARD`. Leaving them all on the producer's shard is what made
  `relay_consume` grow with fan-out before the cross-shard fix, so a sweep
  without the spread measures core contention rather than fan-out.

Two metrics in this mode don't mean what their names suggest:

- `relay_pushes_delta` counts shard-local delivery passes rather than pushes.
  `redpanda_relay_pushes_total` increments inside `deliver_locally()`, which
  `push()` invokes once for the producer's shard plus once per other shard
  holding a subscriber. At fanout 10 it read 6,000,000 for 600,000 logical
  pushes.
- So `delivered/pushes` isn't a fan-out ratio, and it read about 1.0 at every
  level, which hid fan-out entirely. It's now reported as
  `delivered_per_shard_pass`, with the real ratio as
  `delivered_per_logical_push`, being delivered / orders x 2.

Consumer CPU is aggregated across all N consumers, weighted by each one's own
invocation count. A single-name lookup reported `n/a` at every level in an
earlier round while the data sat in `per_function` the whole time.

Readiness gates on `redpanda_relay_active_subscriptions` on the producer's
node, never on `rpk transform list`. `RELAY_TARGET_SHARD=N` pins shard N on
every node, and an idle duplicate on a non-producer node reports "running"
before the real subscription exists. Pushes sent before a subscription exists
are dropped silently and permanently, because `push()` treats "no subscriber"
as a no-op.

There's a capacity pre-flight before the first level. Each consumer costs one
instance slot per node, so the largest level needs `N+1` partition-instances
per node against `data_transforms_max_instances_per_core x cores`. The sweep
aborts up front rather than failing several levels in. Note that the cap is
`needs_restart::yes`: setting it without rolling the brokers changes nothing,
and the pre-flight can only read the desired value, so it can pass while the
running brokers still refuse the deploy.

The boot reservation, being `cap x per_function_bytes x cores` and taken on
every core whether or not anything is deployed, is recorded into the summary
header, because it's a real capacity cost of the approach.

### What the sweeps don't isolate

By default `rate`, `payload` and `guest` share topics and a long-running
transform across levels. Level N therefore reads an `orders` log containing
every prior level's records, against a warm batch/readers cache, with a
matcher whose guest order book has been live since level 1. `guest` mode
redeploys the producer, so it resets guest state but not the log.

That's fine when the variable is purely client-side, and not fine when
comparing a first level against a later one, or when the read path could
plausibly matter. Set `FRESH_TOPICS_PER_LEVEL=1` for strict isolation. It
recreates topics and redeploys both transforms before every level, at about
30-45s per level. `partitions` mode always recreates.

Two further limits, both inherent rather than fixable here:

- One partition is one processor. A single-partition sweep measures a single
  serial pipeline and says nothing about aggregate cluster capacity.
- Cross-clock stages. `match` and `total` mix the client's clock with the
  broker's guest clock. `produce`, `send_lateness` and `relay_consume` are
  single-clock and safe. Quote a cross-clock number only with the run's
  `clock-sync.json` residual attached.

### Per-level safety checks

Each level, before spending the window:

1. Topic-variable guard. All three topic variables must still match
   `*-e2-$RUN_ID`, or the level aborts naming the offender. A variable holding
   the topic name was once overwritten with an order count, and every level
   after the first produced to a nonexistent topic.
2. Deliverability pre-check. 10 orders must produce receipts. Both
   zero-receipt failures this harness has produced, being relay
   node-locality and the topic-variable clobber, would have been caught here
   in seconds rather than after a multi-minute level or a five-level sweep. A
   failing pre-check records `PRECHECK-FAILED` and skips the level rather than
   recording noise. It doesn't catch the `partitions` mode failure above,
   since 10 orders need not reach every partition.
3. The exact `loadgen` invocation is echoed, so a wrong topic or flag is
   visible in the log rather than inferred from failures.

Inside `loadgen`, each level is marked `clean:false` on any of: zero receipts,
missing receipts, produce errors tallied by kind, transform failures, relay
drops, a growing backlog past a threshold, window coverage below 95%,
duplicate receipts, or the client failing to offer the requested rate.
Client-bound and broker-bound shortfalls are separate verdicts, since a client
that can't offer the rate says nothing about the broker, and conflating the
two is the easiest way to publish a wrong throughput number.

### Reading the summary table

```
level offered/s achieved/s cores relay_p50us relay_p99us total_p50us \
      match_us/inv match_us/order probe_us/inv dropped clean
```

- `achieved/s` is receipts over the send window. It can read close to 100%
  even when the system fell behind and caught up during the drain, so it isn't
  proof of health on its own. The `clean` column and the lag verdict are.
- `cores` is the count of distinct leader cores the input partitions occupy,
  from `leader_id` rather than `replicas[0]`, which is an arbitrary follower
  at RF>1. A `?` means the topic wasn't found, which is a bug rather than a
  measurement.
- `match_us/inv` is CPU per guest invocation, so per record. `match_us/order`
  is per crossing pair, so two records, and therefore roughly twice it. Both
  are printed because confusing them is a clean 2x error. Invocations are
  measured from `redpanda_transform_execution_latency_sec_count`, never
  inferred.
- `relay_p50us` is single-clock and quotable. `total_p50us` is cross-clock,
  see above.
- `clean=false` means don't quote that row. The reason is printed beneath it.

These identities have to hold, and a violation is a harness bug rather than a
finding: `match_us/order` is about 2 x `match_us/inv`, `receipts` equals
`orders x num_probes`, `invocations` equals `records_sent`, and
`window_coverage` is about 1.0.

## `run-inbroker-vs-external.sh`

The in-broker against external comparison behind `wb reproduce`. Don't drive
the two arms by hand.

The comparison is only valid if both arms are identical in every respect
except who consumes, which is five-plus variables that have to match across
two invocations. Getting one wrong yields a plausible number that isn't a
comparison. This script takes the parameters once and drives both arms, so the
invariant is enforced by code. It also waits for arm 1's transforms to be
cleaned up before starting arm 2, because the runner's
`assert_no_concurrent_run` correctly refuses to start while any other run's
transforms exist.

| arm | who consumes the matcher's output | measured consumer |
|---|---|---|
| `inbroker` | N relay consumers inside the broker | consumer 0 (`relay-probe`), receipt read by loadgen from `probe_out` |
| `external` | N ordinary Kafka consumer groups outside it | loadgen itself, consuming the `fills` topic directly |

The external arm's N-1 background consumers come from `cmd/fanout-load`, and
loadgen is the Nth, so both arms carry the same consumer count.

Both arms report `total` on loadgen's own clock, being order send to receipt
observed, so the headline figure is single-clock with no skew residual. That's
the whole reason it's done this way rather than comparing a broker-clock stage
against a client-clock one.

The arms differ in one way that isn't in the in-broker path's favour:

```
inbroker: send -> matcher -> relay -> probe -> probe_out -> loadgen
external: send -> matcher -> fills                      -> loadgen
```

The in-broker arm carries an extra produce and fetch hop the external arm
doesn't, so the comparison understates the in-broker path. If in-broker wins
anyway it wins carrying weight. If it loses, that extra hop is a candidate
explanation before any conclusion gets drawn.

The script prints a per-level table with `ratio = external / inbroker`. Above
1.00x means the in-broker consumer saw the data sooner. A level counts only if
both arms are `clean:true`, since one clean arm against one saturated arm
isn't a comparison: the saturated side's latency is a backlog age.

`relay_consume` and `match` are absent in the external arm. Both would need a
broker-clock reading, and mixing clocks is exactly what this design avoids.
Absent is honest, a subtly cross-clock number isn't.

## `run-traditional-sharded.sh`

The traditional ladder behind `wb reproduce`, with consumer load sharded
across dedicated instances so the load generator is never the bottleneck.

The single-client version starved loadgen: 499 consumer groups plus the
generator on one `c5n.9xlarge` drove `send_lateness` p99 to 2,486us against a
1,000us pacing interval, so the reported latency included client-side queueing
and couldn't be attributed to the architecture. Measured budget on this
instance type is 49 groups plus loadgen holding pacing at p99 544us, while 99
groups plus loadgen did not, at p99 1,003us. Roles follow from that budget:
client0 runs loadgen and nothing else, client1 hosts the external matcher, and
client2 onward carry background consumer groups evenly sharded.

The measured consumer is loadgen itself, so total consumers of the fills topic
is 1 plus the background groups.

The co-located placements (`PLACEMENTS=loopback`, `loopback-isolated`) run the
matcher on a broker host. The brokers listen only on their private address,
so a client there gets connection refused on 127.0.0.1:9092; it dials the
broker's own private IP instead, which Linux routes over `lo` without touching
the NIC. Rebinding the broker to 0.0.0.0 would change the broker for one arm
only.

## Invocation

```bash
cd redpanda-wasm-bench             # wherever you cloned it
./scripts/wb doctor               # always first

# rate sweep at production replication settings
./scripts/wb run e2 MODE=rate RF=3 WRITE_CACHING=true FLUSH_MS=100 LINGER_MS=0 PRODUCERS=2 \
  RATE_LEVELS="1000 5000 10000 20000" DURATION=30s WARMUP=10s DRAIN=40s

# strict per-level isolation
./scripts/wb run e2 MODE=rate FRESH_TOPICS_PER_LEVEL=1 RATE_LEVELS="1000 10000"

./scripts/wb results              # pull JSON back locally
```

Multi-word values have to be quoted as one argument, so
`RATE_LEVELS="1000 5000"`. `wb` quotes them on the way to the remote shell,
and rejects anything that isn't `KEY=VALUE`.
