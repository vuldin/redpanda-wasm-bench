# Latency during leadership movement

What an in-broker wasm deployment costs while its partition changes leader,
and how long until it's normal again. Produced by `wb run resilience`.

To run it yourself, see [Running the series](#running-the-series) at the end; `plans/resilience.plan` runs it unattended.

This is separate from [REPRODUCE.md](../REPRODUCE.md) on purpose. Those tables
are steady state, and folding a disturbance into them would make them
incomparable.

Every figure here comes from one series on one cluster, measured 2026-09-12.
That is a DIFFERENT cluster instantiation from the steady-state tables in
[REPRODUCE.md](../REPRODUCE.md), which were re-measured 2026-09-16. The two
docs share a topology, a module and an AZ, but not a cluster, so do not
subtract a number here from a number there. Each arm below is read against its
own baseline taken minutes earlier on its own cluster, which is what makes the
series internally valid regardless.

The cluster: 3 x `r8id.8xlarge`
brokers and 8 x `c5n.9xlarge` clients in `us-east-1a`, RF=3, 1,000 orders/sec,
650 B payload, `acks=all`, `write.caching=true`, a single input partition, and
an opt build of `transform-latency-instrumentation`. Eight arms, being two
actions x two drain budgets x residency off/on. Each arm gets its own 60 s
baseline, a 30 s window with the action injected at +10 s, and recovery
windows after that. The client is tuned with `metadata-min-age=100ms` and
`retry-backoff-max=500ms`.

Absolute latencies aren't comparable across regions or cluster
instantiations, so every arm is read against its own baseline, measured
minutes earlier on the same cluster.

Receipts come from an ordinary Kafka fetch of the fills topic rather than from
an in-broker relay consumer. See
[Why not the relay path](#why-not-the-relay-path).

The invocation that produced all of it:

```bash
./scripts/wb run resilience ACTIONS="maintenance transfer" DRAIN_AB=1 \
  RESIDENCY_LEVELS="0 8"
```

## The graceful drain: duplicates versus tail

`data_transforms_graceful_transfer_timeout_ms` lets a transform finish and
commit in-flight work before leadership moves, instead of discarding it for
the next owner to reprocess. Reprocessing shows up downstream as duplicate
output, because a processor takes a fresh `producer_id` on every start and so
can't be deduplicated across owners.

The four arms with residency off, each against its own baseline:

| arm | produce p99 | e2e p99 | e2e max | over 10 ms | dups |
|---|---|---|---|---|---|
| maintenance, no drain | 1,060 -> 916 us | 2,916 -> 2,957 us | 5,102 -> 195,748 us | 0 -> 187 | 27 |
| maintenance, 200 ms | 691 -> 746 us | 1,108 -> 70,124 us | 2,705 -> 367,720 us | 0 -> 361 | 0 |
| transfer, no drain | 650 -> 744 us | 919 -> 1,273 us | 2,424 -> 214,948 us | 0 -> 207 | 48 |
| transfer, 200 ms | 669 -> 713 us | 1,098 -> 70,770 us | 3,269 -> 367,910 us | 0 -> 362 | 0 |

Zero missing receipts in every arm, and every arm recovered.

A 200 ms budget eliminates duplicates. Without it they're 27 to 48. In-flight
work is about one batch, so the budget only has to cover a flush, which is why
the harness default is 200 ms. Both no-drain arms produced duplicates and both
drain arms produced none, so this is a statement about a drain that
demonstrably did work rather than one that had nothing to do.

The produce path doesn't pay for the drain. Produce p99 stays between 647 and
1,060 us across all eight arms, baseline and during, drained and undrained.
Whatever a leadership move costs here, it isn't the write path.

The cost is e2e and it's real. A drain stops the consumer immediately and the
transfer then waits out the budget, so records arriving in that window aren't
transformed until the new owner starts. That's inherent to draining rather
than a defect, which is why the budget should be the smallest value that
eliminates duplicates rather than the largest one that fits.

Both actions pay it about equally. maintenance and transfer at a 200 ms budget
land within 1% of each other, 70,124 against 70,770 us at p99 and 367,720
against 367,910 us at max. The two only differ sharply without a drain, where
maintenance shows up in max and barely moves p99 while transfer moves neither
much.

Without a drain the cost leaves p99 entirely and survives only in max.
`transfer, no drain` read 1,273 us at p99 against a 919 us baseline, which p99
alone would report as nearly free, while its max was 215 ms. That's the
strongest argument for reporting max and over-threshold counts alongside
percentiles.

A p99 of 70 ms is a tail rather than a stall. On the 200 ms maintenance arm,
361 of 30,000 sampled records (1.2%) exceeded 10 ms and 270 (0.9%) exceeded
100 ms, while p50 and p90 were untouched at 726 and 859 us. A window aggregate
reads as a fleet-wide freeze. The counts say about 1% of records were
affected.

### Why a drain costs anything at all, in the code

The cost isn't where a consumer-side quiesce belongs, and working out why took
two fixes rather than one. The lock chain:

- `cluster::partition::transfer_leadership` takes the STM prepare lock, then
  calls `_raft->do_transfer_leadership()`
- `rm_stm::prepare_transfer_leadership()` is `_state_lock.hold_write_lock()`
- `rm_stm::do_replicate()`, which is the produce path, takes
  `hold_read_lock()`

The obvious reading is that the quiesce ran too late and moving it to the
start of `cluster::partition::transfer_leadership` would fix it. That was
done, and it wasn't sufficient. raft's hook is deliberately kept, because
`transfer_and_stepdown` (decommission) never goes through `cluster::partition`,
so on the transfer path both hooks fire. The claim that the second one finds
the work already done and costs nothing only holds when the first drain
succeeded. When the transform is behind, the first drain times out at its
budget, and raft's hook then re-runs the whole budget from behind the write
lock, which reproduces the exact pathology the move was meant to remove.

So `processor::drain()` is single-shot per processor lifetime. It records the
first attempt's verdict and answers from it, with `start()` clearing it so
each new owner gets its own bounded chance. The produce figures above are what
that looks like once both fixes are in.

A passthrough transform can't find any of this. It never lags, so its drain
never times out, so the second drain really is free and the defect is
invisible. Reproducing it needs a guest slow enough that every drain hits its
deadline.

## Keeping the module resident

The drain fixes the departing side. It does nothing for the arriving side: the
broker that takes the partition over still has to fetch the wasm binary and
compile it, and it pays for both at the moment it becomes responsible for the
partition. `data_transforms_max_resident_factories` holds a compiled module in
memory on every broker that replicates a transform's input topic, so the
handover lands on a warm broker instead of a cold one.

### How it's measured, and why per broker

Client-observed latency can't isolate this. From the client a cold start is
indistinguishable from any other stall, and the arm's total can move for
reasons that have nothing to do with bring-up. So the series scrapes two
broker-side histograms, per node, filtered to the transform under test:

```
transform_processor_create_latency_seconds   fetch + compile
transform_startup_latency_seconds            the whole dark window
```

Per node matters. With one input partition the broker that gains leadership
performs exactly one create, so its count delta is 1 and its sum delta is that
single bring-up. A cluster-wide sum would average it with the two brokers that
did nothing.

The ON arms are gated on `wasm_binary_resident_factories` reaching at least 1
before anything is measured. The property is `needs_restart::no`, but set
isn't the same as in effect, since residency reconciles on a debounce plus a
settle, and an arm labelled "residency on" that's actually off looks exactly
like "residency does nothing".

### The result

Bring-up on the broker that gained leadership:

| arm | residency | create | startup | resident |
|---|---|---|---|---|
| maintenance, no drain | off | 94 ms | 95 ms | 0 |
| maintenance, 200 ms | off | 91 ms | 92 ms | 0 |
| transfer, no drain | off | 93 ms | 94 ms | 0 |
| transfer, 200 ms | off | 92 ms | 93 ms | 0 |
| maintenance, no drain | 8 | 1 ms | 2 ms | 1 |
| maintenance, 200 ms | 8 | 1 ms | 3 ms | 1 |
| transfer, no drain | 8 | 0 ms | 2 ms | 1 |
| transfer, 200 ms | 8 | 0 ms | 2 ms | 1 |

Fetch plus compile goes from about 92 ms to about 1 ms, and `startup` tracks
it, 92-95 ms down to 2-3 ms. Startup being only ~1 ms more than create is
itself the finding that fetch and compile are ~99% of the dark window,
measured here rather than assumed.

End to end, the saving shows up in the worst-case record rather than at p99:

| arm | e2e max, residency off | residency on | delta |
|---|---|---|---|
| maintenance, 200 ms | 367,720 us | 276,171 us | -91.5 ms |
| transfer, no drain | 214,948 us | 115,665 us | -99.3 ms |
| transfer, 200 ms | 367,910 us | 280,145 us | -87.8 ms |

Those deltas are the bring-up saving arriving end to end: 87.8 to 99.3 ms
against an independently measured ~92 ms of removed fetch and compile. The
cleanest of the three is `maintenance, 200 ms`, where both arms gained
leadership on the same node and recorded exactly one create each.

### One arm isn't comparable, and the instrumentation is why we know

`maintenance, no drain` with residency on read 868,365 us max and 1,002
duplicates, far worse than its off counterpart. That isn't a residency cost.
The arm recorded two creates, one on each of two brokers, where every other
arm recorded exactly one. `maintenance` enables and then immediately disables,
and leadership went node2 to node0 to node2 inside the window: two moves, two
bring-ups, two lots of reprocessing. Its 977 records over 100 ms against 1,002
duplicates is one contiguous ~1 s block of reprocessed traffic.

Only the per-node create attribution makes that visible. A cluster-wide sum
would have shown "p99 200x worse with residency on" and invited the wrong
conclusion. Use the `transfer` action for a single deterministic move when the
client-side comparison is what matters.

### What it costs, and what it doesn't do

363 KiB of executable memory per broker per resident module, read from
`wasm_binary_resident_memory_usage`. The bound counts modules rather than
bytes because a module's size is only known after it's compiled, so a byte
bound would have to fetch and compile in order to discover it must refuse,
spending the exact resource the bound protects. Size the count by dividing
`wasm_binary_resident_memory_usage` by `wasm_binary_resident_factories` for
your own binaries.

At RF=3 on three brokers every broker replicates the single input partition,
so one transform is held on all three: `resident=1` on each, three references
cluster-wide. That's intended rather than over-pinning. A leader that flaps
between two brokers stays warm on both.

Residency doesn't change duplicates. The drain arms recorded 0 with residency
both off and on, and the no-drain arms recorded 27 to 48 off and 42 on.
Eliminating duplicates is the drain's job.

## Recovery

All eight arms returned to baseline within about 20 s, which is the first two
10 s windows after the disturbance, on both the drained and undrained paths
and with residency off and on. "Recovered" means p99 back within
`RECOVERY_BAND` (1.35x) of that arm's own baseline and held there for
`RECOVERY_SUSTAIN` (2) consecutive windows. Without the second condition one
lucky window reads as recovered and time-to-baseline is understated exactly
when it's most interesting.

Recovery-window p99 came in at 987 to 1,205 us across every arm, with zero
records over 10 ms and zero duplicates in all sixteen windows.

Two things to know before reading a recovery verdict:

- Compare an arm only against its own baseline. The first arm of a series runs
  against a nearly empty log and its baseline reflects that. Here it came in
  at 2,080 us p50 against 705 to 822 us for every later arm. Cross-arm
  baseline comparison within a session isn't meaningful, which is why each arm
  measures its own.
- A baseline isn't automatically quiet. Arms are separated by a settling
  period (`SETTLE_SECS`, leadership unchanged for 30 s) precisely because
  without one the previous arm's residual lands in the next arm's baseline,
  and a baseline containing 200 ms outliers makes "return to baseline"
  meaningless in both directions. Every baseline in this series reported zero
  records over 10 ms, which is the evidence that the settle is doing its job.

`leader_balancer_mute_timeout` is worth knowing separately. It defaults to
300 s, longer than the recovery budget, so a node leaving maintenance
genuinely can't be given leadership back within the measurement. That's a real
effect on post-maintenance balance rather than on the arms above.

## Configure the client, or measure the client

The multi-second tail that gets attributed to a maintenance drain is mostly a
Kafka client default. Worth knowing before you run this series with a client
of your own.

`franz-go@v1.21.5` defaults `metadataMinAge` to 5 s
(`pkg/kgo/config.go:649`). That rate-limits how soon a client may re-fetch
metadata after a `NOT_LEADER`, so a record can wait ~5 s to learn the new
leader, and the default retry backoff of 250 ms escalating to a 5 s cap
compounds it. franz-go's floor for `metadataMinAge` is 10 ms.

Two things make this hard to catch:

- franz-go retries transparently and succeeds, so a client records zero
  produce errors while paying seconds of latency. Error counters can't
  distinguish retry latency from broker latency.
- Only a broker-side metric separates them. Sampling
  `redpanda_kafka_request_latency_seconds{redpanda_request="produce"}` across
  a drain shows the broker handling produce at a flat sub-millisecond cost
  throughout, while receiving fewer requests. Records weren't being processed
  slowly, they weren't arriving.

`redpanda-wasm-clients` and `loadgen` therefore set `metadata-min-age=100ms`
and `retry-backoff-max=500ms`, and record both in every report's config
fingerprint so a tuned run can't be mistaken for an untuned one. Set both to
`5s` to reproduce the untuned tail deliberately. Every figure in this document
is a tuned-client figure.

## Why not the relay path

The steady-state runners measure through an in-broker relay consumer
(`orders -> matcher -> fills -> relay consumer -> probe-out`). That path can't
measure this experiment.

An unpinned relay consumer follows its own input partition's leadership, and
the relay has no cross-node hop. The first leadership move separated the
consumer from the matcher writing to fills and receipts stopped permanently.
The arm survived one drain and then delivered nothing, with loadgen reporting
`relay drops, or a shard-locality mismatch`. Co-locating at setup doesn't
help, because the disturbance under test is what destroys the co-location.

So this series measures through an ordinary fills fetch, which is
leadership-agnostic, and deploys one transform rather than two so the
measurement doesn't have a second transform's placement as its own failure
mode. It costs absolute comparability with the in-broker figures in
REPRODUCE.md, since it includes an external fetch, but every number here is a
delta against a baseline taken on the same cluster minutes earlier, so that
offset cancels.

## What each window reports

The series reports the same per-stage quantiles the steady-state tables carry
(`scripts/render-tables.py`: p50/p90/p99 for `produce`, `relay_consume` and
`total`), so a disturbance run and a steady-state run are read the same way,
plus the fields a disturbance needs that a steady-state table doesn't.

| field | why a disturbance needs it |
|---|---|
| `produce` p50/p99 | says where the cost landed. Reporting `total` alone once made a drain look like a produce regression when the cost was the transform not consuming, a different problem with a different fix |
| `max`, `p999` | a leadership move's cost can leave p99 entirely and survive only in max, which is what `transfer, no drain` did above, so p99 alone can show a disturbance as nearly free when it isn't |
| `over 10ms` / `over 100ms` counts | how many records were affected rather than how bad it was for the worst few. One 900 ms record and three hundred of them give nearly the same p99 and mean completely different things |
| `mean` | the aggregate cost. On the drained arms p50 and p90 were untouched while mean moved to 3 ms, so the hit was confined to about the top 1%, which the p99 figure overstates as fleet-wide |
| `send_lateness` p99 | a load generator that fell behind reports its own queueing as system latency. render-tables rejects a level for this, and a disturbance is exactly when the client is most likely to fall behind, so the series warns when it exceeds one pacing interval |
| `timeline` | per-interval p50/p99/max bucketed by send time (`-timeline-ms`, 500 ms here). An aggregate can't say when a stall began or how long it lasted, and those identify the mechanism: a stall starting one drain-budget after the injection is a second drain, one starting at the injection is the transfer itself, and both give the same window aggregate |

Bucketing by send time rather than receipt time matters. A record delayed by
seconds belongs to the moment it was offered, or the stall gets reported as
having happened after it ended.

## What isn't measured

- Decommission. It exercises `consensus::transfer_and_stepdown`, the path that
  bypasses `cluster::partition::transfer_leadership`. Covered by unit tests
  against a real raft group rather than on a cluster.
- Broker restart. Stubbed in `run-resilience.sh`, needs the ansible/ssh path.
  Maintenance and explicit transfer cover leadership movement without stopping
  a process.
- Multi-partition transforms. Everything here is a single input partition,
  which is the worst case for cold starts since only one broker is ever warm,
  and the simplest case for the drain since there's one processor to quiesce.
  It's also the best case for residency: at RF=3 every broker replicates that
  one partition, so every broker is warm. A transform with more partitions
  than brokers is already warm everywhere without residency, and one whose
  partitions outrun the resident bound will start refusing pins. Neither case
  is measured here.
- Residency under memory pressure. The bound was never reached, since this is
  one transform and one module against a bound of 8. Eviction and the refusal
  path are covered by unit tests rather than on a cluster, so
  `wasm_binary_resident_admission_failures` read 0 throughout.
- Whether residency helps an unplanned leadership loss. Every arm here moves
  leadership deliberately. A broker that dies takes its resident modules with
  it, and what matters then is whether the survivor was warm, which these arms
  don't isolate.
- Drain budgets other than 200 ms. The A/B is unset against 200 ms. 200 ms is
  enough to eliminate duplicates and the e2e cost scales with the budget, so
  larger values weren't worth cluster time. If you need to know what a
  particular budget costs, sweep it with `DRAIN_TIMEOUT_SET`.

## Running the series

The published tables measure steady state. This series is separate, and
deliberately not part of `wb reproduce`, since folding a disturbance into
those numbers would make them incomparable.

Unattended, with provisioning, metrics export and teardown handled:

```bash
scripts/autorun.sh start plans/resilience.plan
```

By hand, on a cluster you brought up with `wb up` and `wb deploy`:

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
