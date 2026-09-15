# Relay-path latency stage definitions

The shared vocabulary for `cmd/loadgen` and `run-e2-throughput.sh`. This file
exists so that "what does this number actually measure" doesn't have to be
re-derived from a conversation every time someone asks.

Which stages you can trust:

- `produce` is client-clock only, and `relay_consume` is broker-guest-clock
  only, so both are sound.
- `match`, and any total that spans client-send to guest-receipt, mixes the
  two clocks and carries the full offset between hosts. On these clusters
  chrony is pinned to Amazon Time Sync, with no PTP hardware clock on the AMI,
  and settles to single-digit microsecond RMS, which is small against these
  stages. Quote a cross-clock number with that residual attached rather than
  bare. `wb status` prints it per host.

## The pipeline, as it exists in this codebase

```
client          orders topic        wasm-matcher         relay          relay-probe (N instances)
  |  produce -->  (durable)  --input-delay+exec--> fills   --push-->  on_push()  --scheduling delay-->  guest runs
  |<---ack---|                                       |                    |                                |
  |                                                  +--- matched_at -----+------------ consume -----------+
```

There's no step where the relay reads from a topic, and that's true for any
producer, wasm or not. `relay::service::push()` is never wired into the
ordinary Kafka produce/replication path at all, confirmed by grepping
`src/v/kafka/`, `src/v/cluster/` and `src/v/raft/` for any `relay::`
reference, which gives zero hits in all three.

The synchronous, in-process, no-network-hop push is specific to wasm
transforms rather than general. `relay::service::push()` has exactly one call
site in the entire codebase, in `transform/transform_processor.cc`, inside a
wasm transform's own emit callback. A plain non-transform Kafka producer
writing directly to a topic never touches the relay: not synchronously, not
asynchronously, not at all. The only way data reaches the relay is via a wasm
transform's output. So "push-only fan-out, no read" is true unconditionally,
while "synchronous, same-process, producer's own shard" describes the
wasm-transform-as-producer case, which is the only case that exists today.

That's a current-scope gap rather than a permanent architectural one, and
`relay::service.h`'s own doc comment says so: "Producers (today, transform
processors) push bytes." `push(ntp, data)` is a plain function with nothing
wasm-specific about its signature or implementation, and any code holding a
`relay::service*` could call it for any ntp. Extending the relay to a plain
external producer means adding a second call site somewhere in the ordinary
Kafka produce path under `src/v/kafka/`, which is a real scoped addition
rather than something the relay's design forecloses. Nobody has built that
second call site yet.

## Stage definitions

### 1. `produce`

Boundary: the client's `Produce()` call to the producer's ack callback firing,
durable per `RequiredAcks`.

Includes network RTT to the seed broker, produce-path queuing, and
replication/fsync per whatever durability config is active.

Measured client-side, in Go, on the high-resolution host clock via
`time.Now()`. No precision caveat.

### 2. `match`

Boundary: order durable (ack) to `wasm-matcher`'s own guest code finishing
`process()` and stamping its own timestamp, before the relay push happens.

Includes the matcher transform's own read-loop pickup delay, meaning queuing
before it's scheduled to read the durable record, plus wasm guest
invocation/dispatch overhead, plus the matching algorithm's execution.

Does not include the relay push itself, which happens after this timestamp is
stamped, inside the same emit callback, once the guest's `write()` call
returns to the host.

Measured guest-side in `wasm-matcher-rs`'s `main.rs` via `SystemTime::now()`,
appended as trailing bytes after the fill-list encoding. The guest's
`CLOCK_REALTIME` sources directly from `std::chrono::system_clock`, so this is
nanosecond-capable rather than the millisecond-quantized
`model::timestamp::now()` the wasi layer used to hand out.

Because the matcher stamps immediately after local append while the producer's
`acks=all` ack returns a few hundred microseconds later, `match` is normally
negative. It's a race between two paths rather than a duration, and its sign
carries information. Never quote it as latency.

### 3. `relay-fanout`

Boundary: `relay::service::push()` starting to iterate subscribers, to a
specific subscriber's `on_push()`/`deliver()` being called.

Includes the fan-out loop's own per-subscriber iteration cost, which is a real
cost that scales with subscriber count N, since subscriber 99 of 100 waits for
98 preceding synchronous `deliver()` calls to return first. Owned by the
producer's shard rather than the consumer's.

Measured host-side as `fanout_duration_seconds`, per shard, on
`ss::steady_clock_type`. It's not part of the per-order JSON stages, so it
shows up in the scraped histograms rather than next to `produce` in a report.

### 4. `consume`, which is the JSON field `relay_consume`

Boundary: a specific subscriber's `on_push()` being called, so data enqueued
into that consumer's own pending queue, to that consumer's own processor fiber
being scheduled by the reactor, dequeuing it via `read_batch()`, and the guest
finishing processing it.

Includes almost entirely the consumer's own scheduling delay: how long
relay-probe instance N's fiber waited its turn on a shared core, given every
other relay-probe instance competing for the same core. That's why it scales
with fan-out count while `produce` and `match` don't.

Does not include meaningful relay transit time. The push-to-enqueue hand-off
is synchronous and near-instant, so almost none of this number is the relay's
own work.

The JSON field is still named `relay_consume` rather than `consume`. Read it
per this definition rather than as "relay work".

Measured guest-side, on the same clock as `match`.

## Why `consume` used to grow with fan-out

The relay's subscriber map is per-shard, and `push()` originally only ever
delivered to subscribers on the same shard as the pushing transform. Every
relay-sourced transform's shard was assigned purely by raft leadership of its
declared input topic's partition, with no override, so every consumer in a
fan-out benchmark landed on the same core as every other consumer and competed
for that core's scheduling regardless of how many cores the broker had. That,
rather than the relay's push mechanism, was the bottleneck.

The fix has two parts:

1. `relay::service` delivers cross-shard. `push()` still delivers to
   same-shard subscribers exactly as before, adding no cost there, then fans
   out to every other shard with a subscriber for that ntp via a
   fire-and-forget `sharded<>::invoke_on`, using the inherited
   `peering_sharded_service::container()`. Which shards have subscribers is
   tracked via a small per-shard replicated set (`_shards_with_subscribers`),
   maintained by `add_subscription`/`remove_subscription` broadcasting local
   subscriber-count transitions. That's the same lock-free "broadcast on rare
   change, cheap local read on the hot path" pattern `partition_leaders_table`
   and `node_status_table` already use.
2. `transform_manager` can place a relay-sourced transform on a shard other
   than the one its input topic's leadership would dictate, via the opt-in
   `RELAY_TARGET_SHARD=<N>` env var (see `transform/relay_source_env.h`),
   using the same `--var` mechanism `RELAY_SOURCE` already uses.

That second part needed three separate leadership-gated call sites fixed
rather than one, because leadership-change notifications fire on any shard
hosting a replica of the partition, leader or follower, on any leadership
churn anywhere in the raft group. A naive single-call-site version would have
had a hint-placed processor killed by unrelated leadership events elsewhere in
the cluster and never recover from a transient error.

Worth knowing before you read a cluster-wide transform report during a sweep:
`RELAY_TARGET_SHARD=N` pins shard N on every node that has the transform
deployed, not a single cluster-wide pin. On a multi-node cluster every node
whose shard count includes N independently creates a processor for it, and only
the node the actual producer's shard lives on ever receives real pushes, since
there's no cross-node relay hop. The others sit idle.

Readiness gating had to change with it. `rpk transform list` reporting
"running" for a hint-placed transform can reflect one of the idle duplicate
processors on a non-producer node coming up faster than the real one, so a
sweep could declare a level ready before every real subscription existed, and
the matcher's earliest pushes for that level went out before those
subscriptions did, silently and permanently missed. `push()` correctly treats
"no subscriber yet" as a no-op rather than something to retry. Gate on
`redpanda_relay_active_subscriptions` on the producer's node instead.

## The four stages tile the timeline

| # | stage | metric | clock starts | clock stops |
|---|---|---|---|---|
| 1 | producer fan-out cost | `crossshard_dispatch_duration_seconds` | before the payload copy | after the last submission, before awaiting anything |
| 2 | cross-shard transit | `crossshard_transit_duration_seconds` | after the copy, before the first submission | on arrival at a destination shard |
| 3 | local subscriber loop | `fanout_duration_seconds` | on arrival at that shard | after the last `deliver()` |
| 4 | consumer scheduling | `consume_delay_seconds` | when the record is enqueued there | when the processor dequeues it |

Stage 1 stops before awaiting on purpose, since it measures how long the
matcher was held up, which is the number that matters for the producing
transform's critical path. Stage 4 starts only once the record is already
enqueued at the destination. Before stage 2 existed, time spent inside
seastar's cross-shard machinery was charged to no stage at all, and a record
could take milliseconds to cross a core with every existing stage reporting
microseconds. That mattered because the fan-out ceiling had been misattributed
twice, first to single-core CPU saturation and then to cross-shard submission
backpressure, and both explanations died for want of this measurement.

### Why measure transit rather than read seastar's `smp` metrics

The instinct is to look at seastar's cross-shard queue metrics. They can't
answer it:

- All seven are registered `(sm::metric_disabled)` in
  `seastar/src/core/reactor.cc`, and there's no config or CLI path to enable
  them. The only enable path is `set_relabel_configs`, which Redpanda never
  calls.
- Even fully enabled they wouldn't show the backlog. The queue that grows
  without bound under overload is `_tx.a.pending_fifo` on the source shard,
  and it has no metric at all. `send_queue_length` counts only what
  `move_pending` managed to push into the 128-slot ring minus completions, so
  it saturates at 128, and the three `*_batch_queue_length` gauges are the
  size of the last batch, which is at most 16.

Measuring transit latency directly is more informative than the queue depth
would have been, and needs no seastar change.

### Reading the transit number

- It's recorded on the destination shard, like `consume_delay`, so it's
  charged where the waiting happened rather than where the push originated.
  Aggregated over the shard label like every other relay metric, so a scrape
  gives a per-broker distribution.
- It's single-clock. `ss::steady_clock_type` is `CLOCK_MONOTONIC`, consistent
  across cores, so it's directly comparable with the other three stages with
  no cross-clock caveat.
- One stamp per push rather than per destination shard. A later shard's
  transit therefore also includes the submission cost of the shards ahead of
  it, bounded by (remote - 1) x per-submission cost. That was measured at 25
  to 100 ns, so under about 2 us even at fan-out 20, which is negligible
  against a millisecond-scale effect. Taking one clock read per remote shard
  would multiply the reads on this hot path by the fan-out factor, which is
  how instrumentation starts changing what it measures.
- Gated on `relay_stage_metrics_enabled` like the other three, and not
  recorded for `push()`'s purely local delivery, where there's no transit.
  `relay_cross_shard_test` asserts both halves, that it's recorded on
  destinations and not on the producer, with `fanout_duration` on the producer
  as the tripwire so the negative half can't pass by the flag simply being
  off.

### What to conclude from it

- Transit small and `consume_delay` large means the deferral is consumer
  wakeup. The cross-shard path is fine and shard count isn't the lever.
- Transit large means the deferral is in the cross-shard path itself, so
  destination shards aren't draining the queue fast enough.
- Both small while `transform_e2e` is milliseconds means the time is somewhere
  none of these four stages covers, and the next thing to instrument is the
  transform's own three-loop pipeline rather than the relay.
