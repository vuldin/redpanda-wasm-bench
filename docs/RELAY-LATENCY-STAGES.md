# Relay-path latency stage definitions

This is the shared vocabulary for `run-relay-wasm-fanout.sh` /
`relay-wasm-fanout-bench`, and for the open-loop `cmd/loadgen` +
`run-e2-throughput.sh` path that `SCALING-TEST-PLAN.md` uses - the stage
definitions are the same in both; only how load is offered differs.

It also covers the external-TCP counterpart, `run-relay-external-fanout.sh`,
whose own multi-shard relay listener bug is still open. **Scope decision
2026-08-28: the external TCP arm is deliberately out of scope for
SCALING-TEST-PLAN.md's phases** - it will need handling eventually, but the
wasm consumer path is where the latency story lives, and carrying a blocked
dependency through every phase for a strictly-slower path was not worth it. The
stage definitions below still apply there when it is picked back up: `match`
and `produce` unchanged, `relay-fanout`/`consume` the same concepts against a
real TCP client instead of an in-broker wasm guest.

**Clock provenance, added 2026-08-28:** `produce` is client-clock only and
`relay_consume` is broker-guest-clock only, so both are sound. `match` and any
total that spans client-send to guest-receipt mix the two clocks and carry the
full offset between hosts. On this cluster chrony (pinned to Amazon Time Sync;
there is no PTP hardware clock on the AMI) settles to single-digit microsecond
RMS, which is small against these stages - but quote a cross-clock number with
that residual attached, never bare. `wb status` prints it per host.
Written because a first pass at a per-stage breakdown (2026-08-18) used a label
("relay_consume") that turned out to conflate two architecturally
distinct costs - this doc exists so that mistake doesn't have to be
re-derived from a conversation every time someone asks "what does this
number actually measure."

## The pipeline, as it actually exists in this codebase

```
client          orders topic        wasm-matcher         relay          relay-probe (N instances)
  |  produce -->  (durable)  --input-delay+exec--> fills   --push-->  on_push()  --scheduling delay-->  guest runs
  |<---ack---|                                       |                    |                                |
  |                                                  +--- matched_at -----+------------ consume -----------+
```

There is **no step where the relay reads from a topic** - true for
any producer, wasm or not: `relay::service::push()` is never wired
into the ordinary Kafka produce/replication path at all (confirmed by
grepping `src/v/kafka/`, `src/v/cluster/`, and `src/v/raft/` for any
`relay::` reference - zero hits anywhere in any of them).

**The synchronous, in-process, no-network-hop push is WASM-transform-
specific, not general.** `relay::service::push()` has exactly one call
site in the entire codebase: `transform/transform_processor.cc`, inside
a WASM transform's own emit callback. **A plain (non-transform) Kafka
producer writing directly to a topic never touches the relay at all** -
not synchronously, not asynchronously, not at all. The *only* way data
ever reaches the relay is via a WASM transform's output. So "push-only
fan-out, no read" is true unconditionally; "synchronous, same-process,
producer's own shard" specifically describes the WASM-transform-as-
producer case, which is the only case that exists today.

**This is a current-scope gap, not a permanent architectural one** -
`relay::service.h`'s own doc comment says so explicitly: "Producers
(**today**, transform processors) push bytes." `push(ntp, data)` itself
is a plain function, nothing wasm-specific about its signature or
implementation; any code holding a `relay::service*` could call it for
any ntp. Extending the relay to a plain external producer means adding
a second call site somewhere in the ordinary Kafka produce path
(`src/v/kafka/`) - a real, scoped addition, not something the relay's
own design forecloses. Nobody has built that second call site yet.

## Stage definitions

### 1. `produce`
**Boundary:** client's `Produce()` call → the producer's ack callback
fires (durable, per `RequiredAcks`).
**What it includes:** network RTT to the seed broker, produce-path
queuing, replication/fsync per whatever durability config is active.
**Measured:** client-side (Go `relay-wasm-fanout-bench`), high-resolution
host clock (`time.Now()`). No precision caveat.

### 2. `match`
**Boundary:** order durable (ack) → `wasm-matcher`'s own guest code
finishes `process()` and stamps its own timestamp, *before* the relay
push happens.
**What it includes:** the matcher transform's own read-loop pickup
delay (queuing before it's scheduled to read the durable record) +
wasm guest invocation/dispatch overhead + the actual matching-algorithm
execution.
**What it does NOT include:** the relay push itself (that happens
*after* this timestamp is stamped, inside the same emit callback, once
the guest's `write()` call returns to the host).
**Measured:** guest-side (`wasm-matcher-rs`'s `main.rs`,
`SystemTime::now()`), appended as trailing bytes after the fill-list
encoding. High-resolution as of 2026-08-18 - see clock precision
history below.

### 3. `relay-fanout` (not separately measured - deferred, see below)
**Boundary:** `relay::service::push()` starts iterating subscribers →
a specific subscriber's `on_push()`/`deliver()` is called.
**What it includes:** the fan-out loop's own per-subscriber iteration
cost - a real cost that scales with subscriber count N, since
subscriber 99 of 100 waits for 98 preceding synchronous `deliver()`
calls to return first. Owned by the *producer's* shard, not the
consumer's.
**Currently:** folded into `relay_consume` (the JSON field name; see
stage 4) - undercounted as a result, not isolated. Isolating it needs
new C++ instrumentation, deliberately deferred - see "Deferred: splitting
relay-fanout from consume" below for why.

### 4. `consume` (JSON field: `relay_consume`)
**Boundary:** a specific subscriber's `on_push()` is called (data
enqueued into that consumer's own pending queue) → that consumer's own
processor fiber is scheduled by the reactor, dequeues it
(`read_batch()`), and the guest finishes processing it.
**What it includes:** almost entirely the *consumer's* own scheduling
delay - how long relay-probe instance #N's fiber waited its turn on a
shared core, given every other relay-probe instance competing for the
same core. This is why it scales with fanout count and `produce`/`match`
don't.
**What it does NOT include:** meaningful relay transit time - the
push→enqueue hand-off itself is synchronous and near-instant; almost
none of this number is "the relay's" work.
**Naming:** the JSON field in `relay-wasm-fanout-bench`'s reports is
still `relay_consume`, not renamed to `consume` - that rename was
scoped out along with the relay-fanout/consume C++ split (see below).
Read `relay_consume` in any report as "consume" per this doc's
definition, not as "relay work."
**Measured:** guest-side, same clock as `match`. High-resolution as of
2026-08-18 - see below.

## Clock precision: history and current state

**Fixed 2026-08-18, verified via a real AWS re-run the same day.**
Before this date, every wasm guest's view of `CLOCK_REALTIME`
(`SystemTime::now()` in Rust) was sourced from `model::timestamp::now()`
(`src/v/wasm/wasi.cc`), which is **deliberately millisecond-resolution**
- it mirrors Kafka's own on-wire record timestamp field width, not a
real-time-clock limitation. This made any stage computed from two
guest-side reads (`match`, `consume`) quantized to whole milliseconds -
visible as an *exactly* constant value across dozens of independent
samples at low fanout (2000µs/3000µs, zero variance - not noise, real
quantization).

**Fix:** `CLOCK_REALTIME` now sources directly from
`std::chrono::system_clock` (nanosecond-capable on Linux), the same
clock `model::timestamp` was already built from before truncating it -
confirmed zero performance cost (strictly fewer operations than the old
path, which already called `system_clock::now()` internally before
truncating to ms and reconstructing ns from that truncated value). No
new determinism concern - the existing comment on `REALTIME_CLOCK_ID`
already established that a real, non-deterministic-across-replicas wall
clock was an accepted tradeoff; this only improves its resolution.

**Verified fixed**, not just fixed-in-code: re-ran all four fanout
levels on the AWS cluster after deploying the fix. Every `match`/
`relay_consume` sample now shows genuine, non-repeating microsecond
variance (e.g. fanout=1's `relay_consume` ranged 2395-2494µs across 50
samples, rather than the flat 2000µs it read before the fix). **Both
`match` and `consume` (JSON: `relay_consume`) are now reliable to real
microsecond precision, same as `produce`.** This section is not a live
caveat anymore - kept as history so the "why was it ever rounded"
question doesn't need re-investigating.

`produce` was never affected either way - it's a host-side (Go client)
timestamp, never touched the guest clock.

## Deferred: splitting `relay-fanout` from `consume`

**Explicitly deferred by the user (2026-08-18), not just unstarted.**
The plan below was proposed, the user asked whether it would affect
performance, and after hearing the honest answer (small but real
overhead, scoped to the relay-active-subscriber path only - never
touches produce/match/non-relay consumers) chose to skip it for now and
revisit only if it ends up mattering. Keeping the concrete plan here so
it doesn't need to be re-derived if that happens:

1. In `relay::service::push()` (`relay_service.cc`): capture
   `ss::steady_clock_type::now()` immediately before and after the
   subscriber loop; record the delta into a new per-push histogram
   (aggregate, not per-order - e.g. `redpanda_relay_fanout_duration_seconds`).
   Answers "does fan-out cost itself scale with N" directly, host-side,
   nanosecond-resolution.
2. In `relay_source` (`relay_source.h`/`.cc`): capture
   `ss::steady_clock_type::now()` in `on_push()` when data is enqueued;
   compute the delta against it when `read_batch()` dequeues; record
   into a new per-processor histogram (e.g.
   `redpanda_transform_relay_consume_delay_seconds`). This is the real
   `consume` number, host-side, nanosecond-resolution.

Both would be aggregate/distributional (via the existing
probe/histogram infrastructure), not per-order-correlated like
`produce`/`match`/`total` currently are - a deliberate scope cut to
avoid threading multiple host timestamps back through the wasm guest
boundary via wire-format changes. Revisit if per-order correlation for
these two specific stages turns out to matter later, in addition to
revisiting the performance-impact question itself.

## Cross-shard relay (2026-08-18): why `consume` grew with fanout, and the fix

The clean, unrounded numbers above still showed `consume` (JSON:
`relay_consume`) growing with fanout - 2438us at fanout=1 up to 22341us
at fanout=100, p50. Root cause, confirmed by direct code reading: the
relay's subscriber map is per-shard, and `push()` only ever delivered to
subscribers on the *same* shard as the pushing transform. Every
relay-sourced transform's shard was assigned purely by raft leadership
of its declared input topic's partition, with no override - so every
consumer in a fanout benchmark landed on the *same one core* as every
other consumer, competing for that core's own scheduling regardless of
how many cores the broker actually had. This - not the relay's push
mechanism itself, which is synchronous/near-zero - was the real
bottleneck, and it's why a go-based *external* relay (a different,
less-constrained architecture entirely) showed flat latency across
fanout while this in-broker path didn't: the external relay's consumers
were never forced onto one shard to begin with.

**Fix, two parts, both new:**
1. `relay::service` (`relay_service.h`/`.cc`) now delivers cross-shard:
   `push()` still delivers to same-shard subscribers exactly as before
   (no cost added there), then fans out to every *other* shard with a
   subscriber for that ntp via a fire-and-forget `sharded<>::invoke_on`,
   using the inherited `peering_sharded_service::container()` - no new
   per-instance plumbing needed. Which shards have subscribers is
   tracked via a small per-shard replicated set (`_shards_with_subscribers`),
   maintained by `add_subscription`/`remove_subscription` broadcasting
   local subscriber-count transitions, the same lock-free "broadcast on
   rare change, cheap local read on the hot path" pattern already used
   by `partition_leaders_table`/`node_status_table` elsewhere in this
   codebase.
2. `transform_manager` (`transform_manager.h`/`.cc`, `api.cc`) can now
   place a relay-sourced transform on a shard other than the one its
   input topic's leadership would dictate, via a new opt-in env var,
   `RELAY_TARGET_SHARD=<N>` (see `transform/relay_source_env.h`) - the
   same `--var` mechanism `RELAY_SOURCE` already uses. This needed three
   separate leadership-gated call sites fixed, not one, because
   leadership-change notifications fire on any shard hosting a replica
   of the partition (leader *or* follower) on *any* leadership churn
   anywhere in the raft group - a naive single-call-site version would
   have had a hint-placed processor killed by unrelated leadership
   events elsewhere in the cluster and never recovered from a transient
   error. `RELAY_TARGET_SHARD=N` pins shard N *on every node* that has
   the transform deployed, not a single cluster-wide pin - on a
   multi-node cluster, every node whose shard count includes N
   independently creates a processor for it, and only the node the
   actual producer's shard lives on ever receives real pushes (there is
   no cross-node relay hop); the others sit idle. Harmless for a
   benchmark, worth knowing before reading a cluster-wide transform
   report during one.

**Result, real AWS cluster (3x r8id.8xlarge, 13 shards/broker),
`bench/run-relay-wasm-fanout-crossshard.sh` (round-robins
`RELAY_TARGET_SHARD` across the producer's node's shards instead of
leaving every consumer on the producer's own shard), zero missing
samples at every level:**

| fanout | `relay_consume` p50, shard-local (before) | `relay_consume` p50, cross-shard (after) |
|---|---|---|
| 1   | 2438us  | 542us   |
| 10  | 5439us  | 495.5us |
| 50  | 7245us  | 1016us  |
| 100 | 22341us | 1257.5us |

`relay_consume` stopped scaling with fanout - it stays in the same
sub-1.3ms range from fanout=1 through fanout=100 once consumers are
spread across cores instead of piling onto one, roughly matching the
flat shape of the external-relay chart that originally motivated this
work, an 11-18x improvement at fanout=10/100 specifically. This
confirms the diagnosis: shard-locality, not the relay's push mechanism,
was the bottleneck.

**A real gotcha hit and fixed while measuring this**: an initial full
run showed real missing samples (350/500 at fanout=10, 1200/2500 at
fanout=50) despite the fix working correctly - traced to the
benchmark's own readiness check, not the relay. `rpk transform list`
reporting "running" for a hint-placed transform can reflect one of the
*idle* duplicate processors on a non-producer node coming up faster
than the real one on the producer's own node - so the original
readiness check could declare a fanout level ready before every real
subscription actually existed, and wasm-matcher's earliest pushes for
that level went out before those subscriptions did, silently and
permanently missed (`push()` correctly treats "no subscriber yet" as a
no-op, not something to retry). Fixed by waiting on
`redpanda_relay_active_subscriptions` - a per-node gauge, already
summed across that node's own shards - reaching the expected count on
the producer's node specifically, instead of trusting the cluster-wide
transform-list aggregate.

---

## `crossshard_transit`: the gap in the timeline, closed 2026-09-01

Until now the four relay-side stages did not tile the timeline. There was a hole:

| # | stage | metric | clock starts | clock stops |
|---|---|---|---|---|
| 1 | producer fan-out cost | `crossshard_dispatch_duration_seconds` | before the payload copy | after the last submission, **before awaiting anything** |
| 2 | **cross-shard transit** | **`crossshard_transit_duration_seconds`** | **after the copy, before the first submission** | **on arrival at a destination shard** |
| 3 | local subscriber loop | `fanout_duration_seconds` | on arrival at that shard | after the last `deliver()` |
| 4 | consumer scheduling | `consume_delay_seconds` | when the record is enqueued there | when the processor dequeues it |

Stage 1 stops before awaiting *on purpose* - it measures how long the matcher
was held up, which is the number that matters for the producing transform's
critical path. Stage 4 starts only once the record is already enqueued at the
destination. So **time spent inside seastar's cross-shard machinery was charged
to no stage at all**, and a record could take milliseconds to cross a core with
every existing stage reporting microseconds.

That mattered because the fan-out ceiling has been misattributed twice - first to
single-core CPU saturation, then to cross-shard submission backpressure - and
both explanations died for want of exactly this measurement.

### Why measure it rather than read seastar's `smp` metrics

The obvious instinct is to look at seastar's cross-shard queue metrics. They
cannot answer it:

- All seven are registered `(sm::metric_disabled)`
  (`seastar/src/core/reactor.cc:3944-3957`), and there is no config or CLI path
  to enable them - only `set_relabel_configs`, which Redpanda never calls
  (METHODOLOGY #24).
- Even fully enabled they would not show the backlog. The queue that grows
  without bound under overload is `_tx.a.pending_fifo` on the **source** shard,
  and **it has no metric at all**. `send_queue_length` counts only what
  `move_pending` managed to push into the 128-slot ring minus completions
  (`reactor.cc:3804`/`3917`), so it **saturates at 128**; the three
  `*_batch_queue_length` gauges are the size of the *last* batch (≤16).

Measuring the transit latency directly is strictly more informative than the
queue depth would have been, and needs no seastar change.

### Reading it

- **Recorded on the destination shard**, like `consume_delay` - charged where the
  waiting happened, not where the push originated. Aggregated over the shard
  label like every other relay metric, so the scrape gives a per-broker
  distribution.
- **Single-clock.** `ss::steady_clock_type` is `CLOCK_MONOTONIC`, consistent
  across cores, so this is directly comparable with the other three stages - no
  cross-clock skew caveat of the kind that applies to produce→consume totals.
- **One stamp per push, not per destination shard.** A later shard's transit
  therefore also includes the submission cost of the shards ahead of it, bounded
  by (remote − 1) × per-submission cost. That was measured at 25–100 ns, so under
  ~2 µs even at fan-out 20 - negligible against a millisecond-scale effect, and
  taking one clock read per remote shard would multiply the reads on this hot
  path by the fan-out factor, which is how instrumentation starts changing what
  it measures.
- **Gated on `relay_stage_metrics_enabled`** like the other three, and **not**
  recorded for `push()`'s purely local delivery, where there is no transit.
  `relay_cross_shard_test` asserts both halves (recorded on destinations, not on
  the producer), with `fanout_duration` on the producer as the tripwire so the
  negative half cannot pass by the flag simply being off.

### What to conclude from it in Round 4

- transit small, `consume_delay` large -> the deferral is **consumer wakeup**;
  the cross-shard path is fine and shard-count is not the lever.
- transit large -> the deferral is in the **cross-shard path itself**, i.e.
  destination shards not draining the queue fast enough, and the `FANOUT_SHARDS`
  arm becomes the primary result rather than a control.
- both small while `transform_e2e` is milliseconds -> the time is somewhere none
  of these four stages covers, and the next thing to instrument is the
  transform's own three-loop pipeline, not the relay.
