# Latency during leadership movement

What an in-broker wasm deployment costs while its partition changes leader, and
how long until it is normal again. Produced by `wb run resilience` (see the
README section for how to run it).

Separate from [REPRODUCE.md](../REPRODUCE.md) on purpose: those tables are
steady state, and folding a disturbance into them would make them
incomparable.

All figures below: one AWS cluster, 3 x `r8id.8xlarge` brokers + 1 x
`c5n.9xlarge` client, `us-east-2c`, RF=3, 1,000 orders/sec, 650 B payload,
`acks=all`, `write.caching=true`, single input partition, opt build of
`transform-latency-instrumentation`. Measured 2026-09-09.

Receipts come from an ordinary Kafka fetch of the fills topic, **not** from an
in-broker relay consumer - see [Why not the relay path](#why-not-the-relay-path).

---

## The headline: it was a client default, not Redpanda

The first measurement showed a maintenance drain producing an e2e p99 of
**4.72 s** against a 648 us baseline. Almost all of it was one client setting.

| stage | baseline | during drain |
|---|---|---|
| `produce` (client -> quorum ack) | 636 us | **4,710,398 us** |
| `match` (ack -> guest stamp) | 23 us | 30,600 us |
| `relay_consume` (stamp -> receipt) | 70 us | 4,026 us |
| **`total`** | 648 us | **4,715,693 us** |

`produce` is 99.9% of it. Everything transform-attributable is ~35 ms.

Broker-side produce latency, sampled every 2 s across the same drain from
`redpanda_kafka_request_latency_seconds{redpanda_request="produce"}`, 46
windows carrying traffic:

| | value |
|---|---|
| mean | 0.179 ms |
| min | 0.162 ms |
| **max** | **0.565 ms** |

Flat. The broker's *worst* window is ~0.012% of the client-observed tail. And
during the transfer the broker received **fewer** produce requests (652 in a
2 s window against ~2,109 steady) at the **same** per-request latency -
records were not being processed slowly, they were not arriving.

### Cause

`franz-go@v1.21.5` defaults `metadataMinAge` to **5 s**
(`pkg/kgo/config.go:649`). That rate-limits how soon a client may re-fetch
metadata after a `NOT_LEADER`, so a record can wait ~5 s to learn the new
leader. The untuned max was **5,009,738 us** - the default, near-exactly. The
default retry backoff compounds it (250 ms escalating to a 5 s cap). franz-go's
floor for `metadataMinAge` is 10 ms.

Note franz-go retries transparently and succeeds, so loadgen recorded **zero
produce errors**. The error counters cannot distinguish retry latency from
broker latency; only the broker-side metric separates them.

### Effect of tuning the client

Same cluster, same drain, `metadata-min-age=100ms retry-backoff-max=500ms`:

| | untuned (5 s default) | tuned |
|---|---|---|
| produce p99 during drain | 4,712,698 us (4.71 s) | **610 us** |
| produce max during drain | 5,011,475 us (5.01 s) | **105,819 us** |
| total p99 during drain | 4,719,327 us | **1,375 us** |
| total max during drain | 5,020,085 us | **284,593 us** |
| duplicates (sampled) | 240-319 | 28 |

The tuned produce p99 during the drain (610 us) is *below* its own baseline
produce p99 (655 us): the tail left p99 entirely and survives only in `max`.
With a properly configured client a maintenance drain is close to invisible at
p99.

Duplicates fell too, as a second-order effect - a shorter leaderless window
leaves less work in flight when leadership moves.

**This improves a client number, not Redpanda.** The broker was already
handling produce in 0.18 ms throughout. `redpanda-wasm-clients` now sets these
options; `loadgen` exposes them as flags defaulting to franz-go's values, and
records both in every report's config fingerprint so a tuned run cannot be
mistaken for an untuned one.

---

## The graceful drain: duplicates versus tail

`data_transforms_graceful_transfer_timeout_ms` lets a transform finish and
commit in-flight work before leadership moves, instead of discarding it for the
next owner to reprocess. Reprocessing shows up downstream as duplicate output,
because a processor takes a fresh `producer_id` on every start and so cannot be
deduplicated across owners.

Measured with the **tuned** client (the untuned numbers were dominated by the
client artifact above and are not usable for this comparison):

| budget | duplicates | during p90 | during p99 |
|---|---|---|---|
| none | 196 | 1,036 us | 61,508 us |
| 200 ms | **0** | 973 us | 222,531 us |
| 500 ms | **0** | 991 us | 1,251 us |
| 1000 ms | **0** | 897 us | 961,276 us |
| 5000 ms | **0** | 92,519 us | 2,791,944 us |

**Every budget at or above 200 ms eliminates duplicates.** In-flight work is
about one batch, so seconds of budget buy nothing. The harness default was
lowered from 5000 ms to 1000 ms on the strength of this - 5x headroom over the
measured requirement, rather than a budget the drain cannot plausibly need.

**The tail column is not yet characterised, and should not be read as a
curve.** It looks like p99 tracks the budget at 200/1000/5000 ms, but the
500 ms point contradicts that outright - 1,251 us against its own 1,498 us
baseline, no visible spike at all. One run per budget, with baselines drifting
between 770 us and 1,498 us across the sweep, cannot support a relationship.
Whether p99 captures the stall depends on when the drain lands relative to the
sampled records. The duplicate column is the part that replicated.

### Why the drain cost anything at all: TWO causes, not one

The 5,000 ms arm's cost landed in `produce` (2.79 s of the 2.85 s total), which
is not where a consumer-side quiesce belongs. The relevant lock chain, verified
in source:

- `cluster::partition::transfer_leadership` takes the STM prepare lock, *then*
  calls `_raft->do_transfer_leadership()`
- `rm_stm::prepare_transfer_leadership()` is `_state_lock.hold_write_lock()`
- `rm_stm::do_replicate()` - the produce path - takes `hold_read_lock()`

The obvious reading is that the quiesce simply ran too late, and moving it to
the start of `cluster::partition::transfer_leadership` - before the prepare
phases - would fix it. That was done, and **it was not sufficient.** raft's
hook is deliberately kept, because `transfer_and_stepdown` (decommission) never
goes through `cluster::partition`, so on the transfer path BOTH hooks fire. The
claim that the second one "finds the work already done and costs nothing" holds
only when the first drain *succeeded*. When the transform is behind, the first
drain times out at its budget, and raft's hook then re-runs the whole budget
from behind the write lock - reproducing the exact pathology the move was meant
to remove.

`processor::drain()` is now single-shot per processor lifetime: it records the
first attempt's verdict and answers from it, with `start()` clearing it so each
new owner gets its own bounded chance.

### Measured, after both fixes

Local 3-broker cluster, one shard per broker so every partition on a node
shares one reactor thread, a guest burning 1 ms per record at 600 records/sec
(saturated, so every drain times out - the worst case for this defect), and a
`ctl-*` control partition led by the same node with no transform on it.
`in_max` is the transform's own input partition; both are client-observed
produce max over a 30 s window with the drain injected at 15 s.

| budget | in_max BEFORE the single-shot fix | in_max AFTER | control AFTER |
|---|---|---|---|
| none | 135 ms | 125 ms | 37 ms |
| 1000 ms | 1,140 ms | **158 ms** | 24 ms |
| 5000 ms | 5,171 ms | **130 ms** | 140 ms |

After the fix no budget appears in produce latency at all: each arm shows a
single ~125-158 ms spike, which is the leadership transfer itself, and overall
p99 across the three arms sits within 21-24 ms of itself. The 5,000 ms arm
improved 40x and is now indistinguishable from not draining.

The control column is what makes this a measurement rather than a story. A
saturated guest keeps running *during* a drain by design, so reactor starvation
predicts a stall proportional to the budget just as convincingly as a lock
does. The control partition, on the same single reactor, stayed flat while the
transform's partition tracked the budget - so starvation was not the cause. The
other tell was timing: the stall began one full budget *after* the injection,
which is the second drain, not the first.

Two caveats. These are a fastbuild broker, so read the arm-to-arm relationship
and not the absolute values - the ~10-14 ms p50 here would be sub-millisecond
on an opt build. And the budget still delays the **transfer** when a transform
is behind (the drain times out); what it no longer delays is **produces**,
which is the intended design.

A passthrough transform cannot find any of this: it never lags, so its drain
never times out, so the second drain really is free and the defect is invisible.

---

## Recovery

Every arm returned to baseline within ~20 s, on both the drained and undrained
paths.

Two caveats on how that is judged. The figures above were taken with a 30 s
baseline window and `RECOVERY_BAND=1.20`, which produced several false "DID NOT
RECOVER" verdicts on arms that were flat and healthy: those short baselines
came in at 770-890 us while the same cluster's post-disturbance steady state
sat at 1,025-1,180 us. Both have since been changed - the baseline window to
60 s, which fixes the cause, and the band to 1.35 to cover the residual drift -
so a rerun will not reproduce those verdicts. And session baselines drifted
upward monotonically (839 -> 890 -> 1,019 -> 1,250 -> 1,316 us total p99) across
both tuned and untuned runs, so cross-run baseline comparisons within a session
are unreliable - compare each arm against its own baseline only.

---

## Why not the relay path

The steady-state runners measure through an in-broker relay consumer
(`orders -> matcher -> fills -> relay consumer -> probe-out`). That path cannot
measure this experiment.

An unpinned relay consumer follows *its own* input partition's leadership, and
the relay has no cross-node hop. The first leadership move separated the
consumer from the matcher writing to fills and receipts stopped **permanently**  -
the arm survived one drain and then delivered nothing, with loadgen reporting
`relay drops, or a shard-locality mismatch`. Co-locating at setup does not
help, because the disturbance under test is what destroys the co-location.

So this series measures through an ordinary fills fetch, which is
leadership-agnostic, and deploys one transform rather than two so the
measurement does not have a second transform's placement as its own failure
mode. It costs absolute comparability with the published in-broker figures,
since it includes an external fetch - but every number here is a delta against
a baseline taken on the same cluster minutes earlier, so that offset cancels.

---

## What is not measured

- **Decommission.** Exercises `consensus::transfer_and_stepdown`, the path that
  bypasses `cluster::partition::transfer_leadership`. Covered by unit tests
  against a real raft group, not on a cluster.
- **Broker restart.** Stubbed in `run-resilience.sh`; needs the ansible/ssh
  path. Maintenance and explicit transfer cover leadership movement without
  stopping a process.
- **Multi-partition transforms.** Everything here is a single input partition,
  which is the worst case for cold starts (only one broker is ever warm) and
  the simplest case for the drain (one processor to quiesce).
- **The drain's tail cost after the placement fix**, as noted above.
