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

### How the defect was located, locally

A local 3-broker cluster at one shard per broker, so every partition on a node
shares one reactor thread, with a guest burning 1 ms per record at 600
records/sec (saturated, so every drain times out - the worst case for this
defect) and a `ctl-*` control partition led by the same node with no transform
on it. Client-observed produce max over a 30 s window, drain injected at 15 s:

| budget | input partition BEFORE | AFTER | control AFTER |
|---|---|---|---|
| none | 135 ms | 125 ms | 37 ms |
| 1000 ms | 1,140 ms | **158 ms** | 24 ms |
| 5000 ms | 5,171 ms | **130 ms** | 140 ms |

The control column is what makes this a measurement rather than a story. A
saturated guest keeps running *during* a drain by design, so reactor starvation
predicts a stall proportional to the budget just as convincingly as a lock
does. The control partition, on the same single reactor, stayed flat while the
transform's partition tracked the budget - so starvation was not the cause. The
other tell was timing: the stall began one full budget *after* the injection,
which is the second drain, not the first.

A passthrough transform cannot find any of this: it never lags, so its drain
never times out, so the second drain really is free and the defect is invisible.
Those figures are a fastbuild broker - read the arm-to-arm relationship, not
the absolute values.

### Quotable numbers, opt build on AWS

3 x `r8id.8xlarge` + 1 x `c5n.9xlarge`, `us-east-2c`, RF=3, 1,000 orders/sec,
650 B, `acks=all`, single input partition, opt build carrying both fixes.
Measured 2026-09-09. Each arm's own 60 s baseline, 30 s during-window.

| arm | produce p99 | e2e p99 | duplicates |
|---|---|---|---|
| maintenance, no drain | 646 us | 1,536 us | 30 |
| maintenance, 200 ms | - | 114,464 us | **0** |
| maintenance, 1000 ms | **689 us** | 875,620 us | **0** |
| transfer, no drain | 562 us | 1,466 us | 42 |
| transfer, 1000 ms | 523 us | 1,134 us | **0** |

**The produce path is fixed.** A maintenance drain with a 1000 ms budget costs
689 us of produce p99 against 646 us with no drain - about 43 us. The same
measurement before these fixes was **2.79 s**. Nothing about the budget appears
in produce latency any more.

**The remaining cost is e2e, it is real, and it scales with the budget.** A
drain stops the CONSUMER immediately and the transfer then waits out the
budget, so records arriving in that window are not transformed until the new
owner starts. That is inherent to draining, not a defect.

**It is paid in full under maintenance, and not at all under a targeted
transfer** (875 ms vs 1,134 us at the same budget). Maintenance moves the
OUTPUT topic's leadership too, so the transform's writes cannot land, its
commits cannot complete, and the drain runs to its deadline every time. A
targeted transfer leaves the output topic alone and the drain finishes in
milliseconds. Note the timed-out drain still eliminated duplicates - the
flush-on-timeout path does useful work.

So the budget should be the smallest value that eliminates duplicates, which
is why the harness default is now 200 ms and not 1000 ms: identical correctness
for an eighth of the e2e cost. An earlier "keep 5x headroom" argument for
1000 ms was wrong, because headroom is not free here.

This run is also the first to show BOTH halves on one cluster - the no-drain
arms emit duplicates (30 and 42) and the drain arms eliminate them - so "the
drain costs the produce path nothing" is a statement about a drain that
demonstrably did work, rather than about one that had nothing to do.

### Second run, with the full field set

Same cluster shape, 200 ms budget (the new default), all four arms:

| arm | produce p99 | total p99 | total max | over 10 ms | dups |
|---|---|---|---|---|---|
| maintenance, no drain | 592 -> **596** | 1,307 -> 1,319 | 9,389 -> 6,847 | 0 -> 0 | 0 |
| maintenance, 200 ms | 524 -> **799** | 1,160 -> 223,811 | 3,777 -> 376,286 | 0 -> 732 | **0** |
| transfer, no drain | 573 -> **732** | 1,508 -> 1,292 | 202,304 -> 205,692 | 386 -> 198 | 42 |
| transfer, 200 ms | 592 -> **684** | 1,307 -> 66,874 | 21,532 -> 364,146 | 13 -> 358 | **0** |

Produce p99 stays between 596 and 799 us in every arm, so the produce-path fix
holds across both actions and both budgets. The e2e cost continues to scale
with the budget: 224 ms at a 200 ms budget against 875 ms at 1000 ms.

What the counts add: on the 200 ms maintenance arm, 732 of 30,000 sampled
records (2.4%) exceeded 10 ms and 550 (1.8%) exceeded 100 ms, while p50 and p90
were untouched at 868 / 945 us. "p99 = 224 ms" on its own reads as a
fleet-wide stall; it is closer to a 2% tail.

And the timeline shows the disturbance is several discrete events rather than
one continuous stall - consistent with maintenance moving each partition
independently, each draining separately:

```
t=+3000ms   n=500  p50=928us     p99=371,344us  max=376,286us
t=+3500ms   n=500  p50=898us     p99=194,432us  max=199,412us
t=+13000ms  n=500  p50=863us     p99=5,587us    max=10,311us
t=+23000ms  n=500  p50=120,605us p99=363,659us  max=367,795us
```

The last bucket has a p50 of 120 ms - half the records offered in that 500 ms
were affected. No window aggregate conveys that.

One caveat on duplicates: `transfer, no drain` reported 42 in both runs, but
`maintenance, no drain` reported 30 in the first run and 0 in the second. The
no-drain duplicate count is not reliably reproducible, so treat "the drain
eliminates duplicates" as established by the arms where the no-drain side
actually produced some.

---

## Recovery

Every arm returned to baseline within ~20 s, on both the drained and undrained
paths - with one exception that is worth understanding, because it recurs.

### A first-arm "DID NOT RECOVER" happened once and did not reproduce

On the first 2026-09-09 opt run the first arm reported DID NOT RECOVER within
180 s, sitting at 1,066-1,478 us against its own 740 us baseline, and that
run's per-arm baselines drifted monotonically: 740 -> 1,067 -> 1,114 ->
1,195 us. The explanation offered at the time was that all arms in one
invocation share one set of topics (`orders-res-$RUN_ID`), so the log grows
underneath the series and arm 1's baseline is measured on an almost empty log
while its recovery windows run against a much larger one.

**A second run with the identical harness did not reproduce it.** Baselines
came in at 1,307 -> 1,160 -> 1,508 -> 1,307 us - no trend - and every arm
recovered inside 20 s. The shared-topic design was unchanged, so a mechanism
driven by log growth would have to produce the drift every time. It did not.
Treat the log-growth account as unconfirmed; the observation was real for that
run, the explanation was over-claimed.

`leader_balancer_mute_timeout` is worth knowing separately: it defaults to
300 s, longer than the 180 s recovery budget, so a node leaving maintenance
genuinely cannot be given leadership back within the measurement. That is a
real effect on post-maintenance balance, but it is not what produced the
verdict - the arms that recovered were under the same mute.

### What the richer fields DID establish: the baselines are not quiet

The second run's `transfer-nodrain` **baseline** carried 386 records over
10 ms and 206 over 100 ms, with p999 172 ms and max 202 ms - while its
p50/p90/p99 read a clean 891 / 1,072 / 1,508 us. The timeline shows it as two
discrete ~200 ms events inside an undisturbed window:

```
t=+28500ms  p99=196,853us  max=202,304us
t=+52000ms  p99=196,903us  max=202,252us
```

Arms run back to back with no settling period, so one arm's residual can land
in the next arm's baseline, and a baseline containing 200 ms outliers makes any
"return to baseline" judgement meaningless in both directions. This is a better
supported account of cross-arm contamination than log growth, and it was
invisible until max/p999/over-threshold counts were reported - which is the
argument for reporting them.

The fix that follows is a settling period between arms, and defining
"recovered" as "p99 has stabilised" rather than "p99 is back under a number
measured earlier".

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

## What each window reports

The series reports the same per-stage quantiles the published tables carry
(`scripts/render-tables.py`: p50/p90/p99 for `produce`, `relay_consume` and
`total`), so a disturbance run and a steady-state run are read the same way,
plus the fields a disturbance needs that a steady-state table does not.

| field | why a disturbance needs it |
|---|---|
| `produce` p50/p99 | says WHERE the cost landed. Reporting `total` alone once made a drain look like a produce regression when produce was 689 us inside an 875 ms total - the cost was the transform not consuming, a different problem with a different fix |
| `max`, `p999` | a leadership move's cost can leave p99 entirely and survive only in max; that already happened in this doc's own client-tuning figures, so p99 alone can show a disturbance as free when it is not |
| `over 10ms` / `over 100ms` counts | how MANY records were affected, not how bad for the worst few. One 900 ms record and three hundred of them give nearly the same p99 and mean completely different things |
| `mean` | the aggregate cost. On the 1000 ms arm, p50 815 us and p90 916 us were untouched while mean was 24 ms - i.e. the hit was confined to about the top 1%, which "p99 = 875 ms" overstates as a fleet-wide impact |
| `send_lateness` p99 | a load generator that fell behind reports its own queueing as system latency. render-tables REJECTS a level for this; a disturbance is exactly when the client is most likely to fall behind, so the series warns when it exceeds one pacing interval |
| `timeline` | per-interval p50/p99/max bucketed by SEND time (`-timeline-ms`, 500 ms here). An aggregate cannot say when a stall began or how long it lasted, and those identify the mechanism: a stall starting one drain-budget AFTER the injection is a second drain, one starting at the injection is the transfer itself, and both give the same window aggregate |

Bucketing by send time rather than receipt time matters: a record delayed by
seconds belongs to the moment it was offered, or the stall is reported as
having happened after it ended.

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
