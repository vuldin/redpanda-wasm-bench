# Reproducing the published results

Two results, both from the same cluster and the same byte-identical wasm module.
Read [Reading these numbers honestly](#reading-these-numbers-honestly) before
quoting any of them — several are easy to misread in a way that overstates the
case.

Prerequisites and cost are in [README.md](README.md). Budget ~$40/hr, 1–2 hours.

---

## Result 1 — in-broker wasm, 500 consumers

500 in-broker consumers, 1,000 orders/sec, 650 B, RF=3, `acks=all`, 1 partition,
`write.caching=true`, producer linger 0.

Three trials, all `clean:true`, 30,000 receipts each, **zero missing, zero
duplicate**, binary gate OK on all three.

| stage | p50 | p90 | p99 |
|---|---|---|---|
| `produce` (client → quorum ack) | 555 µs | 597 µs | 749 µs |
| `relay_consume` (matcher stamp → receipt) | 377 µs | 401 µs | 433 µs |
| **`total` — this is e2e** | **824 µs** | **932 µs** | **1,073 µs** |

Broker-side `emit_to_guest`, the fan-out cost itself, 40 M samples/trial:
**mean 74 µs, p50 ~81, p90 ~124, p99 ~230 µs**.

Figures are medians across the three trials (p50: 818/881/824; p90: 860/932/950;
p99: 1,073/1,056/1,105) — trial-to-trial spread ~8% at p50 and under 5% at p99.

```sh
INBROKER_CONSUMERS=500 RATE=1000 PAYLOAD=650 RF=3 ACKS=all \
  ./scripts/wb run-inbroker --trials 3
```

## Result 2 — traditional deployment, 1 → 500 external consumers

Same cluster, same module, hosted by `wasm-client` on its own instance. No
in-broker wasm and no relay on either side of the measurement.

| deployment | consumers | e2e p50 | e2e p90 | e2e p99 | produce p50 |
|---|---|---|---|---|---|
| internal wasm (in-broker + relay) | 500 in-broker | 824 µs | 932 µs | 1,073 µs | 555 µs |
| traditional | 1 external | 1,052 µs | 1,314 µs | 1,698 µs | 442 µs |
| traditional | 10 external | 1,116 µs | 1,403 µs | 2,276 µs | 402 µs |
| traditional | 50 external | 1,527 µs | 1,999 µs | 2,507 µs | 490 µs |
| traditional | 500 external | 7,776 µs | 9,816 µs | 11,178 µs | 443 µs |

Every row had pacing held (`send_lateness` p99 under the 1,000 µs interval).

```sh
EXTERNAL_LADDER="1 10 50 500" RATE=1000 PAYLOAD=650 RF=3 ACKS=all \
  ./scripts/wb run-traditional
```

### Ratios at matched consumer count (500 vs 500)

| | ratio |
|---|---|
| p50 | **9.44×** |
| p90 | **10.53×** |
| p99 | **10.42×** |

> **These ratios are from one cluster and did not reproduce on a second.**
> A fresh cluster with identical config put the same comparison at 17.6× / 20.7× /
> 20.3×, because the traditional 500-consumer point is unstable across clusters.
> See [Reproduction outcome](#reproduction-outcome-2026-09-04--read-this-before-quoting-a-ratio)
> before publishing any single figure.

## What the data actually shows

**The advantage widens at the tail.** 9.4× at the median, 10.5× at p90 and p99
in this run (17.6× / 20.7× reproduced on a second cluster — the direction holds,
the constant does not).
The in-broker distribution stays tight — 824 → 1,073 µs is a 1.30× p50→p99
spread — while the traditional deployment at 500 consumers spreads 7,776 →
11,178 µs, a 1.44× spread.

**In-broker at 500 consumers has a better p99 (1,073 µs) than the traditional
deployment's p99 at ONE consumer (1,698 µs).** That is the strongest single line
in the dataset, and it holds at the tail rather than only at the median.

**The traditional tail is elevated before its median catches up.** From 1 → 500
consumers, p50 grows 7.4× while p99 grows 6.6×; between 10 and 50 consumers p99
barely moves (2,276 → 2,507 µs) while p50 climbs 37%. That shape is consistent
with fetch-path scheduling rather than a smooth per-consumer cost.

**`produce` is the same leg in both arms** — 442–555 µs either way, as it should
be, since both are a client producing to a quorum. The entire divergence is
downstream. That is the finding: fan-out is nearly free in-broker and roughly
linear in cost traditionally, because every additional traditional consumer is
another full copy over the network.

---

## Reproduction outcome, 2026-09-04 — read this before quoting a ratio

The tables above were reproduced from scratch on a fresh cluster with identical
measurement config (verified field by field). Result: **the in-broker figures
and the low traditional levels reproduce well; the 500-consumer traditional
point does not, and therefore neither does the headline ratio.**

| measurement | published | reproduced | |
|---|---|---|---|
| in-broker e2e p50 | 824 µs | 784 µs | within 5% |
| in-broker e2e p99 | 1,073 µs | 978 µs | within 9% |
| traditional, 1 consumer | 1,052 µs | 992 / 1,006 µs | within 6% |
| traditional, 10 | 1,116 µs | 1,223 / 1,185 µs | within 10% |
| traditional, 50 | 1,527 µs | 1,605 / 1,706 µs | within 12% |
| **traditional, 500** | **7,776 µs** | **13,234 / 14,372 µs** | **1.7–1.8× higher** |
| **ratio at p50** | **9.44×** | **17.6×** | **not reproducible** |

Both runs are individually valid: pacing held on every row (`send_lateness` p99
559 / 561 µs against a 1,000 µs interval), 30,000/30,000 receipts, zero missing,
identical config, and the produce leg is unchanged (443 vs 451 µs). The whole
divergence is downstream — 7,333 µs vs 12,783 µs of fetch and fan-out.

**What this means.** At 500 external consumers all fetching one topic, the
broker's fetch path is near saturation, and that regime is not stable across
cluster instantiations: partition leadership placement, which broker owns the
fills partition, and per-instance EC2 variance all move it substantially. This
is consistent with the shape already noted below — the traditional tail is
elevated before its median catches up, which is scheduling-dominated behaviour
rather than a smooth per-consumer cost.

**So quote the conclusion, not the constant.** "In-broker is roughly an order of
magnitude better at 500 consumers" is robust: two independent runs put it at
9.4× and 17.6×, and the direction and scale are never in question. "9.4×" as a
figure is not reproducible and should not be published as one. If a single
number is needed, quote **≥9×** and say the traditional 500-consumer point
varies by up to 1.8× between clusters.

The three-trial spread reported for the in-broker arm (~8% at p50) measured
run-to-run variance *within one cluster*. It says nothing about variance
*between* clusters, which is what this reproduction exposed. That is a general
caution, not specific to this workload.

## Reading these numbers honestly

**The two arms measure at different endpoints, and that is the architecture, not
a measurement artifact.** The in-broker figure ends when an in-broker consumer
receives the record; the traditional figure ends when an external client
receives it over the network. In the in-broker deployment there *is* no network
hop, because the consumer lives in the broker — that is what is being sold. But
the 824 µs does **not** include a fetch out to an external client, and any
comparison that does not say so is overstating the case.

**In-broker relay consumers read pre-commit.** A relay consumer sees the record
before replication completes, so part of the latency win is a durability trade.
This must be disclosed, not buried.

**`match` is a race, not a duration.** The matcher stamps `matched_at_nanos`
inside the guest right after local append, while the producer's `acks=all`
acknowledgement returns several hundred microseconds later. So `match` is
legitimately *negative* and must never be quoted as latency. A run reporting
negative `match` on well-synced hosts is correct, not broken —
`docs/METHODOLOGY.md` #27 covers the version of this that was misdiagnosed as
clock skew.

**A row without pacing held is not a result.** `loadgen` fails a run whose
`send_lateness` p99 exceeds one pacing interval, because a CPU-starved generator
emits the right *number* of orders at the wrong *times*, and the same starvation
delays its receipt handling. The 500-consumer traditional row was originally
measured on a single client instance and read 7,629 µs with **every other gate
green** — `attempted_ratio` 99.99%, `client_bound` false, lag never grew — while
`send_lateness` p99 was 2,486 µs. Re-running it across six dedicated instances
dropped pacing error 4.4× and moved e2e p50 by 2%, which is how we know the
7,776 µs is broker-side fan-out and not the test rig.

**Instance types are part of the result.** `r8id.8xlarge` brokers,
`c5n.9xlarge` clients. The same instance name on a different hardware
generation is not the same machine, so a run on other types is a different
experiment.

**Region is part of the fingerprint.** `select-region.sh` will fall back to
another region when capacity is short, and warns when it does. Record the region
with any result.

## If your numbers differ

Work through these in order:

1. **Is every row `clean:true` with pacing held?** An unclean row is not a slow
   result, it is no result.
2. **Same module?** `make checksum` in the clients repo. The published runs used
   `94c3d0177a754b9f0c1df80c10dea298`.
3. **Same instance types and region?** See above.
4. **Enough client instances?** `size-clients.sh` derives this; running the
   500-consumer level on fewer than 8 clients measures your load generator.
5. **`RISK_LIMIT` the same on both arms?** The in-broker leg passes
   `--var RISK_LIMIT=0`; the external host takes `RISK_LIMIT=0`. Mismatched,
   the two arms run different logic.
6. **Producer linger 0 everywhere?** franz-go defaults to 10 ms, which
   historically accounted for most of this workload's millisecond-scale
   figures.
