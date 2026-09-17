# Reproducing the published results

Two results, both from the same cluster and the same byte-identical wasm
module. Read [Reading these numbers honestly](#reading-these-numbers-honestly)
before quoting any of them, since a few are easy to misread in a way that
overstates the case.

Prerequisites and cost are in [README.md](README.md). Budget ~$40/hr for 1-2
hours.

The cluster: 3 x `r8id.8xlarge` brokers, 8 x `c5n.9xlarge` clients and one
`m6i.2xlarge` monitoring host in `us-east-1a`, RF=3, `acks=all`,
`write.caching=true`, producer linger 0, one input partition, an opt build of
`transform-latency-instrumentation`, and matcher module
`94c3d0177a754b9f0c1df80c10dea298`. Measured 2026-09-16. `num_cores` is 13 per
broker, not 32, since SMT is off and a core is reserved for the OS.

Host clocks were synced to Amazon Time Sync with a measured RMS offset of
13.6 us, which is the error bar on `total` and is recorded in each run's own
`clock-sync.json`. No trial reported `cross_clock_unusable`.

## Result 1: in-broker wasm, 500 consumers

500 in-broker consumers, 1,000 orders/sec, 650 B payload.

Three trials, all `clean:true`, 30,000 receipts each, zero missing and zero
duplicate, binary gate OK on all three.

| stage | p50 | p90 | p99 |
|---|---|---|---|
| `produce` (client to quorum ack) | 666 us | 797 us | 1,000 us |
| `relay_consume` (matcher stamp to receipt) | 339 us | 425 us | 1,026 us |
| `total`, which is e2e | 751 us | 920 us | 1,483 us |

Figures are medians across the three trials. The three came in at 739 / 986 /
751 us at p50, so trial-to-trial spread on this arm is wide, around 34%
between the fastest and slowest trial. Run at least three and report the
median.

The fan-out was complete rather than merely fast. Each trial delivered
30,000,000 records to 500 consumers, `relay_delivered_per_logical_push` came
out at exactly 500, and `relay_dropped_delta` was zero. A fast in-broker
number with drops is the latency of an incomplete stream, so check that field
before quoting anything here.

Live `emit_to_guest`, the in-broker fan-out path itself, read 83 us at p50 and
223 us at p99 during a window. That is the span to quote for the relay, and it
is not the same thing as `relay_consume` above, which is measured over a wider
span on the broker-guest clock.

```bash
INBROKER_CONSUMERS=500 RATE=1000 PAYLOAD=650 RF=3 ACKS=all \
  ./scripts/wb reproduce
```

## Result 2: traditional deployment, 1 to 500 external consumers

Same cluster, same module, hosted by `wasm-client` on its own instance. No
in-broker wasm and no relay on either side of the measurement.

| deployment | consumers | e2e p50 | e2e p90 | e2e p99 | produce p50 |
|---|---|---|---|---|---|
| internal wasm (in-broker + relay) | 500 in-broker | 751 us | 920 us | 1,483 us | 666 us |
| traditional | 1 external | 1,387 us | 1,704 us | 2,040 us | 578 us |
| traditional | 10 external | 1,722 us | 2,164 us | 2,629 us | 750 us |
| traditional | 50 external | 2,049 us | 2,461 us | 2,859 us | 606 us |
| traditional | 500 external | 13,414 us | 17,060 us | 18,830 us | 963 us |

Every row held pacing and delivered 30,000 of 30,000 receipts with none
missing. `send_lateness` p99 by level: 5, 21, 7 and 410 us against a 1,000 us
interval. The 500-consumer row is the closest to its budget, at 41% of one
pacing slot, which is the level that once starved a single-client rig and is
why the fleet is eight instances.

```bash
EXTERNAL_LADDER="1 10 50 500" RATE=1000 PAYLOAD=650 RF=3 ACKS=all \
  ./scripts/wb reproduce
```

### Ratios at matched consumer count, 500 vs 500

| | ratio |
|---|---|
| p50 | 17.9x |
| p90 | 18.6x |
| p99 | 12.7x |

Quote the conclusion rather than the constant. "In-broker is roughly an order
of magnitude better at 500 consumers" holds up. The specific multiple doesn't,
because the traditional 500-consumer point is the least stable number in the
suite and moves substantially between cluster instantiations. At 500 external
consumers all fetching one topic the broker's fetch path is near saturation,
and that regime depends on partition leadership placement, which broker owns
the fills partition, and per-instance EC2 variance. If you need a single
figure, say roughly 10x and note that the traditional point varies.

## What the data shows

In-broker at 500 consumers has a better p99 (1,483 us) than the traditional
deployment's p99 at ONE consumer (2,040 us). That is the strongest single line
in the dataset, it holds at the tail rather than only at the median, and it has
survived every instantiation this has been measured on.

The fan-out is complete, not just fast. 30,000,000 deliveries per trial to 500
consumers, `relay_delivered_per_logical_push` exactly 500, zero dropped. The
comparison is between two arms that both delivered everything.

Do not read a trend into the ratio across quantiles. This run came out 17.9x at
p50, 18.6x at p90 and 12.7x at p99, so the advantage narrows at the tail. The
2026-09-12 run on the same topology came out the other way round, 13.6x at p50
against 16.3x and 16.4x at p90 and p99, widening at the tail. Same instance
types, same module, same AZ, opposite shape. Whatever governs the tail ratio is
not stable between cluster instantiations, so "the advantage widens at the
tail" is not a finding this data supports in either direction.

The in-broker tail is not reliably tight either. Here p50 to p99 spans 751 to
1,483 us, a 1.98x spread; on the previous run the same span was 1.15x. The
traditional 500-consumer spread is the more stable of the two, 1.40x here
against 1.39x before.

The big jump is between 50 and 500 external consumers, 2,049 to 13,414 us at
p50, a 6.6x step against a 10x step in consumer count. From 1 to 500 the growth
is 9.7x at p50 and 9.2x at p99, while from 10 to 50 p50 moves 19% and p99 only
9%. That shape is consistent with fetch-path scheduling rather than a smooth
per-consumer cost.

`produce` varies more between clusters than the architecture does, and that is
the most important caveat on this page. Two runs of this identical single-AZ
topology, same module, same AZ, a day apart:

| | 2026-09-16 | 2026-09-17 |
|---|---|---|
| `produce` p50 | 666 us | 1,126 us |
| `relay_consume` p50 | 339 us | 397 us |
| `total` p50 | 751 us | 1,246 us |

The total moved 495 us and 460 us of that is `produce`, a leg that is identical
in both arms and has nothing to do with the relay. The relay path moved 58 us.
That is not a clock artifact: `produce` is client-side ack minus client-side
send, single-clock, and the residuals were smaller on the slower run. Both runs
sent 60,000 records with zero produce errors and held the offered rate.

So `relay_consume` is the reproducible number here and `total` is not, because
`total` is dominated by a leg that varies with whatever hardware the AZ gave
you. Quote the ratio as roughly an order of magnitude, and if you need one
stage to compare across runs, use `relay_consume`.

Each run now records what it cannot control: `clock-sync.json` for the
cross-clock error bar, `network-baseline.json` for the client-to-broker RTT,
and `broker-network-baseline.json` for the broker-to-broker RTT that carries
raft replication. The brokers also sit in a cluster placement group by default
(`broker_placement_group`), which removes rack distance as one candidate
explanation - though not noisy neighbours, hardware generation, or
instance-store variance.

The RTT files report a target as `"reachable": false` when ping got nothing
back, rather than as a zero. If you see that, the cluster's security group is
probably missing the icmp rule inside the VPC, and a `terraform apply` adds it
in place without replacing any instance. See docs/METHODOLOGY.md #31.

The older framing of this paragraph follows, and still holds within a run. It reads 578, 750, 606 and 963 us across the traditional ladder,
and the in-broker arm's own three trials put it at 666, 899 and 625 us. Both
arms are a client producing to a quorum, so this leg should be identical and
stable across all of them, and it varies by 1.7x instead. Treat it as cluster
noise on this instantiation rather than a property of either deployment. The
trial that produced the slowest in-broker total (986 us) is also the one with
the slowest produce (899 us) and the highest send lateness (151 us), which is
what noise in this leg looks like.

The divergence that matters is downstream of produce. Fan-out is nearly free
in-broker and roughly linear in cost traditionally, because every additional
traditional consumer is another full copy over the network.

## Reading these numbers honestly

The two arms measure at different endpoints, and that's the architecture
rather than a measurement artifact. The in-broker figure ends when an
in-broker consumer receives the record. The traditional figure ends when an
external client receives it over the network. In the in-broker deployment
there is no network hop, because the consumer lives in the broker, and that's
what's being sold. But the 751 us doesn't include a fetch out to an external
client, and any comparison that doesn't say so is overstating the case.

In-broker relay consumers read pre-commit. A relay consumer sees the record
before replication completes, so part of the latency win is a durability
trade. Disclose it.

`match` is a race rather than a duration. The matcher stamps `matched_at_nanos`
inside the guest right after local append, while the producer's `acks=all`
acknowledgement returns several hundred microseconds later. So `match` is
legitimately negative and must never be quoted as latency. A run reporting
negative `match` on well-synced hosts is correct rather than broken.
`docs/METHODOLOGY.md` covers the version of this that was misdiagnosed as
clock skew.

A row without pacing held isn't a result. `loadgen` fails a run whose
`send_lateness` p99 exceeds one pacing interval, because a CPU-starved
generator emits the right number of orders at the wrong times, and the same
starvation delays its receipt handling. The 500-consumer traditional row was
originally measured on a single client instance and passed every other gate:
`attempted_ratio` 99.99%, `client_bound` false, lag never grew, while
`send_lateness` p99 sat 2.5 pacing slots late. Re-running it across dedicated
instances dropped pacing error 4.4x, which is how we know the figure is
broker-side fan-out rather than the test rig.

Instance types are part of the result. `r8id.8xlarge` brokers, `c5n.9xlarge`
clients. The same instance name on a different hardware generation isn't the
same machine, so a run on other types is a different experiment.

Region is part of the fingerprint. `select-region.sh` will fall back to
another region when capacity is short, and warns when it does. Absolute
latencies aren't comparable across regions. Record the region with any result.

## If your numbers differ

Work through these in order:

1. Is every row `clean:true` with pacing held? An unclean row isn't a slow
   result, it's no result.
2. Same module? Run `make checksum` in the clients repo. These runs used
   `94c3d0177a754b9f0c1df80c10dea298`.
3. Same instance types and region? See above.
4. Enough client instances? `size-clients.sh` derives this. Running the
   500-consumer level on fewer than 8 clients measures your load generator.
5. Is `RISK_LIMIT` the same on both arms? The in-broker leg passes
   `--var RISK_LIMIT=0` and the external host takes `RISK_LIMIT=0`. Mismatched,
   the two arms run different logic.
6. Producer linger 0 everywhere? franz-go defaults to 10 ms, which
   historically accounted for most of this workload's millisecond-scale
   figures.
