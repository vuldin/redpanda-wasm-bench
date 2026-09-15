# Reproducing the published results

Two results, both from the same cluster and the same byte-identical wasm
module. Read [Reading these numbers honestly](#reading-these-numbers-honestly)
before quoting any of them, since a few are easy to misread in a way that
overstates the case.

Prerequisites and cost are in [README.md](README.md). Budget ~$40/hr for 1-2
hours.

The cluster: 3 x `r8id.8xlarge` brokers and 8 x `c5n.9xlarge` clients in
`us-east-1a`, RF=3, `acks=all`, `write.caching=true`, producer linger 0, one
input partition, an opt build of `transform-latency-instrumentation`, and
matcher module `94c3d0177a754b9f0c1df80c10dea298`.

## Result 1: in-broker wasm, 500 consumers

500 in-broker consumers, 1,000 orders/sec, 650 B payload.

Three trials, all `clean:true`, 30,000 receipts each, zero missing and zero
duplicate, binary gate OK on all three.

| stage | p50 | p90 | p99 |
|---|---|---|---|
| `produce` (client to quorum ack) | 831 us | 900 us | 1,007 us |
| `relay_consume` (matcher stamp to receipt) | 355 us | 380 us | 413 us |
| `total`, which is e2e | 958 us | 1,013 us | 1,103 us |

Figures are medians across the three trials. The three came in at 981 / 704 /
958 us at p50, so trial-to-trial spread on this arm is wide, around 39%
between the fastest and slowest trial. Run at least three and report the
median.

Broker-side `emit_to_guest`, which is the fan-out cost itself, averaged 79 us
over 40,000,000 samples per trial.

```bash
INBROKER_CONSUMERS=500 RATE=1000 PAYLOAD=650 RF=3 ACKS=all \
  ./scripts/wb reproduce
```

## Result 2: traditional deployment, 1 to 500 external consumers

Same cluster, same module, hosted by `wasm-client` on its own instance. No
in-broker wasm and no relay on either side of the measurement.

| deployment | consumers | e2e p50 | e2e p90 | e2e p99 | produce p50 |
|---|---|---|---|---|---|
| internal wasm (in-broker + relay) | 500 in-broker | 958 us | 1,013 us | 1,103 us | 831 us |
| traditional | 1 external | 1,615 us | 1,963 us | 2,385 us | 500 us |
| traditional | 10 external | 1,742 us | 2,071 us | 2,441 us | 496 us |
| traditional | 50 external | 2,109 us | 2,553 us | 3,027 us | 509 us |
| traditional | 500 external | 13,057 us | 16,530 us | 18,109 us | 835 us |

Every row held pacing, with `send_lateness` p99 well inside the 1,000 us
interval, and delivered 30,000 of 30,000 receipts with none missing.

```bash
EXTERNAL_LADDER="1 10 50 500" RATE=1000 PAYLOAD=650 RF=3 ACKS=all \
  ./scripts/wb reproduce
```

### Ratios at matched consumer count, 500 vs 500

| | ratio |
|---|---|
| p50 | 13.6x |
| p90 | 16.3x |
| p99 | 16.4x |

Quote the conclusion rather than the constant. "In-broker is roughly an order
of magnitude better at 500 consumers" holds up. The specific multiple doesn't,
because the traditional 500-consumer point is the least stable number in the
suite and moves substantially between cluster instantiations. At 500 external
consumers all fetching one topic the broker's fetch path is near saturation,
and that regime depends on partition leadership placement, which broker owns
the fills partition, and per-instance EC2 variance. If you need a single
figure, say roughly 10x and note that the traditional point varies.

## What the data shows

The advantage widens at the tail. 13.6x at the median against 16.3x and 16.4x
at p90 and p99. The in-broker distribution stays tight, 958 to 1,103 us being
a 1.15x p50-to-p99 spread, while the traditional deployment at 500 consumers
spreads 13,057 to 18,109 us, a 1.39x spread.

In-broker at 500 consumers has a better p99 (1,103 us) than the traditional
deployment's p99 at one consumer (2,385 us). That's the strongest single line
in the dataset, and it holds at the tail rather than only at the median.

The traditional tail is elevated before its median catches up. From 1 to 500
consumers p50 grows 8.1x while p99 grows 7.6x, and between 10 and 50 consumers
p99 moves 24% while p50 climbs 21%. The big jump is entirely between 50 and
500. That shape is consistent with fetch-path scheduling rather than a smooth
per-consumer cost.

`produce` is the same leg in both arms, and it's the one number here that
isn't clean. It reads 496-509 us on the low traditional levels but 831 us
in-broker and 835 us at 500 external, and the in-broker arm's own three trials
put it at 581, 831 and 886 us. Both arms are a client producing to a quorum,
so this leg should be identical and stable. Treat the elevated produce figures
as cluster noise on this instantiation rather than as a property of either
deployment. It inflates the in-broker total, which means the 13.6x ratio is
conservative.

The divergence that matters is downstream of produce. Fan-out is nearly free
in-broker and roughly linear in cost traditionally, because every additional
traditional consumer is another full copy over the network.

## Reading these numbers honestly

The two arms measure at different endpoints, and that's the architecture
rather than a measurement artifact. The in-broker figure ends when an
in-broker consumer receives the record. The traditional figure ends when an
external client receives it over the network. In the in-broker deployment
there is no network hop, because the consumer lives in the broker, and that's
what's being sold. But the 958 us doesn't include a fetch out to an external
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
