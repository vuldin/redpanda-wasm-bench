# The sweeps: what each one varies, pins, and cannot isolate

Reference for `run-e2-throughput.sh` and `run-e1-ceiling.sh`. Read this before
running one, and before quoting a number out of one.

Every sweep obeys the same contract: **one variable per mode, everything else
pinned and written into the run's fingerprint header.** A result whose
fingerprint differs in any other field is not comparable.

Drive them through `wasm-transforms-fanout-bench/wb run e2 MODE=... K=V ...`,
which supplies brokers, admin endpoints and a run id.

---

## `run-e2-throughput.sh`

| MODE | Varies | Answers | Levels default |
|---|---|---|---|
| `rate` | offered orders/sec | per-record cost and the saturation knee (E2.1) | 1000 5000 10000 50000 100000 |
| `payload` | bytes per record | splits per-record from per-byte cost (E2.2) | 0 64 512 4096 |
| `guest` | matcher vs passthrough | splits engine floor from guest work (E2.3) | matcher passthrough |
| `partitions` | input partition count | whether throughput scales with partitions, and whether processors reach other cores (E2.4) | 1 2 4 8 13 |
| `fanout` | number of in-broker relay consumers on ONE partition | how latency and CPU scale toward the customer's 800-1,000 consumers at ~1,000:1 (E3.1) | 1 5 10 25 50 |

### Pinned in every mode

- **1 producer transform, 1 relay consumer** unless the mode says otherwise.
  Client count is Phase 3's variable, not this script's.
- **Placement is chosen, not observed.** `rate`/`payload`/`guest` put the
  producer's topics on `PIN_NODE`/`PIN_CORE` and the consumer on `PROBE_CORE`
  (a different shard). Co-locating them would fold shard contention into the
  sweep. `partitions` deliberately does *not* pin cores - the spread is what it
  measures - but still co-locates *nodes*, because the relay has no cross-node
  hop.
- `RF`, `WRITE_CACHING`/`FLUSH_MS`, `LINGER_MS`, `PRODUCERS`, `PACING`,
  `SAMPLE_EVERY`, `DURATION`, `WARMUP`, `DRAIN` - all fingerprinted.

### `fanout` mode specifics

The customer shape is **one hot partition with 800-1,000 consumers**
so this sweep holds partitions at 1 and moves only the consumer
count. Fix `RATE` at a level a prior sweep proved clean, so fan-out is the only
variable.

Three things about it that are easy to get wrong:

- **Consumer 0 is `relay-probe`; consumers 1..N-1 are `relay-sink`.** Only
  consumer 0 writes to `probe_out`, because loadgen correlates one receipt per
  order and would report `max_receipts_for_any_sampled_order = N` if they all
  did - a verdict failure caused purely by the harness.
- **The fillers must be zero-emit, and this is not optional.** `relay-sink`
  receives and parses every pushed record and writes nothing. Round 2
  (2026-08-29) used `relay-probe` as the filler, so all N-1 *produced* a record
  per delivery: ~33M extra writes in a 30s window at fanout 50, ~106M at 250.
  **The cluster saturated on the benchmark's own output and the ladder measured
  the harness, not relay dispatch.** The two guests differ only in the write, so
  `relay-probe` vs `relay-sink` is also a clean one-variable comparison if the
  write cost itself is ever the question. The fillers are still not free: each
  owns a wasm VM, a pending queue and a scheduling slot, which are the costs the
  ladder exists to measure.
- **Consumers are round-robined across the producer node's shards** via
  `RELAY_TARGET_SHARD`. Leaving them all on the producer's shard is what made
  `relay_consume` grow with fan-out before the cross-shard fix, so a sweep
  without the spread measures core contention rather than fan-out.

Two metrics in this mode do not mean what their names suggest:

- **`relay_pushes_delta` counts shard-local delivery passes, not pushes.**
  `redpanda_relay_pushes_total` increments inside `deliver_locally()`, which
  `push()` invokes once for the producer's shard plus once per other shard
  holding a subscriber. At fanout 10 it read 6,000,000 for 600,000 logical
  pushes.
- **So `delivered/pushes` is not a fan-out ratio** and read ~1.0 at every level,
  hiding fan-out entirely. It is now reported as
  `delivered_per_shard_pass`, with the real ratio as
  `delivered_per_logical_push` (delivered / orders x 2).

Consumer CPU is **aggregated across all N consumers**, weighted by each one's
own invocation count. A single-name lookup reported `n/a` at every level in
Round 2 while the data sat in `per_function` the whole time.

Readiness gates on `redpanda_relay_active_subscriptions` on the **producer's**
node, never on `rpk transform list`: `RELAY_TARGET_SHARD=N` pins shard N on
every node, and an idle duplicate on a non-producer node reports "running"
before the real subscription exists. Pushes sent before a subscription exists
are dropped silently and permanently, because `push()` treats "no subscriber"
as a no-op.

There is a **capacity pre-flight** before the first level. Each consumer costs
one instance slot *per node*, so the largest level needs `N+1` partition-
instances per node against `data_transforms_max_instances_per_core x cores`.
The sweep aborts up front rather than failing several levels in. Note the cap is
`needs_restart::yes`: setting it without rolling the brokers changes nothing,
and the pre-flight can only read the *desired* value (METHODOLOGY #13), so it
can pass while the running brokers still refuse the deploy.

The boot reservation (`cap x per_function_bytes x cores`, taken on every core
whether or not anything is deployed) is recorded into the summary header,
because it is a real capacity cost of the approach.

### What the sweeps do NOT isolate

**By default, `rate`, `payload` and `guest` share topics and a long-running
transform across levels.** Level N therefore reads an `orders` log containing
every prior level's records, against a warm batch/readers cache, with a matcher
whose guest order book has been live since level 1 (`guest` mode redeploys the
producer, so it resets guest state but not the log).

That is acceptable when the variable is purely client-side, and **not**
acceptable when comparing a first level against a later one, or when the read
path could plausibly matter. Set `FRESH_TOPICS_PER_LEVEL=1` for strict
isolation - it recreates topics and redeploys both transforms before every
level, at ~30-45s per level. `partitions` mode always recreates.

Two further limits, both inherent rather than fixable here:

- **One partition is one processor.** A single-partition sweep measures a single
  serial pipeline; it says nothing about aggregate cluster capacity.
- **Cross-clock stages.** `match` and `total` mix the client's clock with the
  broker's guest clock. `produce`, `send_lateness` and `relay_consume` are
  single-clock and safe. Quote a cross-clock number only with the run's
  `clock-sync.json` residual attached.

### Per-level safety checks

Each level, before spending the window:

1. **Topic-variable guard** - all three topic variables must still match
   `*-e2-$RUN_ID`, or the level aborts naming the offender. This is defect 19's
   guard: a variable holding the topic name was once overwritten with an order
   count, and every level after the first produced to a nonexistent topic.
2. **Deliverability pre-check** - 10 orders must produce receipts. Both
   zero-receipt failures this harness has produced (relay node-locality, and the
   defect-19 clobber) would have been caught here in seconds rather than after a
   multi-minute level or a five-level sweep. A failing pre-check records
   `PRECHECK-FAILED` and skips the level rather than recording noise.
3. **The exact `loadgen` invocation is echoed**, so a wrong topic or flag is
   visible in the log rather than inferred from failures.

And inside `loadgen`, each level is marked `clean:false` on any of: zero
receipts, missing receipts, produce errors (tallied **by kind**), transform
failures, relay drops, a growing backlog past a threshold, **window coverage
below 95%**, duplicate receipts, or the client failing to offer the requested
rate. Client-bound and broker-bound shortfalls are **separate verdicts** - a
client that cannot offer the rate says nothing about the broker, and conflating
the two is the easiest way to publish a wrong throughput number.

### Reading the summary table

```
level offered/s achieved/s cores relay_p50us relay_p99us total_p50us \
      match_us/inv match_us/order probe_us/inv dropped clean
```

- **`achieved/s`** is receipts over the send window. It can read ~100% even when
  the system fell behind and caught up during the drain, so it is **not** proof
  of health on its own - the `clean` column and the lag verdict are.
- **`cores`** is the count of distinct **leader** cores the input partitions
  occupy (`leader_id`, not `replicas[0]`, which is an arbitrary follower at
  RF>1). `?` means the topic was not found - a bug, not a measurement.
- **`match_us/inv`** is CPU per guest invocation, i.e. per record. **`match_us/order`**
  is per crossing pair = 2 records, so roughly twice it. Both are printed because
  confusing them is a clean 2x error. Invocations are **measured** from
  `redpanda_transform_execution_latency_sec_count`, never inferred.
- **`relay_p50us`** is single-clock and quotable. **`total_p50us`** is
  cross-clock - see above.
- **`clean=false` means do not quote that row.** The reason is printed beneath
  it.

Sanity identities that must hold - a violation is a harness bug, not a finding:
`match_us/order ≈ 2 × match_us/inv`, `receipts == orders × num_probes`,
`invocations == records_sent`, `window_coverage ≈ 1.0`.

---


## `run-round3.sh` - the in-broker vs external comparison

**Run it with `./wb run round3`.** Do not drive the two arms by hand.

    ./wb run round3                                  # defaults: rate 5000, fanout 1 5 10
    ./wb run round3 RATE=5000 FANOUT_LEVELS="1 5 10"

### Why a dedicated script

The comparison is only valid if both arms are identical in every respect except
who consumes. That is five-plus variables that must match across two
invocations, and getting one wrong yields a plausible number that is not a
comparison. `run-round3.sh` takes the parameters once and drives both arms, so
the invariant is enforced by code. It also waits for arm 1's transforms to be
cleaned up before starting arm 2, because the runner's `assert_no_concurrent_run`
correctly refuses to start while any other run's transforms exist.

### What each arm is

| arm | who consumes the matcher's output | measured consumer |
|---|---|---|
| `inbroker` | N relay consumers inside the broker | consumer 0 (`relay-probe`), receipt read by loadgen from `probe_out` |
| `external` | N ordinary Kafka consumer groups outside it | **loadgen itself**, consuming the `fills` topic directly |

The external arm's N-1 background consumers come from `cmd/fanout-load`; loadgen
is the Nth, so both arms carry the same consumer count.

### The measurement, and the handicap

Both arms report `total` on **loadgen's own clock** (order send -> receipt
observed), so the headline figure is single-clock with no skew residual - which is
the whole reason it is done this way rather than comparing a broker-clock stage
against a client-clock one.

The arms differ in one way that is **not** in the in-broker path's favour:

    inbroker: send -> matcher -> relay -> probe -> probe_out -> loadgen
    external: send -> matcher -> fills                      -> loadgen

The in-broker arm carries an extra produce+fetch hop the external arm does not.
**The comparison understates the in-broker path.** If in-broker wins anyway it
wins carrying weight; if it loses, that extra hop is a candidate explanation
before any conclusion is drawn.

### Reading the output

The script prints a per-level table with `ratio = external / inbroker`. Above
1.00x means the in-broker consumer saw the data sooner.

**A level counts only if BOTH arms are `clean:true`.** One clean arm against one
saturated arm is not a comparison - the saturated side's latency is a backlog age.

### What the external arm does NOT record

`relay_consume` and `match` are absent in the external arm. Both would need a
broker-clock reading, and mixing clocks is exactly what this design avoids.
Absent is honest; a subtly cross-clock number is not.

## `run-e1-ceiling.sh`

| MODE | Varies | Answers |
|---|---|---|
| `admit` | deploy count | where admission rejects, and the accepted-vs-running gap (E1.1) |
| `instances` | `data_transforms_max_instances_per_core` | instance ceiling and its memory price (E1.2) |
| `memory` | `data_transforms_per_function_memory_limit` | per-client memory cost at fixed count (E1.3) |

Throughput is zero, so no load generator is involved.

`instances` and `memory` change `needs_restart` properties and **refuse to run
without `RESTART_CMD`**: a live `rpk cluster config set` on those changes
nothing in the running broker while `rpk cluster config get` reports the new
value (METHODOLOGY.md #13). The runner rolls between levels and waits for the
relay metric families to reappear - rpk answering is not the wasm runtime being
back.

`admit` accepts `ADMIT_CAP_PER_CORE` to lower the admission cap **live and
deliberately**, so the ceiling is reachable on a large cluster. That exercises
the split above on purpose: admission moves, the boot-carved pool does not.

---

## Invocation

```sh
cd ~/redpanda/projects/wasm-transforms-fanout-bench
./wb doctor                       # always first

# rate sweep at production replication settings
./wb run e2 MODE=rate RF=3 WRITE_CACHING=true FLUSH_MS=100 LINGER_MS=2 \
  RATE_LEVELS="1000 5000 10000 20000" DURATION=30s WARMUP=10s DRAIN=40s

# strict per-level isolation
./wb run e2 MODE=rate FRESH_TOPICS_PER_LEVEL=1 RATE_LEVELS="1000 10000"

./wb results                      # pull JSON back locally
```

Multi-word values must be quoted as one argument (`RATE_LEVELS="1000 5000"`);
`wb` quotes them on the way to the remote shell, and rejects anything that is
not `KEY=VALUE`.
