# Benchmark methodology

This exists because the first two real runs of this benchmark (2026-07-28)
each produced a confidently-wrong number before the actual mistake was
found. Both mistakes were in the *measurement*, not in Redpanda or in
either entrypoint - which is exactly the dangerous kind, since a wrong
number from a broken harness looks identical to a right one. This
doc is the checklist for not repeating them, and the record of what
each one actually cost.

**If you're about to run a new comparison for a code change, read the
"Before you trust a number" checklist below, then run
`run-comparison.sh`. It applies all of this automatically.**

## What went wrong, in the order it was found

### 1. `data_transforms_*` cluster config defaults don't fit a Go guest

First symptom: `unable to deploy transform: ... data transforms disabled`,
then `memory minimum size of 38 pages exceeds memory limits`, then `wasm
invocation exceeded its configured runtime limit` on the very first
deploy. None of these are benchmark bugs - they're real onboarding
friction for any Go-built transform-sdk guest against an out-of-the-box
cluster. Fixed once, permanently, via `bootstrap-cluster.sh`. See that
script for the exact settings and why each is needed.

### 2. A real correctness bug in the engine, found by the first real run

The first successful deploy produced `encoded crc ... does not match
calculated crc ...` on every single output record, from a real `franz-go`
consumer. This was `rpc_client_sink::write()` recomputing Redpanda's
internal `header_crc` but not the actual Kafka wire-protocol `crc`,
after mutating fields both cover. Fixed on the
[`transform-latency-instrumentation`](https://github.com/vuldin/redpanda/tree/transform-latency-instrumentation)
branch, folded into the commit that introduced the mutation it corrects
("transform: give guests control of output timestamps, partitioning and
idempotence"), pending upstream review - not a benchmark issue, but recorded
here
because it's the reason "the benchmark returned 0 records" doesn't
always mean the benchmark is broken. Check the broker log for `crc`
mismatches before assuming the harness is at fault.

Cited by commit SUBJECT rather than SHA on purpose. That branch is rewritten
deliberately - each commit is meant to hold one feature so it can be split up
later - and the SHA this used to name was orphaned by exactly that, leaving a
404. Subjects survive a rebase; hashes do not.

### 3. Topic reuse across runs contaminates the next one

Reusing the same topic names across repeated debugging attempts meant a
"clean" 500-record run was actually measuring 500 records queued behind
thousands of leftover records from earlier failed attempts - reported as
~1.02 **seconds** of latency, flat to the microsecond across all 500,
which was itself the tell (a real distribution doesn't look like that).
**Fixed:** `run-comparison.sh` names every topic pair with a fresh
timestamp suffix, every run. Never reuse a topic name across runs unless
you specifically want to test recovery/backlog behavior.

### 4. Pipelined send-then-drain measures the wrong thing

The original harness sent all N orders as fast as possible, then drained
the output topic - which measures pipeline-fill time, not per-record
latency: however many records land in one underlying Kafka fetch response
get timestamped together in the same tight receive-loop iteration,
collapsing their individual latencies into one value (visible as
near-zero spread across every percentile). **Fixed:** `cmd/bench` sends
one order, waits for its specific corresponding output record, *then*
sends the next. Slower to run, but the number it produces is actually a
per-record latency.

### 5. franz-go's default 10ms producer linger, on **both** sides

The single biggest one. franz-go batches produces for up to 10ms by
default, hoping more records arrive to send together. Neither `cmd/bench`
nor `cmd/kafka-matcher` ever has more than one record in flight under
this benchmark's own methodology (see #4) - so that 10ms was never doing
its job, just adding pure dead time to every single measurement, on
**both** the order-submission side (`cmd/bench`'s own producer) and the
fill-production side (`cmd/kafka-matcher`'s producer). This alone
accounted for the overwhelming majority of every "double-digit
millisecond" number this benchmark ever produced. **Fixed:**
`kgo.ProducerLinger(0)` in both `cmd/bench` and `cmd/kafka-matcher`
(the latter via `PRODUCER_LINGER_MS`, default `0`).

Before/after, same cluster, same everything else - this one fix alone:

| | before | after |
|---|---|---|
| wasm-inbroker mean | ~15.2ms | ~0.4ms |
| kafka-external mean | ~11.2ms | ~0.5ms |

### 6. Per-record fsync

With `write.caching` off (Redpanda's default), every produce durably
flushes to disk before acking - a real, honest durability guarantee, and
a real, honest few milliseconds of added latency on whatever disk the
benchmark happens to run on. Enabling `write.caching=true` on the
benchmark's topics cut several more milliseconds off, before the linger
fix was even applied. **This is a genuine durability/latency tradeoff,
not a free win** - see "write.caching: read this before you set it" below.
`run-comparison.sh` enables it by default and says so loudly in its
output; a result run without it is not wrong, just measuring a different
(more durable, slower) configuration, and should be labeled as such.

### 7. Cold VM / cold connections pollute the first N samples

The very first records against a freshly deployed transform pay for
things a steady-state record never will: Go-runtime cold start inside
the guest (can be well over a second), and consumer-group/metadata setup
on the client side. Averaging those into a 20-100 record run swamps the
real signal. **Fixed:** `cmd/bench -warmup N` (default 20) sends and
discards N records before the timed window starts.

### 8. Reusing numeric order IDs across separate runs against the same topic

Both entrypoints keep their order book (or, for `wasm-matcher`,
snapshotted guest state) in memory for the matcher process's
entire lifetime, which spans every bench run pointed at the same topic
pair, not just the current one. `cmd/bench` used to start IDs at `"0"`
every run - so run 2's order `"0"` collides with run 1's still-resting
order `"0"`, gets rejected as `ErrDuplicateOrder`, and is silently
dropped per both entrypoints' documented error policy. The result: bench
hangs forever waiting for a fill that will never arrive, because the
input it's waiting on was never accepted in the first place. **Fixed:**
every order ID is prefixed with the run's own start-time nanoseconds,
making collision across runs impossible without needing to clear the
topic between them.

### 9. The wasm engine can restart its own VM mid-benchmark, unrelated to guest activity

Observed twice, not yet root-caused: a transform that had been running
cleanly for two full 100-record runs suddenly logged `wasm invocation
exceeded its configured runtime limit` and restarted, immediately (within
1ms of `starting wasm vm`) on one occasion - which rules out a genuinely
slow invocation as the cause, since the trap fired before any real guest
code could have run. Both times this happened, it produced exactly one
~1-second outlier in that run's results (Go-runtime cold start on the
forced restart), with every other record unaffected.

**This cannot be fixed by the benchmark harness - only detected.**
`cmd/bench -admin-url -transform-name` scrapes the transform's own
`failures` counter (`redpanda_transform_failures`, the same metric
`transform_manager` increments on every give-up-and-restart cycle) before
and after the timed window. If it moved, the report's `"clean": false`
and `"failures_during_run"` fields say so - re-run rather than trust that
report's percentiles. **Always pass `-admin-url`/`-transform-name` for
the wasm leg.** There is nothing equivalent to check for `kafka-matcher` -
it has no wasm VM to restart, which is itself worth noting as an
asymmetry between the two paths' tail-latency risk profiles.

This is a real, open finding, not a benchmark artifact to shrug off, and
it is not yet root-caused - see the "not yet root-caused" note above for
what's been ruled out so far.

### 10. Reader-cache thrashing above `readers_cache_target_max_size` (default 200) - a real, general fetch-path cost with nothing to do with WASM

Found while investigating heavy consumer fanout (231 groups): the
per-partition *reader-object* cache (distinct from, and independent of,
the batch/data cache) is capped at 200 entries by default. A fan-out
above that cap thrashes it - visible as `vectorized_storage_log_cache_hits`/
`_cache_misses` (the **internal** `/metrics` endpoint, not `/public_metrics`)
sitting at a 97-99.99% *miss* ratio even while
`vectorized_storage_log_cached_read_bytes`/`_read_bytes` (the batch cache)
stays a clean 100% hit. This is a known Redpanda fetch-path cache-sizing
behavior - the segment-reader cache, distinct from the batch/data cache -
not something specific to this benchmark or to WASM. Check for
already-documented mechanisms before proposing a new causal story for a
performance anomaly.

**Now automated**: `run-fanout-comparison.sh` sets
`readers_cache_target_max_size` to `FANOUT_N + 100` before every run
(override with the env var of the same name to deliberately test *under*
the cap). `check-bottlenecks.sh` reports the actual hit/miss ratio for the
topics under test after every measurement, so a thrashing run says so in
its own output instead of requiring after-the-fact metric archaeology.
Raising the cap cut absolute fanout latency by roughly a third in testing
- real and worth doing - but the miss ratio can stay high even after
raising it well above the fan-out count (a cache *hit* needs an *exact*
start-offset match; capacity alone doesn't guarantee independent async
consumers land on the same offset at the same instant). Don't assume
"raised the cap" means "problem solved" - check the actual ratio.

### 11. The wrong admin-API endpoint for shard/core placement gives a completely wrong answer, silently

`GET /v1/partitions/{ns}/{topic}/{partition}`'s `.replicas[].core` field
does **not** reflect live node-local shard assignment - it returned `0`
for every partition checked, unconditionally, regardless of where
`shard_balancer` actually placed it. This produced a wrong "everything
collapses onto shard 0, even with 4 shards available" conclusion that
looked completely plausible (consistent across a dedicated multi-partition
test topic, consistent after an explicit rebalance trigger) and was wrong
throughout. **Use `GET /v1/cluster/partitions`** (what
`rpk cluster partitions list -a` uses internally) for real shard/core
placement - cross-check with `rpk cluster partitions list -a` directly if
in doubt, and be suspicious of a placement result that contradicts what
you already know about how Redpanda's balancer is supposed to behave
(it continuously balances partitions and leadership - "everything landed
on shard/broker 0 and stayed there" should be a red flag to re-verify
the *query*, not a fact to build a conclusion on). `check-bottlenecks.sh`
now always queries the correct endpoint.

### 12. A single run under a high-variance regime is not a measurement - it's a sample

Heavy fanout (231 concurrent consumer groups against one hot partition)
is a genuinely high-variance regime: three consecutive trials of the
*identical* configuration produced in-broker/external p50 ratios of
1.10x, 0.98x, and 1.62x - a spread wide enough that any single trial's
specific decimal is misleading on its own, even though the qualitative
direction (advantage shrinks under fanout) held in all three. Report a
range across multiple trials for any number measured under heavy
contention, the same rigor already applied to the scheduling-shares
experiment (three trials each way) - don't state a specific ratio from
one N=300 run under fanout and treat it as precise.

### 13. `rpk cluster config get` reports a value the running broker is not using

Found 2026-08-28 while writing the Phase 1 capacity runner, then **measured on
a real cluster the same day - the first version of this entry got the mechanism
wrong and is corrected below.**

`data_transforms_max_instances_per_core` and
`data_transforms_per_function_memory_limit` are `needs_restart: yes`. What that
actually means:

- `rpk cluster config set` updates the cluster's **desired** config, and
  `rpk cluster config get` immediately echoes the new value back.
- The **running** broker keeps the value it **booted** with. Both the
  deploy-time admission check (`plugin_frontend::validate_mutation`) and
  `wasm::heap_allocator`'s pool use the boot value.
- `rpk cluster config status` is the only thing that tells you: it reports
  `NEEDS-RESTART true` per node.

So a live `set` on these properties changes **nothing** in the running broker,
while `get` reports success. That is the trap: not a partial application, a
completely inert one that reads as applied.

**What was originally written here, and why it was wrong.** The first version
claimed the admission check read the value *live* while only the pool stayed at
boot, producing a "split failure" where a deploy is accepted and then the
processor fails to start with `unable to allocate memory within requested
bounds`. That mechanism was inferred from reading the code
(`validate_mutation` does call `config::shard_local_cfg()` on each invocation)
and never verified. It is wrong: `shard_local_cfg()` itself still holds the
boot value for a `needs_restart` property, so admission does not move either.

**How it was settled** (worth repeating as a technique - the code read was not
enough, and neither was a walk-up test):

1. With desired cap = 1/core on a 13-shard node (ceiling 13), a **single**
   deploy against a **50-partition** input topic was **accepted**. One deploy
   crossing the ceiling on its own removes any question about accumulation.
2. The cluster was rolled so 1/core became the boot value.
3. The identical deploy then **rejected**, at exactly the predicted ceiling,
   with the broker logging `deploy of transform ... needs 50
   partition-instances, which would bring the total to 68 (existing 18 + new
   50), exceeding the estimated cluster capacity of 13`.

Same command, same desired config, opposite outcome across a restart. That is
the proof; nothing short of it would have been.

**Consequences for measurement:**

- Never trust `rpk cluster config get` as evidence that a `needs_restart`
  property is in effect. Check `rpk cluster config status`, or restart.
- Any capacity experiment that raises a cap and reads back "we deployed N" is
  measuring the boot config. `run-e1-ceiling.sh` refuses to run its
  instance/memory sweeps without a `RESTART_CMD` for this reason, and
  `wasm-transforms-fanout-bench/wb set` rolls the cluster automatically for the
  affected properties.
- `run-relay-wasm-fanout*.sh` set `max_instances_per_core=200` live with no
  restart. That raise was **entirely inert** - the pool and the admission check
  both stayed at the ansible-provisioned boot value of 100/core. It did not
  corrupt those results (fanout=100 over 13 shards needed ~8 per shard, far
  under 100), but the scripts were not doing what they appeared to.

Separately, and still true: the admission check is necessary-but-not-sufficient
by its own comment - it scales by the deciding node's core count only and does
not model uneven placement - so "accepted" and "actually running" can differ,
and that gap is what E1.1 measures.

### 14. A drain that is too short truncates the metric window and silently corrupts every per-record figure

Found 2026-08-29 while diagnosing a result whose numbers contradicted each other.

The symptom was a report where per-ORDER CPU came out **lower** than
per-RECORD CPU - impossible, since an order is two records. Raw counts at
5,000 orders/sec with the then-default 10s drain:

| | 10s drain | 40s drain |
|---|---|---|
| records sent | 200,000 | 200,000 |
| producer invocations (metric delta) | **59,948** | **200,000** |
| coverage | **30%** | 100% |
| receipts vs expected | 109,017 / 100,000 | 100,000 / 100,000 |
| us/order over us/record | 0.60 (impossible) | **2.00** |
| verdict reported | **clean** | clean |

**Mechanism.** `loadgen` brackets the timed window with a before/after metric
scrape. The after-scrape happens once the drain expires. If the pipeline has not
finished by then, the invocation delta is **truncated** while `records_sent`
stays whole - so per-record cost is a real numerator divided by a partial
denominator. It scales with rate, so the error grows exactly where the numbers
matter most, and it reported `clean`.

**What was NOT the cause** (each checked and eliminated, in this order):

- *A mislabelled counter.* `wasmtime.cc`'s measurement is taken inside
  `pre_record()`/`post_record()`, so `execution_latency_sec_count` counts
  records, as labelled. Confirmed empirically at 500 orders/sec: invocations
  equalled records exactly, and us/order over us/record was exactly 2.00.
- *Leadership churn.* 35 election lines on the leader broker looked conclusive,
  but every one was `term: 0 -> term: 1` - initial elections for freshly created
  partitions plus the harness's own leadership transfer. No mid-run churn.
- *Duplicate receipts from a processor restart.* Plausible, and instrumented:
  `duplicate_receipts_sampled` came back 0, `max_receipts_for_any_order` 1,
  `transform_failures` 0.

Two of those three were confident hypotheses that measurement killed. Recording
them because the *order* matters: the cheap empirical check (a low-rate run
where nothing can be backlogged) settled in one run what two rounds of code
reading and log archaeology did not.

**Still unexplained:** the short-drain run's receipts *exceeded* expected by 9%,
which truncation alone does not account for. It did not reproduce at 40s. Left
recorded as unexplained rather than given an invented mechanism.

**Fixed two ways:**

1. `cmd/loadgen` computes `window_coverage` = producer invocations over records
   sent, and **fails the run below 95%**, naming the cause and the remedy. This
   is the check whose absence let a 30%-coverage run pass as clean.
2. The default drain is now 40s, not 10s, with the measured reason in the
   comment so it is not reverted as over-cautious.

**Rule:** a per-record figure is only valid if the run reports
`window_coverage` at or near 1.0. Quote nothing derived from a truncated window.

### 15. A number that stays identical across a 10x range of inputs is not a measurement

Resolved 2026-08-29, after roughly four hours and seven wrong hypotheses.

The symptom: every rate level above the first reported an attempted send rate of
~2,739 orders/sec. It was read as the client's throughput ceiling. It was
**identical whether 10,000, 20,000, 50,000 or 100,000 orders/sec was offered**.

That invariance was the whole answer and it was visible from the first table. A
real ceiling is approached from below and then held; it does not produce the
*same* number for a 10x range of demand. What produces an identical number is an
**artifact of a fixed failure path** - here, every produce failing, franz-go
retrying, the buffer filling, `Produce` blocking for tens of seconds, the send
window overrunning 30s to 73s, and `attempted = sent / elapsed` landing in the
2,700s by arithmetic.

The actual trigger, found by the cheapest possible experiment: **two identical
levels at a rate that passes on its own.** Level 1 clean, level 2 zero receipts
and 100% produce failure. Sequence position, not rate. That test took four
minutes and could have been run first.

**The rule:** before theorising about a limit, check whether the number responds
to the input at all. If it does not, you are measuring a failure mode, not a
capacity. Vary the *suspected* variable and one *unrelated* variable; if both
give the same answer, neither is the cause.

**The cause, for the record.** A shell variable holding the input topic name
(`orders`) was overwritten with an order count inside the level loop, so every
level after the first produced to a topic named e.g. `30000`. All seven
hypotheses were about the broker or the Kafka client; none was about the
harness's own variable scoping. The guard now in `run-e2-throughput.sh` -
validate that the topic variables still look like this run's topics before each
level - would have caught it in the first second of level 2.

**Corollary on hypothesis order.** The seven eliminated hypotheses (fsync,
producer linger, client in-flight caps, leadership churn, processor restarts, a
stale binary, unscoped cleanup deleting live topics) were each plausible, each
took real time, and each was killed by measurement. Every one was a mechanism
proposed *before* the failure had been characterised. Characterising it first -
does it depend on rate? on position? does the data reach the broker at all? - is
strictly cheaper than proposing mechanisms and testing them one at a time.

### 16. A ratio identity is the cheapest bug detector available - build one into every report

This workload emits exactly two records per order, so `us/order` must be
**exactly** `2 x us/invocation`. That single identity has now caught three
separate denominator bugs, including two that produced entirely plausible
numbers:

- #12's CPU-divided-by-receipts (ratio came out ~1, should have been 2)
- #15's variable collision (found by other means, but the ratio also broke)
- the 2026-08-29 window mismatch: `6.94 us/order` against `2.60 us/invocation`,
  a ratio of **2.67**. Nothing about 6.94 looks wrong on its own. The ratio is
  what made it obviously wrong.

That last one was not a measurement error at all - loadgen's figures were
correct (`invocations == records_sent`, `window_coverage == 1.0`). It was the
runner's *outer* `scrape-metrics` line, whose snapshot pair brackets loadgen's
whole invocation (warmup included, 80,000 invocations) while dividing by a
warmup-excluded order count (30,000). **Two windows, one division.**

Rules that follow:

- **Never divide two quantities unless you can name the single window both were
  measured over.** Mismatched windows are the most common source of confidently
  wrong per-unit figures in this project.
- **Print a known-value ratio next to every derived per-unit number.** It costs
  one line and it is the tripwire that fires when a denominator drifts.
- When the tripwire fires, **check the raw counts before rewriting any
  analysis** - the bug is more often in the reporting line than in the system.

---

### 17. Never pipe a build through `tail` - it hides both the error and the failure

Hit 2026-08-29. The redpanda build was launched as
`bazel build ... 2>&1 | tail -40`, backgrounded. It reported **exit code 0,
completed**, and the visible output ended with a normal-looking progress line.
The build had failed.

Two independent things went wrong, and either one alone is enough to waste a
cycle:

- **The exit code belonged to `tail`, not to bazel.** A pipeline's status is its
  *last* command's, so a failed build wrapped in `| tail` reports success. The
  only reason this was caught is that bazel prints `ERROR: Build did NOT
  complete successfully` on its own, which happened to survive inside the last
  40 lines.
- **`tail -40` discarded the compiler diagnostic.** The error was ~9,300 actions
  earlier in the log. Diagnosing it required re-running the whole build (cheap
  only because bazel had cached 8,810 actions - on a cold tree this is an hour).

Rules:

- **Redirect a build to a file, then grep it.** `bazel build ... > build.log
  2>&1; echo "exit=$?"` - never `| tail`, `| head`, or `| grep` as the outermost
  stage.
- **Check the tool's own exit status**, not a pipeline's. If a pipe is
  unavoidable, `set -o pipefail` first.
- Generalise past builds: this applies to any long command whose failure lives
  far from its end - test suites, ansible runs, terraform applies.

(The actual bug, for the record, was a name-collision worth knowing on its own:
`relay::service` has a nested `struct config`, so inside its member functions an
unqualified `config::shard_local_cfg()` resolves to the nested struct rather
than the global `config` namespace. It needs a leading `::`.)

---

### 18. `bazel-bin` is a moving symlink - a `bazel test` can silently swap the binary you benchmark

Caught 2026-08-29, at the moment of provisioning, before any spend. `wb doctor`
reported the local build as `2026-08-18 09:54` - eleven days old - immediately
after a successful build that same evening.

`bazel-bin` is a **convenience symlink that bazel repoints to whichever
configuration it built last.** The benchmark build is `-c opt`, which writes to
`bazel-out/k8-opt/bin`. The unit-test commands run earlier that evening omitted
`-c opt`, so they built in fastbuild config and repointed `bazel-bin` at
`bazel-out/k8-fastbuild/bin`, where an untouched 11-day-old binary was still
sitting. `wb` ships `$REDPANDA_SRC/bazel-bin/...`.

Had this not been caught, `wb deploy` would have pushed:

- a binary **without the change under test** (verified: zero occurrences of the
  new config property), and
- a **762MB unoptimized** broker instead of the 207MB opt one, whose latency
  numbers are meaningless,

while `deploy`, `ready` and `status` all reported success. This is the
stale-binary failure mode that was one of the seven wrong hypotheses during the
defect-19 investigation, arriving by a different route.

Rules:

- **Never compare timestamps or shas through `bazel-bin` and assume a
  configuration.** Resolve it: `readlink -f` the directory and confirm
  `k8-opt`. `wb doctor` now fails hard if it is anything else.
- **Pass `-c opt` to `bazel test` as well**, not only `bazel build`, or the next
  test run silently re-points the symlink again.
- **Prefer a content check over a freshness check.** `strings <binary> | grep -x
  <new symbol>` answers "is my change in the thing I am about to ship", which is
  the actual question; an mtime does not.
- Generalise: when a tool hands you a *stable* path to a *variable* artifact,
  the path is not the identity. Verify the artifact.

A near-miss worth recording separately: `ls -d` with colour enabled emits ANSI
escapes into the path, so a later `stat`/`strings` on that captured string reads
a nonexistent file and returns **empty, not an error** - which looked exactly
like "the change is missing". Two checks in a row disagreed before the cause was
found. Build paths from literals or `find -print0`, never from coloured `ls`.

---

## `write.caching`: read this before you set it

`write.caching=true` defers fsync, batching multiple produces into one
flush on a time/size window (`flush.ms`/`flush.bytes`, defaults 100ms /
256KiB) instead of syncing every single one. That's real, meaningful
latency back - and it means a hard crash between accepting a produce and
the next flush can lose that record, even though the client already got
an ack. For a benchmark measuring best-case latency, that's the right
knob to reach for. For a production exchange-matching deployment, that's
a durability decision someone with the authority to make it needs to make
deliberately - not something to inherit from a benchmark script by
accident. Every report produced with it on should be labeled as such;
`run-comparison.sh` does this by setting it explicitly (not relying on a
cluster-wide default) and printing what it did.

## Before you trust a number

- [ ] Fresh topics for this run, not reused from a previous one (#3).
- [ ] Sending one at a time, waiting for the matching output before the
      next send (#4) - `cmd/bench` always does this; don't write a new
      harness that pipelines unless you specifically want pipeline-fill
      time instead of per-record latency.
- [ ] `-warmup` was non-zero and actually ran before timing started (#7).
- [ ] `-admin-url`/`-transform-name` were set for any wasm-transform leg,
      and the report says `"clean": true` (#9). If `false`, re-run - don't
      report the numbers with a caveat, just get a clean run.
- [ ] Order IDs are run-scoped, not restarted at 0 (#8) - true by default
      in `cmd/bench`; don't hardcode IDs in anything that calls it.
- [ ] You know whether `write.caching` was on or off for this run, and
      you're labeling results accordingly - "in-broker vs external,
      write-caching on" is a different, non-comparable number from
      "...write-caching off."
- [ ] If comparing two entrypoints, both are tuned equally (#5) - a fix
      applied to `cmd/bench`'s own producer but not to `cmd/kafka-matcher`'s
      makes one leg look artificially slower for a reason that has
      nothing to do with in-broker vs. external.
- [ ] If testing consumer fanout, `readers_cache_target_max_size` is above
      the fan-out count (#10) - `run-fanout-comparison.sh` does this
      automatically now; check `check-bottlenecks.sh`'s output (or the
      `*.diag.json` files) for the actual reader-cache hit ratio rather
      than assuming the config change alone fixed it.
- [ ] Shard/core placement was checked via `GET /v1/cluster/partitions`
      or `rpk cluster partitions list -a` (#11) - never
      `GET /v1/partitions/{ns}/{topic}/{partition}`'s `.replicas[].core`.
- [ ] Any number from a heavy-contention scenario (high fanout, high
      concurrency) comes from at least 2-3 trials, not one (#12) - report
      the range, not a single decimal.
- [ ] If the run changed any `needs_restart` property (the
      `data_transforms_*` sizing ones, `relay_enabled`, `relay_port`), the
      cluster was RESTARTED afterwards, and `rpk cluster config status` shows
      no pending restart (#13). `rpk cluster config get` echoing the new value
      is NOT evidence it is in effect - set live, these change nothing at all
      in the running broker.
- [ ] A result that is suspiciously stable across different inputs has been
      challenged, not accepted (#15). Vary an unrelated input; if the number
      does not move, it is a failure artifact rather than a capacity.
- [ ] `window_coverage` is at or near 1.0 (#14). Below that, the metric window
      closed before the pipeline finished and every per-record figure is
      divided by a truncated denominator - raise `-drain`.
- [ ] For any stage computed from one client-side and one broker-side
      timestamp (`match`, `total` in `cmd/loadgen`; anything derived from a
      guest clock against a client clock), the run's own clock-sync residual is
      recorded and quoted alongside it. Single-clock stages (`produce`,
      `send_lateness`, `relay_consume`) need no such caveat. See
      SCALING-TEST-PLAN.md P0.5.

## Running it

```sh
# one-time, after a fresh node's first boot (see bootstrap-cluster.sh)
BROKERS=127.0.0.1:9092 bash bench/bootstrap-cluster.sh
# restart the node per that script's own printed instructions, then:
BROKERS=127.0.0.1:9092 N=500 bash bench/run-comparison.sh
```

Results land in `bench/results/{wasm,kafka}.json`, each carrying its own
`clean`/`warmup_n` fields so a result file is self-describing about
whether it followed this checklist - don't hand-edit those fields.

### 19. Instrumentation gated on a config flag records nothing unless the harness sets the flag itself

`relay_stage_metrics_enabled` gates recording into the three relay stage
histograms. It defaults to false, and for two rounds nothing in the harness ever
set it — so whether a run captured a stage breakdown depended entirely on what
the cluster had last been left at by hand.

Both times the flag was false, and both times the loss was invisible until
analysis:

- **Round 2 (2026-08-29)** ran the full sweep and produced no
  dispatch-vs-scheduling attribution — the exact question the histograms had
  been built to answer.
- **Round 3 (2026-08-31)** started the same way, on a cluster reporting
  `relay_stage_metrics_enabled=false`.

Two failure modes compounded here, and they need separate fixes:

1. **Nobody owned the flag.** A parameter that changes what a run measures must
   be set *by the runner*, applied once for every arm of a comparison, verified
   after setting (a silently-ignored config set looks exactly like a successful
   one), and printed in the run header. If it is not in the header, it is
   ambient state, and ambient state does not survive to the next session.
2. **An empty histogram was omitted rather than reported.** The scraper dropped
   zero-sample histograms, so the output had no relay keys at all — which is
   indistinguishable from nobody having asked for them. Absence of a measurement
   must be recorded *as* a measurement, with the reason. Silence is the one
   result that cannot be distinguished from not looking.

The generalisation: for any flag-gated instrumentation, the harness owns the
flag and the report names the flag's value. Anything less means the run's
meaning depends on a human or an agent remembering, and across sessions that
memory does not exist.

A corollary on when *not* to enable it: recording costs work on the measured
path. When the headline number is a comparison between arms, either arm having
it on while the other is off biases the comparison — so enable it for both arms
or neither, and never flip it between them. Round 3's headline client-clock
comparison stayed valid with it off in both arms; what that pair lost was only
the stage-level explanation, not the result.

### 20. A variable that is printed but never applied is worse than one that is missing

Four separate instances of one defect turned up within a day of each other, and
they share a shape worth naming.

| variable | printed in | actually applied? |
|---|---|---|
| `run-round3.sh` in ansible list | deploy | never staged |
| `relay_stage_metrics_enabled` | run header (`relay_stage_metrics=...`) | never set |
| `READ_LINGER_US` | per-level header (`read_linger_us=N`) | only in `MODE=readlinger` |
| `RELAY_STAGE_METRICS` | fingerprint line | never set |

In every case the harness *announced* a configuration it did not apply. That is
strictly worse than omitting it, because the run record then carries a false
statement about itself — and the record is what survives to the next session,
long after the cluster is gone. `READ_LINGER_US=250 MODE=fanout` printed
`read_linger_us=250` and ran at whatever the cluster already had. Anyone reading
that JSON later, human or agent, would have drawn a conclusion about a linger
setting that was never in effect.

Three rules follow:

1. **Print from the thing you set, not from the variable you meant to set.**
   Better still, read the value back from the cluster and print *that*. The
   fingerprint should be evidence, not intent.
2. **Apply configuration at one choke point, and verify it there.** `cfg_set` is
   now that point: it saves the original, writes, reads back, and refuses to
   continue on a mismatch. Every property the harness touches goes through it,
   so one fix covers all of them — and it auto-restores on exit.
3. **A wrapper should not touch cluster state directly.** Both wrappers tried to
   set a property with a bare `rpk`, which is not on the client's PATH:
   `run-josh236.sh: line 66: rpk: command not found`. `run-round3.sh` carried the
   identical bug and had never executed it. Wrappers now pass an env var and let
   the runner that already owns `rpk` and `cfg_set` do the work.

The generalisation of #2 and #19 together: **the harness must own every input
that changes what a run measures, and the run must record what was actually
applied rather than what was requested.** Anything less makes the meaning of a
result depend on someone remembering, and across sessions nobody does.

### 21. `jq`'s `//` operator falls through on `false`, not just `null`

`.clean // "-"` returns `"-"` when `.clean` is **false**, because jq's
alternative operator treats both `null` and `false` as absent. Every unclean
level therefore reported `"-"` instead of `"false"`, the knee test compared
against `"false"`, never matched, and the sweep printed:

```
First level that stopped being clean:
  A (linger 0)     : none in swept range
  B (linger 125us) : none in swept range
```

Three of four levels had saturated. The report stated the exact opposite of its
own data, and only the separately-printed `unclean_reasons` gave it away.

For any field that can legitimately be `false`, never use `//`:

```bash
# WRONG - false becomes "-"
jq -r '.clean // "-"' f.json
# RIGHT - only null becomes "-"
jq -r 'if .clean == null then "-" else (.clean|tostring) end' f.json
```

This is the same failure family as #20: a report making a false statement about
its own run. Boolean flags are where it hides, because the bug is invisible
whenever the flag happens to be true - `run-round3.sh` carried it through a full
clean pair without a symptom.

### 22. Do not edit a shell script while it is running

`bash` reads a script incrementally from a file offset rather than loading it
whole. Editing `wb` while `wb run josh236` was mid-execution shifted the bytes
under the running interpreter, which resumed at a now-meaningless offset and died
with a syntax error pointing at a line that is perfectly valid:

```
./wb: line 524: syntax error near unexpected token `else'
```

`bash -n wb` passed before and after, which is what makes this confusing to
diagnose: the file was never broken. The run had already completed, so nothing
was lost, but the same edit landing mid-sweep would have killed it. Edit a copy
and move it into place, or wait for the run to finish.

### 23. "Build completed successfully" proves nothing unless the changed file was compiled

A change to `transform_processor.cc`. Two builds reported success:

```
//src/v/transform:transform        -> Build completed successfully
//src/v/redpanda:application       -> Build completed successfully, 253 actions
```

Neither had compiled `transform_processor.cc`. The first compiled only `api.cc`;
the second compiled **zero** files under `src/v/transform/`. The file belongs to
`//src/v/transform:impl` (BUILD:90), a target neither request pulled in, and
bazel is perfectly happy to succeed at building what you asked for.

The change did not compile. A `bazel test` run surfaced it immediately:

```
error: variable 'min' cannot be implicitly captured in a lambda
       with no capture-default specified
```

This is #17 and #18's family - trusting a build's summary line instead of its
actual work - but with a new mechanism: not a discarded error, not a stale
symlink, simply **the wrong target**. The summary was truthful; the inference
drawn from it was not.

The check is cheap, so make it unconditional:

```bash
grep -c 'Compiling src/v/path/to/changed_file.cc' build.log   # must be >= 1
```

If a changed file does not appear in the compile actions, the build said nothing
about it. Two habits follow: prefer `bazel test //<pkg>/tests/...` over `build`
for validating a source change, since tests link the real objects and will not
silently skip them; and when a build must be used, name the target that owns the
file (`bazel query 'attr(srcs, changed_file.cc, //src/v/...:*)'` will tell you
which that is) rather than the one whose name matches the directory.

### 24. Redpanda has two metric namespaces on two endpoints, and asking the wrong one returns silence

`redpanda_*` lives on `/public_metrics`. `vectorized_*` lives on `/metrics` -
that is where the whole seastar internal registry sits: `reactor`, `smp`,
`io_queue`, scheduler internals. Neither endpoint is a superset of the other.

`scrape-metrics.sh` only ever queried `/public_metrics`, and its header comment
asserted that was sufficient. On 2026-09-01 ten `vectorized_*` names were added
to `FAMILIES` to test a cross-shard-backpressure hypothesis. They could never
have matched. A metric name asked of the wrong endpoint does not error and does
not return zero - it simply produces no series, which reads identically to *the
metric does not exist*, and that reading was one step from becoming a finding.

Two fixes, both in the harness:

- `cmd_snap` scrapes **both** endpoints per broker. `/public_metrics` is
  required; `/metrics` is best-effort (much larger, and prefiltered with
  `grep -F` at the pipe rather than carried whole in a shell variable).
- Any family that matched **no** series is now recorded in the snapshot JSON as
  `missing_families` and warned on stderr. In the artifact, not just the
  terminal - so a later reader of the JSON sees the gap even if nobody was
  watching the run.

A related trap sits one level down, and the harness cannot fix it. Every metric
in seastar's `smp` group is registered `(sm::metric_disabled)`
(`seastar/src/core/reactor.cc:3944-3957`), so it is absent from `/metrics` too.
Seastar's only enable path is `set_relabel_configs` with
`relabel_action::keep` (`metrics.cc:171-173` sets `info.enabled` from the
action); Redpanda never calls it, and the prometheus module does not expose
relabeling over HTTP. **Enabling the `smp` metrics requires a Redpanda code
change**, not a config or a scrape change. Before building a plan on a seastar
metric, check for `metric_disabled` at its registration site.

### 25. `git revert` of a commit that bundled several changes removes all of them

That change did not boot, so it was reverted. The revert did not compile:

```
error: allocating an object of abstract class type 'registry_adapter'
note: unimplemented pure virtual method 'all_partitions'
```

The commit had bundled **four** separable changes: the scheduling group (the part
that fails to boot), a `registry_adapter::all_partitions` implementation, a move
of `is_relay_sourced` into its own header, and a `require_state_recovery`
correctness guard. Reverting took all four - including the implementation of a
pure virtual declared by a *different*, uncommitted change elsewhere in the tree.

Two habits:

- **Before reverting, read the commit's own diffstat and ask which files have
  nothing to do with the reason you are reverting.** Here `api.cc`'s
  `registry_adapter` and the state-recovery guard had nothing to do with
  scheduling groups. A targeted partial revert is usually the right move.
- **After reverting, build - do not assume a revert is safe because the code
  compiled before.** "Before" was a different tree. This tree also contained
  uncommitted work that had come to depend on part of what was reverted.

The general trap: a revert is only as clean as the commit was atomic. A commit
that does one thing can be reverted; a commit that does four cannot be reverted
for one of them.

### 26. A config list that is whitespace-split can hold nothing but bare values - a comment inside it becomes data

`bench/scrape-metrics.sh` keeps its metric families in a single-quoted string
that is later whitespace-split into exact-match keys. On 2026-09-02 a
three-line explanatory comment was added *inside* that string. Shell does not
strip comments inside a quoted string, so every word of the prose became a
"metric family":

```
missing_families: ["#", "Buckets,", "for", "real", "percentiles.", ... 22 entries]
```

The two families that were *genuinely* absent (the `vectorized_smp_*` set, which
is `metric_disabled` in Seastar) were buried in that noise. A warning channel
that reports 22 false entries alongside 8 real ones is not a warning channel.

This was the second bug of its kind in the same string - the first was an
apostrophe in `Seastar's`, which terminated the string outright.

**The fix is a validator, not care.** A Prometheus metric name is
`[a-zA-Z_:][a-zA-Z0-9_:]*`; anything else in the list is a typo or stray prose,
never a family that happens to be missing. `validate_families` now exits 2 and
names the offenders. Verified by reintroducing the exact bug into a copy and
confirming exit 2 plus the three offending tokens.

**Generalisation:** any list that gets `$(...)`-split or `for f in $LIST` needs a
syntactic validator, and the prose explaining it belongs *above* the assignment
where a `#` is actually a comment.

### 27. A negative cross-clock stage has TWO causes, and assuming the wrong one suppresses a good number

`judgeCrossClock` decides whether `match` and `total` are usable by comparing
the skew estimator's output against a step threshold. On run 1788398875 it
returned **"usable"** - estimate 189us, uncertainty 628us, threshold 1000us -
while `match` in the same report read:

```
match  p50 = -61.6us   min = -227.4us
```

**The first conclusion drawn from this was wrong.** It looked like proof of a
227us clock offset that the threshold test had missed, and a check was added
that marked the cross-clock stages unusable on any negative sample.

Then `chronyc` was actually consulted. All five hosts - three brokers, two
clients - were synced to the **same** AWS reference (169.254.169.123) with RMS
offsets of **2.2 to 9.3us**, and the run's own `clock-sync.json` recorded 6.7us.
A 227us clock offset is impossible under those readings.

The real cause is architectural. `matched_at_nanos` is stamped **inside the
matcher transform on the broker** (`rust/src/main.rs`), immediately after the
record is appended to the leader's log. `ackT` is when the **client** received
its `acks=all` quorum acknowledgement - a further ~510us of replication plus
~133us of network away. The matcher therefore normally finishes *before* the
producer is told its write is durable, so `match = matchedT - ackT` is
**legitimately negative**.

That is the in-broker pre-commit read: a known, intended property of this
architecture and part of where the latency win comes from. It is a durability
trade, not a measurement error.

**Consequences, all of which the wrong reading got backwards:**

- `match` is a **race between two paths, not a duration.** Its sign carries
  information. Never quote it as latency, and do not expect it to be positive.
- **`total` is valid.** With hosts inside 10us, `total = receiptT - sendT` is
  accurate to about that, and it is the e2e number. The premise that it carries
  a +/-400-650us bar came from an assumption about sync quality that the chrony
  data refutes.
- **`produce + relay_consume` is not a lower bound on e2e, it is an
  OVERestimate**, because it drops a term that is genuinely negative.
- The bracket skew estimator's +/-628us uncertainty is *wider than the
  quantity*, so its 189us point estimate is indistinguishable from zero. It is
  too weak to detect a 5us offset and must not be read as evidence of one.

**The check that survives** compares the negative magnitude against an
independently measured clock offset (`-clock-rms-micros`, fed from
`clock-sync.json`). If the magnitude dwarfs it, the clocks cannot explain the
sign and the pre-commit read is established; if it is comparable, clock error is
sufficient and the stages are unquotable; if no offset was supplied, the report
says it cannot tell rather than guessing. It **reports** and never suppresses
`total` - the earlier version's false positive would have hidden a good number,
and this file already learned that a check whose default is "broken" is worse
than no check (see `CrossClockUnusable`'s own comment).

**Generalisation:** before attributing an impossible-looking reading to
instrument error, measure the instrument. "This value is impossible, therefore
the clock is wrong" and "this value is impossible, therefore my model of what
the timestamps mean is wrong" look identical in the data, and only an
independent measurement separates them.

### 28. `total = produce + match + relay_consume` is an identity in this loadgen, so their agreement proves nothing

Having found `match` corrupted, the tempting move is to report the sum of the
two trustworthy single-clock legs (`produce + relay_consume`) and note that it
agrees with the measured `total` to within ~60us, calling that a cross-check.

It is not one. The loadgen builds these stages from four timestamps such that
`total ≡ produce + match + relay_consume` exactly, so `sum - total = -match`
always, by construction. The "agreement" is a restatement of the corrupted
number, not independent evidence about it.

~~What the single-clock sum *is* good for: since true elapsed `match >= 0`, the
sum `produce + relay_consume` is a genuine **lower bound** on true e2e.~~
**RETRACTED - see 27.** `match` is not an elapsed time and is legitimately
negative, so dropping it makes the sum an **overestimate**, not a lower bound.
Quote `total` itself: the hosts are synced to under 10us.

A real cross-check has to come from a different measurement path. The
count-vs-buckets check on `emit_to_guest` (40,000,000 both ways, exact) is one;
the sum-vs-buckets mean check on the same histogram (70.7us vs 80.8us) is
another, and it earned its keep by revealing that samples cluster toward the low
end of each log-spaced bucket - which means interpolated percentiles from those
buckets are biased **high**.

### 29. Sending the right NUMBER of records says nothing about sending them at the right TIMES

The traditional-deployment leg at 500 external consumers passed every gate this
harness had: `attempted_ratio` 99.99%, `achieved_ratio` 99.99%,
`client_bound` false, `lag.grew_during_run` false, 29,997 of 29,997 receipts,
zero missing. It reported `total_p50 = 7,629us`.

It was contaminated. `send_lateness` - how late each order went out against its
own fixed-pacing schedule - read p90 534us and p99 2,486us against a 1,000us
interval, i.e. **2.5 pacing slots late at the tail**. The in-broker reference
run on the same cluster read p90 2.1us.

loadgen was the producer AND the measured consumer, sharing one 36-vCPU
instance with 499 background consumer groups pulling the same topic. The CPU
starvation that delayed its sends equally delayed its receipt handling, so an
unknown share of that 7,629us was client-side queueing rather than the system
under test.

**Why every existing check missed it.** They all count records.
`attempted_ratio` is (orders attempted / orders offered) over the whole window -
a starved client that drifts late but eventually emits the full count scores
~100%. Lag is measured on the broker, which was fine. Nothing looked at the
distribution of send TIMESTAMPS against the schedule, even though loadgen was
already recording exactly that and reporting it.

**Fixed:** a pacing-fidelity gate. Under `-pacing fixed`, if
`send_lateness` p99 exceeds one pacing interval (1e6/rate us), the run is not
clean and the reason names the slot count, the p90, and what to do about it.
Tested against both the real contaminated numbers and the healthy reference so
neither can regress silently. Does not apply to burst pacing, where there is no
per-order schedule to be late against.

**Generalisation:** when a rig generates load, measure the rig's own fidelity
and gate on it. A saturated load generator produces numbers that look like
results, and every consistency check that counts totals rather than timings
will agree with them.
