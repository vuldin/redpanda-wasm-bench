# Benchmark methodology

Every entry below is a way this harness produced a confidently wrong number.
A wrong number from a broken harness looks identical to a right one, which is
why this file exists: it's the checklist for not repeating them, and the record
of what each one cost.

Start with [Before you trust a number](#before-you-trust-a-number). The
numbered entries after it are the evidence behind each line of that checklist,
and the code cross-references them by number (`METHODOLOGY #5` and so on), so
the numbering doesn't change even when an entry stops applying.

A note on where these came from. The first nine entries predate this repo.
They were found against an earlier closed-loop harness in
[wasm-orderbook](https://github.com/vuldin/wasm-orderbook), where `cmd/bench`
sent one order and waited for its reply before sending the next. This repo's
`cmd/loadgen` is open-loop and rate-paced instead, so a few of those entries
are history rather than instructions. Each one says which it is.

## Before you trust a number

- Fresh topics for this run, not reused from a previous one (#3).
- The run reports `clean:true`, and pacing held, meaning `send_lateness` p99
  inside one pacing interval (#29). If it's `clean:false`, re-run. Don't
  report the numbers with a caveat.
- `window_coverage` is at or near 1.0 (#14). Below that the metric window
  closed before the pipeline finished, and every per-record figure is divided
  by a truncated denominator. Raise `-drain`.
- Warmup was non-zero and actually ran before timing started (#7).
- `-admin-url` and `-transform-name` were set for any wasm-transform leg, so a
  VM restart during the window shows up as `clean:false` instead of one
  silent outlier (#9).
- Order IDs are run-scoped rather than restarted at 0 (#8). `loadgen` prefixes
  them with the run's start-time nanoseconds by default, so don't hardcode
  IDs in anything that calls it.
- You know whether `write.caching` was on or off, and you're labelling results
  accordingly. "In-broker vs external, write-caching on" is a different and
  non-comparable number from the same thing with it off.
- If you're comparing two entrypoints, both are tuned equally (#5). A fix
  applied to one side's producer and not the other makes one leg look slower
  for a reason that has nothing to do with in-broker vs external.
- If you're testing consumer fanout, `readers_cache_target_max_size` is above
  the fan-out count (#10). Check `check-bottlenecks.sh` output for the actual
  reader-cache hit ratio rather than assuming the config change alone fixed
  it.
- Shard and core placement was checked via `GET /v1/cluster/partitions` or
  `rpk cluster partitions list -a` (#11), never
  `GET /v1/partitions/{ns}/{topic}/{partition}`'s `.replicas[].core`.
- Any number from a heavy-contention scenario comes from at least two or three
  trials rather than one (#12). Report the range.
- Arms you compare ran on the same cluster, in the same run. Two clusters
  built from the same tfvars have measured 40-70% apart on per-invocation
  guest CPU with identical code, rate and build, so a difference smaller than
  that between runs on different clusters is not evidence. That is why plans
  interleave the arms they compare.
- A p99.99 comes from at least 100,000 samples (RESULTS.md prints n/a below
  that), and tails are only compared between windows of similar length.
- If the run changed any `needs_restart` property, so the `data_transforms_*`
  sizing ones, `relay_enabled` or `relay_port`, the cluster was restarted
  afterwards and `rpk cluster config status` shows no pending restart (#13).
  `rpk cluster config get` echoing the new value is not evidence it's in
  effect.
- A result that's suspiciously stable across different inputs has been
  challenged rather than accepted (#15). Vary an unrelated input. If the
  number doesn't move, it's a failure artifact rather than a capacity.
- For any stage computed from one client-side and one broker-side timestamp,
  so `match` and `total`, the run's own clock-sync residual is recorded and
  quoted alongside it. Single-clock stages (`produce`, `send_lateness`,
  `relay_consume`) need no such caveat (#27).

## Things that produced a wrong number

### 1. `data_transforms_*` cluster config defaults don't fit a Go guest

First symptom was `unable to deploy transform: ... data transforms disabled`,
then `memory minimum size of 38 pages exceeds memory limits`, then `wasm
invocation exceeded its configured runtime limit` on the very first deploy.
None of these are benchmark bugs. They're real onboarding friction for any
Go-built transform-sdk guest against an out-of-the-box cluster. This repo's
ansible playbook ships the settings that fix it, so you shouldn't hit it here.

### 2. A real correctness bug in the engine, found by the first real run

The first successful deploy produced `encoded crc ... does not match
calculated crc ...` on every single output record, from a real `franz-go`
consumer. That was `rpc_client_sink::write()` recomputing Redpanda's internal
`header_crc` but not the actual Kafka wire-protocol `crc`, after mutating
fields both cover. It's fixed on the
[`transform-latency-instrumentation`](https://github.com/vuldin/redpanda/tree/transform-latency-instrumentation)
branch, folded into the commit that introduced the mutation it corrects
("transform: give guests control of output timestamps, partitioning and
idempotence").

The reason it's recorded here is that "the benchmark returned 0 records"
doesn't always mean the benchmark is broken. Check the broker log for `crc`
mismatches before assuming the harness is at fault.

Cited by commit subject rather than SHA on purpose. That branch gets rewritten
deliberately, since each commit is meant to hold one feature so it can be
split up later, and the SHA this used to name was orphaned by exactly that,
leaving a 404. Subjects survive a rebase, hashes don't.

### 3. Topic reuse across runs contaminates the next one

Reusing the same topic names across repeated debugging attempts meant a
"clean" 500-record run was really measuring 500 records queued behind
thousands of leftover records from earlier failed attempts. It reported about
a second of latency, flat to the microsecond across all 500, which was itself
the tell: a real distribution doesn't look like that.

Every runner here names its topics with the run id. Never reuse a topic name
across runs unless you specifically want to test recovery or backlog
behaviour.

### 4. Pipelined send-then-drain measures the wrong thing

The original harness sent all N orders as fast as possible and then drained
the output topic, which measures pipeline-fill time rather than per-record
latency. However many records land in one underlying Kafka fetch response get
timestamped together in the same tight receive-loop iteration, collapsing
their individual latencies into one value. You can see it as near-zero spread
across every percentile.

This is history rather than a live check. `cmd/loadgen` is open-loop and
rate-paced: it offers orders on a fixed schedule and correlates each receipt
back to its own order, so there's no drain-and-timestamp step to get wrong.
What replaced this trap is the pacing gate in #29.

### 5. franz-go's default 10ms producer linger, on both sides

The single biggest one. franz-go batches produces for up to 10ms by default,
hoping more records arrive to send together. Under a latency methodology
there's never more than one record in flight, so that 10ms was never doing its
job, just adding dead time to every measurement, on both the order-submission
side and the fill-production side. It accounted for the overwhelming majority
of every double-digit millisecond number this benchmark ever produced.

Set `kgo.ProducerLinger(0)`. `loadgen` defaults `-linger-ms` to 0 and records
it in the fingerprint.

Keep linger 0 for throughput tests as well, and raise offered load with
`PRODUCERS` instead. A client linger adds roughly its own length to every
produce and also groups records within one transform invocation. An earlier
version of this entry said linger 0 caps throughput at RF>1 and recommended
1-5ms; that came from a harness defect (topics deleted under a live run by an
orphaned runner's cleanup) and has not been reproduced: rate sweeps at
linger 0 with two producers run clean well past 60,000 orders/sec at RF=3.

### 6. Per-record fsync

With `write.caching` off, which is Redpanda's default, every produce durably
flushes to disk before acking. That's a real durability guarantee and a real
few milliseconds of added latency on whatever disk you're running on.
Enabling `write.caching=true` on the benchmark's topics cut several more
milliseconds off, before the linger fix was even applied.

This is a genuine durability/latency tradeoff rather than a free win. See
[write.caching](#writecaching-read-this-before-you-set-it) below. The runners
set it explicitly rather than relying on a cluster-wide default, and print
what they did. A result run without it isn't wrong, it's measuring a more
durable and slower configuration, and should be labelled that way.

### 7. Cold VM and cold connections pollute the first N samples

The very first records against a freshly deployed transform pay for things a
steady-state record never will: Go-runtime cold start inside the guest, which
can be well over a second, and consumer-group and metadata setup on the client
side. Averaging those into a short run swamps the real signal.

Every runner here sends and discards a warmup window before the timed one.
Check that warmup was non-zero and actually ran.

### 8. Reusing numeric order IDs across separate runs against the same topic

The matcher keeps its order book, or for the wasm guest its snapshotted state,
in memory for the processor's entire lifetime, which spans every run pointed
at the same topic pair rather than just the current one. An earlier harness
started IDs at `"0"` every run, so run 2's order `"0"` collided with run 1's
still-resting order `"0"`, got rejected as `ErrDuplicateOrder`, and was
silently dropped per the documented error policy. The harness then hung
forever waiting for a fill that would never arrive, because the input it was
waiting on was never accepted.

`loadgen` prefixes every order ID with the run's own start-time nanoseconds,
which makes collision across runs impossible without needing to clear the
topic between them.

### 9. The wasm engine can restart its own VM mid-benchmark, unrelated to guest activity

Observed twice and not root-caused: a transform that had been running cleanly
for two full runs logged `wasm invocation exceeded its configured runtime
limit` and restarted, on one occasion within 1ms of `starting wasm vm`, which
rules out a genuinely slow invocation since the trap fired before any real
guest code could have run. Both times it produced exactly one roughly
one-second outlier in that run's results, from the Go-runtime cold start on
the forced restart, with every other record unaffected.

The harness can't fix this, only detect it. Pass `-admin-url` and
`-transform-name` for the wasm leg and `loadgen` scrapes the transform's own
`redpanda_transform_failures` counter, the same metric `transform_manager`
increments on every give-up-and-restart cycle, before and after the timed
window. If it moved, the report is `clean:false` and says so. Re-run rather
than trust that report's percentiles.

There's nothing equivalent to check for an external matcher. It has no wasm VM
to restart, which is itself worth noting as an asymmetry between the two
paths' tail-latency risk profiles.

This is still open and still not root-caused.

### 10. Reader-cache thrashing above `readers_cache_target_max_size`

Found while investigating heavy consumer fanout at 231 groups. The
per-partition reader-object cache, which is distinct from and independent of
the batch/data cache, is capped at 200 entries by default. A fan-out above
that cap thrashes it. You can see it as `vectorized_storage_log_cache_hits`
and `_cache_misses` on the internal `/metrics` endpoint, not
`/public_metrics`, sitting at a 97-99.99% miss ratio even while
`vectorized_storage_log_cached_read_bytes` against `_read_bytes`, which is the
batch cache, stays a clean 100% hit.

This is known Redpanda fetch-path cache-sizing behaviour rather than anything
specific to this benchmark or to wasm. Check for already-documented mechanisms
before proposing a new causal story for a performance anomaly.

Set `readers_cache_target_max_size` above the fan-out count.
`check-bottlenecks.sh` reports the actual hit/miss ratio for the topics under
test after every measurement, so a thrashing run says so in its own output
instead of requiring after-the-fact metric archaeology. Raising the cap cut
absolute fanout latency by roughly a third in testing, which is real and worth
doing, but the miss ratio can stay high even after raising it well above the
fan-out count: a cache hit needs an exact start-offset match, and capacity
alone doesn't guarantee independent async consumers land on the same offset at
the same instant. Don't assume "raised the cap" means "problem solved". Check
the ratio.

### 11. The wrong admin-API endpoint for shard placement gives a wrong answer silently

`GET /v1/partitions/{ns}/{topic}/{partition}`'s `.replicas[].core` field
doesn't reflect live node-local shard assignment. It returned `0` for every
partition checked, unconditionally, regardless of where `shard_balancer`
actually placed it. That produced a wrong "everything collapses onto shard 0,
even with 4 shards available" conclusion which looked completely plausible,
being consistent across a dedicated multi-partition test topic and consistent
after an explicit rebalance trigger, and was wrong throughout.

Use `GET /v1/cluster/partitions`, which is what `rpk cluster partitions list
-a` uses internally, for real shard and core placement. Cross-check with `rpk
cluster partitions list -a` directly if in doubt.

Be suspicious of a placement result that contradicts what you already know
about how Redpanda's balancer is supposed to behave. It continuously balances
partitions and leadership, so "everything landed on shard 0 and stayed there"
should make you re-verify the query rather than build a conclusion on it.
`lib-cluster.sh` always queries the correct endpoint.

### 12. A single run under a high-variance regime is a sample, not a measurement

Heavy fanout, meaning hundreds of concurrent consumer groups against one hot
partition, is a genuinely high-variance regime. Three consecutive trials of an
identical configuration produced in-broker against external ratios that
ranged widely enough that any single trial's specific decimal was misleading
on its own, even though the qualitative direction held in all three.

Report a range across multiple trials for any number measured under heavy
contention. Don't state a specific ratio from one run under fanout and treat
it as precise.

The in-broker arm in REPRODUCE.md is three trials for this reason, and its own
spread is about 39% at p50.

### 13. `rpk cluster config get` reports a value the running broker isn't using

`data_transforms_max_instances_per_core` and
`data_transforms_per_function_memory_limit` are `needs_restart: yes`. What that
means in practice:

- `rpk cluster config set` updates the cluster's desired config, and `rpk
  cluster config get` immediately echoes the new value back.
- The running broker keeps the value it booted with. Both the deploy-time
  admission check in `plugin_frontend::validate_mutation` and
  `wasm::heap_allocator`'s pool use the boot value.
- `rpk cluster config status` is the only thing that tells you. It reports
  `NEEDS-RESTART true` per node.

So a live `set` on these properties changes nothing in the running broker
while `get` reports success. That's the trap: not a partial application, a
completely inert one that reads as applied.

The first version of this entry got the mechanism wrong, and the correction is
worth keeping. It claimed the admission check read the value live while only
the pool stayed at boot, producing a split failure where a deploy is accepted
and then the processor fails to start with `unable to allocate memory within
requested bounds`. That was inferred from reading the code, since
`validate_mutation` does call `config::shard_local_cfg()` on each invocation,
and never verified. It's wrong: `shard_local_cfg()` itself still holds the boot
value for a `needs_restart` property, so admission doesn't move either.

How it was settled, which is worth repeating as a technique because the code
read wasn't enough and neither was a walk-up test:

1. With desired cap at 1/core on a 13-shard node, so a ceiling of 13, a single
   deploy against a 50-partition input topic was accepted. One deploy crossing
   the ceiling on its own removes any question about accumulation.
2. The cluster was rolled so 1/core became the boot value.
3. The identical deploy then rejected, at exactly the predicted ceiling, with
   the broker logging `deploy of transform ... needs 50 partition-instances,
   which would bring the total to 68 (existing 18 + new 50), exceeding the
   estimated cluster capacity of 13`.

Same command, same desired config, opposite outcome across a restart. Nothing
short of that would have been proof.

What follows for measurement:

- Never trust `rpk cluster config get` as evidence that a `needs_restart`
  property is in effect. Check `rpk cluster config status`, or restart.
- Any capacity experiment that raises a cap and reads back "we deployed N" is
  measuring the boot config.
- An earlier round of fanout scripts set `max_instances_per_core=200` live with
  no restart. That raise was entirely inert: the pool and the admission check
  both stayed at the boot value of 100/core. It didn't corrupt those results,
  since fanout 100 over 13 shards needed about 8 per shard, far under 100, but
  the scripts weren't doing what they appeared to.

Separately, and still true: the admission check is
necessary-but-not-sufficient by its own comment, since it scales by the
deciding node's core count only and doesn't model uneven placement. So
"accepted" and "actually running" can differ.

### 14. A drain that's too short truncates the metric window and corrupts every per-record figure

The symptom was a report where per-order CPU came out lower than per-record
CPU, which is impossible since an order is two records. Raw counts at 5,000
orders/sec with a then-default 10s drain:

| | 10s drain | 40s drain |
|---|---|---|
| records sent | 200,000 | 200,000 |
| producer invocations (metric delta) | 59,948 | 200,000 |
| coverage | 30% | 100% |
| receipts vs expected | 109,017 / 100,000 | 100,000 / 100,000 |
| us/order over us/record | 0.60 (impossible) | 2.00 |
| verdict reported | clean | clean |

`loadgen` brackets the timed window with a before/after metric scrape, and the
after-scrape happens once the drain expires. If the pipeline hasn't finished
by then, the invocation delta is truncated while `records_sent` stays whole,
so per-record cost is a real numerator divided by a partial denominator. It
scales with rate, so the error grows exactly where the numbers matter most,
and it reported clean.

Three things it wasn't, each checked and eliminated in this order:

- A mislabelled counter. `wasmtime.cc`'s measurement is taken inside
  `pre_record()`/`post_record()`, so `execution_latency_sec_count` counts
  records, as labelled. Confirmed empirically at 500 orders/sec, where
  invocations equalled records exactly and us/order over us/record was exactly
  2.00.
- Leadership churn. 35 election lines on the leader broker looked conclusive,
  but every one was `term: 0 -> term: 1`, so initial elections for freshly
  created partitions plus the harness's own leadership transfer. No mid-run
  churn.
- Duplicate receipts from a processor restart. Plausible, and instrumented:
  `duplicate_receipts_sampled` came back 0, `max_receipts_for_any_order` 1,
  `transform_failures` 0.

Two of those three were confident hypotheses that measurement killed, and the
order matters. The cheap empirical check, being a low-rate run where nothing
can be backlogged, settled in one run what two rounds of code reading and log
archaeology did not.

Still unexplained: the short-drain run's receipts exceeded expected by 9%,
which truncation alone doesn't account for. It didn't reproduce at 40s. Left
recorded as unexplained rather than given an invented mechanism.

Fixed two ways. `loadgen` computes `window_coverage` as producer invocations
over records sent and fails the run below 95%, naming the cause and the
remedy. That's the check whose absence let a 30%-coverage run pass as clean.
And the default drain is now 40s rather than 10s, with the measured reason in
the comment so it doesn't get reverted as over-cautious.

A per-record figure is only valid if the run reports `window_coverage` at or
near 1.0. Quote nothing derived from a truncated window.

### 15. A number that stays identical across a 10x range of inputs isn't a measurement

Every rate level above the first reported an attempted send rate of about
2,739 orders/sec. It was read as the client's throughput ceiling. It was
identical whether 10,000, 20,000, 50,000 or 100,000 orders/sec was offered.

That invariance was the whole answer and it was visible from the first table.
A real ceiling is approached from below and then held. It doesn't produce the
same number for a 10x range of demand. What produces an identical number is an
artifact of a fixed failure path: here every produce failed, franz-go retried,
the buffer filled, `Produce` blocked for tens of seconds, the send window
overran 30s to 73s, and `attempted = sent / elapsed` landed in the 2,700s by
arithmetic.

The actual trigger was found by the cheapest possible experiment: two
identical levels at a rate that passes on its own. Level 1 clean, level 2 zero
receipts and 100% produce failure. Sequence position, not rate. That test took
four minutes and could have been run first.

The rule: before theorising about a limit, check whether the number responds
to the input at all. If it doesn't, you're measuring a failure mode rather
than a capacity. Vary the suspected variable and one unrelated variable, and
if both give the same answer then neither is the cause.

The cause, for the record, was a shell variable holding the input topic name
being overwritten with an order count inside the level loop, so every level
after the first produced to a topic named e.g. `30000`. All seven hypotheses
were about the broker or the Kafka client. None was about the harness's own
variable scoping. The topic-variable guard now in `run-e2-throughput.sh` would
have caught it in the first second of level 2.

On hypothesis order: the seven eliminated hypotheses, being fsync, producer
linger, client in-flight caps, leadership churn, processor restarts, a stale
binary, and unscoped cleanup deleting live topics, were each plausible, each
took real time, and each was killed by measurement. Every one was a mechanism
proposed before the failure had been characterised. Characterising it first,
so does it depend on rate, on position, does the data reach the broker at all,
is strictly cheaper than proposing mechanisms and testing them one at a time.

### 16. A ratio identity is the cheapest bug detector available

This workload emits exactly two records per order, so `us/order` has to be
exactly `2 x us/invocation`. That single identity has caught three separate
denominator bugs, including two that produced entirely plausible numbers:

- #12's CPU-divided-by-receipts, where the ratio came out about 1 and should
  have been 2.
- #15's variable collision, found by other means, but the ratio also broke.
- A window mismatch reporting `6.94 us/order` against `2.60 us/invocation`, a
  ratio of 2.67. Nothing about 6.94 looks wrong on its own. The ratio is what
  made it obviously wrong.

That last one wasn't a measurement error at all. loadgen's figures were
correct, with `invocations == records_sent` and `window_coverage == 1.0`. It
was the runner's outer `scrape-metrics` line, whose snapshot pair brackets
loadgen's whole invocation including warmup, so 80,000 invocations, while
dividing by a warmup-excluded order count of 30,000. Two windows, one
division.

What follows:

- Never divide two quantities unless you can name the single window both were
  measured over. Mismatched windows are the most common source of confidently
  wrong per-unit figures in this project.
- Print a known-value ratio next to every derived per-unit number. It costs
  one line and it's the tripwire that fires when a denominator drifts.
- When the tripwire fires, check the raw counts before rewriting any analysis.
  The bug is more often in the reporting line than in the system.

### 17. Never pipe a build through `tail`

The redpanda build was launched as `bazel build ... 2>&1 | tail -40`,
backgrounded. It reported exit code 0 and completed, and the visible output
ended with a normal-looking progress line. The build had failed.

Two independent things went wrong, and either one alone wastes a cycle:

- The exit code belonged to `tail` rather than to bazel. A pipeline's status is
  its last command's, so a failed build wrapped in `| tail` reports success.
  The only reason this was caught is that bazel prints `ERROR: Build did NOT
  complete successfully` on its own, which happened to survive inside the last
  40 lines.
- `tail -40` discarded the compiler diagnostic. The error was about 9,300
  actions earlier in the log. Diagnosing it needed a re-run of the whole
  build, which was cheap only because bazel had cached 8,810 actions. On a cold
  tree that's an hour.

So redirect a build to a file and then grep it: `bazel build ... > build.log
2>&1; echo "exit=$?"`. Never `| tail`, `| head`, or `| grep` as the outermost
stage. Check the tool's own exit status rather than a pipeline's, and if a
pipe is unavoidable, `set -o pipefail` first. This generalises past builds to
any long command whose failure lives far from its end, so test suites, ansible
runs, terraform applies.

The actual bug, for the record, was a name collision worth knowing on its own.
`relay::service` has a nested `struct config`, so inside its member functions
an unqualified `config::shard_local_cfg()` resolves to the nested struct
rather than the global `config` namespace. It needs a leading `::`.

### 18. `bazel-bin` is a moving symlink, so a `bazel test` can swap the binary you benchmark

Caught at the moment of provisioning, before any spend. `wb doctor` reported
the local build as eleven days old, immediately after a successful build that
same evening.

`bazel-bin` is a convenience symlink that bazel repoints to whichever
configuration it built last. The benchmark build is `-c opt`, which writes to
`bazel-out/k8-opt/bin`. Unit-test commands run earlier that evening omitted
`-c opt`, so they built in fastbuild config and repointed `bazel-bin` at
`bazel-out/k8-fastbuild/bin`, where an untouched eleven-day-old binary was
still sitting. `wb` ships `$REDPANDA_SRC/bazel-bin/...`.

Had it not been caught, `wb deploy` would have pushed a binary without the
change under test, verified by zero occurrences of the new config property,
and a 762MB unoptimized broker instead of the 207MB opt one, whose latency
numbers are meaningless, while `deploy`, `ready` and `status` all reported
success.

So never compare timestamps or shas through `bazel-bin` and assume a
configuration. Resolve it with `readlink -f` and confirm `k8-opt`. `wb doctor`
now fails hard if it's anything else. Pass `-c opt` to `bazel test` as well,
not only `bazel build`, or the next test run re-points the symlink again. And
prefer a content check over a freshness check: `strings <binary> | grep -x
<new symbol>` answers "is my change in the thing I'm about to ship", which is
the actual question. An mtime doesn't.

Generally: when a tool hands you a stable path to a variable artifact, the
path isn't the identity. Verify the artifact.

A near-miss worth recording separately. `ls -d` with colour enabled emits ANSI
escapes into the path, so a later `stat` or `strings` on that captured string
reads a nonexistent file and returns empty rather than an error, which looks
exactly like "the change is missing". Two checks in a row disagreed before the
cause was found. Build paths from literals or `find -print0`, never from
coloured `ls`.

### 19. Instrumentation gated on a config flag records nothing unless the harness sets the flag

`relay_stage_metrics_enabled` gates recording into the relay stage histograms.
It defaults to false, and for two rounds nothing in the harness ever set it,
so whether a run captured a stage breakdown depended entirely on what the
cluster had last been left at by hand.

Both times the flag was false, and both times the loss was invisible until
analysis. One round produced no dispatch-versus-scheduling attribution, which
was the exact question the histograms had been built to answer. The next round
started the same way, on a cluster reporting
`relay_stage_metrics_enabled=false`.

Two failure modes compounded, and they need separate fixes:

1. Nobody owned the flag. A parameter that changes what a run measures has to
   be set by the runner, applied once for every arm of a comparison, verified
   after setting since a silently-ignored config set looks exactly like a
   successful one, and printed in the run header. If it isn't in the header
   it's ambient state, and ambient state doesn't survive to the next session.
2. An empty histogram was omitted rather than reported. The scraper dropped
   zero-sample histograms, so the output had no relay keys at all, which is
   indistinguishable from nobody having asked for them. Absence of a
   measurement has to be recorded as a measurement, with the reason. Silence
   is the one result that can't be distinguished from not looking.

For any flag-gated instrumentation, the harness owns the flag and the report
names the flag's value. Anything less means the run's meaning depends on a
human or an agent remembering, and across sessions that memory doesn't exist.

One caveat on when not to enable it: recording costs work on the measured
path. When the headline number is a comparison between arms, either arm having
it on while the other is off biases the comparison, so enable it for both arms
or neither and never flip it between them. A headline client-clock comparison
stays valid with it off in both arms. What that pair loses is only the
stage-level explanation, not the result.

### 20. A variable that's printed but never applied is worse than one that's missing

Four separate instances of one defect turned up within a day of each other,
and they share a shape worth naming.

| variable | printed in | actually applied? |
|---|---|---|
| a wrapper script in the ansible list | deploy | never staged |
| `relay_stage_metrics_enabled` | run header | never set |
| `READ_LINGER_US` | per-level header | only in one mode |
| `RELAY_STAGE_METRICS` | fingerprint line | never set |

In every case the harness announced a configuration it didn't apply. That's
strictly worse than omitting it, because the run record then carries a false
statement about itself, and the record is what survives to the next session
long after the cluster is gone. `READ_LINGER_US=250 MODE=fanout` printed
`read_linger_us=250` and ran at whatever the cluster already had. Anyone
reading that JSON later, human or agent, would have drawn a conclusion about a
linger setting that was never in effect.

Three rules follow:

1. Print from the thing you set, not from the variable you meant to set.
   Better still, read the value back from the cluster and print that. The
   fingerprint should be evidence rather than intent.
2. Apply configuration at one choke point, and verify it there. `cfg_set` is
   that point: it saves the original, writes, reads back, and refuses to
   continue on a mismatch. Every property the harness touches goes through it,
   so one fix covers all of them, and it auto-restores on exit.
3. A wrapper shouldn't touch cluster state directly. Two wrappers tried to set
   a property with a bare `rpk`, which isn't on the client's PATH, giving
   `rpk: command not found`. Wrappers now pass an env var and let the runner
   that already owns `rpk` and `cfg_set` do the work.

Taking #19 and #20 together: the harness has to own every input that changes
what a run measures, and the run has to record what was actually applied
rather than what was requested.

### 21. `jq`'s `//` operator falls through on `false`, not just `null`

`.clean // "-"` returns `"-"` when `.clean` is false, because jq's alternative
operator treats both `null` and `false` as absent. Every unclean level
therefore reported `"-"` instead of `"false"`, the knee test compared against
`"false"`, never matched, and the sweep printed:

```
First level that stopped being clean:
  A (linger 0)     : none in swept range
  B (linger 125us) : none in swept range
```

Three of four levels had saturated. The report stated the exact opposite of its
own data, and only the separately-printed `unclean_reasons` gave it away.

For any field that can legitimately be `false`, never use `//`:

```bash
# wrong, false becomes "-"
jq -r '.clean // "-"' f.json
# right, only null becomes "-"
jq -r 'if .clean == null then "-" else (.clean|tostring) end' f.json
```

Same failure family as #20: a report making a false statement about its own
run. Boolean flags are where it hides, because the bug is invisible whenever
the flag happens to be true, and one script carried it through a full clean
pair without a symptom.

### 22. Don't edit a shell script while it's running

`bash` reads a script incrementally from a file offset rather than loading it
whole. Editing `wb` while a `wb run` was mid-execution shifted the bytes under
the running interpreter, which resumed at a now-meaningless offset and died
with a syntax error pointing at a line that's perfectly valid:

```
./wb: line 524: syntax error near unexpected token `else'
```

`bash -n wb` passed before and after, which is what makes this confusing to
diagnose. The file was never broken. That run had already completed so nothing
was lost, but the same edit landing mid-sweep would have killed it. Edit a copy
and move it into place, or wait for the run to finish.

### 23. "Build completed successfully" proves nothing unless the changed file was compiled

A change to `transform_processor.cc`. Two builds reported success:

```
//src/v/transform:transform        -> Build completed successfully
//src/v/redpanda:application       -> Build completed successfully, 253 actions
```

Neither had compiled `transform_processor.cc`. The first compiled only
`api.cc`, and the second compiled zero files under `src/v/transform/`. The file
belongs to `//src/v/transform:impl`, a target neither request pulled in, and
bazel is perfectly happy to succeed at building what you asked for.

The change didn't compile. A `bazel test` run surfaced it immediately:

```
error: variable 'min' cannot be implicitly captured in a lambda
       with no capture-default specified
```

Same family as #17 and #18, trusting a build's summary line instead of its
actual work, but with a new mechanism: not a discarded error and not a stale
symlink, simply the wrong target. The summary was truthful. The inference drawn
from it wasn't.

The check is cheap, so make it unconditional:

```bash
grep -c 'Compiling src/v/path/to/changed_file.cc' build.log   # must be >= 1
```

If a changed file doesn't appear in the compile actions, the build said nothing
about it. Two habits follow. Prefer `bazel test //<pkg>/tests/...` over `build`
for validating a source change, since tests link the real objects and won't
silently skip them. And when a build has to be used, name the target that owns
the file, which `bazel query 'attr(srcs, changed_file.cc, //src/v/...:*)'` will
tell you, rather than the one whose name matches the directory.

### 24. Redpanda has two metric namespaces on two endpoints, and the wrong one returns silence

`redpanda_*` lives on `/public_metrics`. `vectorized_*` lives on `/metrics`,
which is where the whole seastar internal registry sits: `reactor`, `smp`,
`io_queue`, scheduler internals. Neither endpoint is a superset of the other.

`scrape-metrics.sh` only ever queried `/public_metrics`, and its header comment
asserted that was sufficient. Ten `vectorized_*` names were later added to
`FAMILIES` to test a cross-shard-backpressure hypothesis. They could never have
matched. A metric name asked of the wrong endpoint doesn't error and doesn't
return zero, it produces no series, which reads identically to the metric not
existing, and that reading was one step from becoming a finding.

Two fixes, both in the harness. `cmd_snap` scrapes both endpoints per broker,
with `/public_metrics` required and `/metrics` best-effort since it's much
larger, prefiltered with `grep -F` at the pipe rather than carried whole in a
shell variable. And any family that matched no series is now recorded in the
snapshot JSON as `missing_families` and warned on stderr, so a later reader of
the JSON sees the gap even if nobody was watching the run.

A related trap sits one level down and the harness can't fix it. Every metric
in seastar's `smp` group is registered `(sm::metric_disabled)` in
`seastar/src/core/reactor.cc`, so it's absent from `/metrics` too. Seastar's
only enable path is `set_relabel_configs` with `relabel_action::keep`, and
Redpanda never calls it while the prometheus module doesn't expose relabeling
over HTTP. Enabling the `smp` metrics needs a Redpanda code change rather than
a config or scrape change. Before building a plan on a seastar metric, check
for `metric_disabled` at its registration site.


Some histograms are registered `skip_when_empty`, `vectorized_reactor_stalls`
among them. For those, an entry in `missing_families` means zero events in the
window, not that the metric is unavailable.
### 25. `git revert` of a commit that bundled several changes removes all of them

A change didn't boot, so it was reverted. The revert didn't compile:

```
error: allocating an object of abstract class type 'registry_adapter'
note: unimplemented pure virtual method 'all_partitions'
```

The commit had bundled four separable changes: the scheduling group, which is
the part that fails to boot, a `registry_adapter::all_partitions`
implementation, a move of `is_relay_sourced` into its own header, and a
`require_state_recovery` correctness guard. Reverting took all four, including
the implementation of a pure virtual declared by a different, uncommitted
change elsewhere in the tree.

Two habits. Before reverting, read the commit's own diffstat and ask which
files have nothing to do with the reason you're reverting. Here `api.cc`'s
`registry_adapter` and the state-recovery guard had nothing to do with
scheduling groups, so a targeted partial revert was the right move. And after
reverting, build. Don't assume a revert is safe because the code compiled
before, since "before" was a different tree, and this tree also contained
uncommitted work that had come to depend on part of what was reverted.

The general trap: a revert is only as clean as the commit was atomic. A commit
that does one thing can be reverted. A commit that does four can't be reverted
for one of them.

### 26. A whitespace-split config list can hold nothing but bare values

`bench/scrape-metrics.sh` keeps its metric families in a single-quoted string
that's later whitespace-split into exact-match keys. A three-line explanatory
comment was added inside that string. Shell doesn't strip comments inside a
quoted string, so every word of the prose became a metric family:

```
missing_families: ["#", "Buckets,", "for", "real", "percentiles.", ... 22 entries]
```

The two families that were genuinely absent, being the `vectorized_smp_*` set
which is `metric_disabled` in seastar, were buried in that noise. A warning
channel that reports 22 false entries alongside 8 real ones isn't a warning
channel.

This was the second bug of its kind in the same string. The first was an
apostrophe in `Seastar's`, which terminated the string outright.

The fix is a validator rather than care. A Prometheus metric name is
`[a-zA-Z_:][a-zA-Z0-9_:]*`, and anything else in the list is a typo or stray
prose rather than a family that happens to be missing. `validate_families` now
exits 2 and names the offenders, verified by reintroducing the exact bug into a
copy and confirming exit 2 plus the three offending tokens.

Generally: any list that gets `$(...)`-split or `for f in $LIST` needs a
syntactic validator, and the prose explaining it belongs above the assignment
where a `#` is actually a comment.

### 27. A negative cross-clock stage has two causes, and assuming the wrong one suppresses a good number

`judgeCrossClock` decides whether `match` and `total` are usable by comparing
the skew estimator's output against a step threshold. On one run it returned
usable, with a 189us estimate, 628us uncertainty and a 1000us threshold, while
`match` in the same report read:

```
match  p50 = -61.6us   min = -227.4us
```

The first conclusion drawn from this was wrong. It looked like proof of a 227us
clock offset the threshold test had missed, and a check was added that marked
the cross-clock stages unusable on any negative sample.

Then `chronyc` was actually consulted. All five hosts, three brokers and two
clients, were synced to the same AWS reference at `169.254.169.123` with RMS
offsets of 2.2 to 9.3us, and the run's own `clock-sync.json` recorded 6.7us. A
227us clock offset is impossible under those readings.

The real cause is architectural. `matched_at_nanos` is stamped inside the
matcher transform on the broker, immediately after the record is appended to
the leader's log. The ack timestamp is when the client received its `acks=all`
quorum acknowledgement, a further few hundred microseconds of replication plus
network away. The matcher therefore normally finishes before the producer is
told its write is durable, so `match = matchedT - ackT` is legitimately
negative.

That's the in-broker pre-commit read: a known and intended property of this
architecture, and part of where the latency win comes from. A durability trade
rather than a measurement error.

The consequences, all of which the wrong reading got backwards:

- `match` is a race between two paths rather than a duration. Its sign carries
  information. Never quote it as latency, and don't expect it to be positive.
- `total` is valid. With hosts inside 10us, `total = receiptT - sendT` is
  accurate to about that, and it's the e2e number. The premise that it carried
  a several-hundred-microsecond bar came from an assumption about sync quality
  that the chrony data refutes.
- `produce + relay_consume` isn't a lower bound on e2e, it's an overestimate,
  because it drops a term that's genuinely negative.
- The bracket skew estimator's 628us uncertainty is wider than the quantity,
  so its 189us point estimate is indistinguishable from zero. It's too weak to
  detect a 5us offset and mustn't be read as evidence of one.

The check that survives compares the negative magnitude against an
independently measured clock offset via `-clock-rms-micros`, fed from
`clock-sync.json`. If the magnitude dwarfs it, the clocks can't explain the
sign and the pre-commit read is established. If it's comparable, clock error is
sufficient and the stages are unquotable. If no offset was supplied, the report
says it can't tell rather than guessing. It reports and never suppresses
`total`, since the earlier version's false positive would have hidden a good
number, and this file already learned that a check whose default is "broken" is
worse than no check.

Generally: before attributing an impossible-looking reading to instrument
error, measure the instrument. "This value is impossible, therefore the clock
is wrong" and "this value is impossible, therefore my model of what the
timestamps mean is wrong" look identical in the data, and only an independent
measurement separates them.

### 28. `total = produce + match + relay_consume` is an identity, so their agreement proves nothing

Having found `match` corrupted, the tempting move is to report the sum of the
two trustworthy single-clock legs, `produce + relay_consume`, and note that it
agrees with the measured `total` to within about 60us, calling that a
cross-check.

It isn't one. loadgen builds these stages from four timestamps such that
`total` is identically `produce + match + relay_consume`, so `sum - total` is
always `-match`, by construction. The agreement is a restatement of the
corrupted number rather than independent evidence about it.

An earlier version of this entry claimed the single-clock sum was at least a
genuine lower bound on true e2e, since true elapsed `match >= 0`. That's
retracted, see #27. `match` isn't an elapsed time and is legitimately negative,
so dropping it makes the sum an overestimate rather than a lower bound. Quote
`total` itself, since the hosts are synced to under 10us.

A real cross-check has to come from a different measurement path. The
count-versus-buckets check on `emit_to_guest`, which came out exact at
40,000,000 both ways, is one. The sum-versus-buckets mean check on the same
histogram is another, and it earned its keep by revealing that samples cluster
toward the low end of each log-spaced bucket, which means interpolated
percentiles from those buckets are biased high.

### 29. Sending the right number of records says nothing about sending them at the right times

The traditional-deployment leg at 500 external consumers passed every gate the
harness had: `attempted_ratio` 99.99%, `achieved_ratio` 99.99%, `client_bound`
false, `lag.grew_during_run` false, 29,997 of 29,997 receipts, zero missing.

It was contaminated. `send_lateness`, meaning how late each order went out
against its own fixed-pacing schedule, read p90 534us and p99 2,486us against
a 1,000us interval, so 2.5 pacing slots late at the tail. The in-broker
reference run on the same cluster read p90 2.1us.

loadgen was the producer and the measured consumer, sharing one 36-vCPU
instance with 499 background consumer groups pulling the same topic. The CPU
starvation that delayed its sends equally delayed its receipt handling, so an
unknown share of that latency was client-side queueing rather than the system
under test.

Every existing check missed it because they all count records.
`attempted_ratio` is orders attempted over orders offered across the whole
window, so a starved client that drifts late but eventually emits the full
count scores about 100%. Lag is measured on the broker, which was fine.
Nothing looked at the distribution of send timestamps against the schedule,
even though loadgen was already recording exactly that and reporting it.

Fixed with a pacing-fidelity gate. Under `-pacing fixed`, if `send_lateness`
p99 exceeds one pacing interval, being `1e6/rate` us, the run isn't clean and
the reason names the slot count, the p90, and what to do about it. Tested
against both the real contaminated numbers and the healthy reference so
neither can regress silently. It doesn't apply to burst pacing, where there's
no per-order schedule to be late against.

`run-traditional-sharded.sh` exists because of this entry: it gives loadgen an
instance to itself and shards the background groups across the rest.

Generally: when a rig generates load, measure the rig's own fidelity and gate
on it. A saturated load generator produces numbers that look like results, and
every consistency check that counts totals rather than timings will agree with
them.

### 30. A quantile off a dashboard is not the quantile in the report

The monitoring stack (docs/MONITORING.md) graphs the same histograms loadgen
scrapes, so it is tempting to read a p99 off a panel. Don't quote it.

Grafana's `histogram_quantile(0.99, sum by (le) (rate(..._bucket[...])))` is a
quantile of a RATE over a sliding window. It interpolates within whichever
bucket the quantile falls in, and `$__rate_interval` widens that window to
several scrape intervals, which averages across the window rather than over
it. loadgen's p99 is the 99th percentile of the actual samples it collected
during the timed window, on its own clock. Those are different estimators of
different populations, and they disagree most exactly where it matters: in the
tail, and at the start and end of a short window where the rate window spans
time outside it.

The bucket boundaries make this concrete. Redpanda's public histograms are
log-scale, so a panel showing a p99 of "1.2ms" may be interpolating inside a
bucket several hundred microseconds wide. That is fine for watching a shape
and useless as a published figure.

So: the dashboards are for watching a run and for deciding whether a result is
admissible. Every number in REPRODUCE.md comes from the run's own JSON. The
panels are deliberately labelled with what they are for, and MONITORING.md
says the same thing in its first paragraphs, because the failure here is not a
wrong graph - the graph is right - it is quoting it as though it were the
measurement.

Corollary worth keeping in mind: the scrape is also load. It is aimed at
`/public_metrics`, which is curated and small, at 2s; the full internal
`/metrics` set goes at 15s precisely because it is the expensive one and
nothing on these dashboards needs it. If you want a run with no observer on
the network at all, `monitor_count = 0` removes it and changes nothing else.

### 31. A measurement that falls back to zero cannot be told apart from a real reading

The RTT baseline exists to remove network distance as a variable, so that a
produce leg that moved between runs can be checked against the network it was
measured on. Both recorders parsed `ping`'s summary line and substituted a
default when the parse found nothing: `${avg:-0}` in the ansible play,
`stats="0 0 0 0"` in `bench/lib-cluster.sh`.

The security group permits TCP only. ICMP was never allowed inside the VPC, so
every ping was 100% loss, no summary line was printed, and the whole feature
reported `0` on its first run. `broker 0 -> ['10.0.0.116 0 0', ...]` and
`client-to-broker RTT: worst avg 0us (the produce leg's network floor)` are
what a working cluster of adjacent machines would plausibly print. Nothing
looked wrong.

Zero is a legal value in the units being reported, which is what makes the
substitution dangerous. A missing value has to be recorded as missing:
`{"host": "...", "reachable": false}` rather than a number, and a summary line
that says NOT MEASURED rather than a figure. The ansible play now fails with
the likely cause named, because a baseline nobody can measure should stop the
deploy rather than quietly ship a zero into a results directory.

`terraform/main.tf` carries an icmp ingress rule for `10.0.0.0/16` for this
reason. A cluster provisioned before that rule existed needs a `terraform
apply` to add it; the SG updates in place, so no instance is replaced.

This is the same shape as #21 and #24, and worth stating as a rule: when a
parse or probe can come back empty, the empty case needs its own
representation. Choosing a neutral-looking default makes the failure
indistinguishable from success in exactly the output a reader trusts.

### 32. A verifier reports clean about resources it has never heard of

Adding the broker placement group added a resource type, and the capacity
probe creates a throwaway one per region it probes so it can ask about placed
capacity rather than unplaced. `wb verify-clean` checked instances, volumes,
Elastic IPs and VPCs, because those were the resource types that existed when
it was written. It printed VERIFIED CLEAN with six probe placement groups
still sitting in three regions.

They leaked because `cancel-capacity-reservation` returns before the
reservation has let go of the group, so the delete issued immediately after
the cancel fails with the group in use - and the cleanup swallowed that with
`|| true`. Deleting the same groups by hand an hour later succeeded first
time, which is how the timing was confirmed.

Placement groups cost nothing, so this leaked no money. It is in this list
because of the shape, not the impact: the probe groups carry no `owner` tag,
since there is nothing to tag before the cluster exists, so they are matched
by name prefix instead of by the tag every other check uses. A sweep written
around one identifying convention silently excludes whatever does not follow
it.

Two rules worth keeping. A cleanup that cannot fail out loud is not a
cleanup - retry it, then warn. And when you add a resource type, add it to
the verifier in the same commit, because a verifier is only evidence about the
resources it was taught.

### 33. A lower-bound gate cannot catch a number that is too high

Three identical trials on 2026-09-17 (run 1789658612). Trials 1 and 3 ran with
`relay_active_subscriptions_end` at exactly 1500 and
`relay_delivered_per_logical_push` at exactly 500. Trial 2 ran at 1562 and
562.1: it started with 62 relay subscriptions from trial 1 still attached, did
12.4% more delivery work (33,725,745 deliveries against 30,000,000), and its
transform lag peaked at 1166 against the 1000-record threshold, so it was
rejected. Its throughput and receipts were perfect - 30,000 of 30,000, zero
missing, zero dropped.

`wait_for_relay_subscriptions` was in the path and passed. It could not have
caught this. It takes the CONSUMER count (500) and compares it against a
subscription count that is consumers x partitions led (1500), so it is a loose
lower bound by construction, and it reported "1562 (>= 1500 wanted)" - which
reads exactly like success. `h >= w` is satisfied by a contaminated count as
comfortably as by a correct one.

The lesson generalises past this gate. Most readiness checks are written as
"has enough of X appeared yet", because the failure being guarded against is
too little. Contamination from a previous run produces too MUCH, and every
lower-bound check in the harness is blind to it by design. Where a count has a
known exact value, assert the value; where the concern is leftovers, wait for
the drain to zero before setting up, which is what
`wait_for_relay_subscriptions_drained` now does.

The rejection message is also worth noting on its own. "Rejected for transform
lag" was true and useless: the lag was a symptom of a fan-out the level never
asked for. The runner now reports measured fan-out against intended whenever
they differ by more than 2%, so a rejected trial can be attributed instead of
puzzled over. That is reported rather than folded into `clean`, because what
counts as publishable is a policy decision and not a script's to make.

## `write.caching`: read this before you set it

`write.caching=true` defers fsync, batching multiple produces into one flush
on a time or size window, via `flush.ms` and `flush.bytes` defaulting to 100ms
and 256KiB, instead of syncing every single one. That's real latency back, and
it means a hard crash between accepting a produce and the next flush can lose
that record even though the client already got an ack.

For a benchmark measuring best-case latency that's the right knob to reach
for. For a production exchange-matching deployment it's a durability decision
someone with the authority to make it needs to make deliberately, rather than
something to inherit from a benchmark script by accident. Every report
produced with it on should be labelled as such. The runners here set it
explicitly rather than relying on a cluster-wide default, and print what they
did.

## Running it

```bash
./scripts/wb doctor       # resolves bazel-bin, checks the binary, see #18
./scripts/wb up           # provisions, then brings up prometheus/grafana
./scripts/wb monitor      # the Grafana URL, for watching what follows
./scripts/wb deploy
./scripts/wb reproduce    # both arms, then render-tables
./scripts/wb teardown --yes && ./scripts/wb verify-clean
```

Results land under `results/<phase>-<run_id>/`, each report carrying its own
`clean`, `unclean_reasons`, `window_coverage` and config fingerprint, so a
result file is self-describing about whether it followed this checklist. Don't
hand-edit those fields.
