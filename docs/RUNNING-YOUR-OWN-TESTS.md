# Running your own tests on the branch

This is the path for testing the `transform-latency-instrumentation` broker
branch ([vuldin/redpanda](https://github.com/vuldin/redpanda)) with your own
changes, guests or test plans, unattended, without leaving a cluster billing.

The short version:

```bash
cp wb.local.env.example wb.local.env    # your AWS profile, key, checkouts
aws sso login --profile <yours>
scripts/offline-gate.sh                 # every free check, incl. a local broker SIGKILL test
scripts/autorun.sh check plans/smoke.plan
scripts/autorun.sh start plans/smoke.plan
scripts/autorun.sh status               # or: tail -f results/run-*/autorun.log
```

`start` returns immediately. The run provisions a cluster, runs the plan,
fetches results, tears down and verifies the teardown through the EC2 API on
its own, with an independent watchdog that tears down at the deadline even if
the run process has died.

## What is on the branch

Beyond upstream `dev`, the branch carries (each commit builds standalone; see
`scripts/verify-each-commit.sh`):

- the in-broker **relay**: transform output pushed straight to in-broker
  consumers, no Kafka fetch, plus per-stage latency histograms
  (`relay_stage_metrics_enabled`) - see [RELAY-LATENCY-STAGES.md](RELAY-LATENCY-STAGES.md)
- **trusted modules** (`wasm_trusted_modules`, keyed by the binary's sha256)
  granting capabilities: `shared_memory`, `network`, `relay_consumer`
- **guest-state persistence**: a trusted guest registers a shared-memory
  region; the broker checkpoints it every 5s into the input partition's raft
  group and writes it back before the first record after a restart.
  `require_state_recovery` (admin-API deploy only) makes a restore failure
  fatal instead of silently starting empty
- **graceful leadership transfer** (`data_transforms_graceful_transfer_timeout_ms`):
  drain, commit and checkpoint together before a leadership move. **Off by
  default** - see "What we know" below
- per-phase startup histograms (`transform_startup_latency_seconds` and friends)

## One-time setup

1. **Three checkouts.** This repo; [redpanda-wasm-clients](https://github.com/vuldin/redpanda-wasm-clients)
   (the guests); the broker branch. Build the broker with the capped bazel
   command in the [README](../README.md#a-redpanda-binary-built-from-the-branch)
   (`-c opt --jobs=6 --local_resources=memory=16384`; uncapped builds have
   exhausted workstation memory). Build the guests with `make build-wasm`.
2. **`wb.local.env`.** Copy `wb.local.env.example` and set your AWS profile,
   EC2 keypair and checkout paths. It is gitignored; `wb`, `autorun.sh` and
   `offline-gate.sh` all read it, and anything in your environment wins.
3. **`scripts/wb doctor`** checks tools, the AWS session, keys, the local
   broker build and the guest artifacts.

## The offline gate

`scripts/offline-gate.sh` runs everything that costs nothing: the guests' Rust
tests (including a 200k-operation differential test of the order book), the
wasm build with the published-module checksums, `go vet`/`go test`, the bench
shell suites, a syntax check of every script and plan, and
`bench/local-rehearsal.sh` - one broker from your build, a snapshotting guest,
traffic, sentinel orders, SIGKILL, restart, and a check that the book came
back. It records the commits of all three repos in `results/GATE-PASSED`.

`autorun.sh start` refuses to provision unless the gate passed for exactly the
commits you are running, and refuses uncommitted bench changes. Commit first,
then gate, then start. `REHEARSAL=0` skips the local broker test when the
broker has not changed; don't skip it after broker changes - it is the check
that found snapshot persistence silently not working in a real broker.

## Plans

A plan is a small bash file in `plans/`:

```bash
TOPOLOGY=six-broker      # or single-az, multi-az, highcore (terraform/*.tfvars)
DEPLOY_ARGS=(-e transform_memory_limit=16777216 -e transform_snapshot_max_size=16777216)

phase NAME EST_MINUTES <wb arguments...>
phase depth 10 run e2 MODE=bookdepth GUEST=book-matcher 'BOOKDEPTH_LEVELS=0 1000'
phase kill  35 run durability ACTIONS=kill 'DEPTHS=1000 10000' OUT=@RUN_DIR@/kill
```

- Each phase is one `wb` invocation. Quote a `KEY=VALUE` whose value has
  spaces. `@RUN_DIR@` becomes the run's directory.
- Estimates matter: a phase starts only if it fits before the teardown
  reserve, and is killed at 1.5x its estimate. `autorun.sh check PLAN` prints
  the phases and whether the total fits the cap.
- Between phases the cluster is reset (transforms and test topics deleted,
  maintenance cleared, balancers and graceful-transfer back to defaults, every
  broker started). A failed phase is retried once.

Shipped plans:

| plan | what | phases | cluster |
|---|---|---|---|
| `smoke.plan` | proves your build, keys and account end to end | ~22 min | 3 brokers |
| `book-durability.plan` | the 2026-10-15 book-depth + durability suite | ~171 min | 6 brokers |

### Run modes and knobs

| | |
|---|---|
| `autorun.sh start PLAN` | provision, run, tear down |
| `autorun.sh start --existing PLAN` | run on a cluster you brought up with `wb up` / `wb deploy` (keeps an armed watchdog for that RUN_DIR) |
| `CAP_HOURS` (5) | wall-clock cap; the deadline is also pulled in to your credentials' expiry minus the reserve |
| `RESERVE_MIN` (25) | time held back for fetch, archive, teardown, verify |
| `KEEP_CLUSTER=1` | skip the end-of-run teardown to keep iterating; the watchdog still tears down at the deadline |
| `RUN_DIR`, `ARCHIVE_DIR` | where results go; ARCHIVE_DIR also gets a sha256-checked tarball |

Credentials: the run exports your role credentials once at start, so it
survives the SSO token expiring; the role credentials' own expiry (often 8-12h
after login) is the real limit, and `start` refuses if they will not outlive
the deadline. Log in fresh before a long plan.

## The runners a phase can call

- **`run e2`** (on client0) - steady-state latency and cost sweeps; modes and
  their contracts are in [TESTS.md](TESTS.md). For stateful guests:
  `GUEST=book-matcher`, `STATE=off|snapshot`, `SNAPSHOT_EVERY_MS`,
  `CANCEL_FRACTION` (share of input records that are cancels, with as many
  passive inserts), `MODE=bookdepth` with `BOOKDEPTH_LEVELS`.
- **`run durability`** (from your machine, because a kill needs ssh to the
  leader) - per trial: fresh topics, deploy book-matcher, traffic over a
  preloaded book, sentinel orders in waves at T-10/-6/-3/-1/-0.3s, the action
  at T, recovery, then a read-back of the book. Knobs: `ACTIONS`
  (`maintenance transfer kill`), `DEPTHS`, `TRIALS`, `STATE`, `GRACEFUL_MS`,
  `REPLAY=1` (rebuild the same book by replaying the topic from the start and
  time it), `PER_WAVE`, `WAVES`, `KILL_DOWN_S`, `RATE`, `ORDERS_PER_RECORD`,
  `CANCEL_FRACTION`, `OUT`.
- **`run resilience`** - latency during leadership movement; see
  [RESILIENCE.md](RESILIENCE.md).

Every durability trial line says whether traffic spanned T and whether
leadership actually moved. A trial that says `LEADERSHIP-DID-NOT-MOVE` tested
nothing; exclude it.

## Testing your own guest

- Build it in redpanda-wasm-clients; add it to `make build-wasm`,
  `scripts/deploy-wasm-cluster.sh`'s staging list and the ship list in
  `ansible/deploy-wasm.yaml` (they must agree, or the deploy fails mid-way).
- A guest importing `redpanda_wasm_shared_memory` does not instantiate unless
  its sha256 is trusted with `shared_memory`. Deploy it with
  `scripts/deploy-transform.sh`, which trusts it (read-merge of
  `wasm_trusted_modules`), retries across brokers, and can set
  `--require-state-recovery` and `--from-start`. Stock `rpk transform deploy`
  can do neither.
- Register the region in `main`, before the first record; the broker restores
  into it before delivering one. Write the region only at the end of an
  invocation (the broker reads it between invocations), and carry a checksum:
  the region is opaque bytes to the broker.
- Guests cannot see input offsets, so replay detection after a restore has to
  come from something in the records. `rust/src/bin/book-matcher.rs` shows one
  way (per-producer sequence lanes) and logs `STATE restored|gap|stats` lines
  to the transform log topic, which the durability runner collects.

## What we know (2026-10-15)

From the 2026-10-15 runs (results are not checked in; the internal write-up
has the full tables):

- The depth-tolerant book holds 10,000 resting orders per side at the same
  latency as an empty book (~500us p50, ~1ms p99.9 at 30k orders/sec); the
  reference sorted-Vec book saturates by 1,000.
- With the broker default (graceful transfer off), planned moves lose state
  like crashes: the last 1-3s of acknowledged orders in 7 of 8 trials. With
  `data_transforms_graceful_transfer_timeout_ms=5000`, no acknowledged order
  was lost in 13 real moves; a residual 0.07-0.5s gap remains because the
  guest re-serialises its region every 500ms and nothing tells it the broker is
  about to checkpoint.
- A broker kill loses the last ~3.5-4s (the snapshot and the committed offset
  are written independently); failover is 6.5-10s.
- Snapshot restore is 1-14ms; replaying ~800k records to rebuild the same
  book is ~10s.
- Whole-region snapshots every 500ms push p99.9 to 10-18ms at depth 1k-10k.

## Gotchas that have cost real runs

- `wb run e2` exits 0 even when the remote runner failed. Check the phase
  log and the run's `summary.txt`, not the phase outcome alone.
- Admin API calls that act on a partition must reach its leader:
  `transfer_leadership` sent elsewhere without following redirects silently
  does nothing. Deploys can return 503 `Leader not available` from a
  non-leader broker; retry elsewhere.
- Backgrounding over ssh: `ssh host "cd x && nohup cmd &"` keeps the ssh
  channel open until `cmd` exits (the backgrounded list still holds stdout).
  Redirect the shell's fds first, as `run-durability.sh`'s `CBG` does.
- Never edit a script in place while a run is executing it - bash reads
  scripts incrementally. Write a copy and `mv` it over.
- Read [METHODOLOGY.md](METHODOLOGY.md) before quoting any number.
