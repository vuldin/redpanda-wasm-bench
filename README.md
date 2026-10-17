# redpanda-wasm-bench

A benchmark harness that compares in-broker wasm transforms against a
traditional external Kafka deployment on AWS.

Both arms run the byte-identical wasm module from
[redpanda-wasm-clients](https://github.com/vuldin/redpanda-wasm-clients). The
only variable is where it executes: inside the broker as a data transform, or
on its own instance as an ordinary Kafka client. Keeping the module identical
makes the result a statement about execution location rather than about two
different implementations.

At 500 consumers and 1,000 orders/sec, in-broker end-to-end p50 was 751 us
against 13,414 us for the same work done externally, so roughly an order of
magnitude. Full tables and the commands that produced them are in
[REPRODUCE.md](REPRODUCE.md).


## Run the tests

Everything runs unattended: a run provisions its cluster (with Prometheus and
Grafana), runs a test plan, exports the metrics, writes a results summary,
tears the cluster down and verifies that through the EC2 API. A watchdog
tears down at the deadline even if the run itself dies. A run takes as long as
its plan takes; the one limit is your AWS credentials, which teardown needs.

### Once

1. Clone this repo, [the broker branch](https://github.com/vuldin/redpanda/tree/transform-latency-instrumentation)
   and [redpanda-wasm-clients](https://github.com/vuldin/redpanda-wasm-clients)
   side by side, install the tools, and build the broker and the guests:
   [docs/SETUP.md](docs/SETUP.md) (the broker build takes 1-2 hours cold).
2. `cp wb.local.env.example wb.local.env` and set your EC2 keypair (required).
   An AWS profile and checkout paths are optional; see the comments in the file.

### Every run

```bash
aws sso login                              # add --profile <your-profile> if you use one
scripts/wb doctor                          # tools, credentials, keys, builds
scripts/offline-gate.sh                    # after your last commit: every free check, incl. local broker tests
scripts/autorun.sh check plans/baseline.plan  # the phases and the time they need; spends nothing
scripts/autorun.sh start plans/baseline.plan  # provision, run, export, tear down, verify
scripts/autorun.sh status                  # progress, the Grafana URL, where results are
```

- `start` refuses before spending anything if the gate has not passed for
  your current commits, if the bench repo has uncommitted changes, or if your
  credentials will not last as long as the plan needs. After a login, check
  that their expiry actually moved: [Credentials](docs/RUNNING-YOUR-OWN-TESTS.md#run-modes-and-knobs).
- New or changed plan: run `bench/local-plan-smoke.sh plans/<plan>.plan` first.
  It runs the plan's e2 phases, scaled down, on a local broker.
- The first time, run `smoke.plan` instead: it proves your build, keys and
  account end to end for a fraction of the cost.
- `baseline.plan` is the default run, and the one to quote: in-broker WASM
  against the other architectures, with nothing else. RESULTS.md opens with
  its "Architecture comparison" table. Every other plan below investigates
  something or validates a change; run one when that is the question.

| plan | what it tests | phases | cost, roughly |
|---|---|---|---|
| `smoke.plan` | your build, keys and account work end to end; start here | ~22 min | $15-40 |
| **`baseline.plan`** | **the default: the same matcher, book and order flow in-broker (precommit and committed delivery) and as an external, co-located and isolated-core client, highest latency first, at fan-out 1 and 20, two interleaved trials** | ~1.5 h | $110 |
| `published.plan` | the published in-broker vs external comparison ([REPRODUCE.md](REPRODUCE.md)), then the resilience series | ~2.5 h | $130 |
| `resilience.plan` | latency and correctness while a broker goes into maintenance mode or leadership is transferred, graceful drain on vs off ([RESILIENCE.md](docs/RESILIENCE.md)) | ~1.5 h | $90 |
| `book-durability.plan` | a stateful matcher's book: depth up to 10,000 orders, and whether it survives maintenance, transfers and broker kills | ~3 h | $200 |
| `production-shape.plan` | in-broker vs external vs co-located (also on isolated cores), all with a 1,000-deep book and cancels; p99.99; rate with that book; kills with failover | ~1.8 h | $120 |
| `relay-modes.plan` | the latency cost of delivering to in-broker consumers only after the output is quorum-acknowledged (`RELAY_DELIVERY=committed`) instead of before (`precommit`, the default) | ~30 min | $70 |
| `state-and-jitter.plan` | durable state without the tail (full vs live snapshots, graceful moves), every deployment on one cluster with p99.99, and where the in-broker tail comes from | ~4.5 h | $250 |
| `checkpoint-ab.plan` | whether the broker's guest-state checkpoint still stalls produces once it runs in the background without a forced flush | ~1.4 h | $80 |
| `gateways.plan` | server-side filtering for FIX gateways: broker egress and gateway CPU per event with every gateway reading the whole events topic, against the in-broker gateway-demux routing by topic or by key | ~1h20 | $75 |
| `gateway-headroom.plan` | the topics demux at 8 gateways over 1 events partition against 4 (and 4 at twice the rate): whether spreading the ingest and the demux over several shards stops it falling behind | ~1h | $55 |
| `checkpoint-chunks.plan` | whether chunked checkpoints (or a longer interval) remove the depth-10,000 checkpoint stall; plus partition scale-out and the committed relay at fan-out 50 | ~3 h | $165 |

### Results

Each run writes to its own `results/run-<date>-<time>-<plan>/`:

- `RESULTS.md`: the tables, with unclean levels and trials to exclude flagged
- `STATUS.md`: what ran, outcomes, timings, and the teardown verdict
- `metrics-bundle/`: the Prometheus data for every measured window, viewable
  with no cluster (below)

Set `ARCHIVE_DIR` in `wb.local.env` to have every run copied somewhere you keep,
with a checksummed tarball. While a run is going, Grafana's URL is in
`autorun.log` and in `status` ([docs/MONITORING.md](docs/MONITORING.md)).

### Looking at a finished run in Grafana

Teardown destroys the run's Prometheus and Grafana, but the export kept every
measured window. Bring it up locally with Docker (Compose v2):

```bash
cd results/run-<date>-<time>-<plan>/metrics-bundle
docker compose up -d          # Prometheus on :9090, Grafana on :3000
# open http://localhost:3000  - no login; opens on the overview, set to the recording's time range
./validate.sh                 # optional: checks every panel returns data
docker compose down           # when you are done
```

Every graph is shaded by autorun phase (more layers are toggles at the top),
the strip at the top names the phase at the end of the selected range, and
latency shows only the measured samples by default (the "latency samples"
switch brings back each level's start-up spike). Only the measured windows hold data,
so the gaps between them are empty by design. One bundle at a time (they share
ports 3000 and 9090): `docker compose down` in the open one before `up` in
another. A bundle exported before 2026-10-16 has no phase shading, and one
exported before 2026-10-17 has older dashboards: stop it, run
`scripts/bundle-add-phases.sh results/run-...` (it replaces the rebuilt
series and dashboards, and refuses while the bundle is running), and start it
again. Bundles from before 2026-10-17 all share the compose project name
`metrics-bundle`; stop one of those with `docker compose -p metrics-bundle down`.
If every panel errors with `lookup prometheus ... server misbehaving`, the
Prometheus container is up but was left without its network (seen when it was
first created while another bundle held port 9090): `docker compose down &&
docker compose up -d` in the bundle recreates it. More, including archived tarballs:
[Looking at a finished run](docs/RUNNING-YOUR-OWN-TESTS.md#looking-at-a-finished-run).

### When something goes wrong

- A phase failed: autorun retries it once, then moves on. The phase's log is in
  the run directory, and a level can be unclean even when its phase passed, so
  read RESULTS.md's clean column.
- The teardown verdict in STATUS.md is not "verified-clean": run
  `scripts/wb verify-clean`, and see [Teardown](docs/MANUAL-RUNS.md#teardown).
- Everything else: [Gotchas that have cost real runs](docs/RUNNING-YOUR-OWN-TESTS.md#gotchas-that-have-cost-real-runs).

## More

- [docs/RUNNING-YOUR-OWN-TESTS.md](docs/RUNNING-YOUR-OWN-TESTS.md): writing
  plans, the runners' knobs, testing your own guest, what we know so far,
  gotchas
- [docs/SETUP.md](docs/SETUP.md): prerequisites, building, cost, AWS
  capacity, configuration
- [docs/MONITORING.md](docs/MONITORING.md): the dashboards, what each panel
  reads, and the exported bundle
- [REPRODUCE.md](REPRODUCE.md): the published results and how they were
  produced
- [docs/RESILIENCE.md](docs/RESILIENCE.md): latency during leadership movement
- [docs/MANUAL-RUNS.md](docs/MANUAL-RUNS.md): driving `wb` by hand, and
  teardown when you do
- [docs/TESTS.md](docs/TESTS.md) and [docs/METHODOLOGY.md](docs/METHODOLOGY.md):
  what each sweep varies, and the ways this harness has produced confidently
  wrong numbers. Read METHODOLOGY before quoting anything.

## Repo layout

```
plans/      test plans for scripts/autorun.sh (smoke, published, resilience, book-durability, production-shape, relay-modes, state-and-jitter, checkpoint-ab, checkpoint-chunks, gateways)
scripts/    wb (entry point), autorun.sh (unattended plan runs + watchdog),
            offline-gate.sh (every free check; autorun requires it),
            summarize-run.py (a run directory -> RESULTS.md),
            deploy-transform.sh (admin-API deploy with state options and the
            trusted-module grant), select-region.sh, size-clients.sh,
            lib-capacity.sh, switch-topology.sh, deploy-wasm-cluster.sh,
            verify-each-commit.sh (verifies the redpanda branch commit by commit),
            make-dashboards.py and check-dashboards.py (the Grafana panels and
            a validator for their queries), check-counter-resets.py (finds
            "counters" that rate() misreads), check-scrapes.py (every failed
            broker scrape, timeout or fast failure), annotate-phases.py (phase
            regions for runs older than the marker), check-prometheus-template.py,
            cost-guard.sh (cron-friendly reminder that a cluster is billing),
            export-metrics.sh + metrics-windows.py + metrics-bundle.py
            (the metrics bundle that outlives the cluster), bench-series.py
            and bundle-add-phases.sh (phases and windows as series in it)
terraform/  topologies as tfvars. region.auto.tfvars and clients.auto.tfvars
            are generated by the scripts above
ansible/    broker install and client tool deployment, plus monitoring.yaml
            (prometheus + grafana, node_exporter everywhere) and the
            dashboards it ships
bench/      the measurement scripts both arms run, plus run-resilience.sh
            (the optional maintenance/leadership-move series),
            run-durability.sh (does a stateful guest's book survive moves and
            kills), local-rehearsal.sh (the same on one local broker) and
            local-relay-delivery.sh (RELAY_DELIVERY on one local broker) and
            local-plan-smoke.sh (a plan's e2 phases, scaled down, locally)
cmd/        loadgen (rate-paced generator, source of every published number),
            fanout-load (background consumer groups), statecheck (sentinel
            orders that read a matcher's book back after an event)
wire/       Go fill decoder plus a conformance test against a Rust-generated fixture
docs/       RUNNING-YOUR-OWN-TESTS.md, SETUP.md, MANUAL-RUNS.md, METHODOLOGY.md,
            RELAY-LATENCY-STAGES.md, TESTS.md,
            RESILIENCE.md (latency during leadership movement),
            MONITORING.md (the dashboards and what they read)
```

Read [docs/METHODOLOGY.md](docs/METHODOLOGY.md) before trusting any number you
produce with this. It's a list of specific ways this harness has produced
confidently wrong results, each one found by getting a wrong number and
chasing it.
