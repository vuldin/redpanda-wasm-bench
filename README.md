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
scripts/autorun.sh check plans/smoke.plan  # the phases and the time they need; spends nothing
scripts/autorun.sh start plans/smoke.plan  # provision, run, export, tear down, verify
scripts/autorun.sh status                  # progress, the Grafana URL, where results are
```

- `start` refuses before spending anything if the gate has not passed for
  your current commits, if the bench repo has uncommitted changes, or if your
  credentials will not last as long as the plan needs. After a login, check
  that their expiry actually moved: [Credentials](docs/RUNNING-YOUR-OWN-TESTS.md#run-modes-and-knobs).
- New or changed plan: run `bench/local-plan-smoke.sh plans/<plan>.plan` first.
  It runs the plan's e2 phases, scaled down, on a local broker.
- Start with `smoke.plan`, which proves your build, keys and account end to end.

| plan | what it tests | phases | cost, roughly |
|---|---|---|---|
| `smoke.plan` | your build, keys and account work end to end; start here | ~22 min | $15-40 |
| `published.plan` | the published in-broker vs external comparison ([REPRODUCE.md](REPRODUCE.md)), then the resilience series | ~2.5 h | $130 |
| `resilience.plan` | latency and correctness while a broker goes into maintenance mode or leadership is transferred, graceful drain on vs off ([RESILIENCE.md](docs/RESILIENCE.md)) | ~1.5 h | $90 |
| `book-durability.plan` | a stateful matcher's book: depth up to 10,000 orders, and whether it survives maintenance, transfers and broker kills | ~3 h | $200 |
| `production-shape.plan` | in-broker vs external vs co-located (also on isolated cores), all with a 1,000-deep book and cancels; p99.99; rate with that book; kills with failover | ~1.8 h | $120 |
| `relay-modes.plan` | the latency cost of delivering to in-broker consumers only after the output is quorum-acknowledged (`RELAY_DELIVERY=committed`) instead of before (`precommit`, the default) | ~30 min | $70 |
| `state-and-jitter.plan` | durable state without the tail (full vs live snapshots, graceful moves), every deployment on one cluster with p99.99, and where the in-broker tail comes from | ~4.5 h | $250 |
| `checkpoint-ab.plan` | whether the broker's guest-state checkpoint still stalls produces once it runs in the background without a forced flush | ~1.4 h | $80 |
| `checkpoint-chunks.plan` | whether chunked checkpoints (or a longer interval) remove the depth-10,000 checkpoint stall; plus partition scale-out and the committed relay at fan-out 50 | ~3 h | $165 |

### Results

Each run writes to its own `results/run-<date>-<time>-<plan>/`:

- `RESULTS.md`: the tables, with unclean levels and trials to exclude flagged
- `STATUS.md`: what ran, outcomes, timings, and the teardown verdict
- `metrics-bundle/`: the Prometheus data for every measured window, viewable
  with no cluster (`docker compose up` inside it)

Set `ARCHIVE_DIR` in `wb.local.env` to have every run copied somewhere you keep,
with a checksummed tarball. While a run is going, Grafana's URL is in
`autorun.log` and in `status` ([docs/MONITORING.md](docs/MONITORING.md)).

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
plans/      test plans for scripts/autorun.sh (smoke, published, resilience, book-durability, production-shape, relay-modes, state-and-jitter, checkpoint-ab, checkpoint-chunks)
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
            (the metrics bundle that outlives the cluster)
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
