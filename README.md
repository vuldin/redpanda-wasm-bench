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
tears the cluster down and verifies that through the EC2 API. A separate
watchdog tears down at a hard deadline even if the run itself dies.

First time only: install the tools and build the broker branch and the guests,
as in [docs/SETUP.md](docs/SETUP.md) (the broker build takes 1-2 hours cold).
Then:

```bash
cp wb.local.env.example wb.local.env    # once: AWS profile, ssh key, checkout paths
aws sso login --profile <your-profile>
scripts/offline-gate.sh                 # every free check: tools, builds, tests, a local broker crash test
scripts/autorun.sh start plans/smoke.plan
scripts/autorun.sh status               # progress, the Grafana URL, where results are
```

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

`scripts/autorun.sh check <plan>` lists a plan's phases and whether they fit
the time cap, without spending anything. Each run writes to its own
`results/run-<date>-<time>-<plan>/`:

- `RESULTS.md`: the tables, with trials to exclude flagged
- `STATUS.md`: what ran, outcomes, timings, the teardown verdict
- `metrics-bundle/`: the Prometheus data for every measured window, viewable
  with no cluster (`docker compose up` inside it)

While a run is going, Grafana's URL is in `autorun.log` and in `status`.

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
plans/      test plans for scripts/autorun.sh (smoke, published, resilience, book-durability, production-shape, relay-modes, state-and-jitter, checkpoint-ab)
scripts/    wb (entry point), autorun.sh (unattended plan runs + watchdog),
            offline-gate.sh (every free check; autorun requires it),
            summarize-run.py (a run directory -> RESULTS.md),
            deploy-transform.sh (admin-API deploy with state options and the
            trusted-module grant), select-region.sh, size-clients.sh,
            lib-capacity.sh, switch-topology.sh, deploy-wasm-cluster.sh,
            verify-each-commit.sh (verifies the redpanda branch commit by commit),
            make-dashboards.py and check-dashboards.py (the Grafana panels and
            a validator for their queries), check-prometheus-template.py,
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
