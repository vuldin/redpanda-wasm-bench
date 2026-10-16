# Running by hand

The unattended path (`scripts/autorun.sh` with a plan) is what most people want; see
[RUNNING-YOUR-OWN-TESTS.md](RUNNING-YOUR-OWN-TESTS.md). This is the manual path - driving
`wb` yourself - and the teardown it leaves to you.

## Reproducing the published results by hand

```bash
# everything: prerequisites, client-fleet sizing, region selection,
# provisioning, monitoring, deploy, both arms, tables
REDPANDA_BIN=/path/to/bazel-bin/src/v/redpanda/redpanda \
WASM_DIR=/path/to/redpanda-wasm-clients/bin \
  ./scripts/wb reproduce

# the Grafana URL, once anything is provisioned
./scripts/wb monitor

# tear down, and verify it
./scripts/wb teardown --yes && ./scripts/wb verify-clean
```

`wb reproduce` runs `size-clients.sh`, `select-region.sh`, `wb up` and
`wb deploy` itself, so you don't run those separately. Doing both is worse
than redundant: `select-region.sh` rewrites
`terraform/region.auto.tfvars`, so if capacity moved between your
`wb up` and `reproduce`'s own region selection, the second apply targets a
different region than the one holding your instances.

To drive the steps yourself, or to re-run one arm against a cluster that is
already up:

```bash
EXTERNAL_LADDER="1 10 50 500" ./scripts/size-clients.sh
REGIONS="us-east-1 us-east-2 us-west-2" ./scripts/select-region.sh
./scripts/wb up          # capacity probe, provision, then the monitoring stack
./scripts/wb deploy      # broker binary + client tools
SKIP_PROVISION=true ./scripts/wb reproduce
```

## Changing a cluster property

Use `scripts/wb set <property> <value>`, not `rpk cluster config set`. For a
`needs_restart` property it rolls the brokers and confirms the value is live;
rpk only updates the desired config, and `rpk cluster config get` then echoes
a value that is not in effect (see METHODOLOGY #13). Runners that change
properties for one sweep (`TRANSFORM_SHARES`, `CHECKPOINT_INLINE`,
`RELAY_STAGE_METRICS`) verify the read-back and restore the old value.

## Teardown

```bash
./scripts/wb teardown       # terraform destroy
./scripts/wb verify-clean   # EC2 API check, don't skip it
```

### Don't rely on remembering

Teardown is manual because the results live on the cluster and an automatic
destroy would throw them away. That makes forgetting the failure mode, and
clusters in the development account have been left running for days on a
"tear it down when I'm done" intention.

`scripts/cost-guard.sh` is the nag. It takes no arguments, holds no state,
prints nothing while the account is clean, and exits non-zero while anything
is billing, so cron mails you only when there's something to act on:

```bash
# every 30 minutes, while a bench cluster is up
*/30 * * * * /path/to/redpanda-wasm-bench/scripts/cost-guard.sh --quiet
```

It reports each instance, how long it's been up and roughly what it has cost,
across every region `select-region.sh` can land in. It never destroys
anything: deciding to tear down mid-run isn't a decision a cron job should
make, which is the whole reason teardown is manual.

### Verification

`verify-clean` queries the EC2 API rather than trusting terraform state,
across instances in any non-terminated state, `available` volumes, EIPs with a
null association, and VPCs tagged for this benchmark. That's not paranoia: the
development account both removed infrastructure on its own and kept billing
after an apparently-successful destroy.

If you're running in an account with automated cleanup, note that
`cloud-nuke`-style tooling tags resources (`cloud-nuke-first-seen`, for
example) and deletes them on its own schedule. A `terraform apply` will show
those tags as drift it wants to remove, which is expected and harmless, but it
means a long-running cluster is on a clock you don't control.
