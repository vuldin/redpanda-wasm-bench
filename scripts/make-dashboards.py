#!/usr/bin/env python3
"""Generate the Grafana dashboards ansible ships to the monitoring host.

The JSON this writes is committed, so the monitoring host needs no python and
the deploy has nothing to generate. This file is the source of truth for it:
edit here, re-run, commit both.

Why generate rather than hand-maintain the JSON: these dashboards are mostly
the same panel shape repeated over a list of metrics, and a Grafana export is
several hundred lines per dashboard with the interesting part buried. Getting a
`le` label wrong in one of forty PromQL strings produces an empty panel and no
error, so the queries are written once here and the repetition is the
computer's problem.

EVERY METRIC NAME BELOW WAS READ OUT OF THE REDPANDA SOURCE, not guessed:

  redpanda_relay_*            src/v/relay/probe.cc
  redpanda_transform_*        src/v/transform/probe.cc
  redpanda_transform_execution_latency_sec
                              src/v/wasm/transform_probe.cc
  redpanda_wasm_engine_*      src/v/wasm/engine_probe.cc
  redpanda_wasm_binary_*      src/v/wasm/cache.cc
  redpanda_cpu_busy_seconds_total
                              src/v/redpanda/application.cc

All of them are registered on the PUBLIC metric registry, so all of them arrive
on /public_metrics as redpanda_*. That is the cheap endpoint, which is what
makes a 2s scrape defensible while a run is in flight.

Usage: scripts/make-dashboards.py [outdir]
"""
import json
import sys
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}

# Stage histograms, in the order a record actually traverses them. The order is
# the point: read top to bottom and you are following one record from the
# producing transform's emit to the consuming guest's dequeue.
RELAY_STAGES = [
    ("crossshard_dispatch_duration_seconds",
     "1. crossshard dispatch (producer shard: one payload copy + every submission)"),
    ("crossshard_transit_duration_seconds",
     "2. crossshard transit (seastar's cross-shard path, charged on arrival)"),
    ("fanout_duration_seconds",
     "3. fanout loop (per-subscriber dispatch on one shard; scales with subscribers)"),
    ("consume_delay_seconds",
     "4. consume delay (queued, waiting for the consumer's processor to be scheduled)"),
]

QUANTILES = [(0.5, "p50"), (0.95, "p95"), (0.99, "p99")]


def hist_quantile(metric, q, extra=""):
    """A quantile over a seastar public histogram.

    `sum by (le)` and nothing else: these histograms are declared
    .aggregate({shard_label}), so the shard dimension is already gone, but the
    series still arrive per broker. Summing over `le` alone therefore gives a
    cluster-wide quantile, which is what every published figure is.
    """
    sel = f'{{{extra}}}' if extra else ""
    return (f"histogram_quantile({q}, sum by (le) "
            f"(rate({metric}_bucket{sel}[$__rate_interval])))")


def panel(pid, title, x, y, w, h, targets, unit=None, desc=None,
          kind="timeseries", opts=None, thresholds=None, legend_table=False):
    custom = {
        "drawStyle": "line",
        "lineWidth": 2,
        "fillOpacity": 5,
        "showPoints": "never",
        "spanNulls": True,
    }
    fc = {"defaults": {"custom": custom, "color": {"mode": "palette-classic"}},
          "overrides": []}
    if unit:
        fc["defaults"]["unit"] = unit
    if thresholds:
        fc["defaults"]["thresholds"] = thresholds
        fc["defaults"]["color"] = {"mode": "thresholds"}
    p = {
        "id": pid,
        "type": kind,
        "title": title,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": fc,
        "targets": [
            {"datasource": DS, "expr": e, "legendFormat": l,
             "refId": chr(65 + i), "editorMode": "code", "range": True}
            for i, (e, l) in enumerate(targets)
        ],
    }
    if desc:
        p["description"] = desc
    if kind == "timeseries":
        p["options"] = {
            "legend": {
                "displayMode": "table" if legend_table else "list",
                "placement": "bottom",
                "showLegend": True,
                "calcs": ["mean", "max"] if legend_table else [],
            },
            "tooltip": {"mode": "multi", "sort": "desc"},
        }
    if opts:
        p.setdefault("options", {}).update(opts)
    return p


def stat(pid, title, x, y, w, h, targets, unit=None, desc=None, thresholds=None,
         text_size=42):
    p = panel(pid, title, x, y, w, h, targets, unit=unit, desc=desc,
              kind="stat", thresholds=thresholds)
    p["options"] = {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "textMode": "auto",
        "colorMode": "value",
        "graphMode": "area",
        "justifyMode": "auto",
        "text": {"titleSize": 14, "valueSize": text_size},
    }
    return p


def row(pid, title, y, collapsed=False):
    return {"id": pid, "type": "row", "title": title, "collapsed": collapsed,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def dashboard(uid, title, panels, desc, tags, refresh="5s", time_from="now-15m"):
    return {
        "uid": uid,
        "title": title,
        "description": desc,
        "tags": tags,
        "timezone": "utc",
        "editable": True,
        "schemaVersion": 39,
        "refresh": refresh,
        "time": {"from": time_from, "to": "now"},
        # 2s matches the public-endpoint scrape interval. Without this Grafana
        # picks a step from the panel width, which on a wide screen over a 30s
        # window lands below the scrape interval and draws gaps.
        "timepicker": {"refresh_intervals": ["2s", "5s", "10s", "30s", "1m", "5m"]},
        "panels": panels,
        "annotations": {"list": []},
        "templating": {"list": []},
    }


# ===========================================================================
# 1. Overview - the dashboard the Grafana URL opens on.
#
# Ordered as the questions actually get asked: is the in-broker path fast, is
# it delivering everything, is the transform healthy, and where is the CPU.
# The correctness gates are stat panels with thresholds rather than graphs,
# because their only interesting property is whether they are zero.
# ===========================================================================
def overview():
    p, y = [], 0

    p.append(row(1, "Headline: in-broker fan-out latency", y)); y += 1
    p.append(panel(
        10, "emit -> guest, the whole in-broker path", 0, y, 12, 8,
        [(hist_quantile("redpanda_relay_emit_to_guest_duration_seconds", q), lab)
         for q, lab in QUANTILES],
        unit="s", legend_table=True,
        desc=("The single span to quote for the in-broker arm: the producing "
              "transform's emit through to the consuming guest dequeuing the "
              "record. Prefer this over adding the four stages up - they "
              "overlap (transit starts inside dispatch's window) and leave "
              "gaps between each other, so the sum is neither an upper nor a "
              "lower bound. Empty means relay_stage_metrics_enabled is off, "
              "which is its default."),
    ))
    p.append(panel(
        11, "transform e2e: input append -> output written and committed", 12, y, 12, 8,
        [(hist_quantile("redpanda_transform_e2e_latency_seconds", q), lab)
         for q, lab in QUANTILES],
        unit="s", legend_table=True,
        desc=("The durable path, for contrast with the relay panel beside it. "
              "This includes input delay, queueing and the write path, so it "
              "is the honest number for a consumer that reads the output "
              "topic rather than subscribing to the relay."),
    ))
    y += 8

    p.append(row(2, "Is the relay delivering everything it was handed?", y)); y += 1
    p.append(stat(
        20, "relay drops/sec", 0, y, 6, 5,
        [("sum(rate(redpanda_relay_dropped_total[$__rate_interval]))", "dropped/s")],
        desc=("MUST be zero. The relay is best-effort: a consumer that falls "
              "behind has its oldest queued record dropped rather than the "
              "producer being slowed down. Any non-zero value here means the "
              "latency on this dashboard is the latency of an incomplete "
              "stream, and the run is not comparable to one without drops."),
        thresholds={"mode": "absolute", "steps": [
            {"color": "green", "value": None}, {"color": "red", "value": 0.001}]},
    ))
    p.append(stat(
        21, "active subscriptions", 6, y, 6, 5,
        [("sum(redpanda_relay_active_subscriptions)", "subscriptions")],
        desc=("Subscriptions, NOT consumers. Each consumer transform gets a "
              "processor per input partition it leads, so the cluster-wide sum "
              "is (consumers x partitions led) - measured at 1,500 for 500 "
              "consumers over 3 partitions, which is correct and not a 3x "
              "overshoot. What matters is that it is STABLE during a window "
              "and that it did not collapse partway through; a figure that "
              "moves mid-run means processors were restarting, and the "
              "latency spans a changing fan-out."),
    ))
    p.append(panel(
        22, "relay throughput", 12, y, 12, 5,
        [("sum(rate(redpanda_relay_pushes_total[$__rate_interval]))", "pushes/s"),
         ("sum(rate(redpanda_relay_delivered_total[$__rate_interval]))", "delivered/s"),
         ("sum(rate(redpanda_relay_dropped_total[$__rate_interval]))", "dropped/s")],
        unit="reqps", legend_table=True,
        desc=("delivered/s divided by pushes/s is NOT the fan-out factor, and "
              "reading it that way is a mistake this panel used to invite. "
              "pushes_total is incremented inside deliver_locally(), which "
              "push() calls once for the producer's own shard plus once per "
              "other shard holding a subscriber - so it counts shard-delivery "
              "passes and grows with how widely subscribers are spread, not "
              "with the offered rate. Measured at 500 consumers over 13 "
              "shards: 780,000 pushes for 30,000,000 deliveries, a ratio of "
              "38.5, while the actual fan-out was exactly 500. The run's own "
              "report computes both and names them apart "
              "(relay_delivered_per_shard_pass vs "
              "relay_delivered_per_logical_push); the logical figure needs the "
              "order count, which Prometheus does not have. What to read here "
              "is that delivered tracks pushes smoothly and dropped stays "
              "flat at zero."),
    ))
    y += 5

    p.append(row(3, "Is the transform healthy? (these gate the result)", y)); y += 1
    p.append(stat(
        30, "transform failures/sec", 0, y, 6, 5,
        [("sum(rate(redpanda_transform_failures[$__rate_interval]))", "failures/s")],
        desc="MUST be zero. A failing transform retries, which shows up as latency.",
        thresholds={"mode": "absolute", "steps": [
            {"color": "green", "value": None}, {"color": "red", "value": 0.001}]},
    ))
    p.append(stat(
        31, "batches given up", 6, y, 6, 5,
        [("sum(redpanda_transform_batches_given_up)", "given up")],
        desc=("MUST be zero. A given-up batch was skipped or dead-lettered "
              "after exhausting its retries, so output is missing records - "
              "that is data loss, not a slowdown."),
        thresholds={"mode": "absolute", "steps": [
            {"color": "green", "value": None}, {"color": "red", "value": 1}]},
    ))
    p.append(panel(
        32, "transform lag", 12, y, 12, 5,
        [("sum by (function_name) (redpanda_transform_lag)", "{{function_name}}")],
        unit="short", legend_table=True,
        desc=("Records on the input topic not yet processed. A flat non-zero "
              "value is a steady backlog; a rising line means the transform "
              "is not keeping up with the offered rate and the run is "
              "measuring the backlog rather than the architecture."),
    ))
    y += 5

    p.append(row(4, "Where the broker CPU is going", y)); y += 1
    p.append(panel(
        40, "guest CPU by function", 0, y, 8, 8,
        [("sum by (function_name) (rate(redpanda_wasm_engine_cpu_seconds_total[$__rate_interval]))",
          "{{function_name}}")],
        unit="percentunit", legend_table=True,
        desc=("CPU-seconds per second inside each wasm guest, so 1.0 is one "
              "fully saturated core. Divide by the guest invocation rate "
              "(redpanda_transform_execution_latency_sec_count) for per-record "
              "cost."),
    ))
    p.append(panel(
        41, "reactor utilisation by shard, per broker", 8, y, 8, 8,
        [("rate(redpanda_cpu_busy_seconds_total[$__rate_interval])",
          "{{host}} shard {{shard}}")],
        unit="percentunit", legend_table=True,
        desc=("The one panel here that is deliberately NOT aggregated over "
              "shards. Relay and transform work lands on specific shards, and "
              "a single saturated shard against 12 idle ones is the shape of "
              "this workload's ceiling. num_cores is 13 on r8id.8xlarge, not "
              "32 - SMT is off and cores are reserved for the OS."),
    ))
    p.append(panel(
        42, "guest execution latency (one record through the guest)", 16, y, 8, 8,
        [(hist_quantile("redpanda_transform_execution_latency_sec", q), lab)
         for q, lab in QUANTILES],
        unit="s", legend_table=True,
        desc=("Just the guest call, with no queueing or write path. The floor "
              "any in-broker number is measured against."),
    ))
    y += 8

    p.append(row(5, "Module residency", y)); y += 1
    p.append(panel(
        50, "resident compiled modules", 0, y, 8, 6,
        [("sum(redpanda_wasm_binary_resident_factories)", "resident"),
         ("sum(redpanda_wasm_binary_resident_admission_failures)", "admission failures")],
        unit="short", legend_table=True,
        desc=("Compiled modules held in memory so a transform starting later "
              "does not recompile. Zero when "
              "data_transforms_max_resident_factories is 0, which is the "
              "default. Admission failures mean the bound was hit while every "
              "resident module was in use."),
    ))
    p.append(panel(
        51, "resident module memory", 8, y, 8, 6,
        [("sum(redpanda_wasm_binary_resident_memory_usage)", "image bytes")],
        unit="bytes", legend_table=True,
        desc=("Divided by the resident module count, this is what raising the "
              "residency bound costs per module for these binaries."),
    ))
    p.append(panel(
        52, "processor create latency (fetch + compile)", 16, y, 8, 6,
        [(hist_quantile("redpanda_transform_processor_create_latency_seconds", q), lab)
         for q, lab in QUANTILES],
        unit="s", legend_table=True,
        desc=("Fetching the wasm binary from the cluster and compiling it. "
              "Near zero on a broker already running the transform, and it "
              "dominates startup on one that is not - which is what residency "
              "exists to remove. Watch this across a leadership move."),
    ))
    y += 6

    return dashboard(
        "wasm-bench-overview", "wasm bench: overview", p,
        desc=("The in-broker wasm transform benchmark at a glance. Latency "
              "first, then the gates that decide whether the latency means "
              "anything, then where the CPU went. Published numbers come from "
              "loadgen's own histograms, not from this Prometheus - see "
              "REPRODUCE.md."),
        tags=["wasm-bench"],
    )


# ===========================================================================
# 2. Relay stages - where the in-broker microseconds actually go.
#
# One row per stage, in traversal order, each with its own quantiles. This is
# the dashboard for the question "the number moved, which stage moved it",
# which a single combined panel cannot answer.
# ===========================================================================
def relay_stages():
    p, y = [], 0

    p.append(row(1, "The whole span, and the stages under it", y)); y += 1
    p.append(panel(
        10, "emit -> guest (quote this one)", 0, y, 12, 8,
        [(hist_quantile("redpanda_relay_emit_to_guest_duration_seconds", q), lab)
         for q, lab in QUANTILES],
        unit="s", legend_table=True,
        desc=("One span, measured end to end, so it has no overlap or gap "
              "problem. The four stage panels below explain it; they do not "
              "sum to it."),
    ))
    p.append(panel(
        11, "all four stages, p99, on one axis", 12, y, 12, 8,
        [(hist_quantile(f"redpanda_relay_{m}", 0.99), lab.split(" (")[0])
         for m, lab in RELAY_STAGES],
        unit="s", legend_table=True,
        desc=("The comparison view: which stage dominates at this fan-out. "
              "Deliberately p99 only - four stages times three quantiles on "
              "one axis is unreadable, and the per-stage panels below carry "
              "the full set."),
    ))
    y += 8

    for i, (metric, label) in enumerate(RELAY_STAGES):
        p.append(row(100 + i, label, y)); y += 1
        p.append(panel(
            200 + i, "latency", 0, y, 16, 7,
            [(hist_quantile(f"redpanda_relay_{metric}", q), lab)
             for q, lab in QUANTILES],
            unit="s", legend_table=True,
        ))
        p.append(panel(
            300 + i, "samples/sec recorded", 16, y, 8, 7,
            [(f"sum(rate(redpanda_relay_{metric}_count[$__rate_interval]))",
              "samples/s")],
            unit="reqps",
            desc=("Flat zero here with a live workload means "
                  "relay_stage_metrics_enabled is off, not that the stage is "
                  "free. The histograms are always registered so that "
                  "flipping the property live starts populating a series "
                  "something is already graphing, rather than making a new "
                  "one appear."),
        ))
        y += 7

    return dashboard(
        "wasm-bench-relay-stages", "wasm bench: relay stages", p,
        desc=("The in-broker fan-out path broken into the four stages the "
              "relay instruments, in the order a record traverses them. "
              "Needs relay_stage_metrics_enabled=true, which `wb reproduce` "
              "sets for the in-broker arm."),
        tags=["wasm-bench"],
    )


# ===========================================================================
# 3. Measurement health - is this run's number trustworthy?
#
# This dashboard exists because of METHODOLOGY 29: 499 consumer groups
# co-located with loadgen starved the load generator, its send_lateness p99 hit
# 2,486us against a 1,000us pacing interval, and the latency it reported
# included client-side queueing that looked exactly like an architectural
# result. Nothing on the broker side showed it. The client host panels are the
# first thing to check when a number looks wrong.
# ===========================================================================
def health():
    p, y = [], 0

    p.append(row(1, "Client hosts - client0 is loadgen and must have headroom", y)); y += 1
    p.append(panel(
        10, "CPU utilisation by host", 0, y, 12, 9,
        [('1 - avg by (host) (rate(node_cpu_seconds_total{mode="idle",role="client"}'
          '[$__rate_interval]))', "{{host}}")],
        unit="percentunit", legend_table=True,
        desc=("client0 runs loadgen and nothing else, deliberately, so that "
              "pacing stays clean. client1 is the external matcher. client2 "
              "and up carry the background consumer groups at ~84 each. If "
              "client0 is near saturation, the measurement is of a starved "
              "load generator and not of the architecture - see METHODOLOGY "
              "29, which is exactly this failure."),
    ))
    p.append(panel(
        11, "run-queue pressure by host (load / cores)", 12, y, 12, 9,
        # Both sides reduce to exactly {host}, so this is a 1:1 match on
        # identical label sets and needs no on()/group_left. An earlier
        # version joined `on (instance)`, which for one-to-one matching keeps
        # ONLY the on() labels - dropping `host` and leaving every legend
        # entry blank.
        [('sum by (host) (node_load1{role="client"}) / '
          'count by (host) (node_cpu_seconds_total{mode="idle",role="client"})',
          "{{host}}")],
        unit="percentunit", legend_table=True,
        desc=("Load average normalised by core count, so 1.0 means the run "
              "queue is as long as the machine is wide. Catches a client that "
              "is not CPU-pegged but is still waiting to be scheduled, which "
              "is what a paced generator is most sensitive to."),
    ))
    y += 9

    p.append(row(2, "Broker hosts", y)); y += 1
    p.append(panel(
        20, "CPU utilisation by broker", 0, y, 8, 8,
        [('1 - avg by (host) (rate(node_cpu_seconds_total{mode="idle",role="broker"}'
          '[$__rate_interval]))', "{{host}}")],
        unit="percentunit", legend_table=True,
        desc=("Whole-host CPU, so it covers the 19 vCPU redpanda is not using "
              "as well as its own 13. Compare against the per-shard reactor "
              "panel on the overview: a busy shard with an idle host is a "
              "placement result, not a capacity one."),
    ))
    p.append(panel(
        21, "network in/out by broker", 8, y, 8, 8,
        [('sum by (host) (rate(node_network_receive_bytes_total{role="broker"}'
          '[$__rate_interval])) * 8', "{{host}} in"),
         ('sum by (host) (rate(node_network_transmit_bytes_total{role="broker"}'
          '[$__rate_interval])) * 8', "{{host}} out")],
        unit="bps", legend_table=True,
        desc=("The external arm's fan-out is paid here, as egress to 500 "
              "consumers. The in-broker arm's is not, which is most of the "
              "point of the comparison."),
    ))
    p.append(panel(
        22, "transform bytes in/out", 16, y, 8, 8,
        [("sum(rate(redpanda_transform_read_bytes[$__rate_interval])) * 8", "read"),
         ("sum(rate(redpanda_transform_write_bytes[$__rate_interval])) * 8", "written")],
        unit="bps", legend_table=True,
    ))
    y += 8

    p.append(row(3, "loadgen live metrics (only up while a window is running)", y)); y += 1
    p.append(panel(
        30, "send lateness against the pacing interval", 0, y, 12, 8,
        [(hist_quantile("loadgen_send_lateness_seconds", q), lab)
         for q, lab in QUANTILES],
        unit="s", legend_table=True,
        desc=("How late each produce call was against its scheduled slot. At "
              "RATE=1000 the pacing interval is 1,000us, so a p99 approaching "
              "that means the generator is no longer holding the offered rate "
              "and its latency figures include its own queueing. This is the "
              "single most useful client-side number in the harness. Needs "
              "loadgen started with -metrics-listen; empty otherwise."),
    ))
    p.append(panel(
        31, "offered vs acknowledged rate", 12, y, 12, 8,
        [("sum(rate(loadgen_orders_sent_total[$__rate_interval]))", "orders sent/s"),
         ("sum(rate(loadgen_orders_acked_total[$__rate_interval]))", "orders acked/s"),
         ("sum(rate(loadgen_receipts_total[$__rate_interval]))", "receipts/s")],
        unit="reqps", legend_table=True,
        desc=("Sent tracking acked tracking receipts is a healthy window. "
              "Sent above acked means the broker is applying backpressure; "
              "receipts below acked means output is not coming back, which "
              "the run's own `missing` count will also report."),
    ))
    y += 8

    p.append(row(4, "The observer itself", y)); y += 1
    p.append(panel(
        40, "prometheus scrape duration by job", 0, y, 12, 6,
        [("max by (job) (scrape_duration_seconds)", "{{job}}")],
        unit="s", legend_table=True,
        desc=("If a scrape duration approaches its interval, Prometheus is "
              "the bottleneck and the gaps in these panels are its fault "
              "rather than the cluster's. redpanda-internal is expected to be "
              "the slow one; it is scraped at 15s for that reason."),
    ))
    p.append(panel(
        41, "targets up by job", 12, y, 12, 6,
        [('sum by (job) (up)', "{{job}}")],
        unit="short", legend_table=True,
        desc=("loadgen is expected to be down between runs - it is a batch "
              "process, not a service. Everything else should be flat at its "
              "instance count for the whole run."),
    ))
    y += 6

    return dashboard(
        "wasm-bench-health", "wasm bench: measurement health", p,
        desc=("Whether this run's numbers can be trusted. Client saturation "
              "has produced a confident wrong answer in this harness before "
              "(METHODOLOGY 29) and nothing broker-side revealed it, so the "
              "client host panels come first."),
        tags=["wasm-bench"],
    )


DASHBOARDS = {
    "bench-overview.json": overview,
    "relay-stages.json": relay_stages,
    "bench-health.json": health,
}


def main():
    outdir = Path(sys.argv[1] if len(sys.argv) > 1
                  else "ansible/files/dashboards/wasm-bench")
    outdir.mkdir(parents=True, exist_ok=True)
    for name, fn in DASHBOARDS.items():
        d = fn()
        (outdir / name).write_text(json.dumps(d, indent=2) + "\n")
        n = sum(1 for x in d["panels"] if x["type"] != "row")
        print(f"  {name:24s} {d['uid']:28s} {n} panels")


if __name__ == "__main__":
    main()
