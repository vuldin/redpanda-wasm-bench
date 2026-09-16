#!/usr/bin/env python3
"""Print the live state of a benchmark window from Prometheus.

The question this answers is "is the run happening right now healthy", which
during a run is asked repeatedly and is otherwise a handful of curl-and-parse
incantations each time. The gates here are the same ones the overview
dashboard leads on, so a terminal and a browser agree.

Usage: scripts/run-health.py [prometheus-url]   (default: from `wb env`)
"""
import json
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def prom_url():
    if len(sys.argv) > 1:
        return sys.argv[1]
    out = subprocess.run(["bash", str(ROOT / "scripts" / "wb"), "env"],
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        if line.startswith("PROMETHEUS_URL="):
            return line.split("=", 1)[1].strip().strip('"')
    sys.exit("no PROMETHEUS_URL - is a cluster up with a monitoring host?")


def query(base, expr):
    data = urllib.parse.urlencode({"query": expr}).encode()
    req = urllib.request.Request(base.rstrip("/") + "/api/v1/query", data=data)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            res = json.load(r)["data"]["result"]
    except Exception as e:
        return None
    if not res:
        return None
    v = res[0]["value"][1]
    try:
        f = float(v)
    except ValueError:
        return None
    return None if f != f else f  # NaN


# (label, expr, formatter, what a healthy value looks like)
W = "[30s]"
CHECKS = [
    ("emit->guest p50",
     f"histogram_quantile(0.5, sum by (le) (rate(redpanda_relay_emit_to_guest_duration_seconds_bucket{W})))",
     lambda v: f"{v*1e6:,.0f} us", None),
    ("emit->guest p99",
     f"histogram_quantile(0.99, sum by (le) (rate(redpanda_relay_emit_to_guest_duration_seconds_bucket{W})))",
     lambda v: f"{v*1e6:,.0f} us", None),
    ("transform e2e p50",
     f"histogram_quantile(0.5, sum by (le) (rate(redpanda_transform_e2e_latency_seconds_bucket{W})))",
     lambda v: f"{v*1e6:,.0f} us", None),
    ("pushes/s",
     f"sum(rate(redpanda_relay_pushes_total{W}))", lambda v: f"{v:,.0f}", None),
    ("delivered/s",
     f"sum(rate(redpanda_relay_delivered_total{W}))", lambda v: f"{v:,.0f}", None),
    # The gates. Anything non-zero here means the latency above is the latency
    # of an incomplete or retrying stream.
    ("dropped/s (must be 0)",
     f"sum(rate(redpanda_relay_dropped_total{W}))", lambda v: f"{v:,.3f}",
     lambda v: v == 0),
    ("transform failures/s (must be 0)",
     f"sum(rate(redpanda_transform_failures{W}))", lambda v: f"{v:,.3f}",
     lambda v: v == 0),
    ("batches given up (must be 0)",
     "sum(redpanda_transform_batches_given_up)", lambda v: f"{v:,.0f}",
     lambda v: v == 0),
    ("transform lag",
     "sum(redpanda_transform_lag)", lambda v: f"{v:,.0f}", None),
    # Subscriptions are per (consumer x input partition led), NOT per consumer,
    # so this is reported as a fact rather than checked against a target.
    ("relay subscriptions",
     "sum(redpanda_relay_active_subscriptions)", lambda v: f"{v:,.0f}", None),
    ("guest CPU (cores)",
     f"sum(rate(redpanda_wasm_engine_cpu_seconds_total{W}))",
     lambda v: f"{v:,.2f}", None),
    ("busiest shard utilisation",
     f"max(rate(redpanda_cpu_busy_seconds_total{W}))",
     lambda v: f"{v*100:,.1f}%", None),
    # The client side. METHODOLOGY 29: a starved generator reports latency that
    # includes its own queueing and looks exactly like an architecture result.
    ("loadgen send lateness p99",
     f"histogram_quantile(0.99, sum by (le) (rate(loadgen_send_lateness_seconds_bucket{W})))",
     lambda v: f"{v*1e6:,.0f} us", None),
    ("loadgen pacing interval",
     "max(loadgen_pacing_interval_seconds)", lambda v: f"{v*1e6:,.0f} us", None),
    # One decimal, because during the in-broker arm the clients are genuinely
    # near-idle and "0%" reads like a broken query rather than a real answer.
    ("busiest client CPU",
     f'max(1 - avg by (host) (rate(node_cpu_seconds_total{{mode="idle",role="client"}}{W})))',
     lambda v: f"{v*100:,.1f}%", None),
]


def main():
    base = prom_url()
    print(f"# {base}   window={W}\n")
    bad = 0
    for label, expr, fmt, ok in CHECKS:
        v = query(base, expr)
        if v is None:
            print(f"  {label:34s} -")
            continue
        mark = ""
        if ok is not None and not ok(v):
            mark = "   <-- PROBLEM"
            bad += 1
        print(f"  {label:34s} {fmt(v)}{mark}")
    print()
    if bad:
        print(f"{bad} gate(s) failing - the latency above is not a clean measurement")
        return 1
    print("gates clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
