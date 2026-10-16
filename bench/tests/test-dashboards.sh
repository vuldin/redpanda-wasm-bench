#!/usr/bin/env bash
# Offline checks on the Grafana dashboards and the counter-reset guard:
#   - the committed JSON is what scripts/make-dashboards.py generates (edit the
#     generator, forget to regenerate, and the deploy ships the old panels);
#   - the generator refuses rate() over a NOT_COUNTERS metric, the mistake that
#     drew single shards at 1,400% busy;
#   - check-counter-resets.py's threshold and its query, against a stub
#     Prometheus, flag the many-resets series and pass the few-resets one.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
fail=0

if python3 "$ROOT/scripts/make-dashboards.py" "$T/gen" >/dev/null &&
   diff -r "$T/gen" "$ROOT/ansible/files/dashboards/wasm-bench" >/dev/null; then
  echo "PASS  committed dashboard JSON matches the generator"
else
  echo "FAIL  committed dashboard JSON is stale - run: python3 scripts/make-dashboards.py"; fail=1
fi

python3 - "$ROOT" <<'PY' || fail=1
import importlib.util, sys
root = sys.argv[1]
def load(name, path):
    spec = importlib.util.spec_from_file_location(name, f"{root}/{path}")
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m
md = load("md", "scripts/make-dashboards.py")
cr = load("cr", "scripts/check-counter-resets.py")
bad = 0
def expect(name, ok):
    global bad
    print(("PASS  " if ok else "FAIL  ") + name); bad += (not ok)

for name, fn in md.DASHBOARDS.items():
    expect(f"{name}: no rate() over a NOT_COUNTERS metric", md.rate_misuse(fn()) == [])
# Latency lines must break at idle gaps: NaN kept (a `>= 0` filter makes
# Grafana join across the gap), spanNulls off everywhere, and both "latency
# samples" choices keep NaN rather than dropping points.
lat = [t["expr"] for fn in md.DASHBOARDS.values() for p in fn()["panels"]
       for t in p.get("targets", []) if "histogram_quantile" in t.get("expr", "")]
expect("latency queries keep NaN (no >= 0 filter) and carry $window",
       lat and all(">= 0" not in e and "$window" in e for e in lat if "increase(" not in e))
expect("spanNulls is off on every time series panel",
       all(p["fieldConfig"]["defaults"]["custom"].get("spanNulls") is False
           for fn in md.DASHBOARDS.values() for p in fn()["panels"] if p.get("type") == "timeseries"))
expect("measured-only is a NaN mask (X * M / M), not an `and` filter",
       md.MEASURED_ONLY.startswith("* on() ") and " / on() " in md.MEASURED_ONLY and md.ALL_SAMPLES == "* 1")
expect("now-running strips always return a series (idle fallback)",
       all("or on() label_replace(vector(0)" in t["expr"]
           for p in md.overview()["panels"] if p["id"] in (9, 109) for t in p["targets"]))

planted = {"panels": [{"title": "planted", "targets": [
    {"expr": "sum by (shard) (irate( redpanda_cpu_busy_seconds_total[$__rate_interval]))"}]}]}
expect("generator refuses a planted irate() over the busy-time gauge",
       [m for _, m, _ in md.rate_misuse(planted)] == ["redpanda_cpu_busy_seconds_total"])
expect("the busy-time gauge is not among the metrics the dashboards rate()",
       not set(md.NOT_COUNTERS) & set(cr.dashboard_rate_metrics()))

expect("limit: 30m -> 10, 20m -> 6, 5m -> 5, 2h -> 40",
       [cr.default_limit(w) for w in ("30m", "20m", "5m", "2h")] == [10, 6, 5, 40])

# Stub Prometheus: answers the `> limit` filter the way the real one would.
series = {"good_total": [("loadgen", "c0", 4)], "bad_total": [("redpanda", "b0", 112), ("redpanda", "b1", 3)]}
def fake_query(prom, expr):
    m = expr.split("resets(")[1].split("[")[0]; lim = int(expr.rsplit(">", 1)[1])
    return [{"metric": {"job": j, "instance": i}, "value": [0, str(n)]} for j, i, n in series[m] if n > lim]
cr.query = fake_query
got = cr.check("stub", ["good_total", "bad_total"], "20m", 6)
expect("check() flags only the series resetting more than the limit",
       [(m, l["instance"]) for m, l, _ in got] == [("bad_total", "b0")])
ap = load("ap", "scripts/annotate-phases.py")
log = """[2026-10-16T12:30:14Z] WATCH: Grafana http://203.0.113.9:3000
[2026-10-16T12:30:20Z] START off attempt 1 (est 7min, cap 10min): wb run e2 X=1
[2026-10-16T12:36:52Z] OK off in 6m32s
[2026-10-16T12:37:10Z] START inline-t1 attempt 1 (est 7min, cap 10min): wb run e2
[2026-10-16T12:38:10Z] FAIL inline-t1 attempt 1 rc=1 after 1m0s (tail of inline-t1.log below)
[2026-10-16T12:38:20Z] START inline-t1 attempt 2 (est 7min, cap 10min): wb run e2
"""
got = [(n, a, o) for n, a, _, _, o in ap.phases(log, now_ms=1)]
expect("annotate-phases: attempts, outcomes and the running phase",
       got == [("off", 1, "ok"), ("inline-t1", 1, "failed"), ("inline-t1", 2, "running")])
t = ap.phases(log, now_ms=1)[0]
expect("annotate-phases: region spans START to OK", (t[3] - t[2]) == 392_000)
sys.exit(1 if bad else 0)
PY

exit "$fail"
