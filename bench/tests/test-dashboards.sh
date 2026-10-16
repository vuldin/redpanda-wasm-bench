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
# In-place updates: a closed phase that matches is untouched, the running one
# is extended, a new one created, a stale or old-format one removed.
body = lambda u, n, a, t1, o: {"dashboardUID": u, "time": 0, "timeEnd": t1,
    "tags": ["wasm-bench-phase", "run:r", f"phase:{n}", f"attempt:{a}", o], "text": f"{n} {o}"}
desired = {("d", "off", "1"): body("d", "off", "1", 10, "ok"),
           ("d", "inline", "1"): body("d", "inline", "1", 30, "running"),
           ("d", "bg", "1"): body("d", "bg", "1", 40, "running")}
existing = [dict(body("d", "off", "1", 10, "ok"), id=1),
            dict(body("d", "inline", "1", 20, "running"), id=2),
            dict(body("d", "gone", "1", 5, "ok"), id=3),
            {"id": 4, "dashboardUID": "d", "time": 0, "timeEnd": 9, "tags": ["phase:off"], "text": "old"}]
c, u, dl = ap.plan_changes(existing, desired)
expect("annotate-phases: in place - matching phase untouched, running extended, new created, stale removed",
       [b["tags"][2] for b in c] == ["phase:bg"] and [i for i, _ in u] == [2] and sorted(dl) == [3, 4])
c, u, dl = ap.plan_changes([dict(b, id=i) for i, b in enumerate(desired.values())], desired)
expect("annotate-phases: a second pass over an unchanged log changes nothing", (c, u, dl) == ([], [], []))
# summarize-run.py's stall column: periodic slow buckets are named with their
# period, scattered ones are only counted.
# summarize-run.py is a script, not a module: lift STALL_US and stalls() out
# of it by name rather than running it.
import ast, types
tree = ast.parse(open(f"{root}/scripts/summarize-run.py").read())
keep = [n for n in tree.body if (isinstance(n, ast.FunctionDef) and n.name == "stalls")
        or (isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "STALL_US" for t in n.targets))]
sr = types.ModuleType("sr"); exec(compile(ast.Module(body=keep, type_ignores=[]), "sr", "exec"), sr.__dict__)
bucket = lambda ms, mx: {"offset_ms": ms, "max_micros": mx}
periodic_tl = [bucket(i * 500, 9000 if i % 10 == 3 else 900) for i in range(180)]
scattered_tl = [bucket(i * 500, 9000 if i in (5, 31, 77, 140) else 900) for i in range(180)]
expect("stall column: a 5s checkpoint stall reads '18 @ 5,000ms'", sr.stalls(periodic_tl) == "18 @ 5,000ms")
expect("stall column: scattered stalls are only counted", sr.stalls(scattered_tl) == "4")
expect("stall column: no timeline -> '-'", sr.stalls(None) == "-")

cs = load("cs", "scripts/check-scrapes.py")
cfg = """global:
  scrape_interval: 15s
  scrape_timeout: 10s
scrape_configs:
- job_name: redpanda
  scrape_interval: 2s
  scrape_timeout: 2s
- job_name: node
  scrape_interval: 5s
  scrape_timeout: 5s
"""
expect("check-scrapes: effective timeout per job", cs.job_timeouts(cfg) == {"redpanda": 2.0, "node": 5.0})
up = {("redpanda", "broker0"): [(t, 0.0 if t in (10, 20, 30, 40) else 1.0) for t in range(0, 60, 2)]}
dur = {("redpanda", "broker0"): [(t, 1.95 if t in (10, 20) else 0.02) for t in range(0, 60, 2)]}
r = cs.analyse(up, dur, {"redpanda": 2.0})[0]
expect("check-scrapes: timeouts vs fast failures, and the spacing",
       (r["failed"], r["timeouts"], r["fast_failures"], r["spacing"]) == (4, 2, 2, "every 10,000ms"))
sys.exit(1 if bad else 0)
PY

exit "$fail"
