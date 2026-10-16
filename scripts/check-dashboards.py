#!/usr/bin/env python3
"""Ask a live Prometheus whether every dashboard query actually works.

Two failure modes this catches, both of which are silent in Grafana:

  1. A query that does not parse. Grafana renders an empty panel and puts the
     error behind a hover, so a dashboard can look finished and be broken.
  2. A query that parses and returns nothing, because the metric name is wrong
     or a label does not exist. Indistinguishable from "the feature is off"
     unless you know which is which - so the ones that are legitimately empty
     are listed here by name and everything else is a failure.

Usage:
  scripts/check-dashboards.py <prometheus-url> [dashboard-dir] [--at UNIX]

--at evaluates every query at that instant instead of now, which is what makes
this usable against an exported bundle. A bundle is a RECORDING: its newest
sample is however old the run is, and Prometheus instant queries only look back
five minutes, so every query against a recording returns empty at `now` and the
whole validator reports failure on a perfectly good bundle. Pass the midpoint
of the recorded range - export-metrics.sh reads it out of the bundle manifest.

Exit status is non-zero if anything failed, so it can gate a deploy.
"""
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

# Queries allowed to come back empty, with the reason. Anything empty and NOT
# listed here is a real failure: a typo in a metric name looks exactly like a
# feature being off, and that ambiguity is the thing this script exists to
# remove.
ALLOWED_EMPTY = {
    "loadgen_": "loadgen is a per-window batch process, not a service - it is "
                "only up while a measurement is in flight",
    "redpanda_wasm_binary_resident": "residency is off unless "
                                     "data_transforms_max_resident_factories > 0",
    "redpanda_relay_": "relay stage histograms are only recorded when "
                       "relay_stage_metrics_enabled is true, and the relay "
                       "counters only exist once a transform has pushed",
    "redpanda_transform_": "transform metrics only exist while a transform is "
                           "deployed and running",
    "bench_phase_info": "autorun's phase marker only exists while an autorun "
                        "phase is running",
}


# In RECORDING mode the allowance is much narrower. A live cluster can
# legitimately have no relay or transform series - nothing is deployed between
# runs. A recording cannot: it exists precisely because a measurement happened,
# so an empty relay panel means the wrong time range was captured, not that a
# feature was off.
#
# This distinction is why a mis-windowed bundle passed validation. The export
# captured ~12s of each ~95s traffic period, every latency panel was blank, and
# the relay families were waved through as "only exist while running".
RECORDING_ALLOWED_EMPTY = {
    "loadgen_": ALLOWED_EMPTY["loadgen_"],
    "redpanda_wasm_binary_resident": ALLOWED_EMPTY["redpanda_wasm_binary_resident"],
    "bench_phase_info": ("a recording made before the phase marker existed "
                         "has none, and a window can fall between phases"),
    "redpanda_transform_processor_create_latency": (
        "processor create latency is only recorded when a processor is created, "
        "which may not happen inside a measured window"),
}


def _real(series):
    """True if any sample is a real number.

    NaN is NOT data. histogram_quantile over a rate window with no samples
    returns a result entry whose value is NaN, and Grafana renders that as an
    empty panel. Counting it as data is how this validator once reported 57
    queries returning data against a recording that had captured the wrong
    time range and whose every latency panel was blank.
    """
    for smp in series:
        v = smp.get("value", [None, "NaN"])[1]
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv == fv:
            return True
    return False


def allowed_empty(expr, recording=False):
    table = RECORDING_ALLOWED_EMPTY if recording else ALLOWED_EMPTY
    for prefix, why in table.items():
        if prefix in expr:
            return why
    return None


def variable_defaults(d):
    """{name: value} for the dashboard's template variables, at their defaults.

    Grafana substitutes these before a query leaves the browser; Prometheus
    has never heard of $broker, so a query sent with it unexpanded matches
    nothing and the panel reads as broken when it is not."""
    out = {}
    for v in d.get("templating", {}).get("list", []):
        cur = v.get("current", {}).get("value", ".+")
        vals = cur if isinstance(cur, list) else [cur]
        out[v["name"]] = ".+" if "$__all" in vals or not vals else "|".join(vals)
    return out


def query(base, expr, at=None):
    params = {"query": expr}
    if at is not None:
        params["time"] = str(at)
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(
        base.rstrip("/") + "/api/v1/query", data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r), None
    except urllib.error.HTTPError as e:
        # A parse error comes back as HTTP 400 with the reason in the body,
        # which is the case worth reading rather than just the status.
        try:
            return json.load(e), None
        except Exception:
            return None, f"HTTP {e.code}"
    except Exception as e:
        return None, str(e)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    args = [a for a in sys.argv[1:]]
    ats = []
    if "--at" in args:
        i = args.index("--at")
        ats = [float(x) for x in args[i + 1].split(",") if x.strip()]
        del args[i:i + 2]
    base = args[0]
    ddir = Path(args[1] if len(args) > 1 else "ansible/files/dashboards")

    if ats:
        print("# evaluating at " + ", ".join(str(int(a)) for a in ats)
              + " rather than now: a recording's newest\n"
              "# sample is older than the 5m instant-query lookback, so every\n"
              "# query would read empty at `now`. A query passes if ANY of these\n"
              "# instants has data - windows are padded, so the midpoint of the\n"
              "# whole recording can land in a pad after traffic stopped.\n")
    files = sorted(ddir.rglob("*.json"))
    if not files:
        print(f"no dashboards under {ddir}")
        return 2

    bad, empty, ok = [], [], 0
    for f in files:
        d = json.loads(f.read_text())
        print(f"\n=== {f.relative_to(ddir)}  ({d.get('title')}) ===")
        variables = variable_defaults(d)
        for pan in d.get("panels", []):
            for t in pan.get("targets", []):
                expr = t["expr"]
                # Grafana expands this from the panel's step and the
                # datasource interval; Prometheus does not know it.
                concrete = expr.replace("$__rate_interval", "30s")
                # Longest name first, so $broker never eats the front of $brokers.
                for name in sorted(variables, key=len, reverse=True):
                    concrete = concrete.replace("${" + name + "}", variables[name])
                    concrete = concrete.replace("$" + name, variables[name])
                # Any instant with real data is enough: a panel works if
                # the recording contains its data somewhere, not if it happens
                # to be non-empty at one arbitrary moment.
                title = pan.get("title", "?")
                res, err, n = None, None, 0
                for _at in (ats or [None]):
                    res, err = query(base, concrete, _at)
                    if err or res is None or res.get("status") != "success":
                        continue
                    got = res["data"]["result"]
                    if got and _real(got):
                        n = len(got)
                        break
                if n == 0 and err:
                    bad.append((f.name, title, expr, err))
                    print(f"  FAIL  {title}: {err}")
                    continue
                if n == 0 and res is not None and res.get("status") != "success":
                    reason = res.get("error", "unknown")
                    bad.append((f.name, title, expr, reason))
                    print(f"  FAIL  {title}: {reason}")
                    continue
                if n == 0:
                    why = allowed_empty(expr, recording=bool(ats))
                    if why:
                        empty.append((f.name, title, expr, why))
                        print(f"  empty {title}  (expected: {why[:60]}...)")
                    else:
                        bad.append((f.name, title, expr, "returned no series"))
                        print(f"  FAIL  {title}: returned no series")
                    continue
                ok += 1
                print(f"  ok    {title}  ({n} series)")

    print("\n" + "=" * 72)
    print(f"{ok} queries returning data, {len(empty)} legitimately empty, "
          f"{len(bad)} failed")
    if empty:
        print("\nempty but expected:")
        for _, title, _, why in empty:
            print(f"  {title}\n      {why}")
    if bad:
        print("\nFAILURES:")
        for fn, title, expr, why in bad:
            print(f"  {fn} / {title}\n      {why}\n      {expr}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
