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
  scripts/check-dashboards.py <prometheus-url> [dashboard-dir]

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
}


def allowed_empty(expr):
    for prefix, why in ALLOWED_EMPTY.items():
        if prefix in expr:
            return why
    return None


def query(base, expr):
    body = urllib.parse.urlencode({"query": expr}).encode()
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
    base = sys.argv[1]
    ddir = Path(sys.argv[2] if len(sys.argv) > 2
                else "ansible/files/dashboards")

    files = sorted(ddir.rglob("*.json"))
    if not files:
        print(f"no dashboards under {ddir}")
        return 2

    bad, empty, ok = [], [], 0
    for f in files:
        d = json.loads(f.read_text())
        print(f"\n=== {f.relative_to(ddir)}  ({d.get('title')}) ===")
        for pan in d.get("panels", []):
            for t in pan.get("targets", []):
                expr = t["expr"]
                # Grafana expands this from the panel's step and the
                # datasource interval; Prometheus does not know it.
                concrete = expr.replace("$__rate_interval", "30s")
                res, err = query(base, concrete)
                title = pan.get("title", "?")
                if err:
                    bad.append((f.name, title, expr, err))
                    print(f"  FAIL  {title}: {err}")
                    continue
                if res.get("status") != "success":
                    reason = res.get("error", "unknown")
                    bad.append((f.name, title, expr, reason))
                    print(f"  FAIL  {title}: {reason}")
                    continue
                n = len(res["data"]["result"])
                if n == 0:
                    why = allowed_empty(expr)
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
