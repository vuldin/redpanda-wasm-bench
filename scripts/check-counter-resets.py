#!/usr/bin/env python3
"""Flag counters the dashboards rate() over that reset without a restart.

rate(), irate() and increase() assume a counter only ever goes up, and treat
any decrease as a process restart: they add the counter's whole value back in.
A metric that steps backwards while its process keeps running therefore shows
up as huge spikes - redpanda_cpu_busy_seconds_total did exactly that, single
shards drawn at 1,400% busy (2026-10-16). It is registered as a gauge over
seastar's total_busy_time(), which is not strictly increasing.

A real reset follows a process restart, which happens a handful of times in a
window at most (loadgen restarts once per measured level); a fake one happens
continually (the busy-time gauge "reset" 100+ times per broker in 20 minutes).
So the signature is many resets on one series. Measured on a live six-broker
run: loadgen counters reset at most 4 times an hour, the busy-time gauge 5-10
times a minute per broker. The default limit is one reset per three minutes of
window (at least 5), which sits between the two with a wide margin either way. Target liveness is no help: a
missed scrape takes `up` to 0 without any restart. This checks every metric
any dashboard wraps in rate(), irate() or increase() against a live
Prometheus.

  scripts/check-counter-resets.py http://<monitor-ip>:9090 [window, default 30m] [max resets]
  scripts/check-counter-resets.py <prom> 20m --metric redpanda_cpu_busy_seconds_total

--metric checks the named metric instead of the dashboards' (the known-bad
one above is the negative control: it should be flagged).

Prints one line per suspicious (metric, target) and exits 1 if there are any,
0 if none, 2 if Prometheus could not be queried.
"""
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "ansible" / "files" / "dashboards" / "wasm-bench"
RATE_RE = re.compile(r"(?:rate|irate|increase)\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*)")


def dashboard_rate_metrics():
    names = set()
    for f in sorted(DASH.glob("*.json")):
        for p in json.loads(f.read_text()).get("panels", []):
            for t in p.get("targets", []):
                names.update(RATE_RE.findall(t.get("expr", "")))
    return sorted(names)


def query(prom, expr):
    url = prom.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(body.get("error", "query failed"))
    return body["data"]["result"]


def check(prom, metrics, window, limit):
    """[(metric, labels, resets)] for series resetting more than `limit` times."""
    out = []
    for m in metrics:
        for r in query(prom, f"max by (job, instance) (resets({m}[{window}])) > {limit}"):
            out.append((m, r["metric"], r["value"][1]))
    return out


def default_limit(window):
    """One reset per three minutes of window, at least 5."""
    m = re.fullmatch(r"(\d+)([smhd])", window)
    if not m:
        raise ValueError(f"window {window!r}: expected e.g. 30m or 2h")
    minutes = int(m[1]) * {"s": 1 / 60, "m": 1, "h": 60, "d": 1440}[m[2]]
    return max(5, int(minutes // 3))


def main():
    args = sys.argv[1:]
    metrics = None
    if "--metric" in args:
        i = args.index("--metric")
        metrics = [args[i + 1]]
        del args[i:i + 2]
    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    prom, window = args[0], (args[1] if len(args) > 1 else "30m")
    limit = int(args[2]) if len(args) > 2 else default_limit(window)
    flagged = 0
    try:
        for m, lab, n in check(prom, metrics or dashboard_rate_metrics(), window, limit):
            flagged += 1
            print(f"SUSPECT {m} job={lab.get('job')} instance={lab.get('instance')} "
                  f"resets={n} in {window} (more than {limit} on one series)")
    except Exception as e:  # noqa: BLE001 - report and let the caller decide
        print(f"counter check could not query {prom}: {e}", file=sys.stderr)
        return 2
    if flagged:
        print(f"{flagged} counter series reset far more often than restarts explain: "
              "rate() over them is wrong; see scripts/check-counter-resets.py")
        return 1
    print(f"counter check: no dashboard counter reset more than {limit} times in {window}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
