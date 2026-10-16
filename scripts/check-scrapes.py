#!/usr/bin/env python3
"""Report every failed scrape in a window, and what kind of failure it was.

Brokers' `up` dropped to 0 about 8 times per broker per 20 minutes on the
2026-10-16 checkpoint run, with nothing restarting and scrapes normally taking
~0.02s against a 2s interval (which is also the default timeout). This lists
each failure so it can be explained instead of guessed at:

  - when it happened, and whether failures on one target recur at a fixed
    spacing (a multiple of 5,000ms would point at the transform checkpoint);
  - what Prometheus recorded as that scrape's duration: close to the timeout
    means the target was too slow to answer (a stalled shard), well under it
    means the scrape failed fast (connection refused or reset).

  scripts/check-scrapes.py <prometheus-url> <start-unix> <end-unix> [jobs-regex] [--tsv FILE]

jobs-regex defaults to redpanda.*. Exit 0 always, unless Prometheus could not
be queried (2): this is a report, not a gate.
"""
import collections
import json
import sys
import urllib.parse
import urllib.request


def query(prom, expr, at):
    url = prom.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": at})
    with urllib.request.urlopen(url, timeout=60) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(body.get("error", "query failed"))
    return body["data"]["result"]


def raw(prom, metric, jobs, start, end):
    """{(job, host): [(ts, value), ...]} - the raw samples, not a resampled range."""
    secs = max(int(end - start), 1)
    out = {}
    for s in query(prom, f'{metric}{{job=~"{jobs}"}}[{secs}s]', end):
        m = s["metric"]
        key = (m.get("job", "?"), m.get("host") or m.get("instance", "?"))
        out[key] = [(float(t), float(v)) for t, v in s["values"]]
    return out


def periodic(times_ms):
    """'every Pms' when one gap covers >= 40% of >= 3 gaps, else ''."""
    gaps = [round(b - a, -2) for a, b in zip(times_ms, times_ms[1:])]
    if len(gaps) < 3:
        return ""
    period, n = collections.Counter(gaps).most_common(1)[0]
    return f"every {period:,.0f}ms" if n / len(gaps) >= 0.4 else ""


def job_timeouts(config_yaml):
    """{job: timeout seconds} from /api/v1/status/config, which lists the
    effective scrape_timeout under every job (a job whose interval is below
    the global timeout gets its interval)."""
    out, job = {}, None
    for line in config_yaml.splitlines():
        t = line.strip()
        if t.startswith("- job_name:") or t.startswith("job_name:"):
            job = t.split(":", 1)[1].strip()
        elif t.startswith("scrape_timeout:") and job is not None:
            v = t.split(":", 1)[1].strip()
            try:
                out[job] = float(v[:-2]) / 1000 if v.endswith("ms") else float(v.rstrip("s"))
            except ValueError:
                pass
    return out


def analyse(up, duration, timeouts):
    """Per target: failures, how many look like timeouts, spacing."""
    report = []
    for key in sorted(up):
        timeout_s = timeouts.get(key[0], 2.0)
        down = [t for t, v in up[key] if v == 0]
        dur = dict(duration.get(key, []))
        slow = sum(1 for t in down if dur.get(t, 0) >= 0.9 * timeout_s)
        report.append({
            "job": key[0], "target": key[1], "timeout_s": timeout_s,
            "scrapes": len(up[key]), "failed": len(down),
            "timeouts": slow, "fast_failures": len(down) - slow,
            "spacing": periodic([t * 1000 for t in down]), "down_at": down,
            "max_ok_duration_s": max((d for t, d in duration.get(key, []) if t not in down), default=0.0),
        })
    return report


def main():
    args = sys.argv[1:]
    tsv = None
    if "--tsv" in args:
        i = args.index("--tsv")
        tsv = args[i + 1]
        del args[i:i + 2]
    if len(args) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    prom, start, end = args[0], float(args[1]), float(args[2])
    jobs = args[3] if len(args) > 3 else "redpanda.*"
    try:
        up = raw(prom, "up", jobs, start, end)
        duration = raw(prom, "scrape_duration_seconds", jobs, start, end)
        cfg = json.load(urllib.request.urlopen(prom.rstrip("/") + "/api/v1/status/config", timeout=30))
    except Exception as e:  # noqa: BLE001 - a report must not fail a run
        print(f"scrape check could not query {prom}: {e}", file=sys.stderr)
        return 2
    report = analyse(up, duration, job_timeouts(cfg.get("data", {}).get("yaml", "")))
    total = sum(r["failed"] for r in report)
    mins = (end - start) / 60
    print(f"scrape check over {mins:.0f} min, jobs {jobs}: {total} failed scrapes "
          f"across {sum(1 for r in report if r['failed'])} of {len(report)} targets")
    for r in report:
        if not r["failed"]:
            continue
        kind = (f"{r['timeouts']} timed out (>= 90% of {r['timeout_s']:g}s), "
                f"{r['fast_failures']} failed fast")
        print(f"  {r['job']:18s} {r['target']:10s} {r['failed']:3d}/{r['scrapes']} failed: {kind}"
              f"{'; ' + r['spacing'] if r['spacing'] else ''}; slowest ok scrape {r['max_ok_duration_s'] * 1000:.0f}ms")
    if tsv:
        with open(tsv, "w") as f:
            f.write("job\ttarget\tdown_at_unix\n")
            for r in report:
                for t in r["down_at"]:
                    f.write(f"{r['job']}\t{r['target']}\t{t:.3f}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
