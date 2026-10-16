#!/usr/bin/env python3
"""Draw a run's autorun phases on the wasm-bench dashboards as Grafana annotations.

A cluster deployed by the current bench needs none of this: autorun writes
bench_phase_info on the monitoring host and the dashboards draw it (see
docs/MONITORING.md, "Which phase is running"). This is for everything else -
a run whose cluster was deployed before the marker existed, or an exported
bundle of such a run opened in its local Grafana. It reads the START / OK /
FAIL lines in <run-dir>/autorun.log and posts one region annotation per phase
attempt to each wasm-bench dashboard. A phase still running is drawn up to
now. Re-running replaces this run's annotations rather than adding duplicates,
and --follow does that every N seconds (default 30) until the run's DONE file
appears, so a live run's current phase is shaded while it runs.

  scripts/annotate-phases.py <run-dir> [grafana-url] [--follow [seconds]]

The Grafana URL defaults to the one in <run-dir>/MONITORING.txt. Credentials
come from GRAFANA_AUTH (user:password), default the bench's admin/wasmbench.
"""
import base64
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DASHBOARDS = ["wasm-bench-overview", "wasm-bench-relay-stages", "wasm-bench-health"]
TAG = "wasm-bench-phase"
LINE = re.compile(r"^\[(\S+)\] (START|OK|FAIL) (\S+)(?: attempt (\d+))?")


def epoch_ms(ts):
    return int(datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
               .replace(tzinfo=timezone.utc).timestamp() * 1000)


def phases(log_text, now_ms=None):
    """[(phase, attempt, start_ms, end_ms, outcome)] from autorun.log text."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    out, open_ = [], {}
    for line in log_text.splitlines():
        m = LINE.match(line)
        if not m:
            continue
        ts, kind, name, attempt = m[1], m[2], m[3], m[4]
        if kind == "START":
            open_[name] = (int(attempt or 1), epoch_ms(ts))
        elif name in open_:
            a, t0 = open_.pop(name)
            out.append((name, a, t0, epoch_ms(ts), "ok" if kind == "OK" else "failed"))
    for name, (a, t0) in open_.items():
        out.append((name, a, t0, now_ms, "running"))
    return sorted(out, key=lambda p: p[2])


def grafana_from(run_dir):
    txt = (run_dir / "MONITORING.txt").read_text() if (run_dir / "MONITORING.txt").exists() else ""
    m = re.search(r"Grafana\s+(http\S+)", txt)
    return m[1] if m else None


def call(base, auth, method, path, body=None):
    req = urllib.request.Request(
        base.rstrip("/") + path, method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": "Basic " + base64.b64encode(auth.encode()).decode()})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
    return json.loads(raw) if raw else None


def main():
    args = sys.argv[1:]
    follow = None
    if "--follow" in args:
        i = args.index("--follow")
        follow = 30
        if i + 1 < len(args) and args[i + 1].isdigit():
            follow = int(args.pop(i + 1))
        args.pop(i)
    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    if follow is None:
        return draw(args)
    run_dir = Path(args[0])
    while True:
        try:
            draw(args)
        except Exception as e:  # noqa: BLE001 - a Grafana blip must not end the watch
            print(f"annotate-phases: {e}; retrying in {follow}s", file=sys.stderr)
        if (run_dir / "DONE").exists():
            return draw(args)
        time.sleep(follow)


def draw(args):
    run_dir = Path(args[0])
    base = args[1] if len(args) > 1 else grafana_from(run_dir)
    if not base:
        print(f"no Grafana URL given and none in {run_dir}/MONITORING.txt", file=sys.stderr)
        return 2
    auth = os.environ.get("GRAFANA_AUTH", "admin:wasmbench")
    run_tag = "run:" + run_dir.resolve().name
    plan = (run_dir.resolve().name.split("-", 5) + [""])[5] or "?"
    ps = phases((run_dir / "autorun.log").read_text())
    if not ps:
        print(f"no phases in {run_dir}/autorun.log yet")
        return 0

    # Replace, never accumulate: drop this run's earlier annotations first.
    q = urllib.parse.urlencode([("tags", TAG), ("tags", run_tag), ("matchAny", "false"), ("limit", "5000")])
    for a in call(base, auth, "GET", f"/api/annotations?{q}") or []:
        call(base, auth, "DELETE", f"/api/annotations/{a['id']}")

    n = 0
    for uid in DASHBOARDS:
        try:
            call(base, auth, "GET", f"/api/dashboards/uid/{uid}")
        except urllib.error.HTTPError:
            continue  # not on this Grafana
        for name, attempt, t0, t1, outcome in ps:
            call(base, auth, "POST", "/api/annotations", {
                "dashboardUID": uid, "time": t0, "timeEnd": t1,
                "tags": [TAG, run_tag, f"phase:{name}", outcome],
                "text": f"{name} (attempt {attempt}, {outcome}) - plan {plan}"})
            n += 1
    print(f"{len(ps)} phase attempts drawn on {n // max(len(ps), 1)} dashboards at {base}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
