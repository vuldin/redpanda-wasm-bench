#!/usr/bin/env python3
"""Write a run's phases and measured windows as Prometheus series (OpenMetrics).

A metrics bundle is a recording: whatever is not a series in its TSDB cannot be
drawn, and Grafana annotations live in Grafana's own database, which a bundle
does not carry. So the run's structure goes in as two series the dashboards
already draw as shaded regions:

  bench_phase_info{plan, phase, attempt, outcome} 1    from <run-dir>/autorun.log
  bench_window_info{window} 1                          from the export's windows

one sample every STEP seconds while each is in progress. The live marker
autorun writes on the monitoring host (scripts/autorun.sh phase_marker) is the
same series for a cluster that has it; this makes every bundle carry phases,
including runs whose cluster predates the marker. Feed the output to
`promtool tsdb create-blocks-from openmetrics`.

  scripts/bench-series.py <run-dir> [--windows FILE] [--step 5]

FILE has one window per line, "<label> <start_ms> <end_ms>" (what
metrics-windows.py prints); without it, windows are read from
<run-dir>/metrics-bundle/manifest.json when there is one.
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

LINE = re.compile(r"^\[(\S+)\] (START|OK|FAIL) (\S+)(?: attempt (\d+))?")


def epoch(ts):
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


def phases(log_text):
    """[(phase, attempt, start_s, end_s, outcome)]; a phase with no end is dropped."""
    out, open_ = [], {}
    for line in log_text.splitlines():
        m = LINE.match(line)
        if not m:
            continue
        ts, kind, name, attempt = m[1], m[2], m[3], m[4]
        if kind == "START":
            open_[name] = (int(attempt or 1), epoch(ts))
        elif name in open_:
            a, t0 = open_.pop(name)
            out.append((name, a, t0, epoch(ts), "ok" if kind == "OK" else "failed"))
    return sorted(out, key=lambda p: p[2])


def windows_from(path):
    out = []
    for line in Path(path).read_text().splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[1].isdigit() and parts[2].isdigit():
            out.append((parts[0], int(parts[1]) / 1000, int(parts[2]) / 1000))
    return out


def windows_from_manifest(run_dir):
    m = run_dir / "metrics-bundle" / "manifest.json"
    if not m.exists():
        return []
    return [(w["label"], w["start_ms"] / 1000, w["end_ms"] / 1000)
            for w in json.loads(m.read_text()).get("windows", [])]


def esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"')


def series(name, labels, start, end, step):
    """OpenMetrics sample lines for one constant-1 series over [start, end]."""
    lab = ",".join(f'{k}="{esc(v)}"' for k, v in labels.items())
    t, out = start, []
    while t <= end:
        out.append(f"{name}{{{lab}}} 1 {t:.3f}")
        t += step
    return out


def render(run_dir, windows, step=5.0):
    plan = (run_dir.name.split("-", 5) + [""])[5] or "unknown"
    log = run_dir / "autorun.log"
    lines = []
    ps = phases(log.read_text()) if log.exists() else []
    if ps:
        lines.append("# TYPE bench_phase_info gauge")
        lines.append("# HELP bench_phase_info Autorun phase in progress (from autorun.log).")
        for name, attempt, t0, t1, outcome in ps:
            lines += series("bench_phase_info", {"plan": plan, "phase": name, "attempt": attempt,
                                                 "outcome": outcome, "source": "autorun-log"}, t0, t1, step)
    if windows:
        lines.append("# TYPE bench_window_info gauge")
        lines.append("# HELP bench_window_info Measured window captured by the metrics export.")
        for label, t0, t1 in windows:
            lines += series("bench_window_info", {"window": label}, t0, t1, step)
    lines.append("# EOF")
    return "\n".join(lines) + "\n", len(ps), len(windows)


def main():
    args = sys.argv[1:]
    step, wfile = 5.0, None
    if "--step" in args:
        i = args.index("--step"); step = float(args[i + 1]); del args[i:i + 2]
    if "--windows" in args:
        i = args.index("--windows"); wfile = args[i + 1]; del args[i:i + 2]
    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    run_dir = Path(args[0]).resolve()
    windows = windows_from(wfile) if wfile else windows_from_manifest(run_dir)
    text, np, nw = render(run_dir, windows, step)
    sys.stdout.write(text)
    print(f"bench-series: {np} phase attempts, {nw} windows", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
