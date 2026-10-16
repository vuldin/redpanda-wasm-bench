#!/usr/bin/env python3
"""Emit the measured windows of a run, as `label start_ms end_ms` lines.

PREFERRED SOURCE IS PROMETHEUS, not the reports. With --prometheus, the windows
are the periods when the `loadgen` scrape target was UP, which is exactly when
a measurement was in flight: loadgen is a per-window batch process, so its
target exists only while it runs. Measured 2026-09-17 against a 3-trial run:
three up-periods of 65-70s, matching the configured warmup+duration+drain of
70s to within a scrape interval.

The report-derived fallback below is WRONG BY ABOUT 150 SECONDS and is only
used when no loadgen signal exists. run_id is when the RUNNER started, not when
measuring started, and between those two instants the runner creates topics,
deploys 500 consumers and runs a deliverability precheck. Anchoring on run_id
therefore exports the deploy phase. That is not a hypothetical: the first real
export did it, capturing 12-17s of each 95s traffic period, and every latency
panel in the bundle was blank.

This is what makes "the metrics during the tests" a precise statement rather
than an approximate one. A reproduction is ~90 minutes of wall clock containing
~10 minutes of measurement; the rest is provisioning, 500 consumers being
deployed, topic cleanup and settle time.

The windows come from the reports rather than from wall clock, so an export run
hours later still covers exactly what was measured.

Each loadgen report carries the run id it belongs to and the warmup, duration
and drain it was configured with. The measured span is warmup+duration, and
drain is included because receipts are still arriving during it - a window
clipped before the drain cuts the tail off the very distribution being
measured.

Usage: metrics-windows.py <results-dir> [--pad SECONDS] [--prometheus URL]
"""
import argparse
import json
import re
import sys
from pathlib import Path


def dur_to_secs(v):
    """Go-style duration ("30s", "1m30s", "500ms") or a bare number of seconds."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        pass
    total, seen = 0.0, False
    for num, unit in re.findall(r"([0-9.]+)\s*(ms|s|m|h)", s):
        seen = True
        n = float(num)
        total += {"ms": n / 1000, "s": n, "m": n * 60, "h": n * 3600}[unit]
    return total if seen else None


def window_for(path, d):
    """(label, start_epoch, end_epoch) for one report, or None.

    run_id is the runner's start instant in unix seconds and is the only
    absolute clock reference in the report, so everything is anchored to it.
    Reports whose timestamps cannot be established are skipped loudly rather
    than guessed at: a window in the wrong place produces a bundle full of
    flat lines, which reads as "the cluster was idle" instead of "the export
    was wrong".
    """
    cfg = d.get("config", {}) or {}
    label = d.get("label") or path.stem

    started = d.get("started_at_unix") or d.get("start_unix") or cfg.get("run_id")
    if not started:
        # loadgen's own run_id is its start time in Unix NANOseconds. The
        # durability runner's reports depend on it: they live in a flat OUT
        # directory (kill-held-down/...), so there is no epoch in the parent
        # name, and without this their windows exported as unlabelled
        # "windowNN" (2026-10-15).
        rid = d.get("run_id")
        if isinstance(rid, (int, float)) and rid > 1e17:
            started = rid / 1e9
    if not started:
        for name in (path.parent.name, path.stem, str(label)):
            m = re.search(r"-(\d{10})(?:\D|$)", str(name))
            if m:
                started = int(m.group(1))
                break
    if not started:
        return None

    warmup = dur_to_secs(cfg.get("warmup")) or 0.0
    duration = dur_to_secs(cfg.get("duration")) or 0.0
    drain = dur_to_secs(cfg.get("drain")) or 0.0
    span = warmup + duration + drain
    if span <= 0:
        return None
    return label, float(started), float(started) + span


def loadgen_windows(prom, lo, hi, step=5):
    """Periods where the loadgen scrape target was up, within [lo, hi] seconds.

    This is the authoritative window signal. loadgen runs for exactly one
    measurement and its Prometheus target is up only while it does, so the
    up-periods ARE the measured windows - no arithmetic on configured durations
    and no assumption about how long setup took.
    """
    import urllib.parse
    import urllib.request
    q = urllib.parse.urlencode({
        "query": 'max(up{job="loadgen"})',
        "start": str(int(lo)), "end": str(int(hi)), "step": str(step),
    }).encode()
    req = urllib.request.Request(
        prom.rstrip("/") + "/api/v1/query_range", data=q)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            res = json.load(r)["data"]["result"]
    except Exception as e:
        print(f"metrics-windows: could not query {prom}: {e}", file=sys.stderr)
        return []
    if not res:
        return []
    runs, cur = [], None
    for ts, val in res[0]["values"]:
        t = float(ts)
        if val == "1":
            if cur is None:
                cur = [t, t]
            else:
                cur[1] = t
        elif cur is not None:
            runs.append(cur)
            cur = None
    if cur is not None:
        runs.append(cur)
    # A scrape gap inside a window would split it; stitch anything closer than
    # a few steps back together.
    merged = []
    for a, b in runs:
        if merged and a - merged[-1][1] <= step * 4:
            merged[-1][1] = b
        else:
            merged.append([a, b])
    return [(a, b) for a, b in merged if b - a >= 5]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    # 30s, not 60s. The measured span here is warmup+duration+drain = 70s at
    # the published settings, so 60s either side nearly triples each window for
    # shoulders nobody reads. Measured 2026-09-16: pad=60 turned 7 minutes of
    # measurement into 19 minutes of export.
    ap.add_argument("--pad", type=float, default=30.0)
    ap.add_argument("--prometheus", default="",
                    help="query loadgen target uptime for the real windows "
                         "instead of deriving them from run_id")
    a = ap.parse_args()

    root = Path(a.results)
    wins, skipped = [], []
    # cmd/gateways consumer reports carry their own absolute start/end, so
    # they need neither run_id arithmetic nor the loadgen target (which a
    # gateways run never starts). Kept apart from the loadgen windows so the
    # Prometheus replacement below cannot drop them.
    gw_wins = []
    for p in sorted(root.rglob("*.consume.json")):
        try:
            d = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if "arm" in d and d.get("start_unix_ms") and d.get("end_unix_ms"):
            gw_wins.append((p.name[:-len(".consume.json")], d["start_unix_ms"] / 1000, d["end_unix_ms"] / 1000))
    for p in sorted(root.rglob("*.json")):
        try:
            d = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if "total" not in d or "config" not in d:
            continue  # not a loadgen report
        lbl = d.get("label", "")
        if lbl.startswith("precheck") or lbl == "precheck":
            continue  # the 10-order deliverability probe, not a measurement
        w = window_for(p, d)
        if w is None:
            skipped.append(str(p.relative_to(root)))
            continue
        wins.append(w)

    # Prometheus wins when available. The reports still supply the SEARCH
    # RANGE, so the query does not have to scan the whole retention.
    if a.prometheus and wins:
        lo = min(s for _, s, _ in wins) - 300
        hi = max(e for _, _, e in wins) + 1800
        live = loadgen_windows(a.prometheus, lo, hi)
        if live:
            print(f"metrics-windows: using {len(live)} loadgen up-period(s) "
                  f"from {a.prometheus} rather than run_id arithmetic",
                  file=sys.stderr)
            labels = [l for l, _, _ in wins]
            wins = []
            for i, (s0, e0) in enumerate(live):
                lbl = labels[i] if i < len(labels) else f"window{i + 1}"
                wins.append((lbl, s0, e0))
        else:
            print("metrics-windows: WARNING - no loadgen target uptime found. "
                  "Falling back to run_id arithmetic, which anchors on when "
                  "the RUNNER started rather than when measuring started and "
                  "is wrong by however long setup took (about 150s for a "
                  "500-consumer fan-out level). The exported windows may miss "
                  "the traffic entirely.", file=sys.stderr)

    wins += gw_wins

    if skipped:
        print(f"metrics-windows: {len(skipped)} report(s) had no usable "
              f"timestamp and were skipped:", file=sys.stderr)
        for s in skipped[:5]:
            print(f"  {s}", file=sys.stderr)

    # Merge overlaps. Trials of the same level run back to back and their
    # padding overlaps; two blocks covering the same instants would be
    # rejected by the importer as out-of-order, and separately would store the
    # same samples twice.
    padded = sorted(((s - a.pad, e + a.pad, l) for l, s, e in wins))
    merged = []
    for s, e, l in padded:
        if merged and s <= merged[-1][1]:
            prev_s, prev_e, prev_l = merged[-1]
            merged[-1] = (prev_s, max(prev_e, e), f"{prev_l}+{l}")
        else:
            merged.append((s, e, l))

    # Trials of one level all carry the same label (fanout-500), so a bare
    # label is not a unique window name. Number them in time order, which also
    # makes the manifest read as the run's actual sequence.
    width = len(str(len(merged)))
    for i, (s, e, l) in enumerate(merged, start=1):
        safe = re.sub(r"[^A-Za-z0-9._+-]", "_", l)[:52]
        print(f"{i:0{width}d}-{safe} {int(s * 1000)} {int(e * 1000)}")


if __name__ == "__main__":
    sys.exit(main())
