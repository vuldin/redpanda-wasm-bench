#!/usr/bin/env python3
"""Emit the measured windows of a run, as `label start_ms end_ms` lines.

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

Usage: metrics-windows.py <results-dir> [--pad SECONDS]
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
        m = re.search(r"-(\d{10})(?:\D|$)", str(path.parent.name))
        started = int(m.group(1)) if m else None
    if not started:
        return None

    warmup = dur_to_secs(cfg.get("warmup")) or 0.0
    duration = dur_to_secs(cfg.get("duration")) or 0.0
    drain = dur_to_secs(cfg.get("drain")) or 0.0
    span = warmup + duration + drain
    if span <= 0:
        return None
    return label, float(started), float(started) + span


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    # 30s, not 60s. The measured span here is warmup+duration+drain = 70s at
    # the published settings, so 60s either side nearly triples each window for
    # shoulders nobody reads. Measured 2026-09-16: pad=60 turned 7 minutes of
    # measurement into 19 minutes of export.
    ap.add_argument("--pad", type=float, default=30.0)
    a = ap.parse_args()

    root = Path(a.results)
    wins, skipped = [], []
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
