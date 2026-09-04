#!/usr/bin/env python3
"""Render the two published tables from a results directory.

Reads loadgen report JSONs and prints markdown. Deliberately opinionated about
what it refuses to print:

  - a level whose report is not clean:true is shown as UNCLEAN, never as a
    number. An unclean run is not a slow result, it is no result.
  - a level whose send_lateness p99 exceeds one pacing interval is shown as
    PACING-LOST. That run's latency includes the load generator's own CPU
    starvation, which is indistinguishable from system latency in the p50 but
    is not the system's.
  - `match` is never printed as a duration. It is the difference between a
    broker-side stamp and a client-side ack, so it is legitimately negative and
    quoting it as latency is simply wrong.

Multiple trials of the same level are reduced by MEDIAN, not mean, so one
outlier trial cannot move the headline.
"""
import argparse
import json
import statistics
import sys
from pathlib import Path


# Labels that are not measurements. `precheck` is the 10-order deliverability
# probe every level runs before its timed window; including it in a median is
# meaningless.
NON_MEASUREMENT_LABELS = {"precheck"}


def load_reports(root: Path, want_labels=None):
    """Loadgen reports under root.

    want_labels, when given, is the set of label PREFIXES this run produced.
    Without it a results directory that also holds earlier experiments gets
    aggregated together - the first reproduction run reported "24 clean trials"
    from a rate sweep it had not been asked to run, and produced a median
    across entirely different experiments that still looked like a result.
    """
    out = []
    for p in sorted(root.rglob("*.json")):
        try:
            d = json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if "total" not in d or "config" not in d:
            continue  # not a loadgen report
        lbl = d.get("label", "")
        if lbl in NON_MEASUREMENT_LABELS or lbl.startswith("precheck"):
            continue
        if want_labels and not any(lbl.startswith(w) for w in want_labels):
            continue
        out.append((p, d))
    return out


def pacing_held(d):
    cfg = d.get("config", {})
    if cfg.get("pacing") != "fixed":
        return True  # no per-order schedule to be late against
    rate = cfg.get("offered_rate_orders_per_sec") or 0
    if rate <= 0:
        return True
    interval_us = 1e6 / rate
    return d.get("send_lateness", {}).get("p99_micros", 0) <= interval_us


def consumers(d):
    """How many consumers this level had, from the label or config."""
    lbl = d.get("label", "")
    for tok in lbl.replace("-", " ").split():
        if tok.isdigit():
            return int(tok)
    return d.get("config", {}).get("num_probes", 1)


def med(vals):
    return statistics.median(vals) if vals else None


def fmt(v):
    return f"{v:,.0f} µs" if v is not None else "—"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results_dir")
    ap.add_argument("--inbroker-consumers", type=int, default=500)
    ap.add_argument("--matcher-md5", default="")
    ap.add_argument("--labels", default="",
                    help="comma-separated label prefixes this run produced; "
                         "without it, earlier experiments in the same results "
                         "directory are aggregated in")
    a = ap.parse_args()

    root = Path(a.results_dir)
    want = {w for w in a.labels.split(",") if w} or None
    reports = load_reports(root, want)
    if not reports:
        where = f" matching {sorted(want)}" if want else ""
        print(f"no loadgen reports under {root}{where}", file=sys.stderr)
        return 1
    if want:
        print(f"<!-- {len(reports)} report(s) matching {sorted(want)} -->")

    inbroker, traditional, rejected = [], [], []
    for p, d in reports:
        lbl = d.get("label", p.stem)
        if not d.get("clean", False):
            reasons = d.get("unclean_reasons") or ["clean:false"]
            # A missing -admin-url is the expected, benign asymmetry on the
            # external arm: there is no in-broker transform whose failure
            # counter could be scraped. It is not a data-quality problem.
            benign = all("no -admin-url" in r for r in reasons)
            if not benign:
                rejected.append((lbl, "; ".join(reasons)[:90]))
                continue
        if not pacing_held(d):
            sl = d["send_lateness"]["p99_micros"]
            rejected.append((lbl, f"PACING-LOST: send_lateness p99 {sl:,.0f}µs"))
            continue
        (inbroker if "trad" not in lbl.lower() else traditional).append(d)

    print(f"# Results\n")
    if a.matcher_md5:
        print(f"matcher module: `{a.matcher_md5}`")
        if a.matcher_md5 != "94c3d0177a754b9f0c1df80c10dea298":
            print("**not** the module the published numbers used — this is a "
                  "different experiment\n")
        else:
            print("(matches the published runs)\n")

    ib = {}
    if inbroker:
        print(f"## In-broker wasm, {a.inbroker_consumers} consumers "
              f"({len(inbroker)} clean trial(s), median)\n")
        print("| stage | p50 | p90 | p99 |")
        print("|---|---|---|---|")
        for stage, label in (("produce", "produce (client → quorum ack)"),
                             ("relay_consume", "relay_consume (matcher stamp → receipt)"),
                             ("total", "**total — this is e2e**")):
            row = {q: med([d[stage][f"p{q}_micros"] for d in inbroker if stage in d])
                   for q in (50, 90, 99)}
            if stage == "total":
                ib = row
            print(f"| {label} | {fmt(row[50])} | {fmt(row[90])} | {fmt(row[99])} |")
        print("\n`match` is omitted on purpose: it is a broker-side stamp minus a "
              "client-side ack, so it is legitimately negative and is not a duration.\n")

    if traditional:
        print("## Traditional deployment\n")
        print("| consumers | e2e p50 | e2e p90 | e2e p99 | produce p50 |")
        print("|---|---|---|---|---|")
        by_level = {}
        for d in traditional:
            by_level.setdefault(consumers(d), []).append(d)
        for lvl in sorted(by_level):
            ds = by_level[lvl]
            t = {q: med([x["total"][f"p{q}_micros"] for x in ds]) for q in (50, 90, 99)}
            pr = med([x["produce"]["p50_micros"] for x in ds])
            print(f"| {lvl} external | {fmt(t[50])} | {fmt(t[90])} | {fmt(t[99])} | {fmt(pr)} |")

        peak = max(by_level)
        if ib and peak in by_level:
            tp = {q: med([x["total"][f"p{q}_micros"] for x in by_level[peak]])
                  for q in (50, 90, 99)}
            print(f"\n### Ratio at matched consumer count "
                  f"({a.inbroker_consumers} in-broker vs {peak} external)\n")
            print("| | ratio |")
            print("|---|---|")
            for q in (50, 90, 99):
                if ib.get(q) and tp.get(q):
                    print(f"| p{q} | **{tp[q] / ib[q]:.2f}×** |")

    if rejected:
        print("\n## Rejected levels\n")
        print("Shown rather than hidden — a level excluded silently is how a "
              "partial result gets read as a complete one.\n")
        print("| level | why |")
        print("|---|---|")
        for lbl, why in rejected:
            print(f"| {lbl} | {why} |")

    return 0


if __name__ == "__main__":
    sys.exit(main())
