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


def spread(vals):
    """Across-trial range as "median (lo-hi)".

    A bare median hides the thing a jitter claim most needs disclosed: the
    2026-09-16 in-broker arm came in at 739 / 986 / 751 µs at p50, a 34%
    spread between the fastest and slowest trial, and the published table
    showed one number. METHODOLOGY #250-262 already requires reporting a
    range; this is where that becomes automatic rather than remembered.
    """
    if not vals:
        return "—"
    m = statistics.median(vals)
    if len(vals) == 1:
        return f"{m:,.0f}"
    lo, hi = min(vals), max(vals)
    return f"{m:,.0f} ({lo:,.0f}-{hi:,.0f})"


def tail_row(label, dicts, stage):
    """One tail row: p99, p99.9, p99.99, max, stddev, and how many samples
    carried it - because a p99.9 drawn from 3,000 samples is three samples
    with a line through them, not a measurement."""
    # .get throughout: reports written before stddev/p9999 existed must still
    # render, since reproducing a published figure means reading its original
    # JSON rather than a re-run of it.
    def g(key):
        vals = [d[stage].get(key) for d in dicts if stage in d]
        return med([v for v in vals if v is not None])

    samples = med([d[stage].get("samples", 0) for d in dicts if stage in d])
    p9999 = g("p9999_micros")
    # loadgen suppresses p99.99 below 10^5 samples rather than publishing an
    # interpolation; say so instead of printing a zero.
    p9999_txt = fmt(p9999) if p9999 else "n/a"
    over1ms = med([
        d[stage].get("over_micros", {}).get("1000", 0)
        for d in dicts if stage in d
    ])
    sd = g("stddev_micros")
    return (
        f"| {label} | {fmt(g('p99_micros'))} | {fmt(g('p999_micros'))} | "
        f"{p9999_txt} | {fmt(g('max_micros'))} | "
        f"{fmt(sd) if sd is not None else 'n/a'} | "
        f"{(over1ms or 0):,.0f} | {(samples or 0):,.0f} |"
    )


def print_tail_table(title, dicts, stages, sample_unit="envelope"):
    print(f"\n### {title} - tail and spread\n")
    print(f"Latency sample unit: **{sample_unit}**. One record earns one send "
          f"instant, one match stamp and one receipt, so a sample covers every "
          f"order the record carried - per-order latency does not exist here.\n")
    print("| stage | p99 | p99.9 | p99.99 | max | stddev | >1ms | samples |")
    print("|---|---|---|---|---|---|---|---|")
    for stage, label in stages:
        print(tail_row(label, dicts, stage))
    if dicts:
        n = med([d.get("config", {}).get("orders_per_record", 1) for d in dicts])
        if n and n > 1:
            print(f"\nEach sample represents {n:,.0f} orders, so the sample "
                  f"count is {n:,.0f}x lower than the order count. p99.9 needs "
                  f"~10^5 samples to mean anything; check the samples column "
                  f"before quoting a tail figure.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results_dir")
    ap.add_argument("--inbroker-consumers", type=int, default=500)
    ap.add_argument("--matcher-md5", default="")
    ap.add_argument("--orders-per-record", type=int, default=None,
                    help="only include reports at this envelope size. Envelope "
                         "size is a DIMENSION, not a trial: medianing N=15 and "
                         "N=30 together yields a figure describing no run that "
                         "was ever performed. Required when a directory holds "
                         "more than one size; render once per size.")
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

    # Three arms, not two. The co-located arm (label trad-loopback-*) is a
    # DIFFERENT experiment from the external one (trad-external-*): only the
    # matcher's location changes, which is the whole point. Bucketing them
    # together would median them into one meaningless number at the same
    # consumer count, which is precisely the silent conflation this split
    # exists to prevent.
    # Filter to one envelope size before anything is aggregated, and REFUSE
    # rather than silently mix. This directory held three N=15 runs and three
    # N=30 runs on 2026-10-15 and the tables reported "6 clean trial(s)" with
    # "each sample represents 22 orders" - 22 being the median of 15 and 30,
    # i.e. a number describing nothing that ran.
    sizes = sorted({d.get("config", {}).get("orders_per_record", 1)
                    for _, d in reports})
    if a.orders_per_record is not None:
        reports = [(p, d) for p, d in reports
                   if d.get("config", {}).get("orders_per_record", 1) == a.orders_per_record]
        if not reports:
            print(f"no reports at orders_per_record={a.orders_per_record} "
                  f"(found: {sizes})", file=sys.stderr)
            return 1
        print(f"<!-- orders_per_record={a.orders_per_record} -->")
    elif len(sizes) > 1:
        print(f"ERROR: this directory holds several envelope sizes ({sizes}).\n"
              f"       Medianing them together would report a latency for a\n"
              f"       configuration that was never run. Re-run once per size:\n"
              + "".join(f"         --orders-per-record {n}\n" for n in sizes),
              file=sys.stderr)
        return 1

    inbroker, traditional, loopback, rejected = [], [], [], []
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
        low = lbl.lower()
        if "loopback" in low:
            loopback.append(d)
        elif "trad" in low:
            traditional.append(d)
        else:
            inbroker.append(d)


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
        print("| stage | p50 (range across trials) | p90 | p99 |")
        print("|---|---|---|---|")
        for stage, label in (("produce", "produce (client → quorum ack)"),
                             ("relay_consume", "relay_consume (matcher stamp → receipt)"),
                             ("total", "**total — this is e2e**")):
            row = {q: med([d[stage][f"p{q}_micros"] for d in inbroker if stage in d])
                   for q in (50, 90, 99)}
            if stage == "total":
                ib = row
            # p50 carries its across-trial range: a single median hides a 34%
            # trial-to-trial spread, which is exactly what a tail claim must
            # not do.
            p50s = [d[stage]["p50_micros"] for d in inbroker if stage in d]
            print(f"| {label} | {spread(p50s)} µs | {fmt(row[90])} | {fmt(row[99])} |")
        print("\n`match` is omitted on purpose: it is a broker-side stamp minus a "
              "client-side ack, so it is legitimately negative and is not a duration.\n")
        print_tail_table(
            f"In-broker wasm, {a.inbroker_consumers} consumers",
            inbroker,
            (("produce", "produce → quorum ack"),
             ("relay_consume", "relay_consume"),
             ("total", "**total — e2e**")),
            sample_unit=(inbroker[0].get("config", {}).get("latency_sample_unit")
                         or "envelope"),
        )

    if traditional:
        print("## Traditional deployment\n")
        print("| consumers | e2e p50 (range) | e2e p90 | e2e p99 | e2e p99.9 | produce p50 |")
        print("|---|---|---|---|---|---|")
        by_level = {}
        for d in traditional:
            by_level.setdefault(consumers(d), []).append(d)
        for lvl in sorted(by_level):
            ds = by_level[lvl]
            t = {q: med([x["total"][f"p{q}_micros"] for x in ds]) for q in (50, 90, 99)}
            t999 = med([x["total"]["p999_micros"] for x in ds])
            pr = med([x["produce"]["p50_micros"] for x in ds])
            p50s = [x["total"]["p50_micros"] for x in ds]
            print(f"| {lvl} external | {spread(p50s)} µs | {fmt(t[90])} | "
                  f"{fmt(t[99])} | {fmt(t999)} | {fmt(pr)} |")

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
            print("\nQuote the conclusion, not the constant: this pairing has "
                  "spanned ~9.4x to ~19.1x across cluster instantiations, "
                  "because at high external consumer counts the broker's fetch "
                  "path is near saturation and that regime is not stable "
                  "between clusters. 'Roughly an order of magnitude at "
                  f"{peak} consumers' is what holds.")
        print_tail_table(
            "Traditional deployment",
            traditional,
            (("produce", "produce → quorum ack"),
             ("total", "**total — e2e**")),
            sample_unit=(traditional[0].get("config", {}).get("latency_sample_unit")
                         or "envelope"),
        )

    if loopback:
        print("\n## Co-located matcher, over loopback\n")
        print("The co-location alternative: the same external matcher, "
              "on a broker host, dialling that broker's OWN address - which "
              "Linux routes via `lo` (MTU 65536), never the NIC. Not "
              "127.0.0.1: Redpanda binds kafka_api to the configured private "
              "address only, so loopback is refused. Only the matcher's "
              "location differs from the traditional arm above. A local "
              "user->kernel->user roundtrip is expected to be well under 50us.\n")
        print("A row whose p90/p99 are orders of magnitude above its own p50 is "
              "a startup or rebalance artifact in that level, not a tail - the "
              "p50 may still be usable where the tails are not. Check it "
              "against the neighbouring fan-out level before quoting either.\n")
        print("| consumers | e2e p50 (range) | e2e p90 | e2e p99 | e2e p99.9 | produce p50 |")
        print("|---|---|---|---|---|---|")
        lb_level = {}
        for d in loopback:
            lb_level.setdefault(consumers(d), []).append(d)
        for lvl in sorted(lb_level):
            ds = lb_level[lvl]
            t = {q: med([x["total"][f"p{q}_micros"] for x in ds]) for q in (50, 90, 99)}
            t999 = med([x["total"]["p999_micros"] for x in ds])
            pr = med([x["produce"]["p50_micros"] for x in ds])
            p50s = [x["total"]["p50_micros"] for x in ds]
            print(f"| {lvl} external | {spread(p50s)} µs | {fmt(t[90])} | "
                  f"{fmt(t[99])} | {fmt(t999)} | {fmt(pr)} |")
        print("\nRead this against BOTH other arms. If it sits much closer to "
              "in-broker than to traditional, co-location rather than in-broker "
              "execution is carrying most of the win, and a Unix-socket "
              "listener would not change that conclusion - loopback's 64 KiB "
              "MTU already removes most packetisation cost.")

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
