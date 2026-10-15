#!/usr/bin/env python3
"""Turn a run directory into RESULTS.md: the tables, the caveats, the pointers.

    scripts/summarize-run.py results/run-<...>  > results/run-<...>/RESULTS.md

scripts/autorun.sh runs this at the end of every plan, before teardown, so a
finished run always carries a readable summary next to its raw files. It is
mechanical on purpose - it reports what the runners wrote and flags what they
said not to trust (unclean levels, trials whose leadership never moved, traffic
that did not span the action). Interpretation stays with the reader.

Reads:
  phases.tsv, PLAN, MONITORING.txt     written by autorun.sh
  client-results/**/summary.txt        e2 sweeps (run-e2-throughput.sh), any MODE
  */summary.txt                        durability series (run-durability.sh)
  metrics-bundle/                      wb monitor export, if present
"""
import glob
import os
import re
import sys

R = sys.argv[1].rstrip("/")
out = []
w = out.append


def read(p, default=""):
    try:
        return open(p).read()
    except OSError:
        return default


# ---------------------------------------------------------------------------
w(f"# Results: {os.path.basename(R)}\n")
plan = read(f"{R}/PLAN").strip()
w(f"- plan: `{plan or 'unknown'}`")
gate = [l.split("\t") for l in read(f"{R}/phases.tsv").splitlines() if "\tgate\t" in l]
if gate:
    w(f"- commits: `{gate[0][-1]}`")
td = read(f"{R}/TEARDOWN-RESULT").strip()
w(f"- teardown: {td or 'not run (yet)'}")
mon = read(f"{R}/MONITORING.txt").strip()
if os.path.isdir(f"{R}/metrics-bundle"):
    w(f"- metrics: exported to `metrics-bundle/` - open with `cd metrics-bundle && docker compose up`, then http://localhost:3000")
elif mon:
    w("- metrics: NOT exported (the monitoring host held the only copy)")
w("")

phases = [l.split("\t") for l in read(f"{R}/phases.tsv").splitlines() if l.strip()]
if phases:
    w("## Phases\n")
    w("| phase | outcome | est min | seconds |")
    w("|---|---|---|---|")
    for p in phases:
        if len(p) >= 5 and p[1] != "gate":
            w(f"| {p[1]} | {p[2]} | {p[3]} | {p[4]} |")
    w("")

# ---------------------------------------------------------------------------
# e2 sweeps: whatever MODE, the runner writes a fingerprint and a header row.
WANT = ["level", "offered/s", "achieved/s", "total_p50us", "total_p999us", "relay_p99us",
        "match_us/order", "records/s", "clean"]
e2 = sorted(glob.glob(f"{R}/client-results/*/summary.txt"))
if e2:
    w("## Steady-state sweeps (run e2)\n")
    w("Latency columns are loadgen's own histograms (microseconds). A level that is "
      "not clean is a backlog or a delivery failure, not a latency; see the runner's "
      "per-level report for the reason.\n")
for s in e2:
    txt = read(s)
    fp = [l.lstrip("# ").strip() for l in txt.splitlines() if l.startswith("#")]
    hdr = next((l.split() for l in txt.splitlines() if l.startswith("level ")), None)
    rows = [l.split() for l in txt.splitlines()
            if hdr and l and not l.startswith("#") and not l.startswith("level ") and len(l.split()) >= len(hdr) - 2]
    w(f"### `{os.path.basename(os.path.dirname(s))}`\n")
    keyfp = [f for f in fp if re.match(r"(fingerprint|rate=|orders_per_record|cancel_fraction)", f)]
    for f in keyfp:
        w(f"- `{f}`")
    if not hdr or not rows:
        w("\n_no completed levels (the runner failed before measuring - see the phase log)_\n")
        continue
    cols = [c for c in WANT if c in hdr]
    w("")
    w("| " + " | ".join(cols) + " |")
    w("|" + "---|" * len(cols))
    for r in rows:
        cells = []
        for c in cols:
            i = hdr.index(c)
            v = r[i] if i < len(r) else "-"
            try:
                f = float(v)
                if c == "level" or f == int(f) and abs(f) >= 100:
                    v = f"{f:,.0f}"
                else:
                    v = f"{f:,.0f}" if abs(f) >= 100 else f"{f:.2f}"
            except ValueError:
                pass
            cells.append(v)
        w("| " + " | ".join(cells) + " |")
    w("")

# ---------------------------------------------------------------------------
dur = sorted(d for d in glob.glob(f"{R}/*/summary.txt") if "/client-results/" not in d)
if dur:
    w("## Durability series (run durability)\n")
    w("Sentinels are the ground truth: acknowledged resting orders at a price no other "
      "traffic touches, read back after the action. `gap seqs` is the guest's own "
      "diagnostic (envelope sequences skipped between the restored snapshot and the "
      "resumed offset). Exclude any trial marked as not moved or not spanned.\n")
for d in dur:
    txt = read(d)
    hdr = txt.splitlines()[0].lstrip("# ") if txt else ""
    w(f"### `{os.path.basename(os.path.dirname(d))}`\n\n`{hdr}`\n")
    w("| trial | moved | traffic spans T | failover ms | recovered/placed | strays | waves | restore | gap seqs |")
    w("|---|---|---|---|---|---|---|---|---|")
    for b in re.split(r"\n############ ", txt)[1:]:
        tag = b.split(" ", 1)[0]
        moved = "no - EXCLUDE" if "LEADERSHIP-DID-NOT-MOVE" in b else ("yes" if "leadership moved" in b else "-")
        span = (re.search(r"traffic_spans_T_plus_30s=(\S+)", b) or [None, "-"])[1]
        if span == "NO":
            span = "NO - EXCLUDE"
        fo = re.search(r"kill: transform running on node\d+ (\d+)ms", b) or re.search(r"running again after (\d+)ms", b)
        v = re.search(r"VERDICT \S+ placed=(\d+) recovered=(\d+) lost=(\d+) duplicated=(\d+) strays=(\d+) ok=\S+ waves=(\S+)", b)
        rest = re.search(r"STATE restored .*?orders=(\d+) .*?bytes=(\d+) restore_us=(\d+)", b)
        gap = re.search(r"STATE gap .*?skipped_seqs=(\d+)", b)
        if rest:
            rs = f"{int(rest.group(1)):,} orders, {int(rest.group(2))/1024:,.0f} KiB, {int(rest.group(3))/1000:.1f} ms"
        elif "STATE fresh" in b or "STATE off" in b:
            rs = "none (fresh/off)"
        else:
            rs = "-"
        if v:
            w(f"| {tag} | {moved} | {span} | {fo.group(1) if fo else '-'} | {v.group(2)}/{v.group(1)} | "
              f"{v.group(5)} | {v.group(6)} | {rs} | {gap.group(1) if gap else 0} |")
        else:
            w(f"| {tag} | {moved} | {span} | - | no verdict | | | | |")
    reps = re.findall(r"REPLAY (\S+) history_records=(\d*) deploy_to_running_ms=(-?\d+) deploy_to_caught_up_ms=(-?\d+)", txt)
    rv = dict((a, f"{c}/{b}") for a, b, c, _ in re.findall(r"REPLAY-VERDICT (\S+) placed=(\d+) recovered=(\d+) lost=(\d+)", txt))
    if reps:
        w("\nReplay arm (a fresh snapshot-less deploy from the start of the same input topic):\n")
        w("| after trial | history records | deploy to running ms | deploy to caught up ms | sentinels |")
        w("|---|---|---|---|---|")
        for t, h, a, c in reps:
            w(f"| {t} | {int(h):,} | {a} | {c} | {rv.get(t, '-')} |" if h else f"| {t} | - | {a} | {c} | {rv.get(t, '-')} |")
    w("")

repro = sorted(glob.glob(f"{R}/**/reproduce-*", recursive=True)) + sorted(glob.glob(f"{R}/reproduce"))
if repro:
    w("## Published-result reproduction\n")
    for d in repro:
        w(f"- `{os.path.relpath(d, R)}` - tables as rendered by `scripts/render-tables.py` are in that directory")
    w("")

if mon:
    w("## Monitoring during the run\n")
    w("```")
    w(mon)
    w("```")

print("\n".join(out))
