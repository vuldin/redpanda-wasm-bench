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
import json
import os
import re
import sys

R = sys.argv[1].rstrip("/")
out = []
w = out.append


def p9999(t):
    """loadgen emits p99.99 as 0 below ~10^5 samples rather than restating the
    top few samples; say so instead of printing a 0 that reads as a latency."""
    v = t.get("p9999_micros") or 0
    return f"{v:,.0f}" if v else "n/a (<100k samples)"


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

# A stall is a timeline bucket (loadgen -timeline-ms, 500ms by default) whose
# slowest sample took longer than this. A run of them at a fixed spacing is a
# periodic stall; the 2026-10-16 checkpoint run found its depth-10k tail that
# way, at exactly the 5,000ms guest-state checkpoint interval.
STALL_US = 3000


def stalls(timeline):
    """'N' slow buckets, plus '@ Pms' when their spacing is periodic.

    Periodic means the commonest gap between consecutive slow buckets accounts
    for at least 40% of the gaps, over at least 3 of them."""
    if not timeline:
        return "-"
    slow = [b["offset_ms"] for b in timeline if b.get("max_micros", 0) > STALL_US]
    gaps = [b - a for a, b in zip(slow, slow[1:])]
    if len(gaps) >= 3:
        period, n = max(((g, gaps.count(g)) for g in set(gaps)), key=lambda x: (x[1], -x[0]))
        if n / len(gaps) >= 0.4:
            return f"{len(slow)} @ {period:,}ms"
    return str(len(slow))


# ---------------------------------------------------------------------------
# Architecture comparison (plans/baseline.plan): the same matcher, book and
# order flow in every deployment, side by side per fan-out. In-broker arms are
# e2 fanout sweeps, named by their relay delivery; traditional arms are run
# traditional's placements. Both are loadgen's own end-to-end report, so the
# columns mean the same thing in every row. Sweeps are numbered per arm in the
# order they ran; a phase retried after a failure can add a partial one.
ARM_ORDER = ["in-broker, precommit", "in-broker, committed", "external", "co-located",
             "co-located, isolated cores"]
PLACEMENT = {"external": "external", "loopback": "co-located", "loopback-isolated": "co-located, isolated cores"}


def run_id_of(d):
    m = re.search(r"-(\d{9,})/?$", d)
    return int(m.group(1)) if m else 0


def comparison_rows(run_dir, started):
    """[(fanout, arm, sweep, total, clean)] for this run's in-broker fanout
    sweeps and traditional placements."""
    found = []  # (run_id, fanout, arm, report)
    for s in glob.glob(f"{run_dir}/client-results/*/summary.txt"):
        d = os.path.dirname(s)
        txt = read(s)
        if run_id_of(d) < started - 60 or not re.search(r"MODE=fanout\b", txt) or "consumer_arm=inbroker" not in txt:
            continue
        arm = "in-broker, " + (re.search(r"relay_delivery=(\S+)", txt) or [None, "precommit"])[1]
        for rj in glob.glob(f"{d}/fanout-*.json"):
            m = re.search(r"/fanout-(\d+)\.json$", rj)
            if m:
                try:
                    found.append((run_id_of(d), int(m[1]), arm, json.load(open(rj))))
                except (OSError, ValueError):
                    pass
    for d in glob.glob(f"{run_dir}/client-results/traditional-sharded-*"):
        if run_id_of(d) < started - 60:
            continue
        for rj in glob.glob(f"{d}/trad-*.json"):
            try:
                j = json.load(open(rj))
            except (OSError, ValueError):
                continue
            m = re.match(r"trad-(.+)-(\d+)$", j.get("label", ""))
            if m:
                found.append((run_id_of(d), int(m[2]), PLACEMENT.get(m[1], m[1]), j))
    rows, trial = [], {}
    for rid, n, arm, j in sorted(found, key=lambda f: f[0]):
        trial[(n, arm)] = trial.get((n, arm), 0) + 1
        rows.append((n, arm, trial[(n, arm)], j.get("total") or {}, j.get("clean")))
    return rows


try:
    _started = int(read(f"{R}/RUN_STARTED_AT").strip())
except ValueError:
    _started = 0
cmp_rows = comparison_rows(R, _started)
if len({arm for _, arm, *_ in cmp_rows}) >= 2:
    w("## Architecture comparison\n")
    w("The same matcher (book-matcher), book and order flow in every deployment, per fan-out "
      "(subscribers per order). End to end is loadgen's send to the measured consumer's "
      "receipt in every row. in-broker precommit delivers from the transform before its output "
      "write; committed delivers once the output quorum has it, which is what a Kafka consumer "
      "of the output could fetch, so it is the like-for-like comparison with the traditional "
      "arms. One row per sweep, numbered per arm in the order they ran (a phase retried after a "
      "failure can add a partial sweep). The per-arm sections below have the detail.\n")
    for n in sorted({r[0] for r in cmp_rows}):
        rows = sorted((r for r in cmp_rows if r[0] == n),
                      key=lambda r: (ARM_ORDER.index(r[1]) if r[1] in ARM_ORDER else 99, r[2]))
        ext = [r[3].get("p50_micros", 0) for r in rows if r[1] == "external" and r[3].get("p50_micros")]
        # The median of the external sweeps: with an even count, the mean of
        # the middle two (the upper one alone flattered every other arm by up
        # to ~10% with two trials, 2026-10-17).
        e = sorted(ext)
        ext50 = (e[(len(e) - 1) // 2] + e[len(e) // 2]) / 2 if e else 0
        w(f"### fan-out {n}\n")
        w("| arm | sweep | p50 us | p99 | p99.9 | p99.99 | samples | p50 vs external | clean |")
        w("|---|---|---|---|---|---|---|---|---|")
        for _, arm, t, tot, clean in rows:
            f = lambda k: f"{tot.get(k, 0):,.0f}"
            vs = f"{tot.get('p50_micros', 0) / ext50:.2f}x" if ext50 and tot.get("p50_micros") else "-"
            w(f"| {arm} | {t} | {f('p50_micros')} | {f('p99_micros')} | {f('p999_micros')} | {p9999(tot)} | "
              f"{tot.get('samples', 0):,} | {vs} | {str(clean).lower()} |")
        w("")

# ---------------------------------------------------------------------------
# e2 sweeps: whatever MODE, the runner writes a fingerprint and a header row.
WANT = ["level", "offered/s", "achieved/s", "total_p50us", "total_p999us", "relay_p99us",
        "match_us/order", "records/s", "clean", "leader_changes"]
e2 = sorted(s for s in glob.glob(f"{R}/client-results/*/summary.txt")
            if not os.path.basename(os.path.dirname(s)).startswith(("resilience-", "gateways-")))
# `wb results` copies everything on the client, which on a cluster reused with
# --existing includes earlier runs' sweeps. Keep only this run's: each sweep
# directory ends in its runner's start (unix seconds).
try:
    started = int(read(f"{R}/RUN_STARTED_AT").strip())
except ValueError:
    started = 0
older = [s for s in e2 if (m := re.search(r"-(\d{9,})$", os.path.basename(os.path.dirname(s)))) and int(m.group(1)) < started - 60]
e2 = [s for s in e2 if s not in older]
if e2:
    w("## Steady-state sweeps (run e2)\n")
    if older:
        w(f"_{len(older)} sweep(s) from before this run, also on the client, are not shown._\n")
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
    mode = (re.search(r"MODE=(\S+)", txt) or [None, ""])[1]
    # The two legs the end-to-end tail decomposes into: the client's acks=all
    # produce (replication; the same in every deployment) and the in-broker
    # relay to the consumer (broker clock). Where the e2e p99.99 comes from
    # is the jitter question, so both are reported next to it.
    # stalls>3ms: timeline buckets whose slowest sample exceeded STALL_US, with
    # the period when they recur at a fixed spacing (see stalls()).
    extra = ["p99us", "p9999us", "produce_p9999us", "relay_p9999us", "stalls>3ms", "samples"]
    w("")
    w("| " + " | ".join(cols + extra) + " |")
    w("|" + "---|" * (len(cols) + len(extra)))
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
        rep_json = os.path.join(os.path.dirname(s), f"{mode}-{r[0]}.json")
        try:
            rep = json.load(open(rep_json))
            t = rep.get("total") or {}
            cells += [f"{t.get('p99_micros', 0):,.0f}", p9999(t),
                      p9999(rep.get("produce") or {}), p9999(rep.get("relay_consume") or {}),
                      stalls(rep.get("timeline")), f"{t.get('samples', 0):,}"]
        except (OSError, ValueError):
            cells += ["-", "-", "-", "-", "-", "-"]
        w("| " + " | ".join(cells) + " |")
    w("")

# ---------------------------------------------------------------------------
# Gateway filtering (run gateways): one table per sweep, the runner's own rows.
gw = sorted(glob.glob(f"{R}/client-results/gateways-*/summary.txt"))
if gw:
    w("## Gateway filtering (run gateways)\n")
    w("Per level: N gateways each want only their own events. firehose = every "
      "gateway reads the whole events topic (no transform); topics / keyed = the "
      "in-broker gateway-demux routes each event to the gateway's own topic, or to "
      "a partition keyed by gateway id. egress is broker fetch bytes "
      "(vectorized_kafka_handler_sent_bytes_total{handler=\"fetch\"}) per produced "
      "event; kept is the fraction of what gateways received that was theirs; "
      "CPU is the gateway processes' own. clean = every event reached its gateway "
      "exactly once. latency_usable = false when the demux fell more than a "
      "quarter-second of input behind (lag_peak records): that level's percentiles "
      "are a queue age, not a latency. leader_changes = raft leadership changes during the level "
      "(should be 0: the runner pauses the leader balancer; summaries without the "
      "column predate that).\n")
cols = ["level", "produced", "kept_fraction", "bytes_per_gateway", "egress_per_event",
        "cpu_us_per_kept", "p50_us", "p99_us", "p999_us", "clean", "latency_usable", "lag_peak", "leader_changes"]
for s in gw:
    txt = read(s)
    hdr = next((l.split() for l in txt.splitlines() if l.startswith("level ")), None)
    w(f"### `{os.path.basename(os.path.dirname(s))}`\n")
    for f in (l.lstrip("# ").strip() for l in txt.splitlines() if l.startswith("#")):
        w(f"- `{f}`")
    rows = [l.split() for l in txt.splitlines() if hdr and l and not l.startswith(("#", "level "))]
    if not rows:
        w("\n_no completed levels_\n")
        continue
    w("")
    w("| " + " | ".join(cols) + " |")
    w("|" + "---|" * len(cols))
    for r in rows:
        d = dict(zip(hdr, r))
        cells = []
        for c in cols:
            v = d.get(c, "-")
            try:
                f = float(v)
                v = f"{f:,.0f}" if c in ("produced", "bytes_per_gateway", "egress_per_event") else v
            except ValueError:
                pass
            cells.append(v)
        w("| " + " | ".join(cells) + " |")
    w("")

# ---------------------------------------------------------------------------
# Monitoring health (autorun's monitor_health, just before the export).
health = [(f, read(f"{R}/{f}").strip()) for f in ("counter-resets.txt", "scrape-health.txt")
          if os.path.exists(f"{R}/{f}")]
if health:
    w("## Monitoring health\n")
    w("Checked just before the metrics export. Counters: any metric a dashboard "
      "rate()s that reset far more often than restarts explain (its chart would be "
      "wrong). Scrapes: every failed broker scrape since the cluster was ready, and "
      "whether it timed out (a target too slow to answer) or failed fast.\n")
    for f, txt in health:
        w(f"`{f}`\n\n```\n{txt}\n```\n")

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

res = sorted(d for d in glob.glob(f"{R}/client-results/resilience-*/summary.txt")
             if not ((m := re.search(r"-(\d{9,})$", os.path.basename(os.path.dirname(d)))) and int(m.group(1)) < started - 60))
if res:
    w("## Resilience series (run resilience)\n")
    w("`missing` must be zero (anything else is data loss). Duplicates are expected without a "
      "graceful drain. Recovery is time until p99 is back within the band of this arm's own baseline "
      "and holds; a first-arm `DID NOT RECOVER` is usually the shared log growing under it - see "
      "docs/RESILIENCE.md.\n")
for d in res:
    txt = read(d)
    w(f"### `{os.path.basename(os.path.dirname(d))}`\n")
    w("| arm | total p99 us (base -> during) | max us | produce p99 us | duplicates | missing | recovery |")
    w("|---|---|---|---|---|---|---|")
    for m in re.finditer(r"RESULT (\S+): total p99 (\S+)->(\S+) max (\S+)->(\S+) \| produce p99 (\S+)->(\S+) \| over10ms \S+ \| duplicates=(\S+) missing=(\S+) \| recovery=(\S+)", txt):
        g = m.groups()
        w(f"| {g[0]} | {g[1]} -> {g[2]} | {g[3]} -> {g[4]} | {g[5]} -> {g[6]} | {g[7]} | {g[8]} | {g[9]} |")
    w("")

trad = []
for d in sorted(glob.glob(f"{R}/client-results/traditional-sharded-*")):
    m = re.search(r"-(\d{9,})$", d)
    if m and int(m.group(1)) < started - 60:
        continue
    for rj in sorted(glob.glob(f"{d}/trad-*.json")):
        try:
            j = json.load(open(rj))
        except (OSError, ValueError):
            continue
        n = re.search(r"trad-(\d+)\.json$", rj).group(1)
        pl = {}
        try:
            pl = json.load(open(f"{d}/placement-{n}-after.json"))
        except (OSError, ValueError):
            pass
        t = j.get("total") or {}
        cfg = j.get("config") or {}
        trad.append((j.get("label", ""), int(n), pl, t, j.get("clean"), cfg))
if trad:
    w("## Traditional arms (run traditional)\n")
    w("The matcher outside the broker: `external` on its own client instance, `loopback` on the "
      "partition leader's broker over loopback, `-isolated` pinned to cores outside Redpanda's "
      "cpuset. End to end is client send to the measured consumer's receipt, which here includes "
      "a fetch back out to a client (the in-broker figure does not).\n")
    w("| label | consumers | matcher cpus | e2e p50 us | p99 | p99.9 | p99.99 | samples | depth | cancels | clean |")
    w("|---|---|---|---|---|---|---|---|---|---|---|")
    for lab, n, pl, t, clean, cfg in sorted(trad, key=lambda x: (x[0].rsplit("-", 1)[0], x[1])):
        f = lambda k: f"{t.get(k, 0):,.0f}"
        w(f"| {lab} | {n} | {pl.get('matcher_cpus') or '-'} | {f('p50_micros')} | {f('p99_micros')} | {f('p999_micros')} | "
          f"{p9999(t)} | {t.get('samples', 0):,} | {cfg.get('book_depth', 0)} | {cfg.get('cancel_fraction', 0)} | {clean} |")
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
