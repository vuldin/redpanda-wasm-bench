#!/usr/bin/env python3
"""Write the viewer half of a metrics bundle: configs, dashboards, manifest.

The blocks are already in <out>/data by the time this runs. This produces
everything needed to serve them with no cluster, and it is the half where the
failure mode is a bundle that opens to empty panels and looks broken.

Four things have to be right or that happens:

  1. The dashboards must carry an ABSOLUTE time range. They ship defaulting to
     now-15m, which in a bundle opened later shows nothing at all.
  2. Retention must be a LARGE DURATION, not zero. `retention.time=0` is
     treated as "not set" and Prometheus falls back to its 15d default, which
     it says out loud: "No time or size retention was set so using the default
     time retention duration=15d". So the obvious way to disable retention is
     a no-op. 100y is used instead. (An idle Prometheus was separately observed
     NOT to evict 30-day-old blocks under a 15d retention within 150s, so this
     may never bite - but a bundle should not depend on that.)

  2b. The data directory must be WRITABLE BY THE CONTAINER USER. prom/prometheus
     runs as nobody (65534) while an exported bundle's files are owned by
     whoever ran the export, so Prometheus panics on startup with "open
     /prometheus/queries.active: permission denied" - it needs to write a lock
     file and the active-query log even when it is only serving. The bundle's
     data dir is therefore made group/other writable at build time, which needs
     no environment variables from whoever opens it.
  3. The datasource keeps uid "prometheus" and timeInterval 2s. The uid is what
     every panel refers to, and the interval is what $__rate_interval derives
     from - change it and the quantile panels stop matching what was seen live.
  4. Versions are pinned to the ones that produced the blocks. TSDB format
     compatibility is not guaranteed across major versions.

Usage: metrics-bundle.py <out-dir> <windows-file> [--prom V] [--grafana V]
                         [--scope S] [--results DIR]
"""
import argparse
import json
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("windows")
    ap.add_argument("--prom", default="3.5.0")
    ap.add_argument("--grafana", default="11.6.1")
    ap.add_argument("--scope", default="full")
    ap.add_argument("--results", default="")
    a = ap.parse_args()

    out = Path(a.out)
    wins = []
    for line in Path(a.windows).read_text().splitlines():
        if not line.strip():
            continue
        label, s, e = line.split()
        wins.append({"label": label, "start_ms": int(s), "end_ms": int(e)})
    if not wins:
        raise SystemExit("no windows")

    lo = min(w["start_ms"] for w in wins)
    hi = max(w["end_ms"] for w in wins)

    # ---------------------------------------------------------------- prometheus
    (out / "prometheus.yml").write_text(f"""\
# Serves the blocks in ./data and scrapes NOTHING. There is no cluster to
# scrape: this is a recording.
global:
  scrape_interval: 2s
  external_labels:
    redpanda_id: wasm-bench
scrape_configs: []
""")

    # ------------------------------------------------------------------- compose
    (out / "docker-compose.yml").write_text(f"""\
# Bring the recording up:  docker compose up -d   (stop: docker compose down)
# Without -d, `up` stays attached to the containers' logs and looks idle - it is
# not hung. If they were already running it only attaches.
# Then open http://localhost:3000 - anonymous viewer, no login.
#
# Versions are pinned to the ones that produced ./data. TSDB format
# compatibility is not guaranteed across Prometheus majors, so a newer image
# may refuse these blocks.
services:
  prometheus:
    image: prom/prometheus:v{a.prom}
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      # 100y, NOT 0. Zero is read as "not set" and silently falls back to the
      # 15d default - Prometheus logs "No time or size retention was set so
      # using the default time retention duration=15d" when you try it.
      - --storage.tsdb.retention.time=100y
      - --web.listen-address=:9090
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
      # Read-write, and the directory is world-writable in the bundle: this
      # image runs as nobody (65534) and panics if it cannot write
      # queries.active, even though it only ever serves.
      - ./data:/prometheus
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:{a.grafana}
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_SECURITY_ADMIN_PASSWORD: wasmbench
      GF_ANALYTICS_REPORTING_ENABLED: "false"
      GF_ANALYTICS_CHECK_FOR_UPDATES: "false"
      GF_USERS_DEFAULT_THEME: dark
      GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: /var/lib/grafana/dashboards/wasm-bench/bench-overview.json
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports: ["3000:3000"]
    depends_on: [prometheus]
""")

    # -------------------------------------------------------------- provisioning
    ds = out / "grafana" / "provisioning" / "datasources"
    ds.mkdir(parents=True, exist_ok=True)
    (ds / "datasources.yaml").write_text("""\
apiVersion: 1
datasources:
  - name: Prometheus
    # uid "prometheus" is what every panel in these dashboards refers to.
    # Change it and every panel resolves to nothing.
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
    jsonData:
      httpMethod: POST
      # Matches the interval the data was scraped at, so $__rate_interval comes
      # out the same as it did live and the quantile panels agree.
      timeInterval: 2s
      manageAlerts: false
""")
    dp = out / "grafana" / "provisioning" / "dashboards"
    dp.mkdir(parents=True, exist_ok=True)
    (dp / "dashboards.yaml").write_text("""\
apiVersion: 1
providers:
  - name: wasm-bench
    orgId: 1
    type: file
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: true
    allowUiUpdates: false
""")

    # ---------------------------------------------------------------- dashboards
    src = ROOT / "ansible" / "files" / "dashboards"
    dst = out / "grafana" / "dashboards"
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    retimed = []
    for p in sorted(dst.rglob("*.json")):
        d = json.loads(p.read_text())
        # The whole point: an absolute range covering the recording. Left at
        # now-15m every panel in this bundle is empty.
        d["time"] = {"from": iso(lo), "to": iso(hi)}
        d["refresh"] = ""          # nothing is being scraped; refreshing is noise
        d["timezone"] = "utc"
        # Keep the dashboards' own annotations (autorun phases from
        # bench_phase_info, loadgen's measured windows) and add the export's
        # windows, from bench_window_info. All three read series in ./data:
        # this used to be a Grafana built-in annotation, which nothing in a
        # bundle ever populated, so it showed nothing.
        anns = [x for x in d.setdefault("annotations", {}).get("list", [])
                if x.get("name") != "measured windows"]
        anns.append({
            "name": "measured windows",
            "datasource": {"type": "prometheus", "uid": "prometheus"},
            "enable": True,
            "hide": False,
            "iconColor": "rgba(255, 96, 96, 0.2)",
            "expr": "max by (window) (bench_window_info)",
            "target": {"refId": "Windows", "expr": "max by (window) (bench_window_info)",
                       "interval": "5s"},
            "step": "5s",
            "titleFormat": "{{window}}",
            "textFormat": "exported window {{window}}",
            "tagKeys": "window",
            "useValueForTime": False,
        })
        d["annotations"]["list"] = anns
        d["description"] = (
            (d.get("description", "") or "")
            + "  RECORDING: fixed time range, nothing is being scraped. "
              "Panels outside the recorded windows are empty by construction.")
        p.write_text(json.dumps(d, indent=2) + "\n")
        retimed.append(p.relative_to(dst).as_posix())

    # ------------------------------------------------------------- self-validation
    # The validator ships INSIDE the bundle, so a recording can be checked by
    # whoever receives it without a copy of this repo. check-dashboards.py is
    # stdlib-only, which is what makes that cheap.
    shutil.copy2(ROOT / "scripts" / "check-dashboards.py",
                 out / "check-dashboards.py")
    shutil.copy2(ROOT / "scripts" / "bundle-validate.sh", out / "validate.sh")
    (out / "validate.sh").chmod(0o755)

    # ------------------------------------------------------------------ manifest
    def git(*args):
        try:
            return subprocess.run(["git", "-C", str(ROOT), *args],
                                  capture_output=True, text=True,
                                  timeout=10).stdout.strip() or None
        except Exception:
            return None

    data_dir = out / "data"
    blocks = sorted(p.name for p in data_dir.glob("01*") if p.is_dir()) \
        if data_dir.exists() else []

    # VERIFY THE BLOCKS ACTUALLY COVER THE WINDOWS before writing a manifest
    # that claims they do.
    #
    # The first real export shipped a manifest listing three windows over a
    # single block containing only the first. An ssh call inside the dump loop
    # had eaten the remaining stdin, so two windows were never dumped - and the
    # bundle was 60 MB of real data with a plausible manifest, which is the
    # worst possible shape for a mistake. The manifest is the thing someone
    # trusts when they cannot see the cluster any more, so it must not be
    # written unless it is true.
    #
    # Block boundaries come from each block's own meta.json, so this needs no
    # promtool and works wherever the bundle is assembled.
    # The run-structure block (scripts/bench-series.py: phases and windows as
    # series) spans the whole run, so counting it would make every window look
    # covered and hide one that genuinely failed to export.
    try:
        series_blocks = set(json.loads((data_dir / "bench-series.json").read_text())["blocks"])
    except (OSError, ValueError, KeyError):
        series_blocks = set()
    covered = []
    for b in (data_dir.glob("01*") if data_dir.exists() else []):
        if b.name in series_blocks:
            continue
        meta = b / "meta.json"
        if not meta.is_file():
            continue
        try:
            m = json.loads(meta.read_text())
            covered.append((int(m["minTime"]), int(m["maxTime"])))
        except (json.JSONDecodeError, OSError, KeyError, ValueError):
            continue

    def overlap_ms(w):
        """How much of window w any block actually covers."""
        total = 0
        for lo_b, hi_b in covered:
            total += max(0, min(w["end_ms"], hi_b) - max(w["start_ms"], lo_b))
        return total

    missing = []
    for w in wins:
        span = w["end_ms"] - w["start_ms"]
        got = overlap_ms(w)
        w["covered_ms"] = got
        # 50%: a window's edges are padding, and the importer aligns blocks, so
        # demanding the full span would false-alarm. A window that is genuinely
        # absent shows up as zero or near-zero, not as 60%.
        if span > 0 and got < span * 0.5:
            missing.append((w["label"], got, span))

    if missing:
        lines = "\n".join(
            f"    {lbl}: only {g / 1000:.0f}s of {sp / 1000:.0f}s is in the blocks"
            for lbl, g, sp in missing)
        raise SystemExit(
            f"REFUSING to write a manifest that misrepresents the data.\n"
            f"  {len(missing)} of {len(wins)} window(s) are not covered by any "
            f"block:\n{lines}\n"
            f"  Blocks present: {len(covered)} covering "
            f"{sorted((lo, hi) for lo, hi in covered)}\n"
            f"  The dump loop did not produce a block for every window. Check "
            f"the per-window output above for one that failed, and note that an "
            f"ssh inside a `while read` loop will silently end it early.")
    size = sum(f.stat().st_size for f in data_dir.rglob("*") if f.is_file()) \
        if data_dir.exists() else 0

    manifest = {
        "created_unix": int(time.time()),
        "created_utc": iso(int(time.time() * 1000)),
        "scope": a.scope,
        "prometheus_version": a.prom,
        "grafana_version": a.grafana,
        "recording": {
            "start_utc": iso(lo), "end_utc": iso(hi),
            "start_ms": lo, "end_ms": hi,
            "exported_seconds": sum(
                (w["end_ms"] - w["start_ms"]) / 1000 for w in wins),
        },
        # covered_ms per window is the verified overlap with the blocks on
        # disk, not a restatement of the request.
        "windows": wins,
        "blocks": blocks,
        "series_blocks": sorted(series_blocks),
        "data_bytes": size,
        "dashboards": retimed,
        "bench_repo_commit": git("rev-parse", "HEAD"),
        "bench_repo_dirty": bool(git("status", "--porcelain")),
        "results_dir": a.results or None,
        "notes": [
            "Windows only: the gaps between them were provisioning, consumer "
            "deployment, cleanup and settle time, and were not exported.",
            "promtool tsdb dump-openmetrics is experimental and drops native "
            "histograms. Redpanda's are classic _bucket series, so nothing was "
            "lost here, but a future build using native histograms would lose "
            "them silently.",
            "Every published figure comes from the run's own JSON reports, not "
            "from these metrics. See docs/METHODOLOGY.md entry 30 for why a "
            "quantile read off a dashboard is not the quantile in the report.",
        ],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    # -------------------------------------------------------------------- README
    rows = "\n".join(
        f"| {w['label']} | {iso(w['start_ms'])} | "
        f"{(w['end_ms'] - w['start_ms']) / 1000:.0f} s |" for w in wins)
    (out / "README.md").write_text(f"""\
# Recorded metrics, {iso(lo)[:10]}

The scraped metrics for one benchmark run's measured windows, with the
dashboards that read them. No cluster, no Redpanda, nothing to provision.

```bash
docker compose up -d      # stop it later with: docker compose down
```

Then open <http://localhost:3000>. (Without `-d`, `up` stays attached to the
containers' quiet logs and looks hung while it is serving.) It lands on the bench overview with the
recorded range already selected. Anonymous viewer access is on; `admin` /
`wasmbench` if you want to edit.

## What is in here

| window | start (UTC) | length |
|---|---|---|
{rows}

{len(wins)} window(s), {manifest['recording']['exported_seconds'] / 60:.1f} minutes
of metrics, scope `{a.scope}`, {size / 1e6:.0f} MB of TSDB blocks.

Only the measured windows were exported. The gaps between them were
provisioning, deploying 500 consumers, topic cleanup and settle time, so the
dashboards are empty outside the windows by construction rather than because
something is broken.

## Without Docker

```bash
prometheus --config.file=prometheus.yml --storage.tsdb.path=./data \\
  --storage.tsdb.retention.time=0
```

Use `100y`, not `0`. Zero is read as "not set" and Prometheus falls back to
its 15d default - it says so in the log - so the obvious way to turn retention
off does nothing. With a long retention the recording cannot evict itself
however long you leave it.

Point a Grafana at `http://localhost:9090` with the datasource uid
`prometheus`, or just use the compose file.

## Reading it

The three dashboards are the same ones that were live during the run:
`overview` for latency and the gates that decide whether it is admissible,
`relay stages` for which of the four relay stages the time is in, and
`measurement health` for whether the load generator held its offered rate.

These metrics are for understanding the run. They are not where the published
figures come from - those come from the run's own JSON reports, because a
quantile computed by Grafana over a rate window is a different estimator than
a quantile over the samples themselves. `docs/METHODOLOGY.md` entry 30 has the
detail.

`manifest.json` carries the full fingerprint, including the bench repo commit
this was exported from.
""")
    # prom/prometheus runs as nobody (65534) and must be able to write its
    # lock file and queries.active into the data dir, or it panics on start.
    # Doing this here means the bundle needs no env vars or chown from whoever
    # opens it.
    if data_dir.exists():
        # Best effort, and it has to be: once a container has served from this
        # directory it holds files owned by nobody (queries.active, lock) that
        # the exporting user cannot chmod. Failing the whole build over a file
        # Prometheus will recreate anyway would be absurd.
        denied = 0
        for d in [data_dir, *data_dir.rglob("*")]:
            try:
                d.chmod(0o777 if d.is_dir() else 0o666)
            except PermissionError:
                denied += 1
        if denied:
            print(f"  note: {denied} file(s) in data/ are owned by another user "
                  f"and were left alone (Prometheus recreates them)")

    print(f"  wrote bundle: {len(wins)} window(s), "
          f"{manifest['recording']['exported_seconds'] / 60:.1f} min, "
          f"{len(blocks)} block(s), {size / 1e6:.0f} MB")
    print(f"  recording range: {iso(lo)} .. {iso(hi)}")


if __name__ == "__main__":
    main()
