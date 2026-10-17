#!/usr/bin/env bash
# Add phase and measured-window shading to a metrics bundle exported before the
# export did it itself, and refresh its dashboards to the current ones.
#
#   scripts/bundle-add-phases.sh results/run-<date>-<plan>
#
# Writes bench_phase_info (from the run's autorun.log) and bench_window_info
# (from the bundle's manifest) into the bundle's TSDB as one extra block, built
# with the bundle's own Prometheus image so the block format matches, then
# re-renders the viewer half (dashboards, compose, manifest) with
# scripts/metrics-bundle.py. Safe to re-run: every series an earlier build
# wrote is deleted first. Needs docker and python3; stop the bundle first if it
# is running (docker compose down in it) - Prometheus only reads blocks at
# startup.
set -euo pipefail
HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
RUN="${1:?usage: bundle-add-phases.sh <run-dir>}"
RUN="$(cd "$RUN" && pwd)"
B="$RUN/metrics-bundle"
die() { echo "bundle-add-phases: $*" >&2; exit 1; }
[ -f "$B/manifest.json" ] && [ -d "$B/data" ] || die "no metrics bundle under $RUN"
[ -f "$RUN/autorun.log" ] || die "no $RUN/autorun.log - phases come from it"
command -v docker >/dev/null || die "docker is needed (the bundle's Prometheus image builds the block)"

read -r PROM GRAFANA SCOPE < <(python3 -c '
import json, sys
m = json.load(open(sys.argv[1]))
print(m.get("prometheus_version", "3.5.0"), m.get("grafana_version", "11.6.1"), m.get("scope", "full"))
' "$B/manifest.json")

T="$(mktemp -d)"; trap 'rm -rf -- "${T:?}"' EXIT
python3 -c '
import json, sys
m = json.load(open(sys.argv[1]))
with open(sys.argv[2], "w") as f:
    for w in m["windows"]:
        f.write("%s %d %d\n" % (w["label"], w["start_ms"], w["end_ms"]))
' "$B/manifest.json" "$T/windows"

# Delete what an earlier build (or the export) wrote, by series rather than by
# block: opening a bundle lets Prometheus compact its blocks, which merges the
# series block into the data blocks under new IDs, so removing the recorded
# block ID silently removed nothing and every rebuild added another copy
# (2026-10-16). The rebuilt series all carry a source label (bench_window_info
# is only ever rebuilt); the live-scraped markers carry none and are kept.
REBUILT=('bench_phase_info{source!=""}' 'loadgen_phase_info{source!=""}' 'bench_window_info' 'bench_e2e_latency_seconds')
running="$(docker ps --format '{{.ID}}' | while read -r id; do
  if docker inspect -f '{{range .Mounts}}{{.Source}} {{end}}' "$id" | tr ' ' '\n' | grep -qxF "$B/data"; then echo "$id"; fi
done || true)"
[ -z "$running" ] || die "the bundle's Prometheus is running ($running): docker compose down in $B first"
CNAME="bundle-add-phases-$$"
PORT="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
trap 'docker stop "$CNAME" >/dev/null 2>&1 || true; rm -rf -- "${T:?}"' EXIT
docker run -d --rm --name "$CNAME" -p "127.0.0.1:$PORT:9090" \
  -v "$B/prometheus.yml:/etc/prometheus/prometheus.yml:ro" -v "$B/data:/prometheus" "prom/prometheus:v$PROM" \
  --config.file=/etc/prometheus/prometheus.yml --storage.tsdb.path=/prometheus \
  --storage.tsdb.retention.time=100y --web.enable-admin-api >/dev/null \
  || die "could not start the bundle's Prometheus to delete the old series"
for _ in $(seq 1 60); do curl -sf "http://127.0.0.1:$PORT/-/ready" >/dev/null && break; sleep 1; done
# Each level's measure start, which places an older report's latency timeline
# in time (its offsets are from the window's first timed send, and only a
# current loadgen records that instant).
python3 - "http://127.0.0.1:$PORT" "$B/manifest.json" > "$T/anchors" <<'PY' || die "could not read the measure starts"
import json, sys, urllib.parse, urllib.request
prom, man = sys.argv[1], json.load(open(sys.argv[2]))
lo = min(w["start_ms"] for w in man["windows"]) / 1000
hi = max(w["end_ms"] for w in man["windows"]) / 1000
q = urllib.parse.urlencode({"query": 'max by (label) (loadgen_phase_info{phase="measure"})',
                            "start": lo, "end": hi, "step": "1"})
for s in json.load(urllib.request.urlopen(f"{prom}/api/v1/query_range?{q}"))["data"]["result"]:
    ts = [float(t) for t, _ in s["values"]]
    # A new measure stage starts wherever the marker reappears after a gap.
    for t in [ts[0]] + [b for a, b in zip(ts, ts[1:]) if b - a > 5]:
        print(s["metric"].get("label", ""), int(t))
PY
args=(); for m in "${REBUILT[@]}"; do args+=(--data-urlencode "match[]=$m"); done
curl -sf -X POST "http://127.0.0.1:$PORT/api/v1/admin/tsdb/delete_series" "${args[@]}" \
  && curl -sf -X POST "http://127.0.0.1:$PORT/api/v1/admin/tsdb/clean_tombstones" \
  || die "could not delete the previously rebuilt series"
left="$(curl -sf "http://127.0.0.1:$PORT/api/v1/series" "${args[@]}" | python3 -c 'import json, sys; print(len(json.load(sys.stdin)["data"]))')"
[ "$left" = 0 ] || die "$left rebuilt series survived the delete"
docker stop "$CNAME" >/dev/null
echo "  deleted the previously rebuilt phase, window and latency series"
python3 "$HERE/bench-series.py" "$RUN" --anchors "$T/anchors" > "$T/series.om"

mkdir -p "$T/blocks"
docker run --rm --user "$(id -u):$(id -g)" --entrypoint promtool \
  -v "$T:/work" "prom/prometheus:v$PROM" \
  tsdb create-blocks-from openmetrics -q /work/series.om /work/blocks >/dev/null \
  || die "promtool could not build the series block"
new=()
for d in "$T/blocks"/01*; do
  [ -d "$d" ] || continue
  new+=("$(basename "$d")")
  mv "$d" "$B/data/"
done
[ "${#new[@]}" -gt 0 ] || die "promtool produced no block"
python3 -c '
import json, sys
json.dump({"blocks": sys.argv[2:], "written_by": "scripts/bench-series.py"}, open(sys.argv[1], "w"), indent=2)
' "$B/data/bench-series.json" "${new[@]}"
# The container runs as nobody and must be able to read (and compact) what was
# added (see metrics-bundle.py, 2b). Only what this script added: files the
# container wrote while the bundle was open belong to it, not to us.
for n in "${new[@]}"; do chmod -R a+rwX "$B/data/$n"; done
chmod a+rw "$B/data/bench-series.json"

python3 "$HERE/metrics-bundle.py" "$B" "$T/windows" \
  --prom "$PROM" --grafana "$GRAFANA" --scope "$SCOPE" --results "$RUN" >/dev/null \
  || die "could not refresh the bundle's dashboards"

echo "bundle-add-phases: added block ${new[*]} and refreshed the dashboards in $B"
echo "  view it:  cd $B && docker compose up -d   then http://localhost:3000"
