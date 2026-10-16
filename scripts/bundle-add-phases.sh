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
# scripts/metrics-bundle.py. Safe to re-run: the block it added last time is
# replaced. Needs docker and python3; stop the bundle first if it is running
# (docker compose down in it) - Prometheus only reads blocks at startup.
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
python3 "$HERE/bench-series.py" "$RUN" > "$T/series.om"
python3 -c '
import json, sys
m = json.load(open(sys.argv[1]))
with open(sys.argv[2], "w") as f:
    for w in m["windows"]:
        f.write("%s %d %d\n" % (w["label"], w["start_ms"], w["end_ms"]))
' "$B/manifest.json" "$T/windows"

# Replace the block a previous run of this script (or the export) added.
python3 - "$B/data" <<'PY'
import json, shutil, sys
from pathlib import Path
d = Path(sys.argv[1])
try:
    old = json.loads((d / "bench-series.json").read_text()).get("blocks", [])
except (OSError, ValueError):
    old = []
for b in old:
    if len(b) == 26 and b.startswith("01") and (d / b).is_dir():
        shutil.rmtree(d / b)
        print(f"  removed the previous series block {b}")
PY

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
