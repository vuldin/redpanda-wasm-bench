#!/usr/bin/env bash
# Export the scraped metrics for a run's MEASURED WINDOWS into a self-contained
# bundle that opens in a fresh Grafana with no cluster.
#
# WHY WINDOWS AND NOT THE WHOLE RUN: a reproduction is ~90 minutes of wall
# clock containing ~10 minutes of measurement. The rest is provisioning, 500
# consumers being deployed, topic cleanup and settle time. Exporting only the
# windows is roughly a 10x reduction, which is what makes it affordable to
# include the full internal vectorized_* set rather than just the families the
# dashboards happen to query.
#
# WHY promtool AND NOT A COPY OF THE DATA DIRECTORY: Prometheus keeps the most
# recent ~2h in the write-ahead log, uncompacted. For a run shorter than that
# the blocks on disk are EMPTY and `cp -r data` produces a bundle with nothing
# in it. `promtool tsdb dump-openmetrics` replays the WAL itself - verified
# 2026-09-16 by dumping a 25s-old Prometheus and getting 15,458 lines when no
# block existed yet. It also needs no admin API and no restart, so the running
# stack is untouched.
#
# The dump and the block build both happen ON THE MONITORING HOST, which
# already has promtool from ansible/monitoring.yaml. The bundle therefore ships
# finished TSDB blocks and the person opening it needs no Prometheus tooling.
#
# Usage: export-metrics.sh <results-dir> [out-dir]
#   results-dir  a local run directory holding loadgen reports, e.g.
#                results/reproduce-1789583726
#   out-dir      defaults to <results-dir>/metrics-bundle
set -uo pipefail

HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
WB="$HERE/wb"

RESULTS="${1:?usage: export-metrics.sh <results-dir> [out-dir]}"
OUT="${2:-$RESULTS/metrics-bundle}"
# Never into an existing bundle: the blocks would merge with another export's
# (possibly another cluster's) and the manifest would describe only the last.
if [ -e "$OUT" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ]; then
  NEW="$OUT-$(date -u +%Y%m%dT%H%M%SZ)"
  echo "  $OUT already holds a bundle; writing this export to $NEW instead"
  OUT="$NEW"
fi
[ -d "$RESULTS" ] || { echo "no such results dir: $RESULTS" >&2; exit 1; }

# Seconds of padding either side of each measured window. The ramp into a
# window and the settle after it are the parts that explain its shape, so a
# window clipped exactly to the measurement is harder to read than one with
# shoulders.
# 30s, matching metrics-windows.py's own default. These were 60 and 30
# respectively, so the export silently padded twice as wide as the tool it
# calls documents.
PAD="${METRICS_EXPORT_PAD_SECS:-30}"
# full | public | dashboards. `full` includes the internal vectorized_* set,
# which is where the series count is; the windowing above is what pays for it.
SCOPE="${METRICS_EXPORT_SCOPE:-full}"
PROM_VERSION="${PROM_VERSION:-3.5.0}"
GRAFANA_VERSION="${GRAFANA_VERSION:-11.6.1}"

say() { printf '\n=== %s ===\n' "$*"; }
die() { echo "export-metrics: $*" >&2; exit 1; }

case "$SCOPE" in
  full)       MATCH='' ;;
  public)     MATCH='{__name__=~"redpanda_.*|node_.*|loadgen_.*|bench_.*|up|scrape_.*"}' ;;
  dashboards) MATCH='{__name__=~"redpanda_relay_.*|redpanda_transform_.*|redpanda_wasm_.*|redpanda_cpu_.*|node_cpu_.*|node_load1|node_network_.*|loadgen_.*|bench_.*|up|scrape_duration_seconds"}' ;;
  *) die "METRICS_EXPORT_SCOPE must be full, public or dashboards (got '$SCOPE')" ;;
esac

eval "$(bash "$WB" env 2>/dev/null | grep -E '^(MONITOR_PUB|PROMETHEUS_URL)=')" || true
[ -n "${MONITOR_PUB:-}" ] || die "no monitoring host in this cluster.
     The metrics live on it, and \`wb teardown\` destroys it, so there is
     nothing to export from once the cluster is gone."

# --- the windows ------------------------------------------------------------
# Derived from the reports rather than from wall clock: each report carries the
# window it measured, so the bundle covers exactly what was measured even if
# the export runs hours later.
say "deriving measured windows from $RESULTS"
# --prometheus is what makes the windows right. Without it the windows are
# derived from run_id, which is when the RUNNER started rather than when
# measuring started, and is wrong by however long setup took - about 150s for a
# 500-consumer fan-out level, enough to export the deploy phase instead of the
# measurement.
WINDOWS="$(python3 "$HERE/metrics-windows.py" "$RESULTS" --pad "$PAD" \
            --prometheus "${PROMETHEUS_URL:-}")" \
  || die "could not derive windows"
[ -n "${WINDOWS//[[:space:]]/}" ] || die "no loadgen reports with usable timestamps under $RESULTS"
echo "$WINDOWS" | sed 's/^/  /'
NWIN=$(echo "$WINDOWS" | grep -c .)

# --- dump + build, on the monitoring host -----------------------------------
# wb's rsh wraps every ssh in `timeout ${RSH_TIMEOUT:-300}`, and its own
# header says long-running work must not use that default. A full-scope window
# dump is 6-20 GB of OpenMetrics text and takes many minutes, so 300s kills it
# mid-dump - and the failure arrives as an empty error string, because the
# output is lost with the connection. That is exactly how window 2 of the first
# corrected export "FAILED" with nothing to show for it. 2h per window is
# effectively no limit while still bounding a wedged host.
export RSH_TIMEOUT="${RSH_TIMEOUT:-7200}"

say "dumping $NWIN window(s) on the monitoring host (scope=$SCOPE)"
# Unique per invocation. A fixed path is a collision: killing an export does
# NOT kill the promtool it started on the monitoring host - the remote command
# has no tty and survives its ssh - so a retry would find the previous dump
# still writing to the same w.om and both would truncate it. Observed: a
# retried export sat at 0 bytes while an orphan from the prior attempt wrote to
# the same file.
REMOTE_WORK="/tmp/wasm-bench-metrics-export-$$-$(date +%s)"

# And refuse to start if a dump is already in flight, rather than racing it for
# the Prometheus data directory.
_inflight="$(bash "$WB" ssh monitor "ps -eo args | grep -c '[p]romtool tsdb'" </dev/null 2>/dev/null | tr -dc '0-9')"
if [ -n "$_inflight" ] && [ "$_inflight" -gt 0 ]; then
  die "a promtool dump is already running on the monitoring host ($_inflight
       process(es)). An earlier export was probably interrupted - its remote
       work survives the ssh that started it. Wait for it, or clear it:
         wb ssh monitor \"sudo pkill -f 'promtool tsdb'\""
fi

bash "$WB" ssh monitor "sudo rm -rf $REMOTE_WORK && sudo mkdir -p $REMOTE_WORK/data && sudo chmod 777 $REMOTE_WORK $REMOTE_WORK/data" </dev/null \
  || die "could not prepare the remote work dir"

# Read the windows into an ARRAY rather than piping them into the loop.
#
# ssh reads stdin. A `while read ... done <<< "$WINDOWS"` loop whose body calls
# ssh therefore loses the rest of the list to the first ssh invocation: the loop
# runs exactly once and then stdin is exhausted. That is not hypothetical - it
# is what the first real export did, and the failure was silent. Three windows
# were derived, one was dumped, and the bundle came out looking entirely
# plausible at 60 MB with a manifest that listed all three.
#
# The array removes stdin from the picture entirely. The redirections on the
# ssh calls below are belt-and-braces for the same hazard.
mapfile -t WINLINES <<< "$WINDOWS"

# Windows are dumped EXPORT_PARALLEL at a time (default 4; the monitoring
# host has 8 vCPUs). One at a time a full-scope window took ~50s, and a
# 56-window run (2026-10-16, state-and-jitter) needed ~50 minutes - more than
# was left before its deadline, so its bundle was lost. Each job writes its own
# dump file and its own block directory, so concurrent importers never share
# a path, and deletes its multi-GB dump once imported; the blocks are moved
# into one data directory afterwards.
PAR="${EXPORT_PARALLEL:-4}"
JOBS="$(mktemp -d)"
export_window() {
  local i="$1" label="$2" start_ms="$3" end_ms="$4"
  bash "$WB" ssh monitor \
    "sudo mkdir -p $REMOTE_WORK/blk.$i \
     && sudo promtool tsdb dump-openmetrics --min-time=$start_ms --max-time=$end_ms \
       ${MATCH:+--match='$MATCH'} /var/lib/prometheus > $REMOTE_WORK/w.$i.om 2>/dev/null \
     && sudo promtool tsdb create-blocks-from openmetrics -q $REMOTE_WORK/w.$i.om $REMOTE_WORK/blk.$i \
     && wc -c < $REMOTE_WORK/w.$i.om; sudo rm -f $REMOTE_WORK/w.$i.om" </dev/null 2>&1 \
    | tr -d '\r' | grep -E '^[0-9]+$' | tail -1 > "$JOBS/$i.out"
  printf '%s\t%s\n' "$label" "$(cat "$JOBS/$i.out")" > "$JOBS/$i.done"
}
i=0
for _line in "${WINLINES[@]}"; do
  [ -z "${_line//[[:space:]]/}" ] && continue
  read -r label start_ms end_ms <<< "$_line"
  i=$((i+1))
  while [ "$(jobs -rp | wc -l)" -ge "$PAR" ]; do wait -n 2>/dev/null || true; done
  # promtool reads the live data directory, including the WAL, so nothing is
  # stopped and no admin endpoint is needed.
  export_window "$i" "$label" "$start_ms" "$end_ms" &
done
wait
failed=0
for n in $(seq 1 "$i"); do
  IFS=$'\t' read -r label bytes < "$JOBS/$n.done" 2>/dev/null || { label="window $n"; bytes=""; }
  if [ -z "$bytes" ]; then
    printf '  [%d/%d] %-26s FAILED\n' "$n" "$NWIN" "$label"; failed=$((failed + 1))
  else
    printf '  [%d/%d] %-26s ok (%s of openmetrics text)\n' "$n" "$NWIN" "$label" \
      "$(numfmt --to=iec "$bytes" 2>/dev/null || echo "$bytes bytes")"
  fi
done
rm -rf -- "${JOBS:?}"
[ "$failed" -eq 0 ] || die "$failed window(s) could not be exported.
       An EMPTY remote output usually means the ssh was killed rather than the
       command failing - check RSH_TIMEOUT (currently ${RSH_TIMEOUT}s) against
       how long a ${SCOPE}-scope dump takes, and check disk on the monitoring
       host (a full-scope window is 6-20 GB of intermediate text, times
       EXPORT_PARALLEL=${PAR} at once)."
bash "$WB" ssh monitor "sudo sh -c 'for d in $REMOTE_WORK/blk.*; do mv \"\$d\"/* $REMOTE_WORK/data/ 2>/dev/null; rmdir \"\$d\" 2>/dev/null; done; true'" </dev/null \
  || die "could not gather the window blocks into one data directory"

# One import per window, so each becomes its own time-disjoint block. Do NOT
# concatenate the dumps into a single import: per-series sample ordering is
# only guaranteed within one window, and the importer needs it in order.
say "blocks built"
bash "$WB" ssh monitor "sudo promtool tsdb list $REMOTE_WORK/data 2>/dev/null | head -20" </dev/null | sed 's/^/  /'

# --- bring it down ----------------------------------------------------------
say "copying blocks to $OUT"
mkdir -p "$OUT/data" "$OUT/grafana/provisioning/datasources" \
         "$OUT/grafana/provisioning/dashboards" "$OUT/grafana/dashboards"
H="$(echo "$MONITOR_PUB" | awk '{print $1}')"
# --exclude tmp_*: promtool leaves root-owned tmp_dbro_sandbox* scratch dirs
# in the output; they hold no blocks, and an unreadable one made rsync exit 23
# and lose the whole bundle's viewer half (2026-10-15).
rsync -az --delete --exclude 'tmp_*' -e "ssh -i ${BENCH_SSH_KEY:?set BENCH_SSH_KEY in wb.local.env} \
  -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR" \
  "ubuntu@$H:$REMOTE_WORK/data/" "$OUT/data/" || die "rsync of the blocks failed"
bash "$WB" ssh monitor "sudo rm -rf $REMOTE_WORK" >/dev/null 2>&1 || true

echo "  bundle data: $(du -sh "$OUT/data" | cut -f1)"

# --- the viewer half -------------------------------------------------------
say "writing the bundle"
WINFILE="$(mktemp)"; trap 'rm -f "$WINFILE"' EXIT
printf '%s\n' "$WINDOWS" > "$WINFILE"
python3 "$HERE/metrics-bundle.py" "$OUT" "$WINFILE" \
  --prom "$PROM_VERSION" --grafana "$GRAFANA_VERSION" \
  --scope "$SCOPE" --results "$RESULTS" || die "could not write the bundle"

say "done"
cat <<EOM
  $OUT

  View it with no cluster:
    cd $OUT && docker compose up
    open http://localhost:3000

  Check that every panel returns data:
    cd $OUT && docker compose up -d && ./validate.sh

  The bundle covers the measured windows only. Everything between them was
  provisioning, consumer deployment, cleanup and settle, so panels are empty
  outside those ranges by construction.
EOM
