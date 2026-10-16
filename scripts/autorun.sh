#!/usr/bin/env bash
# Run a test PLAN unattended: provision -> deploy -> phases -> fetch/archive
# -> teardown -> EC2 verify, with a hard wall-clock cap enforced by an
# INDEPENDENT watchdog process.
#
#   autorun.sh check PLAN              list the phases, estimates and budget; spends nothing
#   autorun.sh start PLAN              provision a cluster, run PLAN, tear down (detached)
#   autorun.sh start --existing PLAN   run PLAN on the cluster that is already up (detached)
#   autorun.sh status [RUN_DIR]        what is running, what has finished
#
# A plan is a small bash file (see plans/*.plan) that sets TOPOLOGY and
# DEPLOY_ARGS and lists phases:
#
#   phase NAME EST_MINUTES <wb arguments...>
#   phase P1-depth 10 run e2 MODE=bookdepth GUEST=book-matcher 'BOOKDEPTH_LEVELS=0 1000'
#
# Every phase is one `wb` invocation. Quote a KEY=VALUE whose value has spaces.
#
# Rules, all automatic, so a run can go unwatched:
#   - Nothing is provisioned unless scripts/offline-gate.sh passed for the
#     exact commits being run, and the AWS credentials outlive the deadline.
#   - A phase starts only if its estimate fits before the teardown reserve;
#     otherwise it, and every phase after it that also does not fit, is
#     skipped and recorded. A failed phase is retried once, then skipped.
#   - Between phases the cluster is put back into a known state (transforms
#     and test topics deleted, maintenance cleared, balancers on, every broker
#     running) rather than trusting the previous runner's traps.
#   - The end is always fetch -> archive -> teardown -> EC2 verify -> STATUS.md.
#     The watchdog does the same at the hard deadline if the run has not
#     finished, whether or not the run process is still alive.
#
# Environment (all optional):
#   RUN_DIR          where everything is written (default results/run-<date>-<plan>)
#   CAP_HOURS        optional wall-clock cap from start (default: none - the
#                    run takes as long as the plan; credentials still bound it)
#   RESERVE_MIN      teardown/archive reserve before the deadline (default 25)
#   ARCHIVE_DIR      also copy the run + a sha256-checked tarball here
#   KEEP_CLUSTER=1   skip the end-of-run teardown (the watchdog still tears
#                    down at the deadline - it is the cost guard)
#   GATE_FILE        offline gate record (default results/GATE-PASSED)
#   WB, CLIENTS_REPO, REDPANDA_SRC, AWS_PROFILE
set -uo pipefail

HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
SELF="$HERE/$(basename "${BASH_SOURCE[0]}")"
# Your per-machine settings (wb.local.env, via lib-env.sh), read here as well as
# by wb so that autorun's own settings - ARCHIVE_DIR above all - can live there
# instead of on every command line. The environment still wins.
# shellcheck source=lib-env.sh
. "$HERE/lib-env.sh"
WB="${WB:-$ROOT/scripts/wb}"
RESERVE_MIN="${RESERVE_MIN:-25}"
GATE_FILE="${GATE_FILE:-$ROOT/results/GATE-PASSED}"
ARCHIVE_DIR="${ARCHIVE_DIR:-}"

log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$RUN_DIR/autorun.log"; }
left() { echo $(( PHASE_END - $(date +%s) )); }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$1" "$2" "$3" "$4" >> "$RUN_DIR/phases.tsv"; }
die() { echo "autorun: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# plans
# ---------------------------------------------------------------------------
PHASE_NAMES=() PHASE_EST=() PHASE_CMD=()
TOPOLOGY="" DEPLOY_ARGS=()
phase() {
  [ $# -ge 3 ] || die "plan: phase needs NAME EST_MINUTES WB_ARGS... (got: $*)"
  case "$2" in ''|*[!0-9]*) die "plan: phase $1: estimate '$2' is not whole minutes" ;; esac
  PHASE_NAMES+=("$1"); PHASE_EST+=("$2")
  local c; printf -v c '%q ' "${@:3}"; PHASE_CMD+=("$c")
}
load_plan() {
  local plan="$1"
  [ -f "$plan" ] || die "no such plan: $plan"
  # Plans may refer to RUN_DIR (e.g. for a runner's own output directory).
  # `check` and `start` load the plan before a run directory exists, so give
  # the plan a placeholder for that one source - and take it away again, or
  # the placeholder becomes the run directory (it did, once).
  local had_rd=0; [ -n "${RUN_DIR+x}" ] && had_rd=1
  [ "$had_rd" = 1 ] || RUN_DIR="<run-dir>"
  # shellcheck disable=SC1090
  source "$plan"
  [ "$had_rd" = 1 ] || unset RUN_DIR
  [ "${#PHASE_NAMES[@]}" -gt 0 ] || die "plan $plan defines no phases"
  local n; for n in "${PHASE_NAMES[@]}"; do
    case "$n" in *[!A-Za-z0-9._-]*) die "plan: phase name '$n' must be [A-Za-z0-9._-]" ;; esac
  done
}
plan_minutes() { local t=0 e; for e in "${PHASE_EST[@]}"; do t=$((t + e)); done; echo "$t"; }

# ---------------------------------------------------------------------------
# credentials: exported once, so a run survives its SSO token expiring. The
# role credentials have their own (usually longer) expiry; that is the limit.
# ---------------------------------------------------------------------------
load_creds() {
  [ "${DRY_RUN:-0}" = 1 ] && return 0
  local c
  c="$(aws configure export-credentials --format env 2>/dev/null)" || return 1
  eval "$c"
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  [ -n "${AWS_CREDENTIAL_EXPIRATION:-}" ] && export AWS_CREDENTIAL_EXPIRATION
  return 0
}
creds_expiry_epoch() {
  [ "${DRY_RUN:-0}" = 1 ] && { echo $(( $(date +%s) + 30 * 86400 )); return; }
  date -d "${AWS_CREDENTIAL_EXPIRATION:-1970-01-01T00:00:00Z}" +%s 2>/dev/null || echo 0
}

# ---------------------------------------------------------------------------
# phases
# ---------------------------------------------------------------------------
# The dashboards' phase marker: a node_exporter textfile on the monitoring
# host, so Prometheus records which phase is running as
# bench_phase_info{plan,phase,attempt} 1. Being a scraped series, the phase
# boundaries are in an exported bundle too, not only in the live Grafana. Best
# effort and never fatal: a cluster deployed before the textfile collector
# existed has no directory, and the marker is skipped without a warning.
# The directory is ansible's node_exporter_textfile_dir; overridden only by the
# self-test, whose stub `wb ssh` runs the command locally.
PHASE_MARK_DIR="${PHASE_MARK_DIR:-/var/lib/node_exporter/textfile}"
phase_marker() { # phase_marker NAME ATTEMPT | phase_marker clear
  grep -q 'Grafana' "$RUN_DIR/MONITORING.txt" 2>/dev/null || return 0
  local f="$PHASE_MARK_DIR/bench_phase.prom" cmd
  if [ "$1" = clear ]; then
    cmd="rm -f $f"
  else
    local plan="${PLAN_NAME:-}"
    local line; line="$(printf 'bench_phase_info{plan="%s",phase="%s",attempt="%s"} 1' \
      "${plan//[^A-Za-z0-9_.-]/_}" "${1//[^A-Za-z0-9_.-]/_}" "${2//[^0-9]/}")"
    cmd="[ -d $PHASE_MARK_DIR ] || exit 0; printf '%s\\n' '# TYPE bench_phase_info gauge' '$line' > $f.tmp && mv $f.tmp $f"
  fi
  timeout 30 "$WB" ssh monitor "$cmd" < /dev/null > /dev/null 2>&1 \
    || log "WARN: could not update the phase marker on the monitoring host"
}

# run_phase NAME EST_MIN CMD-STRING: budget check, timeout at 1.5x the
# estimate, one retry. Returns 0 ok, 1 failed, 2 skipped for time.
run_phase() {
  local name="$1" est="$2" cmd="$3"
  # Plans name output directories relative to the run with @RUN_DIR@.
  local rd; printf -v rd '%q' "$RUN_DIR"
  cmd="${cmd//@RUN_DIR@/$rd}"
  # EST_UNIT_S is 60 (estimates are minutes); the dry-run test shrinks it.
  local need=$(( est * ${EST_UNIT_S:-60} ))
  if [ "$(left)" -le 0 ] || [ "$(left)" -lt "$need" ]; then
    log "SKIP $name: ${est}min estimate, $(( $(left) / 60 ))min left before the teardown reserve"
    record "$name" skipped-time "$est" "-"
    return 2
  fi
  local attempt rc
  for attempt in 1 2; do
    local cap=$(( need * 3 / 2 ))
    { [ "$cap" -le 0 ] || [ "$cap" -gt "$(left)" ]; } && cap="$(left)"
    [ "$cap" -le 0 ] && { log "SKIP $name: phase window closed"; record "$name" skipped-time "$est" "-"; return 2; }
    log "START $name attempt $attempt (est ${est}min, cap $((cap / 60))min): wb $cmd"
    local t0; t0=$(date +%s)
    phase_marker "$name" "$attempt"
    eval "timeout --kill-after=60 $cap \"\$WB\" $cmd" >> "$RUN_DIR/$name.log" 2>&1
    rc=$?
    phase_marker clear
    local dt=$(( $(date +%s) - t0 ))
    if [ "$rc" -eq 0 ]; then
      log "OK $name in $((dt / 60))m$((dt % 60))s"
      record "$name" ok "$est" "$dt"
      return 0
    fi
    log "FAIL $name attempt $attempt rc=$rc after $((dt / 60))m$((dt % 60))s (tail of $name.log below)"
    tail -5 "$RUN_DIR/$name.log" | sed 's/^/    /' | tee -a "$RUN_DIR/autorun.log" >/dev/null
    record "$name" "fail-rc$rc-attempt$attempt" "$est" "$dt"
    [ "$attempt" = 1 ] && [ "$(left)" -lt "$need" ] && { log "no time to retry $name"; break; }
    [ "$attempt" = 1 ] && hygiene
  done
  return 1
}

# Put the cluster back into a known state between phases. Idempotent; never
# fatal. Re-asserts what the runners' traps would have restored, because a
# phase killed by its timeout never ran them.
hygiene() {
  [ "${DRY_RUN:-0}" = 1 ] && { log "hygiene (dry run)"; return 0; }
  log "hygiene: deleting transforms + test topics, clearing maintenance, re-enabling balancers"
  local env; env="$("$WB" env 2>/dev/null | grep -E '^(BROKERS|ADMIN_URL|ADMIN_URLS|BROKER_PUB)=')"
  eval "$env"
  local hosts; hosts="$(echo "${ADMIN_URLS:-}" | sed 's|http://||g')"
  local R="./rpk -X brokers=${BROKERS:-} -X admin.hosts=$hosts"
  # A timed-out phase leaves its remote runner going. The [t] keeps each
  # pattern from matching this very ssh command line, which pkill -f would
  # otherwise kill along with its target.
  timeout 300 "$WB" ssh client0 "cd /opt/wasm-bench && pkill -f 'run-e2-throughpu[t]'; pkill -x loadgen; pkill -x statecheck; pkill -x gateways; \
    for t in \$($R transform list 2>/dev/null | awk 'NR>1{print \$1}'); do $R transform delete \$t --no-confirm >/dev/null 2>&1; done; \
    sleep 3; \
    for t in \$($R topic list 2>/dev/null | awk 'NR>1 && \$1 ~ /^(orders|fills|probe|gwevents-|gw-|gwk-)/ {print \$1}'); do $R topic delete \$t >/dev/null 2>&1; done; \
    for n in \$(curl -sf ${ADMIN_URL:-}/v1/brokers 2>/dev/null | jq -r '.[].node_id' 2>/dev/null); do $R cluster maintenance disable \$n >/dev/null 2>&1; done; \
    $R cluster config set enable_leader_balancer true >/dev/null 2>&1; \
    $R cluster config set core_balancing_continuous true >/dev/null 2>&1; \
    $R cluster config set data_transforms_graceful_transfer_timeout_ms null >/dev/null 2>&1; true" >> "$RUN_DIR/autorun.log" 2>&1 || true
  local n i; n=$(echo "${BROKER_PUB:-}" | wc -w)
  for ((i = 0; i < n; i++)); do
    timeout 60 "$WB" ssh "broker$i" "systemctl is-active --quiet redpanda || sudo systemctl start redpanda" >/dev/null 2>&1 || true
  done
}

# Where to watch. Written once the cluster is up, so `status` and RESULTS.md
# can show it. The security group admits only the address that ran `wb up`.
monitor_info() {
  timeout 120 "$WB" monitor status > "$RUN_DIR/MONITORING.txt" 2>&1 || true
  local g; g="$(grep -m1 -E 'Grafana' "$RUN_DIR/MONITORING.txt" | awk '{print $2}')"
  if [ -n "$g" ]; then
    # Where monitor_health's scrape check starts: after provisioning, so a
    # deploy's rolling restart is not reported as failed scrapes.
    date +%s > "$RUN_DIR/MONITOR_READY_AT"
    log "WATCH: Grafana $g (anonymous viewer; admin/wasmbench to edit) - start with the 'wasm bench: overview' dashboard"
  else
    log "no monitoring host in this cluster (monitor_count=0) - nothing to watch or export"
  fi
}

# The monitoring host holds the only copy of the scraped metrics and teardown
# destroys it, so export before every teardown, time-boxed so a slow export
# can never hold a billing cluster.
#
# The box is whatever time is left before the hard deadline, less
# EXPORT_RESERVE_S for archive + teardown + verify, clamped to
# [EXPORT_MIN_S, EXPORT_MAX_S]. A fixed 15 minutes was too short: a full-scope
# dump runs ~50s per measured window, so the 27-window production-shape run
# (2026-10-15) was killed at window 19 with hours of cap left. An explicit
# argument (the watchdog's, past the deadline) is used as given.
export_metrics() {
  local cap="${1:-}"
  if [ -z "$cap" ]; then
    cap=$(( ${HARD_DEADLINE_EPOCH:-0} - $(date +%s) - ${EXPORT_RESERVE_S:-900} ))
    [ "$cap" -lt "${EXPORT_MIN_S:-600}" ] && cap="${EXPORT_MIN_S:-600}"
    [ "$cap" -gt "${EXPORT_MAX_S:-3600}" ] && cap="${EXPORT_MAX_S:-3600}"
  fi
  grep -q 'Grafana' "$RUN_DIR/MONITORING.txt" 2>/dev/null || { [ "${DRY_RUN:-0}" = 1 ] || return 0; }
  log "exporting the scraped metrics (measured windows) to metrics-bundle/ (time box ${cap}s)"
  if timeout "$cap" "$WB" monitor export "$RUN_DIR" >> "$RUN_DIR/metrics-export.log" 2>&1; then
    log "metrics exported: $RUN_DIR/metrics-bundle (open: cd metrics-bundle && docker compose up -d, then http://localhost:3000)"
  elif [ "${KEEP_CLUSTER:-0}" = 1 ]; then
    log "WARN: metrics export failed or timed out (see metrics-export.log); KEEP_CLUSTER=1, so the cluster stays up - retry with: wb monitor export $RUN_DIR"
  else
    log "WARN: metrics export failed or timed out (see metrics-export.log); the cluster is torn down regardless"
  fi
}

# Monitoring health, just before the export: counters the dashboards rate()
# that reset far more often than restarts explain (check-counter-resets.py),
# and every failed broker scrape since the cluster was ready, with whether it
# timed out (check-scrapes.py). Reports for RESULTS.md, never a gate, and
# time-boxed so a slow Prometheus cannot hold a billing cluster.
monitor_health() {
  if [ "${DRY_RUN:-0}" = 1 ]; then log "monitor health checks skipped (dry run)"; return 0; fi
  local prom; prom="$(grep -m1 -E 'Prometheus' "$RUN_DIR/MONITORING.txt" 2>/dev/null | awk '{print $2}')"
  [ -n "$prom" ] || return 0
  local now since mins
  now="$(date +%s)"
  since="$(cat "$RUN_DIR/MONITOR_READY_AT" 2>/dev/null || echo $(( now - 3600 )))"
  mins=$(( (now - since) / 60 + 1 ))
  timeout 300 python3 "$ROOT/scripts/check-counter-resets.py" "$prom" "${mins}m" \
    > "$RUN_DIR/counter-resets.txt" 2>&1
  # Both scripts start their summary line with their own name.
  log "$(tail -1 "$RUN_DIR/counter-resets.txt" 2>/dev/null)"
  timeout 300 python3 "$ROOT/scripts/check-scrapes.py" "$prom" "$since" "$now" 'redpanda.*' \
    --tsv "$RUN_DIR/scrape-failures.tsv" > "$RUN_DIR/scrape-health.txt" 2>&1
  log "$(head -1 "$RUN_DIR/scrape-health.txt" 2>/dev/null)"
}

summarize() {
  python3 "$ROOT/scripts/summarize-run.py" "$RUN_DIR" > "$RUN_DIR/RESULTS.md" 2>> "$RUN_DIR/autorun.log" \
    && log "results summary: $RUN_DIR/RESULTS.md" || log "WARN: summarize-run.py failed"
}

fetch_results() {
  log "fetching client-side results"
  timeout 600 "$WB" results "$RUN_DIR/client-results" >> "$RUN_DIR/autorun.log" 2>&1 || log "WARN: results fetch reported an error (files fetched so far are kept)"
}

archive() {
  [ -n "$ARCHIVE_DIR" ] || return 0
  log "archiving to $ARCHIVE_DIR"
  mkdir -p "$ARCHIVE_DIR"
  local base; base="$(basename "$RUN_DIR")"
  cp -a "$RUN_DIR" "$ARCHIVE_DIR/" 2>>"$RUN_DIR/autorun.log" || log "WARN: archive copy failed"
  tar -C "$(dirname "$RUN_DIR")" -czf "$ARCHIVE_DIR/$base.tar.gz" "$base" 2>>"$RUN_DIR/autorun.log" \
    && (cd "$ARCHIVE_DIR" && sha256sum "$base.tar.gz" > "$base.tar.gz.sha256" && sha256sum -c "$base.tar.gz.sha256" >> "$RUN_DIR/autorun.log" 2>&1) \
    || log "WARN: tarball failed"
}

# Exactly one teardown, whoever gets here first (run or watchdog).
teardown_and_verify() {
  local who="$1"
  if ! mkdir "$RUN_DIR/.teardown.lock" 2>/dev/null; then
    log "$who: teardown already in progress by another process"
    return 0
  fi
  log "$who: TEARDOWN"
  local ok=0 i
  for i in 1 2 3; do
    if timeout 1500 "$WB" teardown --yes >> "$RUN_DIR/teardown.log" 2>&1; then ok=1; break; fi
    # Provisioning that failed before terraform created anything leaves
    # nothing to tear down, and wb says so with a non-zero exit. Not a
    # failure: verify-clean below still checks the EC2 API either way.
    if tail -5 "$RUN_DIR/teardown.log" | grep -q "no cluster is provisioned"; then ok=1; break; fi
    log "$who: teardown attempt $i failed (see teardown.log)"
    [ "$i" = 2 ] && timeout 600 "$WB" cleanup-orphans --force >> "$RUN_DIR/teardown.log" 2>&1
    sleep 20
  done
  if [ "$ok" = 1 ] && timeout 300 "$WB" verify-clean >> "$RUN_DIR/teardown.log" 2>&1; then
    log "$who: teardown VERIFIED CLEAN via the EC2 API"
    echo "verified-clean $(date -u +%FT%TZ)" > "$RUN_DIR/TEARDOWN-RESULT"
  else
    log "$who: !!! TEARDOWN NOT VERIFIED - instances may still be billing. See teardown.log."
    echo "NOT-VERIFIED $(date -u +%FT%TZ)" > "$RUN_DIR/TEARDOWN-RESULT"
  fi
}

write_status() {
  local why="$1"
  {
    echo "# Autorun status"
    echo ""
    echo "- plan: $(cat "$RUN_DIR/PLAN" 2>/dev/null)"
    echo "- finished: $(date -u +%FT%TZ) ($why)"
    echo "- teardown: $(cat "$RUN_DIR/TEARDOWN-RESULT" 2>/dev/null || echo 'not run')"
    echo "- hard deadline: $(date -u -d @"$HARD_DEADLINE_EPOCH" +%FT%TZ), phase end $(date -u -d @"$PHASE_END" +%FT%TZ)"
    if [ -f "$RUN_DIR/PROVISIONED_AT" ]; then
      local p0 p1; p0=$(cat "$RUN_DIR/PROVISIONED_AT")
      p1=$(stat -c %Y "$RUN_DIR/TEARDOWN-RESULT" 2>/dev/null || date +%s)
      echo "- cluster lifetime: $(( (p1 - p0) / 60 )) min (provision start to teardown end)"
    fi
    echo ""
    echo "| time | phase | outcome | est min | seconds |"
    echo "|---|---|---|---|---|"
    awk -F'\t' '{printf "| %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5}' "$RUN_DIR/phases.tsv" 2>/dev/null
    echo ""
    echo "Logs: autorun.log, <phase>.log, teardown.log. Results: the phase OUT directories and client-results/."
  } > "$RUN_DIR/STATUS.md"
}

refuse() { log "REFUSING: $1"; record "${2:-gate}" refused 0 -; write_status "$1 - nothing provisioned"; touch "$RUN_DIR/DONE"; exit 1; }

heads() {
  echo "bench=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null) clients=$(git -C "${CLIENTS_REPO:-$WASM_CLIENTS_SRC}" rev-parse HEAD 2>/dev/null) broker=$(git -C "$REDPANDA_SRC" rev-parse HEAD 2>/dev/null)"
}

# ---------------------------------------------------------------------------
# the run (what `start` launches, detached)
# ---------------------------------------------------------------------------
run() {
  local plan="$1" existing="$2"
  load_plan "$plan"
  PLAN_NAME="$(basename "$plan" .plan)"
  echo $$ > "$RUN_DIR/autorun.pid"
  log "autorun run pid=$$ plan=$plan existing_cluster=$existing hard_deadline=$(date -u -d @"$HARD_DEADLINE_EPOCH" +%FT%TZ) phase_end=$(date -u -d @"$PHASE_END" +%FT%TZ)"

  if [ "$existing" = 0 ]; then
    local h; h="$(heads)"
    if [ "${DRY_RUN:-0}" != 1 ]; then
      grep -qxF "$h" "$GATE_FILE" 2>/dev/null || refuse "scripts/offline-gate.sh has not passed for $h"
      [ -z "$(git -C "$ROOT" status --porcelain -- bench scripts cmd ansible plans terraform/*.tfvars)" ] || refuse "uncommitted changes in the bench repo"
    fi
    record gate ok 0 "$h"
  fi
  load_creds || refuse "cannot export AWS credentials ($(aws_login_hint))" creds
  local exp; exp="$(creds_expiry_epoch)"
  [ "$exp" -ge $(( HARD_DEADLINE_EPOCH + RESERVE_MIN * 60 )) ] \
    || refuse "credentials expire $(date -u -d @"$exp" +%FT%TZ), before the deadline + reserve" creds
  log "credentials valid until $(date -u -d @"$exp" +%FT%TZ)"

  if [ "$existing" = 0 ]; then
    date +%s > "$RUN_DIR/PROVISIONED_AT"
    # A plan may define pre_up() for steps that must precede provisioning,
    # e.g. sizing the client fleet. A failure stops the run before any spend.
    if declare -F pre_up >/dev/null; then
      log "pre_up: running the plan's pre-provisioning step"
      if ! (pre_up) >> "$RUN_DIR/pre_up.log" 2>&1; then
        refuse "the plan's pre_up step failed (see pre_up.log)" pre_up
      fi
    else
      # No pre_up of its own: pick this run's region for this topology now, so
      # a run never inherits terraform/region.auto.tfvars from an earlier one
      # (2026-10-16: refused at P0-up because that overlay was a day old).
      # REGIONS (exported by the plan, or the environment) is the preference
      # order; select-region.sh takes the first with capacity.
      local tv
      case "$TOPOLOGY" in   # same mapping as scripts/switch-topology.sh
        single-az) tv=r8id-8xl-wasm-single-az.tfvars ;;
        multi-az) tv=r8id-8xl-wasm-multi-az.tfvars ;;
        six-broker) tv=r8id-8xl-wasm-6broker.tfvars ;;
        highcore) tv=r6id-32xl-wasm-highcore.tfvars ;;
        *) tv="" ;;
      esac
      if [ -n "$tv" ] && [ "${DRY_RUN:-0}" != 1 ]; then
        log "selecting a region for $TOPOLOGY (${REGIONS:-select-region.sh default order})"
        TFVARS="$ROOT/terraform/$tv" bash "$ROOT/scripts/select-region.sh" >> "$RUN_DIR/pre_up.log" 2>&1 \
          || refuse "region selection failed (see pre_up.log)" pre_up
      fi
    fi
    run_phase P0-up 25 "up ${TOPOLOGY:?plan must set TOPOLOGY}" || { finish "provisioning failed"; return 1; }
    local d=""; [ "${#DEPLOY_ARGS[@]}" -gt 0 ] && printf -v d '%q ' "${DEPLOY_ARGS[@]}"
    run_phase P0-deploy 25 "deploy $d" || { finish "deploy failed"; return 1; }
    run_phase P0-ready 10 "ready" || { hygiene; run_phase P0-ready-2 10 "ready" || { finish "cluster never ready"; return 1; }; }
    "$WB" doctor >> "$RUN_DIR/doctor.log" 2>&1 || log "WARN: wb doctor reported problems (see doctor.log)"
  else
    hygiene
  fi
  monitor_info

  local i
  for i in "${!PHASE_NAMES[@]}"; do
    run_phase "${PHASE_NAMES[$i]}" "${PHASE_EST[$i]}" "${PHASE_CMD[$i]}"
    hygiene
  done
  finish "all phases attempted"
}

finish() {
  local why="$1"
  phase_marker clear
  fetch_results
  monitor_health
  export_metrics
  summarize
  write_status "$why (pre-teardown)"
  archive
  if [ "${KEEP_CLUSTER:-0}" = 1 ]; then
    log "KEEP_CLUSTER=1: leaving the cluster up. The watchdog still tears it down at $(date -u -d @"$HARD_DEADLINE_EPOCH" +%FT%TZ); tear down sooner with: wb teardown --yes"
    echo "kept-by-request $(date -u +%FT%TZ)" > "$RUN_DIR/TEARDOWN-RESULT"
    write_status "$why (cluster kept)"
    touch "$RUN_DIR/PHASES-DONE"
    return 0
  fi
  teardown_and_verify run
  write_status "$why"
  archive
  touch "$RUN_DIR/DONE"
  log "DONE: $why"
}

# The independent cap. Sleeps until the hard deadline, then - if the run has
# not finished - kills it, fetches what it can, and tears down. It does not
# depend on the run process being alive, which is the point.
watchdog() {
  echo $$ > "$RUN_DIR/watchdog.pid"
  load_creds || true
  log "watchdog pid=$$ armed for $(date -u -d @"$HARD_DEADLINE_EPOCH" +%FT%TZ)"
  while [ "$(date +%s)" -lt "$HARD_DEADLINE_EPOCH" ]; do
    [ -f "$RUN_DIR/DONE" ] && { log "watchdog: run finished on its own; standing down"; return 0; }
    sleep 20
  done
  [ -f "$RUN_DIR/DONE" ] && return 0
  log "watchdog: HARD DEADLINE reached - stopping the run"
  local pid; pid="$(cat "$RUN_DIR/autorun.pid" 2>/dev/null)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    if [ -d "$RUN_DIR/.teardown.lock" ]; then
      log "watchdog: the run is already tearing down; giving it 20 more minutes"
      local i; for i in $(seq 1 60); do [ -f "$RUN_DIR/DONE" ] && return 0; sleep 20; done
    fi
    kill -TERM -- "-$(ps -o pgid= "$pid" | tr -d ' ')" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
    sleep 10
    kill -KILL "$pid" 2>/dev/null || true
  fi
  rmdir "$RUN_DIR/.teardown.lock" 2>/dev/null || true
  record watchdog fired 0 -
  fetch_results
  export_metrics 600
  summarize
  teardown_and_verify watchdog
  write_status "watchdog fired at the hard deadline"
  archive
  touch "$RUN_DIR/DONE"
}

# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------
# Minutes a plan needs from start to verified teardown: its phases at their
# estimates, provisioning (unless continuing on a cluster), and the reserve.
plan_need_minutes() {
  local existing="${1:-0}" total; total=$(plan_minutes)
  echo $(( total + (existing == 1 ? 0 : 35) + RESERVE_MIN ))
}

# The optional wall-clock cap in minutes, or empty for none. A run takes as
# long as its plan takes: by default nothing but the credentials limits it,
# and that limit is real - teardown needs live credentials - so the deadline
# (and the watchdog) sit at their expiry minus the teardown reserve, and start
# refuses a run they cannot cover. CAP_HOURS adds a spending limit on top.
cap_minutes() {
  [ -n "${CAP_HOURS:-}" ] && echo $(( CAP_HOURS * 60 ))
}

cmd_check() {
  local plan="$1"
  load_plan "$plan"
  local total; total=$(plan_minutes)
  echo "plan: $plan  topology=${TOPOLOGY:-<unset>}  deploy: ${DEPLOY_ARGS[*]:-<defaults>}"
  printf '  %-34s %5s  %s\n' PHASE MIN "WB ARGS"
  local i; for i in "${!PHASE_NAMES[@]}"; do
    printf '  %-34s %5s  %s\n' "${PHASE_NAMES[$i]}" "${PHASE_EST[$i]}" "${PHASE_CMD[$i]}"
  done
  local need cap; need=$(plan_need_minutes 0); cap=$(cap_minutes 0)
  echo "  phases: $total min; + provisioning ~25-35 min + teardown reserve ${RESERVE_MIN} min = ~${need} min"
  if [ -z "$cap" ]; then
    echo "  no cap: runs until the plan is done; credentials must last ~$(( need + 10 )) min (start checks)"
  elif [ "$need" -le "$cap" ]; then
    echo "  fits CAP_HOURS=${CAP_HOURS}"
  else
    echo "  WILL NOT FIT CAP_HOURS=${CAP_HOURS}: ~${need} min needed - later phases will be skipped for time"
  fi
}

cmd_start() {
  local existing=0
  [ "${1:-}" = "--existing" ] && { existing=1; shift; }
  local plan; plan="$(readlink -f "${1:?usage: autorun.sh start [--existing] PLAN}")"
  load_plan "$plan"
  # A new run never writes into an existing directory: the default name is
  # second-resolution and gets a suffix if taken. An explicit RUN_DIR that
  # already holds a run is only accepted to continue it (--existing), and
  # never once it is DONE.
  if [ -z "${RUN_DIR:-}" ]; then
    local base; base="$ROOT/results/run-$(date -u +%Y-%m-%d-%H%M%S)-$(basename "$plan" .plan)"
    RUN_DIR="$base"; local n=2
    while [ -e "$RUN_DIR" ]; do RUN_DIR="$base-$n"; n=$((n + 1)); done
  else
    [ -f "$RUN_DIR/DONE" ] && die "$RUN_DIR already holds a finished run; choose a new RUN_DIR"
    if [ -f "$RUN_DIR/phases.tsv" ] && [ "$existing" = 0 ]; then
      die "$RUN_DIR already holds a run; choose a new RUN_DIR, or use --existing to continue it on its cluster"
    fi
  fi
  mkdir -p "$RUN_DIR"
  [ -f "$RUN_DIR/RUN_STARTED_AT" ] || date +%s > "$RUN_DIR/RUN_STARTED_AT"
  if [ -f "$RUN_DIR/autorun.pid" ] && kill -0 "$(cat "$RUN_DIR/autorun.pid")" 2>/dev/null; then
    die "a run is already active in $RUN_DIR (pid $(cat "$RUN_DIR/autorun.pid"))"
  fi
  load_creds || die "cannot export AWS credentials - run: $(aws_login_hint)"
  local now cap exp hard need
  need=$(plan_need_minutes "$existing")
  now=$(date +%s); exp="$(creds_expiry_epoch)"
  hard=$(( exp - (RESERVE_MIN + 10) * 60 ))
  cap="$(cap_minutes "$existing")"
  [ -n "$cap" ] && [ $(( now + cap * 60 )) -lt "$hard" ] && hard=$(( now + cap * 60 ))
  [ $(( hard - now )) -gt $(( (RESERVE_MIN + 30) * 60 )) ] \
    || die "credentials expire $(date -u -d @"$exp" +%FT%TZ): not enough time for a run. Log in again: $(aws_login_hint)"
  # Credentials, not the plan, would set the deadline and the budget rule would
  # quietly skip the last phases. Say so before anything is spent.
  if [ $(( (hard - now) / 60 )) -lt "$need" ] && [ "${ALLOW_SHORT_RUN:-0}" != 1 ] && [ -z "${HARD_DEADLINE_EPOCH:-}" ]; then
    die "this plan needs ~${need} min, but credentials expiring $(date -u -d @"$exp" +%FT%TZ) leave $(( (hard - now) / 60 )) min.
       Log in again ($(aws_login_hint)) for a full run, or set ALLOW_SHORT_RUN=1 to run what fits."
  fi
  [ -n "${HARD_DEADLINE_EPOCH:-}" ] && hard="$HARD_DEADLINE_EPOCH"
  # Continuing a run whose watchdog is still armed: that watchdog's deadline
  # is the real one, so plan against it rather than against a fresh cap.
  if [ -f "$RUN_DIR/watchdog.pid" ] && kill -0 "$(cat "$RUN_DIR/watchdog.pid")" 2>/dev/null; then
    local wd=""
    [ -f "$RUN_DIR/DEADLINE" ] && wd="$(cat "$RUN_DIR/DEADLINE")"
    [ -n "$wd" ] || wd="$(sed -n 's/.*watchdog pid=[0-9]* armed for \([0-9TZ:-]*\).*/\1/p' "$RUN_DIR/autorun.log" 2>/dev/null | tail -1 | xargs -r -I{} date -d {} +%s)"
    [ -n "$wd" ] && hard="$wd"
  fi
  echo "$hard" > "$RUN_DIR/DEADLINE"
  echo "$plan" > "$RUN_DIR/PLAN"
  local e=(env RUN_DIR="$RUN_DIR" HARD_DEADLINE_EPOCH="$hard" WB="$WB" RESERVE_MIN="$RESERVE_MIN" GATE_FILE="$GATE_FILE" ARCHIVE_DIR="$ARCHIVE_DIR")
  # One watchdog per RUN_DIR: a --existing run on a dir whose watchdog is still
  # armed keeps that one (and its deadline).
  if [ -f "$RUN_DIR/watchdog.pid" ] && kill -0 "$(cat "$RUN_DIR/watchdog.pid")" 2>/dev/null; then
    echo "autorun: keeping the armed watchdog (pid $(cat "$RUN_DIR/watchdog.pid"))"
  else
    setsid nohup "${e[@]}" bash "$SELF" __watchdog > "$RUN_DIR/watchdog.out" 2>&1 < /dev/null &
  fi
  setsid nohup "${e[@]}" bash "$SELF" __run "$plan" "$existing" > "$RUN_DIR/run.out" 2>&1 < /dev/null &
  sleep 2
  echo "autorun started: $RUN_DIR"
  echo "  deadline $(date -u -d @"$hard" +%FT%TZ) (plan needs ~${need} min; ${CAP_HOURS:+cap ${CAP_HOURS}h, }credentials until $(date -u -d @"$exp" +%FT%TZ))"
  echo "  follow:  tail -f $RUN_DIR/autorun.log"
  echo "  status:  $SELF status $RUN_DIR"
}

cmd_status() {
  local d="${1:-$(ls -td "$ROOT"/results/run-* 2>/dev/null | head -1)}"
  [ -d "$d" ] || die "no run directory"
  echo "run: $d"; echo "plan: $(cat "$d/PLAN" 2>/dev/null)"
  local p
  for p in autorun watchdog; do
    if [ -f "$d/$p.pid" ] && kill -0 "$(cat "$d/$p.pid")" 2>/dev/null; then echo "$p: running (pid $(cat "$d/$p.pid"))"; else echo "$p: not running"; fi
  done
  [ -f "$d/DONE" ] && echo "DONE - teardown: $(cat "$d/TEARDOWN-RESULT" 2>/dev/null)"
  grep -E 'Grafana|Prometheus' "$d/MONITORING.txt" 2>/dev/null | sed 's/^ */watch: /'
  [ -f "$d/RESULTS.md" ] && echo "results: $d/RESULTS.md"
  echo "--- phases"; cat "$d/phases.tsv" 2>/dev/null
  echo "--- last log lines"; tail -5 "$d/autorun.log" 2>/dev/null
}

case "${1:-}" in
  check) shift; cmd_check "${1:?usage: autorun.sh check PLAN}" ;;
  start) shift; cmd_start "$@" ;;
  status) shift; cmd_status "$@" ;;
  __run)
    RUN_DIR="${RUN_DIR:?}"; HARD_DEADLINE_EPOCH="${HARD_DEADLINE_EPOCH:?}"
    PHASE_END=$(( HARD_DEADLINE_EPOCH - RESERVE_MIN * 60 ))
    run "$2" "$3" ;;
  __watchdog)
    RUN_DIR="${RUN_DIR:?}"; HARD_DEADLINE_EPOCH="${HARD_DEADLINE_EPOCH:?}"
    PHASE_END=$(( HARD_DEADLINE_EPOCH - RESERVE_MIN * 60 ))
    watchdog ;;
  *) sed -n '2,12p' "$SELF" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
