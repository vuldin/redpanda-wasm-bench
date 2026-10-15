#!/usr/bin/env bash
# Unattended run: provision -> deploy -> phases -> fetch/archive -> teardown
# -> verify, with a hard wall-clock cap enforced by an INDEPENDENT watchdog.
#
# Built to run with nobody watching, so every decision is written down:
#
#   - Nothing is provisioned unless the offline gate passed for the commit
#     being run (GATE_FILE holds the tested SHAs) and the AWS credentials
#     outlive the hard deadline by RESERVE_MIN.
#   - Each phase has an estimate. A phase starts only if the time left before
#     PHASE_END leaves room for it; otherwise it, and everything after it, is
#     skipped and recorded.
#   - A failed phase is retried once (if time allows), then skipped.
#   - Between phases the cluster is put back into a known state rather than
#     trusting the previous runner's traps - a timed-out runner never ran them.
#   - The end is always: fetch -> archive -> teardown -> EC2 verify (retried)
#     -> STATUS.md. The watchdog does the same if this process is gone or
#     over time at HARD_DEADLINE.
#
# usage: autorun.sh start   # detaches this run and its watchdog, returns
#        autorun.sh main    # the run itself (what `start` launches)
#        autorun.sh watchdog
#
# Environment: RUN_DIR (required), HARD_DEADLINE_EPOCH (required), WB, GATE_FILE,
# DRY_RUN=1 (stub mode, see bench/tests/test-autorun.sh).
set -uo pipefail

HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
WB="${WB:-$ROOT/scripts/wb}"
RUN_DIR="${RUN_DIR:?set RUN_DIR}"
HARD_DEADLINE_EPOCH="${HARD_DEADLINE_EPOCH:?set HARD_DEADLINE_EPOCH (unix seconds)}"
# Teardown + verify + archive budget, reserved off the end of the run.
RESERVE_MIN="${RESERVE_MIN:-25}"
PHASE_END=$(( HARD_DEADLINE_EPOCH - RESERVE_MIN * 60 ))
GATE_FILE="${GATE_FILE:-$RUN_DIR/GATE-PASSED}"
ARCHIVE_DIR="${ARCHIVE_DIR:-}"
mkdir -p "$RUN_DIR"
LOG="$RUN_DIR/autorun.log"
STATUS="$RUN_DIR/STATUS.md"
PHASES="$RUN_DIR/phases.tsv"

log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }
left() { echo $(( PHASE_END - $(date +%s) )); }
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$1" "$2" "$3" "$4" >> "$PHASES"; }

# ---------------------------------------------------------------------------
# credentials: export once, so the run survives the SSO token expiring. The
# role credentials have their own (longer) expiry; that is the real limit.
# ---------------------------------------------------------------------------
load_creds() {
  [ "${DRY_RUN:-0}" = 1 ] && return 0
  local c
  c="$(aws configure export-credentials --profile "${AWS_PROFILE:-sandbox-cse}" --format env 2>/dev/null)" || return 1
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
# run_phase NAME EST_MIN CMD...: budget check, timeout at 1.5x the estimate,
# one retry. Returns 0 on success, 1 failed, 2 skipped for time.
run_phase() {
  local name="$1" est="$2"; shift 2
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
    log "START $name attempt $attempt (est ${est}min, cap $((cap / 60))min)"
    local t0; t0=$(date +%s)
    timeout --kill-after=60 "$cap" "$@" >> "$RUN_DIR/$name.log" 2>&1
    rc=$?
    local dt=$(( $(date +%s) - t0 ))
    if [ "$rc" -eq 0 ]; then
      log "OK $name in $((dt / 60))m$((dt % 60))s"
      record "$name" ok "$est" "$dt"
      return 0
    fi
    log "FAIL $name attempt $attempt rc=$rc after $((dt / 60))m$((dt % 60))s (tail of $name.log below)"
    tail -5 "$RUN_DIR/$name.log" | sed 's/^/    /' | tee -a "$LOG" >/dev/null
    record "$name" "fail-rc$rc-attempt$attempt" "$est" "$dt"
    [ "$attempt" = 1 ] && [ "$(left)" -lt "$need" ] && { log "no time to retry $name"; break; }
    [ "$attempt" = 1 ] && hygiene
  done
  return 1
}

# Put the cluster back into a known state between phases. Idempotent; never
# fatal. Re-asserts the config the runners' traps would have restored.
hygiene() {
  [ "${DRY_RUN:-0}" = 1 ] && { log "hygiene (dry run)"; return 0; }
  log "hygiene: deleting transforms + test topics, clearing maintenance, re-enabling balancers"
  local env; env="$("$WB" env 2>/dev/null | grep -E '^(BROKERS|ADMIN_URL|ADMIN_URLS)=')"
  eval "$env"
  local hosts; hosts="$(echo "$ADMIN_URLS" | sed 's|http://||g')"
  local R="./rpk -X brokers=$BROKERS -X admin.hosts=$hosts"
  # A timed-out phase leaves its remote runner going. The [t] keeps each
  # pattern from matching this very ssh command line, which pkill -f would
  # otherwise kill along with its target.
  timeout 300 "$WB" ssh client0 "cd /opt/wasm-bench && pkill -f 'run-e2-throughpu[t]'; pkill -x loadgen; pkill -x statecheck; \
    for t in \$($R transform list 2>/dev/null | awk 'NR>1{print \$1}'); do $R transform delete \$t --no-confirm >/dev/null 2>&1; done; \
    sleep 3; \
    for t in \$($R topic list 2>/dev/null | awk 'NR>1 && \$1 ~ /^(orders|fills|probe)/ {print \$1}'); do $R topic delete \$t >/dev/null 2>&1; done; \
    for n in \$(curl -sf $ADMIN_URL/v1/brokers | jq -r '.[].node_id'); do $R cluster maintenance disable \$n >/dev/null 2>&1; done; \
    $R cluster config set enable_leader_balancer true >/dev/null 2>&1; \
    $R cluster config set core_balancing_continuous true >/dev/null 2>&1; true" >> "$LOG" 2>&1 || true
  # Any broker a killed kill-trial left stopped.
  local n i; n=$(echo "$("$WB" env 2>/dev/null | grep '^BROKER_PUB=' | cut -d'"' -f2)" | wc -w)
  for ((i = 0; i < n; i++)); do
    timeout 60 "$WB" ssh "broker$i" "systemctl is-active --quiet redpanda || sudo systemctl start redpanda" >/dev/null 2>&1 || true
  done
}

fetch_results() {
  log "fetching client-side results"
  if [ "${DRY_RUN:-0}" = 1 ]; then "$WB" results "$RUN_DIR/client-results" >> "$LOG" 2>&1; return 0; fi
  timeout 600 "$WB" results "$RUN_DIR/client-results" >> "$LOG" 2>&1 || log "WARN: results fetch failed or timed out"
}

archive() {
  [ -n "$ARCHIVE_DIR" ] || return 0
  log "archiving to $ARCHIVE_DIR"
  mkdir -p "$ARCHIVE_DIR"
  local base; base="$(basename "$RUN_DIR")"
  cp -a "$RUN_DIR" "$ARCHIVE_DIR/" 2>>"$LOG" || log "WARN: archive copy failed"
  tar -C "$(dirname "$RUN_DIR")" -czf "$ARCHIVE_DIR/$base.tar.gz" "$base" 2>>"$LOG" \
    && (cd "$ARCHIVE_DIR" && sha256sum "$base.tar.gz" > "$base.tar.gz.sha256" && sha256sum -c "$base.tar.gz.sha256" >> "$LOG" 2>&1) \
    || log "WARN: tarball failed"
}

# Exactly one teardown, whoever gets here first (main run or watchdog).
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
    echo "- finished: $(date -u +%FT%TZ) ($why)"
    echo "- teardown: $(cat "$RUN_DIR/TEARDOWN-RESULT" 2>/dev/null || echo 'not run')"
    echo "- hard deadline: $(date -u -d @"$HARD_DEADLINE_EPOCH" +%FT%TZ), phase end $(date -u -d @"$PHASE_END" +%FT%TZ)"
    if [ -f "$RUN_DIR/PROVISIONED_AT" ]; then
      local p0; p0=$(cat "$RUN_DIR/PROVISIONED_AT")
      local p1; p1=$(stat -c %Y "$RUN_DIR/TEARDOWN-RESULT" 2>/dev/null || date +%s)
      echo "- cluster lifetime: $(( (p1 - p0) / 60 )) min (provision start to teardown end)"
    fi
    echo ""
    echo "| time | phase | outcome | est min | seconds |"
    echo "|---|---|---|---|---|"
    awk -F'\t' '{printf "| %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5}' "$PHASES" 2>/dev/null
    echo ""
    echo "Logs: autorun.log, <phase>.log, teardown.log. Results: the phase directories and client-results/."
  } > "$STATUS"
}

# ---------------------------------------------------------------------------
# the run
# ---------------------------------------------------------------------------
main() {
  echo $$ > "$RUN_DIR/autorun.pid"
  log "autorun main pid=$$ hard_deadline=$(date -u -d @"$HARD_DEADLINE_EPOCH" +%FT%TZ) phase_end=$(date -u -d @"$PHASE_END" +%FT%TZ)"

  # Gate: refuse to spend anything on code the offline gate did not test.
  local heads
  heads="bench=$(git -C "$ROOT" rev-parse HEAD) clients=$(git -C "${CLIENTS_REPO:-$ROOT/../redpanda-wasm-clients}" rev-parse HEAD) broker=$(git -C "${REDPANDA_SRC:-$HOME/redpanda/redpanda}" rev-parse HEAD)"
  if [ "${DRY_RUN:-0}" != 1 ] && ! grep -qxF "$heads" "$GATE_FILE" 2>/dev/null; then
    log "REFUSING: offline gate has not passed for $heads (GATE_FILE=$GATE_FILE)"
    record gate refused 0 -; write_status "offline gate not passed - nothing provisioned"; touch "$RUN_DIR/DONE"; return 1
  fi
  if [ "${DRY_RUN:-0}" != 1 ] && [ -n "$(git -C "$ROOT" status --porcelain -- bench scripts cmd ansible)" ]; then
    log "REFUSING: uncommitted changes in the bench repo"
    record gate refused-dirty 0 -; write_status "uncommitted bench changes - nothing provisioned"; touch "$RUN_DIR/DONE"; return 1
  fi
  load_creds || { log "REFUSING: cannot export AWS credentials"; record creds refused 0 -; write_status "no AWS credentials"; touch "$RUN_DIR/DONE"; return 1; }
  local exp; exp="$(creds_expiry_epoch)"
  if [ "$exp" -lt $(( HARD_DEADLINE_EPOCH + RESERVE_MIN * 60 )) ]; then
    log "REFUSING: credentials expire $(date -u -d @"$exp" +%FT%TZ), before hard deadline + reserve"
    record creds refused-expiry 0 -; write_status "credentials expire too soon"; touch "$RUN_DIR/DONE"; return 1
  fi
  log "credentials valid until $(date -u -d @"$exp" +%FT%TZ)"
  record gate ok 0 "$heads"

  date +%s > "$RUN_DIR/PROVISIONED_AT"
  if ! run_phase P0-up 25 "$WB" up six-broker; then
    log "provisioning failed - straight to teardown"
    finish "provisioning failed"; return 1
  fi
  if ! run_phase P0-deploy 25 "$WB" deploy -e transform_memory_limit=16777216 \
         -e transform_instances_per_core=40 -e transform_snapshot_max_size=16777216; then
    finish "deploy failed"; return 1
  fi
  run_phase P0-ready 10 "$WB" ready || { hygiene; run_phase P0-ready-2 10 "$WB" ready || { finish "cluster never ready"; return 1; }; }
  "$WB" doctor >> "$RUN_DIR/doctor.log" 2>&1 || log "WARN: wb doctor reported problems (see doctor.log)"

  local E2="ORDERS_PER_RECORD=15 RATE=30000 FANOUT=20 PAYLOAD=650 ACKS=all RF=3 DURATION=30s WARMUP=10s DRAIN=30s RELAY_STAGE_METRICS=true WRITE_CACHING=true LINGER_MS=0 SAMPLE_EVERY=10 PRODUCERS=1 MODE=bookdepth GUEST=book-matcher"
  local t
  # P1: depth with the new book. a) same traffic as the reference run;
  # b) with insert+cancel (the headline); c) with snapshots on.
  # shellcheck disable=SC2086
  run_phase P1a-depth-nocancel 10 "$WB" run e2 $E2 STATE=off CANCEL_FRACTION=0 BOOKDEPTH_LEVELS="0 100 1000 10000"
  hygiene
  for t in 1 2 3; do
    run_phase "P1b-depth-cancel-t$t" 10 "$WB" run e2 $E2 STATE=off CANCEL_FRACTION=0.3 BOOKDEPTH_LEVELS="0 100 1000 10000"
    hygiene
  done
  for t in 1 2; do
    run_phase "P1c-depth-snapshot-t$t" 6 "$WB" run e2 $E2 STATE=snapshot CANCEL_FRACTION=0.3 BOOKDEPTH_LEVELS="1000 10000"
    hygiene
  done
  fetch_results
  # P2: planned moves, with the replay arm after each transfer trial.
  run_phase P2-planned-moves 45 "$WB" run durability ACTIONS="maintenance transfer" DEPTHS="1000 10000" TRIALS=3 \
    REPLAY=1 REPLAY_ACTION=transfer OUT="$RUN_DIR/p2-planned-moves"
  hygiene
  # P3: unplanned failure - the snapshot/offset window.
  run_phase P3-kill 30 "$WB" run durability ACTIONS=kill DEPTHS="1000 10000" TRIALS=3 OUT="$RUN_DIR/p3-kill"
  hygiene
  # P3b: the no-durability control, one kill each depth.
  run_phase P3b-kill-nostate 8 "$WB" run durability ACTIONS=kill DEPTHS="1000 10000" TRIALS=1 STATE=off OUT="$RUN_DIR/p3b-kill-nostate"
  hygiene
  # P5: lowest priority - the reference guest under the same cancel traffic.
  # shellcheck disable=SC2086
  run_phase P5-reference-cancel 9 "$WB" run e2 ${E2/GUEST=book-matcher/GUEST=mass-matcher} CANCEL_FRACTION=0.3 BOOKDEPTH_LEVELS="0 100 1000"
  finish "all phases attempted"
}

finish() {
  local why="$1"
  fetch_results
  write_status "$why (pre-teardown)"
  archive
  teardown_and_verify main
  write_status "$why"
  archive
  touch "$RUN_DIR/DONE"
  log "DONE: $why"
}

# The independent cap. Sleeps until the hard deadline, then - if the run has
# not finished - kills it, fetches what it can, and tears down. It does not
# depend on the main process being alive, which is the point.
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
      log "watchdog: main is already tearing down; giving it 20 more minutes"
      local i; for i in $(seq 1 60); do [ -f "$RUN_DIR/DONE" ] && return 0; sleep 20; done
    fi
    kill -TERM -- "-$(ps -o pgid= "$pid" | tr -d ' ')" 2>/dev/null || kill -TERM "$pid" 2>/dev/null
    sleep 10
    kill -KILL "$pid" 2>/dev/null || true
  fi
  rmdir "$RUN_DIR/.teardown.lock" 2>/dev/null || true
  record watchdog fired 0 -
  if [ "${DRY_RUN:-0}" = 1 ]; then "$WB" results "$RUN_DIR/client-results" >> "$LOG" 2>&1
  else timeout 600 "$WB" results "$RUN_DIR/client-results" >> "$LOG" 2>&1 || true; fi
  teardown_and_verify watchdog
  write_status "watchdog fired at the hard deadline"
  archive
  touch "$RUN_DIR/DONE"
}

case "${1:-}" in
  main) main ;;
  watchdog) watchdog ;;
  start)
    # Separate sessions: neither dies with this shell, nor with each other.
    setsid nohup env RUN_DIR="$RUN_DIR" HARD_DEADLINE_EPOCH="$HARD_DEADLINE_EPOCH" WB="$WB" bash "$0" watchdog \
      > "$RUN_DIR/watchdog.out" 2>&1 < /dev/null &
    setsid nohup env RUN_DIR="$RUN_DIR" HARD_DEADLINE_EPOCH="$HARD_DEADLINE_EPOCH" WB="$WB" bash "$0" main \
      > "$RUN_DIR/main.out" 2>&1 < /dev/null &
    sleep 2
    echo "autorun started: main pid $(cat "$RUN_DIR/autorun.pid" 2>/dev/null) watchdog pid $(cat "$RUN_DIR/watchdog.pid" 2>/dev/null)"
    echo "  log: $LOG" ;;
  *) echo "usage: autorun.sh start|main|watchdog" >&2; exit 2 ;;
esac
