#!/usr/bin/env bash
# Dry run of scripts/autorun.sh against a stub wb: no cloud, a few seconds.
#
#   1. budget rule: with a short phase window most phases are SKIPPED for
#      time, and the run still ends in exactly one teardown + DONE.
#   2. retry: a phase that fails once is retried, and the retry is recorded.
#   3. watchdog: with a phase that hangs past the hard deadline, the
#      independent watchdog kills the run and performs the (single) teardown.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
AUTORUN="$HERE/../../scripts/autorun.sh"
T="$(mktemp -d)"
pass=0 fail=0
check() { if eval "$2"; then echo "ok   $1"; pass=$((pass + 1)); else echo "FAIL $1"; fail=$((fail + 1)); fi; }

cat > "$T/wb" <<'STUB'
#!/usr/bin/env bash
echo "$(date +%s) $*" >> "$STUB_LOG"
case "$1" in
  run)
    case "$*" in
      *HANG*) sleep 600 ;;
      *FAILONCE*) if [ ! -f "$STUB_LOG.failed" ]; then touch "$STUB_LOG.failed"; exit 1; fi ;;
    esac
    sleep "${STUB_PHASE_S:-0}" ;;
  teardown) echo teardown >> "$STUB_LOG.teardowns" ;;
  env) echo 'BROKER_PUB="1.1.1.1"' ;;
esac
exit 0
STUB
chmod +x "$T/wb"

# 1: budget rule. The phase window is ~3 minutes, so P0 (25+25+10 min of
# estimates) cannot even start: everything is skipped and teardown still runs.
R1="$T/r1"; mkdir -p "$R1"
DRY_RUN=1 WB="$T/wb" STUB_LOG="$T/stub1.log" RUN_DIR="$R1" RESERVE_MIN=1 \
  HARD_DEADLINE_EPOCH=$(( $(date +%s) + 240 )) bash "$AUTORUN" main >/dev/null 2>&1
check "budget: P0-up skipped for time" "grep -q $'P0-up\tskipped-time' '$R1/phases.tsv'"
check "budget: teardown ran exactly once" "[ \"\$(wc -l < '$T/stub1.log.teardowns')\" = 1 ]"
check "budget: DONE and STATUS written" "[ -f '$R1/DONE' ] && grep -q 'provisioning failed' '$R1/STATUS.md'"

# 2: a long window, every phase quick; one phase fails once and is retried.
R2="$T/r2"; mkdir -p "$R2"
sed 's/run_phase P3-kill 30 "$WB" run durability /run_phase P3-kill 30 "$WB" run durability FAILONCE=1 /' "$AUTORUN" > "$T/autorun2.sh"
DRY_RUN=1 WB="$T/wb" STUB_LOG="$T/stub2.log" RUN_DIR="$R2" RESERVE_MIN=1 \
  HARD_DEADLINE_EPOCH=$(( $(date +%s) + 86400 )) bash "$T/autorun2.sh" main >/dev/null 2>&1
check "retry: P3-kill failed once then OK" "grep -q $'P3-kill\tfail-rc1-attempt1' '$R2/phases.tsv' && grep -q $'P3-kill\tok' '$R2/phases.tsv'"
check "retry: every phase ran" "[ \"\$(grep -c $'\tok\t' '$R2/phases.tsv')\" -ge 14 ]"
check "retry: one teardown" "[ \"\$(wc -l < '$T/stub2.log.teardowns')\" = 1 ]"

# 3: watchdog. P1a hangs; the hard deadline is 40s out and the phase cap is
# far beyond it, so only the watchdog can end this run.
R3="$T/r3"; mkdir -p "$R3"
sed 's/run_phase P1a-depth-nocancel 10 "$WB" run e2 /run_phase P1a-depth-nocancel 10 "$WB" run e2 HANG=1 /' "$AUTORUN" > "$T/autorun3.sh"
DL=$(( $(date +%s) + 40 ))
# RESERVE_MIN=-5 puts the phase window 5 minutes PAST the hard deadline, so
# the hanging phase's own timeout cannot end it first - only the watchdog can.
DRY_RUN=1 EST_UNIT_S=0 WB="$T/wb" STUB_LOG="$T/stub3.log" RUN_DIR="$R3" RESERVE_MIN=-5 HARD_DEADLINE_EPOCH="$DL" \
  bash "$T/autorun3.sh" start >/dev/null 2>&1
for _ in $(seq 1 90); do [ -f "$R3/DONE" ] && break; sleep 1; done
check "watchdog: run marked DONE" "[ -f '$R3/DONE' ]"
check "watchdog: it was the watchdog that tore down" "grep -q 'watchdog: TEARDOWN' '$R3/autorun.log'"
check "watchdog: exactly one teardown" "[ \"\$(wc -l < '$T/stub3.log.teardowns' 2>/dev/null)\" = 1 ]"
check "watchdog: main process is gone" "! kill -0 \$(cat '$R3/autorun.pid') 2>/dev/null"

echo "$pass passed, $fail failed  (scratch: $T)"
[ "$fail" -eq 0 ]
