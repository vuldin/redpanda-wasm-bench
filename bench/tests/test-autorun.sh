#!/usr/bin/env bash
# Dry run of scripts/autorun.sh against a stub wb: no cloud, about a minute.
#
#   1. check: a plan lists, quoting survives (a value with spaces stays one
#      argument), and @RUN_DIR@ is substituted at run time.
#   2. budget: with no time left, provisioning is skipped for time and the run
#      still ends in exactly one teardown + DONE.
#   3. retry: a phase that fails once is retried and recorded both ways.
#   4. --existing: no provisioning phases run.
#   5. watchdog: a phase that hangs past the hard deadline is killed by the
#      independent watchdog, which performs the single teardown - after
#      exporting the metrics.
#   6. observability: the Grafana URL is logged, metrics are exported BEFORE
#      teardown, and RESULTS.md is written.
#   7. pre_up: runs before `wb up`; a failing pre_up stops the run before any
#      provisioning.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
AUTORUN="$HERE/../../scripts/autorun.sh"
T="$(mktemp -d)"
pass=0 fail=0
check() { if eval "$2"; then echo "ok   $1"; pass=$((pass + 1)); else echo "FAIL $1"; fail=$((fail + 1)); fi; }

cat > "$T/wb" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$(date +%s) ARGC=$# $*" >> "$STUB_LOG"
case "$1" in
  run)
    case "$*" in
      *HANG*) sleep 600 ;;
      *FAILONCE*) if [ ! -f "$STUB_LOG.failed" ]; then touch "$STUB_LOG.failed"; exit 1; fi ;;
    esac ;;
  teardown) echo teardown >> "$STUB_LOG.teardowns" ;;
  monitor)
    case "$2" in
      status) echo "  Grafana     http://203.0.113.9:3000"; echo "  Prometheus  http://203.0.113.9:9090" ;;
      export) mkdir -p "$3/metrics-bundle" ;;
    esac ;;
  env) echo 'BROKER_PUB="1.1.1.1"' ;;
esac
exit 0
STUB
chmod +x "$T/wb"

cat > "$T/test.plan" <<'PLAN'
TOPOLOGY=single-az
DEPLOY_ARGS=(-e x=1)
phase first 1 run e2 'LEVELS=0 100' MARK
phase second 1 run durability FAILONCE=1 OUT=@RUN_DIR@/second
phase third 1 run e2 LAST
PLAN
sed 's/phase first 1 run e2/phase first 1 run e2 HANG=1/' "$T/test.plan" > "$T/hang.plan"

# ARCHIVE_DIR is pinned inside $T: autorun reads the user's wb.local.env, and an
# ARCHIVE_DIR set there would otherwise collect every self-test run.
base() { env DRY_RUN=1 EST_UNIT_S=0 WB="$T/wb" STUB_LOG="$1" RUN_DIR="$2" ARCHIVE_DIR="$T/archive" "${@:3}"; }

# 1: check
out="$(bash "$AUTORUN" check "$T/test.plan" 2>&1)"
check "check lists all phases" "echo \"\$out\" | grep -q 'third' && echo \"\$out\" | grep -q 'fits the cap'"

# 2: budget - deadline already passed, reserve 0
R2="$T/r2"; mkdir -p "$R2"
base "$T/s2" "$R2" RESERVE_MIN=0 HARD_DEADLINE_EPOCH=$(( $(date +%s) - 5 )) bash "$AUTORUN" __run "$T/test.plan" 0 >/dev/null 2>&1
check "budget: P0-up skipped for time" "grep -q $'P0-up\tskipped-time' '$R2/phases.tsv'"
check "budget: exactly one teardown and DONE" "[ \"\$(wc -l < '$T/s2.teardowns')\" = 1 ] && [ -f '$R2/DONE' ]"

# 3: retry + quoting + substitution
R3="$T/r3"; mkdir -p "$R3"
base "$T/s3" "$R3" RESERVE_MIN=0 HARD_DEADLINE_EPOCH=$(( $(date +%s) + 3600 )) bash "$AUTORUN" __run "$T/test.plan" 0 >/dev/null 2>&1
check "retry: 'second' failed once then OK" "grep -q $'second\tfail-rc1-attempt1' '$R3/phases.tsv' && grep -q $'second\tok' '$R3/phases.tsv'"
check "provisioning ran before phases (up, deploy, ready)" "grep -q 'ARGC=2 up single-az' '$T/s3' && grep -q 'deploy -e x=1' '$T/s3' && grep -q ' ready' '$T/s3'"
check "quoting: a spaced value stays one argument" "grep -q 'ARGC=4 run e2 LEVELS=0 100 MARK' '$T/s3'"
check "@RUN_DIR@ substituted" "grep -q 'OUT=$R3/second' '$T/s3'"
check "retry: one teardown" "[ \"\$(wc -l < '$T/s3.teardowns')\" = 1 ]"

# 6: observability on the normal path (run 3's logs)
check "observability: Grafana URL logged" "grep -q 'WATCH: Grafana http://203.0.113.9:3000' '$R3/autorun.log'"
check "observability: export ran before teardown" "awk '/ monitor export /{e=NR} / teardown /{t=NR} END{exit !(e && t && e < t)}' '$T/s3'"
check "observability: metrics-bundle and RESULTS.md written" "[ -d '$R3/metrics-bundle' ] && grep -q '^# Results' '$R3/RESULTS.md'"

# 7: pre_up
printf 'pre_up() { echo ran > "%s/preup-ran"; }\n' "$T" | cat - "$T/test.plan" > "$T/preup.plan"
printf 'pre_up() { return 3; }\n' | cat - "$T/test.plan" > "$T/preup-bad.plan"
R7="$T/r7"; mkdir -p "$R7"
base "$T/s7" "$R7" RESERVE_MIN=0 HARD_DEADLINE_EPOCH=$(( $(date +%s) + 3600 )) bash "$AUTORUN" __run "$T/preup.plan" 0 >/dev/null 2>&1
check "pre_up: ran before provisioning" "[ -f '$T/preup-ran' ] && grep -q 'ARGC=2 up single-az' '$T/s7'"
R8="$T/r8"; mkdir -p "$R8"
base "$T/s8" "$R8" RESERVE_MIN=0 HARD_DEADLINE_EPOCH=$(( $(date +%s) + 3600 )) bash "$AUTORUN" __run "$T/preup-bad.plan" 0 >/dev/null 2>&1
check "pre_up: a failure provisions nothing" "! grep -q ' up ' '$T/s8' 2>/dev/null && grep -q 'pre_up step failed' '$R8/autorun.log'"

# 4: --existing
R4="$T/r4"; mkdir -p "$R4"
base "$T/s4" "$R4" RESERVE_MIN=0 HARD_DEADLINE_EPOCH=$(( $(date +%s) + 3600 )) bash "$AUTORUN" __run "$T/test.plan" 1 >/dev/null 2>&1
check "--existing: no up/deploy" "! grep -qE ' (up|deploy) ' '$T/s4'"
check "--existing: phases ran" "grep -q $'third\tok' '$R4/phases.tsv'"

# 5: watchdog. RESERVE_MIN=-5 puts the phase window 5 minutes PAST the hard
# deadline, so the hanging phase's own timeout cannot end it first.
R5="$T/r5"
base "$T/s5" "$R5" RESERVE_MIN=-5 HARD_DEADLINE_EPOCH=$(( $(date +%s) + 30 )) bash "$AUTORUN" start "$T/hang.plan" >/dev/null 2>&1
for _ in $(seq 1 90); do [ -f "$R5/DONE" ] && break; sleep 1; done
check "watchdog: run marked DONE" "[ -f '$R5/DONE' ]"
check "watchdog: it was the watchdog that tore down" "grep -q 'watchdog: TEARDOWN' '$R5/autorun.log'"
check "watchdog: metrics exported before its teardown" "awk '/ monitor export /{e=NR} / teardown /{t=NR} END{exit !(e && t && e < t)}' '$T/s5'"
check "watchdog: exactly one teardown" "[ \"\$(wc -l < '$T/s5.teardowns' 2>/dev/null)\" = 1 ]"
check "watchdog: run process is gone" "! kill -0 \$(cat '$R5/autorun.pid') 2>/dev/null"

# 8: start without RUN_DIR picks results/run-<date>-<plan>, never the
# placeholder plans see while loading (it once became the run directory).
cp "$T/test.plan" "$T/autorun-selftest.plan"
before="$(ls -d "$HERE"/../../results/run-*-autorun-selftest 2>/dev/null)"
( unset RUN_DIR; cd "$T" && env DRY_RUN=1 EST_UNIT_S=0 WB="$T/wb" STUB_LOG="$T/s9" RESERVE_MIN=0 ARCHIVE_DIR="$T/archive" \
    HARD_DEADLINE_EPOCH=$(( $(date +%s) + 120 )) bash "$AUTORUN" start "$T/autorun-selftest.plan" >/dev/null 2>&1 )
R9="$(ls -d "$HERE"/../../results/run-*-autorun-selftest 2>/dev/null | grep -vxF "$before" | head -1)"
for _ in $(seq 1 60); do [ -n "$R9" ] && [ -f "$R9/DONE" ] && break; sleep 1; done
check "start: default RUN_DIR is results/run-<date>-<plan>" "[ -n '$R9' ] && [ -f '$R9/DONE' ]"
check "start: no placeholder directory created" "[ ! -e '$T/<run-dir>' ]"
[ -n "$R9" ] && [ -f "$R9/DONE" ] && case "$R9" in */results/run-*-autorun-selftest) rm -rf -- "$R9" ;; esac

# 9: a new run never reuses a directory that already holds one
R10="$T/r10"; mkdir -p "$R10"; touch "$R10/phases.tsv"
out="$(env DRY_RUN=1 RUN_DIR="$R10" WB="$T/wb" STUB_LOG="$T/s10" ARCHIVE_DIR="$T/archive" bash "$AUTORUN" start "$T/test.plan" 2>&1)"
check "start: refuses an explicit RUN_DIR that holds a run" "echo \"\$out\" | grep -q 'already holds a run'"

echo "$pass passed, $fail failed  (scratch: $T)"
[ "$fail" -eq 0 ]
