#!/usr/bin/env bash
# Offline test of how a durability trial reports where a killed transform
# resumed (josh-426): resume_lines over broker-state-lines.txt, and the
# "resumed from" column summarize-run.py builds from the RESUME lines. The
# broker lines are verbatim from the 2026-10-17 kill run, one with the
# checkpoint ahead of the committed outputs and one behind them. Exists
# because that run's evidence grep matched no resume line at all.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib-durability.sh
source "$HERE/../lib-durability.sh"

pass=0 fail=0
check() { # name expected actual
  if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "PASS $1"
  else fail=$((fail + 1)); echo "FAIL $1"; echo "  expected: $2"; echo "  actual:   $3"; fi
}

T="$(mktemp -d)"
trap 'rm -r -- "$T"' EXIT

cat > "$T/ahead.broker-state-lines.txt" <<'EOF'
broker3 Oct 17 06:35:04 ip-10-0-0-15 rpk[5819]: INFO  2026-10-17 06:35:04,589 [shard 10:tran] transform - bm-du-1792218814/0 - transform_processor.cc:246 - resuming at input offset 251920, after the restored state's 251919 (committed outputs resume at 231944)
broker3 Oct 17 06:35:04 ip-10-0-0-15 rpk[5819]: INFO  2026-10-17 06:35:04,589 [shard 10:tran] transform - bm-du-1792218814/0 - transform_processor.cc:246 - resuming at input offset 251920, after the restored state's 251919 (committed outputs resume at 231944)
broker3 Oct 17 06:35:04 ip-10-0-0-15 rpk[5819]: INFO  2026-10-17 06:35:04,100 [shard 10:tran] transform - bm-du-1792218814/0 - transform_processor.cc:180 - restored a persisted guest-state checkpoint
EOF
check "one RESUME per distinct restart, duplicates dropped" \
  "RESUME broker3 input=251920 state=251919 committed=231944" \
  "$(resume_lines "$T/ahead.broker-state-lines.txt")"

cat > "$T/behind.broker-state-lines.txt" <<'EOF'
broker0 Oct 17 07:23:03 ip-10-0-0-7 rpk[15683]: INFO  2026-10-17 07:23:03,798 [shard 10:tran] transform - bm-du-1792221727/0 - transform_processor.cc:246 - resuming at input offset 201884, after the restored state's 201883 (committed outputs resume at 231930)
EOF
check "a checkpoint behind the committed outputs" \
  "RESUME broker0 input=201884 state=201883 committed=231930" \
  "$(resume_lines "$T/behind.broker-state-lines.txt")"

: > "$T/none.broker-state-lines.txt"
check "no restart logged: nothing" "" "$(resume_lines "$T/none.broker-state-lines.txt")"
check "a missing file: nothing, no error" "" "$(resume_lines "$T/absent.txt")"

# summarize-run.py: a durability summary with one trial of each.
R="$T/run-test"
mkdir -p "$R/kill"
{
  echo "# durability actions=kill depths=1000"
  echo "############ kill-d1000-snapshot-t1-1 : action=kill"
  echo "  VERDICT kill-d1000-snapshot-t1-1 placed=100 recovered=100 lost=0 duplicated=0 strays=0 ok=true waves=10s:20/20,0.3s:20/20"
  resume_lines "$T/ahead.broker-state-lines.txt" | sed 's/^/  /'
  echo "############ kill-d1000-snapshot-t2-2 : action=kill"
  echo "  VERDICT kill-d1000-snapshot-t2-2 placed=100 recovered=100 lost=0 duplicated=0 strays=0 ok=true waves=10s:20/20,0.3s:20/20"
  resume_lines "$T/behind.broker-state-lines.txt" | sed 's/^/  /'
  echo "############ kill-d1000-snapshot-t3-3 : action=kill"
  echo "  VERDICT kill-d1000-snapshot-t3-3 placed=100 recovered=100 lost=0 duplicated=0 strays=0 ok=true waves=10s:20/20,0.3s:20/20"
} > "$R/kill/summary.txt"
python3 "$HERE/../../scripts/summarize-run.py" "$R" > "$R/RESULTS.md" 2>"$T/err" || cat "$T/err"
row() { grep -E "^\| $1 " "$R/RESULTS.md" | awk -F'|' '{gsub(/^ +| +$/, "", $(NF-1)); print $(NF-1)}'; }
check "ahead: state offset and +distance" "broker3: state 251,919 (+19,976 vs committed)" "$(row kill-d1000-snapshot-t1-1)"
check "behind: state offset and -distance" "broker0: state 201,883 (-30,046 vs committed)" "$(row kill-d1000-snapshot-t2-2)"
check "no RESUME line: a dash" "-" "$(row kill-d1000-snapshot-t3-3)"

echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
