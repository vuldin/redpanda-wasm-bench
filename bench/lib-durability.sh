# Helpers for run-durability.sh that need no cluster, so bench/tests can
# source them on their own.

# resume_lines FILE: one "RESUME <broker> input=X state=Y committed=Z" per
# distinct restart a broker logged in FILE (lines prefixed with the broker's
# name, as run-durability's broker-state-lines.txt is): the input offset the
# processor resumed at, the restored checkpoint's own offset, and where the
# committed outputs would have resumed it before the envelope (josh-426).
resume_lines() {
  sed -nE 's/^(broker[0-9]+) .*resuming at input offset ([0-9]+), after the restored state.s ([0-9]+) \(committed outputs resume at ([0-9]+)\).*/RESUME \1 input=\2 state=\3 committed=\4/p' \
    "$1" 2>/dev/null | sort -u
}
