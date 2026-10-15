#!/usr/bin/env bash
# Offline test of switch-topology.sh's stale-overlay guard, run UNDER THE
# SCRIPT'S OWN SHELL OPTIONS (set -euo pipefail). The first version of this
# test ran the snippet without them and passed, while the real script died
# silently on the first overlay that did not set a key - `wb up` produced no
# output at all and provisioned nothing.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$HERE/../../scripts/switch-topology.sh"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
python3 - "$SRC" > "$T/guard.sh" <<'PY'
import sys
s=open(sys.argv[1]).read()
a=s.index('OVERLAY_MAX_AGE_DAYS="${OVERLAY_MAX_AGE_DAYS:-1}"')
b=s.index('[ "$_stale" = 0 ] || exit 1')+len('[ "$_stale" = 0 ] || exit 1')
print('set -euo pipefail\nTFVARS=topo.tfvars\n'+s[a:b]+'\necho GUARD_PASSED')
PY
cd "$T"
printf 'broker_count = 6\nclient_count = 3\nregion = "us-east-1"\n' > topo.tfvars
pass=0 fail=0
expect() { # expect <name> <pass|refuse>
  local out; out=$(bash guard.sh 2>&1)
  if { [ "$2" = pass ] && grep -q GUARD_PASSED <<<"$out"; } || \
     { [ "$2" = refuse ] && grep -q "^ERROR:" <<<"$out" && ! grep -q GUARD_PASSED <<<"$out"; }; then
    echo "PASS  $1"; pass=$((pass+1))
  else echo "FAIL  $1"; echo "$out" | sed 's/^/      /'; fail=$((fail+1)); fi
}
printf 'region = "us-west-2"\n' > region.auto.tfvars; printf 'client_count = 8\n' > clients.auto.tfvars
expect "fresh overlays, keys absent from some files" pass
touch -d '28 days ago' clients.auto.tfvars
expect "stale overlay that changes client_count" refuse
ALLOW_STALE_OVERLAYS=1 expect "stale overlay with explicit override" pass
printf 'client_count = 3\n' > clients.auto.tfvars; touch -d '28 days ago' clients.auto.tfvars
expect "stale overlay that agrees with topology" pass
echo "--- $pass passed, $fail failed"; [ "$fail" -eq 0 ]
