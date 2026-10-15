# shellcheck shell=bash
# Shared settings for every bench script. Source it; don't run it.
#
# Order: anything already in the environment wins, then your wb.local.env
# (gitignored; copy wb.local.env.example), then the defaults below. The
# defaults assume nothing about the machine beyond the three repos being
# cloned side by side:
#
#   <dir>/redpanda-wasm-bench     this repo
#   <dir>/redpanda                the broker (transform-latency-instrumentation)
#   <dir>/redpanda-wasm-clients   the guests and the external host
#
# There is deliberately NO default for the AWS profile or the ssh keypair -
# those are yours. With AWS_PROFILE unset the AWS CLI uses its normal
# credential chain; the keys must be set before anything reaches a cluster.

_BENCH_ROOT="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)"
_BENCH_PARENT="$(cd "$_BENCH_ROOT/.." && pwd)"
# shellcheck disable=SC1091
[ -f "$_BENCH_ROOT/wb.local.env" ] && . "$_BENCH_ROOT/wb.local.env"

: "${REDPANDA_SRC:=$_BENCH_PARENT/redpanda}"
: "${WASM_CLIENTS_SRC:=$_BENCH_PARENT/redpanda-wasm-clients}"
: "${BENCH_SSH_KEY:=}"
: "${BENCH_PUB_KEY:=}"
export REDPANDA_SRC WASM_CLIENTS_SRC BENCH_SSH_KEY BENCH_PUB_KEY
if [ -n "${AWS_PROFILE:-}" ]; then export AWS_PROFILE; fi

# "--profile X" for aws commands when a profile is set, nothing otherwise
# (the CLI then reads AWS_PROFILE / its default chain itself).
aws_login_hint() { echo "aws sso login${AWS_PROFILE:+ --profile $AWS_PROFILE}"; }

# Call before anything that ssh-es to, or provisions, a cluster.
require_bench_keys() {
  local miss=""
  [ -n "$BENCH_SSH_KEY" ] || miss="$miss BENCH_SSH_KEY"
  [ -n "$BENCH_PUB_KEY" ] || miss="$miss BENCH_PUB_KEY"
  if [ -n "$miss" ]; then
    echo "not set:$miss - put your EC2 keypair's paths in $_BENCH_ROOT/wb.local.env (see wb.local.env.example)" >&2
    return 1
  fi
}
