#!/usr/bin/env bash
# Check that every panel in this recording actually returns data.
#
# --at is computed from the manifest and is not optional: a recording's newest
# sample is older than Prometheus's five-minute instant-query lookback, so
# every query evaluated at `now` comes back empty and the validator would
# condemn a perfectly good bundle. Measured on a real bundle: 51 queries
# returning data with --at, against 0 returning and 9 failing without it.
set -uo pipefail
cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

AT=$(python3 -c "import json; m=json.load(open('manifest.json'))['recording']; print(int((m['start_ms'] + m['end_ms']) / 2000))")

if ! curl -sf --max-time 5 http://localhost:9090/-/ready >/dev/null 2>&1; then
  echo "prometheus is not up on :9090 - start the recording first:" >&2
  echo "    docker compose up -d" >&2
  exit 1
fi

exec python3 check-dashboards.py http://localhost:9090 grafana/dashboards --at "$AT"
