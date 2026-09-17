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

# One instant per WINDOW, not the midpoint of the whole recording. Windows are
# padded and separated by gaps, so the recording's midpoint can land after
# traffic stopped - which reports every latency panel as broken on a bundle
# that is entirely fine. A query passes if any window has its data.
AT=$(python3 -c "
import json
ws = json.load(open('manifest.json'))['windows']
print(','.join(str(int((w['start_ms'] + w['end_ms']) / 2000)) for w in ws))")

if ! curl -sf --max-time 5 http://localhost:9090/-/ready >/dev/null 2>&1; then
  echo "prometheus is not up on :9090 - start the recording first:" >&2
  echo "    docker compose up -d" >&2
  exit 1
fi

exec python3 check-dashboards.py http://localhost:9090 grafana/dashboards --at "$AT"
