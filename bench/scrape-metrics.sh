#!/usr/bin/env bash
# Before/after resource snapshots for a benchmark window.
#
# Why this is separate from check-bottlenecks.sh: that script is a single-shot,
# per-topic diagnostic (shard placement + reader/batch cache ratios for a named
# topic list) and its contract is depended on by the existing run-*.sh scripts.
# This one is a two-shot, cluster-wide delta over whole metric families keyed by
# function_name / scheduling_group / shard. Same broker, different question, so
# it is its own tool rather than a mode bolted onto that one. Run both: this for
# resource attribution, that one for fetch-path bottlenecks.
#
# cmd/loadgen already scrapes the transform/wasm/relay families itself so that
# each run's JSON is self-describing. This script exists for the wider view
# loadgen deliberately skips - per-shard, per-scheduling-group CPU, which is how
# the G18 connection-acceptance tax was originally found, and which is the only
# way to see transform work competing with fetch/produce on a given core.
#
# Usage:
#   scrape-metrics.sh snap  <admin-url> <out.json>
#   scrape-metrics.sh delta <before.json> <after.json> [<orders>] [<out.json>]
#
# Typical:
#   scrape-metrics.sh snap http://broker:9644 /tmp/before.json
#   ... run the benchmark ...
#   scrape-metrics.sh snap http://broker:9644 /tmp/after.json
#   scrape-metrics.sh delta /tmp/before.json /tmp/after.json 300000 results/run.res.json
#
# <orders> is the ORDER count offered during the window (an order is a crossing
# pair, so two records). It is optional and only used for cpu_micros_per_order.
#
# The real per-record denominator is NOT passed in: per-function guest
# invocations are read from redpanda_transform_execution_latency_sec_count, so
# cpu_micros_per_invocation is measured rather than inferred. That matters
# because receipts, records and invocations differ per guest - dividing by a
# client-side count overstated per-record cost by ~2x in the first version of
# this tool, and by a different factor for a guest with a different
# output-per-input ratio.
set -euo pipefail

die() { echo "scrape-metrics.sh: $*" >&2; exit 1; }

# Families collected, from BOTH endpoints. Histograms are collected as their
# _sum/_count pair, which is what gives an average per window (delta sum / delta
# count) - the same method g3-e2e-latency-notes.md used by hand.
#
# TWO NAMESPACES, TWO ENDPOINTS - this trips people up and cost a cycle on
# 2026-09-01:
#   redpanda_*   -> /public_metrics  (curated public registry)
#   vectorized_* -> /metrics         (internal registry: seastar reactor, smp,
#                                     scheduler internals, everything else)
# A vectorized_* name asked of /public_metrics does not error, it just never
# matches, so the series is silently absent and reads as "the metric does not
# exist". cmd_snap now scrapes both endpoints per broker and warns if a family
# listed here produced no series at all (see MISSING_FAMILIES in the output).
#
# The vectorized_smp_* names below are seastar cross-shard (SMP) queue metrics,
# added 2026-09-01 to test whether the fan-out ceiling is per-record cross-shard
# submission backpressure rather than CPU. IMPORTANT: every metric in seastar
# group "smp" is registered with sm::metric_disabled
# (seastar/src/core/reactor.cc ~3947), so they are OFF BY DEFAULT and will be
# ABSENT from the scrape, not zero. They are listed so that if they are ever
# enabled no harness edit is needed; their absence is expected, not a bug.
# The vectorized_reactor_* names ARE enabled and are the fallback signal: a shard
# blocked submitting cross-shard work should show up as stalls and as a gap
# between awake time and useful task time.
#
# NOTE: this list is whitespace-split into exact-match keys, and it is a
# SINGLE-QUOTED string - no comments and no apostrophes inside it.
# Exact-match metric family names, one per line.
#
# This is a single-quoted string that gets whitespace-split into keys, so it
# can hold NOTHING but bare metric names:
#   - no '#' comments - on 2026-09-02 a three-line comment was split into 22
#     words ("#", "Buckets,", "percentiles." ...) which were then all reported
#     as missing families, burying the two that were genuinely absent
#   - no apostrophes - "Seastar's" terminates the string and breaks the script
# validate_families below enforces this; keep explanation up here instead.
#
# The *_bucket entries are what make real percentiles possible. sum/count can
# only ever yield a MEAN, and a mean was once reported as though it were a p50.
# Prometheus buckets are CUMULATIVE and carry an "le" label.
FAMILIES='
redpanda_wasm_engine_cpu_seconds_total
redpanda_wasm_engine_memory_usage
redpanda_wasm_engine_max_memory
redpanda_transform_read_bytes
redpanda_transform_write_bytes
redpanda_transform_failures
redpanda_transform_batches_given_up
redpanda_transform_state_recovery_failures
redpanda_transform_lag
redpanda_transform_e2e_latency_seconds_sum
redpanda_transform_e2e_latency_seconds_count
redpanda_transform_input_delay_seconds_sum
redpanda_transform_input_delay_seconds_count
redpanda_transform_execution_latency_sec_sum
redpanda_transform_execution_latency_sec_count
redpanda_relay_pushes_total
redpanda_relay_delivered_total
redpanda_relay_dropped_total
redpanda_relay_active_subscriptions
redpanda_relay_fanout_duration_seconds_sum
redpanda_relay_fanout_duration_seconds_count
redpanda_relay_consume_delay_seconds_sum
redpanda_relay_consume_delay_seconds_count
redpanda_relay_crossshard_dispatch_duration_seconds_sum
redpanda_relay_crossshard_dispatch_duration_seconds_count
redpanda_relay_crossshard_transit_duration_seconds_sum
redpanda_relay_crossshard_transit_duration_seconds_count
redpanda_relay_emit_to_guest_duration_seconds_sum
redpanda_relay_emit_to_guest_duration_seconds_count
redpanda_relay_emit_to_guest_duration_seconds_bucket
redpanda_relay_crossshard_transit_duration_seconds_bucket
redpanda_relay_consume_delay_seconds_bucket
redpanda_relay_fanout_duration_seconds_bucket
redpanda_transform_e2e_latency_seconds_bucket
redpanda_scheduler_runtime_seconds_total
redpanda_memory_allocated_memory
redpanda_memory_free_memory
redpanda_kafka_request_latency_seconds_sum
redpanda_kafka_request_latency_seconds_count
vectorized_smp_send_queue_length
vectorized_smp_send_batch_queue_length
vectorized_smp_receive_batch_queue_length
vectorized_smp_complete_batch_queue_length
vectorized_smp_total_sent_messages
vectorized_smp_total_received_messages
vectorized_smp_total_completed_messages
vectorized_reactor_stalls
vectorized_reactor_awake_time_ms_total
vectorized_reactor_cpu_busy_ms
'

# A Prometheus metric name is [a-zA-Z_:][a-zA-Z0-9_:]* - anything else in
# FAMILIES is a typo or stray prose, never a family that is merely absent.
validate_families() {
    local bad="" f
    for f in $FAMILIES; do
        case "$f" in
            [a-zA-Z_:]*) ;;
            *) bad="$bad $f"; continue ;;
        esac
        if [ -n "$(printf '%s' "$f" | tr -d 'a-zA-Z0-9_:')" ]; then
            bad="$bad $f"
        fi
    done
    if [ -n "$bad" ]; then
        echo "scrape-metrics: FAMILIES contains entries that are not legal" >&2
        echo "  Prometheus metric names - almost certainly a stray comment or" >&2
        echo "  apostrophe inside the single-quoted string:" >&2
        printf '    %s\n' $bad >&2
        return 1
    fi
    return 0
}
validate_families || exit 2

cmd_snap() {
  local admin_url="${1:?usage: snap <admin-url[,admin-url...]> <out.json>}"
  local out="${2:?usage: snap <admin-url[,admin-url...]> <out.json>}"

  # PASS EVERY BROKER (comma-separated). These metrics are per-node: a
  # transform's engine/transform series exist only on the broker actually
  # running that processor, and which broker that is follows partition
  # leadership. Scraping one endpoint silently omits the transform, or reports
  # an idle RELAY_TARGET_SHARD duplicate instead of the working instance -
  # observed both ways on the same cluster within one session.
  # Literal-string prefilter for the private endpoint. /metrics emits thousands
  # of series per shard and the merged body is held in a shell variable, so the
  # private side is filtered at the pipe instead of being carried whole. This is
  # a superset filter (substring match on family names); the awk below still
  # does the exact-name matching.
  local PREFILTER
  PREFILTER="$(mktemp)"
  # shellcheck disable=SC2064
  trap "rm -f '$PREFILTER'" RETURN
  printf '%s\n' "$FAMILIES" | awk 'NF' > "$PREFILTER"

  local body="" one="" priv=""
  local ok=0
  local IFS_SAVE="$IFS"; IFS=','
  for u in $admin_url; do
    IFS="$IFS_SAVE"
    u="$(echo "$u" | tr -d ' ')"
    [ -z "$u" ] && continue
    # BOTH endpoints. /public_metrics carries redpanda_*, /metrics carries
    # vectorized_* (seastar reactor, smp, internals). Neither is a superset of
    # the other. /public_metrics is required - if it fails the broker is
    # genuinely unreachable and is skipped. /metrics is best-effort: it is much
    # larger and only the vectorized_* families need it, so a failure there
    # warns and continues rather than voiding the broker.
    one="$(curl -sf --max-time 20 "${u%/}/public_metrics" 2>/dev/null)" || { echo "scrape-metrics.sh: WARNING ${u} unreachable" >&2; IFS=','; continue; }
    priv="$(curl -sf --max-time 30 "${u%/}/metrics" 2>/dev/null | grep -F -f "$PREFILTER" 2>/dev/null || true)"
    if [ -z "$priv" ]; then
      echo "scrape-metrics.sh: WARNING ${u}/metrics returned no matching vectorized_* series (endpoint down, or none of the private families are enabled)" >&2
    else
      one="${one}
${priv}"
    fi
    # Tag every line with the endpoint it came from. The merged view below is
    # still produced (it was defect 16's fix - scraping a single broker made the
    # matcher's series look absent), but a merged-only view cannot tell one hot
    # core on one node apart from moderate load on the same shard index across
    # three, which is exactly what blocked attributing the fan-out ceiling on
    # 2026-08-30. Both views are emitted.
    body="${body}
$(printf '%s\n' "$one" | sed "s|^|@@${u}@@|")"
    ok=$((ok + 1))
    IFS=','
  done
  IFS="$IFS_SAVE"
  [ "$ok" -gt 0 ] || die "no admin endpoint reachable from: $admin_url"

  # Build a filter of the families we want, then emit one JSON object of
  # {"<name>{labels}": value}. Anchoring on '{' or ' ' after the name keeps
  # e2e_latency_seconds_sum from also matching e2e_latency_seconds_count, and
  # keeps a family name from matching a longer one that starts with it.
  printf '%s\n' "$body" | awk -v families="$FAMILIES" -v url="$admin_url" '
    BEGIN {
      n = split(families, f, /[ \t\n]+/)
      for (i = 1; i <= n; i++) if (f[i] != "") want[f[i]] = 1
      printf "{\n"
      printf "  \"admin_url\": \"%s\",\n", url
      "date -u +%Y-%m-%dT%H:%M:%SZ" | getline ts
      printf "  \"scraped_at\": \"%s\",\n", ts
      printf "  \"series\": {\n"
      first = 1
    }
    {
      # Strip the @@endpoint@@ tag added above, remembering which broker this
      # line came from.
      ep = ""
      if (substr($0, 1, 2) == "@@") {
        close_at = index(substr($0, 3), "@@")
        if (close_at > 0) {
          ep = substr($0, 3, close_at - 1)
          $0 = substr($0, close_at + 4)
        }
      }
    }
    /^#/ { next }
    NF < 2 { next }
    {
      # split "<name>{labels}" or "<name>" from the trailing value
      value = $NF
      head = $0
      sub(/[ \t]+[^ \t]+$/, "", head)
      name = head
      if (index(head, "{") > 0) name = substr(head, 1, index(head, "{") - 1)
      if (!(name in want)) next
      seen[name] = 1
      gsub(/\\/, "\\\\", head); gsub(/"/, "\\\"", head)
      # Sum rather than overwrite: the same series key can arrive once per
      # broker now that every endpoint is scraped, and these are per-(node,
      # labels) values whose cluster-wide total is the sum.
      agg[head] += value
      if (ep != "") {
        gsub(/\\/, "\\\\", ep); gsub(/"/, "\\\"", ep)
        perkey = ep SUBSEP head
        per[perkey] += value
        eps[ep] = 1
      }
    }
    END {
      for (k in agg) {
        if (!first) printf ",\n"
        printf "    \"%s\": %s", k, agg[k]
        first = 0
      }
      printf "\n  },\n"
      # Per-endpoint view: {"<endpoint>": {"<series>": value}}
      printf "  \"series_by_endpoint\": {\n"
      firste = 1
      for (e in eps) {
        if (!firste) printf ",\n"
        printf "    \"%s\": {\n", e
        firsts = 1
        for (k in per) {
          split(k, parts, SUBSEP)
          if (parts[1] != e) continue
          if (!firsts) printf ",\n"
          printf "      \"%s\": %s", parts[2], per[k]
          firsts = 0
        }
        printf "\n    }"
        firste = 0
      }
      printf "\n  },\n"
      # Families that matched nothing. A metric asked of the wrong endpoint or
      # namespace, a renamed family, or a seastar metric registered
      # sm::metric_disabled all produce silence rather than an error - on
      # 2026-09-01 ten vectorized_* names were added to a scraper that only hit
      # /public_metrics and would have read as "the metric does not exist".
      # Recording it IN THE ARTIFACT means a later reader of the JSON sees the
      # gap even if nobody was watching stderr.
      printf "  \"missing_families\": [";
      firstm = 1
      for (i = 1; i <= n; i++) {
        fam = f[i]
        if (fam == "" || (fam in seen)) continue
        if (fam in reported) continue
        reported[fam] = 1
        if (!firstm) printf ", "
        printf "\"%s\"", fam
        firstm = 0
        print "scrape-metrics.sh: WARNING family matched no series: " fam > "/dev/stderr"
      }
      printf "]\n}\n"
    }
  ' > "$out"

  local count
  count="$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["series"]))' "$out")"
  echo "scrape-metrics.sh: wrote $count series to $out"
  [ "$count" -gt 0 ] || die "no matching series found - is data_transforms_enabled set, and is this the right admin port?"
}

cmd_delta() {
  local before="${1:?usage: delta <before.json> <after.json> [records] [out.json]}"
  local after="${2:?usage: delta <before.json> <after.json> [records] [out.json]}"
  local records="${3:-0}"
  local out="${4:-}"

  python3 - "$before" "$after" "$records" "$out" <<'PYEOF'
import json, sys, re

before_path, after_path, orders_s, out_path = sys.argv[1:5]
orders = int(orders_s) if orders_s else 0
before_doc = json.load(open(before_path))
before = before_doc["series"]
after_doc = json.load(open(after_path))
after = after_doc["series"]

LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')

def split(key):
    i = key.find("{")
    if i < 0:
        return key, {}
    return key[:i], dict(LABEL_RE.findall(key[i:]))

# Counters get a delta; gauges get their end value. Anything ending in _total,
# _sum, _count, or in this explicit list, is a counter.
COUNTER_SUFFIXES = ("_total", "_sum", "_count")
COUNTER_NAMES = {
    "redpanda_transform_read_bytes",
    "redpanda_transform_write_bytes",
    "redpanda_transform_failures",
    "redpanda_transform_batches_given_up",
    "redpanda_transform_state_recovery_failures",
    "redpanda_wasm_engine_cpu_seconds_total",
}

def is_counter(name):
    return name.endswith(COUNTER_SUFFIXES) or name in COUNTER_NAMES

# ---- per-function CPU and memory: the per-client cost this exists to produce
per_function = {}
for key, val in after.items():
    name, labels = split(key)
    fn = labels.get("function_name")
    if fn is None:
        continue
    row = per_function.setdefault(fn, {})
    if name == "redpanda_wasm_engine_cpu_seconds_total":
        row["cpu_seconds_delta"] = row.get("cpu_seconds_delta", 0.0) + val - before.get(key, 0.0)
    elif name == "redpanda_wasm_engine_memory_usage":
        row["memory_usage_end_bytes"] = row.get("memory_usage_end_bytes", 0.0) + val
    elif name == "redpanda_wasm_engine_max_memory":
        row["max_memory_end_bytes"] = row.get("max_memory_end_bytes", 0.0) + val
    elif name == "redpanda_transform_execution_latency_sec_count":
        # The guest's own invocation count - the correct per-record denominator.
        row["invocations"] = row.get("invocations", 0.0) + val - before.get(key, 0.0)

for fn, row in per_function.items():
    cpu = row.get("cpu_seconds_delta")
    if cpu is None:
        continue
    inv = row.get("invocations", 0.0)
    if inv > 0:
        row["cpu_micros_per_invocation"] = cpu * 1e6 / inv
    if orders > 0:
        row["cpu_micros_per_order"] = cpu * 1e6 / orders

# ---- per-shard, per-scheduling-group CPU. This is the view that found G18:
# a shard owning none of a fanout's data still paid real fetch/kafka runtime,
# because connections are spread across shards at accept time independent of
# partition leadership.
sched = {}
for key, val in after.items():
    name, labels = split(key)
    if name != "redpanda_scheduler_runtime_seconds_total":
        continue
    shard = labels.get("shard", "?")
    group = labels.get("redpanda_scheduling_group") or labels.get("group") or labels.get("scheduling_group", "?")
    d = val - before.get(key, 0.0)
    sched.setdefault(shard, {})[group] = round(d * 1e6, 1)  # microseconds

# ---- the same view, but PER BROKER rather than summed across them.
# A merged per-shard figure cannot distinguish one saturated core on one node
# from moderate load on the same shard index across three, which is precisely
# what blocked attributing the fan-out ceiling on 2026-08-30: one shard showed
# ~half the cluster's transforms CPU and which shard it was moved between runs.
# The merged view above is kept (defect 16) and this is additive.
sched_by_ep = {}
before_eps = before_doc.get("series_by_endpoint", {})
after_eps = after_doc.get("series_by_endpoint", {})
for ep, aseries in after_eps.items():
    bseries = before_eps.get(ep, {})
    per = {}
    for key, val in aseries.items():
        name, labels = split(key)
        if name != "redpanda_scheduler_runtime_seconds_total":
            continue
        shard = labels.get("shard", "?")
        group = (labels.get("redpanda_scheduling_group")
                 or labels.get("group")
                 or labels.get("scheduling_group", "?"))
        d = val - bseries.get(key, 0.0)
        per.setdefault(shard, {})[group] = round(d * 1e6, 1)
    if per:
        sched_by_ep[ep] = per

# ---- flat family totals
def total(name, counter=None):
    counter = is_counter(name) if counter is None else counter
    t = 0.0
    for key, val in after.items():
        n, _ = split(key)
        if n != name:
            continue
        t += (val - before.get(key, 0.0)) if counter else val
    return t

families = {}
for key in set(list(after.keys())):
    n, _ = split(key)
    families.setdefault(n, None)
for name in sorted(families):
    families[name] = round(total(name), 6)

# ---- histogram averages over the window: delta(sum)/delta(count)
def hist_avg(base):
    s = total(base + "_sum")
    c = total(base + "_count")
    return (s / c) if c > 0 else None

def hist_quantiles(base, qs=(0.5, 0.9, 0.99), label=None, label_value=None):
    """p50/p90/p99 from CUMULATIVE Prometheus buckets, over the window delta.

    sum/count gives only a mean. That mean was reported as a p50 on 2026-09-02,
    which is why this exists. Buckets are cumulative and log-spaced here, so the
    value is linearly interpolated inside the bucket the quantile lands in - the
    same thing Prometheus histogram_quantile does. Interpolation inside a
    log-spaced bucket is coarse, so treat the result as good to the bucket width,
    not to the digit.

    Returns microseconds, plus the bucket bound the quantile fell in so the
    reader can see how coarse it is. A quantile landing in the +Inf bucket is
    reported as None - it cannot be bounded above.
    """
    acc = {}
    for key, val in after.items():
        n, labels = split(key)
        if n != base + "_bucket":
            continue
        if label is not None and labels.get(label) != label_value:
            continue
        le = labels.get("le")
        if le is None:
            continue
        acc[le] = acc.get(le, 0.0) + (val - before.get(key, 0.0))
    if not acc:
        return None
    def as_f(x):
        return float("inf") if x in ("+Inf", "Inf", "inf") else float(x)
    bounds = sorted(acc, key=as_f)
    total = max(acc[b] for b in bounds)   # cumulative: the top bucket is the total
    if total <= 0:
        return None
    out = {}
    for q in qs:
        rank = q * total
        prev_cum, prev_bound = 0.0, 0.0
        hit = None
        for b in bounds:
            cum = acc[b]
            if cum >= rank:
                ub = as_f(b)
                if ub == float("inf"):
                    hit = None
                else:
                    width = cum - prev_cum
                    frac = ((rank - prev_cum) / width) if width > 0 else 0.0
                    # buckets are in SECONDS on /public_metrics
                    hit = round((prev_bound + (ub - prev_bound) * frac) * 1e6, 1)
                break
            prev_cum, prev_bound = cum, as_f(b)
        out[f"p{int(q * 100)}_micros"] = hit
    out["samples"] = round(total)
    return out

def hist_avg_by_label(base, label):
    """Per-label histogram averages: delta(sum)/delta(count) WITHIN each label value.

    hist_avg above sums _sum and _count across EVERY series in the family, which
    silently averages unrelated things together. At 500 relay consumers the
    aggregate transform_e2e is dominated by the 500 sinks and says almost nothing
    about the matcher - it was quoted as "matcher in->out = 27us" on 2026-09-02
    before that was noticed. Group by the label instead. Returns microseconds.
    """
    acc = {}
    for key, val in after.items():
        n, labels = split(key)
        if n not in (base + "_sum", base + "_count"):
            continue
        lv = labels.get(label)
        if lv is None:
            continue
        d = val - before.get(key, 0.0)
        row = acc.setdefault(lv, {"sum": 0.0, "count": 0.0})
        row["sum" if n.endswith("_sum") else "count"] += d
    return {lv: round(r["sum"] / r["count"] * 1e6, 1)
            for lv, r in acc.items() if r["count"] > 0}

hists = {}
for base in ("redpanda_transform_e2e_latency_seconds",
             "redpanda_transform_input_delay_seconds",
             # The two relay stage histograms. Only populated when
             # relay_stage_metrics_enabled=true; an untouched histogram has
             # count 0, which hist_avg returns None for, so a disabled run
             # simply omits them rather than reporting a fake zero.
             #
             # These were live on the broker throughout Round 2 (2026-08-29)
             # and recorded NOWHERE, because this list was never updated. The
             # dispatch-vs-scheduling attribution that motivated building them
             # was consequently unavailable for the one sweep that needed it.
             "redpanda_relay_fanout_duration_seconds",
             "redpanda_relay_consume_delay_seconds",
             # The producer shard's own fan-out cost: one payload copy plus
             # every cross-shard submission. fanout_duration covers only the
             # LOCAL subscriber loop, so before this existed the instrumentation
             # was timing the cheap half of push() while the expensive half went
             # unmeasured at the time this was written.
             "redpanda_relay_crossshard_dispatch_duration_seconds",
             # The gap between the two above: submission finished -> record
             # arrived on a destination shard. crossshard_dispatch deliberately
             # stops before awaiting anything and consume_delay starts only once
             # a record is already enqueued at the destination, so cross-shard
             # queueing time was charged to no stage at all. Added 2026-09-01
             # instead of chasing seastar's smp queue metrics, which are all
             # metric_disabled and whose only surviving depth gauge saturates at
             # 128 - see METHODOLOGY #24.
             "redpanda_relay_crossshard_transit_duration_seconds",
             # THE span to quote for in-broker fan-out: emit -> consuming guest
             # dequeue, one measurement on one clock. Added 2026-09-02 because
             # summing dispatch+transit+fanout+consume both double-counts
             # (transit starts inside dispatch's window) and misses time between
             # stages - two errors that produced a bogus ~275us e2e figure.
             "redpanda_relay_emit_to_guest_duration_seconds"):
    avg = hist_avg(base)
    if avg is not None:
        hists[base] = {"window_avg_micros": round(avg * 1e6, 1),
                       "samples": round(total(base + "_count"))}
    elif base.startswith("redpanda_relay_"):
        # Record the absence explicitly. Omitting it silently is how Round 2's
        # missing stage breakdown went unnoticed until analysis: the JSON simply
        # had no relay histogram keys, which is indistinguishable from nobody
        # having asked for them. An explicit zero-sample marker means a reader
        # (or a later agent) sees that the histogram existed and captured
        # nothing, and why.
        hists[base] = {"window_avg_micros": None, "samples": 0,
                       "note": "no samples in window - relay_stage_metrics_enabled "
                               "was almost certainly false; recording is gated on it"}

# ---- the latency decomposition, per function and per request type
#
# These exist because the aggregate figures above cannot answer "how long did the
# MATCHER take" or "how much of the produce leg is network". Both were guessed on
# 2026-09-02 and both guesses were wrong.
transform_e2e_by_function = hist_avg_by_label(
    "redpanda_transform_e2e_latency_seconds", "function_name")
transform_input_delay_by_function = hist_avg_by_label(
    "redpanda_transform_input_delay_seconds", "function_name")
# Broker-side request handling, split by request type. For a produce this is
# append (+ replication when acks=all) WITHOUT either network leg, measured on
# the broker's own clock. Subtracting it from the client's own produce latency
# leaves the round-trip network cost, so the one-way hop no longer has to be
# guessed by halving a round trip that also contained the append.
kafka_request_latency_us = hist_avg_by_label(
    "redpanda_kafka_request_latency_seconds", "redpanda_request")

pushes = families.get("redpanda_relay_pushes_total", 0.0)
delivered = families.get("redpanda_relay_delivered_total", 0.0)
dropped = families.get("redpanda_relay_dropped_total", 0.0)

doc = {
    "orders": orders,
    "scraped_after_at": after_doc.get("scraped_at"),
    "per_function": per_function,
    "scheduler_runtime_micros_by_shard": sched,
    "scheduler_runtime_micros_by_shard_per_endpoint": sched_by_ep,
    "relay": {
        # NOTE ON pushes_delta, read this before deriving anything from it.
        # redpanda_relay_pushes_total is incremented inside
        # relay::service::deliver_locally(), which push() invokes once for the
        # producer's own shard PLUS once per other shard that has a subscriber.
        # So it counts shard-local delivery passes, NOT producer pushes, and it
        # grows with how widely subscribers are spread across shards.
        "pushes_delta": pushes,
        "delivered_delta": delivered,
        "dropped_delta": dropped,
        # delivered/pushes was previously reported as "delivered_per_push" and
        # is NOT a fan-out ratio: because pushes counts per-shard passes and
        # each pass delivers to that shard's own subscribers, the quotient sits
        # near 1 at every fan-out level and hides fan-out completely. Measured
        # on 2026-08-29: fanout 10 reported 1.0, while the true ratio was 10
        # (6,000,000 delivered against 600,000 logical pushes). Kept under a
        # name that says what it is, so nobody reads it as fan-out again.
        "delivered_per_shard_pass": (delivered / pushes) if pushes else None,
        # The real fan-out ratio, when the caller told us the order count: this
        # workload emits two records per order, so logical pushes = orders x 2.
        "logical_pushes": (orders * 2) if orders else None,
        "delivered_per_logical_push": (delivered / (orders * 2)) if orders else None,
        "drop_rate": (dropped / (delivered + dropped)) if (delivered + dropped) else None,
        "active_subscriptions_end": families.get("redpanda_relay_active_subscriptions", 0.0),
    },
    "histograms": hists,
    "percentiles": {
      "relay_emit_to_guest": hist_quantiles("redpanda_relay_emit_to_guest_duration_seconds"),
      "relay_crossshard_transit": hist_quantiles("redpanda_relay_crossshard_transit_duration_seconds"),
      "relay_consume_delay": hist_quantiles("redpanda_relay_consume_delay_seconds"),
      "relay_fanout_duration": hist_quantiles("redpanda_relay_fanout_duration_seconds"),
    },
    "transform_e2e_by_function_us": transform_e2e_by_function,
    "transform_input_delay_by_function_us": transform_input_delay_by_function,
    "kafka_request_latency_us": kafka_request_latency_us,
    "family_totals": families,
}

enc = json.dumps(doc, indent=2, sort_keys=False)
if out_path:
    open(out_path, "w").write(enc + "\n")
    print(f"scrape-metrics.sh: wrote delta to {out_path}")
else:
    print(enc)

# A human-readable summary of the two things this tool exists for.
print("", file=sys.stderr)
print("--- per-function cost ---", file=sys.stderr)
if not per_function:
    print("  (none - no function_name-labelled series; is a transform deployed?)", file=sys.stderr)
for fn, row in sorted(per_function.items()):
    cpu = row.get("cpu_seconds_delta", 0.0)
    inv = row.get("invocations", 0.0)
    per_inv = row.get("cpu_micros_per_invocation")
    per_ord = row.get("cpu_micros_per_order")
    mem = row.get("memory_usage_end_bytes", 0.0)
    a = f"{per_inv:.2f}us/invocation" if per_inv is not None else "n/a (no invocations seen)"
    # "-" is correct rather than a computed value: this tool's window includes
    # warmup, so dividing by a warmup-excluded order count mixes two windows.
    # loadgen's own report carries the authoritative per-order figure.
    b = f"{per_ord:.2f}us/order" if per_ord is not None else "- (per-order: see loadgen report)"
    print(f"  {fn:32s} cpu {cpu:8.3f}s  inv {inv:10.0f}  {a:24s} {b:16s} mem {mem/1024/1024:6.2f}MiB", file=sys.stderr)

if dropped > 0:
    print("", file=sys.stderr)
    print(f"  WARNING: relay dropped {dropped:.0f} records - a consumer was backlogged. "
          f"Any latency measured over this window is not a clean number.", file=sys.stderr)
PYEOF
}

case "${1:-}" in
  snap)  shift; cmd_snap "$@" ;;
  delta) shift; cmd_delta "$@" ;;
  *) die "usage: scrape-metrics.sh {snap|delta} ... (see the header of this file)" ;;
esac
