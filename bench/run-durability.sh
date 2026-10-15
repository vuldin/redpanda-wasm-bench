#!/usr/bin/env bash
# Durability of the stateful book-matcher guest: does its book survive a
# leadership move or a broker kill, and how long does it take to come back?
#
# Runs on the WORKSTATION, not on client0: the kill action needs ssh to
# whichever broker leads the input partition, and the clients hold no broker
# keys. loadgen and statecheck still run on client0, next to the cluster.
#
# Per trial:
#   fresh topics -> deploy book-matcher (STATE=snapshot, require_state_recovery)
#   -> loadgen with a preloaded book, cancels, a long timed window
#   -> statecheck places sentinel waves at T-10/-6/-3/-1/-0.3s
#   -> at T: the action (maintenance | transfer | kill)
#   -> wait for the transform to run again and loadgen to finish
#   -> statecheck verify (traffic stopped) -> logs, metrics, placement
#
# REPLAY=1 adds the replay arm: the same history replayed by a fresh,
# snapshot-less deploy from the start of the input topic, timed to lag 0 and
# checked with the same sentinels - the alternative to a snapshot.
#
# The sentinels are the ground truth; the guest's own STATE log lines
# (restored / gap / dup_skipped) are the explanation.
set -uo pipefail

HERE="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
WB="${WB:-$ROOT/scripts/wb}"
eval "$("$WB" env 2>/dev/null | grep -E '^[A-Z_]+=')"
: "${BROKERS:?no cluster env - is a cluster provisioned?}"

ACTIONS="${ACTIONS:-maintenance transfer kill}"
DEPTHS="${DEPTHS:-1000 10000}"
TRIALS="${TRIALS:-3}"
RATE="${RATE:-30000}"
OPR="${ORDERS_PER_RECORD:-15}"
PRODUCERS="${PRODUCERS:-1}"
CANCEL_FRACTION="${CANCEL_FRACTION:-0.3}"
PAYLOAD="${PAYLOAD:-650}"
RF="${RF:-3}"
SNAPSHOT_EVERY_MS="${SNAPSHOT_EVERY_MS:-500}"
# snapshot | off. off is the no-durability control: same binary, no region
# registered, so a restarted processor comes back with an empty book.
STATE="${STATE:-snapshot}"
PER_WAVE="${PER_WAVE:-20}"
WAVES="${WAVES:-10,6,3,1,0.3}"
# Seconds the killed broker stays down before it is started again. Long
# enough that the partition's new leader has to restore from the snapshot
# rather than the old leader coming straight back.
KILL_DOWN_S="${KILL_DOWN_S:-20}"
REPLAY="${REPLAY:-0}"
OUT="${OUT:-$ROOT/results/durability-$(date +%s)}"
TOOLS="${TOOLS:-/opt/wasm-bench}"
mkdir -p "$OUT"
SUMMARY="$OUT/summary.txt"

say() { echo "$@" | tee -a "$SUMMARY"; }
C() { "$WB" ssh client0 "cd $TOOLS && $*"; }
ADMIN_HOSTS="$(echo "$ADMIN_URLS" | sed 's|http://||g')"
RPKR="./rpk -X brokers=$BROKERS -X admin.hosts=$ADMIN_HOSTS"
now_ms() { date +%s%3N; }

# node id -> broker index in BROKER_PRIV order (resolve_host's numbering)
broker_index_of_node() {
  local node="$1" ip
  ip=$(C "curl -sf $ADMIN_URL/v1/brokers" | jq -r --argjson n "$node" '.[] | select(.node_id==$n) | .internal_rpc_address')
  echo "$BROKER_PRIV" | tr ' ' '\n' | awk -v ip="$ip" '$1==ip{print NR-1; exit}'
}
leader_of() { C "curl -sf $ADMIN_URL/v1/partitions/kafka/$1/0" | jq -r '.leader_id // empty'; }
replicas_of() { C "curl -sf $ADMIN_URL/v1/partitions/kafka/$1/0" | jq -r '[.replicas[].node_id] | join(" ")'; }
placement() { C "curl -sf $ADMIN_URL/v1/partitions/kafka/$1/0" > "$2" 2>/dev/null || true; }

cluster_healthy() {
  C "curl -sf $ADMIN_URL/v1/cluster/health_overview" 2>/dev/null | jq -e '.is_healthy == true' >/dev/null 2>&1
}
wait_healthy() {
  local i
  for i in $(seq 1 "${1:-60}"); do cluster_healthy && return 0; sleep 5; done
  return 1
}
transform_running() { C "$RPKR transform list 2>/dev/null" | awk -v n="$1" '$1==n{print $4}' | grep -q '^1$'; }
transform_lag() { C "$RPKR transform list 2>/dev/null" | awk -v n="$1" '$1==n{print $NF}'; }

maintenance_clear_all() {
  local n
  for n in $(C "curl -sf $ADMIN_URL/v1/brokers" | jq -r '.[].node_id'); do
    C "$RPKR cluster maintenance disable $n >/dev/null 2>&1" || true
  done
}

# Make sure every broker process is up - a kill trial that aborted midway
# must not leave the next trial running on five brokers.
all_brokers() {
  local n i; n=$(echo "$BROKER_PUB" | wc -w)
  for ((i = 0; i < n; i++)); do echo "broker$i"; done
}

ensure_all_brokers_running() {
  local t
  for t in $(all_brokers); do
    "$WB" ssh "$t" "systemctl is-active --quiet redpanda || sudo systemctl start redpanda" >/dev/null 2>&1 || true
  done
}

cleanup_trial() {
  local name="$1" orders="$2" fills="$3"
  C "$RPKR transform delete $name --no-confirm >/dev/null 2>&1; $RPKR transform delete $name-replay --no-confirm >/dev/null 2>&1" || true
  sleep 3
  C "$RPKR topic delete $orders $fills $fills-replay >/dev/null 2>&1" || true
}

wait_running() {
  local name="$1" i
  for i in $(seq 1 "${2:-60}"); do transform_running "$name" && return 0; sleep 1; done
  return 1
}

collect_logs() {
  local name="$1" dst="$2"
  # The transform log topic is ordinary Kafka; rpk reads every partition.
  C "timeout 30 $RPKR transform logs $name 2>/dev/null" > "$dst.transform-logs.txt" 2>/dev/null || true
  local t
  : > "$dst.broker-state-lines.txt"
  for t in $(all_brokers); do
    "$WB" ssh "$t" "sudo journalctl -u redpanda --since '-6min' --no-pager 2>/dev/null | grep -E 'guest-state|restored a persisted|state_recovery|failed to checkpoint|starting transform $name|transform_processor.cc:5[01][0-9]'" \
      2>/dev/null | sed "s/^/$t /" >> "$dst.broker-state-lines.txt" || true
    "$WB" ssh "$t" "curl -sf http://localhost:9644/public_metrics | grep -E 'transform_(state_restore|startup|offset_load|engine_start)_latency_seconds_(sum|count)|transform_state_recovery_failures' | grep '$name'" \
      2>/dev/null | sed "s/^/$t /" >> "$dst.metrics.txt" || true
  done
}

run_trial() {
  local action="$1" depth="$2" trial="$3"
  local id; id="$(date +%s)"
  local tag="$action-d$depth-$STATE-t$trial"
  local orders="orders-du-$id" fills="fills-du-$id" name="bm-du-$id"
  local base="$OUT/$tag"
  say ""
  say "############ $tag ($name) ############"
  ensure_all_brokers_running
  maintenance_clear_all
  wait_healthy 24 || { say "  SKIP: cluster not healthy"; return 1; }

  C "$RPKR topic create $orders -p 1 -r $RF -c write.caching=true >/dev/null && $RPKR topic create $fills -p 1 -r $RF -c write.caching=true >/dev/null" \
    || { say "  FAIL: topic create"; return 1; }
  local req=""; [ "$STATE" = snapshot ] && req="--require-state-recovery"
  C "bash ./deploy-transform.sh $ADMIN_URL $name $orders $fills ./book-matcher.wasm $req \
      --env RISK_LIMIT=0 --env STATE=$STATE --env SEQ_LANES=$PRODUCERS --env SNAPSHOT_EVERY_MS=$SNAPSHOT_EVERY_MS" \
    >> "$base.deploy.txt" 2>&1 || { say "  FAIL: deploy (see $base.deploy.txt)"; cleanup_trial "$name" "$orders" "$fills"; return 1; }
  wait_running "$name" 90 || { say "  FAIL: transform never running"; cleanup_trial "$name" "$orders" "$fills"; return 1; }
  placement "$orders" "$base.placement-before.json"

  # Timed window long enough to span the waves, the action and the recovery.
  local dur=75
  C "nohup ./loadgen -brokers $BROKERS -input-topic $orders -receipt-source fills -fills-topic $fills \
      -rate $RATE -orders-per-record $OPR -producers $PRODUCERS -payload-bytes $PAYLOAD -acks all -linger-ms 0 \
      -book-depth $depth -cancel-fraction $CANCEL_FRACTION -warmup 5s -duration ${dur}s -drain 20s -sample-every 1 \
      -timeline-ms 500 -metadata-min-age 250ms -retry-backoff-max 100ms \
      -guest book-matcher/state=$STATE -guest-sha256 \$(sha256sum book-matcher.wasm | cut -d' ' -f1) \
      -admin-url $ADMIN_URLS -transform-name $name -label $tag -out /tmp/$tag.loadgen.json > /tmp/$tag.loadgen.log 2>&1 < /dev/null &"
  # preload (20k records at depth 10k) + 3s ingest + warmup 5s, then into the window
  local lead_in=$(( 12 + depth / 2000 ))
  sleep "$lead_in"
  local leader; leader="$(leader_of "$orders")"
  local reps; reps="$(replicas_of "$orders")"
  local T=$(( $(now_ms) + 14000 ))
  say "  leader=node$leader replicas=[$reps] T=$T"
  C "nohup ./statecheck place -brokers $BROKERS -topic $orders -per-wave $PER_WAVE -waves $WAVES -at-unix-ms $T \
      -out /tmp/$tag.placed.json > /tmp/$tag.place.log 2>&1 < /dev/null &"
  while [ "$(now_ms)" -lt "$T" ]; do sleep 0.05; done

  local act_start act_end note=""
  act_start=$(now_ms)
  case "$action" in
    # Plumbing check only (single-node rehearsal): nothing moves.
    none) : ;;
    maintenance)
      C "$RPKR cluster maintenance enable $leader --wait 2>&1 | tail -1" >> "$base.action.txt" 2>&1 ;;
    transfer)
      local target; target=$(echo "$reps" | tr ' ' '\n' | grep -vx "$leader" | head -1)
      note="target=node$target"
      C "curl -sf -X POST '$ADMIN_URL/v1/partitions/kafka/$orders/0/transfer_leadership?target=$target'" >> "$base.action.txt" 2>&1 ;;
    kill)
      local bi; bi="$(broker_index_of_node "$leader")"
      note="victim=broker$bi(node$leader)"
      # MainPID, not pkill: a pattern match can hit the ssh shell itself.
      "$WB" ssh "broker$bi" "echo killed_at_ms=\$(date +%s%3N); sudo kill -9 \$(systemctl show -p MainPID --value redpanda)" >> "$base.action.txt" 2>&1
      act_end=$(now_ms)
      # Recovery is measured DURING the outage: the point is how fast a
      # surviving replica takes over, not how fast the victim comes back.
      local k
      for k in $(seq 1 $((KILL_DOWN_S * 2))); do
        sleep 0.5
        if transform_running "$name" && [ "$(leader_of "$orders")" != "$leader" ]; then
          say "  kill: transform running on node$(leader_of "$orders") $(( $(now_ms) - act_end ))ms after the kill (poll granularity ~0.5s+ssh)"
          break
        fi
      done
      while [ $(( $(now_ms) - act_end )) -lt $((KILL_DOWN_S * 1000)) ]; do sleep 0.5; done
      "$WB" ssh "broker$bi" "echo restarts=\$(systemctl show -p NRestarts --value redpanda) active=\$(systemctl is-active redpanda); sudo systemctl start redpanda; echo started_at_ms=\$(date +%s%3N)" >> "$base.action.txt" 2>&1 ;;
  esac
  [ -n "${act_end:-}" ] || act_end=$(now_ms)
  say "  action=$action $note start_ms=$act_start took_ms=$((act_end - act_start))"

  # Recovery: transform running again on whichever node now leads.
  local rec_start; rec_start=$(now_ms)
  wait_running "$name" 120 && say "  transform running again after $(( $(now_ms) - rec_start ))ms (poll granularity ~1s+ssh)" \
    || say "  WARNING: transform not running 120s after the action"
  # loadgen finishes on its own; its drain covers the tail.
  local i
  for i in $(seq 1 60); do
    C "pgrep -x loadgen >/dev/null" || break
    sleep 5
  done
  [ "$action" = maintenance ] && maintenance_clear_all
  [ "$action" = kill ] && wait_healthy 36
  placement "$orders" "$base.placement-after.json"
  wait_running "$name" 60 || true
  sleep 8
  C "./statecheck verify -brokers $BROKERS -placed /tmp/$tag.placed.json -fills $fills -label $tag -timeout 30s \
      -out /tmp/$tag.verdict.json > /tmp/$tag.verify.log 2>&1" || true
  local f
  for f in loadgen.json loadgen.log placed.json place.log verdict.json verify.log; do
    C "cat /tmp/$tag.$f 2>/dev/null" > "$base.$f" || true
  done
  collect_logs "$name" "$base"
  local v; v=$(jq -r '"placed=\(.placed) recovered=\(.recovered) lost=\(.lost) duplicated=\(.duplicated) strays=\(.strays_swept) ok=\(.ok) waves=" + ([.waves[] | "\(.wave_seconds_before_t)s:\(.recovered)/\(.placed)"] | join(","))' "$base.verdict.json" 2>/dev/null)
  say "  VERDICT $tag ${v:-<no verdict>}"
  grep -h "STATE \(restored\|fresh\|gap\)" "$base.transform-logs.txt" 2>/dev/null | sed 's/^/  guest: /' | tee -a "$SUMMARY" >/dev/null
  grep -h "STATE stats" "$base.transform-logs.txt" 2>/dev/null | tail -1 | sed 's/^/  guest: /' | tee -a "$SUMMARY" >/dev/null
  if [ "$REPLAY" = "1" ] && [ "$action" = "${REPLAY_ACTION:-transfer}" ]; then
    replay_arm "$tag" "$name" "$orders" "$fills" "$base"
  fi
  cleanup_trial "$name" "$orders" "$fills"
}

# A fresh snapshot-less deploy over the same, now-populated input topic, from
# its first offset: how long a replay takes to rebuild the book the snapshot
# restored, and whether it reaches the same book.
replay_arm() {
  local tag="$1" name="$2" orders="$3" fills="$4" base="$5"
  local rname="$name-replay" rfills="$fills-replay"
  local hist; hist=$(C "$RPKR topic describe $orders -p 2>/dev/null" | awk 'NR==2{print $NF}')
  C "$RPKR topic create $rfills -p 1 -r $RF >/dev/null" || return 1
  # The live transform keeps running: the sentinels verify already bought are
  # gone from BOTH books, so place a fresh set first, against the live book,
  # then replay everything including them.
  local T=$(( $(now_ms) + 3000 ))
  C "./statecheck place -brokers $BROKERS -topic $orders -per-wave $PER_WAVE -waves 1 -at-unix-ms $T -out /tmp/$tag.replay.placed.json > /tmp/$tag.replay.place.log 2>&1"
  local t0; t0=$(now_ms)
  C "bash ./deploy-transform.sh $ADMIN_URL $rname $orders $rfills ./book-matcher.wasm --from-start \
      --env RISK_LIMIT=0 --env STATE=off --env SEQ_LANES=$PRODUCERS" >> "$base.replay.deploy.txt" 2>&1 || { say "  replay: deploy failed"; return 1; }
  local i lag="" running_at="" caught_at=""
  for i in $(seq 1 600); do
    if [ -z "$running_at" ] && transform_running "$rname"; then running_at=$(now_ms); fi
    if [ -n "$running_at" ]; then
      lag=$(transform_lag "$rname")
      if [ "$lag" = "0" ]; then caught_at=$(now_ms); break; fi
    fi
    sleep 0.5
  done
  say "  REPLAY $tag history_records=$hist deploy_to_running_ms=$(( ${running_at:-0} - t0 )) deploy_to_caught_up_ms=$(( ${caught_at:-0} - t0 )) last_lag=$lag"
  sleep 3
  C "./statecheck verify -brokers $BROKERS -placed /tmp/$tag.replay.placed.json -fills $rfills -label $tag-replay -timeout 30s \
      -out /tmp/$tag.replay.verdict.json > /tmp/$tag.replay.verify.log 2>&1" || true
  C "cat /tmp/$tag.replay.verdict.json" > "$base.replay.verdict.json" 2>/dev/null || true
  local v; v=$(jq -r '"placed=\(.placed) recovered=\(.recovered) lost=\(.lost) duplicated=\(.duplicated) strays=\(.strays_swept) ok=\(.ok)"' "$base.replay.verdict.json" 2>/dev/null)
  say "  REPLAY-VERDICT $tag ${v:-<no verdict>}"
}

say "# durability run $(date -u +%FT%TZ) state=$STATE actions=[$ACTIONS] depths=[$DEPTHS] trials=$TRIALS rate=$RATE opr=$OPR producers=$PRODUCERS cancel_fraction=$CANCEL_FRACTION rf=$RF snapshot_every_ms=$SNAPSHOT_EVERY_MS per_wave=$PER_WAVE waves=$WAVES kill_down_s=$KILL_DOWN_S replay=$REPLAY"
say "# guest sha256=$(C 'sha256sum book-matcher.wasm' | cut -d' ' -f1)"
say "# broker cfg: $(C "$RPKR cluster config get data_transforms_per_function_memory_limit; $RPKR cluster config get data_transforms_state_snapshot_max_size; $RPKR cluster config get data_transforms_max_instances_per_core" | tr '\n' ' ')"
# The leader balancer would move the partition on its own mid-trial and
# confound the action under test. Restored at exit.
C "$RPKR cluster config set enable_leader_balancer false >/dev/null" || true
trap 'C "$RPKR cluster config set enable_leader_balancer true >/dev/null" || true; maintenance_clear_all; ensure_all_brokers_running' EXIT

fails=0
for depth in $DEPTHS; do
  for action in $ACTIONS; do
    for trial in $(seq 1 "$TRIALS"); do
      run_trial "$action" "$depth" "$trial" || fails=$((fails + 1))
    done
  done
done
say ""
say "# done $(date -u +%FT%TZ) failed_trials=$fails"
[ "$fails" -eq 0 ]
