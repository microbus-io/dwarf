#!/usr/bin/env bash
# Usage: real-rig.sh   REAL (_REDO_BENCH.md), a realistic day on one database, on the pinned build $BIN: one 32 vCPU
# Cloud SQL Postgres 16 instance (1 TB SSD, Query Insights, $DBFLAGS), two c4a-standard-8 engine replicas that also
# generate half the load each, and a c4a-standard-2 admin host (psql, s-mon.sh, v-mon.sh).
#   0  draw check: both replicas, saturated linear 0 B, 1 + 6 min; dbcpu.sh's core-ms per step above $DRAW_MAX is a slow
#      draw, redrawn at most $REDRAWS times.
# $DBFLAGS defaults to https://docs.dwarf.run/operating/deployment/'s settings: max_wal_size 20 GB, checkpoint_timeout 15 min, wal_compression
# lz4, autovacuum_work_mem 1 GB. Every payload in the calibration and the day is a -payload-json document, which
# compresses as real JSON does.
#   1  calibration: the mix, saturated, 2 + 16 min, fresh database. C_mix is the replicas' summed rate over the last 5
#      minutes of the window. If it is under 14,000 steps/s every level below is scaled to 0.85 x C_mix / 12,000.
#   2  the day: three 90-minute periods (4.5 h) of night 8,000 / ramp / day 12,000 / ramp, bursts to 16,000 at
#      minutes 10 (2 min), 40 (2 min) and 58 (5 min), +-10% seeded noise per minute, Poisson arrivals; the mix below,
#      85% 2 ms / 12% 100 ms / 3% 5 s tasks, 200 fairness keys on a Zipf (s 1.1), flows across all five priority
#      levels (3 / 12 / 55 / 20 / 10% at 1..5), closed flows archived and kept: no -retention, whose row Purge on the
#      archive left 30M dead rows that held every autovacuum worker and starved the live tables (the archive's own
#      retention drops whole days). A rolling restart in period
#      2's day phase: replica A at run minute $RESTART_A (137), replica B at $RESTART_B (141), each by SIGTERM (the
#      bench stops creating and its engine drains; SIGKILL if it has not stopped in 3 min), restarted 10 s later on
#      the same epoch. At the end every replica keeps its engines up until no flow is left running.
#   3  the database's record: flows by status, steps not completed, peer rows left, pg_stat_statements, and
#      Cloud SQL CPU over the day.
# Each replica writes a per-interval series (out/day*-*.series.jsonl, every 10 s): commanded, offered and admitted
# flows/s; executed steps/s; admitted and ended flows/s and steps/s by class and band; outstanding; the backlog by
# band and dispatch lag; each refill phase's calls and mean; Create latency; CPU, crew, connections, heap; and p50/p99
# by class, band and top tenant against the tail, with mergeable histograms (bench/CLAUDE.md). ~5.6 h, ~$18.
# `touch $OUT/stop` ends after the current phase. The EXIT trap pulls everything and tears the rig down.
set -u
BIN=${BIN:?set BIN to a pinned binary in \$RIG_HOME}
D=$(cd "$(dirname "$0")" && pwd)
R=$(cd "$D/../../.." && pwd)
S=${RIG_HOME:-$R/tmp/rig}
PROJECT=${PROJECT:-dwarf-bench-mbus} ZONE=${ZONE:-us-east4-a} NAME=${NAME:-real}
TIER=${TIER:-db-custom-32-131072}
DBFLAGS=${DBFLAGS-cloudsql.enable_auto_explain=on,auto_explain.log_min_duration=10000,log_lock_waits=on,autovacuum_work_mem=1048576,max_wal_size=20480,checkpoint_timeout=900,wal_compression=lz4,log_checkpoints=on}
# The recommended settings (bench/CLAUDE.md, "Running the cloud rig"; docs.dwarf.run's deployment page), each added
# unless DBFLAGS sets its own value: 1 GB of dead-row memory per autovacuum worker (at Cloud SQL's 64 MB a vacuum pass
# rescans every index once per ~11M dead rows), and checkpoints sized for the write load.
for f in autovacuum_work_mem=1048576 max_wal_size=20480 checkpoint_timeout=900 wal_compression=lz4; do
  [[ ,$DBFLAGS, == *,${f%%=*}=* ]] || DBFLAGS=${DBFLAGS:+$DBFLAGS,}$f
done
RTT_MAX=${RTT_MAX:-0.35} DRAW_MAX=${DRAW_MAX:-1.35} REDRAWS=${REDRAWS:-2}
NIGHT=${NIGHT:-8000} DAY=${DAY:-12000} BURST=${BURST:-16000}
RESTART_A=${RESTART_A:-137} RESTART_B=${RESTART_B:-141} # minutes into the run: period 2's day phase, clear of its bursts
MIX=${MIX:-0.459:linear:1K,0.15:linear:0,0.20:fanout:4K:8,0.10:scatter:4K:8:1,0.05:scatter:1K:4:2,0.04:linear:64K,0.001:linear:1M}
DIST=${DIST:-0.85:2ms,0.12:100ms,0.03:5s}
PRIO=${PRIO:-1:0.03,2:0.12,3:0.55,4:0.2,5:0.1}
OUT=$R/bench/results/redo-$NAME-$(date +%Y%m%d); mkdir -p "$OUT"
ADMIN=dwarf-$NAME-m EA=dwarf-$NAME-ea EB=dwarf-$NAME-eb
VMS=() DB="" IP=""
HARD=$(( $(date +%s) + 420 * 60 ))
log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$OUT/rig.log"; }
gc() { gcloud --project="$PROJECT" "$@"; }
authok() { local i; for i in $(seq 1 60); do gcloud auth print-access-token >/dev/null 2>&1 && return 0
  log "AUTH EXPIRED - waiting for gcloud auth login ($i/60)"; sleep 60; done; log "AUTH still expired - stopping"; exit 1; }
late() { (( $(date +%s) > HARD )) && { log "HARD DEADLINE - stopping"; exit 1; }; return 0; }
fw() { local ip; ip=$(curl -fsS -m 10 https://checkip.amazonaws.com | tr -d '[:space:]') &&
  gc compute firewall-rules update dwarf-bench-ssh --source-ranges="$ip/32" >/dev/null 2>&1; }
vmssh() { local vm=$1 i; for i in 1 2 3 4 5; do
  gc compute ssh "$vm" --zone="$ZONE" --command "$2" -- -o ConnectTimeout=20 2>/dev/null && return 0; fw; sleep 15; done; return 1; }
# vmdone <vm> <file>: whether <file> exists on <vm>. Only an ssh failure is retried: vmssh retries ANY nonzero exit,
# so a poll for a file that is not there yet would spend its whole retry ladder (and a firewall update) on each probe.
vmdone() { local i out; for i in 1 2 3 4 5; do
  out=$(gc compute ssh "$1" --zone="$ZONE" --command "test -f $2 && echo yes || echo no" -- -o ConnectTimeout=20 2>/dev/null)
  case $out in yes) return 0 ;; no) return 1 ;; esac; fw; sleep 15; done; return 1; }
pullvm() { local vm=$1; mkdir -p "$OUT/$vm"
  ( vmssh "$vm" 'tar czf /tmp/out.tgz out 2>/dev/null' && gc compute scp --zone=$ZONE "$vm":/tmp/out.tgz "$OUT/$vm/out.tgz" >/dev/null 2>&1 &&
    (cd "$OUT/$vm" && tar xzf out.tgz && rm out.tgz) ) && log "  artifacts from $vm saved" || log "  ARTIFACT PULL FAILED: $vm"; }
teardown() { local vm p=(); log "teardown in ${TEARDOWN_GRACE:-30}s (kill -9 $$ keeps the rig)"; sleep "${TEARDOWN_GRACE:-30}"
  log "teardown: VMs ${VMS[*]+"${VMS[*]}"} instance ${DB:-none}"
  for vm in "${VMS[@]+"${VMS[@]}"}"; do pullvm "$vm" & p+=($!); done; [ ${#p[@]} -gt 0 ] && wait "${p[@]}"
  [ ${#VMS[@]} -gt 0 ] && gc compute instances delete "${VMS[@]}" --zone=$ZONE --quiet >/dev/null 2>&1
  [ -n "$DB" ] && gc sql instances delete "$DB" --quiet --async >/dev/null 2>&1; wait; }
trap 'teardown; log EXIT' EXIT
pass=$(cat $S/dbpass)
[ -n "$pass" ] && [ -x "$S/$BIN" ] || { log "missing $S/dbpass or $S/$BIN"; exit 1; }
authok
stopreq() { [ -f "$OUT/stop" ] && { log "stop requested"; exit 0; }; return 0; }

mkinst() { DB=$1
  gc sql instances create "$1" --database-version=POSTGRES_16 --tier="$TIER" --edition=enterprise \
    --zone=$ZONE --storage-type=SSD --storage-size=1000 --insights-config-query-insights-enabled \
    --network="projects/$PROJECT/global/networks/dwarf-bench-net" --no-assign-ip \
    ${DBFLAGS:+--database-flags="$DBFLAGS"} --root-password="$pass" > "$OUT/create-$1.log" 2>&1 & }
# A failed create can still produce a RUNNABLE instance (bench/CLAUDE.md, INTERNAL_ERROR), so poll for it.
ipof() { local i; for i in $(seq 1 45); do
    [[ "$(gc sql instances describe "$1" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] &&
      { gc sql instances describe "$1" --format='value(ipAddresses[0].ipAddress)'; return 0; }; sleep 20; done; return 1; }
deploy() { local vm=$1 i; for i in 1 2 3 4 5 6; do
  gc compute scp --zone=$ZONE "$D/g-run.sh" "$D/s-mon.sh" "$D/v-mon.sh" "$D/rtt.sh" "$D/fsync.sh" "$vm":~/ >/dev/null 2>&1 &&
    gc compute scp --zone=$ZONE "$S/$BIN" "$vm":~/dwarf-bench.bin >/dev/null 2>&1 &&
    vmssh "$vm" 'ln -sf dwarf-bench.bin dwarf-bench && chmod +x g-run.sh s-mon.sh v-mon.sh rtt.sh fsync.sh dwarf-bench.bin && mkdir -p out' && break
  fw; sleep 20; done
  vmssh "$vm" 'sudo apt-get update -qq >/dev/null 2>&1; sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1'; }
mkvm() { local type=$1 vm; shift
  gc compute instances create "$@" --zone=$ZONE --machine-type="$type" --network=dwarf-bench-net \
    --image-family=debian-12-arm64 --image-project=debian-cloud --labels=app=dwarf-bench >> "$OUT/vm.log" 2>&1 ||
    { log "VM create failed: $*"; exit 1; }
  VMS+=("$@")
  local p=(); for vm in "$@"; do ( deploy "$vm" > "$OUT/deploy-$vm.log" 2>&1 ) & p+=($!); done; wait "${p[@]}"
  for vm in "$@"; do vmssh "$vm" 'test -x g-run.sh && test -x dwarf-bench.bin && command -v psql >/dev/null' ||
    { log "DEPLOY FAILED: $vm"; exit 1; }; done; log "  deployed $*"; }
adm() { vmssh "$ADMIN" "psql '$1' -X -q -At -F' ' -c \"$2\""; }
dsn() { echo "postgres://postgres:${pass}@$IP:5432/$1?sslmode=disable"; }
fresh() { adm "$(dsn postgres)" "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null; adm "$(dsn postgres)" "CREATE DATABASE $1" >/dev/null; }
drop() { adm "$(dsn postgres)" "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null; }
rttgate() { local vm try v; for vm in "$EA" "$EB"; do
    for try in 1 2 3; do v=$(vmssh "$vm" "./rtt.sh '$(dsn postgres)'" | tail -1)
      echo "$(date '+%H:%M:%S') $vm $v" >> "$OUT/rtt.txt"
      awk -v x="$v" -v m="$RTT_MAX" 'BEGIN { exit !(x != "?" && x + 0 <= m + 0) }' && {
        log "  rtt $vm: $v ms; $(vmssh "$vm" "./fsync.sh '$(dsn postgres)'" | tail -1 | tee -a "$OUT/fsync.txt")"; break; }
      log "  rtt $vm: $v ms - over $RTT_MAX, try $try"; (( try == 3 )) && { log "RTT GATE FAILED: $vm"; exit 1; }; sleep 30
    done; done; }
# both <tag> <start epoch> <bench args...>: the same run on both replicas, each with half of the cap and creators
# folded into the args by the caller, waiting for both to finish; a bench that did not exit 0 stops the rig. The wait
# is for the tag's .done marker, so the marker and artifact a tag already has are removed first: left in place, a
# reused tag returns at once, and the caller reads the last run's artifact and drops the database the new bench is
# about to open.
both() { local tag=$1 start=$2 vm x; shift 2
  for vm in "$EA" "$EB"; do
    vmssh "$vm" "rm -f out/$tag-$vm.done out/$tag-$vm.json; ${RUN_ENV:-} setsid nohup ./g-run.sh $tag-$vm $start '$(dsn "$RUN_DB")' $* > /dev/null 2>&1 < /dev/null &"; done
  for vm in "$EA" "$EB"; do until vmdone "$vm" "out/$tag-$vm.done"; do late; sleep 30; done; done
  for vm in "$EA" "$EB"; do x=$(vmssh "$vm" "cat out/$tag-$vm.done" | tail -1)
    [[ $x == "exit 0" ]] || { log "BENCH FAILED: $tag-$vm ${x:-?}: $(vmssh "$vm" "tail -1 out/$tag-$vm.log" | tail -1)"; exit 1; }; done; }
steps() { vmssh "$1" "python3 -c \"
import json
r = json.load(open('out/$2.json'))['results'][0]
ser = [p['stepsPerSec'] for p in (r.get('gauges') or {}).get('series') or []]
last = ser[-30:] or [r['stepsPerSec']]
print(round(r['stepsPerSec']), round(sum(last) / len(last)))
\"" | tail -1; }

# ---- the rig
log "REAL on $BIN: $TIER (DBFLAGS ${DBFLAGS:-none}) + $ADMIN (c4a-2) + $EA, $EB (c4a-8); artifacts $OUT"
mkinst "dwarf-$NAME-db"
mkvm c4a-standard-2 "$ADMIN"
mkvm c4a-standard-8 "$EA" "$EB"
IP=$(ipof "$DB") || { log "$DB not RUNNABLE"; exit 1; }
log "instance ready: $IP, max_connections $(adm "$(dsn postgres)" 'SHOW max_connections')"
rttgate

# ---- 0: draw check
for round in $(seq 0 "$REDRAWS"); do
  authok; late; RUN_DB=dwarf_real_draw; fresh $RUN_DB
  start=$(( $(date +%s) + 30 )) tag=draw-r$round # a tag per round, so each round's log and artifact are kept
  log "draw check on $DB"
  RUN_ENV="WARMUP=1m WINDOW=6m" both $tag "$start" -workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 128 -max-outstanding 1000
  a=$(steps "$EA" "$tag-$EA"); b=$(steps "$EB" "$tag-$EB"); total=$(( ${a%% *} + ${b%% *} ))
  for vm in "$EA" "$EB"; do gc compute scp --zone=$ZONE "$vm":~/out/$tag-$vm.json "$OUT/" >/dev/null 2>&1; done
  drop $RUN_DB
  sleep 150 # Cloud Monitoring lands CPU a few minutes late
  x=""; for t in 1 2 3 4 5; do
    x=$(DB_INSTANCE="$DB" VCPUS=32 PAD_SECONDS=0 STEPS=$total "$R/bench/gcp/dbcpu.sh" "$OUT/$tag-$EA.json" 2>&1 | tail -1)
    echo "$x" | grep -q core-ms && break; sleep 60; done
  cms=$(echo "$x" | sed -n 's/.* \([0-9.]*\) core-ms\/step.*/\1/p'); echo "$DB $total $x" >> "$OUT/draw.txt"
  log "  $DB: $total steps/s, ${cms:-?} core-ms/step"
  awk -v x="${cms:-}" -v m="$DRAW_MAX" 'BEGIN { exit !(x != "" && x + 0 <= m + 0) }' && break
  (( round == REDRAWS )) && { log "STILL SLOW after $REDRAWS redraws - stopping"; exit 1; }
  log "slow draw - redrawing"; gc sql instances delete "$DB" --quiet --async >/dev/null 2>&1
  mkinst "dwarf-$NAME-db-r$((round + 1))"; IP=$(ipof "$DB") || { log "$DB not RUNNABLE"; exit 1; }; rttgate
done
stopreq

# ---- 1: calibration
authok; late; RUN_DB=dwarf_real_cal; fresh $RUN_DB
start=$(( $(date +%s) + 30 ))
log "calibration: the mix saturated, 2 + 16 min"
both cal "$start" -mix "$MIX" -payload-json -task-delay-dist "$DIST" -fairness-keys 200 -open-loop -arrival-rate 0 -concurrency 128 -max-outstanding 3000
a=$(steps "$EA" "cal-$EA"); b=$(steps "$EB" "cal-$EB")
CMIX=$(( ${a##* } + ${b##* } ))
log "  C_mix $CMIX steps/s over the last 5 min (window means $(( ${a%% *} + ${b%% *} )))"
adm "$(dsn $RUN_DB)" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-cal.txt" 2>&1
drop $RUN_DB
if (( CMIX > 1000 && CMIX < 14000 )); then
  scale=$(awk -v c=$CMIX 'BEGIN { printf "%.4f", 0.85 * c / 12000 }')
  NIGHT=$(awk -v l=$NIGHT -v s=$scale 'BEGIN { printf "%.0f", l * s }')
  DAY=$(awk -v l=$DAY -v s=$scale 'BEGIN { printf "%.0f", l * s }')
  BURST=$(awk -v l=$BURST -v s=$scale 'BEGIN { printf "%.0f", l * s }')
  log "  C_mix under 14,000: levels scaled x$scale to night $NIGHT, day $DAY, burst $BURST"
fi
(( CMIX > 1000 )) || { log "NO CALIBRATION - stopping before the day"; exit 1; }
stopreq

# ---- 2: the day
PROFILE="25m:$NIGHT,10m:$NIGHT-$DAY,40m:$DAY,15m:$DAY-$NIGHT"
BURSTS="10m+2m:$BURST,40m+2m:$BURST,58m+5m:$BURST"
authok; late; RUN_DB=dwarf_real; fresh $RUN_DB
EPOCH=$(( $(date +%s) + 120 ))
DAYARGS="-real 270m -arrival-epoch $EPOCH -arrival-share 0.5 -arrival-profile '$PROFILE' -arrival-bursts '$BURSTS' -arrival-noise 0.1 -arrival-seed 1 -mix '$MIX' -payload-json -task-delay-dist '$DIST' -fairness-keys 200 -fairness-skew 1.1 -priority-mix '$PRIO' -concurrency 128 -max-outstanding 150000"
echo "$DAYARGS" > "$OUT/day-args.txt"
log "the day: epoch $(date -r $EPOCH '+%H:%M:%S'), profile $PROFILE, bursts $BURSTS"
vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 300 * 60 )) out/mon-day.log '$(dsn $RUN_DB)' > /dev/null 2>&1 < /dev/null &"
vmssh "$ADMIN" "setsid nohup ./v-mon.sh $(( 300 * 60 )) out/vmon-day.log '$(dsn $RUN_DB)' > /dev/null 2>&1 < /dev/null &"
for vm in "$EA" "$EB"; do
  vmssh "$vm" "setsid nohup ./g-run.sh day1-$vm $EPOCH '$(dsn $RUN_DB)' $DAYARGS > /dev/null 2>&1 < /dev/null &"; done
# restart <vm> <minute of the run>: SIGTERM, wait for its graceful stop, start a fresh process on the same epoch.
restart() { local vm=$1 at=$(( EPOCH + $2 * 60 )) stop
  while (( $(date +%s) < at )); do late; sleep 20; done
  log "restart $vm: SIGTERM at run minute $2"; echo "$(date -u +%FT%TZ) $vm SIGTERM" >> "$OUT/restarts.txt"
  vmssh "$vm" 'pkill -TERM -x dwarf-bench'
  stop=$(( $(date +%s) + 180 ))
  until vmdone "$vm" "out/day1-$vm.done"; do (( $(date +%s) < stop )) || break; sleep 10; done
  vmdone "$vm" "out/day1-$vm.done" || { log "  $vm did not stop in 3 min - SIGKILL"; vmssh "$vm" 'pkill -9 -x dwarf-bench; true'; }
  sleep 10
  vmssh "$vm" "setsid nohup ./g-run.sh day2-$vm 0 '$(dsn $RUN_DB)' $DAYARGS > /dev/null 2>&1 < /dev/null &"
  echo "$(date -u +%FT%TZ) $vm restarted" >> "$OUT/restarts.txt"; log "  $vm restarted"; }
restart "$EA" "$RESTART_A"
restart "$EB" "$RESTART_B"
for vm in "$EA" "$EB"; do until vmdone "$vm" "out/day2-$vm.done"; do late; sleep 60; done; done
log "the day done: $(for vm in "$EA" "$EB"; do vmssh "$vm" "grep -hE '^drain:|INVALID|valid:' out/day2-$vm.log | tr '\n' ' '"; done)"

# ---- 3: the database's record
log "flows by status: $(adm "$(dsn $RUN_DB)" "SELECT string_agg(status||'='||n, ' ') FROM (SELECT status, count(*) n FROM dwarf_flows GROUP BY status) x")"
log "steps not terminal: $(adm "$(dsn $RUN_DB)" "SELECT count(*) FROM dwarf_steps WHERE status NOT IN ('completed','failed','terminated','cancelled')")"
log "peer rows left: $(adm "$(dsn $RUN_DB)" "SELECT count(*) FROM dwarf_peers")"
adm "$(dsn $RUN_DB)" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-day.txt" 2>&1
DB_INSTANCE="$DB" VCPUS=32 PAD_SECONDS=0 "$R/bench/gcp/dbcpu.sh" -w "$(date -u -r $EPOCH +%FT%TZ)" "$(date -u +%FT%TZ)" > "$OUT/dbcpu-day.txt" 2>&1
drop $RUN_DB
log "REAL done"
