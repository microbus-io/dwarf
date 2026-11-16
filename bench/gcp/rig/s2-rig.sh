#!/usr/bin/env bash
# Usage: s2-rig.sh   S2, shards on the pinned build $BIN. Four 32 vCPU Cloud SQL Postgres 16 instances (1 TB SSD,
# Query Insights, $DBFLAGS) and a c4a-standard-2 admin host (psql, s-mon.sh, database create/drop), then:
#   0  draw check: a solo saturated 1 + 6 min run on each instance at once, four processes on the c4a-48, and
#      dbcpu.sh's core-ms per step for each. Above $DRAW_MAX the instance is a slow draw: it is deleted and redrawn,
#      at most $REDRAWS rounds, else the rig stops. The instances are then ranked fastest first, and shard i is the
#      i-th fastest in every arm.
#   1  s4-big:  4 shards x 1 replica on one c4a-standard-48 (1,024 creators, cap 8,000). The c4a-48 is then deleted.
#   2  s4-r8:   4 shards x 8 c4a-standard-8 replicas (128 creators, cap 1,000 each).
#   3  s2-r4:   the two slowest instances and four replicas deleted; 2 shards x 4 replicas.
#   4  s1-r2:   the next instance and two more replicas deleted; 1 shard x 2 replicas.
# Every arm: saturated linear 0 B, 2 min warmup + $WIN window (default 16m), the same cap and creators per shard (2,000 and 256),
# an RTT gate from every engine host to every shard it uses, a fresh database on each shard, a dispatcher snapshot at
# mid-window, flow/step status counts per shard (a multi-process drain verdict is advisory), and the database
# dropped on every surviving instance before the next arm. Resources go as soon as their last arm is done; the EXIT
# trap pulls what is left and tears everything down. `touch $OUT/stop` ends after the current arm.
set -u
BIN=${BIN:?set BIN to a pinned binary in \$RIG_HOME}
D=$(cd "$(dirname "$0")" && pwd)                      # this kit: the VM-side helpers
R=$(cd "$D/../../.." && pwd)                          # the repository: dbcpu.sh, and artifacts in $R/bench/results
S=${RIG_HOME:-$R/tmp/rig}                             # never committed: dbpass and the pinned bench binaries
PROJECT=${PROJECT:-dwarf-bench-mbus} ZONE=${ZONE:-us-east4-a} NAME=${NAME:-s2}
TIER=${TIER:-db-custom-32-131072} WIN=${WIN:-16m}
DBFLAGS=${DBFLAGS-cloudsql.enable_auto_explain=on,auto_explain.log_min_duration=10000,log_lock_waits=on}
# The recommended settings (bench/CLAUDE.md, "Running the cloud rig"; docs.dwarf.run's deployment page), each added
# unless DBFLAGS sets its own value: 1 GB of dead-row memory per autovacuum worker (at Cloud SQL's 64 MB a vacuum pass
# rescans every index once per ~11M dead rows), and checkpoints sized for the write load.
for f in autovacuum_work_mem=1048576 max_wal_size=20480 checkpoint_timeout=900; do
  [[ ,$DBFLAGS, == *,${f%%=*}=* ]] || DBFLAGS=${DBFLAGS:+$DBFLAGS,}$f
done
RTT_MAX=${RTT_MAX:-0.35}                              # ms, the idle RTT gate per engine host and shard
DRAW_MAX=${DRAW_MAX:-1.35}                            # DB core-ms per step: ~1.1-1.2 normal, ~1.5 slow
REDRAWS=${REDRAWS:-2}
OUT=$R/bench/results/redo-$NAME-$(date +%Y%m%d); mkdir -p "$OUT/draw"
ADMIN=dwarf-$NAME-m BIG=dwarf-$NAME-big
SMALL=(); for i in 1 2 3 4 5 6 7 8; do SMALL+=("dwarf-$NAME-e$i"); done
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0"
VMS=() INSTS=()                                       # live resources, for teardown
DBN=() IPS=() CMS=()                                  # per slot 0-3: instance name, IP, draw core-ms per step
HARD=$(( $(date +%s) + 270 * 60 ))
log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$OUT/rig.log"; }
gc() { gcloud --project="$PROJECT" "$@"; }
authok() { local i; for i in $(seq 1 60); do gcloud auth print-access-token >/dev/null 2>&1 && return 0
  log "AUTH EXPIRED - waiting for gcloud auth login ($i/60)"; sleep 60; done; log "AUTH still expired - stopping"; exit 1; }
late() { (( $(date +%s) > HARD )) && { log "HARD DEADLINE - stopping"; exit 1; }; return 0; }
fw() { local ip; ip=$(curl -fsS -m 10 https://checkip.amazonaws.com | tr -d '[:space:]') &&
  gc compute firewall-rules update dwarf-bench-ssh --source-ranges="$ip/32" >/dev/null 2>&1; }
vmssh() { local vm=$1 i; for i in 1 2 3 4 5; do
  gc compute ssh "$vm" --zone="$ZONE" --command "$2" -- -o ConnectTimeout=20 2>/dev/null && return 0; fw; sleep 15; done; return 1; }
without() { local x=$1 y out=(); shift; for y in "$@"; do [[ "$y" == "$x" ]] || out+=("$y"); done; echo "${out[@]+"${out[@]}"}"; }
pullvm() { local vm=$1; mkdir -p "$OUT/$vm"
  ( vmssh "$vm" 'tar czf /tmp/out.tgz out 2>/dev/null' && gc compute scp --zone=$ZONE "$vm":/tmp/out.tgz "$OUT/$vm/out.tgz" >/dev/null 2>&1 &&
    (cd "$OUT/$vm" && tar xzf out.tgz && rm out.tgz) ) && log "  artifacts from $vm saved" || log "  ARTIFACT PULL FAILED: $vm"; }
# Pulls each VM's artifacts first, then deletes the VMs together.
delvms() { local vm p=(); for vm in "$@"; do pullvm "$vm" & p+=($!); done; wait "${p[@]}"
  gc compute instances delete "$@" --zone=$ZONE --quiet >/dev/null 2>&1 &
  for vm in "$@"; do VMS=($(without "$vm" "${VMS[@]+"${VMS[@]}"}")); done; log "deleting VMs $*"; }
delinst() { local n; for n in "$@"; do gc sql instances delete "$n" --quiet --async >/dev/null 2>&1
  INSTS=($(without "$n" "${INSTS[@]+"${INSTS[@]}"}")); done; log "deleting instances $*"; }
# The final wait also lets the delayed dbcpu.sh readings land; nothing billable is left by then.
teardown() { local vm n p=(); log "teardown in ${TEARDOWN_GRACE:-30}s (kill -9 $$ keeps the rig)"; sleep "${TEARDOWN_GRACE:-30}"
  log "teardown: VMs ${VMS[*]+"${VMS[*]}"} instances ${INSTS[*]+"${INSTS[*]}"}"
  for vm in "${VMS[@]+"${VMS[@]}"}"; do pullvm "$vm" & p+=($!); done; [ ${#p[@]} -gt 0 ] && wait "${p[@]}"
  [ ${#VMS[@]} -gt 0 ] && gc compute instances delete "${VMS[@]}" --zone=$ZONE --quiet >/dev/null 2>&1
  for n in "${INSTS[@]+"${INSTS[@]}"}"; do gc sql instances delete "$n" --quiet --async >/dev/null 2>&1; done; wait; }
trap 'teardown; log EXIT' EXIT
pass=$(cat $S/dbpass)
[ -n "$pass" ] && [ -x "$S/$BIN" ] || { log "missing $S/dbpass or $S/$BIN"; exit 1; }
authok

mkinst() { # mkinst <name>: create in the background, logged; poll with ipof
  INSTS+=("$1")
  gc sql instances create "$1" --database-version=POSTGRES_16 --tier="$TIER" --edition=enterprise \
    --zone=$ZONE --storage-type=SSD --storage-size=1000 --insights-config-query-insights-enabled \
    --network="projects/$PROJECT/global/networks/dwarf-bench-net" --no-assign-ip \
    ${DBFLAGS:+--database-flags="$DBFLAGS"} --root-password="$pass" > "$OUT/create-$1.log" 2>&1 & }
# A failed create can still produce a RUNNABLE instance (bench/CLAUDE.md, INTERNAL_ERROR), so poll for it.
ipof() { local i; for i in $(seq 1 45); do
    [[ "$(gc sql instances describe "$1" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] &&
      { gc sql instances describe "$1" --format='value(ipAddresses[0].ipAddress)'; return 0; }; sleep 20; done; return 1; }
mkvms() { # mkvms <type> <name>...: create, then deploy the kit, the binary and psql to each in parallel
  local type=$1 vm; shift
  gc compute instances create "$@" --zone=$ZONE --machine-type="$type" --network=dwarf-bench-net \
    --image-family=debian-12-arm64 --image-project=debian-cloud --labels=app=dwarf-bench >> "$OUT/vm.log" 2>&1 ||
    { log "VM create failed: $*"; exit 1; }
  VMS+=("$@")
  local p=(); for vm in "$@"; do ( deploy "$vm" > "$OUT/deploy-$vm.log" 2>&1 ) & p+=($!); done; wait "${p[@]}"
  for vm in "$@"; do vmssh "$vm" 'test -x g-run.sh && test -x dwarf-bench.bin && command -v psql >/dev/null' ||
    { log "DEPLOY FAILED: $vm"; exit 1; }; done; log "  deployed $*"; }
deploy() { local vm=$1 i; for i in 1 2 3 4 5 6; do
  gc compute scp --zone=$ZONE "$D/g-run.sh" "$D/s-mon.sh" "$D/rtt.sh" "$D/fsync.sh" "$vm":~/ >/dev/null 2>&1 &&
    gc compute scp --zone=$ZONE "$S/$BIN" "$vm":~/dwarf-bench.bin >/dev/null 2>&1 &&
    vmssh "$vm" 'ln -sf dwarf-bench.bin dwarf-bench && chmod +x g-run.sh s-mon.sh rtt.sh fsync.sh dwarf-bench.bin && mkdir -p out' && break
  fw; sleep 20; done
  vmssh "$vm" 'sudo apt-get update -qq >/dev/null 2>&1; sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1'; }

adm() { vmssh "$ADMIN" "psql '$1' -X -q -At -F' ' -c \"$2\""; }
dsn() { echo "postgres://postgres:${pass}@$1:5432/$2?sslmode=disable"; }
fresh() { adm "$(dsn "$1" postgres)" "DROP DATABASE IF EXISTS $2 WITH (FORCE)" >/dev/null; adm "$(dsn "$1" postgres)" "CREATE DATABASE $2" >/dev/null; }
drop() { adm "$(dsn "$1" postgres)" "DROP DATABASE IF EXISTS $2 WITH (FORCE)" >/dev/null; }
stopreq() { [ -f "$OUT/stop" ] && { log "stop requested"; exit 0; }; return 0; }

rttgate() { # rttgate <vm> <slot>...: every shard under $RTT_MAX from this host while idle, 3 tries 30 s apart
  local vm=$1 s try v ok dsns=""; shift
  for s in "$@"; do dsns+=" '$(dsn "${IPS[$s]}" postgres)'"; done
  for try in 1 2 3; do
    v=$(vmssh "$vm" "./rtt.sh $dsns" | tr '\n' ' '); ok=1
    for x in $v; do awk -v x="$x" -v m="$RTT_MAX" 'BEGIN { exit !(x != "?" && x + 0 <= m + 0) }' || ok=0; done
    [ -n "$v" ] || ok=0
    echo "$(date '+%H:%M:%S') $vm slots $* ms $v" >> "$OUT/rtt.txt"
    (( ok )) && { log "  rtt $vm: $v ms; flush floor $(vmssh "$vm" "./fsync.sh $dsns" | tee -a "$OUT/fsync.txt" | sed 's/.*flush //' | tr '\n' ' ')ms"; return 0; }
    log "  rtt $vm: $v ms - over $RTT_MAX, try $try"; sleep 30
  done; return 1; }

draw() { # draw <slot>...: solo saturated 1 + 6 min on each slot at once from $BIG, then core-ms per step for each
  local s db="dwarf_${NAME}_draw" start t x n tag
  authok; late
  for s in "$@"; do fresh "${IPS[$s]}" "$db"; done
  start=$(( $(date +%s) + 30 ))
  log "draw check on slots $*: ${DBN[*]}"
  for s in "$@"; do tag="draw-${DBN[$s]}"
    vmssh "$BIG" "WARMUP=1m WINDOW=6m setsid nohup ./g-run.sh $tag $start '$(dsn "${IPS[$s]}" "$db")' $SAT -concurrency 256 -max-outstanding 2000 > /dev/null 2>&1 < /dev/null &"
  done
  for s in "$@"; do tag="draw-${DBN[$s]}"
    until vmssh "$BIG" "test -f out/$tag.done"; do late; sleep 20; done
    gc compute scp --zone=$ZONE "$BIG":~/out/$tag.json "$OUT/draw/" >/dev/null 2>&1
  done
  for s in "$@"; do drop "${IPS[$s]}" "$db"; done
  # Cloud Monitoring samples CPU once a minute and lands it a few minutes late.
  sleep 150
  for s in "$@"; do tag="draw-${DBN[$s]}" x=""
    for t in 1 2 3 4 5; do
      x=$(DB_INSTANCE="${DBN[$s]}" VCPUS=32 PAD_SECONDS=0 "$R/bench/gcp/dbcpu.sh" "$OUT/draw/$tag.json" 2>&1 | tail -1)
      n=$(echo "$x" | sed -n 's/.* n= *\([0-9]*\) .*/\1/p')
      [[ "${n:-0}" -ge 4 ]] && echo "$x" | grep -q core-ms && break; sleep 60
    done
    echo "$x" >> "$OUT/draw/dbcpu.txt"
    CMS[$s]=$(echo "$x" | sed -n 's/.* \([0-9.]*\) core-ms\/step.*/\1/p')
    log "  slot $s ${DBN[$s]}: $(python3 -c "import json; print(round(json.load(open('$OUT/draw/$tag.json'))['results'][0]['stepsPerSec']))" 2>/dev/null) steps/s, ${CMS[$s]:-?} core-ms/step"
  done; }

run_arm() { # run_arm <tag> <creators> <cap> <slots, comma-separated> <vm>...: every VM gets every listed shard
  local tag=$1 conc=$2 cap=$3 slots vm i s d1="" more="" dsns=() start x per=() tot=0 deadline; IFS=, read -ra slots <<< "$4"; shift 4
  local vms=("$@") db="dwarf_${NAME}_${tag//-/_}" a="$tag-$1"
  authok; late; stopreq
  for vm in "${vms[@]}"; do rttgate "$vm" "${slots[@]}" || { log "RTT GATE FAILED: $vm"; exit 1; }; done
  for i in "${!slots[@]}"; do s=${slots[$i]}; fresh "${IPS[$s]}" "$db"; dsns+=("'$(dsn "${IPS[$s]}" "$db")'")
    if (( i == 0 )); then d1=${dsns[0]}; else more+=" -dsn '$((i + 1))=$(dsn "${IPS[$s]}" "$db")'"; fi; done
  start=$(( $(date +%s) + 60 ))
  log "arm $tag: ${#slots[@]} shard(s) (${slots[*]}) x ${#vms[@]} replica(s), $conc creators and cap $cap each, start $(date -r $start '+%H:%M:%S')"
  vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 60 + (${WIN%m} + 3) * 60 )) out/mon-$tag.log ${dsns[*]} > /dev/null 2>&1 < /dev/null &"
  local p=(); for vm in "${vms[@]}"; do
    vmssh "$vm" "WINDOW=$WIN setsid nohup ./g-run.sh $tag-$vm $start $d1 $SAT -concurrency $conc -max-outstanding $cap$more > /dev/null 2>&1 < /dev/null &" & p+=($!)
  done; wait "${p[@]}"
  sleep $(( start + (2 + ${WIN%m} / 2) * 60 - $(date +%s) )) # the dispatcher snapshot, mid-window
  for i in "${!slots[@]}"; do s=${slots[$i]}
    adm "$(dsn "${IPS[$s]}" "$db")" "SELECT engine_id, to_char(joined_at,'HH24:MI:SS.MS'), (EXTRACT(EPOCH FROM now()-dispatched_at)*1000)::bigint FROM dwarf_peers ORDER BY joined_at, engine_id" |
      sed "s/^/shard $((i + 1)) ${DBN[$s]}: /" >> "$OUT/dispatchers-$tag.txt"
  done
  # Past the window and the drain (-drain, 30 s by default) with slack, a VM still running is hung: kill it.
  deadline=$(( start + (2 + ${WIN%m} + 1 + 4) * 60 ))
  for vm in "${vms[@]}"; do
    until vmssh "$vm" "test -f out/$tag-$vm.done"; do
      (( $(date +%s) > deadline )) && { log "  $vm HUNG past the drain - killing dwarf-bench"; vmssh "$vm" 'pkill -9 -x dwarf-bench; true'; break; }
      late; sleep 20; done; done
  for vm in "${vms[@]}"; do
    x=$(vmssh "$vm" "python3 -c \"
import json
d = json.load(open('out/$tag-$vm.json')); r = d['results'][0]
peer = {k: v for k, v in (r.get('engineCounters') or {}).items() if 'peer' in k and '|' not in k}
print(round(r['stepsPerSec']), 'valid' if d.get('valid') else 'INVALID:' + str(d.get('invalidity')), 'cores', round(r['host']['cpuCores'], 1), 'p99', round(r['p99Ms']), peer)
\"" | tail -1)
    log "  $vm: ${x:-?}"; x=${x%% *}; per+=("${x:-?}"); [[ "$x" =~ ^[0-9]+$ ]] && tot=$(( tot + x ))
  done
  log "arm $tag done: $tot steps/s over ${#slots[@]} shard(s), per replica ${per[*]}"
  for i in "${!slots[@]}"; do s=${slots[$i]}
    log "  shard $((i + 1)) ${DBN[$s]}: flows $(adm "$(dsn "${IPS[$s]}" "$db")" "SELECT string_agg(status||'='||n, ' ') FROM (SELECT status, count(*) n FROM dwarf_flows GROUP BY status) x"), steps not completed $(adm "$(dsn "${IPS[$s]}" "$db")" "SELECT count(*) FROM dwarf_steps WHERE status<>'completed'")"
    adm "$(dsn "${IPS[$s]}" "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag-shard$((i + 1)).txt" 2>&1
    drop "${IPS[$s]}" "$db"
  done
  # Database CPU per shard over this arm's window, once Cloud Monitoring has it; the arm's mean steps/s per shard.
  gc compute scp --zone=$ZONE "${vms[0]}":~/out/$a.json "$OUT/" >/dev/null 2>&1
  for s in "${slots[@]}"; do
    ( sleep 240; DB_INSTANCE="${DBN[$s]}" VCPUS=32 PAD_SECONDS=0 STEPS=$(( tot / ${#slots[@]} )) "$R/bench/gcp/dbcpu.sh" "$OUT/$a.json" 2>&1 |
        tail -1 | sed "s/^/$tag ${DBN[$s]} /" >> "$OUT/dbcpu-arms.txt" ) &
  done
  sleep 30; }

# ---- 0: instances, admin, the c4a-48; RTT and draw check
log "S2 on $BIN: 4 x $TIER (DBFLAGS ${DBFLAGS:-none}) + $ADMIN (c4a-2) + $BIG (c4a-48); artifacts $OUT"
for i in 0 1 2 3; do DBN[$i]="dwarf-$NAME-db$((i + 1))"; mkinst "${DBN[$i]}"; done
mkvms c4a-standard-2 "$ADMIN"
mkvms c4a-standard-48 "$BIG"
for i in 0 1 2 3; do IPS[$i]=$(ipof "${DBN[$i]}") || { log "${DBN[$i]} not RUNNABLE"; exit 1; }; done
log "instances ready: ${IPS[*]}; max_connections $(for i in 0 1 2 3; do adm "$(dsn "${IPS[$i]}" postgres)" 'SHOW max_connections'; done | tr '\n' ' ')"
rttgate "$BIG" 0 1 2 3 || { log "RTT GATE FAILED: $BIG"; exit 1; }
draw 0 1 2 3
for round in $(seq 1 "$REDRAWS") x; do
  slow=(); for i in 0 1 2 3; do awk -v x="${CMS[$i]:-}" -v m="$DRAW_MAX" 'BEGIN { exit !(x != "" && x + 0 <= m + 0) }' || slow+=("$i"); done
  [ ${#slow[@]} -eq 0 ] && break
  [[ "$round" == x ]] && { log "STILL SLOW after $REDRAWS redraws: slots ${slow[*]} - stopping"; exit 1; }
  log "redraw $round: slots ${slow[*]} (core-ms/step ${CMS[*]})"
  for i in "${slow[@]}"; do delinst "${DBN[$i]}"; DBN[$i]="dwarf-$NAME-db$((i + 1))r$round"; mkinst "${DBN[$i]}"; done
  for i in "${slow[@]}"; do IPS[$i]=$(ipof "${DBN[$i]}") || { log "${DBN[$i]} not RUNNABLE"; exit 1; }; done
  rttgate "$BIG" "${slow[@]}" || { log "RTT GATE FAILED: $BIG"; exit 1; }
  draw "${slow[@]}"
done
RANK=($(for i in 0 1 2 3; do echo "${CMS[$i]} $i"; done | sort -n | awk '{ print $2 }'))
log "ranked fastest first: $(for i in "${RANK[@]}"; do printf '%s (%s) ' "${DBN[$i]}" "${CMS[$i]}"; done)"
ALL=$(IFS=,; echo "${RANK[*]}")

# ---- 1: one engine for four shards
run_arm s4-big 1024 8000 "$ALL" "$BIG"
delvms "$BIG"
stopreq

# ---- 2-4: the recommended shape, two replicas per shard, shrinking
mkvms c4a-standard-8 "${SMALL[@]}"
run_arm s4-r8 128 1000 "$ALL" "${SMALL[@]}"
delinst "${DBN[${RANK[2]}]}" "${DBN[${RANK[3]}]}"; delvms "${SMALL[@]:4:4}"
run_arm s2-r4 128 1000 "${RANK[0]},${RANK[1]}" "${SMALL[@]:0:4}"
delinst "${DBN[${RANK[1]}]}"; delvms "${SMALL[@]:2:2}"
run_arm s1-r2 128 1000 "${RANK[0]}" "${SMALL[@]:0:2}"
log "all arms done"
