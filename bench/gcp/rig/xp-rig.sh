#!/usr/bin/env bash
# Usage: xp-rig.sh <name> <db-tier> <engine-vm-type> <tag>   The tier table's engine pairing: one Cloud SQL Postgres 16
# instance of <db-tier> (1 TB SSD, Query Insights), TWO engine hosts of <engine-vm-type> each running one replica that
# also generates half the load, and a c4a-standard-2 admin host for psql and the monitors. One saturated linear 0 B run,
# 2 min warmup + WIN (default 16m) window (cap 2,000 and 256 creators split evenly), fresh database. The shard's steps/s
# is the SUM of the two replicas' artifacts. Pulls everything, then tears down. DBFLAGS and TIER work as in x-rig.sh.
#   ARGS   extra bench flags for both replicas, e.g. "-max-connections 500" (the shard's budget, split between them)
#   ARGS2  when set, a second arm on the same instance after the first, tagged <tag>-b, with these flags in place of
#          ARGS - the A/B a fresh instance per arm cannot give (same-spec instances differ by more than most levers)
#   WARM   minutes of a throwaway run first, on its own database (0 = none): a fresh instance creates its WAL segments
#          until its second checkpoint completes and reads low until then (bench/CLAUDE.md)
set -u
NAME=$1 TIER=$2 ETYPE=$3 TAG=$4 BIN=${BIN:?set BIN to a pinned binary in $RIG_HOME}
D=$(cd "$(dirname "$0")" && pwd)                      # this kit: the step scripts and the VM-side helpers
R=$(cd "$D/../../.." && pwd)                          # the repository; artifacts land in $R/bench/results
S=${RIG_HOME:-$R/tmp/rig}                             # never committed: dbpass and the pinned bench binaries
PROJECT=${PROJECT:-dwarf-bench-mbus} ZONE=${ZONE:-us-east4-a}
OUT=$R/bench/results/$(date +%Y%m%d-%H%M)-$NAME; mkdir -p "$OUT"
ADMIN=dwarf-$NAME-m DB=dwarf-$NAME-db EA=dwarf-$NAME-ea EB=dwarf-$NAME-eb
VMS=("$ADMIN" "$EA" "$EB")
HARD=$(( $(date +%s) + 120 * 60 ))
log() { echo "[$(date '+%H:%M:%S')] $*"; }
gc() { gcloud --project="$PROJECT" "$@"; }
authok() { local i; for i in $(seq 1 60); do gcloud auth print-access-token >/dev/null 2>&1 && return 0
  log "AUTH EXPIRED - waiting for gcloud auth login ($i/60)"; sleep 60; done; log "AUTH still expired - stopping"; exit 1; }
late() { (( $(date +%s) > HARD )) && { log "HARD DEADLINE - stopping"; exit 1; }; return 0; }
fw() { local ip; ip=$(curl -fsS -m 10 https://checkip.amazonaws.com | tr -d '[:space:]') &&
  gc compute firewall-rules update dwarf-bench-ssh --source-ranges="$ip/32" >/dev/null 2>&1; }
vmssh() { local vm=$1 i; for i in 1 2 3 4 5; do
  gc compute ssh "$vm" --zone="$ZONE" --command "$2" -- -o ConnectTimeout=20 2>/dev/null && return 0; fw; sleep 15; done; return 1; }
pull() { local vm; for vm in "${VMS[@]}"; do mkdir -p "$OUT/$vm"
  ( vmssh "$vm" 'tar czf /tmp/out.tgz out 2>/dev/null' && gc compute scp --zone=$ZONE "$vm":/tmp/out.tgz "$OUT/$vm/out.tgz" >/dev/null 2>&1 &&
    (cd "$OUT/$vm" && tar xzf out.tgz && rm out.tgz) ) && log "artifacts from $vm saved" || log "ARTIFACT PULL FAILED: $vm"; done; }
teardown() { log "teardown in ${TEARDOWN_GRACE:-30}s (kill -9 $$ keeps the rig)"; sleep "${TEARDOWN_GRACE:-30}"
  log "teardown ${VMS[*]} $DB"
  gc compute instances delete "${VMS[@]}" --zone=$ZONE --quiet >/dev/null 2>&1
  gc sql instances delete "$DB" --quiet --async >/dev/null 2>&1; }
trap 'pull; teardown; log EXIT' EXIT
pass=$(cat $S/dbpass)
authok

log "creating $DB ($TIER, 1000 GB SSD) + $ADMIN (c4a-2) + $EA, $EB ($ETYPE); tag $TAG"
# The recommended settings (bench/CLAUDE.md, "Running the cloud rig"; docs.dwarf.run's deployment page), each added
# unless DBFLAGS sets its own value: 1 GB of dead-row memory per autovacuum worker (at Cloud SQL's 64 MB a vacuum pass
# rescans every index once per ~11M dead rows), and checkpoints sized for the write load.
for f in autovacuum_work_mem=1048576 max_wal_size=20480 checkpoint_timeout=900; do
  [[ ,$DBFLAGS, == *,${f%%=*}=* ]] || DBFLAGS=${DBFLAGS:+$DBFLAGS,}$f
done
gc sql instances create "$DB" --database-version=POSTGRES_16 --tier="$TIER" --edition=enterprise \
  --zone=$ZONE --storage-type=SSD --storage-size=1000 --insights-config-query-insights-enabled \
  --network="projects/$PROJECT/global/networks/dwarf-bench-net" --no-assign-ip \
  ${DBFLAGS:+--database-flags="$DBFLAGS"} --root-password="$pass" > "$OUT/create-db.log" 2>&1 &
gc compute instances create "$ADMIN" --zone=$ZONE --machine-type=c4a-standard-2 --network=dwarf-bench-net \
  --image-family=debian-12-arm64 --image-project=debian-cloud --labels=app=dwarf-bench > "$OUT/vm.log" 2>&1 &&
gc compute instances create "$EA" "$EB" --zone=$ZONE --machine-type="$ETYPE" --network=dwarf-bench-net \
  --image-family=debian-12-arm64 --image-project=debian-cloud --labels=app=dwarf-bench >> "$OUT/vm.log" 2>&1 ||
  { log "VM create failed"; exit 1; }
for vm in "${VMS[@]}"; do
  deployed=""
  for i in 1 2 3 4 5 6; do
    gc compute scp --zone=$ZONE "$D/g-run.sh" "$D/s-mon.sh" "$D/fsync.sh" "$vm":~/ >/dev/null 2>&1 &&
      gc compute scp --zone=$ZONE "$S/$BIN" "$vm":~/dwarf-bench.bin >/dev/null 2>&1 &&
      vmssh "$vm" 'ln -sf dwarf-bench.bin dwarf-bench && chmod +x g-run.sh s-mon.sh fsync.sh dwarf-bench.bin && mkdir -p out' && { deployed=1; break; }
    fw; sleep 20
  done
  [ -n "$deployed" ] || { log "DEPLOY FAILED: $vm"; exit 1; }
done
vmssh "$ADMIN" 'sudo apt-get update -qq >/dev/null 2>&1; sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1; command -v psql' >/dev/null ||
  { log "no psql"; exit 1; }
wait
# A failed create can still produce a RUNNABLE instance (bench/CLAUDE.md, INTERNAL_ERROR), so poll for it.
for i in $(seq 1 45); do [[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] && break; sleep 20; done
[[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] || { log "$DB not RUNNABLE"; exit 1; }
IP=$(gc sql instances describe "$DB" --format='value(ipAddresses[0].ipAddress)')
adm() { vmssh "$ADMIN" "psql '$1' -X -q -At -F' ' -c \"$2\""; }
dsn() { echo "postgres://postgres:${pass}@$IP:5432/$1?sslmode=disable"; }
ADM=$(dsn postgres)
log "ready: $DB $(adm "$ADM" 'SHOW max_connections') max_connections"
log "flush floor: $(vmssh "$ADMIN" "./fsync.sh '$ADM'" | tail -1 | tee -a "$OUT/fsync.txt")"

BASE="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 128 -max-outstanding 1000"
SAT="$BASE ${ARGS:-}"
WIN=${WIN:-16m} WARM=${WARM:-0}
if (( WARM > 0 )); then
  authok; late; wdb="dwarf_${TAG//-/_}_warm"
  adm "$ADM" "DROP DATABASE IF EXISTS $wdb WITH (FORCE)" >/dev/null; adm "$ADM" "CREATE DATABASE $wdb" >/dev/null
  start=$(( $(date +%s) + 45 ))
  log "warm-up $TAG-warm on $EA + $EB, ${WARM} min, not read"
  for vm in "$EA" "$EB"; do
    vmssh "$vm" "WARMUP=0s WINDOW=${WARM}m setsid nohup ./g-run.sh $TAG-warm $start '$(dsn "$wdb")' $SAT > /dev/null 2>&1 < /dev/null &"
  done
  for vm in "$EA" "$EB"; do until vmssh "$vm" "test -f out/$TAG-warm.done"; do late; sleep 30; done; done
  adm "$ADM" "DROP DATABASE IF EXISTS $wdb WITH (FORCE)" >/dev/null
  sleep 45 # the warm-up's replica rows age out of the peer freshness window (~40 s) before the real run
fi
# run_one <tag> <bench args>: one measured arm on a fresh database, both replicas, then its database is checked and dropped.
run_one() { local tag=$1 SAT=$2 db start vm x tot
  authok; late
  log "flush floor before $tag: $(vmssh "$ADMIN" "./fsync.sh '$ADM'" | tail -1 | tee -a "$OUT/fsync.txt")"
  db="dwarf_${tag//-/_}"
  adm "$ADM" "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null; adm "$ADM" "CREATE DATABASE $db" >/dev/null
  start=$(( $(date +%s) + 45 ))
  log "run $tag on $EA + $EB: $SAT, window $WIN (start $(date -r $start '+%H:%M:%S'))"
  vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 45 + (${WIN%m} + 3) * 60 )) out/mon-$tag.log '$(dsn "$db")' > /dev/null 2>&1 < /dev/null &"
  for vm in "$EA" "$EB"; do
    vmssh "$vm" "WINDOW=$WIN setsid nohup ./g-run.sh $tag $start '$(dsn "$db")' $SAT > /dev/null 2>&1 < /dev/null &"
  done
  for vm in "$EA" "$EB"; do
    until vmssh "$vm" "test -f out/$tag.done"; do late; sleep 30; done
  done
  tot=0
  for vm in "$EA" "$EB"; do
    x=$(vmssh "$vm" "python3 -c \"import json; r=json.load(open('out/$tag.json'))['results'][0]; print(round(r['stepsPerSec']))\"" | tail -1)
    log "  $vm: ${x:-?} steps/s"; [[ "$x" =~ ^[0-9]+$ ]] && tot=$(( tot + x ))
  done
  log "run $tag done: $tot steps/s (sum of both replicas)"
  adm "$(dsn "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
  # The per-process drain verdict is advisory on a multi-host fleet (bench/CLAUDE.md, "Run guards"): the database is
  # the record. Every flow must be completed and no step left non-terminal.
  log "flows by status: $(adm "$(dsn "$db")" "SELECT string_agg(status||'='||n, ' ') FROM (SELECT status, count(*) n FROM dwarf_flows GROUP BY status) x")"
  log "steps not completed: $(adm "$(dsn "$db")" "SELECT count(*) FROM dwarf_steps WHERE status<>'completed'")"
  adm "$ADM" "DROP DATABASE IF EXISTS $db WITH (FORCE)" >/dev/null
}
run_one "$TAG" "$SAT"
if [ -n "${ARGS2:-}" ]; then
  sleep 45 # the first arm's replica rows age out of the peer freshness window (~40 s)
  run_one "$TAG-b" "$BASE $ARGS2"
fi
