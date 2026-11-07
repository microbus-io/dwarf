#!/usr/bin/env bash
# Usage: x-rig.sh <name> <disk-gb> <step>   One 32 vCPU Cloud SQL instance (Query Insights on, <disk-gb> SSD) and one
# c4a-standard-16 that is both the engine host (pinned binary $BIN) and the admin host (psql, monitors). Sources
# $D/<step> (which uses one_run / the helpers below), pulls, tears down. Nothing else is created; the instance and
# the VM are created together because every step uses both from its first minute.
set -u
NAME=$1 DISK=$2 STEP=$3 BIN=${BIN:?set BIN to a pinned binary in $RIG_HOME}
# DBFLAGS (optional) passes Cloud SQL database flags, e.g. slow-statement plans and lock waits:
#   DBFLAGS='cloudsql.enable_auto_explain=on,auto_explain.log_min_duration=10000,log_lock_waits=on'
D=$(cd "$(dirname "$0")" && pwd)                      # this kit: the step scripts and the VM-side helpers
R=$(cd "$D/../../.." && pwd)                          # the repository; artifacts land in $R/bench/results
S=${RIG_HOME:-$R/tmp/rig}                             # never committed: dbpass and the pinned bench binaries
PROJECT=${PROJECT:-dwarf-bench-mbus} ZONE=${ZONE:-us-east4-a}
OUT=$R/bench/results/redo-$NAME-$(date +%Y%m%d); mkdir -p "$OUT"
ADMIN=dwarf-$NAME-m DB=dwarf-$NAME-db
VMS=("$ADMIN") DBS=("$DB")
HARD=$(( $(date +%s) + 600 * 60 ))
log() { echo "[$(date '+%H:%M:%S')] $*"; }
gc() { gcloud --project="$PROJECT" "$@"; }
authok() { local i; for i in $(seq 1 60); do gcloud auth print-access-token >/dev/null 2>&1 && return 0
  log "AUTH EXPIRED - waiting for gcloud auth login ($i/60)"; sleep 60; done; log "AUTH still expired - stopping"; exit 1; }
late() { (( $(date +%s) > HARD )) && { log "HARD DEADLINE - stopping"; exit 1; }; return 0; }
fw() { local ip; ip=$(curl -fsS -m 10 https://checkip.amazonaws.com | tr -d '[:space:]') &&
  gc compute firewall-rules update dwarf-bench-ssh --source-ranges="$ip/32" >/dev/null 2>&1; }
vmssh() { local vm=$1 i; for i in 1 2 3 4 5; do
  gc compute ssh "$vm" --zone="$ZONE" --command "$2" -- -o ConnectTimeout=20 2>/dev/null && return 0; fw; sleep 15; done; return 1; }
pull() { ( vmssh "$ADMIN" 'tar czf /tmp/out.tgz out 2>/dev/null' && gc compute scp --zone=$ZONE "$ADMIN":/tmp/out.tgz "$OUT/out.tgz" >/dev/null 2>&1 &&
  (cd "$OUT" && tar xzf out.tgz && rm out.tgz) ) && log "artifacts saved to $OUT" || log "ARTIFACT PULL FAILED"; }
teardown() { log "teardown in ${TEARDOWN_GRACE:-30}s (kill -9 $$ keeps the rig)"; sleep "${TEARDOWN_GRACE:-30}"
  log "teardown ${VMS[*]} ${DBS[*]}"
  gc compute instances delete "${VMS[@]}" --zone=$ZONE --quiet >/dev/null 2>&1
  gc sql instances delete "$DB" --quiet --async >/dev/null 2>&1; }
trap 'pull; teardown; log EXIT' EXIT
pass=$(cat $S/dbpass)
authok

log "creating $DB (${TIER:-db-custom-32-131072}, ${DISK} GB SSD, Query Insights) + $ADMIN (c4a-16); step $STEP"
# 1 GB of dead-row memory per autovacuum worker: at Cloud SQL's 64 MB default a vacuum pass rescans every index once
# per ~11M dead rows. DBFLAGS may set its own value.
[[ ,$DBFLAGS, == *,autovacuum_work_mem=* ]] || DBFLAGS=${DBFLAGS:+$DBFLAGS,}autovacuum_work_mem=1048576
gc sql instances create "$DB" --database-version=POSTGRES_16 --tier=${TIER:-db-custom-32-131072} --edition=enterprise \
  --zone=$ZONE --storage-type=SSD --storage-size="$DISK" --insights-config-query-insights-enabled \
  --network="projects/$PROJECT/global/networks/dwarf-bench-net" --no-assign-ip \
  ${DBFLAGS:+--database-flags="$DBFLAGS"} --root-password="$pass" > "$OUT/create-db.log" 2>&1 &
gc compute instances create "$ADMIN" --zone=$ZONE --machine-type=${VMTYPE:-c4a-standard-16} --network=dwarf-bench-net \
  --image-family=debian-12-arm64 --image-project=debian-cloud --labels=app=dwarf-bench > "$OUT/vm.log" 2>&1 ||
  { log "VM create failed"; exit 1; }
deployed=""
for i in 1 2 3 4 5 6; do
  gc compute scp --zone=$ZONE "$D/g-run.sh" "$D/s-mon.sh" "$D/v-mon.sh" "$D/fsync.sh" "$ADMIN":~/ >/dev/null 2>&1 &&
    gc compute scp --zone=$ZONE "$S/$BIN" "$ADMIN":~/dwarf-bench.bin >/dev/null 2>&1 &&
    vmssh "$ADMIN" 'ln -sf dwarf-bench.bin dwarf-bench && chmod +x g-run.sh s-mon.sh v-mon.sh fsync.sh dwarf-bench.bin && mkdir -p out' && { deployed=1; break; }
  fw; sleep 20
done
[ -n "$deployed" ] || { log "DEPLOY FAILED"; exit 1; }
vmssh "$ADMIN" 'sudo apt-get update -qq >/dev/null 2>&1; sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1; command -v psql' >/dev/null ||
  { log "no psql"; exit 1; }
wait
# A failed create can still produce a RUNNABLE instance (bench/CLAUDE.md, INTERNAL_ERROR), so poll for it.
for i in $(seq 1 45); do [[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] && break; sleep 20; done
[[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] || { log "$DB not RUNNABLE"; exit 1; }
IPS=("$(gc sql instances describe "$DB" --format='value(ipAddresses[0].ipAddress)')")

adm() { vmssh "$ADMIN" "psql '$1' -X -q -At -F' ' -c \"$2\""; }
dsn() { echo "postgres://postgres:${pass}@$1:5432/$2?sslmode=disable"; }
admdsn() { echo "postgres://postgres:${pass}@$1:5432/postgres?sslmode=disable"; }
fresh() { adm "$(admdsn "$1")" "DROP DATABASE IF EXISTS $2 WITH (FORCE)" >/dev/null; adm "$(admdsn "$1")" "CREATE DATABASE $2" >/dev/null; }
drop() { adm "$(admdsn "$1")" "DROP DATABASE IF EXISTS $2 WITH (FORCE)" >/dev/null; }
log "ready: $DB $(adm "$(admdsn "${IPS[0]}")" 'SHOW max_connections') max_connections, disk ${DISK} GB"
# The draw's disk, which a CPU reading cannot see (bench/CLAUDE.md): the idle WAL flush floor, logged and kept.
log "flush floor: $(vmssh "$ADMIN" "./fsync.sh '$(admdsn "${IPS[0]}")'" | tail -1 | tee -a "$OUT/fsync.txt")"
source "$D/a-step-lib.sh"
log "step $STEP"
source "$D/$STEP"
log "step $STEP done"
