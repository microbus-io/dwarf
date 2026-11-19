#!/usr/bin/env bash
# Usage: BIN=bench-<sha> clock-rig.sh [name]   The database clock stepped under load: the trigger the lease fence
# (lease_seq) exists for, which the fixtures only simulate by writing lease_expires. Cloud SQL's clock cannot be moved,
# so the database is a self-managed PostgreSQL 16 on a GCE VM (DBTYPE, default c4a-standard-8) with its time sync off
# (pgvm-setup.sh); the engine runs on a second VM (ENGTYPE, default c4a-standard-4) whose clock stays synced, so after
# a step the two disagree, as they would in production. Runs the arms in ARMS, pulls, tears everything down.
#
# $RIG_HOME (default tmp/rig) holds dbpass and two binaries built from one clean commit:
#   CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build  -o $RIG_HOME/bench-<sha>        ./bench
#   CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -o $RIG_HOME/bench-<sha>.riginv ./bench
#
# The load (every arm, a fresh database each): the `loop` workload, 10 steps a flow, open loop at RATE flows/s
# (default 150), two replicas in one process (a re-dispatched step can land on the other one), a 25 s time budget -
# so a lease is 55 s and a +60 s step expires every running one - and tasks of 100 ms, 5 s and 20 s, so at the step
# some finish before lease recovery reaches them (their write lands), some after it re-pended them (their write lands,
# the re-claim loses), and some after a peer re-claimed them (the zombie: its write must be fenced). Lease recovery
# runs every minute on each dispatcher, so the 20 s tasks are the ones that reach the third case. Every `loop` step
# checks it was handed exactly one message per step before it, so a zombie write that landed fails the next step.
#
# Arms (ARMS, default "ctl fwd back both"):
#   ctl    no step - the counters' baseline (lease recovery should not fire). Archive held off (grace 24h), so every
#          flow stays live for the invariant checks.
#   fwd    +60 s at window minute 2; the clock is stepped back after the checks, on an idle server. Archive held off.
#   back   -60 s at window minute 2 (leases outlive their tasks; durations read negative); restored after.
#   both   +60 s then -60 s - the NTP correction coming back - with the archive on and 5-minute days
#          (DWARF_TESTING_ARCHIVE_DAY=300s), the run's start aligned so the forward step lands 30 s into a new day and
#          the backward one 40 s back into the day before it. The invariant checks see the live tables only; the
#          per-table history check reads the archive's days too.
#   fwd5m  (opt-in) +300 s: past the peers' straggler window, as a resumed or migrated VM can jump.
# REUSE=1 runs the arms on the rig a killed run left up (kill -9 in its teardown grace), skipping creation and setup;
# FWMADE=1 if that run created the firewall rule, so this one deletes it at teardown.
# Pass: no flow failed, terminated or left running; no `loop` check failed (artifact errors 0, every completed flow's
# history 10 long); the invariant checks pass; dwarf_steps_unwedged and dwarf_steps_write_failed 0 - and in the
# stepped arms, steps claimed more than once (lease_seq > 1), so the fence had something to fence.
set -u
NAME=${1:-ck} BIN=${BIN:?set BIN to a pinned binary in $RIG_HOME}
D=$(cd "$(dirname "$0")" && pwd)
R=$(cd "$D/../../.." && pwd)
S=${RIG_HOME:-$R/tmp/rig}
PROJECT=${PROJECT:-dwarf-bench-mbus} ZONE=${ZONE:-us-east4-a}
OUT=$R/bench/results/$(date +%Y%m%d-%H%M)-$NAME; mkdir -p "$OUT"
DBVM=dwarf-$NAME-db ENG=dwarf-$NAME-m
HARD=$(( $(date +%s) + 240 * 60 ))
FWRULE=dwarf-bench-pg FWMADE=${FWMADE:-}
log() { echo "[$(date '+%H:%M:%S')] $*"; }
gc() { gcloud --project="$PROJECT" "$@"; }
authok() { local i; for i in $(seq 1 60); do gcloud auth print-access-token >/dev/null 2>&1 && return 0
  log "AUTH EXPIRED - waiting for gcloud auth login ($i/60)"; sleep 60; done; log "AUTH still expired - stopping"; exit 1; }
late() { (( $(date +%s) > HARD )) && { log "HARD DEADLINE - stopping"; exit 1; }; return 0; }
fw() { local ip; ip=$(curl -fsS -m 10 https://checkip.amazonaws.com | tr -d '[:space:]') &&
  gc compute firewall-rules update dwarf-bench-ssh --source-ranges="$ip/32" >/dev/null 2>&1; }
vmssh() { local vm=$1 i; for i in 1 2 3 4 5; do
  gc compute ssh "$vm" --zone="$ZONE" --command "$2" -- -o ConnectTimeout=20 2>/dev/null && return 0; fw; sleep 15; done; return 1; }
pull() {
  ( vmssh "$ENG" 'tar czf /tmp/out.tgz out 2>/dev/null' && gc compute scp --zone=$ZONE "$ENG":/tmp/out.tgz "$OUT/out.tgz" >/dev/null 2>&1 &&
    (cd "$OUT" && tar xzf out.tgz && rm out.tgz) ) && log "artifacts saved to $OUT" || log "ARTIFACT PULL FAILED"
  vmssh "$DBVM" 'sudo cat /var/log/postgresql/postgresql-16-main.log' > "$OUT/postgresql.log" 2>/dev/null || log "PG LOG PULL FAILED"
}
teardown() { log "teardown in ${TEARDOWN_GRACE:-30}s (kill -9 $$ keeps the rig)"; sleep "${TEARDOWN_GRACE:-30}"
  log "teardown $ENG $DBVM"
  gc compute instances delete "$ENG" --zone=$ZONE --quiet >/dev/null 2>&1
  gc compute instances delete "$DBVM" --zone=$ZONE --quiet >/dev/null 2>&1
  [ -n "$FWMADE" ] && gc compute firewall-rules delete "$FWRULE" --quiet >/dev/null 2>&1; }
trap 'pull; teardown; log EXIT' EXIT
pass=$(cat "$S/dbpass")
[ -s "$S/$BIN" ] && [ -s "$S/$BIN.riginv" ] || { log "missing $S/$BIN or $S/$BIN.riginv"; exit 1; }
authok

if [ -n "${REUSE:-}" ]; then
  DBIP=$(gc compute instances describe "$DBVM" --zone=$ZONE --format='value(networkInterfaces[0].networkIP)') &&
    [ -n "$DBIP" ] || { log "REUSE: no $DBVM"; exit 1; }
  log "reusing $DBVM ($DBIP) and $ENG"
else
# PostgreSQL from the engine VM: the rig network's private range (the ssh rule admits only this workstation).
if ! gc compute firewall-rules describe "$FWRULE" >/dev/null 2>&1; then
  gc compute firewall-rules create "$FWRULE" --network=dwarf-bench-net --allow=tcp:5432 --source-ranges=10.0.0.0/8 \
    --target-tags=dwarf-pg >/dev/null 2>&1 || { log "firewall rule create failed"; exit 1; }
  FWMADE=1
fi
log "creating $DBVM (${DBTYPE:-c4a-standard-8}, ${DISK:-200} GB hyperdisk) + $ENG (${ENGTYPE:-c4a-standard-4})"
gc compute instances create "$DBVM" --zone=$ZONE --machine-type=${DBTYPE:-c4a-standard-8} --network=dwarf-bench-net \
  --image-family=debian-12-arm64 --image-project=debian-cloud --boot-disk-type=hyperdisk-balanced \
  --boot-disk-size=${DISK:-200}GB --boot-disk-provisioned-iops=${IOPS:-10000} --boot-disk-provisioned-throughput=${TPUT:-400} \
  --tags=dwarf-pg --labels=app=dwarf-bench > "$OUT/db-vm.log" 2>&1 &
gc compute instances create "$ENG" --zone=$ZONE --machine-type=${ENGTYPE:-c4a-standard-4} --network=dwarf-bench-net \
  --image-family=debian-12-arm64 --image-project=debian-cloud --labels=app=dwarf-bench > "$OUT/eng-vm.log" 2>&1
wait
gc compute instances describe "$DBVM" --zone=$ZONE >/dev/null 2>&1 || { log "$DBVM create failed: $(tail -2 "$OUT/db-vm.log")"; exit 1; }
gc compute instances describe "$ENG" --zone=$ZONE >/dev/null 2>&1 || { log "$ENG create failed: $(tail -2 "$OUT/eng-vm.log")"; exit 1; }
DBIP=$(gc compute instances describe "$DBVM" --zone=$ZONE --format='value(networkInterfaces[0].networkIP)')

deployed=""
for i in 1 2 3 4 5 6; do
  gc compute scp --zone=$ZONE "$D/pgvm-setup.sh" "$S/dbpass" "$DBVM":~/ >/dev/null 2>&1 && deployed=1 && break
  fw; sleep 20
done
[ -n "$deployed" ] || { log "DB VM DEPLOY FAILED"; exit 1; }
# One ssh, no retry: vmssh retries a failed command, and the setup is not idempotent.
gc compute ssh "$DBVM" --zone="$ZONE" --command 'chmod +x pgvm-setup.sh && ./pgvm-setup.sh dbpass > setup.log 2>&1; rc=$?; tail -3 setup.log; exit $rc' -- -o ConnectTimeout=20 > "$OUT/db-setup.txt" 2>&1 ||
  { log "PG SETUP FAILED: $(tail -3 "$OUT/db-setup.txt")"; exit 1; }
vmssh "$DBVM" 'rm -f dbpass'
log "database VM: $(tail -1 "$OUT/db-setup.txt")"
deployed=""
for i in 1 2 3 4 5 6; do
  gc compute scp --zone=$ZONE "$D/g-run.sh" "$D/s-mon.sh" "$D/fsync.sh" "$D/clk-mon.sh" "$D/ck-check.sh" "$ENG":~/ >/dev/null 2>&1 &&
    gc compute scp --zone=$ZONE "$S/$BIN" "$ENG":~/dwarf-bench.bin >/dev/null 2>&1 &&
    gc compute scp --zone=$ZONE "$S/$BIN.riginv" "$ENG":~/riginv >/dev/null 2>&1 &&
    vmssh "$ENG" 'ln -sf dwarf-bench.bin dwarf-bench && chmod +x *.sh dwarf-bench.bin riginv && mkdir -p out' && { deployed=1; break; }
  fw; sleep 20
done
[ -n "$deployed" ] || { log "ENGINE VM DEPLOY FAILED"; exit 1; }
vmssh "$ENG" 'sudo apt-get update -qq >/dev/null 2>&1; sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client jq >/dev/null 2>&1; command -v psql && command -v jq' >/dev/null ||
  { log "no psql or jq on $ENG"; exit 1; }
fi

adm() { vmssh "$ENG" "psql '$1' -X -q -At -F' ' -c \"$2\""; }
dsn() { echo "postgres://postgres:${pass}@$DBIP:5432/$1?sslmode=disable"; }
fresh() { adm "$(dsn postgres)" "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null; adm "$(dsn postgres)" "CREATE DATABASE $1" >/dev/null; }
drop() { adm "$(dsn postgres)" "DROP DATABASE IF EXISTS $1 WITH (FORCE)" >/dev/null; }
offset() { vmssh "$ENG" "a=\$(date +%s.%N); d=\$(psql '$(dsn postgres)' -X -At -c 'SELECT extract(epoch FROM clock_timestamp())'); b=\$(date +%s.%N); awk -v a=\$a -v b=\$b -v d=\$d 'BEGIN { printf \"%+.3f\", d - (a + b) / 2 }'"; }
log "ready: $(adm "$(dsn postgres)" 'SHOW max_connections') max_connections; database clock offset $(offset) s"
log "flush floor: $(vmssh "$ENG" "./fsync.sh '$(dsn postgres)'" | tail -1 | tee -a "$OUT/fsync.txt")"

# step <delta>: move the database VM's wall clock by <delta> seconds (+N or -N), and log its offset after.
step() {
  local r; r=$(vmssh "$DBVM" "a=\$(date +%s.%N); sudo date -s '$1 seconds' >/dev/null && b=\$(date +%s.%N) && echo \"\$a -> \$b\"")
  log "  CLOCK STEP $1 s on $DBVM: ${r:-FAILED}; offset now $(offset) s" | tee -a "$OUT/steps.txt"
}
# ck_arm <tag> <window-min> "<s>:<delta> ..." <bench args...>: a fresh database, the run on the engine VM (warmup 2
# min, then the window), each event stepping the database clock <delta> seconds at <s> seconds after the run starts,
# then ck-check.sh, the database dropped, and the clock stepped back by the arm's net step. RUN_ENV prefixes the run
# with environment settings; ALIGN=<day-seconds> starts the run at 30 s past a day boundary of that length.
ck_arm() {
  local tag=$1 win=$2 events=$3; shift 3
  local db="dwarf_${tag//-/_}" start now ev net=0
  authok; late
  fresh "$db"
  vmssh "$ENG" "rm -f out/$tag.done"
  now=$(date +%s); start=$(( now + 30 ))
  [ -n "${ALIGN:-}" ] && start=$(( start + ((30 - start % ALIGN) % ALIGN + ALIGN) % ALIGN ))
  log "run $tag (window ${win} min, steps: ${events:-none}, env: ${RUN_ENV:-none}): $* (start $(date -r $start '+%H:%M:%S'))"
  vmssh "$ENG" "setsid nohup ./clk-mon.sh $(( start - now + (win + 6) * 60 )) out/clk-$tag.log '$(dsn postgres)' > /dev/null 2>&1 < /dev/null &"
  vmssh "$ENG" "setsid nohup sh -c 'sleep $(( start - now + 45 )); ./s-mon.sh $(( (win + 4) * 60 )) out/mon-$tag.log \"$(dsn "$db")\"' > /dev/null 2>&1 < /dev/null &"
  vmssh "$ENG" "${RUN_ENV:-} WINDOW=${win}m setsid nohup ./g-run.sh $tag $start '$(dsn "$db")' $* > /dev/null 2>&1 < /dev/null &"
  for ev in $events; do
    while (( $(date +%s) < start + ${ev%%:*} )); do sleep 1; done
    # A run that already ended (a refused flag, a crash) gets no step: the arm is void, and its log says why.
    if [ "$(vmssh "$ENG" "test -f out/$tag.done && echo y || echo n")" = y ]; then
      log "  run $tag ENDED before its step at ${ev%%:*} s: $(vmssh "$ENG" "cat out/$tag.done; grep -m2 -iE 'error|must|invalid|panic' out/$tag.log")"
      break
    fi
    step "${ev#*:}"; net=$(( net + ${ev#*:} ))
  done
  # vmssh retries a command that fails, so a poll that must fail until the run ends answers instead.
  until [ "$(vmssh "$ENG" "test -f out/$tag.done && echo y || echo n")" = y ]; do late; sleep 20; done
  log "run $tag done: $(vmssh "$ENG" "cat out/$tag.done")"
  vmssh "$ENG" "./ck-check.sh $tag '$(dsn "$db")' 10" | sed 's/^/    /'
  adm "$(dsn "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
  drop "$db"
  (( net != 0 )) && step "$(printf %+d $(( -net )))"
  sleep 20
}

LOAD="-workload loop -linear-steps 10 -open-loop -arrival-rate ${RATE:-150} -max-outstanding 20000 -concurrency 64 -replicas 2 -time-budget 25s -task-delay-dist 0.60:100ms,0.25:5s,0.15:20s"
LIVE="DWARF_TESTING_ARCHIVE_GRACE=24h"
for arm in ${ARMS:-ctl fwd back both}; do
  case $arm in
    ctl) RUN_ENV=$LIVE ck_arm ck-ctl 8 "" $LOAD ;;
    fwd) RUN_ENV=$LIVE ck_arm ck-fwd 8 "240:+60" $LOAD ;;
    back) RUN_ENV=$LIVE ck_arm ck-back 8 "240:-60" $LOAD ;;
    both) RUN_ENV="DWARF_TESTING_ARCHIVE_DAY=300s" ALIGN=300 ck_arm ck-both 11 "240:+60 530:-60" $LOAD ;;
    fwd5m) RUN_ENV=$LIVE ck_arm ck-fwd5m 8 "240:+300" $LOAD ;;
    *) log "unknown arm $arm" ;;
  esac
done
log "arms done"
