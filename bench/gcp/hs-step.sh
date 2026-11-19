# HS (x-rig step: `BIN=bench-<sha> x-rig.sh hs 1000 hs-step.sh`, with $RIG_HOME/bench-<sha>.riginv beside $BIN, both
# from one clean commit; DARM=1 adds the failover arm). The standard rig on the current build: one 32 vCPU instance at
# the recommended settings, one c4a-16, a fresh database per arm, 2 min warmup + 6 min window unless stated. One draw,
# n=1: hardware differs more between instances than runs differ on one, so repeats come from later rigs.
#   hs-warm   3 min saturated, not read: a fresh instance's first checkpoints create its WAL files (bench/CLAUDE.md).
#   hs-a      A, the 0 B headline: saturated linear 0 B, cap 2,000, archiving on. Also whether archiving keeps up:
#             out/age-hs-a.log, the oldest closed root still live, stays near the 60 s grace.
#   hs-off    archiving held off (DWARF_TESTING_ARCHIVE_GRACE=24h): what archiving costs dispatch on this draw.
#   hs-del    every flow DeleteOnCompletion: fast deletion keeps up (out/age-hs-del.log's sixth field, the oldest
#             completed root marked for deletion, near the 60 s deletion grace; the live rows in mon-hs-del.log.tables
#             level off).
#   hs-wide   C: open-loop linear 0 B at two thirds of hs-a's rate, 2 min warmup + 12 min window. From about window
#             minute 1 an await-only bench process creates one forEach of 100,000 branches and awaits it, then
#             270,000, then 1,000,000, a minute apart (inject.sh; times in out/hs-wide-inject.log). Read against
#             them: out/rss-hs-wide.log, every bench process's RSS each second (the main one runs the spawn);
#             out/mon-hs-wide.log.xmin, the oldest open transaction every 5 s (the spawn's and the fan-in's
#             length); out/slow-hs-wide.log, statements active over 500 ms; the main run's 10 s steps/s series.
#   hs-e-*    E: default_toast_compression pglz then lz4 (set on the bench's role, so every new session takes it),
#             at 8 KB and 1 MB -payload-json, saturated. Read DB CPU per step over each window (dbcpu.sh -w),
#             WAL KB/step and archiveStepsBytes per step.
#   hs-d      D (DARM=1, last): the instance patched to high availability (a restart), hs-d-warm 3 min, then the
#             loop workload at 600 flows/s on two replicas, 2 min warmup + 12 min window, `gcloud sql instances
#             failover` at window minute 6. Pass, by ck-check.sh: every flow completed, every history 10 long, the
#             invariant checks, dwarf_steps_unwedged and dwarf_steps_write_failed 0.
# Every saturated arm: out/slow-<tag>.log, the statements active over 500 ms every 250 ms.
gc compute scp --zone=$ZONE "$D/age-mon.sh" "$D/slow-mon.sh" "$D/rss-mon.sh" "$D/inject.sh" "$D/ck-check.sh" "$ADMIN":~/ >/dev/null 2>&1 &&
  gc compute scp --zone=$ZONE "$S/$BIN.riginv" "$ADMIN":~/riginv >/dev/null 2>&1 &&
  vmssh "$ADMIN" 'chmod +x age-mon.sh slow-mon.sh rss-mon.sh inject.sh ck-check.sh riginv; sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq jq >/dev/null 2>&1; command -v jq' >/dev/null ||
  { log "HS DEPLOY FAILED"; exit 1; }
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
# admok <command>: whether a command on the admin VM succeeds. vmssh retries a command that fails, so it answers instead.
admok() { [ "$(vmssh "$ADMIN" "$1 && echo y || echo n")" = y ]; }
# mon <tag> <window-min>: the age and slow-statement samplers on the arm's database, from 45 s in (one_run makes the
# database just before its 30 s lead-in) to the end of the drain.
mon() {
  local secs=$(( (2 + $2) * 60 + 60 )) d; d=$(dsn "${IPS[0]}" "dwarf_${1//-/_}")
  vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./age-mon.sh $secs out/age-$1.log \"$d\"' > /dev/null 2>&1 < /dev/null &"
  vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./slow-mon.sh $secs out/slow-$1.log \"$d\"' > /dev/null 2>&1 < /dev/null &"
}
# ages <tag> [field]: the oldest closed root still live (s), from the age file's field (2, completed roots the archive
# takes; 6, those marked for deletion), at window minutes 2, 4 and 6, the worst from minute 2, and the last - over the
# run only: a sample taken after the bench exited reads a table no engine is draining.
ages() {
  log "  $1 oldest closed root still live (s) at 2/4/6 min, worst from 2, last: $(vmssh "$ADMIN" "e=\$(stat -c %Y out/$1.done); awk -v f=${2:-2} -v e=\$e 'NR==1{t0=\$1} \$1>e {exit} {m=int((\$1-t0)/60); a[m]=\$f; last=\$f} m>=4 && \$f>w {w=\$f} END {printf \"%s %s %s, %s, %s\", a[4], a[6], a[8], w+0, last}' out/age-$1.log")"
}

WIN_MIN=3 one_run hs-warm $SAT
mon hs-a 6; WIN_MIN=6 one_run hs-a $SAT; ages hs-a
mon hs-off 6; RUN_ENV="DWARF_TESTING_ARCHIVE_GRACE=24h" WIN_MIN=6 one_run hs-off $SAT
mon hs-del 6; WIN_MIN=6 one_run hs-del $SAT -delete-on-completion; ages hs-del 6

# C. The age file starts 45 s after one_run is called and the window 2.5 min after it, so window minute 1 is ~3.5 min.
sat=$(vmssh "$ADMIN" "jq -r '.results[0].stepsPerSec // 0' out/hs-a.json" | tr -d '[:space:]')
rate=$(awk -v s="${sat:-0}" 'BEGIN { r = s * 2 / 3 / 10; if (r < 100) r = 1400; printf "%d", r }')
log "hs-wide: background at $rate flows/s, two thirds of hs-a's ${sat:-?} steps/s"
vmssh "$ADMIN" "setsid nohup ./rss-mon.sh $(( 30 + 20 * 60 )) out/rss-hs-wide.log > /dev/null 2>&1 < /dev/null &"
vmssh "$ADMIN" "setsid nohup ./inject.sh 210 '$(dsn "${IPS[0]}" dwarf_hs_wide)' hs-wide 100000 270000 1000000 > /dev/null 2>&1 < /dev/null &"
mon hs-wide 12; WIN_MIN=12 one_run hs-wide -workload linear -payload 0 -open-loop -arrival-rate "$rate" -concurrency 256 -max-outstanding 20000
until admok "grep -q done out/hs-wide-inject.log"; do late; sleep 20; done
log "  hs-wide injections: $(vmssh "$ADMIN" "awk '{ if (\$2 == \"start\") s[\$3] = \$1; else if (\$2 == \"end\") printf \"%s %ds (exit %s), \", \$3, \$1 - s[\$3], \$5 }' out/hs-wide-inject.log")"

# E. The setting is the role's, read by every session the next arm opens.
toast() { adm "$(admdsn "${IPS[0]}")" "ALTER ROLE postgres SET default_toast_compression = '$1'" >/dev/null
  log "  default_toast_compression for new sessions: $(adm "$(admdsn "${IPS[0]}")" 'SHOW default_toast_compression')"; }
for p in 8192 1048576; do
  for c in pglz lz4; do
    toast $c; WIN_MIN=6 one_run hs-e-$c-$p -workload linear -payload $p -payload-json -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
  done
done
adm "$(admdsn "${IPS[0]}")" "ALTER ROLE postgres RESET default_toast_compression" >/dev/null

# D. Last: the patch restarts the instance and doubles its cost from here on.
[ -n "${DARM:-}" ] || return 0
authok
log "patching $DB to high availability (REGIONAL; restarts it)"
gc sql instances patch "$DB" --availability-type=REGIONAL --backup-start-time=08:00 --enable-point-in-time-recovery --quiet > "$OUT/patch-ha.log" 2>&1
for i in $(seq 1 60); do [[ "$(gc sql instances describe "$DB" --format='value(state,settings.availabilityType)' 2>/dev/null)" == RUNNABLE*REGIONAL ]] && break; sleep 20; done
log "  $DB: $(gc sql instances describe "$DB" --format='value(state,settings.availabilityType,gceZone,secondaryGceZone)' 2>/dev/null | tr '\t' ' ')"
[[ "$(gc sql instances describe "$DB" --format='value(settings.availabilityType)' 2>/dev/null)" == REGIONAL ]] || { log "HA PATCH FAILED: $(tail -2 "$OUT/patch-ha.log")"; return 0; }
WIN_MIN=3 one_run hs-d-warm $SAT
DLOAD="-workload loop -linear-steps 10 -open-loop -arrival-rate 600 -max-outstanding 20000 -concurrency 64 -replicas 2 -task-delay-dist 0.80:50ms,0.20:2s"
tag=hs-d db=dwarf_hs_d
authok; late
fresh "${IPS[0]}" "$db"
start=$(( $(date +%s) + 30 ))
log "run $tag: $DLOAD, failover at window minute 6 (start $(date -r $start '+%H:%M:%S'))"
vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 30 + 17 * 60 )) out/mon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
vmssh "$ADMIN" "WINDOW=12m setsid nohup ./g-run.sh $tag $start '$(dsn "${IPS[0]}" "$db")' $DLOAD > /dev/null 2>&1 < /dev/null &"
while (( $(date +%s) < start + 120 + 360 )); do sleep 5; done
if admok "test -f out/$tag.done"; then
  log "  run $tag ENDED before the failover: $(vmssh "$ADMIN" "cat out/$tag.done; grep -m2 -iE 'error|must|invalid|panic' out/$tag.log")"
else
  log "  FAILOVER $DB"
  gc sql instances failover "$DB" --quiet > "$OUT/failover.log" 2>&1
  log "  failover returned $?: $(gc sql instances describe "$DB" --format='value(state,gceZone,secondaryGceZone)' 2>/dev/null | tr '\t' ' ')"
fi
until admok "test -f out/$tag.done"; do late; sleep 20; done
log "run $tag done: $(vmssh "$ADMIN" "cat out/$tag.done")"
vmssh "$ADMIN" "./ck-check.sh $tag '$(dsn "${IPS[0]}" "$db")' 10" | sed 's/^/    /'
adm "$(dsn "${IPS[0]}" "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
drop "${IPS[0]}" "$db"
