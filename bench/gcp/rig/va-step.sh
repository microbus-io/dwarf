# V-archive, lz4 at 1 MB, sustained load (x-rig step, _REDO_BENCH.md "Next rig session", planned 2026-11-07):
#   BIN=<pin> DBFLAGS=autovacuum_work_mem=1048576,max_wal_size=20480,checkpoint_timeout=900,wal_compression=lz4,log_checkpoints=on \
#     x-rig.sh va 1000 va-step.sh
# One 32 vCPU instance and one c4a-16, a fresh database per arm, the archive on. ~4 h with provisioning.
#   va-warm  3 min saturated linear 0 B, not read: a new server creates its WAL files until its first checkpoints.
#   va-big   the archive grows in one day: -volume fill (linear 0 B) to 100M step rows, both sides counted, with a
#            6 min saturated probe on the empty database and at 25M / 50M / 100M, and History/Step of the first
#            archived flow and ListClosed at each. Read: probe steps/s and p99 against the archive's size, the
#            archived read latencies, v-mon's sizes and s-mon's waits (LWLock:LockManager among them).
#   va-days  many days, rollovers and drops under load: DWARF_ARCHIVE_DAY=600s, -archive-retention 1, 30 min
#            saturated linear 0 B. A day rolls every 10 min and, from ~20 min, the oldest is dropped. Read: the
#            10 s series around each rollover and drop, s-mon's waits, the database's checkpoint log.
#   va-reads32 / va-reads128  lock-manager pressure: hr-sat's load plus 32 / 128 goroutines reading archived
#            flows' History, 6 min each (see the arm).
#   hr-sat / hr-100 / hr-95  sustained load: saturated 6 min, then offered at 100% and 95% of its rate for 16 min
#            each, max-outstanding high so a backlog can grow. Read: delivered against offered, outstanding first
#            -> last minute, p50/p99.
#   lz-on / lz-off  1 MB -payload-json, saturated, 6 min each; between them wal_compression is patched off, which
#            restarts the instance, and a 3 min arm (lz-warm, not read) runs before lz-off. Read: steps/s, WAL MB/s
#            and KB/step, full-page images per step; keep the lz4 recommendation only if it buys more than ~3%.
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"

RUN_ENV="WARMUP=30s" WIN_MIN=3 one_run va-warm $SAT

# ---- va-big: a -volume fill prints a line per checkpoint, minutes apart, so it runs outside one_run's line-count
# watchdog; the bench's own -stall-timeout covers a hang.
tag=va-big db=dwarf_va_big
authok; late
fresh "${IPS[0]}" "$db"
vmssh "$ADMIN" "setsid nohup ./v-mon.sh $(( 3 * 3600 )) out/vmon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 3 * 3600 )) out/mon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
start=$(( $(date +%s) + 30 ))
log "run $tag: fill to 100M step rows on the archive side, probing at 25M / 50M / 100M (start $(date -r $start '+%H:%M:%S'))"
vmssh "$ADMIN" "WARMUP=1m setsid nohup ./g-run.sh $tag $start '$(dsn "${IPS[0]}" "$db")' -volume 100000000 -volume-side archive -volume-checkpoints 25000000,50000000,100000000 -workload linear -payload 0 -concurrency 256 -volume-probe 6m -volume-probe-two-thirds=false -max-outstanding 2000 > /dev/null 2>&1 < /dev/null &"
until vmssh "$ADMIN" "test -f out/$tag.done"; do late; sleep 60; done
log "run $tag done: $(vmssh "$ADMIN" "cat out/$tag.done; grep -E '^probe at|INVALID|STALLED' out/$tag.log" | tr '\n' ';')"
adm "$(dsn "${IPS[0]}" "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
drop "${IPS[0]}" "$db"
sleep 20

# ---- va-days
RUN_ENV="DWARF_ARCHIVE_DAY=600s" WIN_MIN=30 one_run va-days $SAT -archive-retention 1

# ---- sustained load: the 100% and 95% rates come from hr-sat's window mean (10 s samples), in flows/s of the
# 10-step chain.
WIN_MIN=6 one_run hr-sat $SAT
# Lock-manager pressure (rerun order item 10): the same saturated load with 32 and then 128 goroutines reading the
# History of archived flows, against hr-sat as the no-reads control. Read: s-mon's nfp= (locks outside the fast-path
# slots) and LWLock/LockManager waits, steps/s against hr-sat, the artifact's archivedReads (reads/s, p50/p99).
WIN_MIN=6 one_run va-reads32 $SAT -archived-readers 32
WIN_MIN=6 one_run va-reads128 $SAT -archived-readers 128
sat=$(vmssh "$ADMIN" "grep -E '^[0-9]+ +[0-9]+ ' out/hr-sat.log | awk '{s+=\$2; n++} END {if (n) printf \"%.0f\", s/n}'")
if [[ "$sat" =~ ^[0-9]+$ ]] && (( sat > 1000 )); then
  r100=$(( sat / 10 )) r95=$(( sat * 95 / 1000 ))
  log "sustained load: saturated $sat steps/s -> offered $r100 and $r95 flows/s"
  WIN_MIN=16 one_run hr-100 -workload linear -payload 0 -open-loop -arrival-rate $r100 -concurrency 256 -max-outstanding 100000
  WIN_MIN=16 one_run hr-95 -workload linear -payload 0 -open-loop -arrival-rate $r95 -concurrency 256 -max-outstanding 100000
else
  log "sustained load: no saturated rate from hr-sat ($sat) - skipping hr-100 and hr-95"
fi

# ---- lz4 at 1 MB. A smaller backlog cap than the 0 B arms: each flow in flight holds megabytes in the engine.
LZ="-workload linear -payload 1048576 -payload-json -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 500"
WIN_MIN=6 one_run lz-on $LZ
log "patching wal_compression off (restarts the instance)"
gc sql instances patch "$DB" --database-flags="${DBFLAGS/wal_compression=lz4/wal_compression=off}" --quiet > "$OUT/patch-lz-off.log" 2>&1
for i in $(seq 1 45); do [[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] && break; sleep 20; done
log "  wal_compression now: $(adm "$(admdsn "${IPS[0]}")" "SHOW wal_compression")"
RUN_ENV="WARMUP=30s" WIN_MIN=3 one_run lz-warm $LZ
WIN_MIN=6 one_run lz-off $LZ
