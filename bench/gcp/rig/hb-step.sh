# HB (x-rig step): the pool and two WAL levers on one 32 vCPU instance, linear saturated (cap 2,000), 2 min warmup + 6
# min window, a fresh database per arm, max_wal_size 20 GB + lz4 + checkpoint_timeout 900 + the 1 min archive grace.
# Launch with a 1 TB disk:
#   BIN=bench-<sha> DBFLAGS=autovacuum_work_mem=1048576,checkpoint_timeout=900,log_checkpoints=on,max_wal_size=20480,wal_compression=lz4 \
#     x-rig.sh hb 1000 hb-step.sh
#   hb-warm      0 B, 3 min, not read: a fresh instance creates its WAL segments until its second checkpoint completes,
#                which reads ~8% low (bench/CLAUDE.md). Its draw is logged (DB core-ms per step).
#   hb-0-250     0 B at the shipped pool (250).
#   hb-0-500     0 B at -max-connections 500: the pool was 250/250 in use, 200-350 callers waiting, in every HA arm.
#                The faster of the two (500 only if >= 2% faster) is the pool for the arms below.
#   hb-64k       64 KB -payload-json, the baseline for the two WAL levers.
#   hb-64k-wb    wal_buffers 64 MB (Cloud SQL's flag counts 8 KB pages: 8192), against the 16 MB the default caps at.
#   hb-64k-2tb   the disk grown to 2 TB, wal_buffers kept: does the flush's ~316 MB/s term scale with the disk?
#                Last, because a Cloud SQL disk grows but never shrinks.
HB_FLAGS=autovacuum_work_mem=1048576,checkpoint_timeout=900,log_checkpoints=on,max_wal_size=20480,wal_compression=lz4
hb_show() {
  log "  $1: $(adm "$(admdsn "${IPS[0]}")" "SELECT string_agg(name||'='||setting||COALESCE(unit,''), ' ' ORDER BY name) FROM pg_settings WHERE name IN ('max_wal_size','checkpoint_timeout','wal_compression','wal_buffers','max_connections')") disk=$(gc sql instances describe "$DB" --format='value(settings.dataDiskSizeGb)')GB"
}
hb_wait() { local i; for i in $(seq 1 45); do [[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] && break; sleep 20; done; sleep 30; }
hb_steps() { vmssh "$ADMIN" "python3 -c \"import json; print(round(json.load(open('out/$1.json'))['results'][0]['stepsPerSec']))\"" | tail -1; }

SAT="-workload linear -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
export WIN_MIN=6
hb_show hb-warm
WIN_MIN=3 one_run hb-warm $SAT -payload 0
sleep 150 # Cloud Monitoring lands CPU a few minutes late
gc compute scp --zone=$ZONE "$ADMIN":~/out/hb-warm.json "$OUT/" >/dev/null 2>&1 &&
  log "  draw: $(DB_INSTANCE="$DB" VCPUS=32 WRITE=1 "$R/bench/gcp/dbcpu.sh" "$OUT/hb-warm.json" 2>&1 | tail -1)"
one_run hb-0-250 $SAT -payload 0
one_run hb-0-500 $SAT -payload 0 -max-connections 500
a=$(hb_steps hb-0-250); b=$(hb_steps hb-0-500)
POOL=""
[[ "$a" =~ ^[0-9]+$ && "$b" =~ ^[0-9]+$ ]] && (( b * 100 >= a * 102 )) && POOL="-max-connections 500"
log "  pool: 250 -> ${a:-?}, 500 -> ${b:-?} steps/s; the 64 KB arms run ${POOL:-at the shipped pool}"

one_run hb-64k $SAT -payload 65536 -payload-json $POOL

authok; log "patching $DB's flags: wal_buffers 64 MB (restarts it)"
if gc sql instances patch "$DB" --database-flags="$HB_FLAGS,wal_buffers=8192" --quiet > "$OUT/patch-wb.log" 2>&1; then
  hb_wait; hb_show hb-64k-wb
  one_run hb-64k-wb $SAT -payload 65536 -payload-json $POOL
else
  log "  wal_buffers PATCH FAILED (patch-wb.log) - skipping hb-64k-wb"
fi

authok; log "growing $DB's disk to 2 TB"
if gc sql instances patch "$DB" --storage-size=2000 --quiet > "$OUT/patch-disk.log" 2>&1; then
  hb_wait; hb_show hb-64k-2tb
  one_run hb-64k-2tb $SAT -payload 65536 -payload-json $POOL
else
  log "  DISK PATCH FAILED (patch-disk.log) - skipping hb-64k-2tb"
fi
