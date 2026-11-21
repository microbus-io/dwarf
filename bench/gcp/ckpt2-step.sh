# CKPT2 (x-rig step, on the rig ckpt-step.sh ran: `BIN=bench-<sha> REUSE=1 x-rig.sh <name> 1000 ckpt2-step.sh`).
# ckpt-step.sh's arms again on a faster WAL disk. A Cloud SQL SSD's bandwidth grows with its size: at 2 TB a saturated
# 64 KB arm wrote ~1.35x the WAL of 1 TB (2026-11-06), but at a fixed 20 GB max_wal_size its checkpoints came that much
# sooner and the extra bandwidth went to page images (12.9 a step against 8.3), not steps. So the disk is grown to 2 TB
# and max_wal_size raised with it, to 96 GB, which keeps checkpoints at least as far apart in seconds as 64 GB put them
# at 1 TB. Against ckpt-step.sh's arms on the same instance. One draw, n=1. The disk only grows: the rig is deleted
# after this step.
#   ck2-warm  saturated, cap 2,000, 2 min + 6 min: past two checkpoints at 96 GB (~50 GB of WAL each), and past the
#             patch's restart if it made one. Read, as an indication of the saturated rate only: its first minutes
#             create WAL files (bench/CLAUDE.md).
#   ck2-r85   open loop at ck-r85's 231 flows/s, 2 min + 10 min.
# Read per arm: archread and ckread (a-step-lib.sh). ~30 min with the patch.
st0=$(adm "$(admdsn "${IPS[0]}")" 'SELECT pg_postmaster_start_time()')
log "growing $DB's disk to 2 TB and max_wal_size to 96 GB"
gc sql instances patch "$DB" --storage-size=2000 --database-flags=max_wal_size=98304,autovacuum_work_mem=1048576,checkpoint_timeout=900 --quiet > "$OUT/patch.log" 2>&1
for i in $(seq 1 45); do [[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] && break; sleep 20; done
sleep 30
mws=$(adm "$(admdsn "${IPS[0]}")" 'SHOW max_wal_size' | tr -d '[:space:]')
disk=$(gc sql instances describe "$DB" --format='value(settings.dataDiskSizeGb)')
st1=$(adm "$(admdsn "${IPS[0]}")" 'SELECT pg_postmaster_start_time()')
log "max_wal_size: $mws, disk: ${disk} GB, restarted by the patch: $([[ "$st0" == "$st1" ]] && echo no || echo "yes ($st1)")"
[[ "$mws" == 96GB && "$disk" == 2000 ]] || { log "the patch did not take - stopping"; exit 1; }
P64="-workload linear -payload 65536 -payload-json"
WIN_MIN=6 one_run ck2-warm $P64 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
archread ck2-warm
ckread ck2-warm
WIN_MIN=10 one_run ck2-r85 $P64 -open-loop -arrival-rate 231 -concurrency 256 -max-outstanding 20000
archread ck2-r85
ckread ck2-r85
