# CKPT (x-rig step: `BIN=bench-<sha> DBFLAGS=max_wal_size=65536 x-rig.sh <name> 1000 ckpt-step.sh`). Whether
# checkpoints spaced well past the archive's one-minute grace cut the WAL an archived step costs, and with it keep the
# archive up at 64 KB. At 20 GB a checkpoint came every 35-65 s under archrate-step.sh's arms, no longer
# than the grace, so nearly every archived step's pages crossed a checkpoint between their write and the archive's
# delete, and ~77 of the 113 KB ar-r85 logged a step were full-page images (2026-11-19). This step reruns ar-r85 on an
# instance created with max_wal_size 64 GB: checkpoints ~3x further apart at the same WAL rate. One draw, n=1, against
# ar-r85 on another instance.
#   ck-warm   saturated, cap 2,000, 2 min + 6 min, not read: past two checkpoints at 64 GB (~34 GB of WAL each), so
#             the window runs on recycled WAL files (bench/CLAUDE.md).
#   ck-r85    open loop at ar-r85's 231 flows/s (85% of its instance's saturated 2,722 steps/s), 2 min + 10 min.
# Read (archread and ckread, logged after the arm): the lag, flows archived against closed, the jobs' batches, live
# dwarf_steps rows by minute, WAL KB and page images a step, the flush, and the seconds between checkpoints. ar-r85
# read 2,299 steps/s, lag peak 93 s, 86% archived, live rows rising, 113 KB and 9.6 page images a step, flush 6.3 ms.
# ~30 min of arms.
mws=$(adm "$(admdsn "${IPS[0]}")" 'SHOW max_wal_size' | tr -d '[:space:]')
log "max_wal_size: $mws"
[[ "$mws" == 64GB ]] || { log "max_wal_size is not 64GB - set DBFLAGS=max_wal_size=65536"; exit 1; }
P64="-workload linear -payload 65536 -payload-json"
WIN_MIN=6 one_run ck-warm $P64 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
WIN_MIN=10 one_run ck-r85 $P64 -open-loop -arrival-rate 231 -concurrency 256 -max-outstanding 20000
archread ck-r85
ckread ck-r85
