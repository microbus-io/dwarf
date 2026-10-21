# V bytes (x-rig step: `BIN=<pin> DBFLAGS=<as gm> x-rig.sh vbytes 2000 vbytes-step.sh`). Does a database hold its
# rate as its data grows past 500 GB? The bench's -volume mode fills with `state` (5 steps, each rewriting a 64 KB
# incompressible payload) and stops as dwarf_steps passes each size on disk, a doubling ladder from 16 to 512 GB
# (GB_LADDER), for a 1 + 6 min saturated probe of `linear` at 0 B; then it measures Purge and the reaper at size.
# The ladder is by stored size, not rows: a probe writes ~8M small step rows (~9 GB) of its own, which a row
# count would read as most of the fill. Its rows count toward the size, so a checkpoint the probe carries the
# database past is taken at once; the artifact records each checkpoint's true size (stepsDiskMB).
# One probe per checkpoint, as planned: the 2/3-rate window doubles each checkpoint's rows and time.
#   read: each probe's saturated steps/s and p99 against stepsDiskMB, with v-mon's per-minute size, dead tuples
#   and cache hit ratio. July (8 vCPU): a one-time ~15-20% settling by ~32 GB, then flat to 405 GB. The first run
#   (2026-10-20, row-keyed) gave 21,573 steps/s empty and 20,963 at 18.6 GB.
#   ~2 h: ~35 min of fill at ~250 MB/s, seven probes of ~8 min, provisioning.
GB_LADDER=${GB_LADDER:-16 32 64 128 256 512}
mb=""
for g in $GB_LADDER; do mb="$mb$(( g * 1024 )),"; done
mb=${mb%,}
tag=v-bytes db=dwarf_v_bytes
authok; late
fresh "${IPS[0]}" "$db"
vmssh "$ADMIN" "setsid nohup ./v-mon.sh $(( 4 * 3600 )) out/vmon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 4 * 3600 )) out/mon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
start=$(( $(date +%s) + 30 ))
log "run $tag: fill by stored size, probing at $mb MB of dwarf_steps (start $(date -r $start '+%H:%M:%S'))"
# -volume prints a line per checkpoint, often many minutes apart, so one_run's line-count watchdog would kill a
# healthy fill; the bench's own -stall-timeout covers a hang instead.
vmssh "$ADMIN" "WARMUP=1m setsid nohup ./g-run.sh $tag $start '$(dsn "${IPS[0]}" "$db")' -volume-checkpoints-mb $mb -workload state -payload 0 -concurrency 256 -volume-probe 6m -volume-probe-workload linear -volume-probe-two-thirds=false -max-outstanding 2000 > /dev/null 2>&1 < /dev/null &"
until vmssh "$ADMIN" "test -f out/$tag.done"; do late; sleep 60; done
log "run $tag done: $(vmssh "$ADMIN" "cat out/$tag.done; grep -E '^probe at|INVALID|STALLED' out/$tag.log" | tr '\n' ';')"
adm "$(dsn "${IPS[0]}" "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
drop "${IPS[0]}" "$db"
