# V bytes (x-rig step: `BIN=<pin> DBFLAGS=<as gm> x-rig.sh vbytes 2000 vbytes-step.sh`). Does a database hold its
# rate as its data grows past 500 GB? The bench's -volume mode fills with `state` (5 steps, each rewriting a 64 KB
# incompressible payload), and at each checkpoint stops the fill for a 1 + 6 min saturated probe and a probe at
# 2/3 of that rate, both `linear` at 0 B, then measures Purge and the reaper at size.
# Checkpoints are step rows, at ~74 KB of stored data each (64 KB x ~1.15 measured in W): a doubling ladder from 16
# to 512 GB (GB_LADDER). The artifact records the true size at each checkpoint (stepsDiskMB) - chart against that.
#   read: each probe's saturated steps/s and its 2/3 p50/p99 against stepsDiskMB, with v-mon's per-minute size,
#   dead tuples and cache hit ratio. July (8 vCPU): a one-time ~15-20% settling by ~32 GB, then flat to 405 GB.
#   ~2.5 h: ~35 min of fill at ~250 MB/s, seven checkpoints of ~15 min of probes and drains.
GB_LADDER=${GB_LADDER:-16 32 64 128 256 512}
ROW_KB=74
rows=""
for g in $GB_LADDER; do rows="$rows$(( g * 1024 * 1024 / ROW_KB )),"; done
rows=${rows%,}
target=${rows##*,}
tag=v-bytes db=dwarf_v_bytes
authok; late
fresh "${IPS[0]}" "$db"
vmssh "$ADMIN" "setsid nohup ./v-mon.sh $(( 5 * 3600 )) out/vmon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 5 * 3600 )) out/mon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
start=$(( $(date +%s) + 30 ))
log "run $tag: fill to $target step rows, probing at $rows (start $(date -r $start '+%H:%M:%S'))"
# -volume prints a line per checkpoint, often 15 minutes apart, so one_run's line-count watchdog would kill a
# healthy fill; the bench's own -stall-timeout covers a hang instead.
vmssh "$ADMIN" "WARMUP=1m setsid nohup ./g-run.sh $tag $start '$(dsn "${IPS[0]}" "$db")' -volume $target -volume-checkpoints $rows -workload state -payload 0 -concurrency 256 -volume-probe 6m -volume-probe-workload linear -max-outstanding 2000 > /dev/null 2>&1 < /dev/null &"
until vmssh "$ADMIN" "test -f out/$tag.done"; do late; sleep 60; done
log "run $tag done: $(vmssh "$ADMIN" "cat out/$tag.done; grep -E '^probe at|INVALID|STALLED' out/$tag.log" | tr '\n' ';')"
adm "$(dsn "${IPS[0]}" "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
drop "${IPS[0]}" "$db"
