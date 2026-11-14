# J15 (x-rig step: `BIN=bench-<sha> x-rig.sh j15 1000 j15-step.sh`, with $RIG_HOME/bench-<sha>-k3 beside it - the same
# build with three fast-archiving and three fast-deletion jobs a shard; _JANITOR.md step 13). J14's arms at the
# recommended database settings (max_wal_size 20 GB, checkpoint_timeout 15 min, wal_compression lz4, which x-rig.sh
# adds). One 32 vCPU instance, fresh databases, saturated linear 0 B, cap 2,000:
#   j15-warm   4 min, not read (a fresh instance's first checkpoints read low, bench/CLAUDE.md).
#   j15-on     two jobs of each fast kind, archiving on: does archiving keep up? out/age-j15-on.log is the oldest closed
#              root not archived every 10 s; out/j15-on.json the steps/s, p99, dwarf_flows_closed against
#              dwarf_flows_archived, the lag gauges by job, dwarf_janitor_batch_seconds by job and part, the pool's wait.
#   j15-off    archiving off (DWARF_ARCHIVE_GRACE=24h): what archiving costs dispatch.
#   j15-k3-on  three jobs of each fast kind, archiving on - only when two did NOT keep up (user): the oldest unarchived
#              root past 90 s (the 60 s grace and slack) at any sample from minute 5 of j15-on. $RIG_HOME/j15-decision
#              (k3 or skip), when present, overrides.
#   j15-large  two jobs, archiving on, plus a few fan-outs of 270,000 branches - past the 256k staging threshold: the
#              staged copies and chunked deletions under the load, out/pgss-chunk-j15-large.txt (the chunks' step
#              reads), and auto_explain's plans over 250 ms in Cloud Logging (DBFLAGS).
# Every arm: out/slow-<tag>.log, the statements active over 500 ms every 250 ms (_BUGS.md #11).
gc compute scp --zone=$ZONE "$D/age-mon.sh" "$D/slow-mon.sh" "$D/pgss-chunk.sh" "$ADMIN":~/ >/dev/null 2>&1 &&
  gc compute scp --zone=$ZONE "$S/$BIN-k3" "$ADMIN":~/dwarf-bench-k3.bin >/dev/null 2>&1 &&
  vmssh "$ADMIN" 'chmod +x age-mon.sh slow-mon.sh pgss-chunk.sh dwarf-bench-k3.bin' || { log "J14 DEPLOY FAILED"; exit 1; }
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
# mon <tag> <window-min>: the age and slow-statement samplers on the arm's database, from 45 s in (one_run makes the
# database just before its 30 s lead-in), to the end of the drain.
mon() {
  local secs=$(( (2 + $2) * 60 + 60 )) d; d=$(dsn "${IPS[0]}" "dwarf_${1//-/_}")
  vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./age-mon.sh $secs out/age-$1.log \"$d\"' > /dev/null 2>&1 < /dev/null &"
  vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./slow-mon.sh $secs out/slow-$1.log \"$d\"' > /dev/null 2>&1 < /dev/null &"
}
# ages <tag>: the oldest closed root not archived (completed), at 2, 5, 8, 11 and 14 min into the window and at the end.
ages() {
  log "  $1 oldest unarchived (s) at 2/5/8/11/14 min, last: $(vmssh "$ADMIN" "awk 'NR==1{t0=\$1} {a[int((\$1-t0)/60)]=\$2; last=\$2} END {printf \"%s %s %s %s %s, %s\", a[2], a[5], a[8], a[11], a[14], last}' out/age-$1.log")"
}
WIN_MIN=4 one_run j15-warm $SAT
mon j15-on 14; WIN_MIN=14 one_run j15-on $SAT; ages j15-on
mon j15-off 14; RUN_ENV="DWARF_ARCHIVE_GRACE=24h" WIN_MIN=14 one_run j15-off $SAT
# Two jobs kept up unless the oldest unarchived root passed 90 s from minute 5 of j15-on.
worst=$(vmssh "$ADMIN" "awk 'NR==1{t0=\$1} \$1-t0>=300 && \$2>m {m=\$2} END {print m+0}' out/age-j15-on.log")
decision=skip; [[ "$worst" =~ ^[0-9]+$ ]] && (( worst > 90 )) && decision=k3
[ -s "$S/j15-decision" ] && decision=$(tr -d '[:space:]' < "$S/j15-decision")
log "j15-on worst oldest-unarchived from minute 5: ${worst:-?} s"
log "decision: $decision"
if [ "$decision" = k3 ]; then
  vmssh "$ADMIN" 'ln -sf dwarf-bench-k3.bin dwarf-bench' || { log "SWITCH TO K3 FAILED"; exit 1; }
  mon j15-k3-on 14; WIN_MIN=14 one_run j15-k3-on $SAT; ages j15-k3-on
  vmssh "$ADMIN" 'ln -sf dwarf-bench.bin dwarf-bench' || { log "SWITCH TO K2 FAILED"; exit 1; }
fi
mon j15-large 14
vmssh "$ADMIN" "setsid nohup ./pgss-chunk.sh $(( 30 + (2 + 14) * 60 - 60 )) out/pgss-chunk-j15-large.txt '$(dsn "${IPS[0]}" dwarf_j15_large)' > /dev/null 2>&1 < /dev/null &"
WIN_MIN=14 one_run j15-large $SAT -mix "0.999997:linear:0,0.000003:fanout:0:270000"
