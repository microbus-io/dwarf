# J14 (x-rig step: `BIN=bench-<sha> x-rig.sh j14 1000 j14-step.sh`, with $RIG_HOME/bench-<sha>-k3 beside it - the same
# build with three fast-archiving and three fast-deletion jobs a shard). One 32 vCPU instance,
# fresh databases, saturated linear 0 B, cap 2,000:
#   j14-warm   4 min, not read (a fresh instance's first checkpoints read low, bench/CLAUDE.md).
#   j14-on     two jobs of each fast kind, archiving on: does archiving keep up? out/age-j14-on.log is the oldest closed
#              root not archived every 10 s; out/j14-on.json the steps/s, p99, dwarf_flows_closed against
#              dwarf_flows_archived, the lag gauges by job, dwarf_janitor_batch_seconds by job and part, the pool's wait.
#   j14-off    archiving off (DWARF_TESTING_ARCHIVE_GRACE=24h): what archiving costs dispatch.
#   j14-k3-on  three jobs of each fast kind, archiving on - only when $RIG_HOME/j14-decision says k3 (written after
#              j14-on and j14-off are read; the rig waits up to 60 min for it, then skips the arm).
#   j14-large  two jobs, archiving on, plus a few fan-outs of 270,000 branches - past the 256k staging threshold: the
#              staged copies and chunked deletions under the load, out/pgss-chunk-j14-large.txt (the chunks' step
#              reads), and auto_explain's plans over 250 ms in Cloud Logging (DBFLAGS).
# Every arm: out/slow-<tag>.log, the statements active over 500 ms every 250 ms: which statement a slow janitor
# round runs.
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
WIN_MIN=4 one_run j14-warm $SAT
mon j14-on 14; WIN_MIN=14 one_run j14-on $SAT; ages j14-on
mon j14-off 14; RUN_ENV="DWARF_TESTING_ARCHIVE_GRACE=24h" WIN_MIN=14 one_run j14-off $SAT
log "WAITING FOR DECISION: echo k3 > $S/j14-decision (or skip); 60 min"
decision=skip
for i in $(seq 1 120); do
  [ -s "$S/j14-decision" ] && { decision=$(tr -d '[:space:]' < "$S/j14-decision"); break; }
  late; sleep 30
done
log "decision: $decision"
if [ "$decision" = k3 ]; then
  vmssh "$ADMIN" 'ln -sf dwarf-bench-k3.bin dwarf-bench' || { log "SWITCH TO K3 FAILED"; exit 1; }
  mon j14-k3-on 14; WIN_MIN=14 one_run j14-k3-on $SAT; ages j14-k3-on
  vmssh "$ADMIN" 'ln -sf dwarf-bench.bin dwarf-bench' || { log "SWITCH TO K2 FAILED"; exit 1; }
fi
mon j14-large 14
vmssh "$ADMIN" "setsid nohup ./pgss-chunk.sh $(( 30 + (2 + 14) * 60 - 60 )) out/pgss-chunk-j14-large.txt '$(dsn "${IPS[0]}" dwarf_j14_large)' > /dev/null 2>&1 < /dev/null &"
WIN_MIN=14 one_run j14-large $SAT -mix "0.999997:linear:0,0.000003:fanout:0:270000"
