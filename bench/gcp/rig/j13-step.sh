# J13 (x-rig step: `BIN=bench-<sha> x-rig.sh j13 1000 j13-step.sh`, with $RIG_HOME/bench-<sha>-turn beside it).
# One 32 vCPU instance, fresh databases, saturated linear 0 B, cap 2,000:
#   j13-warm   build A, 4 min, not read (a fresh instance's first checkpoints read low, bench/CLAUDE.md).
#   j13-a-on   build A, archiving on: does archiving keep up? out/j13-a-on.json - steps/s, p99, dwarf_flows_closed
#              against dwarf_flows_archived, dwarf_archive_lag_seconds, dwarf_janitor_batch_seconds by job and part
#              (against the batch's ~0.25 s aim), the pool's wait; out/age-j13-a-on.log, the oldest closed root not
#              archived every 10 s; and the base run, out/mon-j13-a-on.log.tables (dwarf_flows n_live_tup and bytes).
#   j13-b-on   build B ($BIN-turn: a batch takes a turn and no connection, its statements the pool's), archiving on.
#   j13-a-off  build A, archiving off (DWARF_TESTING_ARCHIVE_GRACE=24h): what archiving costs dispatch.
#   j13-large  build A, archiving on, plus a few fan-outs of 270,000 branches - past the 256k staging threshold, so
#              each is copied in stages and its live copy deleted in chunks, right after the grace, under the load:
#              the large_archiving and sweep_deletion batch times, out/pgss-chunk-j13-large.txt (the chunks' step
#              reads: rows and time a call), and auto_explain's plans over 250 ms in Cloud Logging (DBFLAGS).
# Every arm: out/slow-<tag>.log, the statements active over 500 ms every 250 ms: which statement a slow janitor
# round runs. ~60 min of arms.
gc compute scp --zone=$ZONE "$D/age-mon.sh" "$D/slow-mon.sh" "$D/pgss-chunk.sh" "$ADMIN":~/ >/dev/null 2>&1 &&
  gc compute scp --zone=$ZONE "$S/$BIN-turn" "$ADMIN":~/dwarf-bench-b.bin >/dev/null 2>&1 &&
  vmssh "$ADMIN" 'chmod +x age-mon.sh slow-mon.sh pgss-chunk.sh dwarf-bench-b.bin' || { log "J13 DEPLOY FAILED"; exit 1; }
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
# mon <tag> <window-min>: the age and slow-statement samplers on the arm's database, from 45 s in (one_run makes the
# database just before its 30 s lead-in), to the end of the drain.
mon() {
  local secs=$(( (2 + $2) * 60 + 60 )) d; d=$(dsn "${IPS[0]}" "dwarf_${1//-/_}")
  vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./age-mon.sh $secs out/age-$1.log \"$d\"' > /dev/null 2>&1 < /dev/null &"
  vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./slow-mon.sh $secs out/slow-$1.log \"$d\"' > /dev/null 2>&1 < /dev/null &"
}
WIN_MIN=4 one_run j13-warm $SAT
mon j13-a-on 14; WIN_MIN=14 one_run j13-a-on $SAT
vmssh "$ADMIN" 'ln -sf dwarf-bench-b.bin dwarf-bench' || { log "SWITCH TO B FAILED"; exit 1; }
mon j13-b-on 14; WIN_MIN=14 one_run j13-b-on $SAT
vmssh "$ADMIN" 'ln -sf dwarf-bench.bin dwarf-bench' || { log "SWITCH TO A FAILED"; exit 1; }
mon j13-a-off 14; RUN_ENV="DWARF_TESTING_ARCHIVE_GRACE=24h" WIN_MIN=14 one_run j13-a-off $SAT
mon j13-large 14
vmssh "$ADMIN" "setsid nohup ./pgss-chunk.sh $(( 30 + (2 + 14) * 60 - 60 )) out/pgss-chunk-j13-large.txt '$(dsn "${IPS[0]}" dwarf_j13_large)' > /dev/null 2>&1 < /dev/null &"
WIN_MIN=14 one_run j13-large $SAT -mix "0.999997:linear:0,0.000003:fanout:0:270000"
