# KEEPUP (x-rig step: `BIN=<bench pin> x-rig.sh <name> 1000 keepup-step.sh`). Whether archiving keeps up with a
# saturated 32 vCPU shard, and what it costs dispatch: two saturated arms on fresh databases, archiving on (the 1 min
# grace) and archiving off (DWARF_ARCHIVE_GRACE=24h, so nothing comes due within the run), after a warm-up arm that is
# not read (a fresh instance's first checkpoints read low, bench/CLAUDE.md).
#   read: out/keepup-on.json and out/keepup-off.json - steps/s, p99, dwarf_flows_closed against dwarf_flows_archived,
#   dwarf_archive_lag_seconds (gaugesMean, gaugesPeak, gaugesAfter), dwarf_janitor_batch_seconds by job and part
#   (engineHistograms), the pool's wait (sequel_pool_wait_duration_seconds, gaugesAfter less gaugesBefore); and
#   out/age-keepup-on.log (age-mon.sh): the oldest closed root not archived yet, every 10 s. Flat at about the grace
#   is keeping up; rising is falling behind.
#   ~50 min of arms.
gc compute scp --zone=$ZONE "$D/age-mon.sh" "$ADMIN":~/ >/dev/null 2>&1 && vmssh "$ADMIN" 'chmod +x age-mon.sh' ||
  { log "AGE-MON DEPLOY FAILED"; exit 1; }
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
WIN_MIN=4 one_run keepup-warm $SAT
# The sampler starts before the arm makes its database, and reads errors until the engine's migrations have run.
vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./age-mon.sh $(( (14 + 4) * 60 )) out/age-keepup-on.log \"$(dsn "${IPS[0]}" dwarf_keepup_on)\"' > /dev/null 2>&1 < /dev/null &"
WIN_MIN=14 one_run keepup-on $SAT
RUN_ENV="DWARF_ARCHIVE_GRACE=24h" WIN_MIN=14 one_run keepup-off $SAT
