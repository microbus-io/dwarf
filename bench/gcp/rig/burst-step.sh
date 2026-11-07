# Crew shrink (x-rig step): does the crew come back down to required_concurrency / threshold when tasks stop being
# long, and does throughput recover? Runs bench/gcp/burst.sh on the admin host against the rig's instance: three arms
# (steady0, steadyN, burst), linear open loop at $RATE flows/s, one rep, each on a fresh database.
#   x-rig.sh crew 500 burst-step.sh   with TIER=db-custom-16-65536 (the rate needs a fraction of it) and BIN set.
# The defaults fit about 80 minutes of arms: a 24 min window of two 3 min burst + 9 min quiet cycles. The quiet half
# outlasts four 2-minute retirement rounds, so the crew should lose about two thirds of its surplus in each.
# Pass: in the burst arm the crew falls within every quiet stretch toward steady0's, and a quiet stretch's
# steps/s returns to steady0's. A crew flat at steadyN's plateau through the quiet half is a memory bug.
gc compute scp --zone=$ZONE "$R/bench/gcp/burst.sh" "$ADMIN":~/ >/dev/null 2>&1 && vmssh "$ADMIN" 'chmod +x burst.sh' ||
  { log "burst.sh deploy failed"; exit 1; }
BENV="DSN='$(admdsn "${IPS[0]}")' OUT=out/burst RATE=${RATE:-450} VCPUS=${VCPUS:-16} DELAY=${DELAY:-8s} BURST=${BURST:-3m} QUIET=${QUIET:-9m} WINDOW=${WINDOW:-24m} STATS=${STATS:-15s}"
log "burst.sh: $BENV"
vmssh "$ADMIN" "$BENV setsid nohup bash -c './burst.sh > out/burst.log 2>&1; touch out/burst.done' > /dev/null 2>&1 < /dev/null &"
until vmssh "$ADMIN" 'test -f out/burst.done'; do
  late; sleep 120
  log "  $(vmssh "$ADMIN" "grep -E '^-- ' out/burst.log | tail -1; tail -1 out/burst.log" | tr '\n' ' ')"
done
vmssh "$ADMIN" "sed -n '/== summary ==/,\$p' out/burst.log" | tee "$OUT/burst-summary.txt"
