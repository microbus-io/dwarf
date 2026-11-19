# Crew shrink (x-rig step): does the crew come back down to required_concurrency / threshold when tasks stop being
# long, and does throughput recover? Runs burst.sh (through driver-step.sh) on the admin host against the rig's
# instance: three arms (steady0, steadyN, burst), linear open loop at $RATE flows/s, one rep, each on a fresh database.
#   x-rig.sh crew 500 burst-step.sh   with TIER=db-custom-16-65536 (the rate needs a fraction of it) and BIN set.
# The defaults fit about 80 minutes of arms: a 24 min window of two 3 min burst + 9 min quiet cycles. The quiet half
# outlasts four 2-minute retirement rounds, so the crew should lose about two thirds of its surplus in each.
# Pass: in the burst arm the crew falls within every quiet stretch toward steady0's, and a quiet stretch's
# steps/s returns to steady0's. A crew flat at steadyN's plateau through the quiet half is a memory bug.
DRIVER=burst.sh DRIVER_ENV="RATE=${RATE:-450} DELAY=${DELAY:-8s} BURST=${BURST:-3m} QUIET=${QUIET:-9m} WINDOW=${WINDOW:-24m} STATS=${STATS:-15s} ${DRIVER_ENV:-}"
source "$D/driver-step.sh"
vmssh "$ADMIN" "sed -n '/== summary ==/,\$p' out/burst.log" | tee "$OUT/burst-summary.txt"
