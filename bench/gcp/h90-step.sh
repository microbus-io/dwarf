# H90 (x-rig step, BIN=bench-9e5, DBFLAGS auto_explain at 10 s + log_lock_waits): the headroom chart's third line.
# On ONE instance: a saturated calibration, then commanded at 95% and at 90% of it, 2 + 16 min each, both run
# whether or not 95% holds - the question is how much smoother 90% is (backlog spikes, p99), not where the
# walk-down stops. The once-a-minute dips h-step.sh's 95% rung shows are expected at both; read their size and how
# fast each clears.
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
one_run h-sat $SAT
CAL=$(vmssh "$ADMIN" "python3 -c \"import json; print(round(json.load(open('out/h-sat.json'))['results'][0]['stepsPerSec']))\"" | tail -1)
log "calibration: $CAL steps/s"
[[ "$CAL" =~ ^[0-9]+$ ]] && (( CAL > 1000 )) || { log "NO CALIBRATION - stopping before the rungs"; return 0; }
for PCT in 95 90; do
  STEPS=$(( CAL * PCT / 100 ))
  FLOWS=$(awk "BEGIN { printf \"%.1f\", $STEPS / 10 }")
  one_run "h-r$PCT" -workload linear -payload 0 -open-loop -arrival-rate "$FLOWS" -concurrency 256 -max-outstanding 20000
done
