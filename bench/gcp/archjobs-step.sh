# ARCHJOBS (x-rig step, on the rig archrate-step.sh ran: `BIN=bench-<sha> JOBSBIN=bench-<sha>-j4 REUSE=1 x-rig.sh <name>
# 1000 archjobs-step.sh`, JOBSBIN the same commit built with four archiving and deletion jobs of each kind a shard
# instead of two). Whether more jobs keep the archive up at 64 KB, and what they cost dispatch: archrate-step.sh's
# ar-sat and ar-r85 again on the same instance, with JOBSBIN. Those two arms are the controls. One draw, n=1.
#   ar-sat-j4   saturated, cap 2,000, 2 min + 6 min, against ar-sat.
#   ar-r85-j4   open loop at ar-r85's rate (85% of ar-sat's), 2 min + 10 min, against ar-r85.
# Read per arm (archread): steps/s, the lag's mean and peak, flows archived against closed, the jobs' batch count and
# mean work time, dwarf_steps' live rows by minute. ~25 min of arms.
gc compute scp --zone=$ZONE "$S/${JOBSBIN:?set JOBSBIN to the four-job build in $RIG_HOME}" "$ADMIN":~/dwarf-bench-jobs.bin >/dev/null 2>&1 &&
  vmssh "$ADMIN" 'chmod +x dwarf-bench-jobs.bin && ln -sf dwarf-bench-jobs.bin dwarf-bench' || { log "JOBSBIN DEPLOY FAILED"; exit 1; }
P64="-workload linear -payload 65536 -payload-json"
WIN_MIN=6 one_run ar-sat-j4 $P64 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
archread ar-sat-j4
sat=$(vmssh "$ADMIN" "jq -r '.results[0].stepsPerSec // 0' out/ar-sat.json" | tr -d '[:space:]')
r=$(awk -v s="${sat:-0}" 'BEGIN { if (s < 500) s = 3000; printf "%d", s * 0.85 / 10 }')
log "ar-r85-j4: $r flows/s, 0.85 of ar-sat's ${sat:-?} steps/s"
WIN_MIN=10 one_run ar-r85-j4 $P64 -open-loop -arrival-rate "$r" -concurrency 256 -max-outstanding 20000
archread ar-r85-j4
