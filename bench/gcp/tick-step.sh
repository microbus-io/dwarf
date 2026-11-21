# TICK (x-rig step, on a warmed instance: `BIN=bench-<sha> RBIN=bench-<sha>-r5 x-rig.sh <name> 1000 warm-step.sh
# tick-step.sh`, RBIN the same commit built with the recovery passes on a 5-minute tick - `recoveryInterval` in
# engine/engine.go - instead of the shipped minute). Two questions at 95% of a saturated 0 B shard, on the archive build
# at the rigs' 64 GB max_wal_size:
# - Do the once-a-minute recovery passes still dip dispatch at high load? Before the archive, every backlog spike at 95%
#   lined up with a lease-scan pass or a dwarf_steps autovacuum ending (2026-10-19), most of the p99.
# - Does a 5-minute tick's lease scan cost no more a pass than the minute's? Its cost is the dead index entries since
#   the last pass; before the archive, vacuum fell behind, they piled up, and 5-minute passes grew 2.4 -> 4.3 -> 6.6 s.
#   If tuned vacuum and the archive now remove them promptly, a slow pass costs about what a 1-minute one does.
#   tk-sat    0 B saturated, cap 2,000, 2 min + 6 min: this draw's rate, which the arms below are set from.
#   tk-1m-a   open loop at 95% of it, the shipped 1-minute tick, 2 min + 14 min.
#   tk-5m     the same with RBIN's 5-minute tick: ~3 lease-scan passes.
#   tk-1m-b   the shipped tick again; the drift between tk-1m-a and tk-1m-b bounds what tk-5m can be read for.
# Read per arm (tickread): steps/s, p50 and p99, the most flows outstanding, the slowest 10 s sample and how many fell
# under 90% of the window's mean (the dips), and the lease scan's passes from pgss-<tag>.txt: calls, mean and max ms,
# buffers a pass. One draw, n=1 per arm. ~60 min of arms.
gc compute scp --zone=$ZONE "$S/${RBIN:?set RBIN to the 5-minute-tick build in $RIG_HOME}" "$ADMIN":~/dwarf-bench-r5.bin >/dev/null 2>&1 &&
  vmssh "$ADMIN" 'chmod +x dwarf-bench-r5.bin' || { log "RBIN DEPLOY FAILED"; exit 1; }
# tickread <tag>: the arm's dispatch and its lease-scan passes.
tickread() {
  log "  $1: $(vmssh "$ADMIN" "jq -r '.results[0] | [.gauges.series[]?.stepsPerSec] as \$s | (\$s | add / length) as \$m | \"\(.stepsPerSec | round) steps/s, p50 \(.p50Ms | round) ms, p99 \(.p99Ms | round) ms, most outstanding \(.maxOutstandingObserved), slowest 10 s \(\$s | min | round) steps/s, \([\$s[] | select(. < 0.9 * \$m)] | length) of \(\$s | length) samples under 90% of the mean\"' out/$1.json")"
  log "  $1 lease scan: $(grep -m1 'FROM dwarf_steps WHERE status=.* lease_expires<=' "$OUT/pgss-$1.txt" | awk '{ print $1 " passes, " $2 " ms mean, " $3 " ms max, " $4 " buffers a pass" }')"
}
bin() { vmssh "$ADMIN" "ln -sf $1 dwarf-bench && readlink dwarf-bench" >/dev/null || { log "BINARY SWITCH FAILED"; exit 1; }; }
Z0="-workload linear -payload 0"
WIN_MIN=6 one_run tk-sat $Z0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
tickread tk-sat
sat=$(vmssh "$ADMIN" "jq -r '.results[0].stepsPerSec // 0' out/tk-sat.json" | tr -d '[:space:]')
r=$(awk -v s="${sat:-0}" 'BEGIN { if (s < 5000) s = 20000; printf "%d", s * 0.95 / 10 }')
log "the tick arms: $r flows/s, 0.95 of tk-sat's ${sat:-?} steps/s"
for arm in tk-1m-a:dwarf-bench.bin tk-5m:dwarf-bench-r5.bin tk-1m-b:dwarf-bench.bin; do
  tag=${arm%%:*}
  bin "${arm#*:}"
  WIN_MIN=14 one_run "$tag" $Z0 -open-loop -arrival-rate "$r" -concurrency 256 -max-outstanding 20000
  tickread "$tag"
done
bin dwarf-bench.bin
