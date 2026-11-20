# ARCHRATE (x-rig step: `BIN=bench-<sha> x-rig.sh <name> 1000 archrate-step.sh`). The rate at which the archive stops
# keeping up with a shard at 64 KB: the standard rig (one 32 vCPU instance at the recommended settings, one c4a-16), a
# fresh database per arm, `linear` with 64 KB `-payload-json`, the shipped one-minute grace. Saturated at this size the
# archive fell behind (2026-11-16: the lag peaked at ~300 s, live dwarf_steps rows grew through the window, under half
# the closed flows were archived), while at 8 KB and below it keeps up. One draw, n=1.
#   ar-warm   3 min saturated, not read: a fresh instance's first checkpoints create its WAL files (bench/CLAUDE.md).
#   ar-sat    saturated, cap 2,000, 2 min + 6 min: this draw's rate, which the arms below are set from, and whether the
#             archive still falls behind on this build.
#   ar-r67    open loop at two thirds of ar-sat's rate, 2 min + 10 min.
#   ar-r85 or ar-r40   open loop at 85% of ar-sat's rate if ar-r67 kept up, else at 40%, 2 min + 10 min: the bracket.
#             Kept up, fixed in advance: the archive lag (dwarf_archive_lag_seconds, the larger job's) peaks at 60 s or
#             less over the window - saturated, 8 KB peaked at 36 s and levelled, 64 KB at 302 s and climbing.
# Read per arm (archread, logged after it): steps/s, the lag's mean and peak, flows archived against closed, the
# archiving jobs' batch count and mean work time (dwarf_janitor_batch_seconds), and dwarf_steps' live rows once a
# minute; beside them out/mon-<tag>.log's WAL rate and waits and its .tables file's dead rows and autovacuum count. They
# tell the jobs' own pace (batches back to back for the whole window) from their share of a WAL-bound shard (batches
# slower as the shard nears saturation) and from the live table's vacuum.
# ~45 min of arms.
P64="-workload linear -payload 65536 -payload-json"
WIN_MIN=3 one_run ar-warm $P64 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
WIN_MIN=6 one_run ar-sat $P64 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
archread ar-sat
sat=$(vmssh "$ADMIN" "jq -r '.results[0].stepsPerSec // 0' out/ar-sat.json" | tr -d '[:space:]')
# at <tag> <fraction>: open loop at that fraction of ar-sat's rate (10 steps a flow), 2 min + 10 min. Without a
# reading it falls back to the 2026-11-16 saturated rate, 3,000 steps/s.
at() {
  local r; r=$(awk -v s="${sat:-0}" -v f="$2" 'BEGIN { if (s < 500) s = 3000; printf "%d", s * f / 10 }')
  log "$1: $r flows/s, $2 of ar-sat's ${sat:-?} steps/s"
  WIN_MIN=10 one_run "$1" $P64 -open-loop -arrival-rate "$r" -concurrency 256 -max-outstanding 20000
  archread "$1"
}
at ar-r67 0.667
peak=$(vmssh "$ADMIN" "jq -r '[.results[0].gauges.gaugesPeak // {} | to_entries[] | select(.key | startswith(\"dwarf_archive_lag_seconds\")) | .value] | max // 999' out/ar-r67.json" | tr -d '[:space:]')
if awk -v p="${peak:-999}" 'BEGIN { exit !(p <= 60) }'; then
  log "ar-r67 kept up (lag peak ${peak} s): the bracket is 85%"
  at ar-r85 0.85
else
  log "ar-r67 fell behind (lag peak ${peak:-?} s): the bracket is 40%"
  at ar-r40 0.40
fi
