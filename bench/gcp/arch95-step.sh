# ARCH95 (x-rig step, after warm-step.sh: `BIN=bench-<sha> x-rig.sh <name> 1000 warm-step.sh arch95-step.sh`). Whether
# the archive keeps up with a 64 KB shard at 95% of its saturated rate, the average the sizing guidance plans at, at the
# rigs' 64 GB max_wal_size. At 64 GB it kept up at 85% (2026-11-20, ckpt-step.sh: lag peak 14 s, live rows flat); at
# 20 GB it fell behind at 85% (archrate-step.sh). `linear`, 64 KB -payload-json, the shipped two jobs and one-minute
# grace, a fresh database per arm. One draw, n=1.
#   a95-sat   saturated, cap 2,000, 2 min + 6 min: this draw's rate, which the arms below are set from.
#   a95-r95   open loop at 95% of it, 2 min + 6 min.
#   a95-r90   open loop at 90% of it, 2 min + 6 min, only if a95-r95 fell behind.
# Kept up, as in archrate-step.sh: the archive lag (dwarf_archive_lag_seconds, the larger job's) peaks at 60 s or less
# over the window. Read per arm: archread and ckread (a-step-lib.sh). ~30 min of arms.
P64="-workload linear -payload 65536 -payload-json"
WIN_MIN=6 one_run a95-sat $P64 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
archread a95-sat
ckread a95-sat
sat=$(vmssh "$ADMIN" "jq -r '.results[0].stepsPerSec // 0' out/a95-sat.json" | tr -d '[:space:]')
# at95 <tag> <fraction>: open loop at that fraction of a95-sat's rate (10 steps a flow), 2 min + 6 min. Without a
# reading it falls back to 2026-11-20's saturated rate at 64 GB, 3,029 steps/s.
at95() {
  local r; r=$(awk -v s="${sat:-0}" -v f="$2" 'BEGIN { if (s < 500) s = 3029; printf "%d", s * f / 10 }')
  log "$1: $r flows/s, $2 of a95-sat's ${sat:-?} steps/s"
  WIN_MIN=6 one_run "$1" $P64 -open-loop -arrival-rate "$r" -concurrency 256 -max-outstanding 20000
  archread "$1"
  ckread "$1"
}
at95 a95-r95 0.95
peak=$(vmssh "$ADMIN" "jq -r '[.results[0].gauges.gaugesPeak // {} | to_entries[] | select(.key | startswith(\"dwarf_archive_lag_seconds\")) | .value] | max // 999' out/a95-r95.json" | tr -d '[:space:]')
if awk -v p="${peak:-999}" 'BEGIN { exit !(p <= 60) }'; then
  log "a95-r95 kept up (lag peak ${peak} s)"
else
  log "a95-r95 fell behind (lag peak ${peak:-?} s): 90%"
  at95 a95-r90 0.90
fi
