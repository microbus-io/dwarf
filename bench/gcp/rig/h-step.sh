# H rungs (x-rig step, BIN=bench-9e5): the sustainable rate on one 32 vCPU database, walking DOWN from the saturated
# rate and stopping at the first rung that holds (user, 2026-10-18). linear 0 B, 2 + 16 min, fresh database per run.
#   h-sat:       saturated calibration on THIS instance (cap 2,000, H3's shape). Its 10 s window mean is "100%".
#   h-r100..85:  open loop at that fraction of it, cap 20,000 so a growing backlog shows instead of binding.
# A rung holds when delivered steps/s >= 99% of the command AND the backlog drains (cell.sh's rule: the outstanding
# floor of the last two minutes <= max(1000, 5x) that of the first two).
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
one_run h-sat $SAT
CAL=$(vmssh "$ADMIN" "python3 -c \"import json; print(round(json.load(open('out/h-sat.json'))['results'][0]['stepsPerSec']))\"" | tail -1)
log "calibration: $CAL steps/s"
[[ "$CAL" =~ ^[0-9]+$ ]] && (( CAL > 1000 )) || { log "NO CALIBRATION - stopping before the rungs"; return 0; }
holds() {
  vmssh "$ADMIN" "python3 -c \"
import json
r = json.load(open('out/$1.json'))['results'][0]
ser = (r.get('gauges') or {}).get('series') or []
out = [p.get('outstanding', 0) for p in ser]
n = len(out) // 6
fl = [min(out[i*6:(i+1)*6]) for i in range(n)]
first, last = max(fl[:2]), max(fl[-2:])
ok = r['stepsPerSec'] >= 0.99 * $2 and last <= max(1000, 5 * first)
print('HOLDS' if ok else 'FAILS', round(r['stepsPerSec']), 'of', $2, 'floor', first, '->', last, 'p50', round(r['p50Ms']), 'p99', round(r['p99Ms']))
\"" | tail -1
}
for PCT in 100 95 90 85; do
  STEPS=$(( CAL * PCT / 100 ))
  FLOWS=$(awk "BEGIN { printf \"%.1f\", $STEPS / 10 }")
  one_run "h-r$PCT" -workload linear -payload 0 -open-loop -arrival-rate "$FLOWS" -concurrency 256 -max-outstanding 20000
  v=$(holds "h-r$PCT" "$STEPS")
  log "rung $PCT% ($STEPS steps/s): $v"
  [[ "$v" == HOLDS* ]] && break
done
