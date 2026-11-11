# LADDER (x-rig step: `BIN=<bench pin> LADDER=<reaper test pin> TIER=<tier> VMTYPE=<vm> x-rig.sh <name> 1000
# ladder-step.sh`). How long the reaper's transactions take against what they move, to set its transaction budget:
# internal/reaper's TestMeasure_Ladder - narrow and wide trees, 200 B to 1 MB of incompressible state a step, 64 to
# 262,144 rows, at most 256 MB a tree - each moved atomically, then staged (copied, published, its live copy deleted)
# at the engine's budget, with a probe committing a one-row UPDATE every 5 ms beside it. It runs while a saturated
# bench drives another database of the same instance, so the probe's opening reading is the baseline under load.
#   read: out/ladder.csv (one line a measurement: shape, payload, rows, bytes, op, transactions, slowest and total
#   microseconds, the probe's p50/p99/max microseconds, the window's epoch milliseconds) beside the bench's 10 s
#   series over the same windows. The budget goes where a transaction's duration starts to move dispatch p99.
#   ~5-6 min of ladder; the bench's window covers it with margin.
LADDER=${LADDER:?set LADDER to a pinned reaper test binary in $RIG_HOME}
gc compute scp --zone=$ZONE "$S/$LADDER" "$ADMIN":~/reaper-ladder.test >/dev/null 2>&1 && vmssh "$ADMIN" 'chmod +x reaper-ladder.test' ||
  { log "LADDER DEPLOY FAILED"; exit 1; }
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
tag=ladder-bench db=dwarf_ladder_bench win=14
authok; late
fresh "${IPS[0]}" "$db"
start=$(( $(date +%s) + 30 ))
log "run $tag: $SAT (start $(date -r $start '+%H:%M:%S'), window ${win}m)"
vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 30 + (win + 3) * 60 )) out/mon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
vmssh "$ADMIN" "WARMUP=1m WINDOW=${win}m setsid nohup ./g-run.sh $tag $start '$(dsn "${IPS[0]}" "$db")' $SAT > /dev/null 2>&1 < /dev/null &"
# The ladder starts once the bench is past its warmup and into its window.
while (( $(date +%s) < start + 90 )); do sleep 10; done
log "ladder: start"
vmssh "$ADMIN" "SEQUEL_TESTING_DSN='postgres://postgres:${pass}@${IPS[0]}:5432/dwarftest_%d?sslmode=disable' DWARF_MEASURE_LADDER=1 DWARF_LADDER_OUT=out/ladder.csv setsid nohup sh -c './reaper-ladder.test -test.run TestMeasure_Ladder -test.v -test.timeout 40m > out/ladder.log 2>&1; echo exit \$? > out/ladder.done' > /dev/null 2>&1 < /dev/null &"
until vmssh "$ADMIN" "test -f out/ladder.done"; do authok; late; sleep 30; done
log "ladder done: $(vmssh "$ADMIN" "cat out/ladder.done; wc -l < out/ladder.csv; grep -cE 'FAIL|panic' out/ladder.log" | tr '\n' ' ')"
until vmssh "$ADMIN" "test -f out/$tag.done"; do late; sleep 20; done
log "run $tag done: $(vmssh "$ADMIN" "grep -E '^[0-9]+ +[0-9]+ ' out/$tag.log | awk '{s+=\$2; n++} END {if (n) printf \"%.0f\", s/n}'") steps/s (window mean of 10 s samples)"
adm "$(dsn "${IPS[0]}" "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
drop "${IPS[0]}" "$db"
