# Sourced by x-rig.sh before every step. one_run <tag> <bench args...>: fresh database on shard 1, the run on the
# admin host (c4a-16) with the pinned binary, the backend monitor beside it, then the database is dropped. WIN_MIN
# sets the window in minutes (default 16); RUN_ENV prefixes the run with environment settings (e.g. GOMEMLIMIT=48GiB).
gc compute scp --zone=$ZONE "$D/g-run.sh" "$D/fsync.sh" "$D/archread.jq" "$ADMIN":~/ >/dev/null 2>&1 &&
  vmssh "$ADMIN" 'chmod +x g-run.sh fsync.sh; command -v jq >/dev/null || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq jq >/dev/null 2>&1'
# archread <tag>: whether the archive kept up over the arm (archread.jq), and dwarf_steps' live rows (n_live_tup, an
# estimate) once a minute from the arm's first reading of the table - level is keeping up, rising is falling behind.
archread() {
  log "  $1 archive: $(vmssh "$ADMIN" "jq -r -f archread.jq out/$1.json")"
  log "  $1 live dwarf_steps rows by minute: $(vmssh "$ADMIN" "awk 'BEGIN { m0 = -1 } \$12 == \"dwarf_steps\" { if (t0 == \"\") t0 = \$1; m = int((\$1 - t0) / 60); if (m != m0) { printf \"%d \", \$16; m0 = m } }' out/mon-$1.log.tables")"
}
# ckread <tag>: WAL and page images a step over the window (results[].db), and the seconds between checkpoint starts
# over the whole arm (the .xmin monitor's checkpoint count, sampled every 5 s).
ckread() {
  log "  $1 WAL: $(vmssh "$ADMIN" "jq -r '.results[0] | (.stepsPerSec * .windowSec) as \$n | \"\(.db.walBytes / 1024 / \$n | floor) KB a step, \(.db.walFpi / \$n * 10 | round / 10) page images a step, flush \(.db.flushMs * 100 | round / 100) ms\"' out/$1.json")"
  log "  $1 seconds between checkpoints: $(vmssh "$ADMIN" "awk '\$3 ~ /^[0-9]+\$/ { if (p != \"\" && \$3 > p) { if (t != \"\") printf \"%d \", \$1 - t; t = \$1 } p = \$3 }' out/mon-$1.log.xmin")"
}
one_run() {
  local tag=$1; shift; local db="dwarf_${tag//-/_}" start x
  authok; late
  # The idle WAL flush floor before every arm: a flag patch or a disk change can move it (bench/CLAUDE.md).
  log "  flush floor before $tag: $(vmssh "$ADMIN" "./fsync.sh '$(admdsn "${IPS[0]}")'" | tail -1 | sed "s/^/$tag /" | tee -a "$OUT/fsync.txt" | cut -d' ' -f2-)"
  fresh "${IPS[0]}" "$db"
  start=$(( $(date +%s) + 30 ))
  log "run $tag: $* (start $(date -r $start '+%H:%M:%S'))"
  local win=${WIN_MIN:-16}
  vmssh "$ADMIN" "setsid nohup ./s-mon.sh $(( 30 + (win + 3) * 60 )) out/mon-$tag.log '$(dsn "${IPS[0]}" "$db")' > /dev/null 2>&1 < /dev/null &"
  vmssh "$ADMIN" "${RUN_ENV:-} WINDOW=${win}m setsid nohup ./g-run.sh $tag $start '$(dsn "${IPS[0]}" "$db")' $* > /dev/null 2>&1 < /dev/null &"
  # Stall watchdog, inside the window only (the bench goes quiet after it while it writes the artifact): no new
  # 10 s sample for ~4 min means the engine is hung (one out of memory sat 34 min first), so kill it and move on.
  local last=-1 same=0 n
  while (( $(date +%s) < start + (win + 2) * 60 )); do
    late
    if (( $(date +%s) > start + 180 )); then
      n=$(vmssh "$ADMIN" "grep -cE '^[0-9]+ +[0-9]+ ' out/$tag.log 2>/dev/null" || echo "?")
      if [[ "$n" == "$last" ]]; then same=$((same + 1)); else same=0; last=$n; fi
      if (( same >= 4 )); then
        log "run $tag STALLED: no new sample for ~4 min at $n samples - killing dwarf-bench"
        vmssh "$ADMIN" 'pkill -9 -f dwarf-bench; sleep 2; pgrep -f dwarf-bench || echo killed'
        break
      fi
    fi
    sleep 60
  done
  until vmssh "$ADMIN" "test -f out/$tag.done"; do late; sleep 20; done
  x=$(vmssh "$ADMIN" "grep -E '^[0-9]+ +[0-9]+ ' out/$tag.log | awk '{s+=\$2; n++} END {if (n) printf \"%.0f\", s/n}'; grep -iE 'invalid|error|panic' out/$tag.log | grep -v -E '^conc +flows' | head -2")
  log "run $tag done: ${x:-?} steps/s (window mean of 10 s samples)"
  # Per-statement totals for the run, max included (the bench's own sampler records means only): the lease-recovery
  # sweep's SELECT/UPDATE, the refill fetch, the flow-row writes. Columns: calls, mean ms, max ms, buffers a call (hit
  # plus read), the statement. Read before the database is dropped.
  adm "$(dsn "${IPS[0]}" "$db")" "SELECT calls, round(mean_exec_time::numeric,3), round(max_exec_time::numeric,1), round((shared_blks_hit+shared_blks_read)::numeric/greatest(calls,1)), left(replace(query, chr(10), ' '),160) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY total_exec_time DESC LIMIT 40" > "$OUT/pgss-$tag.txt" 2>&1
  drop "${IPS[0]}" "$db"
  sleep 20
}
