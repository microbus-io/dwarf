# HA (x-rig step): the archived headline and the WAL settings in one session, linear, saturated (cap 2,000), the
# shipped archive grace, 2 min warmup, a fresh database per arm. Launch with the first arm's flags:
#   BIN=bench-<sha> DBFLAGS=autovacuum_work_mem=1048576,checkpoint_timeout=900,log_checkpoints=on,max_wal_size=20480,wal_compression=lz4 \
#     x-rig.sh ha 1000 ha-step.sh
#   ha-1:  0 B, max_wal_size 20 GB + wal_compression lz4, 16 min: the stall check. Minutes 9 on against 1-8 (mean no
#          more than 2% under, no minute more than 5% under; a run that speeds up holds) decide the later windows: 6 min
#          if it holds, else 16. The 10 s series of a 16 min window fills 15 whole minutes.
#   ha-2:  8 KB -payload-json, 20 GB + lz4
#   ha-3:  8 KB -payload-json, 20 GB, wal_compression off   (one setting changed from ha-2)
#   ha-4:  8 KB -payload-json, Cloud SQL's default max_wal_size + lz4   (the other)
# checkpoint_timeout is 900 s in every arm, so checkpoints are started by WAL volume, not by the clock.
# s-mon.sh records checkpoints and table sizes every 5 s beside each run; each arm's live flags are logged.
HA_BASE=autovacuum_work_mem=1048576,checkpoint_timeout=900,log_checkpoints=on
HA_BIG=max_wal_size=20480

ha_flags() {
  authok; log "patching $DB's flags to $1"
  gc sql instances patch "$DB" --database-flags="$1" --quiet > "$OUT/patch-$2.log" 2>&1 ||
    { log "FLAG PATCH FAILED (patch-$2.log) - stopping"; exit 1; }
  local i; for i in $(seq 1 45); do [[ "$(gc sql instances describe "$DB" --format='value(state)' 2>/dev/null)" == RUNNABLE ]] && break; sleep 20; done
  sleep 30
}
ha_show() {
  log "  $1 settings: $(adm "$(admdsn "${IPS[0]}")" "SELECT string_agg(name||'='||setting||COALESCE(unit,''), ' ' ORDER BY name) FROM pg_settings WHERE name IN ('max_wal_size','checkpoint_timeout','wal_compression','full_page_writes','log_checkpoints')")"
}

SAT="-workload linear -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
JSON="-payload 8192 -payload-json"

ha_show ha-1
WIN_MIN=16 one_run ha-1 $SAT -payload 0
# The draw: database core-ms per step on the headline arm (bench/CLAUDE.md). Logged, not gated - the WAL arms compare
# within this instance either way; a slow draw only means the headline is not quoted from it.
sleep 150 # Cloud Monitoring lands CPU a few minutes late
gc compute scp --zone=$ZONE "$ADMIN":~/out/ha-1.json "$OUT/" >/dev/null 2>&1 &&
  log "  draw: $(DB_INSTANCE="$DB" VCPUS=32 WRITE=1 "$R/bench/gcp/dbcpu.sh" "$OUT/ha-1.json" 2>&1 | tail -1)"
verdict=$(vmssh "$ADMIN" "python3 -c \"
import json
r = json.load(open('out/ha-1.json'))['results'][0]
ser = [p['stepsPerSec'] for p in (r.get('gauges') or {}).get('series') or []]
m = [sum(ser[i*6:(i+1)*6]) / 6 for i in range(len(ser) // 6)]
a, b = sum(m[:8]) / 8, sum(m[8:]) / max(1, len(m[8:]))
ok = len(m) >= 15 and b >= 0.98 * a and min(m[8:]) >= 0.95 * a
print('HOLDS' if ok else 'FAILS', 'first8', round(a), 'rest', round(b), 'min-rest', round(min(m[8:] or [0])), 'minutes', ' '.join(str(round(x)) for x in m))
\"" | tail -1)
log "  stall check: $verdict"
WIN=16; [[ "$verdict" == HOLDS* ]] && WIN=6
log "  later arms: ${WIN} min windows"

ha_show ha-2
WIN_MIN=$WIN one_run ha-2 $SAT $JSON

ha_flags "$HA_BASE,$HA_BIG,wal_compression=off" ha-3
ha_show ha-3
WIN_MIN=$WIN one_run ha-3 $SAT $JSON

ha_flags "$HA_BASE,wal_compression=lz4" ha-4
ha_show ha-4
WIN_MIN=$WIN one_run ha-4 $SAT $JSON
