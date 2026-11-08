#!/usr/bin/env bash
# Usage: ./s-mon.sh <seconds> <out> <dsn>...  Every 5s per shard: epoch shard commits wal_bytes state/wait:count...
# fpi=<full-page images, cumulative> nfp=<locks held outside the backends' fast-path slots, the lock-manager spill>
# A second file, <out>.xmin, gets every 5s per shard: epoch shard checkpoints buffers_checkpoint, then the backend
# holding the oldest snapshot: pid state xmin_age_xids xact_age_s query_age_s query-prefix. A minutes-long
# statement or transaction pins vacuum's horizon; this is what names it.
# A third file, <out>.tables, gets every 5s per shard, for dwarf_flows then dwarf_steps: n_tup_upd n_tup_hot_upd
# n_tup_newpage_upd n_live_tup n_dead_tup autovacuum_count autoanalyze_count relation_bytes (cumulative; diff them).
end=$(( $(date +%s) + $1 )); out=$2; shift 2
q="SELECT (SELECT xact_commit FROM pg_stat_database WHERE datname=current_database()), (SELECT wal_bytes FROM pg_stat_wal), (SELECT string_agg(k||':'||n, ' ' ORDER BY k) FROM (SELECT COALESCE(state,'none')||'/'||COALESCE(wait_event_type,'-')||'/'||COALESCE(wait_event,'-') k, COUNT(*) n FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() GROUP BY 1) x), 'fpi='||(SELECT wal_fpi FROM pg_stat_wal), 'nfp='||(SELECT count(*) FROM pg_locks WHERE NOT fastpath)"
qx="SELECT (SELECT checkpoints_timed+checkpoints_req FROM pg_stat_bgwriter), (SELECT buffers_checkpoint FROM pg_stat_bgwriter), COALESCE((SELECT pid||' '||replace(COALESCE(state,'none'),' ','_')||' '||age(backend_xmin)||' '||COALESCE(EXTRACT(EPOCH FROM now()-xact_start)::int,-1)||' '||COALESCE(EXTRACT(EPOCH FROM now()-query_start)::int,-1)||' '||left(regexp_replace(query,'\s+',' ','g'),100) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_xmin IS NOT NULL ORDER BY age(backend_xmin) DESC LIMIT 1),'-')"
qt="SELECT string_agg(concat_ws(' ', relname, n_tup_upd, n_tup_hot_upd, n_tup_newpage_upd, n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count, pg_relation_size(relid)), ' ' ORDER BY relname) FROM pg_stat_user_tables WHERE relname IN ('dwarf_flows','dwarf_steps')"
while (( $(date +%s) < end )); do
  i=0; for d in "$@"; do i=$((i+1))
    ( r=$(psql "$d" -X -q -At -F' ' -c "$q" 2>&1 | tr '\n' ' '); echo "$(date +%s.%N | cut -c1-14) $i $r" >> "$out" ) &
    ( r=$(psql "$d" -X -q -At -F' ' -c "$qx" 2>&1 | tr '\n' ' '); echo "$(date +%s.%N | cut -c1-14) $i $r" >> "$out.xmin" ) &
    ( r=$(psql "$d" -X -q -At -F' ' -c "$qt" 2>&1 | tr '\n' ' '); echo "$(date +%s.%N | cut -c1-14) $i $r" >> "$out.tables" ) &
  done
  wait; sleep 5
done
