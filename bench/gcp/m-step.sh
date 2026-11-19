# M (x-rig step, BIN=bench-9e5). Mixed task durations on one crew: linear 10 steps at a fixed 1,000 flows/s
# (concurrency 256, default max-outstanding), 2 + 6 min, derived pool, fresh database per arm.
#   m-0:      no task delay - the same-rig control, and the cd26115 check: createP99Ms near the old
#             build's ~6.5-9.6 ms, and dwarf_flows HOT ~90% (n_tup_hot_upd / n_tup_upd in out/mon-m-0.log.tables).
#   m-mix1:   0.80:5ms,0.15:200ms,0.05:8s   predicted flow p50 ~680 ms, p99 ~24 s
#   m-real:   0.85:2ms,0.12:100ms,0.03:5s   (real-rig.sh's) predicted p50 ~260 ms, p99 ~10 s
# Falsify: throughput or Create p99 well off the weighted blend of single-duration arms (one -task-delay each)
# means the fast class starves behind the slow one. dwarf_workers_resident must diverge from connections in use.
RATE="-workload linear -payload 0 -open-loop -arrival-rate 1000 -concurrency 256"
WIN_MIN=6 one_run m-0 $RATE
WIN_MIN=6 one_run m-mix1 $RATE -task-delay-dist 0.80:5ms,0.15:200ms,0.05:8s
WIN_MIN=6 one_run m-real $RATE -task-delay-dist 0.85:2ms,0.12:100ms,0.03:5s
