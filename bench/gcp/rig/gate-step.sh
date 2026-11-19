# GATE (x-rig step, BIN=bench-9e5 = 9e5d16b, DBFLAGS = auto_explain at 10 s + log_lock_waits). Verifies the due-index
# usage gate (556c0a4, _WORK.md #32) on BASE's base-k512 shape: saturated linear 0 B, 512 fairness keys, cap 2,000,
# 2 + 16 min, fresh database. Then M runs on the same rig (sourced at the end).
#   pass:  auto_explain (Cloud Logging) shows no refill fetch over 10 s, and out/mon-gate-k512.log.xmin never names
#          the fetch as the oldest snapshot holder for more than a few seconds.
#   read:  the band scan's mean/max in pgss-gate-k512.txt against bench/results/20261017-redo-base/pgss-base-k512.txt
#          (the gate costs the dense band scan ~12% locally); the lease-recovery SELECT ~0.8 s and flat across the
#          window (growth = a held horizon); out/pprof-gate-k512 heap alloc_space against H3's for the no-op tracer
#          swap (9543ad5: otel global newSpan gone, ~6% fewer bytes per step).
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
one_run gate-k512 $SAT -fairness-keys 512
source "$D/m-step.sh"
