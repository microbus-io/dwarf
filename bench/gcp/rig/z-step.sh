# Z (x-rig step, BIN=bench-9e5; NO DBFLAGS: auto_explain would log 250 MB bind parameters). State-size ladder: the
# `state` workload (5 steps, each rewriting one incompressible string field), closed loop, 30 s warmup + 3 min,
# fresh database per run, at concurrency 1 then 4 per rung. -fanout-width 1 keeps the bench's fresh buffer at
# 2 x payload (e3c19cb builds it on first use).
# Rungs are DECIMAL bytes: the engine refuses a payload over 256,000,000 bytes (a651027), and 250 MiB (262,144,000)
# was measured refused locally on 2026-10-18, while 250,000,000 completes.
# Read per run: steps/s and MB/s, heap peak (heapPeakMB) and host RSS, dmesg for an OOM kill, errors (must be 0),
# dwarf_steps_recovered (must be 0: a write outlasting the lease would re-run the task), and the drain block.
for P in 8000000 32000000 64000000 128000000 250000000; do
  for C in 1 4; do
    RUN_ENV="WARMUP=30s" WIN_MIN=3 one_run "z-$((P / 1000000))m-c$C" -workload state -payload $P -fanout-width 1 -concurrency $C
  done
done
vmssh "$ADMIN" 'sudo dmesg | grep -iE "out of memory|oom-kill" | tail -5' || true
