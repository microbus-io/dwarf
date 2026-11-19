# T64 (x-rig step, TIER=db-custom-64-262144, BIN=bench-9e5): the 64 vCPU tier on one c4a-16 engine, saturated linear 0 B,
# cap 2,000, 2 + 16 min, fresh database per run - at the shipped default budget (250) and at MaxConnections 500, which
# the page says a 64 vCPU database wants (+14% on 2026-10-08). Compare against h-step.sh's h-sat (same build,
# different instance) for "past 32 vCPU, add shards".
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
one_run t64-250 $SAT
one_run t64-500 $SAT -max-connections 500
