# WARM (x-rig step, first on a new instance: `BIN=bench-<sha> x-rig.sh <name> 1000 warm-step.sh <step>...`). A fresh
# instance's first checkpoints create its WAL files, and an arm reads low until checkpoints recycle them
# (bench/CLAUDE.md): past two checkpoints is ~68 GB of WAL at the rigs' 64 GB max_wal_size, ~14 min at 0 B. One
# saturated 64 KB -payload-json arm, 2 min + 4 min (~60 GB), not read; with the next arm's 2-minute warm-up the
# instance is past its second checkpoint.
WIN_MIN=4 one_run warm -workload linear -payload 65536 -payload-json -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
