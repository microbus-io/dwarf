# V rows (x-rig step: `BIN=<pin> DBFLAGS=<as gm> x-rig.sh vrows 1000 vrows-step.sh`). Does a saturated database
# hold its rate as dwarf_steps grows past 200M rows with nothing deleted? One saturated linear 0 B window of 200
# minutes on a fresh database (~20k steps/s, so ~240M step rows), its 10 s series read minute by minute, with
# v-mon.sh's per-minute database size, dead tuples, autovacuum count and cache hit ratio beside it.
#   read: steps/s per minute against the step rows v-mon counts. July (8 vCPU, to 100M rows): a one-time ~15-20%
#   settling by ~10M rows, then flat. A slope that continues past the settling is the finding; so is a cache hit
#   ratio falling under ~99%, or dead tuples climbing without bound.
#   1 TB disk: at ~1.5 KB per step row with its indexes, 240M rows is ~360 GB.
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
# Started before one_run creates the database, so its first sample or two fail harmlessly; it samples by minute
# for the window, the drain and the slack.
vmssh "$ADMIN" "setsid nohup ./v-mon.sh $(( (200 + 15) * 60 )) out/vmon-v-rows.log '$(dsn "${IPS[0]}" dwarf_v_rows)' > /dev/null 2>&1 < /dev/null &"
WIN_MIN=200 one_run v-rows $SAT
