# DRIVER (x-rig step: `BIN=bench-<sha> DRIVER=<driver> [DRIVER_ENV='K=V ...'] [NETEM_MS=<ms>] x-rig.sh <name> 1000
# driver-step.sh`). Runs one of the kit's campaign drivers - cell.sh, rttderived.sh, replicaladder.sh, taskdelay.sh,
# burst.sh - on the admin host, which is also the engine host, against the rig's instance. The driver gets DSN and
# DSN1 (the instance's postgres database: each driver creates and drops a database per arm), DB_HOST, VCPUS from $TIER,
# BENCH=./dwarf-bench and OUT=out/<driver>, then DRIVER_ENV, which sets its own knobs and overrides any of those.
# NETEM_MS runs it under withnetem.sh at that delay, for a driver that installs no netem of its own (replicaladder.sh,
# taskdelay.sh). Each driver's header says what it measures, its knobs, and how to read it.
drv=${DRIVER:?set DRIVER to a driver in bench/gcp (cell.sh, rttderived.sh, replicaladder.sh, taskdelay.sh, burst.sh)}
[ -f "$D/$drv" ] || { log "no driver $D/$drv"; exit 1; }
gc compute scp --zone=$ZONE "$D/$drv" "$D/withnetem.sh" "$ADMIN":~/ >/dev/null 2>&1 &&
  vmssh "$ADMIN" "chmod +x $drv withnetem.sh" || { log "$drv deploy failed"; exit 1; }
tier=${TIER:-db-custom-32-131072}; vcpus=${tier#db-custom-}; vcpus=${vcpus%%-*}
cmd="./$drv"; [ -n "${NETEM_MS:-}" ] && cmd="./withnetem.sh $NETEM_MS -- ./$drv"
tag=${drv%.sh}
DENV="DSN='$(admdsn "${IPS[0]}")' DSN1='$(admdsn "${IPS[0]}")' DB_HOST=${IPS[0]} VCPUS=$vcpus BENCH=./dwarf-bench OUT=out/$tag ${DRIVER_ENV:-}"
log "$drv: ${DENV//$pass/PASS}${NETEM_MS:+ (under netem at $NETEM_MS ms)}"
# out/$tag.pgid names the driver's process group while it runs, which x-rig.sh's REUSE stops.
vmssh "$ADMIN" "$DENV setsid nohup bash -c 'echo \$\$ > out/$tag.pgid; $cmd > out/$tag.log 2>&1; echo exit \$? > out/$tag.done; rm -f out/$tag.pgid' > /dev/null 2>&1 < /dev/null &"
until vmssh "$ADMIN" "test -f out/$tag.done"; do
  late; sleep 120
  log "  $(vmssh "$ADMIN" "grep -E '^-- ' out/$tag.log | tail -1; tail -1 out/$tag.log" | tr '\n' ' ' | cut -c1-240)"
done
log "$drv: $(vmssh "$ADMIN" "cat out/$tag.done")"
