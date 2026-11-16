#!/usr/bin/env bash
# Usage: ./inject.sh <delay-s> <dsn> <tag> <width>...   After <delay-s>, for each width in turn, a minute apart: an
# await-only bench process (it dispatches nothing) creates one `fanout` flow - a forEach of <width> branches - and
# awaits it to its end, so the spawn and the fan-in run on the engines already dispatching the shard, beside their
# load. out/<tag>-inject.log has each flow's start and end (epoch), the end-to-end time a 1 s window cannot hold.
sleep "$1"; dsn=$2 tag=$3; shift 3
for w in "$@"; do
  echo "$(date +%s) start $w" >> "out/$tag-inject.log"
  ./dwarf-bench -dsn "1=$dsn" -workload fanout -fanout-width "$w" -concurrency 1 -replica-workers 0 -warmup 0s -window 1s \
    -stall-timeout 0 -label "$tag-$w" -out "out/$tag-$w.json" > "out/$tag-$w.log" 2>&1
  echo "$(date +%s) end $w exit $?" >> "out/$tag-inject.log"
  sleep 60
done
echo "$(date +%s) done" >> "out/$tag-inject.log"
