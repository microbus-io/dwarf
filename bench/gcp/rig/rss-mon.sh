#!/usr/bin/env bash
# Usage: ./rss-mon.sh <seconds> <out>   Every second: epoch, then pid:rss_mb:label for every dwarf-bench process on
# this host - the engine's memory against what it is doing, at a resolution a heap profile taken at the end lacks.
end=$(( $(date +%s) + $1 )); out=$2
while (( $(date +%s) < end )); do
  echo "$(date +%s) $(ps -C dwarf-bench -o pid=,rss=,args= | awk '{ l = "-"; for (i = 3; i <= NF; i++) if ($i == "-label") l = $(i + 1)
    printf "%s:%d:%s ", $1, $2 / 1024, l }')" >> "$out"
  sleep 1
done
