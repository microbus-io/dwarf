#!/usr/bin/env bash
# Usage: ./g-run.sh <tag> <start-epoch> <dsn> <bench args...>  (pinned binary; warmup $WARMUP default 2m, window $WINDOW default 16m)
set -u
tag=$1 start=$2 dsn=$3; shift 3
mkdir -p out "out/pprof-$tag"
now=$(date +%s); (( start > now )) && sleep $(( start - now ))
./dwarf-bench -dsn "1=$dsn" -warmup "${WARMUP:-2m}" -window "${WINDOW:-16m}" -stats-interval 10s -pprof "out/pprof-$tag" "$@" -label "$tag" -out "out/$tag.json" > "out/$tag.log" 2>&1
echo "exit $?" > "out/$tag.done"
