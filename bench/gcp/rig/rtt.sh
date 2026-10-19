#!/usr/bin/env bash
# Usage: ./rtt.sh <dsn>...  Per DSN, one line: the minimum of a dozen SELECT 1 round trips in ms, on ONE psql session
# with the first discarded. A fresh connection or process per sample measures the handshake or ~29 ms of startup,
# not the path. "?" when no sample came back.
for d in "$@"; do
  { echo '\timing on'; for i in $(seq 1 13); do echo 'SELECT 1;'; done; } | psql "$d" -X -At 2>&1 |
    awk '/^Time:/ { n++; if (n > 1 && (m == "" || $2 + 0 < m)) m = $2 + 0 } END { print (m == "" ? "?" : m) }'
done
