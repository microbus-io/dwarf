#!/usr/bin/env bash
# Usage: ./ck-check.sh <tag> <dsn> <messages>   After a clock arm has drained, before its database is dropped: what
# the database holds and what the engine counted, into out/check-<tag>.txt (and stdout).
# - flows and steps by status in the live tables: every flow completed, none running, failed or terminated;
# - steps whose lease generation passed 1: claimed more than once, so a lease was recovered and re-granted - the
#   evidence the fence had something to fence;
# - per flows table (live, and each archive day): completed flows whose `loop` history is not <messages> long - a
#   zombie's write that landed beside the re-execution would leave a repeated message;
# - the engine's invariant checks (TestRig_Invariants, out of the bench's test binary ./riginv);
# - the artifact: validity, errors, the drain's account, and the counters the run is judged on
#   (dwarf_steps_unwedged and dwarf_steps_write_failed must be absent or 0; a counter that never moved is absent).
tag=$1 dsn=$2 n=$3
q() { psql "$dsn" -X -At -F' ' "$@" 2>&1; }
{
  echo "== flows by status (live)"; q -c "SELECT status, count(*) FROM dwarf_flows GROUP BY 1 ORDER BY 1"
  echo "== steps by status (live)"; q -c "SELECT status, count(*) FROM dwarf_steps GROUP BY 1 ORDER BY 1"
  echo "== steps claimed more than once (count, max lease_seq, all steps)"
  q -c "SELECT count(*) FILTER (WHERE lease_seq > 1), max(lease_seq), count(*) FROM dwarf_steps"
  echo "== per flows table: completed, not completed, completed with a history not $n long"
  q <<SQL
SELECT format('SELECT %L, count(*) FILTER (WHERE status = %L), count(*) FILTER (WHERE status <> %L), count(*) FILTER (WHERE status = %L AND COALESCE(jsonb_array_length(final_state::jsonb -> %L), -1) <> %s) FROM %I',
  tablename, 'completed', 'completed', 'completed', 'messages', $n, tablename)
FROM pg_tables WHERE schemaname = 'public' AND (tablename = 'dwarf_flows' OR tablename LIKE 'dwarf_flows_archive_%') ORDER BY tablename \gexec
SQL
  echo "== invariants"
  DWARF_TESTING_RIG_DSN="$dsn" ./riginv -test.run 'TestRig_Invariants$' -test.v 2>&1 | tail -40
  echo "== artifact"
  jq -r '"valid=\(.valid) invalidity=\(.invalidity // "-")",
    (.results[] | "errors=\(.errors) steps/s=\(.stepsPerSec | floor) p50=\(.p50Ms | floor)ms p99=\(.p99Ms | floor)ms",
      "drain \(.drain | tostring)",
      (.engineCounters | to_entries[]
        | select(.key | test("^dwarf_steps_(recovered|unwedged|write_failed|write_retried|executed|claim_lost|claim_preempted)|^dwarf_flows_(closed|started)|^dwarf_peer_changes"))
        | "\(.key) \(.value)"))' "out/$tag.json" 2>&1
  echo "== engine log lines at warn and above"; grep -cE 'level=(WARN|ERROR)' "out/$tag.log"
  grep -E 'level=(WARN|ERROR)' "out/$tag.log" | sed -E 's/time=[^ ]+ //' | cut -c1-200 | sort | uniq -c | sort -rn | head -15
} > "out/check-$tag.txt" 2>&1
cat "out/check-$tag.txt"
