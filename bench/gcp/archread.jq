# The archive over one arm's window, from its artifact (`jq -r -f archread.jq out/<tag>.json`): steps/s; the fast
# archiving lag (dwarf_archive_lag_seconds, how far the shard's oldest due root is past the grace - the larger job's
# mean and peak); the staged archive's oldest attempt (peak); flows archived against flows closed; and the archiving
# jobs' batches by kind, with their mean work time. A lag that stays within seconds is keeping up.
def most($obj; $prefix): [($obj // {}) | to_entries[] | select(.key | startswith($prefix)) | .value] | max // 0;
def batches($h; $kind):
  [$h[]? | select(.name == "dwarf_janitor_batch_seconds" and .attrs.part == "work" and (.attrs.job | startswith($kind)))]
  | (map(.count) | add // 0) as $n | (map(.sumSeconds) | add // 0) as $s
  | "\($kind) \($n) batches\(if $n > 0 then ", \($s / $n * 1000 | floor) ms each" else "" end)";
.results[0] as $r | $r.gauges as $g
| "\($r.stepsPerSec | floor) steps/s; lag mean \(most($g.gaugesMean; "dwarf_archive_lag_seconds") | floor) s, peak \(most($g.gaugesPeak; "dwarf_archive_lag_seconds") | floor) s; oldest attempt peak \(most($g.gaugesPeak; "dwarf_archive_oldest_attempt_seconds") | floor) s; archived \($r.engineCounters.dwarf_flows_archived // 0) of \($r.engineCounters.dwarf_flows_closed // 0) flows closed; \(batches($r.engineHistograms; "fast_archiving")), \(batches($r.engineHistograms; "large_archiving"))"
