# Top-N

FLOAT/DOUBLE sort keys treat positive and negative zero as a tie, so following ORDER BY
columns decide their rank. Only sort-key copies normalize zero; emitted payloads and partition
keys keep the original bits. Snapshot restore normalizes retained sort keys as well, including
older raw snapshots, before ranking new input.

MAP and MULTISET fields in retained rows fall back at planning time, including fields nested
inside ARRAY or ROW. Arrow's row codec cannot store them even when they are only payloads.
This restriction also applies to LIMIT/SortLimit, which use the same state representation.

**Status:** Native — all three rank strategies, idle-state TTL included; a small set of matcher
gaps below.

Flink lowers a rank filter (`ROW_NUMBER()`/`RANK()`/`DENSE_RANK() OVER (PARTITION BY ... ORDER BY
...)` restricted by `WHERE rn <= n` or `rn BETWEEN offset+1 AND offset+n`) to one of three ranker
implementations, chosen from the input's changelog kind and key structure. StreamFusion runs all
three natively:

- **Append-only ranker** — the input is insert-only; a per-partition sorted list of the current
  top rows. TTL expires per sort-key list — every list write refreshes all of that list's tie rows.
- **Update-fast ranker** — the input carries a unique key and the sort key is inferred monotonic
  against updates on that key (e.g. ranking by a descending `COUNT(*)`), mirroring Flink's
  `UpdatableTopNFunction`. For the `rn <= 1` special case this is `FastTop1Function`: rather than
  keeping bounded state for every row, a new row for a key is dropped immediately — no state
  update, no emission — the moment it fails to outrank the currently-held top row, since a
  monotonic sort key means a non-improving challenger can never later become the top row. TTL
  expires per row-key entry.
- **Retracting ranker** — the general case for an arbitrary retracting input, mirroring Flink's
  `RetractableTopNFunction`. TTL expires the *whole* per-partition buffer at once, on a clock
  refreshed by every record processed for that partition — modeling Flink's own per-record
  `SortedMap` rewrite.

Idle-state TTL is native across all three — see [TTL semantics](index.md#idle-state-ttl) and
[Configuration](../configuration.md) for the flag surface.

Also native: a projected rank number and both insert-only and retracting changelog input.
`OFFSET` runs natively over insert-only and update-fast inputs; a general retracting input
requires a projected rank when an offset is present. `RANK`/`DENSE_RANK` never reach the matcher at all —
Flink itself rejects them in streaming, so that's parity, not a gap.

An update-fast offset retains ranks 1 through rankEnd, including the hidden prefix, so unique-key
updates can move rows across the visible boundary. Its output uses Flink's positional update
cascades even when rank is not projected. An updated row moving into the hidden prefix retracts
its former visible position before the remaining visible transitions. Checkpoints and canonical
memory/RocksDB transitions retain the prefix and reapply the selected range on restore.

## Processing-time first-N

An insert-only `ROW_NUMBER() OVER (PARTITION BY key ORDER BY pt ASC)` filtered to
`rn <= N`, where `pt` is a `PROCTIME()` attribute and N is a positive constant no greater
than 2,147,483,647, runs as an arrival counter. The rank may be projected or omitted. Each key,
including a NULL key, emits its first N rows in arrival order as inserts. No clock value is
compared and no payload rows are retained: state is one integer counter per key, independent of N.
The same column-type gates as value-ordered Top-N apply. The existing rank-1 deduplication
path remains in use whenever Flink lowers the query to deduplication, including when it
replaces a projected rank with the constant 1.

This follows Flink's `AppendOnlyFirstNFunction`, including under mini-batch configuration.
Accepted rows increment the counter and refresh its TTL; rejected rows do neither. After expiry,
the next arrival starts again at rank 1 without retracting earlier output. Counters and TTL
timestamps survive memory and RocksDB checkpoints, rescaling by Flink key group, and canonical
savepoints across the two backends. The `streamfusion.operator.topN.enabled` switch controls
this path; rank-1 dedup retains its own switch.

The standalone row-fed release measurement on an Apple M1 Max used 1,000,000 rows,
4,096 keys, N=2, projected rank, parallelism 1, two warmups and five alternating trials per
engine. Median elapsed times were **0.378589 s Flink / 0.446913 s native (0.847x)**.
The row source, both row/Arrow transposes and row blackhole sink remain in the measured path;
the benchmark asserts the native first-N node and both transposes. This workload is slower
natively. The feature is retained for coverage and composition inside native islands, where
first-N previously forced a fallback, rather than as a standalone throughput improvement.

Reproduce with `SF_BENCHMARK=true mvn -Pbench -pl :streamfusion-runtime -am test
-Dtest=FirstNBenchmark -Dsurefire.failIfNoSpecifiedTests=false`.

## Gaps

- A non-constant (variable) rank range.
- A row type the native converter can't carry.
- A general **retracting** input with an `OFFSET` and no projected rank. Flink's hidden-rank
  emission mutates retained row kinds, affecting later retraction matching. The native immutable
  row buffer does not yet reproduce that state contract; this case stays on Flink, as tracked in
  [#102](https://github.com/datafusion-contrib/StreamFusion/issues/102).
- Time-ordered ranks beyond the existing rank-1 dedup forms and the processing-time first-N
  form above: event-time N > 1, descending processing-time N > 1, updating first-N input,
  first-N with an offset, or a first-N bound beyond the signed 32-bit counter range.

[LIMIT](limit.md) reuses this same operator — a plain row-count limit is Top-N with a constant
rank range starting at 1.

## Window Top-N and window dedup

Window Top-N ranks within a windowing TVF's windows (`PARTITION BY window_start, window_end, key
ORDER BY ...`); window dedup is the same shape specialized to a time-ordered rank-1, keeping the
first or last row per key per window. Both are native for **event-time and proctime** windowing:
the windowing TVF assigns each row to the window(s) covering its rowtime (or, under proctime, the
operator's clock), and the rank operator closes each window on the same chained
processing-time-timer model as the [window aggregate](window-aggregate.md) — the slide must divide
the size. As with the other proctime-driven window operators, this is non-deterministic, so it's
tested for routing/execution but not byte-compared to the host.

For plain `TIMESTAMP` rowtime, window start/end remain wall-clock values regardless of the session
zone. A window dedup keep-last replaces a candidate with an equal rowtime; keep-first and general
Top-N preserve the earlier arrival on a tie. This plan-level tie policy is reapplied after memory
or RocksDB restoration, without changing the retained row or snapshot layout.

The one shape gap is a rank that doesn't start at 1 — i.e. an `OFFSET` on the window rank. Both
shapes also hold the [window-assignment zone gate](window-aggregate.md#matcher-declines): a
`TIMESTAMP_LTZ` time attribute in a session zone with any historical or recurring transition, or a fixed offset not
aligned with the window slide, falls back together with the windowing TVF that feeds them.

Attached start/end columns are local wall-clock values, including inside native expressions.
Window Top-N/dedup preserves these payload columns; it converts the watermark or processing-time
threshold into their fixed-offset domain, firing at the last millisecond of the window. It does
not apply another zone shift on output. Plain TIMESTAMP uses a zero offset. This also supports
input from an aggregate whose boundaries are already rendered locally.

The `-Dstreamfusion.operator.windowRank.enabled` switch covers both shapes; window dedup reuses the
window-rank operator rather than getting its own switch — see [Configuration](../configuration.md).
