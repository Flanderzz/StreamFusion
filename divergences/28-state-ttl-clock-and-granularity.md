# 28 — State TTL: clock sampling and expiry granularity

Native idle-state TTL (`table.exec.state.ttl`) replicates Flink's `StateTtlConfig` as the table
runtime configures it — `OnCreateAndWrite`, `NeverReturnExpired`, expired ⟺ `last_write + ttl <=
now` — but diverges from Flink's mechanics in four deliberate ways. All four are invisible outside
timing windows that wall-clock TTL already makes non-deterministic in Flink itself.

## Per-call clock sampling

Flink reads `System.currentTimeMillis()` inside every state access (`TtlTimeProvider.DEFAULT`).
The native operators sample the clock once per ingest call — the host passes its
`ProcessingTimeService` reading (the same wall clock in production) as a JNI argument, and every
row of the batch shares it. The difference is bounded by one batch's processing time, well inside
the run-to-run jitter Flink's own per-access reads have. The win: the test harness steers the
service's clock, so expiry is deterministically testable at the operator level with no test-only
hooks. Corollary: the mini-batch flush paths replay staged work under the bundle's last ingest
clock instead of widening the flush ABI with a second clock argument.

## Retracting Top-N: whole-buffer expiry

Flink's `RetractableTopNFunction` splits its state: a `ValueState<SortedMap>` treemap written on
every record for the partition, and a per-sort-key `MapState` written only when that sort key is
touched. Under TTL the two halves can expire independently, leaving internally inconsistent state
whose observable output is a hardcoded-lenient warn-and-skip. We model the treemap's clock only:
the whole buffer expires atomically on a head-entry timestamp refreshed by every processed record.
A partition idle past the retention loses everything at once (a stale retraction then finds
nothing and emits nothing — the same observable as Flink's lenient path); a partition Flink would
half-expire keeps its rows here. Replicating the half-expired output exactly would mean a second
count structure whose only purpose is reproducing state corruption.

## Proctime keep-last dedup: which Flink to match

Flink's identical-row suppression in proctime keep-last dedup compares `RowKind` through its
generated equaliser, and its heap state backend aliases the stored row with the emitted one — so a
key suppresses duplicates only until its first update mutates the stored kind to `UPDATE_AFTER`,
after which identical rows re-emit forever. On RocksDB the stored bytes keep the pre-mutation
kind and suppression keeps working. Flink's own output therefore differs across its state
backends; we match the heap backend (the one the parity harness runs against), and persist the
stored kind in snapshots as the heap backend effectively does.

## No resurrection when the retention grows across a restore

Physical cleanup (the memory sweep; the Paimon compactor's record-level expire) removes rows that
were expired under the retention in force at the time. Restoring with a larger
`table.exec.state.ttl` cannot bring them back. Flink documents the identical caveat for its
RocksDB compaction filter; keeping the retention out of the persistent table schema (it is passed
per session, never stamped) ensures a stale value can at least never drive future drops.
