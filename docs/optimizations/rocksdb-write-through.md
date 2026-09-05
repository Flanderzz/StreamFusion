# RocksDB write-through on Flink's write path

**Applies to:** every native operator running on the RocksDB state backend's typed store — all
operators today, event-time and proctime, including the multiset aggregate shapes (MIN/MAX
retraction, DISTINCT) with per-element companion tables and every OVER shape (unbounded folds,
bounded ROWS/RANGE frame buffers, proctime, DISTINCT seen-sets); the snapshot path remains only
for shapes whose state has no fixed-type native codec

Found by issue [#26](https://github.com/datafusion-contrib/StreamFusion/issues/26): at 10M events
the backend spent 80% of CPU in its own memory-pressure flush, and a StreamFusion-only tuning knob
(`write-buffer-mb`) swung the result from 3x slower than Flink to 2x faster.

The original store retained dirty entries in a Java-governed map above RocksDB — a second memtable.
When it hit its threshold it drained the whole map on the task thread, encoded every value as a
standalone Arrow IPC stream, forced a memtable flush into small L0 files, and cleared the read
cache. All of that was overhead management for a buffer RocksDB already has.

The store now follows Flink's write path with the batching advantage kept:

- **Write-through per bundle.** Dirty entries are written to the RocksDB memtable (WAL off) at
  every bundle boundary — one coalesced write per touched key per bundle, where Flink pays one per
  record. RocksDB's background threads own all flushing and compaction; the barrier only commits
  the current bundle's residue, so checkpoint sync time no longer scales with the interval's
  write volume. The working map is a per-bundle read/dedup cache, nothing more.
- **One columnar conversion per bundle.** Values are compact arrow-row bytes: the whole dirty set
  encodes in a single `RowConverter` pass, and `begin_batch` hydrates misses with one batched read
  plus a single batch decode — replacing a per-value Arrow IPC stream (schema framing per value,
  parsed even inside the TTL compaction filter). The TTL timestamp is now a fixed 8-byte value
  prefix, making the compaction filter one integer read per entry.
- **Optimized, pinned batch reads.** Point-read stores use rust-rocksdb's
  `batched_multi_get_cf` on the default column family. The older `multi_get` binding reaches
  RocksDB's legacy vector-returning API; the batched API groups SST lookups to share block work
  and pipeline cache misses. It also borrows input key bytes and returns `DBPinnableSlice`
  values, avoiding the legacy binding's copied keys and intermediate value buffers. Cache-backed
  values remain pinned until the batch has been decoded into owned operator state; other read
  paths can still copy into the pinnable slice. Keys need not be sorted, and results retain input
  order. The default column family receives the same translated Flink options as before,
  including its block cache, compression, write buffers, and TTL filter.
- **Columns directly from aggregate state.** The group codec builds integer and floating-point
  Arrow columns directly from accumulator iterators, and restores each group directly from the
  decoded columns. This removes the per-group scalar vectors, their row-to-column transpose,
  and the extra vectors and scalar clones on restore. Decimal and string state keep the general
  scalar-to-array converter. `RowConverter` still produces exactly the existing persisted bytes;
  the optimization is around it, not a replacement row format.
- **Flink's memory governance.** One shared block cache and write-buffer manager per slot, sized by
  `state.backend.rocksdb.memory.*` with Flink's exact split formulas, replaces per-store 256 MB
  caches. Total native state memory stops scaling with operator count.

The `streamfusion.state.rocksdb.write-buffer-mb` knob and the memory-pressure flush are deleted;
there is no StreamFusion-specific state memory tuning.

Measured (Nexmark state-backend A/B, 200K events, parallelism 2, best of 1, vs Flink RocksDB):

- q4: 1.41s → 1.30s (4.20x → 5.05x); with the old knob at 1 MiB (the pathology at small scale) the
  old code degraded to 1.91s — that failure mode no longer exists.
- q7: 1.33s → 1.21s (2.31x → 2.70x).

## Batched-read and codec measurements

Measured on an Apple M1 Max (10 cores, 64 GiB), Java 17, with the release `bench` profile,
2M Nexmark events, parallelism 4, mini-batching off, and one-second checkpoints. These runs use
the existing exactly-once Kafka harness and its native serialization boundaries described in
[Benchmarks](../benchmarks.md); they are separate from generator-to-blackhole measurements.
No benchmark queries, sources, sinks, or backend tuning were changed.
The baseline was commit `62be413`.

Q17 CPU profiles used `exactlyOnceKafkaSinkProfileLoop`, `profile.backend=rocksdb`, a 30-second
loop, and async-profiler CPU sampling at 1 ms. Counts below are inclusive samples divided by
completed 2M-event jobs (10 baseline, 10 pinned-only, 11 with both changes). They estimate CPU
work, not elapsed time; rows overlap and must not be added.

| Q17 path | Baseline | Pinned reads | Pinned reads + column codec |
|---|---:|---:|---:|
| RocksDB MultiGet | 966 | 554 | 577 |
| Group aggregate update | 1,752 | 1,320 | 1,251 |
| Dirty-state writes | 244 | 247 | 167 |
| State-value decode | 40 | 47 | 29 |

Pinned reads reduced sampled read CPU by about **43%** and group-update CPU by **25%**.
The column codec then reduced the sampled dirty-state write path by **32%** and value decode
by **38%** relative to pinned reads alone. Median execution time after the first job in each
profile loop was 2.40s, 2.40s, and 2.25s respectively. The CPU savings therefore do not translate
proportionally into end-to-end throughput: Kafka, checkpoints, and the rest of the pipeline
remain in the measured path. Arrow-row conversion itself accounted for only about 10–12
samples per job; the useful codec improvement was removing the surrounding temporary values.

The unprofiled matrix used one warmup and the best of two timed runs per engine. Native gain is
the ratio of the native before/after times. The Flink control timings are included to show the
run-to-run variability, particularly on Q18; these are observations from this host and workload.

| Query | Native before (s) | Native after (s) | Native throughput gain | Flink before (s) | Flink after (s) |
|---|---:|---:|---:|---:|---:|
| Q4 | 10.275 | 5.098 | **2.02×** | 54.441 | 50.672 |
| Q17 | 2.370 | 2.118 | **1.12×** | 4.247 | 3.919 |
| Q18 | 11.169 | 6.735 | **1.66×** | 42.465 | 36.343 |

The subsequent full 23-query run (2M events, parallelism 4, one warmup, best of two) completed
without failures or fallbacks. StreamFusion's throughput geomean over Flink RocksDB was **2.75×**
with mini-batching off and **3.41×** with it enabled, up from the previous headline's 2.47× and
2.92×. The full per-query ratios and methodology are in [Benchmarks](../benchmarks.md).

Reproduce the unprofiled state-backend comparison with:

```bash
SF_BENCHMARK=true SF_MATRIX_STATE_BACKENDS=true \
SF_MATRIX_QUERIES=q4,q17,q18 SF_ROWS=2000000 SF_WARMUP=1 SF_RUNS=2 \
mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkMatrixBenchmark#stateBackendComparison'
```

For the Q17 CPU recording, set `ASPROF_LIB` to the installed async-profiler library and
`PROFILE_FILE` to an output file, then run:

```bash
SF_BENCHMARK=true SF_PROFILE_KAFKA_SINK=true SF_ROWS=2000000 \
mvn -pl :streamfusion-runtime test -Pbench \
  -Dtest='NexmarkMatrixBenchmark#exactlyOnceKafkaSinkProfileLoop' \
  -Dprofile.query=q17 -Dprofile.backend=rocksdb -Dprofile.seconds=30 \
  "-Dsf.extraJvmArgs=-agentpath:${ASPROF_LIB}=start,event=cpu,interval=1ms,cstack=fp,collapsed,file=${PROFILE_FILE}"
```
