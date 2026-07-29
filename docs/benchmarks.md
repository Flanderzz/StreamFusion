# Benchmarks

Acceleration claims in this project are measured, not asserted. This is where the
method and the numbers live.

## Native operator micro-benchmarks

[`native/benches/operators.rs`](../native/benches/operators.rs) measures each native
operator's steady-state hot loop over an in-memory Arrow batch, isolated from the JVM
bridge and from Flink's job scheduling (which otherwise dominates and hides operator
cost). Built on [Criterion](https://github.com/bheisler/criterion.rs).

Run:

```bash
cd native && cargo bench
```

The optional Kafka sink encoder benchmark requires its connector feature:

```bash
cd native && cargo bench --features kafka --bench kafka_sink
```

Criterion reports time per batch with a confidence interval and compares against the
previous run, so a regression in a hot loop is visible commit-to-commit. Each bench
declares its row count as throughput, so Criterion also prints elements/s.

Current benches:
- `filter/gt_literal` — the compiled-predicate filter (`v > 0`) over a 4096-row batch,
  half passing. The predicate is compiled once before the loop, so this measures
  evaluation + the Arrow filter kernel, not planning.
- `tumbling/sum_update_flush` — a tumbling `SUM` over 16 windows: one `update` of a
  4096-row batch followed by a `flush` of all closed windows, from fresh state each
  iteration.
- `tumbling/sum_keyed_update_flush` — the same, grouped by a bigint key (64 distinct
  values), so it exercises the per-row grouping-key path the unkeyed bench does not.
  The `_accounted` variant attaches a managed-memory budget, measuring the per-touched-group
  footprint tracking an operator pays when the host hands it one (default off in the plain bench,
  so the unaccounted number is the like-for-like baseline).
- `local_group_by_logical_bundle/logical/*` — local two-phase `SUM` over 64 hot keys with
  logical bundle sizes 1, 32, 256, 4,096, and 50,000. Size 1 is the immediate/non-coalescing
  baseline; `physical_batch` pins the former one-flush-per-Arrow-batch behavior.
- `group_by_logical_bundle/*` — single-phase `SUM` over 64 hot keys, comparing per-row changelog
  construction, one final diff per 256-row physical batch, and one diff per 4,096-row bundle.
- `session/sum_keyed_update_flush` — a session `SUM` grouped by key (gap merge). Its rows are
  spaced beyond the gap, so every row opens its own one-row session — the worst case for session
  state (4096 open sessions). `session/sum_keyed_dense_update_flush` is the complementary shape:
  each key's rows chain within the gap into one long session, the common real workload.
- `over/running_sum_keyed`, `over/row_number_keyed`, `over/bounded_rows_sum_keyed` — the columnar
  `OVER` push+flush, for a running `SUM` (specialized fold), `ROW_NUMBER` (per-key counter), and a
  bounded `ROWS 10 PRECEDING` frame (per-key buffer + frame recompute).
- `retract_topn/{immediate,physical_256,logical_4096}` — the retracting Top-N (changelog input,
  full buffers): steady-state inserts into 64 pre-populated partitions, comparing per-row output,
  a diff after every 256-row Arrow batch, and one diff over the logical 4,096-row bundle.
- `append_topn_logical_bundle/*` — append-only ascending Top-10 over 64 partitions with sustained
  boundary churn. It compares the per-record cascade, a net diff after each 256-row physical
  batch, and one net diff over the 4,096-row logical bundle, with and without projected rank.
- `unique_updating_join_logical_bundle/{immediate,physical_256,logical_4096}` — an INNER join whose
  two join keys are unique, under a left-side replacement storm over 64 keys; compares per-record
  output, physical-batch transition folding, and one logical-bundle transition per key.
- `dedup/keep_first_emitted_probe` — keep-first dedup in its steady state: all 256 keys already
  emitted, so each row is one emitted-set probe and a drop.
- `exchange/split_by_key_8` — the columnar shuffle's by-key split: hash each row's key to one of
  8 partitions and gather the sub-batches.
- `interval_join/equi_key_push`, `window_join/equi_key_flush` — the two joins with a unique key
  (1:1 match, no cross product), so they measure the DataFusion hash-join construction per batch.
- `date_format/compiled` vs `date_format/per_row_parse` — the DATE_FORMAT hot loop after and
  before the compile-once change (pattern parsed once vs re-parsed inside every row's Display),
  kept as an A/B pair so the win stays visible.
- `kafka_json_sink/{whole_arrow_batch,one_writer_per_row}` — the production JSON encoder over one
  4096-row Arrow batch versus the same encoder invoked on 4096 one-row slices, isolating the sink's
  batching win from Kafka I/O and checkpointing.

### Results

Numbers are only comparable within a machine; record the host (CPU) alongside. The release
profile pins `codegen-units = 1` (see `native/Cargo.toml`): with the default parallel split,
hot-loop numbers swung ~50% from unrelated code additions elsewhere in the crate, so numbers
measured before the pin (or without it) are not comparable to these.

**Apple M1 Max** (median of 100 Criterion samples):

| Benchmark | Rows/batch | Time/batch | Elements/s | Notes |
|---|---|---|---|---|
| `filter/gt_literal` | 4096 | 2.5 µs | ~1.63 Gelem/s | compiled predicate, ~50% selectivity |
| `tumbling/sum_update_flush` | 4096 | 77 µs | ~53.5 Melem/s | 16 windows, no key |
| `tumbling/sum_keyed_update_flush` | 4096 | 110 µs | ~37.4 Melem/s | 16 windows, 64 bigint keys |
| `tumbling/sum_keyed_update_flush_accounted` | 4096 | 106 µs | ~38.5 Melem/s | same, managed-memory budget attached (≤1% overhead) |
| `interval_join/equi_key_push` | 4096 | 63 µs | ~65 Melem/s | INNER, 1:1, equi-key + interval filter |
| `window_join/equi_key_flush` | 4096 | 130 µs | ~31.5 Melem/s | INNER, 1:1, equi-key + window bounds |
| `over/running_sum_keyed` | 4096 | 183 µs | ~22.3 Melem/s | running aggregate, specialized fold, 64 keys |
| `over/row_number_keyed` | 4096 | 131 µs | ~31.4 Melem/s | per-key counter, 64 keys |
| `over/bounded_rows_sum_keyed` | 4096 | 452 µs | ~9.1 Melem/s | ROWS 10 PRECEDING frame recompute, 64 keys |
| `retract_topn/immediate` | 4096 | 3.29 ms | ~1.24 Melem/s | changelog Top-N, per-row before/after diff |
| `dedup/keep_first_emitted_probe` | 4096 | 16 µs | ~255 Melem/s | steady state: every key already emitted |
| `exchange/split_by_key_8` | 4096 | 57 µs | ~72 Melem/s | by-key split into 8 partitions |
| `session/sum_keyed_update_flush` | 4096 | ~2.2 ms | ~1.9 Melem/s | one-row sessions, 64 keys (high-variance) |
| `session/sum_keyed_dense_update_flush` | 4096 | 101 µs | ~40.4 Melem/s | gap-chained sessions, 64 keys |
| `date_format/compiled` | 4096 | 378 µs | ~10.8 Melem/s | pattern compiled once (`per_row_parse` pins the old loop at 670 µs) |
| `json_decode/three_field_object` | 4096 | 610 µs | ~6.7 Melem/s | ~46 B docs, simd-json tape walk |
| `json_decode/nexmark_bid_shape` | 4096 | 985 µs | ~4.2 Melem/s | ~210 B docs, 4 of 7 fields skipped |
| `kafka_json_sink/whole_arrow_batch` | 4096 | 592 µs | ~6.92 Melem/s | one Arrow JSON writer per batch |
| `kafka_json_sink/one_writer_per_row` | 4096 | 3.55 ms | ~1.15 Melem/s | same production encoder, one writer per row (6.0x slower) |

Local GROUP BY count-boundary baseline on the same Apple M1 Max (median of 100 Criterion samples,
64 bigint keys):

| Logical rows/bundle | Rows/iteration | Time/iteration | Elements/s | vs size 1 |
|---:|---:|---:|---:|---:|
| 1 | 4,096 | 4.250 ms | 0.964 M | 1.00× |
| 32 | 4,096 | 960.2 µs | 4.266 M | 4.43× |
| 256 | 4,096 | 413.0 µs | 9.918 M | 10.29× |
| 4,096 | 4,096 | 230.7 µs | 17.751 M | 18.42× |
| physical Arrow batch (4,096) | 4,096 | 229.7 µs | 17.829 M | 18.50× |
| 50,000 | 50,000 | 2.721 ms | 18.373 M | 19.06× |

The exact logical 4,096-row path and the old physical-batch path are statistically equivalent,
showing that the boundary controller adds no measurable kernel overhead when boundaries coincide.
The curve also quantifies the opportunity: output materialization and per-bundle setup dominate
small bundles, while throughput plateaus around 4K rows for this 64-key shape.

Single-phase GROUP BY on the same Apple M1 Max (median of 100 Criterion samples, 4,096 rows, 64
bigint keys, 256-row physical batches):

| Mode | Time/iteration | Elements/s | Logical speedup |
|---|---:|---:|---:|
| per-row immediate changelog | 568.1 µs | 7.210 M | 3.25× |
| net diff per physical batch | 414.3 µs | 9.886 M | 2.37× |
| net diff per logical bundle | 174.9 µs | 23.414 M | 1.00× |

The logical path retains only Arrow key-buffer references plus one first output tuple per touched
group, mutates durable accumulators for every row, and materializes final aggregate tuples once.

Append-only Top-N logical-bundle baseline on the same Apple M1 Max (median of 100 Criterion
samples, 4,096 descending values across 64 partitions, ascending Top-10):

| Mode | Rank projected | Time/iteration | Elements/s | Logical vs mode |
|---|---:|---:|---:|---:|
| per-record cascade | no | 903.0 µs | 4.536 M | 1.40× |
| net diff per 256-row physical batch | no | 1.740 ms | 2.354 M | 2.70× |
| net diff per 4,096-row logical bundle | no | 645.6 µs | 6.345 M | 1.00× |
| per-record cascade | yes | 1.925 ms | 2.128 M | 3.41× |
| net diff per 256-row physical batch | yes | 1.290 ms | 3.176 M | 2.28× |
| net diff per 4,096-row logical bundle | yes | 565.1 µs | 7.248 M | 1.00× |

The physical-diff baseline deliberately reproduces the former Arrow-batch-sensitive cadence. A
five-second release sample of `logical_diff_rank` attributes most samples to `TopNRanker::push`,
with `arrow_row::Row::owned` and allocator traffic prominent; after output coalescing, row
ownership in state mutation is the next measured target.

Retracting Top-N logical-bundle baseline on the same Apple M1 Max (20 Criterion samples, 4,096
steady-state inserts across 64 pre-populated partitions, Top-10, 256-row physical batches):

| Mode | Time/iteration | Elements/s | Logical vs mode |
|---|---:|---:|---:|
| immediate per-row diff | 3.292 ms | 1.244 M | 2.54× |
| net diff per 256-row physical batch | 1.926 ms | 2.127 M | 1.48× |
| net diff per 4,096-row logical bundle | 1.297 ms | 3.158 M | 1.00× |

All modes mutate the same full retracting buffers. The logical mode retains one visible-window
preimage per touched partition and emits only its final membership/rank transition.

Unique-key updating-join logical-bundle baseline on the same Apple M1 Max (20 Criterion samples,
4,096 left-side delete/insert rows over 64 keys matched by a stable unique right side):

| Mode | Time/iteration | Elements/s | Logical vs mode |
|---|---:|---:|---:|
| immediate per-record join changelog | 455.9 µs | 8.985 M | 3.08× |
| net transitions per 256-row physical batch | 541.1 µs | 7.570 M | 3.66× |
| net transitions per 4,096-row logical bundle | 147.9 µs | 27.692 M | 1.00× |

The first profiled logical implementation measured 15.03 M rows/s. Its sample was dominated by
allocating and freeing an owned key for every repeated staging-map update; borrowed-key probing
raised the final result to 27.69 M rows/s while leaving durable payload ownership unchanged.

The gap between filter and aggregation is the signal: the filter is a compiled
expression plus one Arrow kernel, while an aggregator groups every row by its key and
holds per-group accumulator state across batches.
Profiling-driven cuts so far (tumbling, 4096-row batch):

- reusing the per-row window buffer instead of allocating one per row (244 → 181 µs);
- moving the row's key into its last window instead of cloning it for every window
  (181 → 171 µs unkeyed, 395 → 323 µs keyed);
- a fast hash (`ahash`) for the grouping map instead of the stdlib SipHash
  (171 → ~106 µs unkeyed, 323 → ~252 µs keyed);
- one codegen unit for the release build (~106 → ~84 µs unkeyed; most operators gained
  10–17%, and the numbers stopped drifting with unrelated code churn);
- the joins stopped rebuilding a full DataFusion `SessionContext` (its entire function
  registry) per pushed batch — a bare `TaskContext` (or the operator's cached pool-wired
  one, when accounted) is all a hash join needs (interval join ~115 → ~63 µs, window join
  ~184 → ~130 µs at equal codegen settings);
- the Kafka JSON/CDC decode swapped arrow-json's scalar tokenizer for a simd-json (SIMD
  stage-1) parse walked straight into typed Arrow builders — ~8% on tiny 3-field documents,
  ~27% on a realistic Nexmark-bid-sized document (1.36 ms → 985 µs; decimal-bearing schemas
  keep the arrow-json raw-literal path for exactness — see `divergences/18`);
- the session aggregator stopped slicing the value column one row at a time: rows are grouped
  per key and segmented (in timestamp order) into gap-connected runs — the connected components
  the row-at-a-time walk would build — so a run pays one `take` + one accumulator update
  (2.04 ms → 217 µs, 9.4×, on the dense gap-chained shape; the one-row-session shape is
  per-session-bound and unchanged). The open-session merge scan also became a bounded
  `BTreeMap` range probe instead of a walk of every open session, which matters when a key
  holds many not-yet-closed sessions;
- the windowed aggregators (tumbling/hopping/cumulative and session) swapped their
  `Vec<ScalarValue>` group keys for the arrow-row memcomparable encoding the non-windowed
  GROUP BY already used: keys are encoded once per batch, the per-batch grouping map holds
  borrowed byte-row views (no per-row allocation), and flush decodes stored keys straight
  back into output columns (keyed tumbling 245 → 110 µs, 2.2×; dense session 217 → 101 µs;
  the managed-memory-accounted variant gained the same).

Net so far: the unkeyed tumbling path is ~3.2× faster (244 → ~77 µs) and the keyed path ~3.6×
(395 → ~110 µs). The 2026-07-05 round retired the remaining scalar-keyed loops onto the same
arrow-row byte state: the three keyed `OVER` loops (running sum 422 → 183 µs, ROW_NUMBER
342 → 131 µs, bounded frame 688 → 452 µs), the retracting Top-N (10.2 → 3.1 ms — byte sort keys
replace the scalar comparator, `Arc`-shared payloads make the per-row before/after snapshots
refcount bumps), keep-first dedup's emitted set (+6%), and the exchange split (174 → 57 µs,
hashing the encoded key bytes). The last scalar-keyed maps (window Top-N, changelog normalizer,
temporal join, mini-batch local aggregate) are bench-gated candidates on the [perf backlog
issue](https://github.com/datafusion-contrib/StreamFusion/issues/14).

The running `OVER` aggregate was the per-row outlier (~2.6 Melem/s, a DataFusion accumulator
`update_batch` + `evaluate` per row); replacing it with a specialized typed running fold —
matching the accumulators exactly (wrapping integer sum, null-skipping) but without the per-row
call — took it to ~8 Melem/s (3×), and the arrow-row key swap above to ~22 Melem/s. The session
aggregator's dense (gap-chained) shape runs at
tumbling-level throughput (~40 Melem/s); its sparse shape (~1.9 Melem/s, high-variance) is bound
by genuinely per-session costs — accumulator creation and flush materialization for 4096 one-row
sessions — not by the update loop.

## End to end vs. Flink

`ThroughputBenchmark` (opt-in: `SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench -Dtest=ThroughputBenchmark`)
runs the same query over a large generated source (5M rows; override with `SF_ROWS`) into a
sink, once with native substitution installed and once on stock Flink, single slot. It reports
best-of-3 rows/s for each and the native/Flink ratio. A warmup run absorbs JIT and minicluster
startup so the measured runs reflect execution.

**The `-Pbench` profile is mandatory** — it builds and loads the *release* native library.
Without it, `mvn test` uses the debug build (fast to compile, ~10–20× slower to run), which
makes every native number misleadingly low. (Measured: the columnar copy below ran 0.48× on
the debug build and 3.0× on release — same code.)

| Operator | Query | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| Parquet copy (columnar source → sink) | `INSERT INTO parquet SELECT * FROM parquet` | 1.51 M rows/s | 19.4 M rows/s | **12.85×** |
| Parquet sink (row source) | `INSERT INTO parquet SELECT *` | 1.26 M rows/s | 4.12 M rows/s | **3.26×** |
| Windowed aggregate over a columnar source | `SUM` by 1s window from a Parquet table | 1.80 M rows/s | 3.29 M rows/s | **1.82×** |
| Interval join (event-time) | `a JOIN b ON a.k=b.k AND a.rt BETWEEN b.rt ± 1s` | 0.37 M rows/s | 0.63 M rows/s | **1.71×** |
| `OVER` running `SUM` (row source) | `SUM(v) OVER (ORDER BY rt)` | 0.91 M rows/s | 1.42 M rows/s | **1.56×** |
| Tumbling (row source) | `SUM` by 1s window | 1.69 M rows/s | 2.10 M rows/s | **1.24×** |
| Filter (`WHERE`) | `SELECT * FROM f WHERE v > 50` | 3.23 M rows/s | 2.41 M rows/s | **0.75×** |

The gain tracks how much of the pipeline stays columnar. Fully-columnar paths lead — the copy
**12.85×**, the windowed aggregate over a columnar source **1.82×**, the event-time interval join
**1.71×** (Flink's interval join is slow; ours delegates the match to a DataFusion hash join). The
**Parquet sink reaches 3.26×** even from a row source: it encodes Arrow → Parquet natively and
rolls part files exactly like the host (on checkpoint and on the configured size/time policies —
the sink now runs inside Flink's own streaming file writer, so file lifecycle overhead matches the
host's and the entire margin is the encoding), which also lifted the columnar copy (4.68 → 12.85×:
the old sink rolled a file per million rows; checkpoint-driven rolling writes one). Other row-source ops
still pay a `RowData → Arrow` transpose at the input, ~25% cheaper since the converter was made
row-major + pre-sized ([wontdos/28](../.claude/wontdos/28-native-row-transpose-and-shuffle.md)): `OVER`
running `SUM` **1.56×**, tumbling **1.24×**. The lone stateless **filter stays below 1× at 0.75×** —
a single cheap predicate cannot earn back the `RowData → Arrow → RowData` round-trip. A lone operator
crosses 1× once fed by a columnar source or chained with other native operators (no transpose between
them) — the columnar-flow work ([divergences/08](../divergences/08-columnar-flow-transitions.md)).

### How we got these numbers (a profiling lesson)

The first end-to-end numbers were *far* worse — the columnar copy measured **0.45×**, which
made no sense for a zero-transpose pipeline. Rather than tune blindly, we profiled, and the
chain of measurements is worth recording:

1. **Pure-native ceiling**: a Rust-only Parquet copy of 5M rows ran in **0.36s (14 M rows/s)**
   — so native compute was never the bottleneck; the JVM job was ~13× slower than the compute.
2. **Fixed vs. variable**: at 100K rows native and Flink tied (~0.66s, all fixed job overhead);
   the gap only appeared at scale, so it was a per-row/per-batch cost.
3. **Component timing**: the sink's `Native.writeParquet` dominated (**5.8s of 7.3s**), ~17×
   slower per batch than the *same* native write standalone. Export/serialization were
   negligible (the operators chained, so no IPC).
4. **GC ruled out**: a `-verbose:gc` run showed exactly **one** 5.7ms pause — not GC.
5. **Root cause**: the Maven build loaded the **debug** native library (`cargo build`, no
   `--release`). Debug Rust on Parquet byte-encoding is ~10–20× slower. Building release
   (`-Pbench`) moved the copy from **0.45× to 3.19×** — same code.

The lesson is baked into the harness: benchmarks must run under `-Pbench` (release), and
`mvn test` keeps the fast debug build for the correctness loop only.

## Nexmark

The Nexmark suite is the honest end-to-end read: the source is the rowwise `nexmark` datagen (the
wide event row — `event_type` plus nested `person`/`auction`/`bid` structs) and the sink is
`blackhole` (also rowwise), exactly the published Nexmark plan, so a native island pays a
`RowData → Arrow` transpose at the source **and** an `Arrow → RowData` transpose at the sink. We keep
both transposes in the measured path on purpose — a real deployment feeds us rowwise records and
drains to a rowwise sink, so this is the honest number, not the favorable columnar-source/sink case.
Object reuse is on for both engines (a standard tuned-prod setting).

### q0–q4 (rowwise source + blackhole sink)

The first five queries, 2 M events, single slot — `SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench
-Dtest=NexmarkBenchmark`. q1's decimal arithmetic is exact and native by default (Decimal128 multiply
+ a HALF_UP cast to DECIMAL(23,3), matching Flink).

| Query | Shape | Flink | Native | Native vs. Flink |
|---|---|---|---|---|
| q2 | filter `WHERE MOD(auction, 123) = 0` | 1.91 M ev/s | 2.87 M ev/s | **1.50×** |
| q1 | `0.908 * price` (exact decimal) | 1.92 M ev/s | 2.15 M ev/s | **1.12×** |
| q0 | pass-through projection of `bid` fields | 2.00 M ev/s | 2.17 M ev/s | **1.08×** |
| q4 | regular join → `MAX` per auction → `AVG` per category | 1.12 M ev/s | 1.15 M ev/s | **1.03×** |
| q3 | regular (updating) join `auction ⋈ person` on seller | 2.93 M ev/s | 1.57 M ev/s | **0.54×** |

**q0/q1/q2 beat stock Flink** even on the rowwise perimeter. Four changes got them there, all profiled
on q0: disabling Arrow's per-accessor bounds/refcount checks (deployment flag); object reuse (drops
Flink's per-handoff defensive copy); a zero-copy `ColumnarRowData` at the exit transpose; and — the big
one — **nested projection pushdown at the entry transpose**, which converts only the columns and struct
sub-fields the calc reads rather than the whole wide row, so unread structs never touch Arrow. That
roughly doubled native throughput and was the difference between ~0.6× and >1×.

**q4 reaches parity** (0.69→1.03×): its join is a *regular* updating join (the `B.dateTime BETWEEN
A.dateTime AND A.expires` bound is a data column, not an interval) feeding two `GROUP BY`s. Batching the
INNER join's whole input (one columnar residual-predicate eval, emit by `filter_record_batch`, rows
moved into state rather than re-cloned) removed the per-pair `ScalarValue` and clone churn. **q3 stays
below 1×**: the same regular join but with *unbounded, ever-growing* state (one popular seller matching
many auctions), and the residue is the per-row state store — a fresh `OwnedRow` per buffered row where
Flink reuses pooled `BinaryRowData`. A free-list allocator for the keyed-multiset buffers is the next
lever ([divergences/08](../divergences/08-columnar-flow-transitions.md)).

### q0–q2 from a Kafka source (native decode)

The native decoder is itself a (Rust) bytes→Arrow transpose. Flink does **not** push projection into
the Kafka scan, so its format decodes the whole record; we push the query's projection into the decode
so it builds only the read columns/fields. `SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench
-Dtest=NexmarkKafkaBenchmark` (Testcontainers Kafka). 2 M events, native decode vs Flink's own format:

| Query | JSON (Flink → Native) | Avro (Flink → Native) | Protobuf (Flink → Native) |
|---|---|---|---|
| q0 pass-through | 0.67 → 0.86 M ev/s — **1.27×** | 0.81 → 1.33 M ev/s — **1.64×** | 1.15 → 1.45 M ev/s — **1.26×** |
| q1 currency | 0.77 → 0.85 M ev/s — **1.10×** | 0.82 → 1.34 M ev/s — **1.63×** | 1.14 → 1.49 M ev/s — **1.30×** |
| q2 filter | 0.80 → 0.93 M ev/s — **1.17×** | 0.83 → 1.52 M ev/s — **1.83×** | 1.17 → 1.60 M ev/s — **1.36×** |

**Every format now clears 1× (JSON 1.1–1.3×, Avro 1.6–1.8×, Protobuf 1.3×) — each after attacking
what its profile said it was bound by.** All formats share a large Kafka-I/O + thread-sync cost
(~38–45%) with the Flink run; the decode itself is bound by different work. **JSON was
tokenize-bound** (~19% of CPU in `arrow-json`'s scalar tape parse of the whole document, only ~5%
building the Arrow arrays — so projection pruning couldn't help, and Flink's mature deserializer held
it to ~parity, 0.97–1.02×); swapping the tokenizer for a **simd-json** SIMD parse walked straight
into Arrow builders ([divergences/18](../divergences/18-simd-json-decode.md)) lifted it to
1.10–1.27×. **Avro is build/copy-bound** (~27% `memmove` + ~15% decode, of which `append_null` for
the mostly-null `person`/`auction` union branches was ~15% alone — pushing the projection into the
decode removed that build/copy of unread fields). **Protobuf** is also build/copy-bound (~25%
`memmove` + ~16% ptars decode); pruning via a **pruned descriptor** (ptars builds a column per
descriptor field and skips wire tags it has no field for) flipped it from 0.88–0.94× to 1.26–1.36×.

### The row→columnar ladder (Kafka)

How far into Rust the source-side work moves, on the same q0/q1/q2 over the same produced bytes, all vs
stock Flink. Three rungs, each one layer more native (projection pushed in at every rung that can):

1. **JVM transpose** — Flink consumes *and* decodes to `RowData` with its own format, then a JVM
   `RowData → Arrow` transpose feeds the native calc.
2. **Rust transpose, JVM poll** — Flink's `KafkaSource` polls raw bytes, a native operator decodes them
   straight to Arrow (the shallow decode path).
3. **Rust poll + Rust transpose** — the production native source: rdkafka consumes and the separately
   installed format artifact decodes inside the same poll call, dispatched through the versioned
   cross-DSO driver ABI (divergences/25).

`SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features mimalloc,kafka,json,avro,protobuf"
-Dtest=NexmarkKafkaLadderBenchmark`. 2 M events (2026-07-12), ×vs stock Flink (best rung **bold**; the
`mimalloc` feature — the recommended Kafka build — link-aliases the library's allocator, worth
+12–22% on the source rung, divergences/19):

| Format | Flink (ev/s) | JVM transpose | Rust transpose, JVM poll | Rust poll + Rust transpose |
|---|---|---|---|---|
| JSON q0 | 0.79 M | 1.04× | 1.20× | **2.30×** |
| JSON q1 | 0.78 M | 1.09× | 1.16× | **2.33×** |
| JSON q2 | 0.79 M | 1.07× | 1.21× | **2.41×** |
| Avro q0 | 0.89 M | 1.02× | 1.59× | **3.00×** |
| Avro q1 | 0.88 M | 0.99× | 1.62× | **3.02×** |
| Avro q2 | 0.87 M | 1.06× | 1.73× | **3.22×** |
| Protobuf q0 | 1.26 M | 1.03× | 1.22× | **2.09×** |
| Protobuf q1 | 1.23 M | 1.06× | 1.26× | **2.31×** |
| Protobuf q2 | 1.21 M | 1.15× | 1.35× | **2.38×** |

The full native source is the best rung on every format — **2.1–3.2× stock Flink**, 1.8–2.9 M ev/s
end to end — measurably *faster* than the pre-split fused source on the same machine (JSON q0 2.30×
vs 1.94× re-measured side by side), so the format-artifact modularity now costs nothing. Two caveats
this table's history earned: an early source rung trailed the shallow rung until the consume fast
path landed (divergences/19), and the 2026-07-11 modular split briefly decoded in a downstream
operator, which halved this rung until the in-poll driver-ABI decode restored it (divergences/25).
The matrix harness reads the same BIGINT epoch-millis corpus but declares the Nexmark `WATERMARK`
on its table (this ladder doesn't) — until native per-split source watermarks landed (2026-07-12,
divergences/25), that watermark silently kept the matrix's Kafka scans on Flink entirely, so matrix
Kafka cells of that period measured only the downstream island. Compare rungs within one harness
only, and verify the plan contains the native source before trusting a source rung.

**Reference — the transpose floor (no Kafka).** The same q0/q1/q2 with the source replaced by the
in-process `nexmark` datagen emitting `RowData` directly — no Kafka client, no format decode, just the
columnar island over a free source and `blackhole` sink (`-Dtest=NexmarkBenchmark`). The ceiling for
what columnar execution buys when I/O and decode are free:

| Query | Flink (RowData) | Native (JVM transpose, no decode) | speedup |
|---|---|---|---|
| q0 pass-through | 1.93 M ev/s | 2.11 M ev/s | **1.09×** |
| q1 currency | 1.76 M ev/s | 1.97 M ev/s | **1.12×** |
| q2 filter | 1.75 M ev/s | 2.84 M ev/s | **1.62×** |

Both engines run 2–3× faster in absolute ev/s than any Kafka rung — that gap is exactly the Kafka
consume + decode the ladder is about. The native speedup is pure columnar execution: modest on the
projections (transpose-bound) and large on the filter (native discards rows in Arrow before they are
ever materialized to `RowData`).

### The full accelerating set, every source

`NexmarkMatrixBenchmark` runs **every query StreamFusion accelerates** (q0–q5, q7–q23 — only q6 is out;
see [.claude/wontdos/39-nexmark-q6-exclusion.md](../.claude/wontdos/39-nexmark-q6-exclusion.md)) over **every
source it can be fed by** — the rowwise generator, a local Parquet file, and Kafka json/avro/protobuf
across the ladder — all vs stock Flink, same steelmanned perimeter. 500K events.

`SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features mimalloc,kafka"
-Dtest=NexmarkMatrixBenchmark` (Testcontainers Kafka; the Kafka test build enables its feature, and
`mimalloc` — the recommended build — rebinds the library's allocator, divergences/19). Column
toggles: `SF_MATRIX_GENERATOR` / `SF_MATRIX_PARQUET` / `SF_MATRIX_KAFKA` (`false` skips one), plus
`SF_MATRIX_FLUSS` (`true` *adds* the opt-in Fluss rung — off by default; see below).

The matrix runs with the native managed-memory cap **in force**: the shared test cluster declares a
deployment-like managed-memory size (flink-test-utils' default gave each slot ~10 MB, which the
accounted updating joins outgrow at 500K events; a real TaskManager's 40%-of-process managed memory
holds that state easily, so the benchmark cluster is sized to match). Reserving managed memory is
bookkeeping, not allocation — the budget costs nothing until state actually grows into it.

All the stateful operators run **columnar on Arrow byte-state**: Top-N, keep-last dedup, the updating
join, and the group/`DISTINCT` and windowed (tumbling/hopping/cumulative/session) aggregates key and
buffer their state as memcomparable arrow-row bytes (à la RisingWave's value-encoded state + Arroyo's
`RowConverter`), not boxed `Vec<ScalarValue>`.

### State-TTL fast path (2026-07-29)

The idle-state TTL work (`table.exec.state.ttl`, see `docs/coverage-and-fallbacks.md`) threads a
clock argument through every stateful ingest call and adds one predicted-false branch per key
touch when retention is off. A before/after A/B on the stateful generator rungs (2M events,
release build, min over two runs per side, baseline = the last pre-TTL commit) showed no
TTL-off regression: q3 +4.5%, q15 ±0%, q18 −4%, q19 +6% — all inside this rig's documented ±7%
noise band (q19's spread between two same-binary runs alone was 39%). With retention on, the
per-value timestamps add 8 bytes per stored value/entry and expiry adds a lazy check per key
touch plus a full sweep at most once per retention period.

### Current release matrix (2026-07-13)

Run with `SF_BENCHMARK=true SF_MATRIX_FLUSS=true mvn -pl :streamfusion-runtime test -Pbench
-Dnative.cargo.args="build --release --features mimalloc,kafka,parquet,json,csv,raw,avro,protobuf,fluss"
-Dtest=NexmarkMatrixBenchmark`. This is one combined 500K-event JVM run, best of two after a warmup;
both engines use object reuse, the default Flink configuration, and the same source bytes. Kafka reports
the complete native poll-and-decode rung rather than an intermediate best-of ladder rung.

| Query | Generator | Parquet | Fluss | Kafka JSON | Kafka Avro | Kafka Protobuf |
|---|---|---|---|---|---|---|
| q0 | **1.31×** | **3.25×** | **3.23×** | **2.70×** | **3.98×** | **2.57×** |
| q1 | **1.44×** | **3.23×** | **3.21×** | **2.58×** | **3.23×** | **2.52×** |
| q2 | **1.28×** | **3.09×** | **2.60×** | **2.22×** | **2.42×** | **2.01×** |
| q3 | 0.97× | **3.96×** | **2.03×** | **2.17×** | **2.28×** | **1.86×** |
| q4 | **1.50×** | **2.92×** | **1.56×** | **2.36×** | **3.16×** | **2.54×** |
| q5 | **1.24×** | **4.24×** | **2.29×** | **2.51×** | **3.62×** | **3.04×** |
| q7 | **1.61×** | **3.93×** | **3.41×** | **3.32×** | **3.61×** | **3.07×** |
| q8 | 0.84× | **4.61×** | **2.03×** | **1.95×** | **2.93×** | **2.55×** |
| q9 | **1.34×** | **1.74×** | **1.45×** | **2.07×** | **2.23×** | **1.91×** |
| q10 | **1.39×** | **4.69×** | **3.31×** | **3.00×** | **2.96×** | **2.58×** |
| q11 | **2.77×** | **5.43×** | **4.05×** | **4.07×** | **5.09×** | **5.28×** |
| q12 | **1.41×** | **4.36×** | — | **2.29×** | **2.55×** | **2.16×** |
| q13 | **1.25×** | **3.25×** | **2.13×** | **2.21×** | **2.56×** | **2.22×** |
| q14 | **1.06×** | **3.49×** | **2.50×** | **2.69×** | **3.16×** | **2.84×** |
| q15 | **1.61×** | **2.47×** | **1.37×** | **2.95×** | **3.00×** | **2.55×** |
| q16 | **1.36×** | **1.42×** | 0.98× | **1.82×** | **1.40×** | **1.44×** |
| q17 | **1.46×** | **2.00×** | **1.40×** | **2.71×** | **2.72×** | **2.13×** |
| q18 | **1.26×** | **2.22×** | **1.28×** | **2.62×** | **3.61×** | **2.94×** |
| q19 | **1.58×** | **1.58×** | **2.48×** | **2.00×** | **1.64×** | **1.96×** |
| q20 | 0.95× | **4.39×** | **2.25×** | **2.71×** | **3.62×** | **2.95×** |
| q21 | **1.01×** | **2.53×** | **2.12×** | **2.36×** | **2.88×** | **2.58×** |
| q21 † | **1.77×** | **6.17×** | **5.13×** | **2.39×** | **2.87×** | **2.49×** |
| q22 | **1.31×** | **4.74×** | **3.10×** | **2.61×** | **2.99×** | **2.50×** |
| q23 | **1.18×** | **4.38×** | **1.73×** | **2.11×** | **2.70×** | **2.34×** |

This table is one combined run taken after the 2026-07-12 hot-path round (batched BinaryRow key
encoding, the transpose's intrinsified string encode, the `DATE_FORMAT` digit renderer, and O(1)
accounted-state sizing — `docs/optimizations.md`), the in-poll driver-ABI Kafka decode
(divergences/25), and native per-split source watermarks. The last of these is what re-quoted the
Kafka columns: the matrix's Kafka table declares the canonical Nexmark watermark, which previously
kept its scans on Flink entirely — the earlier chart's near-parity Kafka cells were measuring
Flink's consume+decode with only the downstream island native. With the watermark regenerated
inside the native source, every Kafka cell wins, 1.40× (q16 Avro) to 5.28× (q11 Protobuf). The
generator column reads 20 of 23 wins; the trailers (q3/q8, with q20 just under parity) are the
perimeter-transpose/join-state cluster. All Parquet queries win (floor 1.42×, q16), and every
measurable Fluss cell but q16 (0.98×) is a win. `†` is the non-parity native regex/case path; the
default q21 remains the byte-parity JVM-upcall path.

### Historical matrix (2026-07-05)

The following tables are retained to show the previous fused-source measurements. They are not current
release claims; use the matrix above for current comparisons.

**Generator** (the transpose floor — no I/O, no decode), native vs Flink, sorted by speedup (q21 appears
twice — the byte-parity default and the opt-in native regex/case path, see † below):

| Query | Shape | Native vs. Flink |
|---|---|---|
| q11 | session-window `COUNT` per bidder | **2.79×** |
| q7 | tumble `MAX` ⋈ bid | **1.61×** |
| q12 | proctime tumble `COUNT` per bidder | **1.52×** |
| q19 | `ROW_NUMBER` topN (≤ 10) | **1.50×** |
| q5 | Hot Items (window re-agg + window join) | **1.47×** |
| q15 | multi-`DISTINCT` `COUNT`s per day | **1.42×** |
| q23 | three-way join `bid ⋈ person ⋈ auction` | **1.38×** |
| q16 | multi-`DISTINCT` per channel/day | **1.36×** |
| q0 | pass-through projection of `bid` | **1.33×** |
| q17 | group agg + `AVG`/`MIN`/`MAX`/`SUM` per day | **1.32×** |
| q4 | regular join → `MAX` → `AVG` per category | **1.31×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.30×** |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | **1.18×** |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.18×** |
| q1 | `0.908 * price` — exact `Decimal128` (byte-parity) | **1.13×** |
| q18 | `ROW_NUMBER` dedup (≤ 1) | **1.13×** |
| q10 | `DATE_FORMAT` projection | **1.11×** |
| q13 | lookup join (bounded dimension) | **1.07×** |
| q14 | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.02×** |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — JVM upcall (byte-parity) | 0.96× |
| q3 | updating join `auction ⋈ person` | 0.95× |
| q8 | tumble windowed-distinct ⋈ join | 0.87× |
| q20 | updating join (`category = 10`) | 0.84× |
| q21 † | …same, pure-native Rust regex/case (opt-in, non-parity) | **1.54×**

**Parquet file** — the columnar-source case: the native island reads Arrow straight from the
`filesystem`/`parquet` scan, so there is no `RowData → Arrow` transpose at ingest (only the sink
transpose remains). Same queries, sorted by speedup:

| Query | Native vs. Flink | | Query | Native vs. Flink |
|---|---|---|---|---|
| q11 | **5.39×** | | q12 | **3.23×** |
| q8 | **4.37×** | | q0 | **3.21×** |
| q7 | **4.22×** | | q1 | **3.07×** |
| q23 | **3.91×** | | q10 | **2.54×** |
| q4 | **3.61×** | | q18 | **2.27×** |
| q22 | **3.58×** | | q13 | **2.26×** |
| q3 | **3.57×** | | q17 | **2.23×** |
| q2 | **3.56×** | | q15 | **2.07×** |
| q5 | **3.45×** | | q9 | **1.94×** |
| q20 | **3.40×** | | q19 | **1.75×** |
| q14 | **3.30×** | | q16 | **1.37×** |
| q21 | **2.77×** (6.14× native regex/case) | | | |

Every query clears 1× — most **2–5.4×**, the floor q16 at 1.37× — because the ingest transpose is
gone: the scan feeds Arrow batches directly into the operator, and only the `blackhole` sink pays a
transpose. The queries that are transpose-bound on the generator (q8 at 0.87×, q3 at 0.95×, q20 at
0.84×) are exactly the ones that jump the most here (q8 4.37×, q3 3.57×, q20 3.40×) — confirming their
generator cost was the `RowData` perimeter, not the operator. Parquet's rowtime is a plain
`TIMESTAMP(3)`, so the `DATE_FORMAT`/`HOUR` queries (q10/q14/q15/q16/q17) run natively (over the Kafka
`TIMESTAMP_LTZ` they run natively too now — see the Kafka table's `§` note). q16 — long the one Parquet
query below 1× (its multi-`DISTINCT` accumulator churned `ScalarValue`) — cleared it when the
`mimalloc` build rebound the library's allocator, and again moved (1.10→1.34) when the DISTINCT sets
went typed and the state probes went borrowed-byte.

**Nineteen clear 1.0× even on this conservative combined run** (sixteen before the 2026-07 profiling
round, eighteen after its first pass — the differential flame-graph work recorded in
`.claude/research/nexmark-operator-profiles-2026-07.md`, whose shipped levers are itemized in
`docs/optimizations.md`: shared rowwise prefix under scoped sub-plan reuse, allocation-free state
probes across the join/aggregate/dedup/Top-N maps, typed DISTINCT sets + cached changelog emit,
decode-deduplicated Top-N emit, the transpose string single-copy, the lookup join's collect-time
Arrow writes, and the byte-path parity upcalls). The round's second pass measured its movers on the
75-second profile loop: **q21's parity path +12%** (the byte marshalling + primitive ASCII fold),
**q23 +8.5%**, **q18 +5.4%**, **q16 +3.4%**. The
window-aggregate queries moved earlier when the aggregators went to arrow-row keys and the session
update went run-batched (**q5 1.00→1.47, q8 0.70→0.87, q11 2.41→2.79** cumulatively). The
**updating-join family was the earlier big mover**: a CPU profile put ~40% of the worst query (q9)
in the joiner. Making the INNER join batch its whole input — gather all candidate pairs against the
fixed probe side, evaluate the residual predicate once columnar, emit by `filter_record_batch`, and
move rows into state instead of re-cloning — lifted **q9 0.39→0.97, q4 0.64→1.07, q7 0.91→1.37,
q23 0.66→0.96** at the time. The lever throughout was a differential profile's clearest signal — on
every changelog operator native spent 10–22% of CPU in the system allocator where Flink spends ~1%
(Flink reuses pooled `BinaryRowData`, its cost landing in GC). Cutting those allocations, not
swapping the allocator, closed the gap
([divergences/08](../divergences/08-columnar-flow-transitions.md)).

What still trails 1× on this rung: q8 is transpose-bound (a window join with only a ~9% native
island); q20 is the widest updating join (its state probes are allocation-free and its stored-row
decode no longer registers on the profile — the remainder is intrinsic hash-join work over the
rowwise perimeter, see wontdos/48); and q3 (0.95×) and q21's byte-parity upcall (0.96×) sit at the
line. q14 crossed it this run (1.02×); q13's lookup join,
long below 1×, cleared it when its collector started writing straight into the Arrow builders.

**† q21 is reported on both paths.** By default its `REGEXP_EXTRACT` and `LOWER` run through a
byte-identical **JVM upcall** (one JNI crossing per batch): the compile cost
is cached, the string boundary stays in UTF-8 bytes with a primitive ASCII fold, and the argument
columns marshal once per batch (0.75× → 0.86× → ~parity across the round; this combined run reads
0.96×, and the isolated 75s profile loop puts it above 1× — the upcall path is the most sensitive
to the combined run's accumulated GC pressure). The price of staying
exactly Flink-equal on functions whose Rust regex / case-folding can diverge at a locale/regex edge
is ~1.6× against the opt-in: `-Dstreamfusion.expression.allowIncompatible=true` runs the
**pure-native Rust** path at **1.54×**. Both are documented in
[divergences/07](../divergences/07-expression-encoding-and-compile-once.md).

**‡ q1's approximate-decimal toggle buys nothing.** The exact `Decimal128` multiply (byte-parity) is not
the bottleneck, so the approximate `double` path measures within noise of it (occasionally slower in a
combined run) — exact-by-default costs nothing and the non-parity toggle isn't worth enabling. Reported
as a single row.

**§ `DATE_FORMAT`/`HOUR` over the Kafka `TIMESTAMP_LTZ` now runs natively** (q10/q14/q15/q16/q17 — these
were skipped here before). The default routes the LTZ case through Flink's own zone-aware datetime code
via a JVM upcall (byte-parity); a pure-Rust `chrono-tz` path is opt-in under `allowIncompatible` but
measures within noise (the datetime call isn't the bottleneck), so parity is free — see
[divergences/17](../divergences/17-ltz-datetime-session-zone.md). Reported as a single row.

**Kafka**, the full native rdkafka source rung — after the consume fast path (divergences/19) it is
the best rung on **every row**, so the table reports it directly (native speedup vs that format's own
Flink baseline), sorted by the JSON speedup:

| Query | JSON | Avro | Protobuf |
|---|---|---|---|
| q11 | **3.93×** | **5.18×** | **5.55×** |
| q7 | **2.89×** | **4.11×** | **3.21×** |
| q15 § | **2.76×** | **3.06×** | **2.52×** |
| q0 | **2.71×** | **3.42×** | **2.58×** |
| q10 § | **2.69×** | **2.64×** | **2.26×** |
| q18 | **2.52×** | **3.02×** | **2.58×** |
| q22 | **2.49×** | **2.83×** | **2.28×** |
| q17 § | **2.49×** | **2.59×** | **2.23×** |
| q14 § | **2.47×** | **3.50×** | **2.66×** |
| q21 † | **2.46×** | **3.01×** | **2.62×** |
| q21 | **2.44×** | **2.98×** | **2.64×** |
| q4 | **2.43×** | **3.27×** | **2.66×** |
| q1 | **2.39×** | **3.35×** | **2.62×** |
| q20 | **2.38×** | **3.40×** | **2.92×** |
| q5 | **2.32×** | **3.35×** | **3.04×** |
| q12 | **2.31×** | **2.55×** | **2.14×** |
| q9 | **2.24×** | **2.13×** | **2.35×** |
| q8 | **2.22×** | **2.94×** | **2.58×** |
| q13 | **2.20×** | **2.75×** | **2.14×** |
| q23 | **2.09×** | **2.85×** | **2.38×** |
| q2 | **2.04×** | **2.48×** | **2.09×** |
| q19 | **1.98×** | **1.89×** | **1.85×** |
| q3 | **1.97×** | **2.38×** | **1.80×** |
| q16 § | **1.86×** | **1.87×** | **1.65×** |

**Historically, every Kafka row cleared 1.65×, all but a handful cleared 2×, and the peak was q11 at
3.9–5.6×.** These numbers include the former fused source's per-partition watermark regeneration (the matrix tables declare a
`WATERMARK`, pushed into the scan): windows fire incrementally mid-stream exactly as on stock Flink,
and the per-batch max-rowtime scan that feeds it costs nothing measurable. The same watermark work
collapses the two middle rungs on these tables — the decode rung declines a watermarked table (it
cannot regenerate the pushed watermark), so its per-rung numbers now equal the JVM-transpose rung's;
the un-watermarked ladder tables above are unaffected. An
earlier version of this table reported "best rung per format", because the source rung was capped by
a per-poll ceiling and the shallow decode (or even the JVM transpose) rung often led; the consume
fast path removed that ceiling and made the source rung strictly dominant — including for the
changelog-heavy queries (q9/q19) that previously gained nothing from faster decode, and
q3/q14/q18/q21, whose JSON rows were below 1× on their old best rung and now sit at ~2×+. The floor
of the table is q16 and the changelog-bound q3/q19 — operator-bound queries where the consume saving
is diluted, not reversed.

**Fluss** — the opt-in fourth source rung (`SF_MATRIX_FLUSS=true`), the columnar-on-the-wire
source: the same wide event row is preloaded into a local Fluss test cluster and read back by
both engines in the identical default streaming runtime — stock Flink-on-Fluss vs the native
fluss-rs log-table reader. Boundedness comes from a counting blackhole sink —
raw `RowData`, the same perimeter as the other rungs' `blackhole`, releasing the driver's latch
at the finish line — so each cell measures time-to-Nth-row (or time-to-marker) at `SF_ROWS`
scale. The native reader requires the `fluss` cargo
feature in the build, added alongside the recommended `mimalloc`: `SF_BENCHMARK=true
SF_MATRIX_FLUSS=true mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features
mimalloc,fluss" -Dtest=NexmarkMatrixBenchmark`. Building the `fluss` feature currently needs
`protoc` (`protobuf-compiler`) because fluss-rs generates its RPC protos at build time.

Because the log table is unbounded, the rung needs a deterministic Nth sink row to cancel at.
The benchmark table declares the generator's own 4s bounded-out-of-orderness `WATERMARK` (the
Fluss catalog persists it), so the windowed event-time queries run on both engines — Flink keeps
the watermark as an assigner node above the Fluss scan (no push-down, unlike Kafka), and that
assigner runs natively above the native source. A preloaded sentinel event (an `event_type`
outside 0..2 with a far-future rowtime, invisible to every view) advances the watermark past
every real window end, closing the same windows the bounded generator calibration's end-of-input
flush closes, so the counts line up. Three queries have no usable row count and use a **poison
marker** finish line instead: a traced copy of the preload appends one poison auction+bid pair
(ids outside every real range; the bid's channel is `apple`) after all real events, and the run
cancels when the pair's output row reaches the sink — in a parallelism-1 pipeline that row is
necessarily emitted after every real row, so time-to-marker measures the same full drain,
without a count:

- **q4/q9** — their two-input join feeds an update-collapsing aggregate/rank: Flink skips the
  `-U/+U` pair when an input row doesn't change the aggregate value, so the changelog row *count*
  depends on the join's input interleaving — non-deterministic even between two stock Flink runs
  (a 500K run calibrated 362,710 rows off the generator and observed 316,092 on Fluss, the job
  idle). Values and final state are identical; only the update cadence varies. The marker (q4:
  the poison category's aggregate row; q9: the poison auction's rank row) sidesteps the count
  entirely.
- **q21** — emits zero rows over this generator's data (its channels are `channel-N` and its
  URLs carry no `channel_id=`), so the poison bid's `apple` channel makes the marker row its
  first and only output.

One query skips: **q12** — a proctime window's output count is wall-clock-dependent, and any
marker's own window would close ~10s (the window size) after the drain, so a finish line would
time the window, not the engines. It stays measured on the bounded rungs, whose end-of-input
flush fires proctime windows immediately. Upstreaming `scan.bounded.mode` to Fluss
([issue #10](https://github.com/datafusion-contrib/StreamFusion/issues/10)) would retire the
count, sentinel, and marker machinery at once and admit q12.

Run of 2026-07-06 (500K events, best of 2 after a warmup, time-to-Nth-row / time-to-marker,
native vs the stock Fluss connector in the identical default streaming environment, both over
the watermarked table, both sinking to the counting blackhole), sorted by speedup:

| Query | Native vs. Flink-on-Fluss | Flink (ev/s) | Native (ev/s) |
|---|---|---|---|
| q11 | **4.02×** | 0.72 M | 2.89 M |
| q22 | **3.07×** | 0.99 M | 3.06 M |
| q2 | **2.77×** | 1.49 M | 4.12 M |
| q23 | **2.72×** | 0.59 M | 1.60 M |
| q1 ‡ | **2.65×** | 1.37 M | 3.64 M |
| q0 | **2.58×** | 1.38 M | 3.54 M |
| q10 § | **2.53×** | 1.17 M | 2.97 M |
| q14 § | **2.53×** | 1.37 M | 3.46 M |
| q7 | **2.46×** | 0.57 M | 1.41 M |
| q19 | **2.43×** | 0.37 M | 0.91 M |
| q5 | **2.25×** | 0.79 M | 1.77 M |
| q21 † | **2.14×** | 0.83 M | 1.77 M |
| q8 | **2.12×** | 0.93 M | 1.98 M |
| q13 | **2.11×** | 1.28 M | 2.70 M |
| q20 | **2.02×** | 0.83 M | 1.68 M |
| q18 | **1.88×** | 0.97 M | 1.84 M |
| q9 | **1.59×** | 0.58 M | 0.92 M |
| q3 | **1.55×** | 1.38 M | 2.14 M |
| q4 | **1.43×** | 0.65 M | 0.93 M |
| q17 § | **1.26×** | 0.96 M | 1.21 M |
| q16 § | 1.00× | 0.79 M | 0.79 M |
| q15 § | 0.98× | 0.94 M | 0.92 M |

**Twenty of twenty-two clear 1×, floor 0.98×.** An earlier quote of this table (same day, same
build) had the distinct-agg family at 0.78–0.85× and q19 at 0.97× — an artifact of the rung's
original sink: the count-to-N cancel ran through `toChangelogStream`, whose `TableToDataStream`
conversion turns every internal `RowData` into an external `Row` (boxing, UTF-8 decode,
`LocalDateTime` materialization). Both engines paid it equally, but a large shared perimeter
constant compresses every ratio toward 1× — worst exactly for the changelog-heavy queries that
emit ~2 sink rows per input row. Replacing it with the counting blackhole (raw `RowData`, the
same swallow as every other rung's `blackhole`, plus the latch) restored the rung's
comparability: q19 0.97×→2.43×, q23 1.41×→2.72×, q15 0.78×→0.98×, q16 0.85×→1.00×, q17
0.84×→1.26×. The profiled operator levers (the changelog aggregate's allocation churn and
`DATE_FORMAT`'s per-row formatting — `.claude/research/fluss-source-profile-2026-07.md`) remain
the path to push q15/q16 past the line.

The opt-in variants measure within noise of their byte-parity defaults on this rung — except
**† q21**, whose work is regex-dominated: the byte-parity JVM-upcall default reads **2.14×** and
the opt-in pure-Rust regex/case path **4.98×**, the honest cost of the parity guarantee (the
same split the Parquet rung shows at 2.77× vs 6.14×). q4/q9/q21 are the marker-measured cells.

The table's log rides Fluss's defaults, including ZSTD-compressed Arrow batches (as the Parquet
rung's file rides Flink's Snappy default) — both engines decode the same bytes. Turning
compression off is not a lever: with `'table.log.arrow.compression.type' = 'NONE'` q0 native
*drops* 2.83× → 2.18× (stock unchanged, q15 within noise), because fetches are byte-capped and
an uncompressed log needs ~4× the fetch round-trips for the same rows — the zstd decode the
profile shows is the price of fewer RPCs, not waste.

**The zero-transpose hypothesis holds.** The wire format is Arrow, so the native reader feeds
the island directly — no ingest transpose, no decode — and the stateless queries hit the highest
absolute native rates of any streaming rung (q2 at 4.1 M ev/s). The stock connector is itself
the strongest baseline of any rung (a lazy columnar read of the same Arrow log — its per-row
`ColumnarRow`→`RowData` conversion is the gap the stateless 2.5–3.1× measures), which is why
these ratios sit below Parquet's despite higher absolute rates: stock-on-Fluss is simply much
faster than stock-on-Parquet.

**Two masked native bugs surfaced the first time this rung ran unbounded** — worth recording
because every earlier rung was bounded, where the end-of-input `MAX_WATERMARK` flush forgives
mid-stream watermark mistakes:

1. **A missing sub-plan-reuse barrier on the Fluss scan.** Every native rel carries a digest
   barrier so Flink's post-optimize reuse can never merge two branches onto one columnar
   producer (the Arrow hand-off is zero-copy, single-consumer); the Fluss source node lacked
   one, so multi-view queries merged into one source broadcasting the same batch to two
   consumers — a use-after-free the watermark assigner turned into a hard crash.
2. **A shift-zone asymmetry in the window re-aggregation path (q5's shape).** Flink's rule:
   plain-`TIMESTAMP` rowtime windows compute on epoch millis with UTC digits; only
   `TIMESTAMP_LTZ` shifts boundaries into the session zone. The local window aggregate's exec
   node passed the session zone unconditionally, and its window-attached ingest (the only
   consumer of that zone) "un-shifted" boundaries that were never shifted — every re-aggregated
   window landed the session-zone offset in the future, where only a bounded run's final flush
   ever released it. Both engines' results were still identical on the bounded rungs, which is
   why parity never caught it; the unbounded rung is the first consumer of mid-stream firing
   for that shape.

### The tuned (mini-batch) matrix — the full suite

#### Current four-way comparison (2026-07-13)

The current comparison runs **the same 5M generator events** four ways: stock Flink and
StreamFusion, first with mini-batching disabled and then with
`table.exec.mini-batch.enabled=true`, `allow-latency=2s`, and `size=50000`. Each cell is the best
of two runs after one warmup, both engines use object reuse, and StreamFusion uses the release
`mimalloc` build. This is deliberately separate from the 500K multi-source matrix: using the same
5M input on both sides prevents fixed startup/JIT cost or the two-second latency boundary from
distorting the enabled-versus-disabled comparison.

`Flink on/off` and `SF on/off` measure what enabling mini-batching did to each engine itself; they
are distinct from the `SF/Flink` cross-engine ratios. A value below 1.00x means mini-batching made
that engine slower for that query.

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 1.952 M/s | 2.428 M/s | **1.24x** | 1.887 M/s | 2.170 M/s | **1.15x** | 0.97x | 0.89x |
| q1 | 1.930 M/s | 2.367 M/s | **1.23x** | 1.895 M/s | 2.255 M/s | **1.19x** | 0.98x | 0.95x |
| q2 | 2.071 M/s | 2.838 M/s | **1.37x** | 2.042 M/s | 2.781 M/s | **1.36x** | 0.99x | 0.98x |
| q3 | 2.800 M/s | 1.736 M/s | 0.62x | 2.817 M/s | 1.837 M/s | 0.65x | 1.01x | **1.06x** |
| q4 | 1.164 M/s | 1.448 M/s | **1.24x** | 0.600 M/s | 1.543 M/s | **2.57x** | 0.52x | **1.07x** |
| q5 | 1.378 M/s | 1.510 M/s | **1.10x** | 1.334 M/s | 1.541 M/s | **1.15x** | 0.97x | **1.02x** |
| q7 | 0.760 M/s | 0.767 M/s | **1.01x** | 0.519 M/s | 0.780 M/s | **1.50x** | 0.68x | **1.02x** |
| q8 | 2.852 M/s | 1.929 M/s | 0.68x | 2.838 M/s | 1.938 M/s | 0.68x | 0.99x | 1.00x |
| q9 | 0.753 M/s | 1.046 M/s | **1.39x** | 0.419 M/s | 1.015 M/s | **2.42x** | 0.56x | 0.97x |
| q10 | 1.369 M/s | 1.997 M/s | **1.46x** | 1.331 M/s | 1.924 M/s | **1.45x** | 0.97x | 0.96x |
| q11 | 0.880 M/s | 2.587 M/s | **2.94x** | 0.878 M/s | 2.520 M/s | **2.87x** | 1.00x | 0.97x |
| q12 | 1.723 M/s | 2.608 M/s | **1.51x** | 1.671 M/s | 2.604 M/s | **1.56x** | 0.97x | 1.00x |
| q13 | 1.670 M/s | 1.759 M/s | **1.05x** | 1.620 M/s | 1.751 M/s | **1.08x** | 0.97x | 1.00x |
| q14 | 1.890 M/s | 1.867 M/s | 0.99x | 1.793 M/s | 1.834 M/s | **1.02x** | 0.95x | 0.98x |
| q15 | 1.184 M/s | 1.901 M/s | **1.61x** | 1.153 M/s | 1.589 M/s | **1.38x** | 0.97x | 0.84x |
| q16 | 0.918 M/s | 1.082 M/s | **1.18x** | 0.752 M/s | 1.045 M/s | **1.39x** | 0.82x | 0.97x |
| q17 | 1.183 M/s | 1.678 M/s | **1.42x** | 1.089 M/s | 1.164 M/s | **1.07x** | 0.92x | 0.69x |
| q18 | 0.940 M/s | 1.560 M/s | **1.66x** | 0.580 M/s | 1.053 M/s | **1.81x** | 0.62x | 0.67x |
| q19 | 0.484 M/s | 0.723 M/s | **1.49x** | 0.477 M/s | 1.353 M/s | **2.84x** | 0.99x | **1.87x** |
| q20 | 1.062 M/s | 1.140 M/s | **1.07x** | 0.697 M/s | 1.128 M/s | **1.62x** | 0.66x | 0.99x |
| q21 | 0.958 M/s | 0.982 M/s | **1.02x** | 0.932 M/s | 0.983 M/s | **1.05x** | 0.97x | 1.00x |
| q22 | 1.223 M/s | 1.671 M/s | **1.37x** | 1.192 M/s | 1.698 M/s | **1.42x** | 0.97x | **1.02x** |
| q23 | 0.792 M/s | 1.277 M/s | **1.61x** | 0.434 M/s | 1.366 M/s | **3.14x** | 0.55x | **1.07x** |

The largest direct StreamFusion mini-batch gain is q19's retracting Top-N: **1.87x** over its own
disabled path. q4 and q23 improve by 7%, and q3 by 6%. The cross-engine lead widens much more on
q4/q7/q9/q20/q23 because stock Flink's enabled plans slow down substantially while StreamFusion is
roughly flat or slightly faster; that is a real steelman result, but it must not be described as a
2-3x direct StreamFusion mini-batch speedup. Conversely, q15/q17/q18 regress on StreamFusion when
enabled (0.84x/0.69x/0.67x). Those plan families need profiling before claiming that mini-batching
helps them end to end, despite their enabled StreamFusion/Flink ratios remaining above 1x.

A focused q17 rerun after vectorizing the two-phase local aggregate's key path (`857074f`) measured
1.691 M/s with mini-batching off and 1.436 M/s on, versus the preceding balanced focused run's
1.662/1.149 M/s. Thus the optimization improved the enabled path by about 25% without moving the
disabled path; q17's direct mini-batch ratio narrowed from 0.69x to 0.85x. This focused follow-up does
not rewrite the full matrix above, whose cells remain one contemporaneous run.

q15, which shares the vectorized two-phase local aggregate, measured 1.824 M/s off and 1.857 M/s on
in its focused follow-up. Its enabled path improved about 21% from the preceding balanced 1.535 M/s,
turning the direct mini-batch ratio from 0.81x into 1.02x; enabled StreamFusion was 1.64x Flink.

The analogous focused q18 rerun after replacing its dedup endpoint map with incremental vector
staging (`dc35fb8`) measured 1.495 M/s off and 1.289 M/s on, versus the preceding balanced focused
run's 1.403/1.055 M/s. The enabled path improved about 22%, its direct mini-batch ratio moved from
0.75x to 0.86x, and its enabled StreamFusion/Flink lead measured 2.09x. As with q17, this is a focused
follow-up rather than a rewrite of the contemporaneous full matrix.

Reproduce both halves in one JVM with:

`SF_BENCHMARK=true SF_MATRIX_TUNED=true SF_ROWS=5000000 SF_MATRIX_GENERATOR=true
SF_MATRIX_PARQUET=false SF_MATRIX_KAFKA=false SF_MATRIX_FLUSS=false
SF_MATRIX_QUERIES=q0,q1,q2,q3,q4,q5,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22,q23
mvn -pl :streamfusion-runtime test -Pbench
-Dnative.cargo.args="build --release --features mimalloc" -Dtest=NexmarkMatrixBenchmark`.

### Exactly-once Kafka output matrix

`NexmarkMatrixBenchmark#exactlyOnceKafkaSinkModeComparison` replaces the counting blackhole with a
real exactly-once Kafka sink while retaining the same Kafka JSON input. Stock Flink and
StreamFusion therefore read from and publish to the same test broker. The harness runs both engines
with mini-batching disabled and with the same 2s/50,000-row configuration enabled, alternating mode
order per query. Append-only queries use `kafka`; q4, q9, and q15-q19 use `upsert-kafka` with the
query result's actual unique key. Every measured run uses a fresh topic and transactional ID, waits
for the bounded job's final commit, then removes the output topic outside the timed interval. On
StreamFusion the whole sink data plane is native since 2026-07-19: librdkafka serializes and
produces each Arrow batch inside the checkpoint epoch's transaction, and Flink's stock Java
committer commits it after the checkpoint completes (divergence 26); the harness asserts the
native-producer plan shape for every cell.

#### Current full comparison (2026-07-28 evening — shared native sources)

Same rig and method as the table below (parallelism 4, 2M events, four-partition source and
output topics, release+`mimalloc` full feature build, best of two with no warmup); the one
change is shared native sources: a query whose branches scan the same topic now reads and
decodes it once, where each branch previously ran its own full native source (Flink's plans
always shared their scan). Throughput is millions of input events per second.

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 0.663 | 0.952 | 1.44x | 0.879 | 1.253 | 1.43x | 1.33x | 1.32x |
| q1 | 0.830 | 1.511 | 1.82x | 0.833 | 1.359 | 1.63x | 1.00x | 0.90x |
| q2 | 1.413 | 1.904 | 1.35x | 1.441 | 1.918 | 1.33x | 1.02x | 1.01x |
| q3 | 1.611 | 2.244 | 1.39x | 1.424 | 2.308 | 1.62x | 0.88x | 1.03x |
| q4 | 0.836 | 1.213 | 1.45x | 0.952 | 1.740 | 1.83x | 1.14x | 1.43x |
| q5 | 1.231 | 1.948 | 1.58x | 1.240 | 1.723 | 1.39x | 1.01x | 0.88x |
| q7 | 0.949 | 1.315 | 1.39x | 0.698 | 1.554 | 2.23x | 0.74x | 1.18x |
| q8 | 1.446 | 1.647 | 1.14x | 1.584 | 2.364 | 1.49x | 1.09x | 1.44x |
| q9 | 0.758 | 0.980 | 1.29x | 0.704 | 1.271 | 1.80x | 0.93x | 1.30x |
| q10 | 0.820 | 1.048 | 1.28x | 0.822 | 0.988 | 1.20x | 1.00x | 0.94x |
| q11 | 0.979 | 2.648 | 2.70x | 0.934 | 2.673 | 2.86x | 0.95x | 1.01x |
| q12 | 1.339 | 2.596 | 1.94x | 1.416 | 2.559 | 1.81x | 1.06x | 0.99x |
| q13 | 1.033 | 1.327 | 1.28x | 0.918 | 1.150 | 1.25x | 0.89x | 0.87x |
| q14 | 0.807 | 1.211 | 1.50x | 0.787 | 1.181 | 1.50x | 0.98x | 0.97x |
| q15 | 0.280 | 0.860 | 3.07x | 1.254 | 1.955 | 1.56x | 4.47x | 2.27x |
| q16 | 0.490 | 0.795 | 1.62x | 1.097 | 1.673 | 1.52x | 2.24x | 2.10x |
| q17 | 0.768 | 1.042 | 1.36x | 1.165 | 1.636 | 1.40x | 1.52x | 1.57x |
| q18 | 0.724 | 0.974 | 1.35x | 0.601 | 1.001 | 1.67x | 0.83x | 1.03x |
| q19 | 0.274 | 0.351 | 1.28x | 0.287 | 0.598 | 2.08x | 1.05x | 1.70x |
| q20 | 1.032 | 1.206 | 1.17x | 0.997 | 1.730 | 1.74x | 0.97x | 1.43x |
| q21 | 1.339 | 1.842 | 1.38x | 1.286 | 1.241 | 0.96x | 0.96x | 0.67x |
| q22 | 0.849 | 1.232 | 1.45x | 0.850 | 1.102 | 1.30x | 1.00x | 0.89x |
| q23 | 0.333 | 0.615 | 1.85x | 0.315 | 0.643 | 2.04x | 0.95x | 1.05x |

23 of 23 wins with mini-batching disabled (geometric mean **1.52x**) and 22 of 23 enabled
(geometric mean **1.59x**), up from 1.42x/1.39x before source sharing. The moves are exactly
the multi-view queries: q3 — formerly the one consistent loss, and really a doubled topic
read — goes 0.90x -> 1.39x off and 0.86x -> 1.62x on; q8 on-mode recovers from 0.75x to 1.49x;
q4/q9/q20 stop paying the duplicate ingest. q21 mini-batch-on remains the single sub-parity
cell (0.96x).

#### Prior comparison (2026-07-28 — four-partition output topics, before shared sources)

Apple M1 Max, one-second checkpoints, release+`mimalloc` (full feature build), best of two
measured runs (no warmup — the best-of minimum discards a cold run); throughput is millions of
input events per second. Both engines run at parallelism 4 over a 2M-event corpus on a
four-partition topic (one split per source subtask; round-robin production keeps each partition
ascending in event time), and every exactly-once output topic is pre-created with one partition
per sink subtask. These are the first tables to include post-exchange coalescing (see the
scaling analysis below) and the multi-partition sink. The native side's co-located shuffle hands
Arrow batches over by ownership (the zero-copy local exchange); a multi-TaskManager deployment's
shuffle pays Arrow IPC instead (`streamfusion.exchange.zeroCopyLocal=false` models it, measured
~11% on the shuffle-heaviest mini-batch-off cells and nothing elsewhere).

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 0.634 | 1.003 | 1.58x | 0.778 | 0.979 | 1.26x | 1.23x | 0.98x |
| q1 | 0.757 | 1.255 | 1.66x | 0.618 | 1.151 | 1.86x | 0.82x | 0.92x |
| q2 | 1.189 | 1.315 | 1.11x | 1.342 | 1.723 | 1.28x | 1.13x | 1.31x |
| q3 | 1.536 | 1.386 | 0.90x | 1.391 | 1.190 | 0.86x | 0.91x | 0.86x |
| q4 | 0.827 | 0.986 | 1.19x | 0.872 | 0.959 | 1.10x | 1.06x | 0.97x |
| q5 | 1.143 | 1.260 | 1.10x | 1.169 | 1.212 | 1.04x | 1.02x | 0.96x |
| q7 | 0.815 | 1.063 | 1.30x | 0.622 | 1.084 | 1.74x | 0.76x | 1.02x |
| q8 | 1.402 | 1.478 | 1.05x | 1.409 | 1.053 | 0.75x | 1.00x | 0.71x |
| q9 | 0.645 | 0.824 | 1.28x | 0.659 | 0.988 | 1.50x | 1.02x | 1.20x |
| q10 | 0.712 | 1.013 | 1.42x | 0.742 | 1.006 | 1.36x | 1.04x | 0.99x |
| q11 | 0.797 | 2.181 | 2.73x | 0.889 | 2.035 | 2.29x | 1.11x | 0.93x |
| q12 | 1.258 | 2.272 | 1.81x | 1.242 | 2.240 | 1.80x | 0.99x | 0.99x |
| q13 | 0.985 | 1.320 | 1.34x | 0.965 | 1.371 | 1.42x | 0.98x | 1.04x |
| q14 | 0.725 | 1.210 | 1.67x | 0.724 | 1.148 | 1.59x | 1.00x | 0.95x |
| q15 | 0.261 | 0.797 | 3.05x | 1.091 | 1.754 | 1.61x | 4.17x | 2.20x |
| q16 | 0.445 | 0.757 | 1.70x | 1.017 | 1.514 | 1.49x | 2.29x | 2.00x |
| q17 | 0.683 | 0.841 | 1.23x | 1.090 | 1.359 | 1.25x | 1.60x | 1.62x |
| q18 | 0.576 | 0.946 | 1.64x | 0.534 | 0.823 | 1.54x | 0.93x | 0.87x |
| q19 | 0.225 | 0.297 | 1.32x | 0.231 | 0.639 | 2.76x | 1.03x | 2.15x |
| q20 | 1.005 | 0.978 | 0.97x | 0.840 | 0.834 | 0.99x | 0.84x | 0.85x |
| q21 | 1.229 | 1.431 | 1.16x | 1.268 | 1.110 | 0.88x | 1.03x | 0.78x |
| q22 | 0.810 | 1.245 | 1.54x | 0.824 | 1.227 | 1.49x | 1.02x | 0.99x |
| q23 | 0.214 | 0.310 | 1.45x | 0.198 | 0.298 | 1.51x | 0.92x | 0.96x |

21 of 23 wins with mini-batching disabled (geometric mean **1.42x**) and 19 of 23 enabled
(geometric mean **1.39x**). Against the 2026-07-27 tables below, the single-partition output
topics were the larger distortion: they throttled Flink's sink harder than the native one, so
un-capping them raised Flink's baselines and brought several ratios down even as coalescing
raised native throughput — the two changes move exactly the cells the scaling analysis
predicted (q4 off 0.96x -> 1.19x, q19 off 0.68x -> 1.32x and stable, q3 settling at its real
0.90x rather than a load-skewed 0.64x). q3 (a plain updating join) is the one consistent loss;
q8/q21 mini-batch-on hover just below parity.

#### Prior comparison (2026-07-27 night — single-partition output topics, before post-exchange coalescing)

Apple M1 Max, one-second checkpoints, release+`mimalloc,kafka,json`, one warmup and best of two
measured runs; throughput is millions of input events per second. Both engines run at
parallelism 4 over a 2M-event corpus on a four-partition topic (one split per source subtask;
round-robin production keeps each partition ascending in event time). The native side's
co-located shuffle hands Arrow batches over by ownership (the zero-copy local exchange); a
multi-TaskManager deployment's shuffle pays Arrow IPC instead
(`streamfusion.exchange.zeroCopyLocal=false` models it, measured ~11% on the shuffle-heaviest
mini-batch-off cells and nothing elsewhere).

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 0.763 | 1.435 | 1.88x | 0.828 | 0.759 | 0.92x | 1.09x | 0.53x |
| q1 | 0.861 | 1.087 | 1.26x | 0.723 | 0.817 | 1.13x | 0.84x | 0.75x |
| q2 | 1.367 | 2.186 | 1.60x | 1.318 | 1.330 | 1.01x | 0.96x | 0.61x |
| q3 | 1.715 | 1.102 | 0.64x † | 1.671 | 1.555 | 0.93x † | 0.97x | 1.41x |
| q4 | 1.034 | 0.993 | 0.96x | 0.999 | 1.377 | 1.38x | 0.97x | 1.39x |
| q5 | 1.191 | 1.594 | 1.34x | 1.403 | 1.476 | 1.05x | 1.18x | 0.93x |
| q7 | 0.890 | 1.423 | 1.60x | 0.826 | 1.315 | 1.59x | 0.93x | 0.92x |
| q8 | 1.593 | 2.039 | 1.28x | 1.637 | 1.881 | 1.15x | 1.03x | 0.92x |
| q9 | 0.718 | 1.202 | 1.67x | 0.674 | 1.254 | 1.86x | 0.94x | 1.04x |
| q10 | 0.785 | 1.312 | 1.67x | 0.738 | 1.001 | 1.36x | 0.94x | 0.76x |
| q11 | 0.981 | 2.612 | 2.66x | 0.977 | 2.615 | 2.68x | 1.00x | 1.00x |
| q12 | 1.438 | 2.457 | 1.71x | 1.336 | 2.266 | 1.70x | 0.93x | 0.92x |
| q13 | 0.997 | 1.646 | 1.65x | 1.004 | 1.605 | 1.60x | 1.01x | 0.98x |
| q14 | 0.747 | 1.415 | 1.89x | 0.759 | 1.263 | 1.66x | 1.02x | 0.89x |
| q15 | 0.307 | 0.936 | 3.05x | 1.279 | 2.235 | 1.75x | 4.16x | 2.39x |
| q16 | 0.418 | 0.673 | 1.61x | 1.070 | 1.880 | 1.76x | 2.56x | 2.79x |
| q17 | 0.586 | 1.105 | 1.89x | 1.103 | 1.936 | 1.76x | 1.88x | 1.75x |
| q18 | 0.557 | 0.904 | 1.62x | 0.561 | 1.103 | 1.96x | 1.01x | 1.22x |
| q19 | 0.138 | 0.094 | 0.68x | 0.189 | 0.744 | 3.94x | 1.37x | 7.90x |
| q20 | 0.327 | 1.192 | 3.65x | 0.887 | 1.263 | 1.42x | 2.72x | 1.06x |
| q21 | 1.369 | 2.100 | 1.53x | 1.322 | 1.879 | 1.42x | 0.97x | 0.89x |
| q22 | 0.826 | 1.362 | 1.65x | 0.848 | 1.071 | 1.26x | 1.03x | 0.79x |
| q23 | 0.299 | 0.544 | 1.82x | 0.310 | 0.586 | 1.89x | 1.04x | 1.08x |

† q3's suite cells are load anomalies: a focused repeat directly after the suite, in its own JVM,
measured **1.06x off and 1.06x on** (native 1.564 / 1.667 M events/s) — a modest win, not the
suite's loss, though still far below its parallelism-1 ratio. q19's off cell could not be
re-verified in isolation: two of three focused repeat attempts died mid-q19 with the MiniCluster's
TaskExecutor shutting down (the full suite pass survived it), so 0.68x stands as measured and the
instability is part of the q19 scaling finding.

Geometric means: mini-batch off **1.59x** as measured (**1.63x** with the q3 repeat
substituted), mini-batch on **1.52x** (**1.53x**). The parallelism-1 story compresses at
parallelism 4: Flink's heap pipeline gains more from quadrupled subtasks than the native island
does on the shuffle-heavy changelog shapes, so q4 off sits at parity and q19 off below it, and
q3 hovers near parity even isolated — a scaling gap whose root cause is measured (see below).
The suite's mini-batch-on cells for the stateless queries (q0 0.92x, q2 1.01x, q1 1.13x) read
low against their own off-mode; clean-machine replications of q0 show on/off parity (0.93–1.09x
across independent runs), so those on-cells carry the same sustained-load skew the q3 † repeat
caught, not a native mini-batch cost. Mini-batching still pays off handsomely where changelog
churn amortizes (q19 on 3.94x). The windowed
family (q15–q17) shows both engines gaining from mini-batch mode's two-phase aggregation, native
keeping a 1.6–1.9x edge throughout.

#### Why the off-mode changelog shapes stop scaling (measured 2026-07-28)

Native q4 is flat across parallelism even to a blackhole sink (1.7–1.9 s at p=1, 2, and 4 over
2M events) while stock Flink halves its time — and with the exactly-once sink both engines
converge at ~1.0 M events/s, a shared sink/broker ceiling stacked on top. Stage accounting
pinned the island-internal mechanism to **batch collapse against per-batch fixed cost**: a
p=4 subtask's consumer drains only its own partition's in-flight fetch per poll (~950 rows vs
~4,600 when p=1's single consumer aggregates four partitions), the keyed split quarters that,
and the second aggregate — fed by the first's ~100-row changelog outputs split again — processes
**26-row batches, ~71× the batch count of p=1**, so the ~4-crossing JNI fixed cost per batch
dominates. A wall-clock profile agrees: every task thread is starved (5–37% busy, zero
network-buffer backpressure), columnar compute is ~2.7% of on-CPU time, and librdkafka serve
loops dominate what CPU is used. Ruled out by probes: cross-partition watermark skew (a
one-partition corpus at p=4 is *slower*), the poll's `maxRecords` cap (not binding), and broker
saturation (25–65% of one core). The fix direction is coalescing: mini-batch mode already
recovers q4 to 1.38x by bundling at the operators (Arroyo runs its updating aggregates this way
unconditionally, with a 1 s flush default — per-batch changelog emission has no analog there),
and a latency-bounded source-side floor (accumulate across poll cycles before emitting a batch)
would lift the off-mode path without changing semantics.

**Resolved in part (2026-07-28): post-exchange coalescing.** Every keyed changelog operator now
re-assembles processing-sized batches in front of its native push: sub-batches buffer until a row
target (`streamfusion.exchange.coalesceRows`, default 4096), merge in one native call, and flush
on watermark, checkpoint barrier, end of input, or a processing-time backstop
(`streamfusion.exchange.coalesceLatencyMs`, default 50 ms) — physical chunking only; the
per-record changelog is byte-identical. Interleaved same-binary A/B on this 2M/p=4 blackhole
ladder (coalescing on vs `coalesceRows=0`, two legs per side): **q4 full-native 1.95 → 1.23 s
json (1.59×), 1.39 → 0.96 s avro (1.45×), 1.72 → 0.84 s protobuf (2.06×)** — q4 vs Flink moves
from ~1.06× to **1.78×** on json — and the row-fed q4 variants gain ~2.5× (5.5 → 2.2 s). q3 and
q19 sit within cross-leg noise: the collapse compounds through q4's two-aggregate chain, and
that compounding is what merging removes. The source-side batch floor (a p=4 consumer's ~950-row
polls, quartered by the split before the first operator) remains the open lever; a
cross-poll-cycle accumulator is the direction.

#### Prior comparison (2026-07-27, parallelism 1, 500K events — raw-bytes snapshots + sink encode round)

Apple M1 Max, 500K input events, one-second checkpoints, release+`mimalloc,kafka,json`, one warmup
and best of two measured runs. Throughput is millions of input events per second. This run
includes the day's five perf commits: the incremental Paimon checkpoint listing, the sink
zero-copy + bulk-escape encode, and raw-bytes state snapshots for the updating join, both Top-N
rankers, keep-last dedup, and the changelog normalizer.

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 0.229 | 0.495 | 2.16x | 0.242 | 0.490 | 2.03x | 1.05x | 0.99x |
| q1 | 0.237 | 0.459 | 1.93x | 0.240 | 0.491 | 2.05x | 1.01x | 1.07x |
| q2 | 0.371 | 0.471 | 1.27x | 0.331 | 0.443 | 1.34x | 0.89x | 0.94x |
| q3 | 0.471 | 0.703 | 1.49x | 0.439 | 0.686 | 1.56x | 0.93x | 0.98x |
| q4 | 0.314 | 0.626 | 2.00x | 0.294 | 0.572 | 1.94x | 0.94x | 0.91x |
| q5 | 0.400 | 0.632 | 1.58x | 0.409 | 0.570 | 1.40x | 1.02x | 0.90x |
| q7 | 0.263 | 0.676 | 2.57x | 0.236 | 0.679 | 2.88x | 0.90x | 1.01x |
| q8 | 0.465 | 0.736 | 1.58x | 0.448 | 0.590 | 1.32x | 0.96x | 0.80x |
| q9 | 0.257 | 0.226 | 0.88x | 0.235 | 0.402 | 1.71x | 0.92x | 1.78x |
| q10 | 0.223 | 0.406 | 1.82x | 0.220 | 0.487 | 2.22x | 0.98x | 1.20x |
| q11 | 0.249 | 0.701 | 2.82x | 0.244 | 0.711 | 2.91x | 0.98x | 1.01x |
| q12 | 0.366 | 0.638 | 1.74x | 0.400 | 0.711 | 1.78x | 1.09x | 1.11x |
| q13 | 0.284 | 0.623 | 2.19x | 0.273 | 0.516 | 1.89x | 0.96x | 0.83x |
| q14 | 0.222 | 0.444 | 2.00x | 0.215 | 0.493 | 2.29x | 0.97x | 1.11x |
| q15 | 0.217 | 0.372 | 1.71x | 0.344 | 0.610 | 1.77x | 1.58x | 1.64x |
| q16 | 0.197 | 0.300 | 1.52x | 0.285 | 0.531 | 1.86x | 1.45x | 1.77x |
| q17 | 0.237 | 0.397 | 1.68x | 0.325 | 0.577 | 1.78x | 1.37x | 1.45x |
| q18 | 0.215 | 0.484 | 2.25x | 0.180 | 0.450 | 2.51x | 0.84x | 0.93x |
| q19 | 0.055 | 0.076 | 1.39x | 0.059 | 0.566 | 9.55x | 1.08x | 7.42x |
| q20 | 0.270 | 0.674 | 2.50x | 0.255 | 0.628 | 2.46x | 0.94x | 0.93x |
| q21 | 0.360 | 0.532 | 1.48x | 0.330 | 0.614 | 1.86x | 0.92x | 1.15x |
| q22 | 0.260 | 0.534 | 2.06x | 0.252 | 0.535 | 2.12x | 0.97x | 1.00x |
| q23 | 0.135 | 0.168 | 1.25x | 0.055 | 0.144 | 2.63x | 0.41x | 0.85x |

22 of 23 wins with mini-batching disabled (geometric mean **1.76x**; q9 at 0.88x is the sole
loss, bound by its synchronous state-snapshot copy on the barrier) and 23 of 23 enabled
(geometric mean **2.11x**, 1.29–9.55x). Against the prior table the day's snapshot work moved
exactly the state-heavy cells: q18 off 1.50x→2.25x (keep-last dedup), q19 off 1.07x→1.39x,
q20 off 1.92x→2.50x, and q9 off 0.86x→0.88x with its profile-loop average gap closing 24%→8%
(best-of-2 under-resolves a mean shift; the loop counts are in `docs/optimizations.md`).

#### Prior comparison (2026-07-19, superseded — decoded-column snapshots, per-record sink copy)

Same machine, method, and build flags as above; before the raw-bytes snapshot formats and the
sink encode round.

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 0.228 | 0.473 | 2.07x | 0.242 | 0.482 | 1.99x | 1.06x | 1.02x |
| q1 | 0.241 | 0.536 | 2.22x | 0.239 | 0.489 | 2.04x | 0.99x | 0.91x |
| q2 | 0.359 | 0.531 | 1.48x | 0.330 | 0.522 | 1.58x | 0.92x | 0.98x |
| q3 | 0.466 | 0.702 | 1.51x | 0.441 | 0.700 | 1.59x | 0.95x | 1.00x |
| q4 | 0.336 | 0.572 | 1.71x | 0.303 | 0.539 | 1.78x | 0.90x | 0.94x |
| q5 | 0.396 | 0.634 | 1.60x | 0.409 | 0.568 | 1.39x | 1.03x | 0.90x |
| q7 | 0.275 | 0.637 | 2.31x | 0.235 | 0.629 | 2.68x | 0.85x | 0.99x |
| q8 | 0.444 | 0.641 | 1.44x | 0.448 | 0.635 | 1.42x | 1.01x | 0.99x |
| q9 | 0.274 | 0.235 | 0.86x | 0.232 | 0.477 | 2.06x | 0.85x | 2.03x |
| q10 | 0.229 | 0.431 | 1.88x | 0.232 | 0.447 | 1.93x | 1.01x | 1.04x |
| q11 | 0.242 | 0.685 | 2.84x | 0.240 | 0.812 | 3.39x | 0.99x | 1.19x |
| q12 | 0.429 | 0.693 | 1.62x | 0.413 | 0.408 | 0.99x | 0.96x | 0.59x |
| q13 | 0.287 | 0.537 | 1.87x | 0.275 | 0.545 | 1.98x | 0.96x | 1.01x |
| q14 | 0.226 | 0.534 | 2.36x | 0.224 | 0.497 | 2.22x | 0.99x | 0.93x |
| q15 | 0.214 | 0.432 | 2.02x | 0.358 | 0.634 | 1.77x | 1.67x | 1.47x |
| q16 | 0.197 | 0.262 | 1.33x | 0.265 | 0.346 | 1.30x | 1.35x | 1.32x |
| q17 | 0.238 | 0.346 | 1.45x | 0.352 | 0.503 | 1.43x | 1.48x | 1.46x |
| q18 | 0.221 | 0.331 | 1.50x | 0.182 | 0.309 | 1.70x | 0.82x | 0.93x |
| q19 | 0.058 | 0.061 | 1.07x | 0.060 | 0.486 | 8.15x | 1.03x | 7.92x |
| q20 | 0.296 | 0.569 | 1.92x | 0.260 | 0.506 | 1.94x | 0.88x | 0.89x |
| q21 | 0.362 | 0.480 | 1.33x | 0.349 | 0.590 | 1.69x | 0.96x | 1.23x |
| q22 | 0.267 | 0.519 | 1.95x | 0.263 | 0.472 | 1.79x | 0.99x | 0.91x |
| q23 | 0.145 | 0.193 | 1.33x | 0.155 | 0.194 | 1.25x | 1.07x | 1.00x |

StreamFusion wins 22 of 23 with mini-batching disabled (1.07x–2.84x; q9 is the one loss) and, with
the q12 cell re-verified below, 23 of 23 enabled (1.25x–8.15x). Off-mode is the headline change
from the prior comparison: queries that previously lost without mini-batching now win outright
(q15 1.21x→2.02x, q18 0.99x→1.50x, q19 0.58x→1.07x, q23 0.83x→1.33x) and the record-heavy
passthroughs jumped (q0 1.51x→2.07x, q1 1.65x→2.22x, q14 1.45x→2.36x), because the native producer
out-drains the Java client once its request batching is left at librdkafka's own geometry (see the
producer-parity entries in `docs/optimizations.md`).

A same-day focused repeat of the three cells that looked anomalous in the complete run:

| Query | SF/Flink off | SF/Flink on |
|---|---:|---:|
| q9 | 0.94x | 1.86x |
| q12 | 1.78x | 1.49x |
| q21 | 1.42x | 1.46x |

q12's complete-run 0.99x enabled cell does not reproduce (1.49x on repeat — the anomaly was one
slow StreamFusion measurement); q21 reproduces at 1.33–1.46x, genuinely below its prior 2.07x but
a stable win; q9 off hovers at 0.86–0.94x, the one query still under parity without mini-batching.

#### Prior comparison (2026-07-16, superseded — encode-only native, Java producer data plane)

The table below predates the native exactly-once producer: StreamFusion serialized natively but
Flink's Java `KafkaSink` owned all record production and transactions. Kept for the delta the
producer swap produced. Same machine, method, and build flags.

| Query | Flink off | StreamFusion off | SF/Flink off | Flink on | StreamFusion on | SF/Flink on | Flink on/off | SF on/off |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q0 | 0.211 | 0.320 | 1.51x | 0.233 | 0.281 | 1.20x | 1.10x | 0.88x |
| q1 | 0.196 | 0.324 | 1.65x | 0.190 | 0.270 | 1.42x | 0.97x | 0.83x |
| q2 | 0.324 | 0.534 | 1.65x | 0.324 | 0.540 | 1.67x | 1.00x | 1.01x |
| q3 | 0.438 | 0.630 | 1.44x | 0.429 | 0.623 | 1.45x | 0.98x | 0.99x |
| q4 | 0.285 | 0.491 | 1.72x | 0.286 | 0.579 | 2.02x | 1.00x | 1.18x |
| q5 | 0.403 | 0.592 | 1.47x | 0.394 | 0.538 | 1.37x | 0.98x | 0.91x |
| q7 | 0.265 | 0.672 | 2.54x | 0.236 | 0.565 | 2.40x | 0.89x | 0.84x |
| q8 | 0.437 | 0.572 | 1.31x | 0.443 | 0.565 | 1.28x | 1.01x | 0.99x |
| q9 | 0.233 | 0.128 | 0.55x | 0.210 | 0.372 | 1.77x | 0.90x | 2.91x |
| q10 | 0.210 | 0.290 | 1.38x | 0.202 | 0.229 | 1.14x | 0.96x | 0.79x |
| q11 | 0.252 | 0.661 | 2.62x | 0.259 | 0.675 | 2.61x | 1.03x | 1.02x |
| q12 | 0.361 | 0.557 | 1.54x | 0.364 | 0.560 | 1.54x | 1.01x | 1.00x |
| q13 | 0.268 | 0.394 | 1.47x | 0.259 | 0.370 | 1.43x | 0.97x | 0.94x |
| q14 | 0.221 | 0.320 | 1.45x | 0.225 | 0.323 | 1.43x | 1.02x | 1.01x |
| q15 | 0.157 | 0.190 | 1.21x | 0.331 | 0.587 | 1.77x | 2.11x | 3.08x |
| q16 | 0.122 | 0.173 | 1.42x | 0.256 | 0.481 | 1.88x | 2.09x | 2.78x |
| q17 | 0.197 | 0.200 | 1.02x | 0.314 | 0.478 | 1.52x | 1.60x | 2.39x |
| q18 | 0.204 | 0.201 | 0.99x | 0.167 | 0.193 | 1.15x | 0.82x | 0.96x |
| q19 | 0.053 | 0.031 | 0.58x | 0.043 | 0.238 | 5.49x | 0.82x | 7.73x |
| q20 | 0.301 | 0.467 | 1.55x | 0.261 | 0.458 | 1.76x | 0.87x | 0.98x |
| q21 | 0.335 | 0.693 | 2.07x | 0.342 | 0.767 | 2.24x | 1.02x | 1.11x |
| q22 | 0.249 | 0.326 | 1.31x | 0.245 | 0.328 | 1.34x | 0.99x | 1.01x |
| q23 | 0.144 | 0.120 | 0.83x | 0.097 | 0.127 | 1.31x | 0.67x | 1.06x |

The complete run has 19 of 23 StreamFusion wins with mini-batching disabled and 23 of 23 enabled.
The enabled lead ranges from 1.14x to 5.49x. Unlike the blackhole matrix, this workload exposes the
economic value of logical changelog coalescing: q9/q15/q16/q17/q19 avoid serializer calls, broker
records, transaction bookkeeping, and checkpoint work for updates cancelled inside the bundle.

A same-day focused repeat of q9/q15-q19/q23 produced:

| Query | SF/Flink off | SF/Flink on | SF on/off |
|---|---:|---:|---:|
| q9 | 0.65x | 1.73x | 2.69x |
| q15 | 1.41x | 2.30x | 3.09x |
| q16 | 1.27x | 1.94x | 2.52x |
| q17 | 1.03x | 1.56x | 2.04x |
| q18 | 0.94x | 1.22x | 1.06x |
| q19 | 0.60x | 4.80x | 8.16x |
| q23 | 1.12x | 0.99x | 0.90x |

Thus the high-churn gains reproduce, while q23 should be treated as parity: its short, sparse output
makes fixed job/checkpoint cost large enough to move the ratio from 0.99x to 1.31x.

Run the four-way comparison in the mandatory release build with:

`SF_BENCHMARK=true SF_MATRIX_KAFKA_SINK=true SF_ROWS=2000000
SF_MATRIX_QUERIES=q0,q1,q2,q3,q4,q5,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22,q23
mvn -pl :streamfusion-runtime test -Pbench
-Dnative.cargo.args="build --release --features mimalloc,kafka,json"
-Dtest=NexmarkMatrixBenchmark#exactlyOnceKafkaSinkModeComparison`.

`SF_MATRIX_QUERIES` may select a focused subset, and `SF_PARALLELISM` overrides the Kafka-fed
runs' default parallelism of 4 (the corpus topic is created with one partition per subtask).
For differential profiling,
`SF_PROFILE_KAFKA_SINK=true` runs one selected query repeatedly against one broker and bypasses the
matrix warmup/repetition. Select the query with `-Dprofile.query=q19`, the engine with
`-Dprofile.native=true|false`, mini-batching with `-Dprofile.minibatch=true|false`, and the
sampling window with `-Dprofile.seconds=60`. JVM profilers can be attached through
`-Dsf.extraJvmArgs`; for example, JFR uses
`-Dsf.extraJvmArgs=-XX:StartFlightRecording=filename=/tmp/kafka-sink.jfr,settings=profile,dumponexit=true`.

#### Historical tuned-only matrix (2026-07-05)

Production Flink deployments routinely enable mini-batch for stateful queries, so the matrix has a
**tuned mode**: `table.exec.mini-batch.*` (2s allow-latency, size 50000) on **both** engines — the
steelman rule, and the config behind the only public per-query Alibaba comparison. Generator source
(the tuned question is engine-vs-engine, not the perimeter) and **5M events** so the flush cadence
amortizes (at 500K the run is shorter than one flush interval and measures latency artifacts).
`table.optimizer.distinct-agg.split.enabled` stays default-off: it
is a skew mitigation for parallel deployments (these runs are parallelism 1) and its incremental
plan chain deliberately has no native path (`wontdos/52-distinct-split-chain.md`).
`SF_BENCHMARK=true SF_MATRIX_TUNED=true SF_ROWS=5000000
SF_MATRIX_QUERIES=q0,…,q23 mvn -pl :streamfusion-runtime test -Pbench -Dnative.cargo.args="build --release --features
mimalloc" -Dtest=NexmarkMatrixBenchmark#tunedMiniBatchMatrix` (the query list defaults to the
changelog family — mini-batch changes only those plans — but the full-suite run below doubles as
the coverage check that **every** query still routes native under production tuning; run
2026-07-05, no fallbacks).

| Query | Shape | Native vs. tuned Flink |
|---|---|---|
| q0 | pass-through projection of `bid` | **1.23×** |
| q1 | `0.908 * price` — exact `Decimal128` (byte-parity) | **1.16×** |
| q2 | filter `WHERE MOD(auction, 123) = 0` | **1.45×** |
| q3 | updating join `auction ⋈ person` | 0.69× |
| q4 | regular join → `MAX` → `AVG` per category | **2.85×** |
| q5 | Hot Items (window re-agg + window join) | **1.18×** |
| q7 | tumble `MAX` ⋈ bid | **1.43×** |
| q8 | tumble windowed-distinct ⋈ join | 0.76× |
| q9 | regular join → `ROW_NUMBER` (≤ 1) | **2.15×** |
| q10 | `DATE_FORMAT` projection | **1.18×** |
| q11 | session-window `COUNT` per bidder | **3.01×** |
| q12 | proctime tumble `COUNT` per bidder | **1.70×** |
| q13 | lookup join (bounded dimension) | **1.09×** |
| q14 | `HOUR`/`CASE` + `count_char` UDF + decimal | **1.05×** |
| q15 | multi-`DISTINCT` `COUNT`s per day (`DATE_FORMAT` group) | **1.26×** |
| q16 | multi-`DISTINCT` per channel/day | **1.18×** |
| q17 | group agg + `AVG`/`MIN`/`MAX`/`SUM` per day | 1.00× |
| q18 | `ROW_NUMBER` dedup (≤ 1) | **2.02×** |
| q19 | `ROW_NUMBER` topN (≤ 10) | **2.36×** |
| q20 | updating join (`category = 10`) | **1.34×** |
| q21 | `CASE` + `REGEXP_EXTRACT`/`LOWER` — JVM upcall (byte-parity) | 0.97× |
| q22 | `SPLIT_INDEX(url, '/', n)` projection | **1.25×** |
| q23 | three-way join `bid ⋈ person ⋈ auction` | **3.01×** |

The changelog-family margins are **wider** than the default-config generator column, not narrower:
at 5M events the state-heavy queries dominate their runtime with operator work (the per-event
JIT/setup share shrinks), and under mini-batch the native side emits the net logical-bundle Top-N diff
(divergences/20) where Flink's rank — which has no mini-batch variant — still pays the per-record
cascade (q19 2.36× tuned vs 1.48× default). The non-changelog queries plan identically tuned or
not (mini-batch inserts nothing into them), so their column is effectively the generator rung at
5M events — the same transpose-bound stragglers trail here for the same reason (q3 a thin island
over a wide transposed perimeter, q8's window join, q21's per-batch JVM upcall at 5M-event scale);
the mini-batch config itself costs the native side nothing since calc pruning pushes through the
assigner.

The first tuned run reported q4/q15/q16/q17 as fallbacks — the tuned column doubling as the
mini-batch coverage check, exactly as designed. That coverage has since landed (two-phase FILTER
clauses, filtered distinct views, string MIN/MAX partials, retraction-bearing partials with the
count1 record counter), and all four now run fully native — as does the whole suite, including
the windowed two-phase splits over every value type (the 2026-07-05 nullable-sum-buffer work).
q15/q16 are worth noting: `GROUP BY
day` is a single live grouping key carrying every record's bidder/auction distinct sets — the
hot-key shape `distinct-agg.split` exists to mitigate — and the native no-split plan beats tuned
Flink on it (see `wontdos/52-distinct-split-chain.md`).

## State backends: memory vs Paimon

`PaimonStateBackendBenchmark` (opt-in like the rest: `SF_BENCHMARK=true`) runs native q4 under
identical 500 ms checkpointing on the default memory backend (raw keyed-state snapshots) and on
the Paimon backend (read-through probes against local parquet tables, one incremental commit per
barrier). It lives in the compactor module so the state tables are maintained exactly as a
deployment's would be, and it asserts both gates rather than assuming them: a compactor must be
discoverable, and a live `Paimon*Store` native handle must be observed while a verification job
runs — engagement is never inferred from configuration.

2026-07-24, 2M events, best of 2 after warmup (release build without mimalloc — a foreign-free
crash in the allocator aliasing blocked the standard profile here until the checked-free shims
landed; the memory row measured 4.14 s with mimalloc, so the allocator does not move these
numbers materially):

| backend | time | throughput |
|---|---|---|
| memory | 4.19 s | 478K events/s |
| Paimon, compactor installed | 124.1 s | 16K events/s (**0.03×**) |
| Paimon, compactor absent (unmaintained tables) | 99.6 s | 20K events/s (0.04×) |

The ~30× gap is the measured price of the backend's deliberately pure read-through design: every
barrier drops the whole working set, so q4's ~120K live aggregate keys re-hydrate from parquet
continuously. Notably, running maintenance *synchronously on the barrier* made this job slower
than no maintenance at all — the per-barrier table open, scan plan, and writer/commit lifecycle
cost more than the read amplification they saved. The backend buys incremental checkpoints and
bounded memory, not throughput; jobs whose working set fits in memory should keep the default
backend.

The flame graph of that baseline said where the time was — filesystem metadata syscalls (~57% of
samples: per-write directory creation in the object-store layer, per-scan file opens, the
per-checkpoint hard-link farm) plus per-barrier maintenance setup, not compute — and the fixes
tracked it (same protocol; the memory baseline drifted 4.2–8.5 s with sustained-load thermals, so
ratios are quoted loosely):

| change | Paimon q4 time | throughput |
|---|---|---|
| baseline (maintenance on the barrier) | 124.1 s | 16K events/s |
| maintenance on a background thread (RocksDB model) | 55.6–66.1 s | 30–36K events/s |
| + bucket-granular hydration, resident until the barrier | 35.4 s | 57K events/s |
| + custom local-fs write path (no per-write `create_dir_all`) | 9.85 s | 203K events/s (**0.44×** memory) |
| everything + dir cache, paced maintenance, incremental links, standard `-Pbench` (mimalloc) | 8.99 s | 222K events/s (**0.54×** memory) |
| + de-bucketed tables (`buckets` = 1, recovery-time clip) | 3.08–3.71 s | 539–650K events/s (**1.16–1.27×** memory) |
| write buffer + per-batch key-probe reads (no retained clean rows) | 2.39 s | 836K events/s (**1.12×** memory) |

**The durable backend now beats the memory backend on this shape** (two runs, machine cooled):
at 500 ms checkpoints the memory backend serializes and uploads its whole state as raw
keyed-state snapshots every barrier, while the de-bucketed Paimon backend commits one small
delta file and uploads incrementally — the classic incremental-checkpoint trade, landing exactly
where RocksDB-vs-heap lands in Flink for checkpoint-heavy jobs. Cumulative for the round:
**124.1 s → 3.1 s, ~35×.** Bucket count (= Flink max parallelism, since bucket = key group) was suspected
as a multiplier on the per-file costs, and the benchmark takes `SF_MAX_PARALLELISM` to test it:
at max-parallelism 8 (8 buckets instead of 128, 16× fewer files per commit) the pre-fs-fix run
measured 33.1 s vs 35.4 s — ~7% — so the bucket-per-key-group layout keeps its free-rescale
property at negligible cost. The remaining ~2× against memory decomposes across the
hard-link/upload farm, the commit path, hydration decode, and background-maintenance interference
— the next profile decides which.

The last row is a design simplification measured at parity, not a speed-up: the store was reduced
to exactly a write buffer plus the disk table (reads resolve per batch by pushing the missing
keys into the reader as an exact `IN` predicate; no clean row survives its bundle, so committed
state is never duplicated in operator memory). Same-session A/B against the interval-resident
design: 2.400 s vs 2.393 s — identical within noise (the memory baseline itself drifted
2.46–2.69 s between the two runs). On this shape nearly every key read is also written, so the
retained map's re-read savings never materialize; the memory bound drops from
touched-state-per-interval to written-state-per-interval. The enabling fix was in the reader: the
pinned paimon-rust fork evaluates `IN` literal sets with one hash-set pass instead of one
comparison kernel per literal, and the store plans its scan splits once per pinned snapshot
instead of per probe.

_Apple M1 Max; numbers are comparable only within a machine._

### Flink on RocksDB vs StreamFusion on Paimon (full Nexmark, exactly-once Kafka)

The production-shaped backend comparison (Apple M1 Max, release + `mimalloc`): stock Flink on its RocksDB backend versus the native engine on the Paimon state
backend, over the readme's exactly-once Kafka pipeline — a 2M-event JSON corpus on a
four-partition topic, both engines at parallelism 4, one-second checkpoints, exactly-once
delivery to Kafka, best of two after a warmup — in both mini-batch modes
(`SF_STATE_BACKENDS_MINI_BATCH=true` runs the tuned configuration on both engines; `both`
runs the two mode tables in one pass, sharing the broker, corpus, and preflight). At
parallelism 4 the native side's co-located shuffle hands Arrow batches over by ownership (the
zero-copy local exchange); a multi-TaskManager deployment's shuffle would pay Arrow IPC instead
(`streamfusion.exchange.zeroCopyLocal=false` models it, worth ~11% on the shuffle-heaviest
mini-batch-off cells and nothing elsewhere). The comparison lives in `streamfusion-paimon-compactor` — the only module whose
classpath can hold both the backend and its Java table maintainer — and is run with
`SF_BENCHMARK=true SF_MATRIX_STATE_BACKENDS=true SF_ROWS=2000000 mvn test -Pbench -pl
streamfusion-paimon-compactor -am -Dsurefire.failIfNoSpecifiedTests=false
-Dtest=NexmarkStateBackendBenchmark#stateBackendComparison` (`SF_PARALLELISM` overrides the
default parallelism of 4).
A q4 preflight asserts both backends engage before any number is recorded (RocksDB must
materialize working files under a directed localdir; a live Paimon store handle must be
observed), and additionally requires a **deletion-vector-capable compactor**: without one the
Paimon side runs unmaintained or on merge reads, which is not the configuration this table
claims to measure — an earlier revision of this table unknowingly did exactly that, because the
benchmark then lived in a module whose classpath could not carry the compactor. Deletion-vector
capability currently needs a Paimon bundle with the binary-primary-key lookup comparator fix
(contributed upstream); until it reaches a release, build one locally and select it with
`-Dpaimon.bundle.version`. Operators the Paimon backend does not yet carry (proctime shapes)
fall back to memory state with raw snapshots, exactly as a deployment would run them.

#### Current (2026-07-28 evening — shared native sources)

Same method as the table below; the one change is shared native sources (one topic read per
query, however many views scan it).

Mini-batch off:

| Query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---:|---:|---:|---:|---:|
| q0 | 2.452 | 0.82M | 1.463 | 1.37M | **1.68×** |
| q1 | 2.349 | 0.85M | 1.387 | 1.44M | **1.69×** |
| q2 | 1.435 | 1.39M | 1.079 | 1.85M | **1.33×** |
| q3 | 1.313 | 1.52M | 1.263 | 1.58M | **1.04×** |
| q4 | 9.005 | 0.22M | 5.432 | 0.37M | **1.66×** |
| q5 | 4.644 | 0.43M | 2.197 | 0.91M | **2.11×** |
| q7 | 8.109 | 0.25M | 4.137 | 0.48M | **1.96×** |
| q8 | 2.455 | 0.81M | 2.033 | 0.98M | **1.21×** |
| q9 | 10.779 | 0.19M | 6.155 | 0.32M | **1.75×** |
| q10 | 2.777 | 0.72M | 1.841 | 1.09M | **1.51×** |
| q11 | 8.584 | 0.23M | 0.916 | 2.18M | **9.37×** |
| q12 | 1.838 | 1.09M | 0.829 | 2.41M | **2.22×** |
| q13 | 2.183 | 0.92M | 1.502 | 1.33M | **1.45×** |
| q14 | 2.770 | 0.72M | 1.807 | 1.11M | **1.53×** |
| q15 | 15.504 | 0.13M | 2.343 | 0.85M | **6.62×** |
| q16 | 9.427 | 0.21M | 2.353 | 0.85M | **4.01×** |
| q17 | 4.459 | 0.45M | 2.276 | 0.88M | **1.96×** |
| q18 | 7.305 | 0.27M | 7.361 | 0.27M | 0.99× |
| q19 | 10.012 | 0.20M | 7.874 | 0.25M | **1.27×** |
| q20 | 6.215 | 0.32M | 5.152 | 0.39M | **1.21×** |
| q21 | 1.521 | 1.31M | 1.344 | 1.49M | **1.13×** |
| q22 | 2.379 | 0.84M | 1.635 | 1.22M | **1.45×** |
| q23 | 20.198 | 0.10M | 10.075 | 0.20M | **2.00×** |

Mini-batch on:

| Query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---:|---:|---:|---:|---:|
| q0 | 2.350 | 0.85M | 1.398 | 1.43M | **1.68×** |
| q1 | 2.311 | 0.87M | 1.467 | 1.36M | **1.58×** |
| q2 | 1.526 | 1.31M | 1.599 | 1.25M | 0.95× |
| q3 | 1.449 | 1.38M | 2.249 | 0.89M | 0.64× |
| q4 | 11.830 | 0.17M | 6.646 | 0.30M | **1.78×** |
| q5 | 4.083 | 0.49M | 1.678 | 1.19M | **2.43×** |
| q7 | 8.867 | 0.23M | 2.782 | 0.72M | **3.19×** |
| q8 | 2.302 | 0.87M | 1.213 | 1.65M | **1.90×** |
| q9 | 11.366 | 0.18M | 6.798 | 0.29M | **1.67×** |
| q10 | 2.585 | 0.77M | 1.775 | 1.13M | **1.46×** |
| q11 | 8.358 | 0.24M | 0.870 | 2.30M | **9.61×** |
| q12 | 1.794 | 1.11M | 0.848 | 2.36M | **2.11×** |
| q13 | 1.961 | 1.02M | 1.633 | 1.22M | **1.20×** |
| q14 | 2.566 | 0.78M | 1.612 | 1.24M | **1.59×** |
| q15 | 2.075 | 0.96M | 1.061 | 1.89M | **1.96×** |
| q16 | 2.880 | 0.69M | 1.226 | 1.63M | **2.35×** |
| q17 | 2.269 | 0.88M | 1.367 | 1.46M | **1.66×** |
| q18 | 8.455 | 0.24M | 3.162 | 0.63M | **2.67×** |
| q19 | 8.956 | 0.22M | 4.060 | 0.49M | **2.21×** |
| q20 | 6.767 | 0.30M | 4.607 | 0.43M | **1.47×** |
| q21 | 1.520 | 1.32M | 1.312 | 1.52M | **1.16×** |
| q22 | 2.256 | 0.89M | 1.606 | 1.25M | **1.41×** |
| q23 | 14.396 | 0.14M | 4.918 | 0.41M | **2.93×** |

Off: 22 of 23 wins, geometric mean **1.83x**; on: 21 of 23, **1.84x** (up from 1.65x). Source
sharing lifts q3 off-mode from 0.81x to 1.04x and the mini-batch-on multi-view cells broadly
(q7 1.74x -> 3.19x, q18 1.25x -> 2.67x, q23 1.64x -> 2.93x). The two remaining losses are
q2 on-mode (0.95x, noise-range) and q3 on-mode (0.64x): with the source no longer the
bottleneck, q3's mini-batch cost on the persistent backend is the join's Paimon state path —
the next target this table points at.

#### Prior (2026-07-28 — four-partition output topics, post-exchange coalescing, before shared sources)

Mini-batch off (geometric mean **1.83x**, 22 of 23 wins):

| query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---|---|---|---|---|
| q0 | 3.568 | 561K | 2.190 | 913K | **1.63×** |
| q1 | 3.420 | 585K | 2.001 | 999K | **1.71×** |
| q2 | 1.875 | 1.07M | 1.616 | 1.24M | **1.16×** |
| q3 | 1.729 | 1.16M | 2.139 | 935K | 0.81× |
| q4 | 9.330 | 214K | 5.206 | 384K | **1.79×** |
| q5 | 4.168 | 480K | 1.925 | 1.04M | **2.17×** |
| q7 | 8.537 | 234K | 3.214 | 622K | **2.66×** |
| q8 | 2.243 | 892K | 2.120 | 944K | **1.06×** |
| q9 | 10.790 | 185K | 6.693 | 299K | **1.61×** |
| q10 | 3.055 | 655K | 1.886 | 1.06M | **1.62×** |
| q11 | 8.607 | 232K | 0.971 | 2.06M | **8.87×** |
| q12 | 2.203 | 908K | 0.905 | 2.21M | **2.43×** |
| q13 | 2.056 | 973K | 1.521 | 1.31M | **1.35×** |
| q14 | 2.941 | 680K | 1.764 | 1.13M | **1.67×** |
| q15 | 15.230 | 131K | 2.618 | 764K | **5.82×** |
| q16 | 10.375 | 193K | 2.960 | 676K | **3.50×** |
| q17 | 5.709 | 350K | 2.877 | 695K | **1.98×** |
| q18 | 8.545 | 234K | 8.304 | 241K | **1.03×** |
| q19 | 10.704 | 187K | 8.111 | 247K | **1.32×** |
| q20 | 6.812 | 294K | 5.519 | 362K | **1.23×** |
| q21 | 1.718 | 1.16M | 1.296 | 1.54M | **1.33×** |
| q22 | 2.465 | 811K | 1.549 | 1.29M | **1.59×** |
| q23 | 19.300 | 104K | 8.854 | 226K | **2.18×** |

Mini-batch on, both engines tuned (geometric mean **1.65x**, 22 of 23 wins):

| query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---|---|---|---|---|
| q0 | 2.729 | 733K | 2.159 | 927K | **1.26×** |
| q1 | 2.594 | 771K | 1.424 | 1.40M | **1.82×** |
| q2 | 1.564 | 1.28M | 1.081 | 1.85M | **1.45×** |
| q3 | 1.363 | 1.47M | 1.975 | 1.01M | 0.69× |
| q4 | 8.350 | 240K | 5.506 | 363K | **1.52×** |
| q5 | 4.185 | 478K | 2.579 | 776K | **1.62×** |
| q7 | 7.641 | 262K | 4.385 | 456K | **1.74×** |
| q8 | 2.291 | 873K | 1.758 | 1.14M | **1.30×** |
| q9 | 11.485 | 174K | 6.346 | 315K | **1.81×** |
| q10 | 2.616 | 764K | 2.220 | 901K | **1.18×** |
| q11 | 7.999 | 250K | 0.959 | 2.08M | **8.34×** |
| q12 | 1.815 | 1.10M | 0.838 | 2.39M | **2.17×** |
| q13 | 2.025 | 988K | 1.561 | 1.28M | **1.30×** |
| q14 | 2.731 | 732K | 1.658 | 1.21M | **1.65×** |
| q15 | 2.006 | 997K | 1.050 | 1.91M | **1.91×** |
| q16 | 2.726 | 734K | 1.136 | 1.76M | **2.40×** |
| q17 | 2.203 | 908K | 1.209 | 1.65M | **1.82×** |
| q18 | 7.506 | 266K | 5.983 | 334K | **1.25×** |
| q19 | 8.726 | 229K | 3.626 | 552K | **2.41×** |
| q20 | 6.293 | 318K | 5.095 | 393K | **1.24×** |
| q21 | 1.593 | 1.26M | 1.197 | 1.67M | **1.33×** |
| q22 | 2.208 | 906K | 1.601 | 1.25M | **1.38×** |
| q23 | 13.156 | 152K | 8.042 | 249K | **1.64×** |

q3 (a plain updating join) remains the one loss in both modes. Against the prior tables below,
the multi-partition output topics raised both engines' sink ceilings; the RocksDB baselines
moved less than the heap ones did in the memory comparison, so the disk ratios held up better —
the off geomean is unchanged at 1.83x and the on geomean settles at 1.65x.

#### Prior (2026-07-27 night — single-partition output topics, before post-exchange coalescing)

Mini-batch off:

| query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---|---|---|---|---|
| q0 | 2.529 | 791K | 1.417 | 1.41M | **1.78×** |
| q1 | 2.492 | 802K | 1.472 | 1.36M | **1.69×** |
| q2 | 1.568 | 1.28M | 1.056 | 1.89M | **1.48×** |
| q3 | 1.292 | 1.55M | 1.508 | 1.33M | 0.86× |
| q4 | 7.399 | 270K | 4.501 | 444K | **1.64×** |
| q5 | 3.915 | 511K | 2.300 | 869K | **1.70×** |
| q7 | 7.867 | 254K | 4.487 | 446K | **1.75×** |
| q8 | 2.278 | 878K | 1.445 | 1.38M | **1.58×** |
| q9 | 10.146 | 197K | 6.513 | 307K | **1.56×** |
| q10 | 2.618 | 764K | 1.999 | 1.00M | **1.31×** |
| q11 | 7.788 | 257K | 0.956 | 2.09M | **8.15×** |
| q12 | 2.026 | 987K | 0.943 | 2.12M | **2.15×** |
| q13 | 2.200 | 909K | 1.611 | 1.24M | **1.37×** |
| q14 | 2.840 | 704K | 1.481 | 1.35M | **1.92×** |
| q15 | 14.631 | 137K | 2.246 | 891K | **6.52×** |
| q16 | 9.211 | 217K | 2.138 | 935K | **4.31×** |
| q17 | 4.543 | 440K | 1.777 | 1.13M | **2.56×** |
| q18 | 6.987 | 286K | 5.314 | 376K | **1.31×** |
| q19 | 10.817 | 185K | 6.313 | 317K | **1.71×** |
| q20 | 4.985 | 401K | 5.383 | 372K | 0.93× |
| q21 | 1.481 | 1.35M | 1.398 | 1.43M | **1.06×** |
| q22 | 2.352 | 850K | 1.557 | 1.28M | **1.51×** |
| q23 | 15.194 | 132K | 8.810 | 227K | **1.72×** |

Mini-batch on (both engines tuned):

| query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---|---|---|---|---|
| q0 | 2.418 | 827K | 1.480 | 1.35M | **1.63×** |
| q1 | 2.363 | 846K | 1.371 | 1.46M | **1.72×** |
| q2 | 1.585 | 1.26M | 1.038 | 1.93M | **1.53×** |
| q3 | 1.325 | 1.51M | 1.410 | 1.42M | 0.94× |
| q4 | 8.132 | 246K | 2.737 | 731K | **2.97×** |
| q5 | 3.956 | 506K | 2.536 | 789K | **1.56×** |
| q7 | 7.375 | 271K | 2.992 | 669K | **2.46×** |
| q8 | 2.237 | 894K | 1.516 | 1.32M | **1.48×** |
| q9 | 10.856 | 184K | 6.364 | 314K | **1.71×** |
| q10 | 2.587 | 773K | 1.944 | 1.03M | **1.33×** |
| q11 | 7.920 | 253K | 0.914 | 2.19M | **8.66×** |
| q12 | 1.941 | 1.03M | 1.049 | 1.91M | **1.85×** |
| q13 | 2.135 | 937K | 1.474 | 1.36M | **1.45×** |
| q14 | 2.689 | 744K | 1.740 | 1.15M | **1.55×** |
| q15 | 1.974 | 1.01M | 0.968 | 2.07M | **2.04×** |
| q16 | 2.869 | 697K | 1.100 | 1.82M | **2.61×** |
| q17 | 2.298 | 870K | 1.407 | 1.42M | **1.63×** |
| q18 | 7.546 | 265K | 2.833 | 706K | **2.66×** |
| q19 | 11.373 | 176K | 3.884 | 515K | **2.93×** |
| q20 | 6.359 | 315K | 4.856 | 412K | **1.31×** |
| q21 | 1.514 | 1.32M | 1.402 | 1.43M | **1.08×** |
| q22 | 2.321 | 862K | 1.374 | 1.46M | **1.69×** |
| q23 | 12.358 | 162K | 25.700 | 78K | 0.48× † |

† q23's tuned suite cell is a load anomaly: a focused repeat directly after the suite, in its own
JVM, measured **1.46×** (8.914 s vs the suite's 25.700 s) — in line with its off-mode 1.72× and
its memory-state 1.89×. The suite pass started seconds after the off-mode pass finished.

Off-mode: geometric mean **1.83×**, 21 of 23 wins; the losses are q3 (0.86×) and q20 (0.93×),
with q21 near parity. Tuned mode: geometric mean **1.76×** as measured, **1.86×** with the †
repeat substituted, again 21 of 23 wins. The disk comparison survives parallelism far better
than the memory-state one (its section above): RocksDB pays its per-record serialize/JNI tax in
every subtask, so quadrupling subtasks does not close its gap the way quadrupling Flink's heap
pipeline does. The largest wins remain the shapes RocksDB pays per record for — session windows
(q11, ~8×) and the off-mode window-agg family (q15 6.5×, q16 4.3×). q3 — a small-state join
whose Flink plan runs unusually fast at parallelism 4 — is the one consistent loss on both
backends, the same scaling gap the memory table's off-mode q3/q4 show.

#### Prior comparison (2026-07-27 evening, parallelism 1, 500K events)

The same method at parallelism 1 over the earlier 500K-event single-partition corpus — before
the parallelism-4 harness, the zero-copy local exchange, and the 2M-event corpus:

Mini-batch off:

| query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---|---|---|---|---|
| q0 | 2.212 | 226K | 1.032 | 484K | **2.14×** |
| q1 | 2.071 | 241K | 0.998 | 501K | **2.07×** |
| q2 | 1.371 | 365K | 1.022 | 489K | **1.34×** |
| q3 | 1.081 | 462K | 1.300 | 385K | 0.83× † |
| q4 | 4.992 | 100K | 2.121 | 236K | **2.35×** |
| q5 | 2.302 | 217K | 1.101 | 454K | **2.09×** |
| q7 | 3.961 | 126K | 2.384 | 210K | **1.66×** |
| q8 | 1.071 | 467K | 0.802 | 623K | **1.34×** |
| q9 | 5.676 | 88K | 6.252 | 80K | 0.91× |
| q10 | 2.199 | 227K | 1.046 | 478K | **2.10×** |
| q11 | 7.244 | 69K | 0.793 | 631K | **9.14×** |
| q12 | 1.487 | 336K | 0.804 | 622K | **1.85×** |
| q13 | 1.839 | 272K | 0.955 | 523K | **1.93×** |
| q14 | 2.219 | 225K | 0.921 | 543K | **2.41×** |
| q15 | 4.368 | 114K | 1.374 | 364K | **3.18×** |
| q16 | 5.767 | 87K | 1.833 | 273K | **3.15×** |
| q17 | 3.323 | 150K | 1.174 | 426K | **2.83×** |
| q18 | 5.131 | 97K | 12.860 | 39K | 0.40× † |
| q19 | 10.385 | 48K | 10.082 | 50K | **1.03×** |
| q20 | 3.711 | 135K | 3.180 | 157K | **1.17×** |
| q21 | 1.480 | 338K | 0.829 | 603K | **1.79×** |
| q22 | 1.953 | 256K | 1.115 | 448K | **1.75×** |
| q23 | 8.073 | 62K | 3.865 | 129K | **2.09×** |

† A focused repeat of the two anomalous cells directly after the suite, machine cooled: **q3
1.34×** (0.885 s) and **q18 2.27×** (2.208 s) — both suite cells caught slow StreamFusion
measurements under sustained load (this pass started seconds after the memory pass finished);
q18's repeat sits in its established 1.7–2.4× band.

Mini-batch on (both engines tuned):

| query | Flink/RocksDB s | ev/s | SF/Paimon s | ev/s | SF/Flink |
|---|---|---|---|---|---|
| q0 | 2.129 | 235K | 1.050 | 476K | **2.03×** |
| q1 | 2.101 | 238K | 1.133 | 441K | **1.85×** |
| q2 | 1.383 | 362K | 1.075 | 465K | **1.29×** |
| q3 | 1.129 | 443K | 0.910 | 550K | **1.24×** |
| q4 | 4.357 | 115K | 1.798 | 278K | **2.42×** |
| q5 | 2.212 | 226K | 1.057 | 473K | **2.09×** |
| q7 | 4.401 | 114K | 2.038 | 245K | **2.16×** |
| q8 | 1.099 | 455K | 0.941 | 531K | **1.17×** |
| q9 | 6.148 | 81K | 3.060 | 163K | **2.01×** |
| q10 | 2.247 | 222K | 1.280 | 390K | **1.76×** |
| q11 | 7.076 | 71K | 0.862 | 580K | **8.21×** |
| q12 | 1.439 | 348K | 0.740 | 676K | **1.94×** |
| q13 | 1.855 | 269K | 0.886 | 564K | **2.09×** |
| q14 | 2.264 | 221K | 0.926 | 540K | **2.45×** |
| q15 | 1.468 | 341K | 0.736 | 679K | **1.99×** |
| q16 | 2.157 | 232K | 0.951 | 526K | **2.27×** |
| q17 | 1.819 | 275K | 0.881 | 567K | **2.06×** |
| q18 | 5.719 | 87K | 3.380 | 148K | **1.69×** |
| q19 | 11.022 | 45K | 1.593 | 314K | **6.92×** |
| q20 | 3.727 | 134K | 2.715 | 184K | **1.37×** |
| q21 | 1.496 | 334K | 0.831 | 602K | **1.80×** |
| q22 | 2.032 | 246K | 1.038 | 482K | **1.96×** |
| q23 | 8.518 | 59K | 5.027 | 99K | **1.69×** |

Off-mode: geometric mean **1.80×** (with the two † repeats substituted: **1.94×**), 20 of 23
wins as measured in the suite pass; the remaining sub-parity cells are q9 (0.91×, the same
snapshot/read-through-bound shape as on memory state) and near-parity q19 (1.03×). Tuned mode:
**23 of 23 wins**, geometric mean **2.08×** (1.17–8.21×), with q9 flipping to 2.01× and q19 to
6.92× — the same mini-batch leverage the memory comparison shows, carried intact onto the disk
backends. q11's ~9× is session windows: RocksDB pays the merging window assigner's per-record
window-mapping rewrites, the native session aggregate folds batch runs in memory and commits
once per barrier. The thinnest wins remain the point-read-heaviest shapes — q8, q19, q20 —
where per-batch read-through still trails RocksDB's cached point access.

**How q18 got fixed — and the two wrong answers before it.** The first revision of this table
lost q18 at 0.57×: the per-batch `IN` probe re-decoded the table's whole key column per batch,
because a few thousand uniformly-spread keys land in every file's `[min, max]` range, so range
stats prune nothing. Two probe-side indexes were shipped and withdrawn (see
`.claude/wontdos/58-probe-side-file-indexes.md`): a bloom file index — mathematically unable to
skip a file for a batched probe, since skipping requires *every* probed key to miss,
`(1-fpp)^|probe|` ≈ 0 — and an exact per-file key-hash set, which pruned deterministically but
held resident memory proportional to *physical disk rows*, defeating the point of a disk
backend. The investigation also invalidated every earlier number in this section: the benchmark
then ran without any table maintenance at all (its module's classpath could not carry the
compactor), and q18's run-to-run bimodality was a positive feedback loop — a slow start means a
longer run, more barriers, more sorted runs, slower merge reads. The shipped fix removes merge
reads instead of indexing them: state tables run in **deletion-vector mode**, every committed
read is a raw parquet scan with the exact `IN` probe pushed to the decoder and the vectors
applied as row masks, and the compactor splits into a minimal barrier-synchronous round
(up-level the barrier's level-0 runs, universal triggers disabled — deletion-vector reads skip
level 0, so this is correctness, not tuning) plus background shaping merges that can lag
arbitrarily without affecting results. q18 now measures 1.7–2.4× across independent reruns with
no resident index memory.

