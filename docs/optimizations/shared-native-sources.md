# Shared native sources

**Applies to:** repeated compatible Kafka or Paimon scans, including different sinks in one
statement set (and q3, q4, q8, q9, q20's views over one Kafka topic in Nexmark).

## The problem

Nexmark carves its `person`/`auction`/`bid` views out of one interleaved event topic, so every query
joining two views plans two scans of the same table. Flink's sub-plan reuse merges them — one read,
one decode — but that reuse is digest-based, and every native rel deliberately carries a
digest-unique barrier: an Arrow batch is handed to exactly one consumer, which closes its off-heap
buffers after reading, so a blindly merged native subtree would fan one batch to two consumers and
the second would read freed memory. The consequence, exposed by a per-thread q3 profile, was two
full Kafka byte-source/native-decode boundaries per query — twice the topic read, twice the JSON decode — the whole q3
loss (its join is under 2% of CPU).

## The fix

The substitution pass now merges identical native sources itself, the way the streaming engines that
faced this same problem do:

- Arroyo dedups source nodes by name and fans out `Arc<RecordBatch>` clones.
- RisingWave rewrites any source referenced twice into one shared `StreamShare` node.
- Flink's own `SubplanReuser` is the host-side precedent this mirrors on the native side.

Semantically identical source/decode boundaries collapse into one instance under an explicit share
node carrying the branch count. The planner detects repeated scans before per-branch projection
pushdown; when the branches need different columns, it retains their common writer schema so one
split-aware Flink Kafka source performs one native decode and both branch Calcs project from the
shared Arrow batch. A single-use source keeps decoder projection pushdown.

The deployed planner runs the rewrite after Flink expands all sink roots. For Paimon it first
calls Flink's `ScanReuser` on the stock scans, which unions their top-level projections and inserts
branch projections. The native reader therefore decodes the union once for Parquet or ORC. Scan
digests retain filters, limits, hints, schema and table options, so distinct reads cannot accidentally
share. Consumer counts are computed only after each root's native admission/fallback decision.

At runtime the share operator declares the consumer count on each batch, and every chained
consumer's `root()` take returns its own zero-copy view over the same retained buffers (Arrow's
buffer reference counts; the split-and-transfer share idiom), so each branch keeps its usual
read-then-close contract and the buffers free on the last close. The single-consumer hand-off is
unchanged.
[`streamfusion.plan.shareSources=false`](../configuration.md) restores per-branch sources, as do
Flink's source-reuse and subplan-reuse switches. Cross-sink sharing requires the deployed planner
hook; the embedded `NativePlanner.install` program on a stock planner only sees one root at a time.

## Measured

The original native Kafka transport gained about 1.55× on q3 when this sharing design first landed.
After Kafka consumption moved back under Flink, the decode boundary temporarily lost the sharing
contract. Restoring it on the split-aware source (exactly-once Kafka in/out, JSON, 2M events,
parallelism 4, `max.poll.records=8192`, one warmup, best of two) moves current native q3 from
**1.071 to 1.158 M events/s (+8.1%)** with mini-batching off and from **1.136 to 1.192 M events/s
(+4.9%)** with mini-batching on. The matched CPU profile reduces Kafka source fetch/decode from 41%
to 21% of samples; the keyed exchanges and updating join become the remaining path.

The same dedup applies to every multi-view query — q4, q8, q9, and q20 all join two views of the one
topic; their heavier downstream work amortized the doubled read, but they stop paying it all the
same.

Paimon's SQL regression verifies three native readers become one and all three sinks retain stock
Flink results through an initial snapshot and later commits. A release-build reader microbenchmark
on September 14, 2026 uses 262,144 rows, four files, and three consumers of the same projected
bigint/string/decimal/array schema. It times native decoding, Arrow view retention/close and an id
checksum, excluding fixture generation, SQL planning, network transport and downstream operators.
Each format runs in a fresh JVM on an otherwise idle machine, one warmup plus the best of three
alternating-order measured runs. Row counts and checksums must match, and native file-read counts
must fall from 12 to 4.

| Format | Three independent readers | One shared reader | Speedup |
|---|---:|---:|---:|
| Parquet | 65.7 ms | 23.8 ms | 2.76× |
| ORC | 84.0 ms | 29.1 ms | 2.88× |

These are file-reader measurements, not whole-job throughput. Reproduce each format separately:

```bash
SF_PAIMON_SHARING_BENCHMARK=true SF_PAIMON_FILE_FORMAT=parquet \
  mvn test -Ppaimon,bench -pl :streamfusion-paimon -am \
  -Dtest='PaimonSourceBenchmark#compareThreeArrowConsumers' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Use `SF_PAIMON_FILE_FORMAT=orc` for the ORC comparison.

See [Benchmarks](../benchmarks.md) for the full A/B methodology.
