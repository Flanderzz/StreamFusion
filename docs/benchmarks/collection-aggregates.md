# Collection aggregates

## Integer COLLECT

COLLECT(INT/BIGINT) reuses the released Flink handler and managed MapViews inside the existing
columnar JVM grouped operator. Scalar inputs and non-character aggregate result fields are
required; character grouping keys and the TINYINT/SMALLINT extension are covered below. Other
COLLECT types retain fallback. Results require a verified preserving Calc
or final row sink. [Coverage](../operators/group-by.md#integer-collect) gives the exact gates.

Release/mimalloc measurements on 2026-10-24 use Flink 2.2.1, OpenJDK 17.0.20.1 and Linux
x86-64/WSL2 on an Intel Core i7-12650H. Each job has 200,000 runtime-source rows, parallelism 1,
an 8 GiB test heap, two warmups and three alternating trials per engine. INT keys have 64/4096
groups; BIGINT values cycle through 8 values per group. Every eighth input value is NULL, making
keys congruent to 7 modulo 8 all-NULL groups with empty multiset results. Accumulator caching uses
its default 0 entries. This source uses Java Row records and a rowwise blackhole sink.

Timings include planning, submission, processing, keyed state and both row/Arrow transposes;
cluster setup and initial explain are excluded. No tests, builds or other benchmarks run
concurrently. Previous code at `bbde17c4` uses identical benchmark/input code and the same frozen
native DSO; COLLECT must fall back there with no native operators. Candidate runs require
consumed NativeJvmGroup work. [All trials and profiles](results/integer-collect-2026-10-24.json)
include each JVM's paired stock controls and the full benchmark configuration.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Groups | Paired stock | Previous fallback | Final native route |
| --- | ---: | ---: | ---: |
| 64 | 0.428995 (0.311924–0.440618) | 0.333300 (0.325388–0.412596) | 0.344033 (0.329837–0.415788) |
| 4096 | 0.395371 (0.386500–0.422889) | 0.337087 (0.323164–0.383963) | 0.382120 (0.374743–0.472422) |

Native medians beat paired stock and exceed previous fallback. All ranges overlap, including
a slower wide-group native trial, so these short jobs do not establish sustained acceleration.
This ships under the accepted bounded collection tradeoff without a performance-only opt-in.

Optimization stops after two profiling/optimization passes. Native-only complete jobs have one
unprofiled warmup before each eight-second target capture; profiling stops after the current job
finishes. The 64-group capture has 19,173 samples, including 4,027 inclusive samples in released
group processing, 636 in COLLECT result snapshots and 1,264 in output append. The 4096-group
capture has 16,940 samples, including 4,034 in grouped processing, 913 in result snapshots and 1,071
in output append. Arrow map writing accounts for 460/366 inclusive samples. Counts overlap and
include planning, compilation and all JVM threads; they do not measure allocation or copied bytes.
No safe small shortcut was identified. Snapshot materialization, managed-state/output work and
startup remain performance opportunities in the [native SQL backlog](https://github.com/datafusion-contrib/StreamFusion/issues/239).
The production grouped operator, state codecs and JNI ownership protocol are unchanged.

Reproduce after building a release native library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=collect -Djvmagg.rows=200000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Also run with `-Djvmagg.keys=4096`. The previous checkout copies the same benchmark source and
sets `-Djvmagg.expectNative=false`. The optional `jvmagg.values` bounds the per-key input domain;
zero retains the existing monotonically increasing fixture used by the other aggregate cases.
The diagnostic result parser records this domain size and accepts older summaries without
`values`, treating them as the original unbounded domain (`0`). Trial-count, positivity and
reported-median checks apply to both formats.
For each CPU capture, set `SF_PROFILE_JVM_AGG=true`, select `#profileCollect`, keep the same
case/input/route settings and pass `-Djvmagg.seconds=8 -Dprofile.asprof=/path/to/asprof`.
`jvmagg.engine=flink|native|both` selects a timing engine; profile mode temporarily selects native
and restores its properties afterward. Profiled timings do not enter the trial table.

## Integer ARRAY_AGG

On Flink 2.2.1, ARRAY_AGG(INT/BIGINT) uses the same generated JVM grouped runtime with released
RAW LinkedList accumulators. Scalar inputs and non-character aggregate result fields are required; character grouping keys
are covered below.
Flink 1.18.1 has no released ARRAY_AGG catalog entry and is explicitly N/A. Native work, resolved
types, arrival order, NULL treatment, DISTINCT/FILTER, retractions and repeated heap/RocksDB
recovery are verified. Nullable primitive arrays additionally feed native Top-N/UNNEST;
[coverage](../operators/group-by.md#integer-array_agg) describes sensitive output restrictions.

Measurements use the same release/mimalloc DSO, CPU, OS, JDK, 8 GiB heap, parallelism 1,
stock/native alternation and timing boundaries as integer COLLECT above. Each complete job has
50,000 runtime-source rows, 64/4096 INT groups, two warmups and three measured trials per engine.
BIGINT input values cycle through 8 values per group, with NULL every eighth input. INT jobs add
CAST(v AS INT) before grouping in both engines. The value domain does not bound ARRAY length:
default RESPECT NULLS arrays grow to 781/782 elements at 64 groups and 12/13 at 4096 groups. All-NULL groups
also grow under RESPECT NULLS. Accumulator caching remains at its default 0 entries.

Runtime Row sources, output-array materialization, keyed state, both transposes, a blackhole
row sink and planning/submission remain timed. Initial explain and cluster setup are excluded;
no other local tests/builds or benchmarks run concurrently with these measurements. Previous
code at `9ee3080f` uses identical benchmark/input source and the same frozen native DSO; both
ARRAY_AGG cases require fallback there. [All trials and profiles](results/integer-array-agg-2026-10-24.json)
retain both paired stock controls, input metadata and diagnostic boundaries.

Median seconds (minimum–maximum), retaining complete-job startup costs:

| Element type / groups | Paired stock | Previous fallback | Final native route |
| --- | ---: | ---: | ---: |
| BIGINT / 64 | 0.590129 (0.530637–0.612707) | 0.557446 (0.517953–0.656765) | 0.682652 (0.615160–0.695289) |
| INT / 64 | 0.530445 (0.526918–0.631507) | 0.654646 (0.535394–0.671151) | 0.630828 (0.575247–0.659267) |
| BIGINT / 4096 | 0.177614 (0.170333–0.179082) | 0.114341 (0.098210–0.180525) | 0.187168 (0.109108–0.223839) |
| INT / 4096 | 0.099210 (0.094892–0.128949) | 0.179145 (0.154373–0.208797) | 0.112457 (0.104145–0.200835) |

All native medians exceed paired stock. BIGINT medians also exceed previous fallback, while
INT medians improve it; ranges overlap. Small-array jobs are dominated by startup and do not
establish sustained throughput. This is accepted bounded collection groundwork without a
standalone acceleration claim or performance-only opt-in.

Optimization stops after two profiling/optimization passes. The wide BIGINT capture at 64 groups
has 15,403 samples: 3,999 inclusive samples in ARRAY_AGG getValue, 3,940 self samples in
LinkedList.toArray, 1,288 in array copying and 569 in Arrow array writing. Those snapshots retain
released ownership semantics; no safe small shortcut was identified. The second capture,
INT at 4096 groups, has 25,319 samples, including 902 in grouped processing, 158 in getValue,
114 in array copying and 55 in Arrow array writing. All JVM threads, planning and compilation
are included, and counts overlap. These are not allocation counts or copied-byte measurements.
Snapshot materialization, boxing/copying and startup remain work for the
[native SQL performance backlog](https://github.com/datafusion-contrib/StreamFusion/issues/239).
No production Rust, aggregate runtime or state/ownership protocol changes.

Reproduce both types and cardinalities after building the release library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=array_agg,array_agg_int -Djvmagg.rows=50000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Also run with `-Djvmagg.keys=4096`. Previous-code controls copy the same benchmark source and
set `-Djvmagg.expectNative=false`. For CPU captures set `SF_PROFILE_JVM_AGG=true`, select
`#profileArrayAgg`, use one case at a time, and pass `-Djvmagg.seconds=8 -Dprofile.asprof=/path/to/asprof`.
The two captures choose `array_agg`/64 groups and `array_agg_int`/4096 groups. Profile timings
remain separate from the unprofiled trial table.

## Character grouping keys

Integer COLLECT and, on released Flink 2.2, integer ARRAY_AGG now admit STRING/VARCHAR and
CHAR grouping keys through the existing serialized-key boundary. Character aggregate results
and unverified collection consumers retain fallback. [Coverage](../operators/group-by.md#character-grouping-keys)
records the runtime parity, Unicode/collision, parallel and repeated-recovery checks. The broad
regression matrix passes 233 checks on Flink 2.2.1 and 192 with 41 version skips on 1.18.1.

The same release library, host and complete-job boundary above are retained. COLLECT uses
200,000 rows and ARRAY_AGG 50,000; both use BIGINT values with the same eight-value domain and
every-eighth-value NULL. STRING keys have 16 prefix characters at 64 groups and 264 at 4096,
starting with `é`, padded with `x`, then decimal key digits. Each engine has two warmups and
three alternating trials per fresh JVM, parallelism one, an 8 GiB heap and no accumulator cache.
Previous candidate `1296aa62` uses identical benchmark source and verified stock fallback;
current jobs require consumed NativeJvmGroup work. Retained revision `435a8922` differs from
the measured prior candidate only in diagnostic parser code/tests and benchmark documentation;
its production and benchmark Java/native code is identical. No local tests/builds overlap timings.

Median seconds (minimum–maximum), including planning, source/sink, keyed state and both transposes:

| Shape | Groups | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| COLLECT | 64 | 0.418451 (0.343207–0.439635) | 0.470467 (0.332164–0.592387) | 0.442228 (0.369217–0.507034) |
| COLLECT | 4096 | 0.697764 (0.557432–0.710529) | 0.530568 (0.529844–0.619316) | 0.675497 (0.658833–0.731337) |
| ARRAY_AGG | 64 | 0.617899 (0.531926–0.694289) | 0.620905 (0.534293–0.760524) | 0.807764 (0.652013–0.808651) |
| ARRAY_AGG | 4096 | 0.210483 (0.130218–0.237499) | 0.194822 (0.190731–0.233336) | 0.261850 (0.240825–0.297522) |

These short jobs show substantial startup/JIT variability. Native results do not establish
acceleration; slower results remain visible under the accepted collection tradeoff. The
[artifact](results/collection-string-keys-2026-10-24.json) retains all trials, including each
previous JVM's paired stock controls and exact configuration.

Optimization closes after two captures using the same eight-second native-only profiling
protocol above. COLLECT at 4096 wide-string groups has 17,078 samples: 4,045 inclusive in
released group processing, 962 in result snapshots and 903 in output append. String-key hashing
and equality account for 1,253 and 659 self samples respectively. ARRAY_AGG at 64 groups has
16,893 samples: 6,023 inclusive in group processing, 3,981 in result snapshots and 1,495 in append;
LinkedList.toArray accounts for 3,925 self samples. Counts overlap and cover all JVM threads,
planning and compilation; they do not count allocations or copied bytes. No safe small shortcut
was identified within the two passes. Managed-state key work, growing result snapshots and
startup remain follow-up work in [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).
The grouped operator, key/state codecs and JNI ownership protocol are unchanged.

For reproduction, use the COLLECT/ARRAY_AGG commands above with `-Djvmagg.keyType=string`,
`-Djvmagg.keyPrefixChars=16 -Djvmagg.keys=64`, then prefix 264/keys 4096. Keep 200000 COLLECT
rows and 50000 ARRAY_AGG rows, `values=8`, two warmups and three trials. Compare the same
benchmark source against `1296aa62` with `expectNative=false`; current requires `true`.
For profiles select `#profileCollect` at 4096 keys or `#profileArrayAgg` at 64 keys with the same
STRING inputs, `SF_PROFILE_JVM_AGG=true` and `jvmagg.seconds=8`.

## Small integer elements

TINYINT/SMALLINT COLLECT and Flink 2.2 ARRAY_AGG reuse the same released handlers, state
and columnar grouped operator. [Coverage](../operators/group-by.md#small-integer-collection-elements)
records typed-source limits, NULLs, changelog histories, consumer work and repeated recovery.
The broad matrix passes 253 checks on Flink 2.2.1 and 202 with 51 version skips on 1.18.1.

The release library, host and complete-job boundary above are retained. COLLECT uses 200,000
rows and ARRAY_AGG 50,000; INT keys have 64/4096 groups. Runtime BIGINT values cycle through
eight values per group, with every eighth value NULL, then pass through a TINYINT/SMALLINT CAST
before grouping on both engines. Both perimeter transposes, the native Calc cast, managed
state, source/sink and planning/submission remain in the native timing. Arrays still grow to
781/782 or 12/13 elements; the eight-value domain does not cap their length. Parallelism is one,
heap 8 GiB, accumulator cache 0, with two warmups and three alternating trials per engine.
No local tests/builds/other benchmarks overlap measurements.

Prior candidate `083ee5cf` uses identical benchmark source and the same frozen native DSO,
with verified whole-query stock fallback. Candidate runs require consumed NativeJvmGroup work.
The [artifact](results/small-integer-collections-2026-10-24.json) retains all trials, including
paired stock controls for each prior JVM, configuration and benchmark-source checksum.

Median seconds (minimum–maximum):

| Shape | Element | Groups | Paired stock | Previous fallback | Native route |
| --- | --- | ---: | ---: | ---: | ---: |
| COLLECT | TINYINT | 64 | 0.384040 (0.377551–0.426808) | 0.409516 (0.378629–0.477743) | 0.450483 (0.395221–0.462711) |
| COLLECT | SMALLINT | 64 | 0.334492 (0.303697–0.373923) | 0.308490 (0.304946–0.390241) | 0.316711 (0.316301–0.317838) |
| COLLECT | TINYINT | 4096 | 0.419148 (0.395668–0.439195) | 0.473465 (0.460230–0.479073) | 0.444146 (0.427199–0.493480) |
| COLLECT | SMALLINT | 4096 | 0.359677 (0.332119–0.360388) | 0.353041 (0.351100–0.427021) | 0.369802 (0.351690–0.467924) |
| ARRAY_AGG | TINYINT | 64 | 0.607137 (0.556293–0.666531) | 0.643704 (0.602796–0.664181) | 0.722009 (0.658594–0.729609) |
| ARRAY_AGG | SMALLINT | 64 | 0.685984 (0.533384–0.710872) | 0.566705 (0.546968–0.574309) | 0.725947 (0.638138–0.746778) |
| ARRAY_AGG | TINYINT | 4096 | 0.188956 (0.185946–0.216100) | 0.194958 (0.167390–0.200184) | 0.192006 (0.182821–0.213037) |
| ARRAY_AGG | SMALLINT | 4096 | 0.097794 (0.095426–0.193721) | 0.091185 (0.091072–0.178126) | 0.100762 (0.091968–0.173542) |

Most native medians exceed paired stock. Comparisons with prior fallback vary, and overlapping
ranges/startup costs prevent a sustained acceleration claim. These costs ship under the
accepted collection tradeoff; unfavorable results remain in the table.

Optimization stops after two eight-second target CPU captures using the native-only protocol
above. SMALLINT COLLECT at 4096 groups has 19,872 samples: 3,855 inclusive in released group
processing, 876 in result snapshots, 1,017 in output append and 356 in Arrow map writing.
TINYINT ARRAY_AGG at 64 groups has 15,295 samples: 5,153 inclusive in group processing, 3,441
in result snapshots, 1,121 in append and 483 in array writing; LinkedList.toArray contributes
3,408 self samples. These overlapping counts cover all JVM threads, planning and compilation;
they are not allocation or copied-byte counts. No safe small shortcut was identified.
Managed state, growing result snapshots, output work and startup remain performance follow-ups
in [#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

For reproduction, select `jvmagg.cases=collect_tinyint,collect_smallint` with `rows=200000`,
then `array_agg_tinyint,array_agg_smallint` with `rows=50000`; run each at `keys=64` and `4096`.
Keep `values=8`, two warmups, three trials and all other commands/settings above. Compare the
same benchmark source against `083ee5cf` with `expectNative=false`; current requires `true`.
The two captures select `#profileCollect` with
`-Djvmagg.cases=collect_smallint -Djvmagg.keys=4096`, and `#profileArrayAgg` with
`-Djvmagg.cases=array_agg_tinyint -Djvmagg.keys=64`. Keep `SF_PROFILE_JVM_AGG=true`,
`jvmagg.seconds=8` and an async-profiler path as above.

## Legacy row consumers

Flink 2.2 legacy row exits now admit direct verified integer collection groups through the
prepared-sink metadata boundary. [Coverage](../operators/group-by.md#legacy-row-consumers)
retains the exact gates; this adds no native connector or new state/ownership protocol. The
local broad matrix passes 267 checks on 2.2.1 and 208 with 59 version skips on 1.18.1. Agent
checks pass 56 with one skip, plus six scope and 25 summary checks. The unchanged upstream
COLLECT fixture passes seven/six cases with explicit contracts: two native backend variants
on each line consume 21 JVM-group rows, and all other variants keep their expected fallback.

These release/mimalloc measurements retain the host/library above, INT keys at 64/4096 groups,
parallelism one, an 8 GiB heap and no accumulator cache. COLLECT uses 200,000 runtime rows;
NOT NULL BIGINT ARRAY_AGG uses 50,000 and `COALESCE(v,CAST(0 AS BIGINT))`. Values cycle
through an eight-value per-key domain, with every eighth source value NULL. COLLECT ignores
NULL elements; ARRAY replaces them with zero. Arrays still grow to 781/782 or 12/13 elements.
Two warmups precede three alternating trials per engine in each fresh cohort JVM.

The timed legacy path calls `toRetractStream(query,Row.class).executeAndCollect()` and drains
every external insert/retract record. Planning/legacy translation, submission, state, both
row/Arrow transposes, external conversion and client collect transport remain inside timing;
cluster boot/schema setup stay outside. These results specifically describe the external
client consumer, not every possible legacy sink. Candidate jobs require consumed JVM-group
work and both transposes. Prior candidate `1ed09143`, using identical benchmark source and
the same native DSO, must have no native work for these guard-sensitive cases. Nullable
primitive arrays are not used as a fallback control; the ARRAY case has NOT NULL elements.

A two-second process sampler observed no overlapping external Java/Cargo workloads during
trials or profiles. Two raw matrix observations were its own Maven startup shells before
any warmup/timing. [All trials, configuration and profile counts](results/legacy-collection-consumers-2026-10-24.json)
also retain the prior JVMs' independent stock controls and source checksum.

Median seconds (minimum–maximum):

| Shape | Groups | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| COLLECT | 64 | 1.376737 (1.367508–1.413311) | 1.378298 (1.375485–1.384469) | 1.347112 (1.333860–1.347182) |
| COLLECT | 4096 | 1.260053 (1.243484–1.363648) | 1.359904 (1.346665–1.366877) | 1.243732 (1.210967–1.307879) |
| ARRAY_AGG NOT NULL | 64 | 2.103232 (2.099214–2.204880) | 2.189479 (2.183062–2.201101) | 2.619672 (2.444055–3.043646) |
| ARRAY_AGG NOT NULL | 4096 | 0.549519 (0.537020–0.573537) | 0.554805 (0.553129–0.591117) | 0.554460 (0.552484–0.556155) |

COLLECT medians improve these paired controls, while larger-cardinality ranges overlap and
separate-process controls drift. Growing arrays at 64 groups are slower; at 4096, medians
are close and ranges overlap. These short jobs do not establish sustained acceleration.
All unfavorable trials remain visible under the accepted collection tradeoff.

Optimization closes after two eight-second target CPU captures with the native-only warmup
protocol above. COLLECT at 4096 groups has 21,768 samples: 4,012 inclusive in group processing,
389 in result snapshots, 2,702 in append and 2,184 in client collection. Map serialization has
775 self samples and ByteArrayInputStream.read 1,537. ARRAY at 64 groups has 24,805 samples:
7,990 inclusive in group processing, 733 in result snapshots, 7,030 in append and 5,666 in client
collection. DataOutputStream.writeLong has 5,433 self samples, readUnsignedByte 2,714 and
ByteArrayInputStream.read 2,406; LinkedList.toArray has 731. Inclusive counts overlap, and
append includes chained downstream work. These are CPU samples, not allocation/copied-byte
counts. Serialization/client materialization dominates much of this exit; state snapshots
and Arrow output remain costs too. No safe small change inside the planner/runtime was
identified within the two passes. Further performance remains in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

Reproduce after a release build:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=legacy_collect -Djvmagg.rows=200000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Also run keys 4096, then `cases=legacy_array_agg_not_null` with 50000 rows at both cardinalities.
The prior checkout copies the same benchmark source and sets `expectNative=false`. For the two
profiles set `SF_BENCHMARK=true SF_PROFILE_JVM_AGG=true`, use `#profileCollect` with
`-Djvmagg.cases=legacy_collect -Djvmagg.keys=4096`, then `#profileArrayAgg` with
`-Djvmagg.cases=legacy_array_agg_not_null -Djvmagg.keys=64`. Keep the same row/domain/library settings and
`jvmagg.seconds=8`, passing an async-profiler path with `profile.asprof`. Profiled timings
do not enter the comparison table.
