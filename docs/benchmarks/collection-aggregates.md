# Collection aggregates

The sections before [Rust collection state](#rust-collection-state) retain the measured history
of the preceding generated JVM collection handler. The default verified COLLECT/ARRAY_AGG route now uses
[Rust collection state](../operators/group-by.md#integer-collect); those historical timings
must not be read as measurements of the replacement kernel.

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

## BOOLEAN elements

BOOLEAN collection elements use the existing released handlers, managed state and Arrow
batch boundaries. [Coverage](../operators/group-by.md#boolean-collection-elements) retains
the exact gates. The broad local matrix passes 279 checks on Flink 2.2.1 and 214 with 65
version skips on 1.18.1; agent checks pass 56 with one skip. ARRAY_AGG is absent on 1.18.

These release/mimalloc comparisons retain the host/library and complete-job boundary above.
INT keys use 64/4096 groups, parallelism one, an 8 GiB heap and no accumulator cache.
COLLECT uses 200,000 rows and ARRAY_AGG 50,000. Runtime BIGINT values cycle through eight
values per key and become BOOLEAN through `(v>3)`, retaining NULL every eighth row.
Non-NULL groups have two possible BOOLEAN values; key7mod8 groups contain only NULLs.
Array lengths still grow to 781/782 or 12/13 elements. Planning, submission, generated
comparison, keyed state, output, source/sink and both row/Arrow conversions remain timed.
Two warmups precede three alternating trials per engine, in fresh cohort JVMs.

Prior candidate `e3fd2423` uses identical benchmark source and the same DSO. Its BOOLEAN
queries require no native work; current jobs require consumed JVM-group work and both
transposes. A two-second sampler observes no overlapping external Java/Cargo workload.
The matrix and profile samplers each see one own Maven startup shell before its first
warmup/job; both are classified in the [retained evidence](results/boolean-collections-2026-10-24.json),
alongside every trial, separate prior stock controls, configuration and profile counts.

Median seconds (minimum–maximum):

| Shape | Groups | Paired stock | Previous fallback | Native route |
| --- | ---: | ---: | ---: | ---: |
| COLLECT | 64 | 0.326653 (0.319330–0.338287) | 0.363584 (0.304675–0.405923) | 0.393280 (0.351090–0.430312) |
| COLLECT | 4096 | 0.345625 (0.338376–0.410258) | 0.356046 (0.349711–0.364927) | 0.368659 (0.366698–0.395571) |
| ARRAY_AGG | 64 | 0.756527 (0.667153–0.859359) | 0.660542 (0.656398–0.720487) | 0.849646 (0.771984–0.894879) |
| ARRAY_AGG | 4096 | 0.187237 (0.185224–0.190696) | 0.208227 (0.207688–0.212281) | 0.215862 (0.172185–0.228775) |

All four native medians exceed paired stock; previous-fallback comparisons vary and ranges
overlap. Short jobs and startup costs do not establish sustained throughput. These measured
costs ship under the accepted collection tradeoff, without an acceleration claim.

Optimization closes after exactly two eight-second target CPU captures, each after an
unprofiled native-only job. BOOLEAN COLLECT at 4096 groups has 22,403 samples: 3,109
inclusive in group processing, 595 in result snapshots and 822 in append. BOOLEAN ARRAY_AGG
at 64 groups has 14,669 samples: 5,274 inclusive in group processing, 3,666 in snapshots and
1,062 in append; LinkedList.toArray contributes 3,631 self samples. State/key operations,
growing list snapshots, output and compilation remain visible costs. Inclusive counts
overlap and include all JVM threads, planning and compilation; they are not allocation
or copied-byte counts and do not causally isolate the regression against stock. No safe
small shortcut was identified inside the two passes. Further optimization remains in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239).

Reproduce using the complete-job command above with `jvmagg.cases=collect_boolean`,
`rows=200000`, then `array_agg_boolean`, `rows=50000`, each at `keys=64` and `4096`.
Keep `values=8`, two warmups, three trials, `expectCache=false` and `expectNative=true`;
the prior checkout copies the same benchmark source and requires `expectNative=false`.
For the two captures set `SF_BENCHMARK=true SF_PROFILE_JVM_AGG=true`, select
`#profileCollect` with `cases=collect_boolean`, `keys=4096`, then `#profileArrayAgg` with
`cases=array_agg_boolean`, `keys=64`. Retain the same rows/domain/library and pass
`jvmagg.seconds=8` plus an async-profiler executable through `profile.asprof`. Profiled
timings do not enter the comparison table.

## Rust collection state

Verified primitive COLLECT and Flink 2.2 ARRAY_AGG now use Rust state rather than the generated
JVM handler. These release/mimalloc comparisons use Flink 2.2.1, JDK 17, parallelism one, an
8 GiB heap, INT grouping keys and no JVM accumulator cache. COLLECT uses 200,000 runtime rows;
ARRAY_AGG uses 50,000. Values cycle through an eight-value per-key domain with every eighth
source value NULL. BOOLEAN uses `(v>3)`; narrow integers use the corresponding CAST. Two
warmups precede three alternating stock/native trials per fresh cohort JVM. The ordinary
row-to-Arrow batch preference is 1,024 rows; operator coalescing retains its production defaults.

Each complete-job timer includes planning/submission, source and blackhole sink, keyed state,
result materialization and both perimeter transposes. The plan and consumed-work checks require
the intended Rust or JVM aggregate and both conversions; stock controls require no native work.
The preceding implementation is selected by `rustCollectionAggregates=false` in the same
candidate/library, retaining its unchanged released JVM handler and managed state. Each prior
cohort also has independent stock controls, retained in the artifact. This isolates the state
replacement instead of comparing against an unrelated older fallback.

A two-second process sampler excludes external Java/Cargo/Rust/C++ workloads from all eight
final cohorts. The driver waits for external work to finish; an earlier overlapping cohort is
excluded from the comparison. [All 320 trials, configuration, source/library hashes and profile
counts](results/native-collection-state-2026-10-27.json) retain warmups and every unfavorable
measurement. Median seconds (minimum–maximum):

| Function / element | Groups | Paired stock | Previous JVM handler | Rust state |
| --- | ---: | ---: | ---: | ---: |
| COLLECT / BIGINT | 64 | 0.410145 (0.401101–0.419614) | 0.393537 (0.381584–0.423012) | 0.393378 (0.383773–0.404893) |
| COLLECT / BOOLEAN | 64 | 0.284471 (0.237033–0.301163) | 0.330348 (0.246450–0.333776) | 0.364429 (0.362557–0.367449) |
| COLLECT / TINYINT | 64 | 0.328230 (0.315705–0.346342) | 0.337080 (0.327306–0.346345) | 0.365508 (0.364999–0.368424) |
| COLLECT / SMALLINT | 64 | 0.325115 (0.318091–0.332782) | 0.333629 (0.330462–0.340949) | 0.367524 (0.366840–0.372131) |
| COLLECT / BIGINT | 4096 | 0.460723 (0.421718–0.490280) | 0.435095 (0.375358–0.460587) | 0.401804 (0.390525–0.457847) |
| COLLECT / BOOLEAN | 4096 | 0.358457 (0.294082–0.364548) | 0.356140 (0.272621–0.356972) | 0.399951 (0.381539–0.406156) |
| COLLECT / TINYINT | 4096 | 0.387977 (0.368634–0.399182) | 0.378671 (0.361047–0.379734) | 0.376801 (0.369625–0.380383) |
| COLLECT / SMALLINT | 4096 | 0.367354 (0.366178–0.402927) | 0.363072 (0.350699–0.370132) | 0.377457 (0.375738–0.383737) |
| ARRAY_AGG / BIGINT | 64 | 0.664409 (0.570414–0.683745) | 0.776970 (0.732698–0.982627) | 0.264972 (0.247862–0.326301) |
| ARRAY_AGG / BOOLEAN | 64 | 0.630213 (0.617130–0.732652) | 0.907002 (0.689615–0.959422) | 0.218179 (0.217967–0.232675) |
| ARRAY_AGG / TINYINT | 64 | 0.617490 (0.579958–0.646764) | 0.857952 (0.739006–1.040668) | 0.298172 (0.296794–0.303482) |
| ARRAY_AGG / SMALLINT | 64 | 0.671095 (0.557645–0.689471) | 0.933262 (0.775930–1.092321) | 0.328728 (0.325788–0.334471) |
| ARRAY_AGG / BIGINT | 4096 | 0.214582 (0.192095–0.241505) | 0.188545 (0.180697–0.228420) | 0.205435 (0.097222–0.214132) |
| ARRAY_AGG / BOOLEAN | 4096 | 0.168819 (0.168175–0.203160) | 0.182682 (0.173632–0.211900) | 0.109156 (0.105359–0.169975) |
| ARRAY_AGG / TINYINT | 4096 | 0.105090 (0.087479–0.109616) | 0.094128 (0.085395–0.102327) | 0.099306 (0.093339–0.108791) |
| ARRAY_AGG / SMALLINT | 4096 | 0.094673 (0.094157–0.113881) | 0.103676 (0.100000–0.104341) | 0.102178 (0.095571–0.113202) |

ARRAY_AGG at 64 groups improves paired stock by 2.04–2.89× and the preceding handler by
2.84–4.16× in these jobs. At 4,096 groups the arrays contain only 12/13 elements and timings
are much shorter; medians/ranges overlap in several cells. COLLECT is mixed: BIGINT improves
paired stock, while BOOLEAN and several narrow-integer shapes remain slower. These short,
startup/JIT-sensitive jobs do not establish sustained throughput or a universal acceleration
claim. Slower cases ship as correctness-verified native-state groundwork under the maintainer's
explicit two-pass agreement, with no performance-only opt-in.

Optimization stops after two eight-second CPU captures, sampled only in native jobs after two
unprofiled warmups. The first, BIGINT COLLECT at 64 groups, has 14,552 samples: 4,425 inclusive
in Rust group update, 2,089 in result-column finishing, 1,974 in generic scalar-to-array conversion
and 272 in collection result-type construction. Reusing the operator's immutable result type
removes that per-row schema construction. The initial 64-group BIGINT native median was
0.455002 s and the final median is 0.393378 s; separate cohort/JIT variation prevents attributing
the entire difference to that small change.

The second and final capture, BIGINT ARRAY_AGG at 64 groups after that change, has 15,396 samples:
1,519 inclusive in Rust group update and 525 in result-column finishing; boxed Long construction
and RowData copying have 1,489 and 578 self samples. The Arrow-to-row exit and array
reader have 2,341 and 1,582 inclusive samples. JVM compiler threads account for 6,223/7,799
inclusive samples respectively. Counts overlap and include all JVM threads; they are CPU samples,
not allocation requests or copied-byte measurements. The remaining output materialization,
boxed-array reconstruction, equality work and startup need broader changes; they remain in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239). No further tuning pass is used.

Reproduce after building the release library:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Dsf.testForks=1 -Djvmagg.cases=collect,collect_boolean,collect_tinyint,collect_smallint \
  -Djvmagg.rows=200000 -Djvmagg.keys=64 -Djvmagg.values=8 \
  -Djvmagg.warmup=2 -Djvmagg.runs=3 -Djvmagg.expectNative=true \
  -Djvmagg.expectCache=false -Dstreamfusion.experimental.jvmAccumulatorCacheEntries=0 \
  -Dstreamfusion.experimental.rustCollectionAggregates=true
```

Repeat at 4,096 keys, then select the analogous `array_agg` cases with 50,000 rows. Repeat each
cohort with `rustCollectionAggregates=false` for the preceding handler. The CPU captures use
async-profiler 4.6, `-e cpu --cstack dwarf -i 1ms`, and an eight-second recording after two
unprofiled native-only jobs. Profiled timings are excluded from the comparison table.


## DATE and TIME elements

The temporal extension uses Rust collection state, typed DATE/TIME runtime Row sources and
rowwise blackhole sinks. Input logical values, NULL frequency, group counts and value domains
match across stock, previous-code and candidate jobs. The complete-job timer retains planning,
submission, keyed state, output materialization and both row/Arrow transposes. Cluster setup and
initial explain remain excluded. Native jobs require consumed native grouped-aggregate work;
previous code retains its planning fallback for these element types.

The benchmark accepts `collect_date`, `collect_time`, `array_agg_date` and `array_agg_time`.
DATE values are epoch-day offsets minus three; TIME values are one millisecond plus whole
seconds from the same bounded value domain. Every eighth input remains NULL. ARRAY_AGG is
N/A on 1.18. The two-pass optimization limit applies to the temporal increment as a whole.

Release/mimalloc measurements on 2026-10-27 use Flink 2.2.1, OpenJDK 17.0.20.1, Linux
x86-64/WSL2 and an Intel Core i7-12650H, parallelism one and an 8 GiB heap. COLLECT has
200,000 source rows; ARRAY_AGG has 50,000. Each engine has two warmups and three alternating
measurements at 64/4096 INT groups, with the default zero-entry accumulator cache and local
Arrow IPC enabled. The paired previous-code JVM runs canonical `2b199b0e` with the identical
benchmark source and frozen release library; its temporal aggregates retain whole-query fallback.
No competing Java/native builds or tests were observed during any measured cohort. Measurements
predate the final NOT NULL-argument admission guard; all measured inputs are nullable and
retain the identical native operator/state path. A final fixture smoke verifies that routing.

[All trials, native-work witnesses, provenance and profile counts](results/temporal-collections-2026-10-27.json)
retain both revisions' paired stock controls. Median seconds (minimum–maximum):

| Function / element / groups | Paired stock | Previous fallback | Rust collection state |
| --- | ---: | ---: | ---: |
| collect_date / 64 | 0.428242 (0.304938–0.438641) | 0.302913 (0.298356–0.430385) | 0.380309 (0.373960–0.380745) |
| collect_time / 64 | 0.303027 (0.294204–0.384323) | 0.311299 (0.296475–0.320968) | 0.368347 (0.359526–0.393264) |
| collect_date / 4096 | 0.448308 (0.337164–0.499592) | 0.387096 (0.325800–0.388156) | 0.397779 (0.384294–0.414325) |
| collect_time / 4096 | 0.378311 (0.356739–0.408316) | 0.334876 (0.328102–0.434807) | 0.397740 (0.386408–0.399494) |
| array_agg_date / 64 | 0.536786 (0.517886–0.570472) | 0.572140 (0.528304–0.652226) | 0.266243 (0.247365–0.306507) |
| array_agg_time / 64 | 0.738173 (0.733791–0.801741) | 0.610721 (0.543917–0.819238) | 0.282258 (0.280377–0.282639) |
| array_agg_date / 4096 | 0.191791 (0.186951–0.223859) | 0.165888 (0.089291–0.181733) | 0.110610 (0.100197–0.115949) |
| array_agg_time / 4096 | 0.099233 (0.098050–0.103836) | 0.097837 (0.083647–0.105374) | 0.111883 (0.106399–0.118756) |

At 64 groups, DATE/TIME ARRAY_AGG improve paired stock medians by 2.02×/2.62×, including both
transposes and materialization. DATE COLLECT improves paired stock medians at both cardinalities
but exceeds previous fallback. TIME COLLECT exceeds stock and previous fallback; the small
4096-group TIME ARRAY_AGG also exceeds both controls. Ranges and stock controls vary across JVMs.
These startup-inclusive complete jobs establish workload-specific costs, not universal sustained
throughput. Slower cases remain bounded collection groundwork under the existing collection
tradeoff, without an acceleration claim.

The investigation stops after two eight-second CPU captures, with one native warmup before each.
TIME COLLECT/64 groups has 16,800 samples: grouped processing has 3,778 inclusive samples,
scalar-to-Arrow output assembly 1,730, result emission 858 and nested equality 450. The collection
counter update itself has only 39. TIME ARRAY_AGG/4096 groups has 20,327 samples, including 991
in grouped processing, 126 in scalar-to-Arrow assembly and 214 in result emission; compilation,
planning and other JVM work dominate this short-job capture. Counts overlap and include all
threads; no allocation count or copied-byte measurement is claimed. No safe small state shortcut
was identified. Generic output assembly, equality and startup remain work in
[#239](https://github.com/datafusion-contrib/StreamFusion/issues/239). No third optimization pass.

Reproduce after building the release library and compiling the benchmark with `-Pbench`:

```sh
SF_BENCHMARK=true mvn -pl streamfusion-runtime surefire:test@default-test -Pbench \
  '-Dtest=JvmAggregateBenchmark#completeJobsRetainKeyedStateAndBothTransposes' \
  -Djvmagg.cases=collect_date,collect_time -Djvmagg.rows=200000 \
  -Djvmagg.keys=64 -Djvmagg.values=8 -Djvmagg.warmup=2 -Djvmagg.runs=3 \
  -Djvmagg.expectNative=true -Djvmagg.expectCache=false
```

Repeat at 4096 groups; use `array_agg_date,array_agg_time` and 50,000 rows for arrays.
Previous-code controls use the same benchmark source and release payload with
`jvmagg.expectNative=false`. For the two CPU captures set `SF_PROFILE_JVM_AGG=true`, select
`#profileCollect` / `#profileArrayAgg`, choose the corresponding single case and cardinality,
and pass `-Djvmagg.seconds=8 -Dprofile.asprof=/path/to/asprof`. Profile timings are excluded
from the trial table.
