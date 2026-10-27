# Native collection signatures

The remaining verified STRING collections, general ARRAY_SORT, constructors and container
casts execute on owned Arrow batches. These measurements cover the implementation in
[#533](https://github.com/datafusion-contrib/StreamFusion/pull/533), compared with canonical
main `d98dc2f2` and stock Flink. They measure complete jobs, including both perimeter
transposes. They do not establish sustained kernel throughput or a universal speedup.

The maintainer explicitly accepted correctness-verified groundwork for
[#505](https://github.com/datafusion-contrib/StreamFusion/issues/505) after two profiling and
optimization passes. Slower cases ship under that agreement. The implementation preserves
existing native fast paths and the documented correctness guards; no performance switch is added.

## Method

Candidate `a8f8027c` and previous `d98dc2f2` use released Flink 2.2.1, OpenJDK
17.0.20.1, Linux x86_64 under WSL2, and an Intel Core i7-12650H with 16 logical CPUs.
Both native libraries are release builds with mimalloc. Production baseline sources are
unchanged; only the identical `DynamicCollectionBenchmark` fixture is overlaid to expose its
numeric-STRING, integer-MAP and MULTISET input switches on both revisions.

Each cohort starts a fresh JVM with an 8 GiB heap, parallelism one and the default 1,024-row
transpose batches. Each engine processes 200,000 rows per trial, with two warmups and five
measured trials. Engine order alternates every trial; cohort order alternates across profiles.
No local task builds or tests overlap timing. CPUs are unpinned and host background activity
is uncontrolled. Paired stock measurements from both cohorts remain visible to expose drift.

The timer includes SQL planning/execution, job startup, source generation, JNI, production
serializers, RowData-to-Arrow and Arrow-to-RowData, and the rowwise blackhole sink. Input
construction and source work are identical across controls. Short jobs remain startup sensitive.
Before timing, the harness requires both transposes in the native plan. Every native trial
also requires a consumed NativeCalc input metric, the requested root function in the function
diagnostics, and either an empty JVM function list for the candidate or the named generated
function for the previous revision. These assertions run outside the timer.

STRING arrays contain nullable elements and Unicode/NUL strings with a 16-byte ASCII suffix.
Widths are 8 or 64 with equal-sized domains; every seventh slot is NULL and every eighth
container is NULL. Signed integer arrays use the same widths/domain/null schedule. Parsing
fixtures instead contain signed ASCII decimal strings. MAP fixtures contain three entries
(`a`, `b`, `c`); MULTISET fixtures contain three integer elements with occurrence counts
2, 3 and 4. Their container is NULL every eighth row. The `width` property does not change
these three-entry MAP/MULTISET fixtures.

## Complete-job results

Seconds are medians with measured minimum/maximum in brackets. Negative percentages mean
less elapsed time. The profile identifiers map to exact SQL and fixture properties in
[provenance and commands](native-collections-2026-10-27/provenance.json).

| Profile | Previous stock | Previous SF | Candidate stock | Candidate SF | Versus previous SF | Versus paired stock |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| string-distinct8 | 0.229 [0.228, 0.317] | 0.426 [0.419, 0.454] | 0.239 [0.234, 0.329] | 0.302 [0.292, 0.314] | -29.2% | +26.5% |
| string-distinct64 | 2.520 [2.502, 2.536] | 4.080 [4.067, 4.200] | 2.365 [2.344, 2.479] | 3.571 [3.556, 3.619] | -12.5% | +51.0% |
| string-remove64 | 0.878 [0.869, 0.881] | 2.402 [2.164, 2.446] | 0.859 [0.856, 0.899] | 1.397 [1.379, 1.411] | -41.8% | +62.5% |
| string-reverse64 | 0.812 [0.808, 0.818] | 2.385 [2.323, 2.419] | 0.809 [0.804, 0.819] | 1.357 [1.353, 1.372] | -43.1% | +67.6% |
| string-position64 | 0.615 [0.597, 0.622] | 1.345 [1.322, 1.585] | 0.581 [0.570, 0.594] | 0.876 [0.859, 0.886] | -34.9% | +50.7% |
| string-sort8 | 0.231 [0.222, 0.233] | 0.433 [0.414, 0.516] | 0.242 [0.236, 0.254] | 0.293 [0.284, 0.410] | -32.2% | +21.3% |
| integer-sort64 | 0.595 [0.591, 0.615] | 0.662 [0.657, 0.684] | 0.600 [0.593, 0.619] | 0.541 [0.538, 0.558] | -18.2% | -9.8% |
| split-dynamic | 0.199 [0.194, 0.204] | 0.349 [0.335, 0.436] | 0.196 [0.190, 0.205] | 0.248 [0.229, 0.340] | -28.8% | +27.0% |
| split-empty | 0.309 [0.307, 0.315] | 0.773 [0.748, 0.846] | 0.327 [0.321, 0.384] | 0.412 [0.407, 0.431] | -46.7% | +25.8% |
| string-row8 | 0.190 [0.179, 0.195] | 0.361 [0.332, 0.434] | 0.193 [0.190, 0.261] | 0.249 [0.242, 0.322] | -31.0% | +29.1% |
| map-cast | 0.230 [0.218, 0.298] | 0.340 [0.328, 0.404] | 0.224 [0.210, 0.294] | 0.263 [0.257, 0.288] | -22.4% | +17.6% |
| map-union | 0.239 [0.236, 0.303] | 0.344 [0.339, 0.444] | 0.246 [0.229, 0.306] | 0.235 [0.233, 0.305] | -31.8% | -4.5% |
| parse-array64 | 0.849 [0.833, 0.863] | 1.645 [1.640, 1.648] | 0.819 [0.810, 0.840] | 1.007 [0.995, 1.019] | -38.8% | +23.0% |
| format-array64 | 0.377 [0.356, 0.442] | 0.459 [0.442, 0.475] | 0.376 [0.355, 0.408] | 0.696 [0.678, 0.741] | +51.6% | +85.3% |
| boolean-array64 | 0.229 [0.210, 0.238] | 0.345 [0.338, 0.377] | 0.252 [0.218, 0.280] | 0.304 [0.296, 0.317] | -11.9% | +20.7% |
| multiset-cast | 0.226 [0.214, 0.259] | 0.263 [0.251, 0.317] | 0.225 [0.219, 0.249] | 0.238 [0.230, 0.302] | -9.4% | +6.0% |

Fifteen profiles have lower medians than the previous evaluator. Integer sort improves by
18.2% versus the previous evaluator and 9.8% versus paired stock, with separated measured
ranges in this run. MAP_UNION has a 4.5% lower median than stock but overlapping ranges;
this does not establish a repeatable stock advantage. The other fourteen profiles trail
stock. ARRAY-to-STRING formatting regresses by 51.6% versus the previous evaluator and
85.3% versus stock. It creates per-element decimal text and a represented cached STRING
result; reducing those allocations and the remaining boundary materialization belongs to
[#534](https://github.com/datafusion-contrib/StreamFusion/issues/534). No further optimization
passes are added to this bounded implementation.

[All 448 observations](native-collections-2026-10-27/trials.csv) retain 128 warmups separately
from 320 measured trials. [Execution evidence](native-collections-2026-10-27/execution-evidence.txt)
retains the named function routes and all 224 consumed NativeCalc witnesses, each with
200,000 input rows. The provenance includes both library hashes and the identical benchmark
fixture hash. The broader local SQL matrix passes on Flink 2.2.1 and 1.18.1: 347 cases on
each line, with respectively 7 and 88 skips for released capability/established exclusions,
and no failures. Unchanged upstream runtime contracts and integration suites remain CI gates.

## Bounded profiling work

The two passes targeted the shared STRING boundary: returning a single owned BinaryStringData
wrapper with its original Java cache, and avoiding flag-vector allocation when an array already
has the required generic copy category. Both preserve serializer snapshots and ownership.
The profiles identified ArrayDataSerializer copies and BinaryStringData materialization as
remaining costs. Those two changes did not establish a repeatable elapsed-time or allocation
improvement by themselves. Earlier temporary profile recordings did not survive the paused
session; the final raw repeated comparisons below are retained independently.

STRING set operations also retain stable candidate scans because Flink's mixed cached/cold
equality is nontransitive for isolated surrogates. Replacing these with an ordinary Eq hash set
would change results. General sorts preserve Java TimSort's comparison schedule and failures.
MAP casts preserve Java HashMap ordering and key replacement. These are compatibility costs,
not evidence of acceleration.

Remaining boundary and kernel performance work is tracked in
[#534](https://github.com/datafusion-contrib/StreamFusion/issues/534). Additional admission,
state and sink boundaries remain with
[#234](https://github.com/datafusion-contrib/StreamFusion/issues/234).

## Reproduction

Build each revision from its own checkout with the same benchmark helper. From `native/`:

```sh
cargo build -p streamfusion --release --features mimalloc
```

Compile Java with `mvn -pl streamfusion-runtime -Pbench test-compile
-Dnative.build.skip=true -Dnative.test.resource.skip=true`, then run from the repository root:

```sh
SF_BENCHMARK=true mvn -B -ntp -pl streamfusion-runtime -Pbench \
  surefire:test@default-test -Dtest=DynamicCollectionBenchmark \
  -Dnative.build.skip=true -Dnative.test.resource.skip=true \
  -Dcollection.rows=200000 -Dcollection.warmup=2 -Dcollection.runs=5 \
  '-Dcollection.expression=ARRAY_DISTINCT(arr)' \
  -Dcollection.map=false -Dcollection.width=64 -Dcollection.type=STRING \
  -Dcollection.bytes=16 -Dcollection.rustFunction=ARRAY_DISTINCT
```

For the previous revision use `collection.generatedFunction` in place of
`collection.rustFunction`. The provenance artifact records every expression, property and
command, including the three fixture switches. Change only those matched inputs to reproduce
another profile.
