# Basic type coverage measurements

## Temporal two-phase DISTINCT (2026-10-16)

`TemporalDistinctBenchmark` measures `GROUP BY k, COUNT(DISTINCT v)` over DATE,
millisecond TIME and TIMESTAMP(9). Runtime row input repeats 128 distinct values per key,
with NULL every seventh row. DATE values straddle the epoch; TIME retains milliseconds
under a declared precision of zero; timestamps distinguish adjacent nanoseconds before
the epoch. Both edges remain rowwise, with production Arrow IPC shuffle, local/global
native operators and positive consumed-row metrics required.

The release build uses the system allocator, released Flink 2.2.1/JDK 17, parallelism one,
an 8 GiB JVM heap and an Apple M4 Pro with 24 GiB RAM (macOS 26.5). The shared fixture gives
both engines a 48 GiB off-heap accounting budget, which does not reserve that memory.
Two warmups precede five alternating measured trials per engine. Submission, conversion,
JNI, state, output and job completion are timed; MiniCluster creation is outside timing.

| Rows / keys | Type | Stock seconds | Native seconds | Stock/native |
| --- | --- | ---: | ---: | ---: |
| 2,000,000 / 64 | DATE | 0.876800 | 0.663156 | 1.32x |
| 2,000,000 / 64 | TIME | 0.834877 | 0.634687 | 1.32x |
| 2,000,000 / 64 | TIMESTAMP(9) | 0.864683 | 0.562558 | 1.54x |
| 10,000,000 / 1,024 | DATE | 5.210472 | 4.538462 | 1.15x |
| 10,000,000 / 1,024 | TIME | 5.048048 | 4.703607 | 1.07x |
| 10,000,000 / 1,024 | TIMESTAMP(9) | 5.352014 | 4.662786 | 1.15x |

Unmodified main `b170a474` was measured with the identical harness and release native
payload, asserting zero native substitutions. Its native-enabled fallback medians at
2M rows are 0.852630/0.820695/0.836963 seconds, and at 10M rows
4.947030/4.827481/5.177494 seconds (DATE/TIME/TIMESTAMP). Each run has its own interleaved
stock controls; cross-run absolute times are not normalized speedups. All measured samples,
including variability and baseline controls, are in the
[raw trials](temporal-distinct-trials-2026-10-16.json).
Small-job numbers include substantial fixed overhead; the 10M-row results show narrower
gains, especially for TIME. These memory-backend measurements do not claim a RocksDB speedup.

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=TemporalDistinctBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  '-Dnative.cargo.args=build --release' \
  -Dbasic.rows=10000000 -Dbasic.keys=1024 -Ddistinct.cardinality=128 \
  -Dbasic.warmup=2 -Dbasic.runs=5
```

Set `distinct.types=DATE,TIME,TIMESTAMP` to select types, or `basic.expectNative=false`
when replaying the same harness against the baseline planner. The native Criterion
`operators/typed_distinct` fixtures separately check typed membership views at distinct
cardinalities 4/256; they do not substitute for this complete-job comparison.

## Legacy floating DECIMAL casts (2026-10-16)

Legacy FLOAT/DOUBLE-to-DECIMAL is admitted by default, without an additional setting.
Direct unary casts use a proven native batch kernel when adjacent binary values bound
all possible host decimal renderings to the same HALF_UP result. Scale above 18, scaled
magnitude at least 2^52, ambiguous rounding and non-finite values retain the released
Flink expression for the complete batch. The source FLOAT first widens to DOUBLE, as in
Flink. Composed operands retain generated evaluation. These historical measurements used
whole-row JVM evaluation for competing failures; current Calc keeps independent expressions
on their normal columnar paths instead.

The same M4 Pro/Flink 2.2.1/JDK 17 release setup measures two million runtime rows with
NULL every seven rows, two warmups and five alternating complete jobs per engine.
The source and blackhole sink remain rowwise, retaining both perimeter transposes and
JNI. Successful batches avoid the scalar JVM upcall; this is the measured optimization.
The benchmark's unchanged default cardinality is 9,999 numeric values.

| Cast | Stock median (range) | Native median (range) | Stock / native |
| --- | ---: | ---: | ---: |
| FLOAT to DECIMAL(10,1) | 0.447904s (0.447384-0.455371) | 0.322528s (0.317984-0.332269) | 1.39x |
| DOUBLE to DECIMAL(15,4) | 0.367161s (0.363938-0.375215) | 0.326870s (0.321769-0.328776) | 1.12x |

At ten million rows and input cardinality two million, FLOAT measures stock/native
1.858968/1.348888s (1.38x) and DOUBLE 1.645330/1.348762s (1.22x). All five trials remain
in the raw results: stock/native ranges are 1.815736-1.866674/1.335060-1.359264s for FLOAT
and 1.636341-1.656261/1.341284-1.361720s for DOUBLE. No result memoization is used;
the same batch conversion handles low and high cardinality.

The earlier generated-expression implementation measured 0.493179/0.448349 seconds,
with separate stock medians 0.440520/0.360404. Its default-off prototype setting has been
removed. The unmodified planner's fallback measured 0.432456/0.351380 seconds with its
own stock controls. These are separate runs, so the new same-run stock comparison is
the primary speedup measure. Earlier unfavorable trials remain in the
[historical floating-cast trials](legacy-floating-decimal-trials-2026-10-16.json).
Current trials are retained in the
[numeric cast fast-path results](numeric-cast-fast-path-trials-2026-10-16.json).
These complete-job numbers include fixed costs and do not establish a speedup for every
exceptional input profile or a JVM-fallback-heavy batch.

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  '-Dtest=LegacyCastBenchmark#legacyFloatingDecimalCasts' \
  -Dsurefire.failIfNoSpecifiedTests=false '-Dnative.cargo.args=build --release' \
  -Dbasic.rows=2000000 -Dbasic.warmup=2 -Dbasic.runs=5
```

`basic.castCardinality` selects the number of distinct input values; its numeric default
is 9,999. `basic.expectNative=false` is only for replaying the same fixture against the
unmodified planner's previous fallback. It does not change engine admission.

## Legacy CHAR to DOUBLE casts (2026-10-16)

Legacy CHAR-to-DOUBLE is admitted by default. Direct unary casts parse ordinary ASCII
decimal text as correctly-rounded binary64 in native batches. Hexadecimal forms, suffixes,
special values, Unicode and malformed text retain the complete released Flink expression,
including NULL on failed legacy conversion. VARCHAR-to-floating and CHAR-to-FLOAT legacy
casts remain outside this admission. A composed operand never bypasses its generated
expression. The mixed eight-expression DECIMAL ROUND projection keeps its character sort
key, and an independent runtime DECIMAL control verifies ROUND without character conversion.

The same two-million-row nullable CHAR(8) fixture, with 1,000 input values and both transposes,
measures stock median 0.449092s (0.445728-0.455735) and native 0.384287s (0.381995-0.399833),
a 1.17x same-run speedup. The generated prototype measured 0.519314s against stock 0.458194;
the unmodified planner's fallback measured 0.402268s against its own stock control 0.397853.
The prototype option has been removed. All earlier trials remain in the
[historical character-cast results](legacy-char-double-trials-2026-10-16.json), with current
trials in the [numeric cast fast-path results](numeric-cast-fast-path-trials-2026-10-16.json).
No speedup is claimed for batches which require the released JVM evaluator.

At ten million rows, cardinality two million and CHAR(16), stock/native medians are
1.918966/1.556556s (1.23x); ranges are 1.910777-1.974244/1.523132-1.564620s.
The longer input values fit the declared CHAR length. This comparison retains the
same resources, two warmups, five alternating trials and row/Arrow boundaries.

Reproduce with the floating-cast command, selecting
`-Dtest=LegacyCastBenchmark#legacyCharDoubleCasts` instead. `basic.castCardinality` defaults
to 1,000 for this case; `basic.charLength` defaults to 8 and must fit the generated text.
For a high-cardinality run, set both explicitly, for example cardinality 2,000,000 and
CHAR length 16. Input shape and resources are identical between engines.

For the larger run, add `-Dbasic.rows=10000000 -Dbasic.castCardinality=2000000`
and, for the character case, `-Dbasic.charLength=16`.

## Shared aggregate diagnostics

These diagnostics measure complete Flink jobs from a runtime Row source to a
blackhole sink, including both RowData/Arrow transposes, JNI, native state and
final output conversion. They are separate from Nexmark. The source emits NULL
every seventh row and generates nullable BIGINT and DECIMAL(7,2) values. Shared
aggregation groups by an INT key and selects COUNT, AVG and SUM of the same value.
The benchmark asserts the intended native operator and both transposes in the
actual INSERT plan; two-phase cases require the native local aggregate as well.
Retained task metrics also verify that native operators consumed input.

Environment: Apple M4 Pro, macOS ARM64, JDK 17, released Flink 2.2.1, parallelism 1,
8 GiB JVM heap, release Rust build with the system allocator. Two warmup jobs per
engine precede interleaved stock/native measurements. All samples are retained
in [shared aggregate trials](shared-aggregate-trials-2026-10-12.json).

## Reproduction

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=SharedAggregateBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.testForks=1 -Dbasic.rows=2000000 -Dbasic.keys=64 \
  -Dbasic.warmup=2 -Dbasic.runs=5 '-Dnative.cargo.args=build --release'
```

`basic.types=BIGINT,DECIMAL` selects the types. `basic.miniBatch=false` selects
immediate single-phase aggregation. TWO_PHASE always enables mini-batch because
the released host requires it; the mini-batch size is 1,024 and the latency is
one hour, so count/end-of-input controls flushing. The default single-phase
configuration also enables mini-batch. `basic.keys` varies key cardinality.

The previous main implementation (`bf9837c0`) was measured separately with the
same harness and released library, asserting **zero native substitutions**.
Its native-enabled timings match stock fallback within ordinary run variability.
Compare each candidate against its own interleaved stock controls; absolute
times across separate JVM runs are not directly normalized speedups.

## Initial admission prototype

These preliminary runs used same-JVM handle shuffles. They diagnosed the decimal
bottleneck and are retained as development history; deployment comparisons must
disable that shortcut and retain Arrow IPC, as the `bench` profile does. The
harness now rejects execution without that setting. The Cargo argument above
deliberately selects the system allocator used for these measurements.

For 2,000,000 rows, 64 keys and five measured repetitions, the initial prototype
gave the following medians with mini-batch enabled in both phase strategies:

| Type / phase | Stock seconds | Native seconds | Stock/native |
| --- | ---: | ---: | ---: |
| BIGINT / one | 1.370 | 0.733 | 1.87x |
| BIGINT / two | 1.517 | 0.845 | 1.80x |
| DECIMAL / one | 1.367 | 0.741 | 1.85x |
| DECIMAL / two | 1.487 | 0.866 | 1.72x |

Turning mini-batch off exposed an unfavorable DECIMAL immediate path:
**3.390 s native versus 1.211 s stock**, across three measured repetitions.
BIGINT immediate remained faster (0.726 s versus 1.109 s). Sampling located the
decimal bottleneck in the outer division kernel's per-row BigInt powers and
decimal-string digit counts, rather than the SUM0 accumulator. This result
motivated retaining the exact 38-digit intermediate in bounded fixed-width
long division for 64-bit divisors, with the original BigInt path for wider
divisors. Both HALF_UP rounding steps are retained, including double rounding.

## Deployment shuffle validation

The optimized release path retains Arrow IPC and both edge transposes. Five
measured repetitions follow two warmup jobs per engine. At 1,024 groups, the
Maven `bench` profile gives:

| Type / phase | Stock seconds | Native seconds | Stock/native |
| --- | ---: | ---: | ---: |
| BIGINT / immediate | 1.117 | 0.737 | 1.52x |
| BIGINT / two-phase | 1.663 | 1.225 | 1.36x |
| DECIMAL / immediate | 1.324 | 0.758 | 1.75x |
| DECIMAL / two-phase | 1.768 | 1.234 | 1.43x |

A separate 64-group Maven `bench` run with the same deployment settings gives:

| Type / phase | Stock seconds | Native seconds | Stock/native |
| --- | ---: | ---: | ---: |
| BIGINT / immediate | 1.098 | 0.720 | 1.52x |
| BIGINT / two-phase | 1.518 | 0.829 | 1.83x |
| DECIMAL / immediate | 1.213 | 0.756 | 1.61x |
| DECIMAL / two-phase | 1.458 | 0.840 | 1.74x |

An independent release-launcher 64-group repeat gave DECIMAL immediate
0.725 s native versus 1.173 s stock (1.62x), with the same deployment settings.
This removes
the immediate decimal regression rather than enabling its slower prototype.
All samples, including the unfavorable prototype, remain in the linked JSON.
These are whole-job times, including startup; they are not isolated kernel
throughput or guarantees for every SQL shape. The helper's wider-divisor BigInt
fallback is covered by Criterion and SQL correctness checks, not a claim of an
equal speedup for arbitrary decimal division.

## Legacy basic casts

The same source/blackhole harness measures legacy STRING to DECIMAL, BOOLEAN to
VARCHAR, STRING to CHAR, CHAR to CHAR, and DECIMAL to VARCHAR. Each expression
is measured independently under `table.exec.legacy-cast-behaviour=ENABLED`.
There is no exchange in these Calc-only plans; both edge transposes and the
production JVM callback remain in the native measurement.

```sh
SF_BENCHMARK=true mvn -Pbench -pl streamfusion-runtime -am test \
  -Dtest=LegacyCastBenchmark -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsf.testForks=1 -Dbasic.rows=2000000 -Dbasic.warmup=2 -Dbasic.runs=5 \
  '-Dnative.cargo.args=build --release'
```

For 2,000,000 rows and five interleaved measured repetitions on the same
release/system-allocator environment:

| Legacy cast | Stock seconds | Native seconds | Stock/native |
| --- | ---: | ---: | ---: |
| STRING to DECIMAL(10,2) | 0.831 | 0.833 | 1.00x |
| BOOLEAN to VARCHAR | 1.000 | 0.839 | 1.19x |
| STRING to CHAR(3) | 1.017 | 0.803 | 1.27x |
| CHAR(2) to CHAR(3) | 1.019 | 0.799 | 1.27x |
| DECIMAL(7,2) to VARCHAR | 1.036 | 0.884 | 1.17x |

STRING to DECIMAL is effectively tied: its stock/native trial ranges overlap,
and the earlier independent repeat also tied. This change provides verified
coverage inside a native Calc; it does not claim a standalone parsing speedup.
The other four cases show workload-specific gains. The frozen previous main
routes all five to stock fallback with zero substitutions. All candidate and
previous trials, including variability, are retained in
[legacy cast trials](legacy-cast-trials-2026-10-12.json).
