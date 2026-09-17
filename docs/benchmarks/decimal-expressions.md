# Decimal expression benchmarks

Measured on 2026-09-15 with Apple M4 Pro, JDK 17, UTC, Flink 2.2.1,
DataFusion 54.0.0, and a release native library with mimalloc. These are
end-to-end correctness-baseline measurements for the exact decimal kernels.

Each query evaluates one expression over 2,000,000 rows at parallelism 1.
The rowwise DataStream source and blackhole sink remain in the measured path.
The harness verifies `NativeCalc`, `RowDataToArrow`, and `ArrowToRowData` in
every native plan. Flink and Native alternate execution order in one JVM,
with two warmups and five measured runs per engine per case. The table reports
median seconds. No other tests or benchmarks run concurrently.

| Input type | Expression | Flink (s) | Native (s) |
| --- | --- | ---: | ---: |
| DECIMAL(18,3) | CAST(a AS DECIMAL(12,2)) | 0.338 | 0.753 |
| DECIMAL(18,3) | a + b | 0.324 | 0.804 |
| DECIMAL(18,3) | a - b | 0.331 | 0.794 |
| DECIMAL(18,3) | a * b | 0.324 | 0.795 |
| DECIMAL(38,20) | CAST(a AS DECIMAL(28,6)) | 0.401 | 0.741 |
| DECIMAL(38,20) | a + b | 0.382 | 0.835 |
| DECIMAL(38,20) | a - b | 0.378 | 0.825 |
| DECIMAL(38,20) | a * b | 0.410 | 0.864 |

Both operands use the listed type. The source cycles through four fixed
values (positive, negative, fractional, and zero), with independent left/right
positions and every sixteenth left value NULL. The wide multiplication case
includes intermediates above i128 as well as results that overflow to NULL.
The sink uses Flink's inferred expression type, so no additional output cast
is introduced by the benchmark.

These standalone row-fed queries are slower with Native in every measured
case. The changes fix already-admitted expressions that previously threw or
produced invalid values, and provide the shared exact arithmetic needed by
further decimal support. The measurements do not isolate kernel time or
establish the cost of either transpose; they should not be described as a
kernel speedup or extrapolated to an existing native pipeline. Input data,
NULL rate, and JIT history also affect the result.

## Reproduce

Run with JDK 17, one timing process at a time:

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  '-Dnative.cargo.packages=-p streamfusion' \
  -Dtest=DecimalExpressionBenchmark -Dsf.testForks=1 \
  -Ddecimal.rows=2000000 -Ddecimal.warmup=2 -Ddecimal.runs=5
```

The harness prints individual trials for inspection; raw output is not
versioned as documentation. See [Calc / filter](../operators/calc-filter.md)
for admission and [the decimal semantics note](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/38-exact-decimal-result-types.md)
for the result-type and overflow contract.

## TRY_CAST coverage

Measured on 2026-09-17 with Apple M4 Pro, JDK 17, UTC, Flink 2.2.1 and a
release native library with mimalloc. The scalar harness uses 2,000,000 rows,
parallelism 1, two warmups and five measured runs per engine, alternating
engine order. Native plans assert both transposes and NativeCalc. No other
test or benchmark process ran alongside these measurements; ordinary desktop
background activity was present. Times include the row source and blackhole sink.

| Expression / control | Flink median (s) | Native median (s) |
| --- | ---: | ---: |
| STRING identity, matched source | 0.366 | 0.564 |
| DECIMAL(38,9) identity, matched source | 0.254 | 0.629 |
| TRY_CAST(s AS DECIMAL(38,9)) | 0.402 | 0.961 |
| TRY_CAST(n AS DECIMAL(20,2)) | 0.256 | 0.572 |

The string source cycles through `123456789`, `-2147483648`, `  +0042.9  `,
`0`, and `2147483647`. Decimal input alternates
`12345678901234567890.123456700` and `-0.000000100`, so narrowing also measures
overflow-to-NULL. These timing inputs have no NULLs; malformed strings and NULLs
are covered separately by SQL correctness tests.

Both standalone native queries are slower than the previous Flink fallback.
The support retains exact conversions inside larger native islands; it is not
a demonstrated standalone speedup. The identity controls show the end-to-end
baseline but do not isolate kernel or transpose costs.

```sh
TZ=UTC SF_BENCHMARK=true mvn -pl :streamfusion-runtime test -Pbench \
  '-Dnative.cargo.packages=-p streamfusion' \
  '-Dtest=ScalarFunctionBenchmark#individualFunctions' \
  -Dscalar.functions=TRY_STRING_TO_DECIMAL,TRY_DECIMAL_NARROW \
  -Dscalar.rows=2000000 -Dscalar.warmup=2 -Dscalar.runs=5 -Dsf.testForks=1
```

## TRUNCATE coverage

The same run, source, release library and trial method produced these results
for DECIMAL(38,9). The positive position returns DECIMAL(32,2); the negative
position returns DECIMAL(30,0).

| Expression | Flink median (s) | Native median (s) |
| --- | ---: | ---: |
| TRUNCATE(n, 2) | 0.282 | 0.601 |
| TRUNCATE(n, -3) | 0.325 | 0.665 |

These isolated row-fed queries also remain slower than the prior Flink fallback.
The shared fixed-width kernel adds exact coverage for existing native islands;
no standalone speedup is claimed. To reproduce, use the scalar command above
with `-Dscalar.functions=DECIMAL_TRUNCATE_POS,DECIMAL_TRUNCATE_NEG`.
