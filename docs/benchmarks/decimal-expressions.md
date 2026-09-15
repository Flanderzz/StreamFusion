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
