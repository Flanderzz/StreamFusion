# Scalar charset and JSON serialization

## UTF-16 encoding

Reference: Flink 2.2.1's ENCODE calls the JDK charset encoder. DataFusion's Spark
`string/encode.rs` provides the same UTF-16 code-unit structure, but its UTF-16
branch initializes every result with a BOM, including empty strings. The JDK writes
the BOM only when input is non-empty. StreamFusion follows the JDK and recognizes
charset aliases once in the planner.

The native encoder writes UTF-16 code units directly into an Arrow binary builder.
It avoids DataFusion Spark's per-row temporary byte vectors while retaining the
existing scalar/array invocation convention and NULL propagation. Only verified
literal charsets are admitted; no incompatible-behavior switch is needed.

Coverage is listed in [Calc/filter](../docs/operators/calc-filter.md); independent
Flink/native measurements are in [scalar benchmarks](../docs/benchmarks/scalar-functions.md).
