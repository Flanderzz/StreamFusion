# Flink decimal result types and overflow

Flink 2.2.1 resolves the result precision and scale before execution. Its
`DecimalData.fromBigDecimal` rounds HALF_UP to that scale, then returns NULL if
the rounded value exceeds the declared precision. Overflow is observable by
surrounding predicates and CASE expressions as well as the row reader.

DataFusion's ordinary decimal CAST uses Arrow's strict cast options and can fail
the query on the same overflow. Exact-source casts therefore use a prepared
native rescale with an explicit precision check. Float and string casts retain
the existing Flink runtime path. All result columns still use Decimal128;
neither the JNI schema nor checkpoint representation changes.

The shared rescale supports both i128 values and i256 arithmetic intermediates.
As in DataFusion Comet's wide decimal expression, wider arithmetic is an
implementation detail, not a Decimal256 column at the Java boundary. StreamFusion
uses Flink's declared result type and NULL semantics rather than Spark's evaluation
modes. Powers and precision bounds are prepared outside the row loop; integer
and decimal inputs are downcast once, and scalar inputs stay scalar.

Add, subtract, and multiply also bypass generic DataFusion arithmetic followed
by CAST: a Decimal128 intermediate can overflow before that cast, and a product
can have scale above 38 even when its final rounded value fits. Fused expressions
first attempt checked i128 arithmetic, then widen to i256 when necessary. Two
valid input decimals bound the wide intermediate by `2 * 10^76`, within i256.
Only the final rounded result is checked against the declared precision. A
precision overflow sets output validity to NULL; it never leaves an invalid
decimal marked valid. Both-scalar expressions return a scalar for safe broadcast.

Regression tests cover rounding carries of either sign, increased scale,
integer inputs, sliced arrays, validity, empty batches, and SQL projections,
`IS NULL`, CASE, filters, and grouping. The fixed-width rescale and arithmetic are compared against the
existing arbitrary-precision HALF_UP implementation across scales and precision
boundaries. Division retains its separate 38-significant-digit rounding stage;
aggregate overflow state machines are unchanged. End-to-end per-expression
measurements are recorded in `docs/benchmarks/decimal-expressions.md`.

Modulo retains arbitrary-precision arithmetic because its scale alignment and
quotient can exceed Decimal128. Flink applies `MathContext(38)` to the integral
quotient as well: mathematical remainder alone is insufficient. A quotient
needing more than 38 significant digits fails with `Division impossible`;
trailing zeroes may be removed to represent large exact powers without an
error. SQL tests exercise both failures and the allowed large quotients, and
native tests cover negative signs and non-zero remainders. This is an error
semantics fix; no modulo speedup is claimed.

Flink 2.2.1 generates per-row AND/OR branches, whereas DataFusion 54's
`BinaryExpr` can evaluate the right operand across a mixed batch (AND's selective
filtering is only a heuristic, and OR has no mixed-row selection). A correct
decimal error must not become an error on a row Flink never evaluates. The same
recursive admission check used for fallible SQL/JSON therefore declines decimal
MOD and division beneath AND/OR, including nested projections and filters.
The restriction covers both the finite-quotient error and division by zero.
CASE result branches retain native evaluation because DataFusion selects their
rows before evaluating them. SQL regressions use mixed TRUE/FALSE/NULL guards,
failing values on skipped rows, and valid values on evaluated rows; direct
remainder error tests still verify Flink's `Division impossible` behavior.

For non-nullable operands Flink may still declare arithmetic results `NOT NULL`.
An overflow then reaches the shared downstream constraint enforcer and fails
the job. SQL tests check the same failure on both engines; correcting the native
NULL bitmap must not silently remove a Flink-declared constraint.
