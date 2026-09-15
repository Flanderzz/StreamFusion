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

Regression tests cover rounding carries of either sign, increased scale,
integer inputs, sliced arrays, validity, empty batches, and SQL projections,
`IS NULL`, and filters. The fixed-width rescale is also compared against the
existing arbitrary-precision HALF_UP implementation across scales and precision
boundaries. This is a correctness change; no throughput gain is claimed.
