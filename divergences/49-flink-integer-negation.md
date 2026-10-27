# Integer negation retains Flink's scalar overflow behavior

Arroyo's projection planner delegates expression construction to DataFusion. Comet's
non-ANSI negative expression likewise uses Arrow `numeric::neg_wrapping` for arrays.
StreamFusion follows that columnar execution shape: verified input-reference chains reuse
DataFusion's existing physical negative expression and its Arrow kernel, avoiding generic
scalar-function argument allocation and dispatch.

Released DataFusion 54 uses checked `ScalarValue::arithmetic_negate` for scalar operands,
even though its arrays wrap. Flink's generated unary arithmetic casts the negated Java
primitive back to its resolved result type: signed minima wrap to themselves for all four
integer widths. StreamFusion therefore registers one integer-only DataFusion scalar function
for literal-rooted chains, whose array path delegates to Arrow and whose scalar path uses
wrapping negation. Both paths
retain input types and NULLs. Floating and temporal negation keep their existing paths;
DECIMAL negation keeps released-Flink generated evaluation.

Admission is limited to direct inputs/literals, lossless integer widening and nested
admitted negation. Extracting an enclosing generated expression can expose different overflow
or division failures in its child, so arithmetic, narrowing and other unverified producers
stay inside that program. This restriction preserves values and failures, not independent
exception ordering. Remaining exact numeric signatures are tracked in
[#491](https://github.com/datafusion-contrib/StreamFusion/issues/491).

References consulted before design: Flink `ScalarOperatorGens.generateUnaryArithmeticOperator`,
Arroyo `arroyo-planner` projection/builder, DataFusion 54 physical `NegativeExpr` and scalar
arithmetic, and Comet `spark-expr` negative expression. No JNI, allocator or Arrow ownership
protocol changes are required.
