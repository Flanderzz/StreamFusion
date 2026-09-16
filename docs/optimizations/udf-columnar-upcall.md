# UDFs via a columnar JVM upcall

**Applies to:** user `ScalarFunction`s the native engine can't implement itself

A user `ScalarFunction` the native engine can't implement itself runs *inside* the island instead of
falling the whole query back to Flink: the argument columns are packed into one batch, exported over
the C Data Interface, evaluated by the real function on the JVM, and the result column imported back
— one JNI crossing per batch, never per row. The design is modelled on Comet's `JvmScalarUdfExpr`.

The JVM runs the actual user function. Argument/result conversion and call ordering still need
Flink parity checks: DECIMAL results use Flink's precision/scale conversion, and VARBINARY values
cross as raw bytes. See the [UDF admission rules](../operators/calc-filter.md#user-scalar-functions)
for nested decimal-result and repeated binary-call gates.

Functions are serialized into the operator and registered per-task at `open()`, so this survives
distributed execution, where the UDF instance must be reconstructed on each task's JVM rather than
shared from the planner.
Call-site registrations carry argument/result signatures, while the operator binding owns one
lifecycle per distinct function instance. Repeated and nested calls share initialization and
cleanup. A failed binding removes its registrations and closes successfully opened functions;
cleanup continues through function-close exceptions and retains those failures for reporting.
