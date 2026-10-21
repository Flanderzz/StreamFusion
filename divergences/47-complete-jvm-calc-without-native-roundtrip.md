# Complete generated JVM Calcs can stay on Java Arrow batches

Comet's UDF bridge exports Arrow arguments from its native executor and invokes a generated JVM
batch kernel. StreamFusion follows that pattern for expressions inside Rust Calc execution.

A complete Flink-generated Calc has a narrower opportunity: its owning Flink operator already
runs on the JVM and owns an Arrow batch, while every expression is scheduled by Java. The
experimental temporal path borrows timestamp input vectors directly and returns Arrow columns,
avoiding a native round trip that performs no useful expression work. Other shapes and
changelog-tagged input retain the native bridge. The operator remains columnar on both sides;
row conversion stays at the island edges. Function binding, lifecycle, predicates, exceptions
and call order use the same released Flink evaluator.

The reference reviewed before this change is CometUdfBridge.java and
CometBatchKernelCodegen.scala in datafusion-comet. CPU profiles with symbolized Rust frames show
Arrow FFI/ownership and allocator work in the redundant native stage. Exact timestamp output
comparison can retain Arrow buffers after every call executes; changed columns materialize their
prior prefix. This follows Arrow reference ownership and does not infer function purity.

The direct temporal batch path remains experimental and default off. Verified temporal scalar UDF
execution is default-enabled through the ordinary bridge under the accepted UDF performance
exception. See docs/operators/calc-filter.md and docs/optimizations/udf-columnar-upcall.md.
The semantic support shipped in [PR #368](https://github.com/datafusion-contrib/StreamFusion/pull/368),
closing #364; [issue #389](https://github.com/datafusion-contrib/StreamFusion/issues/389) tracks
remaining temporal conversion performance.

Already-admitted complete programs with primitive numeric/boolean inputs and ordinary
nonempty numeric/boolean/string outputs also use this direct batch evaluation for insert-only input.
Their results are independently owned flat Arrow vectors; no timestamp aliases or function-purity
assumptions are involved. String arguments, represented layouts, binary/decimal/temporal outputs,
and RowKind-tagged batches retain the existing bridge. The same function binding, row order and
exception causes remain intact.

The original direct path improved the then-opt-in INT/BIGINT/STRING jobs by about 28%/32%/10%,
while remaining slower than paired stock controls. Shared stateful UDFs now default on under
the accepted UDF performance exception. The bounded #398 follow-up moves the row loop into
the generated class for numeric/boolean programs, following Comet's generated batch-kernel
structure while preserving the existing row evaluator and writer. STRING-result programs keep
their existing direct row loop; older serialized evaluators also keep row evaluation. This
changes neither signature admission nor Arrow ownership.
The [optimization page](../docs/optimizations/udf-columnar-upcall.md#generated-batch-loops-for-shared-stateful-calls)
records current measurements, limitations and the rejected larger inlining prototype.
