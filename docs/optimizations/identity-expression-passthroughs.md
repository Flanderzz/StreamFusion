# Identity expressions admitted as passthroughs

**Applies to:** the ROWTIME materializer Calc

Flink's rowtime materializer Calc (`Reinterpret(CAST(rt))` with a widening timestamp cast)
changes only the declared type. Encoding it as an identity projection reuses the timestamp
column and keeps event-time pipelines native without copying values.

`PROCTIME()` materialization also remains native. It now uses a volatile execution-time clock,
sampled during each batch evaluation, so projected values advance with execution. Proctime
ordering in deduplication and OVER still follows arrival order; see
[Temporal functions](../operators/temporal-functions.md).
