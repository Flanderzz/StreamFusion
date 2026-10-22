# Binary packing for scalar string exits

Rejected during #392 on 2026-10-22. Packing cache-free scalar STRING outputs into
Flink binary rows preserved typed values, UTF-16 guards and ownership, but changed
released legacy `toAppendStream(..., RowData.class)` behavior. Unchanged Kafka 3.2
`KafkaTableITCase.testKafkaSourceSink` and `testKafkaTableWithMultipleTopics` inspect
`RowData.toString()`: they received `BinaryRowData@...` identities instead of generic
row field values. The failure occurred on PR revision `8a490951`.

Keep the source SQL and upstream assertions unchanged. The retained implementation
borrows canonical string bytes only while the normal ownership copy creates generic
rows. This removes an intermediate UTF-8 copy without changing that observable
representation; caches and represented fields keep their copying reader.

[All paired prototype trials](../../docs/benchmarks/results/collections-392-2026-10-22/followup-2.2.json),
[the 1.18 trials](../../docs/benchmarks/results/collections-392-2026-10-22/followup-1.18.json)
and [profile weights](../../docs/benchmarks/results/collections-392-2026-10-22/profiles/counters.json)
remain retained as rejected evidence. The final string measurements and their profiles
are separate in the [complete report](../../docs/benchmarks/collections-392-2026-10-22.md).
Do not restore binary packing at this boundary without preserving released consumer
behavior. Existing primitive/binary projections are outside this scoped rejection.
