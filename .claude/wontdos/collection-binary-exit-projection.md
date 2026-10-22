# Generated binary exit for collection values

Rejected during issue #392 on 2026-10-22. Extending Flink's generated binary-row
projection to ARRAY/MAP and ordinary nested ROW outputs preserved the ownership
checks but did not justify the extra packing at the Arrow-to-row boundary.
On one million rows, width-64 BIGINT reverse took 1.005108 s versus 0.903946 s
with the preceding exit; paired stock controls were 0.725640 and 0.734968 s.
Width-64 prepend took 0.871043 versus 0.763461 s, with stock 0.756450 and
0.722663 s. MAP construction was effectively unchanged at 0.446799 versus
0.445830 s; other medians moved in both directions.

These exploratory runs use release/mimalloc, JDK 17, Flink 2.2.1, parallelism one,
a 2 GiB heap, one warmup and three alternating measured pairs. Short jobs, JIT
warmup and separate-JVM control drift prevent attributing every difference to
the projection. They provide no robust gain that warrants retaining this broader
path. [All exploratory trials](../../docs/benchmarks/results/collections-392-2026-10-22/exploratory.json)
remain separate from the final comparison; a subsequent intermediate map-view
pass overlapped an external integration workload and is diagnostic only.

The collection projection prototype was removed. Scalar strings without caches
or represented children borrow bytes during a generic-row ownership copy, measured
in the [current ledger](../../docs/optimizations/collection-representation-work.md).
Do not repeat collection packing without a different measured reduction in work.
