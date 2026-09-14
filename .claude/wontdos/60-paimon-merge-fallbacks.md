# Paimon merge combinations retained on the stock writer

Issue #47 adds the remaining specialized aggregates by reusing released Java field kernels,
alongside small native extensions for decimal product, distinct listagg, and floating ordering.
Three configurations deliberately keep their planning-time stock Paimon fallback:

- `sequence.field` with partial-update or aggregation. The twin-table experiment documented in
  [divergence 36](../../divergences/36-paimon-columnar-merge-engines.md) exposed compaction-dependent
  ordering differences. Forcing compaction would change the user's policy; duplicating Paimon's
  complete buffering and compaction grouping is not a small targeted change.
- Sequence fields and partial-update sequence groups outside the verified comparable scalar
  types, including nested values. These require additional ordering and encoding parity work.
- Defaults on primary-key, partition, or bucket-key columns. The current native default fill
  runs after routing; these defaults must also affect every partition and bucket decision.

These jobs continue to work through the released Java sink. The user prefers native performance
work based on existing implementations or small targeted changes, so these fallbacks resolve the
remaining scope of #47 rather than committing to a second implementation of the writer.
Batch sinks remain outside this project's native scope.

Reopen an individual configuration when an existing upstream API or a small, benchmarked port
can preserve its semantics. Java collection and sketch kernels intentionally remain Java:
their ordering and serialized representations are already part of Paimon's compatibility contract.
