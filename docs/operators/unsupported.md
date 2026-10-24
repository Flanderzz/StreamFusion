# Unsupported operators

**Status: Not supported.** These have no native matcher at all — a query containing one falls back
to Flink entirely, per the [all-or-nothing island rule](index.md#the-all-or-nothing-island).

| Operator | SQL surface | Why |
|---|---|---|
| `Correlate` (unverified shapes) | Lateral table functions outside the verified scalar JVM boundary; UNNEST types outside the Arrow bridge, unverified collection producer/consumer boundaries and nullable ROW/ordinality shapes | Fixed BINARY and container JVM table-function types, specialized/Python/async functions and legacy wrappers retain fallback; verified character, numeric, BOOLEAN, VARBINARY, interval and temporal JVM table functions are native. INNER/LEFT UNNEST of verified `ARRAY`/`MAP`/`MULTISET` operands is native, including computed MAPs, representation-sensitive ARRAYs and host-valid builtin element predicates. See [Unnest / Correlate](unnest-correlate.md) for representation, ordinality and released-host restrictions. |
| `Match` | `MATCH_RECOGNIZE` (CEP / row-pattern matching) | No native path; `MATCH_ROWTIME` and `MATCH_PROCTIME` require this operator and fall back with it. |
| `GroupWindowTableAggregate` | A `TableAggregateFunction` inside a legacy group window | No native path. Ordinary legacy `TUMBLE`/`HOP` aggregates and event-time `SESSION` are native — see [Window aggregate](window-aggregate.md). |
| `IncrementalGroupAggregate` | The five-node chain a distinct aggregate plans to only when `table.optimizer.distinct-agg.split.enabled` is on (partial local → incremental → final global over a bucket key) | Deliberate non-goal: the knob mitigates state-backend hot-key skew that an in-process distinct set doesn't exhibit. The default mini-batch plan for distinct aggregates — `LocalGroupAggregate` + `GlobalGroupAggregate` with a distinct `MapView` partial — **is** native; see [GROUP BY](group-by.md). |
| `GroupTableAggregate` | `TableAggregateFunction` | No native path. |
| `Values` | Constant relations | No native path. (A non-temporal `Sort` is parity, not a gap — Flink itself rejects it in streaming.) |
| `DeltaJoin` | A join Flink rewrites into a delta join under `table.optimizer.delta-join.strategy` | No native path. A block containing a delta join is not rejected by the `FORCE` guard, but ordinary admission and island checks still apply; `DeltaJoin` remains unsupported. Under `FORCE`, a block containing an ordinary join and *no* delta join is left unchanged so Flink can perform its statement-wide validation — see [Global switches](index.md#global-switches). |
| `LegacyTableSourceScan`, `LegacySink` | Legacy (pre-`DynamicTableSource`/`Sink`) connectors | The connectors stay on Flink. Verified direct integer collection groups may feed a legacy row sink through a transpose; see [Legacy row consumers](group-by.md#legacy-row-consumers). Other unverified legacy boundaries retain fallback. |
| `Python*` (`PythonCalc`, `PythonCorrelate`, `PythonGroupAggregate`, `PythonOverAggregate`, …) | PyFlink UDFs | No native path — a Python UDF can't run inside a native island. |

Pattern matching, including `MATCH_ROWTIME` and `MATCH_PROCTIME`, is tracked in
[issue #78](https://github.com/datafusion-contrib/StreamFusion/issues/78).
