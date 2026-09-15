# Operators

Every page in this section marks one Flink operator (or operator family) as **native**, **partial**
(native with specific, enumerated gaps), or **unsupported** — see [Unsupported
operators](unsupported.md) for the operators with no native path at all. Together these pages are
the precise answer to "why didn't my query accelerate?" — everything not called out as a gap here
runs natively.

## The all-or-nothing island

A query accelerates only if it forms **one fully-columnar island**: every operator but a rowwise
source/sink runs natively, exchanging Arrow batches, with the row↔Arrow transpose paid once at
each host edge and never between native operators. **One unsupported interior operator drags the
whole query back to Flink** — there's no partial acceleration of a single query. Use
`NativePlanner.explain(...)` or `-Dstreamfusion.logFallbackReasons=true` to see the recorded
reason(s) for a given plan.

**What counts as a fallback.** A fallback is something *Flink executes that StreamFusion doesn't
accelerate* — a real gap that could be closed. It is **not** a fallback when Flink itself rejects
the query in streaming (e.g. `RANK`/`DENSE_RANK` Top-N, non-time `ORDER BY`) — matching Flink by
also not running it is parity, not a gap.

## Timestamp values and event time

Timestamp readers expose Flink's signed epoch milliseconds plus a non-negative
nanosecond remainder within the millisecond. For example, `-1` nanosecond is
`(-1, 999999)`, not `(0, -1)`. Flink BinaryRow key encoding reads these components
directly, including nested timestamp keys; it does not multiply milliseconds into
an `i64` nanosecond count. Event-time readers for sort, window-aggregate input and
watermarks use the millisecond component. The JVM temporal-function bridge preserves
both components for generated expressions and reads milliseconds for millisecond-only builtins.
Readers accept Arrow second, millisecond, microsecond and nanosecond timestamp
columns without interpreting the Arrow timezone label as a timezone conversion.

This reader contract does **not** yet expand end-to-end timestamp range: row-to-Arrow
writers, timestamp-producing expressions and several state/output paths still use
nanosecond columns. The complete representation and consumer/state migration remain
tracked in [#64](https://github.com/datafusion-contrib/StreamFusion/issues/64).
Timestamp-producing functions are not enabled on the strength of reader compatibility
alone. See [the timestamp contract](https://github.com/datafusion-contrib/StreamFusion/blob/main/divergences/39-timestamp-value-contract.md).

## Global switches

- **`-Dstreamfusion.native.enabled=false`** — master switch; run entirely on Flink.
- **`-Dstreamfusion.operator.<name>.enabled=false`** — keep one specific operator on the host. See
  [Configuration](../configuration.md) for the full flag surface.
- **Insert-only guard** — every operator except the changelog-aware ones (`GROUP BY`, regular join,
  a CDC source, `Calc`, `UNION ALL`, `Expand`, changelog normalize, streaming Top-N/`LIMIT`)
  requires an insert-only input; a retracting/updating input falls it back.

## Idle-state TTL

`table.exec.state.ttl` runs **natively** everywhere Flink applies `StateTtlConfig`: non-windowed
`GROUP BY`, changelog normalize, deduplication, the regular join, Top-N/`LIMIT`, `OVER`, and the
temporal join. Semantics match Flink exactly — every stored value carries its last-**write**
wall-clock timestamp (reads never refresh it), expiry happens at `last_write + ttl` inclusive, and
expired state reads as absent and is deleted on read. Each operator's page notes any
operator-specific expiry-granularity wrinkle (e.g. the temporal join's single per-key deadline
instead of per-row TTL).
