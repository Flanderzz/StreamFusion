# Event-time sort

**Status:** Native — `ORDER BY` on a single leading ascending rowtime; a secondary order key is
the one gap.

This is Flink's temporal `Sort`: a full re-ordering of the stream by rowtime, needed because
arbitrary-lateness elements can only be placed once the watermark has passed them, so the operator
must hold and globally order a buffer rather than emit in arrival order.

The admitted shape is exactly Flink's own: the leading order key must be an ascending rowtime. A
descending or non-time leading key isn't a fallback at all — it's a non-temporal `Sort`, which
Flink itself rejects in streaming, so declining it here is parity, not a gap.

## Gap

Any **secondary** order key beyond that leading ascending rowtime falls back.

## Late rows

After emitting rows, the sorter drops subsequent arrivals at or before the last emitted
timestamp. An empty watermark does not advance this cutoff: a row behind the watermark can
still be admitted if it is newer than the last emitted row. Rows tied at a timestamp and
buffered before emission are all retained, in arrival order. This matches Flink's temporal sort.

## State layout

Because a full time order can't be partitioned by any real key, the buffer is raw keyed state
addressed under Flink's own one canonical empty key. That makes it checkpointable and restorable
exactly as Flink restores its own temporal sort, but it deliberately **cannot shard across
subtasks** — the same singleton limitation Flink's implementation has. See
[#22](https://github.com/datafusion-contrib/StreamFusion/issues/22) for that constraint.

The last emitted timestamp survives checkpoints even when the pending buffer is empty.
Memory checkpoints and canonical savepoints include it in a versioned prefix before the
pending Arrow IPC data; legacy snapshots containing only IPC remain readable. RocksDB
checkpoints retain it as reserved metadata. Recovery and memory/RocksDB backend transitions
preserve both the late-row cutoff and any pending rows.
