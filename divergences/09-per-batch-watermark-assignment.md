# Columnar watermark assignment matches Flink's deterministic per-row dropping

**Kind:** behavioral — when the event-time watermark advances relative to the
rows it governs, and which late rows are dropped.
**Status:** matched for Flink's deterministic behavior; the only residual is
Flink's own non-deterministic periodic emission (see below).

## Flink's behavior
Flink's table `WatermarkAssignerOperator` runs the watermark generator **per
row**: each record updates the running max and emits eagerly when the watermark
jumps past the auto-watermark interval. So within a run of rows the watermark can
advance *between* two rows, and a later row whose window that watermark already
closed is dropped as late (at the downstream window, against the watermark it has
received — at parallelism > 1 the min across its input channels).

## What we do
The columnar assigner is per-batch by default, but to match the per-row late-data
dropping it **slices a batch at each point its running watermark jumps past the
interval** and emits `(sub-batch, watermark)` in order — replicating Flink's
eager emission. A row that a sub-batch's watermark closes out therefore arrives at
the window *after* that watermark (across the shuffle, where Flink takes the min
across channels), and the native aggregator **drops rows whose window has already
closed** (`window_end <= current_watermark`, the highest flushed watermark). So
the deterministic late-drop is reproduced byte-for-byte — verified end to end by
`FlinkColumnarWindowSqlHarnessTest.outOfOrderWithinBatchDropsLateRowLikeHost`.

The drop is done in the aggregator (post-shuffle), not the assigner, because at
p > 1 a window's effective watermark is the min across its input channels, which a
single assigner can't know — only fine-grained watermarks flowing through the
shuffle reproduce it. That is why the assigner must slice rather than the window
filter locally.

A **monotonic-rowtime batch whose candidates do not exceed their rowtimes** can
have no within-batch late row (a later row's window can't be closed by an earlier,
smaller rowtime), so it takes a fast path:
the whole batch is forwarded with a single watermark, avoiding slice allocation
for the common in-order case.

Calendar intervals retain their month count and use Flink's `DateTimeUtils.addMonths`
directly while scanning Arrow timestamp values. The existing assigner and source
scan run in Java; reusing Flink's integer calendar routine adds no RowData conversion
or per-row JNI. We evaluate every candidate before reducing, even for monotonic
rowtimes: March 30 at 23:00 minus one month is later than March 31 at 00:00 minus one
month after both clamp to February's last day. Fixed-delay subtraction also stays
inside the reduction to preserve Flink's long overflow behavior.

Arroyo's `watermark_generator.rs` likewise evaluates its watermark expression on
the Arrow batch, but reduces with a batch **minimum** before advancing its running
maximum. We use a batch **maximum** and the eager slicing described here to match
Flink's per-row candidate maximum. Flink's source generator starts at `Long.MIN_VALUE`
and skips NULL candidates, whereas its standalone assigner starts at zero and rejects
NULL rowtimes; those contracts remain distinct.

Kafka and Paimon compute source candidates before handing off the Arrow root. Flink
calls the source watermark generator after forwarding the batch downstream, so the
root may already be closed. A source-local immutable candidate on the batch avoids
reading released buffers and leaves the record's event timestamp unchanged. It is
consumed at the source callback, not serialized as downstream row data. Paimon's final
split flush uses that candidate too, before releasing Flink's per-split output.

## Residual: Flink's own non-determinism (two sources, neither ours)
The drop depends on *when the watermark advances relative to a data row*, and two
things make that timing non-deterministic **in Flink itself** — not in anything we
add. We receive whatever watermark Flink's runtime computed; the native side only
reproduces the deterministic path.

1. **Periodic processing-time emission.** Besides the eager rule, Flink also emits
   on a 200 ms **processing-time** timer. For out-of-order data near a boundary,
   two runs of the same job can drop different rows depending on wall-clock timer
   firing — *even at parallelism 1, single input*. We replicate only the eager
   path, so we match whenever the eager rule decides the drop (the bounded /
   in-burst case, where the timer doesn't fire mid-data).

2. **Min-across-channels interleaving at parallelism > 1.** Where an operator
   combines several input channels into one watermark via `min` — a keyed
   exchange's downstream, or the two inputs of an interval join — the records and
   watermark events from those channels interleave in an order set by
   scheduling/network, which is non-deterministic. So even with perfectly
   deterministic per-jump emission, the effective watermark sitting beside a given
   row varies run to run, and a borderline row can be dropped in one run and kept
   in another. This is a property of Flink's parallel runtime (it affects stock
   Flink the same way); our slicing reproduces the *deterministic* component (a
   row arrives after the watermark that closed it on its own channel), but the
   cross-channel race is not reproducible because there is no single race outcome.

Both cases are unmatchable in principle — there is no single "correct" Flink answer
to match. In-order / monotonic rowtimes and lagging-watermark jobs have no late
rows, so neither source bites, at any parallelism (every parity test relies on
this).

## Scope
- The watermark *value* (`max(rowtime - interval)`, floored at 0, MAX at end of
  input) matches Flink exactly (`NativeColumnarWatermarkAssignerOperatorTest`).
- Slicing reproduces the eager per-row emission; the aggregator's
  `window_end <= current_watermark` drop reproduces the late-data discard. Both
  the slicing (unit test) and the end-to-end drop (parity test) are covered.
- The aggregator's `current_watermark` is carried in the snapshot metadata, so
  late-dropping survives checkpoint/restore.
