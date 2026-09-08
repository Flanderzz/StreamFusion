# String copies and intermediate buffers

**Applies to:** the RowData→Arrow entry transpose, the synchronous lookup join, and string expressions
in [Calc / Filter](../operators/calc-filter.md)

## Entry transpose: one copy instead of two

The entry transpose's VarChar writer called `StringData.toBytes()` — a fresh `byte[]` copy — only
for Arrow's `setSafe` to copy those same bytes again. A single-segment heap `BinaryStringData` —
what every string coming out of Flink's row formats already is — now feeds its segment straight
into the Arrow buffer, halving the copies and deleting the per-string garbage on the
string-dominated entry transposes (q9/q18/q19/q20/q21/q22).

A still-lazy `BinaryStringData` — what a rowwise source delivers through `RowRowConverter`, holding
only a `java.lang.String` — additionally skips Flink's char-at-a-time `StringUtf8Utils`
materialization: the JDK's intrinsified `String.getBytes(UTF_8)` produces the identical bytes
(Flink's encoder documents JDK-equivalent output and delegates its edge cases to it) at
near-memcpy speed for ASCII. A 2026-07-12 profile had this char loop at ~9% of q20's whole job.

**Measured:** with the batched BinaryRow keys above, the string-heavy stateless q22 gained +25%
native throughput (1.23x → 1.53x) on the 2M-event generator rung.

## Lookup join: collect straight into the Arrow builders

The sync lookup join stopped defensively copying every looked-up row (`RowDataSerializer.copy`,
~27% of q13's lookup path) plus buffering them in a list. The collector now writes each row's
fields into the Arrow builders at collect time, while the runner's reused row object is still
valid — removing both the copy and the intermediate list.

## Calc: construct only the final string buffers

`CONCAT` unions its inputs' validity bitmaps before copying strings. Batches without NULL results
return DataFusion's output directly, avoiding the full UTF-8 scan that rebuilding `ArrayData` with
a replacement mask would trigger. When there are NULL results, a narrow UTF-8 kernel appends only
valid rows; NULL rows retain the preceding offset and consume no payload bytes. Each child is
evaluated once, including nested expensive or nondeterministic calls. The unchecked Arrow
constructor is valid because the kernel copies complete UTF-8 strings, checks the i32 offset limit,
and preserves the mask length; native tests explicitly validate the resulting Arrow data.

MD5 and SHA-2 use DataFusion's released `md-5` and `sha2` dependencies directly. A batch reserves
one offsets vector and exactly enough value capacity for its non-NULL fixed-width hex strings.
Each digest stays on the stack and its lowercase hex bytes go straight into that values vector,
with no per-row heap allocation, intermediate `BinaryArray`, or MD5 `Utf8View` conversion. The
output is already the `Utf8` representation that the Java boundary requires. Scalar calls retain
scalar results; sliced and empty arrays retain their validity and offset contracts. Invalid arity
returns a DataFusion error instead of panicking at expression construction.

The end-to-end diagnostics and reproduction commands live on the [coverage page](../operators/calc-filter.md#string-concatenation-and-hashes).
They include both row/Arrow transposes and a rowwise sink; their ratios are not isolated kernel
speedups.

Measured on Apple M4 Pro / JDK 17 on 2026-09-08, with release + mimalloc, 2 million rows,
parallelism 1, one warmup and best of three jobs. Both versions include upstream `a9c6ebdc`;
the baseline uses the concatenation/hash kernels introduced by `53ed0c45`. The short-string
diagnostics run together, with the nullable diagnostic in a separate JVM, for both versions:

| Diagnostic | Native before | Native after | Native throughput change |
|---|---:|---:|---:|
| Short `CONCAT` / `CONCAT_WS` | 0.740 s | 0.733 s | No material change |
| MD5 / SHA-2 | 3.261 s | 2.211 s | +47% |
| `CONCAT`, 1 KiB prefix, 75% NULL results | 0.709 s | 0.613 s | +16% |

The concatenation cases still lose to Flink end to end. Removing redundant work improves the
nullable case within the native pipeline; it does not establish a standalone concatenation win.
