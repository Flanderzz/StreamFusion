# Scalar function allocation and text scans

These techniques describe the current function kernels inside native Calc. The
[scalar function benchmark page](../benchmarks/scalar-functions.md) reports Flink and native
complete-job results for the same workloads, including both row/Arrow transposes. Those
measurements do not isolate kernel cost, and native admission does not guarantee a speedup
for each standalone query.

The optional Criterion `scalar_functions` benchmark invokes production UDFs on 4,096-row Arrow
arrays, including scalar adaptation, NULL handling, and result allocation. Input creation,
planner coercion, JNI, and transposes are excluded from that diagnostic:

```sh
cargo bench --manifest-path native/Cargo.toml --features mimalloc --bench scalar_functions
```

Run one benchmark at a time. Kernel diagnostics and complete SQL jobs measure different work.

## LOCATE

The two-argument form reverses operands into DataFusion strpos. The three-argument Rust kernel follows DataFusion's batch ASCII detection, one reusable memmem::Finder for literal needles, and memmem searches for column needles. Constant needles/starts stay scalar, following Comet's contains pattern. Shared positioning code preserves Flink's empty-needle and signed-start rules; no JVM callback is added.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#locate).

## GREATEST

Integer and matching-scale Decimal extrema use primitive Arrow comparison loops, folding constants once and combining validity masks. ASCII-provable string/Boolean extrema reuse DataFusion with Flink's strict NULL mask. This avoids intermediate Boolean selection arrays and expanded scalar arrays.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#greatest).

## LEAST

Reuses the shared extremum kernel with minimum comparison, including primitive Arrow loops, folded constants, and strict NULL masking. Matching decimal scale is preserved on the output.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#least).

## TRANSLATE

ASCII mappings use direct lookup; non-ASCII codepoints use the project's ahash map. Consecutive equal alphabets reuse the mapping. Duplicate source positions retain their first mapping, including deletion mappings.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#translate).

## BTRIM

Default and literal-set trimming both reuse DataFusion's btrim kernel. Keeping one implementation avoids a duplicate space-only scan while preserving the verified literal-set semantics.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#btrim).

## ELT

A scalar index returns the selected array with its buffers/NULL mask intact. Longer dynamic outputs are sized before writing; short strings keep a single writing pass to avoid a selection vector larger than their payload.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#elt).

## OVERLAY

Copies intact UTF-8 prefix/replacement/suffix slices when UTF-16 cuts align with codepoints; ASCII needs no UTF-16 conversion. Cuts inside surrogate pairs retain full UTF-16 reconstruction, recombined pairs, and Java's encoding of lone surrogates.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#overlay).

## ENCODE

The verified UTF-8 form reinterprets the same Arrow offsets, values, and validity as Binary, so the kernel copies no payload. The charset stays scalar through invocation, avoiding a full-length constant array; single-byte charsets reuse a scratch buffer.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#encode).

## DECODE

A successful safe Arrow Utf8 construction validates the BinaryArray once and reuses its buffers. Malformed byte sequences use the existing JDK-compatible replacement decoder and one reusable output buffer. This avoids per-row output allocation on valid UTF-8 without assuming that arbitrary bytes are valid.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#decode).

## JSON_UNQUOTE

Combines first-token JSON validation and unescaping in one scan, writes UTF-8 directly, and retains only a pending high surrogate instead of a whole intermediate UTF-16 vector. Flink's treatment of trailing text remains covered by SQL differential tests.

[Complete-job Flink/native results](../benchmarks/scalar-functions.md#json_unquote).

