# Arrow columns into Java ORC vectors

Streaming filesystem and Paimon ORC sinks feed Arrow columns directly into their host's Java
vectorized writer. Flink and Paimon supply different released ORC/Hive packages, so two small
typed factories create the writer and a common converter fills either vector layout. This
removes the maintained ORC C++ adapter and build chain while retaining columnar input.

## Current technique

The converter reuses a Hive batch of 4096 rows and its column arrays across writes. Public
field and method lookup is cached by class; primitive conversion loops operate on Java arrays.

- Long and double columns use bulk copies from Arrow buffers. Smaller signed integers first
  copy into reusable primitive scratch arrays and then widen in a Java array loop.
- Null-free columns skip validity scanning. Other columns unpack each validity byte once for
  eight rows; boolean values use the same approach.
- Each string/binary column copies its entire contiguous payload into one reusable heap buffer.
  Hive values share that byte array with separate starts and lengths. There are no per-value
  byte arrays, strings or `setRef` calls, and buffer growth is amortized.
- Precision-18 decimals use Decimal64 vectors where the host supports them. Larger decimals
  fill the host's existing decimal objects from reusable two's-complement bytes, without
  constructing `BigDecimal` values.
- Lists, maps and structs convert child spans recursively. Slices retain null parent and child
  boundaries. The selected-row API preserves order and duplicates; production partition routing
  supplies contiguous Arrow batches.

The shared file codec accepts an existing Java Arrow root directly. Java ORC avoids exporting
that root and immediately importing it again, while Parquet still exports C Data to its Rust
encoder. Callers starting in Rust use a reusable Java Arrow root to import C Data without copying
Arrow buffers; imported buffers are released after each synchronous write. Java ORC still needs
the column copies described above because Hive vectors are heap arrays.

## Measurements

Measured September 14, 2026 with release Rust plus mimalloc, Java 17 in UTC, on an Apple M1 Max
with 64 GiB RAM. Both timed writers start from the same Rust-owned Arrow batches and finish with
a closed local ORC file. Each file has 262,144 rows, batched at 4096, with two warmups and five
alternating trials. Fixture generation, a one-time deep copy into Rust, and correctness scans
are outside timing. Handoff, conversion, encoding and close are inside timing.

Compared with the original Java vector adapter, production Java reduced median elapsed time by
8–29% across the 15 numeric, string/binary and nested schema/codec combinations. For uncompressed
files, numeric time fell from 20.491 to 14.641 ms, strings/binary from 48.316 to 43.127 ms, and the
14-column nested schema from 176.953 to 145.731 ms. The ZSTD numeric result, 16.572 ms, is within
2% of the historical C++ result of 16.333 ms. Java remains slower on the string-heavy fixture;
the shared conversion work does not replace Java ORC's encoder.

The [raw writer results](../benchmarks/orc-java-writers-2026-09-14.csv) include ranges and file sizes.
The [ORC page](../connectors/orc.md#writing-from-arrow-rs) gives the reproducible command, full
methodology and historical C++ comparison, including the unequal LZ4 SPEED compression work.
These are local file microbenchmarks, not whole-job throughput or peak-memory measurements.

The conversion arrays and Java ORC buffers consume Java heap. Paimon size estimates include
conversion storage and the writer's buffer estimate; this is not a Flink managed-memory
reservation. Tests compare every column with stock Paimon vectorization, including nested
nulls, slices, selected duplicates, decimals and timestamps, and verify field IDs, file settings
and output ownership. Flink's unchanged ORC SQL suite exercises the actual filesystem writer.
