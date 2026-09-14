# ORC uses the released Apache C++ implementation

Arroyo's filesystem source and Comet's Arrow/JNI ownership patterns remain the model: the host
owns file lifecycle and the native boundary exchanges columnar batches. ORC differs from Parquet
because the released, full ORC implementation is C++. It is isolated behind a small C ABI in its
own optional DSO, with the existing Rust JNI guard and ownership layer. It does not link Arrow C++
or another deployable StreamFusion DSO. nanoarrow copies between ORC's vectors and Arrow C Data;
strings may borrow input buffers only during the synchronous writer call.

The build consumes checksum-pinned canonical release archives and does not patch third-party
sources. StreamFusion's CMake settings choose static PIC dependencies and published zlib rather
than ORC's default Git dependency. Darwin cross-builds make protobuf's host generators universal
and supply the known 64-bit Darwin time_t probe results. This avoids requiring Rosetta.

ORC C++ exposes fewer writer controls than Java. Configuration admission preserves verified
settings and retains stock behavior for the rest. Timestamp paths currently require UTC because
the C++ and Java timezone implementations differ for DST gaps and historical dates. Footers are
checked before source emission; unsupported files keep Java decoding. No reader changes engine
after it has emitted part of a split.

ORC's legacy timestamp encoding also aliases fractional values in the last negative second to
the first positive second. This can invalidate the sorted-run precondition of paimon-rust's
merger. Java retains those ORC snapshot splits, using original leading-key file bounds to prove
safety for native merging; non-leading fractional timestamp keys conservatively retain Java.
The differential key suite includes both this fallback and native post-epoch keys at each precision.
