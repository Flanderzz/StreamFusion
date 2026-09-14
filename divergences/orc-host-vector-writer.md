# ORC encoding stays in the host Java library

The host-owned file lifecycle and Arrow ownership model follow Arroyo and DataFusion Comet.
ORC decoding uses released orc-rust 0.9.0; its Arrow 59 output enters the engine's Arrow 58 through
C Data. ORC writing uses Flink's or Paimon's released Java vectorized writer. The shared file codec
accepts Arrow roots and owns an encoder object, allowing Java ORC to avoid a redundant JNI trip
while Parquet retains its native C Data encoder. Neither writer constructs intermediate rows.

Paimon's shaded Hive vectors and Flink's unshaded vectors have the same public layout. The common
converter resolves public fields and methods by class, then uses primitive arrays inside its hot
loops. Host-specific factories keep writer options, physical streams and Paimon field IDs typed
against each connector's actual released API. Closing releases codecs and ORC memory-manager
registrations, including failures; an abort discards footer bytes and leaves the host stream alone.

This replaces the maintained ORC C++ integration and its build chain. Measurements favored Rust
reading; optimized Java vector conversion made Java writing competitive on numeric and wide
nested fixtures. String-heavy encoding still costs more than the historical C++ baseline. Exact
measurements and limitations live on the ORC connector page and optimization ledger.

ORC's legacy timestamp encoding also aliases fractional values in the last negative second to
the first positive second. This can invalidate the sorted-run precondition of paimon-rust's
merger. Java retains those ORC snapshot splits, using original leading-key file bounds to prove
safety for native merging; non-leading fractional timestamp keys conservatively retain Java.
The differential key suite includes both this fallback and native post-epoch keys at each precision.
