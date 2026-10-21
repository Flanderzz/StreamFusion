# Native RocksDB memory pool leased beside the delegate's, not shared with it

## Flink's decision

Flink's RocksDB backend bounds all RocksDB memory in a slot with one shared
`LRUCache` + `WriteBufferManager` pair, allocated through the slot's
`MemoryManager` shared-resource lease and sized from managed memory (or the
`state.backend.rocksdb.memory.fixed-per-slot` / `fixed-per-tm` overrides) with
the `write-buffer-ratio` split formulas.

## What we did instead

The native stores follow the same options, the same precedence, the same split
formulas, and the same slot-scoped lease machinery — but their cache and
write-buffer manager are created inside StreamFusion's own RocksDB library and
leased under a separate resource id. The delegate `EmbeddedRocksDBStateBackend`
(JVM keyed state, timers, host fallback operators) keeps its own frocksdbjni
pool.

Native operators retain their own pool lease through handle initialization, recovery and
close. Flink can close the keyed backend concurrently with canceled-task initialization;
its slot lease alone does not cover the native lifetime. As with Comet's explicit native
memory acquisition/release through `CometTaskMemoryManager`, the operation's owner retains
the resource until native work has ended. The existing slot allocation is reference-counted,
without allocating or charging another pool. Closing the final backend lease prevents new
operator leases; the native objects are freed after the final existing operator closes.

## Why

A C++ `Cache`/`WriteBufferManager` cannot be handed across two independently
linked RocksDB libraries; sharing the delegate's Java-owned objects with the
Rust-owned instance is not possible. Sizing our pool from the same budget and
leasing it externally (rather than reserving managed memory a second time)
avoids double-reserving the slot's managed memory, which would fail the task.
The cost is that a slot running both native stores and substantial JVM-delegate
RocksDB state holds two pools of the configured size; in accelerated jobs the
delegate's databases are near-empty, so the second pool stays at its floor.

Two smaller gaps ride along: the rust-rocksdb binding exposes no
high-priority cache pool (`memory.high-priority-pool-ratio` is read by the
delegate only), and `memory.partitioned-index-filters` is not applied to the
native instance.

## Arena allocation in small shared pools

Flink checks the default arena quantum against the shared write-buffer manager's
mutable limit and warns about constant flushing when it is too large
(`RocksDBOperationUtils.sanityCheckArenaBlockSize`). Native stores instead derive
the usual one-eighth quantum from the smaller of the configured write-buffer size
and the shared manager capacity, aligned to RocksDB's 4 KiB minimum. An 8 MiB
arena from the default 64 MiB write buffer can otherwise exceed the mutable limit
of a 10 MiB slot pool and crowd out its block cache even after winner changes stop.

This changes allocation and local flush behavior while preserving the configured
buffer limit, total memory budget, persisted rows and checkpoint formats. Larger
pools that can hold the configured buffer retain the default quantum. Production
options-file and live-memtable tests cover small and sufficient shared budgets.
The Top-1 shared-pool Criterion fixtures include 64 MiB configured buffers with
10 MiB pools; setup and initial state are outside the timed update/flush boundary.
