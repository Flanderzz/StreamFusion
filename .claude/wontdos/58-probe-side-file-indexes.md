# Probe-side file indexes for the Paimon state backend (bloom, exact key sets)

**Status: rejected 2026-07-27 — superseded by deletion-vector mode with synchronous barrier
maintenance.**

Two successive attempts at making the per-batch key probe skip data files on the miss-heavy,
high-cardinality workload (Nexmark q18's `(bidder, auction)` dedup), both shipped briefly and
both removed:

1. **Bloom file index** (schema-stamped `file-index.bloom-filter.columns`, Java-compatible
   reader/writer in the paimon-rust fork). Falsified by arithmetic: a bloom answers one key at a
   time, so a file is skipped only when *every* probed key misses — probability
   `(1-fpp)^|probe|` ≈ 0 for a few thousand probes at any practical fpp. Batched probes
   false-positive into every file; the apparent wins were run-to-run variance. The fork's bloom
   commits remain upstream-worthy for paimon-rust (single-key lookups are the right use).

2. **Exact per-pin key-set index** (per-file `HashSet<i64>` of key hashes, built from a
   key-column-only read once per pinned snapshot). Deterministic and it worked — q18 went from
   bimodal 0.47–2.29× to a stable ~1.8× — but it holds ~12–16 bytes of resident memory per
   *physical table row* (LSM levels overlap, so a key can appear in several files' sets). Memory
   proportional to disk rows defeats the point of a disk state backend; RocksDB's equivalent
   (block-cache-resident bloom filters) is ~10 bits/key. Rejected on Jordan's call.

**What replaced them:** `deletion-vectors.enabled` on the state tables plus synchronous lookup
compaction at every barrier (Paimon's own `lookup-wait` model, run by the Java compactor). Every
committed snapshot holds only level-1+ standalone-correct files, so every read is a raw parquet
scan with exact `IN` pushdown (key column decodes first, value columns only for matches) and the
deletion vectors applied as row masks — no merge reads, no resident index, no compaction race.
The disk-backed key→position lookup index Paimon maintains for the compaction lookups
(`LookupLevels`, bounded local cache) is the principled form of what both rejected designs
approximated in memory.

**Reopen if:** a workload shows the per-interval raw-scan key decode dominating at state sizes
where a persisted per-snapshot key index (LookupLevels analog on the read path) would pay — that
is the flagged follow-up, not a revival of resident-memory indexes.
