# Primary-key level-0 files are written natively and handed to Paimon's writer through its compaction hook

## The decision in 31, and where it stops

[31](31-paimon-sink-via-format-spi.md) keeps Paimon's released Java connector as the table writer
and enters it through the public bundle write: the native side routes batches and encodes files,
Paimon does everything else. That entry only exists for append tables. A primary-key table's
merge-tree writer takes one row at a time into a sort buffer, applies the merge engine when the
buffer flushes, and creates its level-0 files in a private method; the sequence number every later
merge decides by is assigned per row on the way in. There is no released entry that takes a column
batch for a primary-key bucket, and a bundle fed to it would be walked row by row into the same sort
buffer, which is the cost the columnar sink exists to avoid.

## What we did

The sink owns the level-0 file of a primary-key bucket and nothing else:

- **Merge natively.** Each bucket's routed batches, row kinds included, sit in a native buffer that
  stamps arrival sequence numbers and, on flush, sorts by the memcomparable key rows already used
  for keyed state, keeps the last row per key, and drops ignored retracts before numbering, as
  Paimon's table write drops them before its writer sees a row. The flush is a batch in Paimon's
  key-value layout (key columns, sequence number, row kind, full row) with every key once, which is
  the invariant Paimon's merge reader requires of a file.
- **Write the level-0 file natively.** The merged batch goes through the same native Parquet
  encoder as an append file, rolled at the target file size, and is described the way Paimon's
  key-value writer describes a file: key bounds, key and value statistics from the footer, sequence
  range, delete count, level 0, the table's schema id. Sequence numbers are seeded from the bucket's
  committed files exactly as Paimon seeds a restored writer.
- **Hand the files to Paimon's writer.** Before Paimon's writer prepares a checkpoint's commit, the
  new files are passed to it through `notifyNewFiles`, the entry Paimon's dedicated compaction
  operator uses for files written by another job. The writer adds them to its level-0 run set,
  triggers compaction by the table's own strategy, waits or not as the table's options dictate, and
  reports compaction in its commit message; the sink merges the new files into that message. Under
  `write-only` the hand-off is inert and a dedicated compaction job compacts the files through the
  identical entry.

## Why this shape

- **No native compactor and no upstream change.** Compaction, changelog production for later
  producers, lookup, recovery, and the commit protocol are Paimon's released code operating on
  files whose content is what Paimon's own writer would have produced. A native compactor would
  have re-implemented Paimon's universal compaction and its file-level bookkeeping for no throughput
  gain on the hot path, which is the sort and merge of incoming rows.
- **Identical results by construction.** Module and MiniCluster tests write the same changelog
  through the native path and through Paimon's writer into twin tables and compare rows read back,
  every file's metadata, and Parquet footers, across restarts and through in-job compaction.
- **A whitelist, not a merge-engine port.** Only the shapes whose level-0 file the native merge
  reproduces are admitted: fixed buckets, `deduplicate` with optional `ignore-delete`, no
  changelog production, no deletion vectors, no sequence field, no thin mode, and key types whose
  Arrow byte order equals Paimon's key comparator. Everything else falls back to the stock sink at
  planning time.

## Consequences

- **Compaction rewrites are stock speed**, as for append tables (31).
- **An idle Paimon writer is closed and reopened.** Paimon's writer sees files, not rows, so
  between checkpoints it can look idle; Paimon then closes it and the next hand-off recreates it
  with a scan of the bucket's committed files. Paimon's dedicated compactor pays the same scan. A
  bundle entry on the merge-tree writer with a sequence hand-off would remove it; that proposal is
  tracked with the remaining primary-key work in issue #33.
- **Compaction results land one checkpoint later** unless the table waits for compaction, which is
  how the stock streaming sink behaves too.
- **Memory budget.** The native buffers are bounded by `write-buffer-size` per task, spilling the
  largest bucket into level-0 files; Flink managed memory (`sink.use-managed-memory-allocator`) is
  not drawn on, so that option declines.
