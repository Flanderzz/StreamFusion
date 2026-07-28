use super::*;

/// Both sides of an event-time window join as two [`PaimonRowBufferStore`]s under one operator
/// directory (`left/`, `right/`) — the fourth range-read consumer, and the simplest: a window
/// join buffers rows per side and, on a watermark, joins and evicts every row whose window has
/// closed. The fire column is the row's `window_end` millis; both sides' fired rows come back in
/// arrival order, so the join over them is the memory path's join over its concatenated buffers.
/// Nothing else persists: outer-join match state is transient within one flush (both sides of a
/// window close together, so the inner join over the closed rows sees every potential match).
/// The snapshot token packs both snapshot ids and both arrival sequences.
pub(crate) struct PaimonWindowJoinStore {
    pub(crate) left: PaimonRowBufferStore,
    pub(crate) right: PaimonRowBufferStore,
    last_footprint: usize,
}

impl PaimonWindowJoinStore {
    pub(crate) fn create(
        config: PaimonStoreConfig,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        Ok(PaimonWindowJoinStore {
            left: PaimonRowBufferStore::create(
                PaimonOverStore::side_config(&config, "left"),
                left_types,
            )?,
            right: PaimonRowBufferStore::create(
                PaimonOverStore::side_config(&config, "right"),
                right_types,
            )?,
            last_footprint: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        left_sources: &[(String, i64)],
        right_sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        Ok(PaimonWindowJoinStore {
            left: PaimonRowBufferStore::open_merged(
                PaimonOverStore::side_config(&config, "left"),
                left_types,
                left_sources,
                key_groups.clone(),
                aligned,
            )?,
            right: PaimonRowBufferStore::open_merged(
                PaimonOverStore::side_config(&config, "right"),
                right_types,
                right_sources,
                key_groups,
                aligned,
            )?,
            last_footprint: 0,
        })
    }

    /// The store's untracked footprint change since the last call.
    pub(crate) fn footprint_delta(&mut self) -> isize {
        let current = self.left.heap_bytes() + self.right.heap_bytes();
        let delta = current as isize - self.last_footprint as isize;
        self.last_footprint = current;
        delta
    }

    /// Checkpoint sync phase: commits both sides; the caller packs the two manifests and both
    /// arrival sequences into the snapshot token.
    pub(crate) fn checkpoint(
        &mut self,
    ) -> Result<(PaimonCheckpointManifest, PaimonCheckpointManifest), DataFusionError> {
        Ok((self.left.checkpoint()?, self.right.checkpoint()?))
    }
}
