use bytes::Bytes;

/// A block is simply a bytes array.
pub type Block = Bytes;

/// A key in block cache contains `rowset_id`, `column_id`
/// and `block_id`.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct BlockCacheKey {
    pub rowset_id: usize,
    pub storage_column_id: usize,
    pub block_id: usize,
}

impl BlockCacheKey {
    pub fn block(mut self, block_id: usize) -> Self {
        self.block_id = block_id;
        self
    }

    pub fn column(mut self, storage_column_id: usize) -> Self {
        self.storage_column_id = storage_column_id;
        self
    }

    pub fn rowset(mut self, rowset_id: usize) -> Self {
        self.rowset_id = rowset_id;
        self
    }
}
