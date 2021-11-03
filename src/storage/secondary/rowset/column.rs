use bytes::Bytes;
use moka::future::Cache;
use std::sync::Arc;

// TODO(chi): support Windows and macOS by introducing different storage backends
use std::os::unix::fs::FileExt;

use super::{Block, BlockCacheKey, BlockHeader, ColumnIndex, BLOCK_HEADER_SIZE};

/// Represents a column in Secondary.
///
/// [`Column`] contains index, file handler and a reference to block cache. Therefore,
/// it is simply a reference, and can be cloned without much overhead.
#[derive(Clone)]
pub struct Column {
    index: ColumnIndex,
    file: Arc<std::fs::File>,
    block_cache: Cache<BlockCacheKey, Block>,
    base_block_key: BlockCacheKey,
}

impl Column {
    pub fn new(
        index: ColumnIndex,
        file: Arc<std::fs::File>,
        block_cache: Cache<BlockCacheKey, Block>,
        base_block_key: BlockCacheKey,
    ) -> Self {
        Self {
            index,
            file,
            block_cache,
            base_block_key,
        }
    }

    pub fn index(&self) -> &ColumnIndex {
        &self.index
    }

    pub async fn get_block(&self, block_id: u32) -> (BlockHeader, Block) {
        // It is possible that there will be multiple futures accessing
        // one block not in cache concurrently, which might cause avalanche
        // in cache. For now, we don't handle it.

        let key = self.base_block_key.clone().block(block_id);

        let mut block_header = BlockHeader::default();

        if let Some(block) = self.block_cache.get(&key) {
            let mut header = &block[..BLOCK_HEADER_SIZE];
            block_header.decode(&mut header);
            (block_header, block.slice(BLOCK_HEADER_SIZE..))
        } else {
            // block has not been in cache, so we fetch it from disk
            // TODO(chi): support multiple I/O backend

            let file = self.file.clone();
            let info = self.index.index(block_id).clone();
            let block = tokio::task::spawn_blocking(move || {
                let mut data = vec![0; info.length as usize];
                // TODO(chi): handle file system errors
                file.read_exact_at(&mut data[..], info.offset).unwrap();
                Bytes::from(data)
            })
            .await
            .unwrap();

            // TODO(chi): we should invalidate cache item after a RowSet has been compacted.
            self.block_cache.insert(key, block.clone()).await;

            let mut header = &block[..BLOCK_HEADER_SIZE];
            block_header.decode(&mut header);
            (block_header, block.slice(BLOCK_HEADER_SIZE..))
        }
    }
}
