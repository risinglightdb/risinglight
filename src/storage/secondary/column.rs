//! Secondary's [`Column`] builders and iterators.
//!
//! A column stores data of the same kind, e.g. Int32. On the storage format
//! side, a column is composed of multiple blocks and an index. The type of
//! blocks might not be the same. For example, a column could contains several
//! compressed blocks, and several RLE blocks.

mod char_column_builder;
mod column_builder;
mod column_iterator;
mod primitive_column_builder;
mod primitive_column_iterator;
mod row_handler_sequencer;

pub use column_builder::*;
pub use column_iterator::*;
pub use primitive_column_builder::*;
pub use primitive_column_iterator::*;
use risinglight_proto::rowset::BlockIndex;
pub use row_handler_sequencer::*;
use std::io::{Read, Seek, SeekFrom};
mod char_column_iterator;
pub use char_column_iterator::*;

use async_trait::async_trait;
use bytes::Bytes;
use moka::future::Cache;
use std::{
    os::unix::fs::FileExt,
    sync::{Arc, Mutex},
};

use super::{Block, BlockCacheKey, BlockHeader, ColumnIndex, BLOCK_HEADER_SIZE};
use crate::array::Array;

/// Builds a column. [`ColumnBuilder`] will automatically chunk [`Array`] into
/// blocks, calls `BlockBuilder` to generate a block, and builds index for a
/// column. Note that one [`Array`] might require multiple [`ColumnBuilder`] to build.
///
/// * For nullable columns, there will be a bitmap file built with `BitmapColumnBuilder`.
/// * And for concrete data, there will be another column builder with concrete block builder.
///
/// After a single column has been built, an index file will also be generated with `IndexBuilder`.
pub trait ColumnBuilder<A: Array> {
    /// Append an [`Array`] to the column. [`ColumnBuilder`] will automatically chunk it into
    /// small parts.
    fn append(&mut self, array: &A);

    /// Finish a column, return block index information and encoded block data
    fn finish(self) -> (Vec<BlockIndex>, Vec<u8>);
}

/// Iterator on a column. This iterator may request data from disk while iterating.
#[async_trait]
pub trait ColumnIterator<A: Array> {
    /// Get a batch and the starting row id from the column. A `None` return value means that
    /// there are no more elements from the block. By using `expected_size`, developers can
    /// get an array of NO MORE THAN the `expected_size` on supported column types.
    async fn next_batch(&mut self, expected_size: Option<usize>) -> Option<(u32, A)>;

    /// Number of items that can be fetched without I/O. When the column iterator has finished
    /// iterating, the returned value should be 0.
    fn fetch_hint(&self) -> usize;
}

/// When creating an iterator, a [`ColumnSeekPosition`] should be set as the initial location.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum ColumnSeekPosition {
    RowId(u32),
    #[allow(dead_code)]
    SortKey(()),
}

impl ColumnSeekPosition {
    pub fn start() -> Self {
        Self::RowId(0)
    }
}

#[derive(Clone)]
pub enum ColumnReadableFile {
    /// For `read_at`
    #[cfg(unix)]
    PositionedRead(Arc<std::fs::File>),
    /// For `file.lock().seek().read()`
    NormalRead(Arc<Mutex<std::fs::File>>),
    // In the future, we can even add minio / S3 file backend
}

/// Represents a column in Secondary.
///
/// [`Column`] contains index, file handler and a reference to block cache. Therefore,
/// it is simply a reference, and can be cloned without much overhead.
#[derive(Clone)]
pub struct Column {
    index: ColumnIndex,
    file: ColumnReadableFile,
    block_cache: Cache<BlockCacheKey, Block>,
    base_block_key: BlockCacheKey,
}

impl Column {
    pub fn new(
        index: ColumnIndex,
        file: ColumnReadableFile,
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

    pub fn on_disk_size(&self) -> u64 {
        let lst_idx = self.index.index(self.index.len() as u32 - 1);
        lst_idx.offset + lst_idx.length
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
                match file {
                    ColumnReadableFile::PositionedRead(file) => {
                        file.read_exact_at(&mut data[..], info.offset).unwrap()
                    }
                    ColumnReadableFile::NormalRead(file) => {
                        let mut file = file.lock().unwrap();
                        file.seek(SeekFrom::Start(info.offset as u64)).unwrap();
                        file.read_exact(&mut data[..]).unwrap();
                    }
                }
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
