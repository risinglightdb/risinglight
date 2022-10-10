// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::{BlockIndex, BlockStatistics};

use super::{BlockMeta, BLOCK_META_NON_CHECKSUM_SIZE, BLOCK_META_SIZE};
use crate::storage::secondary::{build_checksum, ColumnBuilderOptions};

/// Builds the block index.
pub struct BlockIndexBuilder {
    /// Count of rows which has been sent to builder
    row_count: usize,

    /// Begin row count of the current block
    last_row_count: usize,

    /// All indexes in the current builder
    indexes: Vec<BlockIndex>,

    /// Buffer for each block header, so as to reduce allocation overhead
    block_header: Vec<u8>,

    /// Builder options
    options: ColumnBuilderOptions,
}

impl BlockIndexBuilder {
    pub fn new(options: ColumnBuilderOptions) -> Self {
        Self {
            row_count: 0,
            last_row_count: 0,
            indexes: vec![],
            block_header: vec![],
            options,
        }
    }

    /// Record information of a block and produce a new index entry.
    pub fn finish_block(
        &mut self,
        block_type: BlockType,
        column_data: &mut Vec<u8>,
        block_data: &mut Vec<u8>,
        stats: Vec<BlockStatistics>,
        first_key: Option<Vec<u8>>,
    ) {
        self.indexes.push(BlockIndex {
            offset: column_data.len() as u64,
            length: block_data.len() as u64 + BLOCK_META_SIZE as u64,
            first_rowid: self.last_row_count as u32,
            row_count: (self.row_count - self.last_row_count) as u32,
            /// TODO(chi): support sort key
            is_first_key_null: first_key.is_none(),
            first_key: first_key.unwrap_or_default(),
            stats,
        });

        // the new block will begin at the current row count
        self.last_row_count = self.row_count;

        self.block_header.resize(BLOCK_META_SIZE, 0);
        let mut block_header_nonchecksum = &mut self.block_header[..BLOCK_META_NON_CHECKSUM_SIZE];

        let checksum_type = self.options.checksum_type;

        let mut header = BlockMeta {
            block_type,
            checksum_type,
            checksum: 0,
        };
        header.encode_except_checksum(&mut block_header_nonchecksum);
        debug_assert!(block_header_nonchecksum.is_empty());
        // add block_type to block_data
        block_data.extend_from_slice(&self.block_header[..BLOCK_META_NON_CHECKSUM_SIZE]);

        // calculate checksum and add
        header.checksum = build_checksum(header.checksum_type, block_data);
        let mut block_header_checksum = &mut self.block_header[BLOCK_META_NON_CHECKSUM_SIZE..];
        header.encode_checksum(&mut block_header_checksum);
        debug_assert!(block_header_checksum.is_empty());
        block_data.extend_from_slice(&self.block_header[BLOCK_META_NON_CHECKSUM_SIZE..]);

        // add data to the column file
        column_data.append(block_data);
    }

    /// Add new rows into the block index
    pub fn add_rows(&mut self, rows: usize) {
        self.row_count += rows;
    }

    pub fn into_index(self) -> Vec<BlockIndex> {
        self.indexes
    }
}
