// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::{BlockIndex, BlockStatistics};

use super::{BlockHeader, BLOCK_HEADER_SIZE};
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
            length: block_data.len() as u64 + BLOCK_HEADER_SIZE as u64,
            first_rowid: self.last_row_count as u32,
            row_count: (self.row_count - self.last_row_count) as u32,
            /// TODO(chi): support sort key
            is_first_key_null: first_key.is_none(),
            first_key: first_key.unwrap_or_default(),
            stats,
        });

        // the new block will begin at the current row count
        self.last_row_count = self.row_count;

        self.block_header.resize(BLOCK_HEADER_SIZE, 0);
        let mut block_header_ref = &mut self.block_header[..];

        let checksum_type = self.options.checksum_type;

        BlockHeader {
            block_type,
            checksum_type,
            checksum: build_checksum(checksum_type, block_data),
        }
        .encode(&mut block_header_ref);

        debug_assert!(block_header_ref.is_empty());

        // add data to the column file
        column_data.append(&mut self.block_header);
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
