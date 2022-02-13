// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{BlockBuilder, BlockIndexBuilder, PlainBlobBlockBuilder};
use super::{append_one_by_one, ColumnBuilder};
use crate::array::{Array, BlobArray};
use crate::storage::secondary::ColumnBuilderOptions;
use crate::types::BlobRef;

/// Column builder of char types.
pub struct BlobColumnBuilder {
    data: Vec<u8>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<PlainBlobBlockBuilder<BlobRef>>,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,
}

impl BlobColumnBuilder {
    pub fn new(options: ColumnBuilderOptions) -> Self {
        Self {
            data: vec![],
            block_index_builder: BlockIndexBuilder::new(options.clone()),
            options,
            current_builder: None,
        }
    }

    fn finish_builder(&mut self) {
        let builder = self.current_builder.take().unwrap();
        let (block_type, stats, mut block_data) = (
            BlockType::PlainVarchar,
            builder.get_statistics(),
            builder.finish(),
        );

        self.block_index_builder
            .finish_block(block_type, &mut self.data, &mut block_data, stats);
    }
}

impl ColumnBuilder<BlobArray> for BlobColumnBuilder {
    fn append(&mut self, array: &BlobArray) {
        let mut iter = array.iter().peekable();

        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                self.current_builder = Some(PlainBlobBlockBuilder::new(
                    self.options.target_block_size - 16,
                ));
            }

            let builder = self.current_builder.as_mut().unwrap();
            let (row_count, should_finish) = append_one_by_one(&mut iter, builder);

            self.block_index_builder.add_rows(row_count);

            // finish the current block
            if should_finish {
                self.finish_builder();
            }
        }
    }

    fn finish(mut self) -> (Vec<BlockIndex>, Vec<u8>) {
        self.finish_builder();

        (self.block_index_builder.into_index(), self.data)
    }
}
