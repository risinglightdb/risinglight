// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{BlockBuilder, BlockIndexBuilder, PlainBlobBlockBuilder};
use super::{append_one_by_one, ColumnBuilder};
use crate::array::{Array, BlobArray};
use crate::storage::secondary::block::{DictBlockBuilder, RleBlockBuilder};
use crate::storage::secondary::encode::BlobEncode;
use crate::storage::secondary::ColumnBuilderOptions;
use crate::types::BlobRef;

/// All supported block builders for blob types.
pub(super) enum BlobBlockBuilderImpl {
    Plain(PlainBlobBlockBuilder<BlobRef>),
    RunLength(RleBlockBuilder<BlobArray, PlainBlobBlockBuilder<BlobRef>>),
    Dictionary(DictBlockBuilder<BlobArray, PlainBlobBlockBuilder<BlobRef>>),
}

/// Column builder of blob types.
pub struct BlobColumnBuilder {
    data: Vec<u8>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<BlobBlockBuilderImpl>,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,

    /// First key
    first_key: Option<Vec<u8>>,
}

impl BlobColumnBuilder {
    pub fn new(options: ColumnBuilderOptions) -> Self {
        Self {
            data: vec![],
            block_index_builder: BlockIndexBuilder::new(options.clone()),
            options,
            current_builder: None,
            first_key: None,
        }
    }

    fn finish_builder(&mut self) {
        if self.current_builder.is_none() {
            return;
        }

        let (block_type, stats, mut block_data) = match self.current_builder.take().unwrap() {
            BlobBlockBuilderImpl::Plain(builder) => (
                BlockType::PlainVarchar,
                builder.get_statistics(),
                builder.finish(),
            ),
            BlobBlockBuilderImpl::RunLength(builder) => (
                BlockType::RleVarchar,
                builder.get_statistics(),
                builder.finish(),
            ),
            BlobBlockBuilderImpl::Dictionary(builder) => (
                BlockType::RleVarchar,
                builder.get_statistics(),
                builder.finish(),
            ),
        };

        self.block_index_builder.finish_block(
            block_type,
            &mut self.data,
            &mut block_data,
            stats,
            self.first_key.clone(),
        );
    }
}

impl ColumnBuilder<BlobArray> for BlobColumnBuilder {
    fn append(&mut self, array: &BlobArray) {
        let mut iter = array.iter().peekable();

        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                match self.options.encode_type {
                    crate::storage::secondary::EncodeType::Plain => {
                        self.current_builder = Some(BlobBlockBuilderImpl::Plain(
                            PlainBlobBlockBuilder::new(self.options.target_block_size - 16),
                        ));
                    }
                    crate::storage::secondary::EncodeType::RunLength => {
                        let builder =
                            PlainBlobBlockBuilder::new(self.options.target_block_size - 16);
                        self.current_builder =
                            Some(BlobBlockBuilderImpl::RunLength(RleBlockBuilder::<
                                BlobArray,
                                PlainBlobBlockBuilder<BlobRef>,
                            >::new(
                                builder
                            )));
                    }
                    crate::storage::secondary::EncodeType::Dictionary => {
                        let builder =
                            PlainBlobBlockBuilder::new(self.options.target_block_size - 16);
                        self.current_builder =
                            Some(BlobBlockBuilderImpl::Dictionary(DictBlockBuilder::<
                                BlobArray,
                                PlainBlobBlockBuilder<BlobRef>,
                            >::new(
                                builder
                            )));
                    }
                }
                if let Some(to_be_appended) = iter.peek() {
                    if self.options.record_first_key {
                        self.first_key = to_be_appended.map(|x| x.to_byte_slice().to_vec());
                    }
                }
            }

            let (row_count, should_finish) = match self.current_builder.as_mut().unwrap() {
                BlobBlockBuilderImpl::Plain(builder) => append_one_by_one(&mut iter, builder),
                BlobBlockBuilderImpl::RunLength(builder) => append_one_by_one(&mut iter, builder),
                BlobBlockBuilderImpl::Dictionary(builder) => append_one_by_one(&mut iter, builder),
            };

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
