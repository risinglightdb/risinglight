// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{BlockBuilder, BlockIndexBuilder, PlainBlobBlockBuilder};
use super::{append_one_by_one, ColumnBuilder};
use crate::array::{Array, BlobArray};
use crate::storage::secondary::block::{DictBlockBuilder, NullableBlockBuilder, RleBlockBuilder};
use crate::storage::secondary::encode::BlobEncode;
use crate::storage::secondary::ColumnBuilderOptions;
use crate::types::BlobRef;

type PlainNullableBlobBlockBuilder =
    NullableBlockBuilder<BlobArray, PlainBlobBlockBuilder<BlobRef>>;

/// All supported block builders for blob types.
pub(super) enum BlobBlockBuilderImpl {
    Plain(PlainBlobBlockBuilder<BlobRef>),
    PlainNullable(PlainNullableBlobBlockBuilder),
    RunLength(RleBlockBuilder<BlobArray, PlainBlobBlockBuilder<BlobRef>>),
    RleNullable(RleBlockBuilder<BlobArray, PlainNullableBlobBlockBuilder>),
    Dictionary(DictBlockBuilder<BlobArray, PlainBlobBlockBuilder<BlobRef>>),
    DictNullable(DictBlockBuilder<BlobArray, PlainNullableBlobBlockBuilder>),
}

macro_rules! for_all_blob_block_builder_enum {
    ($marco:tt) => {
        $marco! {
            Plain,
            PlainNullable,
            RunLength,
            RleNullable,
            Dictionary,
            DictNullable
        }
    };
}

/// Column builder of blob types.
pub struct BlobColumnBuilder {
    data: Vec<u8>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<BlobBlockBuilderImpl>,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,

    /// Indicates whether the current column accepts null elements
    nullable: bool,

    /// First key
    first_key: Option<Vec<u8>>,
}

impl BlobColumnBuilder {
    pub fn new(nullable: bool, options: ColumnBuilderOptions) -> Self {
        Self {
            data: vec![],
            block_index_builder: BlockIndexBuilder::new(options.clone()),
            options,
            current_builder: None,
            nullable,
            first_key: None,
        }
    }

    fn finish_builder(&mut self) {
        if self.current_builder.is_none() {
            return;
        }

        macro_rules! finish_current_builder {
            ($($enum_val:ident),*) => {
                match self.current_builder.take().unwrap() {
                    $(
                    BlobBlockBuilderImpl::$enum_val(builder) => (
                        BlockType::$enum_val,
                        builder.get_statistics(),
                        builder.finish(),
                    ),)*
            }
            }
        }

        let (block_type, stats, mut block_data) =
            for_all_blob_block_builder_enum! { finish_current_builder };

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
                let target_size = self.options.target_block_size - 16;
                match (self.nullable, self.options.encode_type) {
                    (false, crate::storage::secondary::EncodeType::Plain) => {
                        self.current_builder = Some(BlobBlockBuilderImpl::Plain(
                            PlainBlobBlockBuilder::new(target_size),
                        ));
                    }
                    (true, crate::storage::secondary::EncodeType::Plain) => {
                        self.current_builder = Some(BlobBlockBuilderImpl::PlainNullable(
                            NullableBlockBuilder::new(
                                PlainBlobBlockBuilder::new(target_size),
                                target_size,
                            ),
                        ));
                    }
                    (false, crate::storage::secondary::EncodeType::RunLength) => {
                        let builder = PlainBlobBlockBuilder::new(target_size);
                        self.current_builder =
                            Some(BlobBlockBuilderImpl::RunLength(RleBlockBuilder::<
                                BlobArray,
                                PlainBlobBlockBuilder<BlobRef>,
                            >::new(
                                builder
                            )));
                    }
                    (true, crate::storage::secondary::EncodeType::RunLength) => {
                        let nullable_builder = NullableBlockBuilder::new(
                            PlainBlobBlockBuilder::<BlobRef>::new(target_size),
                            target_size,
                        );
                        self.current_builder = Some(BlobBlockBuilderImpl::RleNullable(
                            RleBlockBuilder::new(nullable_builder),
                        ));
                    }
                    (false, crate::storage::secondary::EncodeType::Dictionary) => {
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
                    (true, crate::storage::secondary::EncodeType::Dictionary) => {
                        let nullable_builder = NullableBlockBuilder::new(
                            PlainBlobBlockBuilder::<BlobRef>::new(target_size),
                            target_size,
                        );
                        self.current_builder = Some(BlobBlockBuilderImpl::DictNullable(
                            DictBlockBuilder::new(nullable_builder),
                        ));
                    }
                }
                if let Some(to_be_appended) = iter.peek() {
                    if self.options.record_first_key {
                        self.first_key = to_be_appended.map(|x| x.to_byte_slice().to_vec());
                    }
                }
            }

            macro_rules! append_one_by_one {
                ($($enum_val:ident),*) => {
                    match self.current_builder.as_mut().unwrap() {
                        $(
                            BlobBlockBuilderImpl::$enum_val(builder) => {append_one_by_one(&mut iter, builder)}
                        ),*
                    }
                }
            }

            let (row_count, should_finish) = for_all_blob_block_builder_enum! { append_one_by_one };

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
