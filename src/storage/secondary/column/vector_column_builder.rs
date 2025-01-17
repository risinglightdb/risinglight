// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{BlockBuilder, BlockIndexBuilder};
use super::{append_one_by_one, ColumnBuilder};
use crate::array::{Array, VectorArray};
use crate::storage::secondary::block::NullableBlockBuilder;
use crate::storage::secondary::{ColumnBuilderOptions, PlainVectorBlockBuilder};

type PlainNullableVectorBlockBuilder = NullableBlockBuilder<VectorArray, PlainVectorBlockBuilder>;

/// All supported block builders for blob types.
pub(super) enum VectorBlockBuilderImpl {
    Plain(PlainVectorBlockBuilder),
    PlainNullable(PlainNullableVectorBlockBuilder),
}

macro_rules! for_all_vector_block_builder_enum {
    ($marco:tt) => {
        $marco! {
            Plain,
            PlainNullable
        }
    };
}

/// Column builder of blob types.
pub struct VectorColumnBuilder {
    data: Vec<u8>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<VectorBlockBuilderImpl>,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,

    /// Indicates whether the current column accepts null elements
    nullable: bool,

    /// First key
    first_key: Option<Vec<u8>>,
}

impl VectorColumnBuilder {
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
                    VectorBlockBuilderImpl::$enum_val(builder) => (
                        BlockType::$enum_val,
                        builder.get_statistics(),
                        builder.finish(),
                    ),)*
                }
            }
        }

        let (block_type, stats, mut block_data) =
            for_all_vector_block_builder_enum! { finish_current_builder };

        self.block_index_builder.finish_block(
            block_type,
            &mut self.data,
            &mut block_data,
            stats,
            self.first_key.clone(),
        );
    }
}

impl ColumnBuilder<VectorArray> for VectorColumnBuilder {
    fn append(&mut self, array: &VectorArray) {
        let mut iter = array.iter().peekable();

        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                let target_size = self.options.target_block_size - 16;
                match (self.nullable, self.options.encode_type) {
                    (false, _) => {
                        self.current_builder = Some(VectorBlockBuilderImpl::Plain(
                            PlainVectorBlockBuilder::new(target_size),
                        ));
                    }
                    (true, _) => {
                        self.current_builder = Some(VectorBlockBuilderImpl::PlainNullable(
                            NullableBlockBuilder::new(
                                PlainVectorBlockBuilder::new(target_size),
                                target_size,
                            ),
                        ));
                    }
                }
                if let Some(to_be_appended) = iter.peek() {
                    if self.options.record_first_key {
                        self.first_key = to_be_appended.map(|x| {
                            let mut key = Vec::new();
                            for i in x.iter() {
                                key.extend_from_slice(&i.to_le_bytes());
                            }
                            key
                        });
                    }
                }
            }

            macro_rules! append_one_by_one {
                ($($enum_val:ident),*) => {
                    match self.current_builder.as_mut().unwrap() {
                        $(
                            VectorBlockBuilderImpl::$enum_val(builder) => {append_one_by_one(&mut iter, builder)}
                        ),*
                    }
                }
            }

            let (row_count, should_finish) =
                for_all_vector_block_builder_enum! { append_one_by_one };

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
