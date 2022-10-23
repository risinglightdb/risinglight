// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::iter::Peekable;

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;
use rust_decimal::Decimal;

use super::super::{
    BlockBuilder, BlockIndexBuilder, ColumnBuilderOptions, PlainPrimitiveBlockBuilder,
    PrimitiveFixedWidthEncode,
};
use super::ColumnBuilder;
use crate::array::Array;
use crate::storage::secondary::block::{DictBlockBuilder, NullableBlockBuilder, RleBlockBuilder};
use crate::storage::secondary::EncodeType;
use crate::types::{Date, Interval, F64};

/// All supported block builders for primitive types.
pub(super) enum BlockBuilderImpl<T: PrimitiveFixedWidthEncode> {
    Plain(PlainPrimitiveBlockBuilder<T>),
    PlainNullable(NullableBlockBuilder<T::ArrayType, PlainPrimitiveBlockBuilder<T>>),
    RunLength(RleBlockBuilder<T::ArrayType, PlainPrimitiveBlockBuilder<T>>),
    RleNullable(
        RleBlockBuilder<
            T::ArrayType,
            NullableBlockBuilder<T::ArrayType, PlainPrimitiveBlockBuilder<T>>,
        >,
    ),
    Dictionary(DictBlockBuilder<T::ArrayType, PlainPrimitiveBlockBuilder<T>>),
    DictNullable(
        DictBlockBuilder<
            T::ArrayType,
            NullableBlockBuilder<T::ArrayType, PlainPrimitiveBlockBuilder<T>>,
        >,
    ),
}

pub type I32ColumnBuilder = PrimitiveColumnBuilder<i32>;
pub type I64ColumnBuilder = PrimitiveColumnBuilder<i64>;
pub type F64ColumnBuilder = PrimitiveColumnBuilder<F64>;
pub type BoolColumnBuilder = PrimitiveColumnBuilder<bool>;
pub type DecimalColumnBuilder = PrimitiveColumnBuilder<Decimal>;
pub type DateColumnBuilder = PrimitiveColumnBuilder<Date>;
pub type IntervalColumnBuilder = PrimitiveColumnBuilder<Interval>;

/// Column builder of primitive types.
pub struct PrimitiveColumnBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,

    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<BlockBuilderImpl<T>>,

    /// Indicates whether the current column accepts null elements
    nullable: bool,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,

    /// First key
    first_key: Option<Vec<u8>>,
}

impl<T: PrimitiveFixedWidthEncode> PrimitiveColumnBuilder<T> {
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

        let (block_type, stats, mut block_data) = match self.current_builder.take().unwrap() {
            BlockBuilderImpl::Plain(builder) => {
                (BlockType::Plain, builder.get_statistics(), builder.finish())
            }
            BlockBuilderImpl::PlainNullable(builder) => (
                BlockType::PlainNullable,
                builder.get_statistics(),
                builder.finish(),
            ),
            BlockBuilderImpl::RunLength(builder) => (
                BlockType::RunLength,
                builder.get_statistics(),
                builder.finish(),
            ),
            BlockBuilderImpl::RleNullable(builder) => (
                BlockType::RleNullable,
                builder.get_statistics(),
                builder.finish(),
            ),
            BlockBuilderImpl::Dictionary(builder) => (
                BlockType::Dictionary,
                builder.get_statistics(),
                builder.finish(),
            ),
            BlockBuilderImpl::DictNullable(builder) => (
                BlockType::DictNullable,
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

/// Append data to builder one by one. After appending each item, check if
/// the block should be finished. Return true if a new block builder should
/// be created.
///
/// In the future, for integer data, we should be able to skip the `should_finish`
/// check, as we can calculate expected number of items to add simply by
/// `size_of::<T>() * N`.
pub fn append_one_by_one<'a, A: Array>(
    iter: &mut Peekable<impl Iterator<Item = Option<&'a A::Item>>>,
    builder: &mut impl BlockBuilder<A>,
) -> (usize, bool) {
    let mut cnt = 0;
    while let Some(to_be_appended) = iter.peek() {
        // peek and see if we could push more items into the builder

        if builder.should_finish(to_be_appended) {
            return (cnt, true);
        }

        // get the item from iterator and push it to the builder
        let to_be_appended = iter.next().unwrap();

        builder.append(to_be_appended);
        cnt += 1;
    }

    (cnt, false)
}

impl<T: PrimitiveFixedWidthEncode> ColumnBuilder<T::ArrayType> for PrimitiveColumnBuilder<T> {
    fn append(&mut self, array: &T::ArrayType) {
        let mut iter = array.iter().peekable();
        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                match (self.nullable, self.options.encode_type) {
                    (true, EncodeType::RunLength) => {
                        let builder = NullableBlockBuilder::new(
                            PlainPrimitiveBlockBuilder::new(self.options.target_block_size - 16),
                            self.options.target_block_size - 16,
                        );
                        self.current_builder =
                            Some(BlockBuilderImpl::RleNullable(RleBlockBuilder::<
                                T::ArrayType,
                                NullableBlockBuilder<T::ArrayType, PlainPrimitiveBlockBuilder<T>>,
                            >::new(
                                builder
                            )));
                    }
                    (true, EncodeType::Plain) => {
                        self.current_builder =
                            Some(BlockBuilderImpl::PlainNullable(NullableBlockBuilder::new(
                                PlainPrimitiveBlockBuilder::new(
                                    self.options.target_block_size - 16,
                                ),
                                self.options.target_block_size - 16,
                            )));
                    }
                    (true, EncodeType::Dictionary) => {
                        let builder = NullableBlockBuilder::new(
                            PlainPrimitiveBlockBuilder::new(self.options.target_block_size - 16),
                            self.options.target_block_size - 16,
                        );
                        self.current_builder =
                            Some(BlockBuilderImpl::DictNullable(DictBlockBuilder::<
                                T::ArrayType,
                                NullableBlockBuilder<T::ArrayType, PlainPrimitiveBlockBuilder<T>>,
                            >::new(
                                builder
                            )));
                    }
                    (false, EncodeType::RunLength) => {
                        let builder =
                            PlainPrimitiveBlockBuilder::new(self.options.target_block_size - 16);
                        self.current_builder =
                            Some(BlockBuilderImpl::RunLength(RleBlockBuilder::<
                                T::ArrayType,
                                PlainPrimitiveBlockBuilder<T>,
                            >::new(
                                builder
                            )));
                    }
                    (false, EncodeType::Plain) => {
                        self.current_builder = Some(BlockBuilderImpl::Plain(
                            PlainPrimitiveBlockBuilder::new(self.options.target_block_size - 16),
                        ));
                    }
                    (false, EncodeType::Dictionary) => {
                        let builder =
                            PlainPrimitiveBlockBuilder::new(self.options.target_block_size - 16);
                        self.current_builder =
                            Some(BlockBuilderImpl::Dictionary(DictBlockBuilder::<
                                T::ArrayType,
                                PlainPrimitiveBlockBuilder<T>,
                            >::new(
                                builder
                            )));
                    }
                }

                if let Some(to_be_appended) = iter.peek() {
                    if self.options.record_first_key {
                        self.first_key = to_be_appended.map(|x| {
                            let mut first_key = vec![];
                            x.encode(&mut first_key);
                            first_key
                        });
                    }
                }
            }

            let (row_count, should_finish) = match self.current_builder.as_mut().unwrap() {
                BlockBuilderImpl::Plain(builder) => append_one_by_one(&mut iter, builder),
                BlockBuilderImpl::PlainNullable(builder) => append_one_by_one(&mut iter, builder),
                BlockBuilderImpl::RunLength(builder) => append_one_by_one(&mut iter, builder),
                BlockBuilderImpl::RleNullable(builder) => append_one_by_one(&mut iter, builder),
                BlockBuilderImpl::Dictionary(builder) => append_one_by_one(&mut iter, builder),
                BlockBuilderImpl::DictNullable(builder) => append_one_by_one(&mut iter, builder),
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

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use super::*;
    use crate::array::I32Array;

    #[test]
    fn test_i32_column_builder_finish_boundary() {
        let item_each_block = (128 - 16) / 4;
        // In the first case, we append array that just fits size of each block
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..10 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(item_each_block),
            ));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 10);
        assert_eq!(index[3].first_rowid as usize, item_each_block * 3);
        assert_eq!(index[3].row_count as usize, item_each_block);

        // In this case, we append array that is smaller than each block, and fill fewer than 2
        // blocks of contents
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..12 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(4),
            ));
        }
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append two array that sums up to exactly 2 blocks
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(30),
        ));
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(26),
        ));
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append an array that is larger than 1 block.
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        builder.append(&I32Array::from_iter(
            [Some(1)]
                .iter()
                .cycle()
                .cloned()
                .take(item_each_block * 100),
        ));
        assert_eq!(builder.finish().0.len(), 100);

        // And finally, some chaos test
        let mut builder =
            I32ColumnBuilder::new(false, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..100 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(23),
            ));
        }
        assert_eq!(builder.finish().0.len(), 83);
    }

    #[test]
    fn test_nullable_i32_column_builder() {
        let mut builder =
            I32ColumnBuilder::new(true, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..100 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(23),
            ));
        }
        builder.finish();
    }

    #[test]
    fn test_rle_i32_column_builder() {
        let distinct_item_each_block = (128 - 16) / 4;

        let mut builder =
            I32ColumnBuilder::new(true, ColumnBuilderOptions::default_for_rle_block_test());
        for num in 0..(distinct_item_each_block + 1) {
            builder.append(&I32Array::from_iter(
                [Some(num)].iter().cycle().cloned().take(23),
            ));
        }
        assert_eq!(builder.finish().0.len(), 2);
    }

    #[test]
    fn test_i32_block_index_first_key() {
        let item_each_block = (128 - 16) / 4;

        // Test for first key
        let mut builder =
            I32ColumnBuilder::new(true, ColumnBuilderOptions::record_first_key_test());
        for _ in 0..10 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(item_each_block),
            ));
        }

        let (index, _) = builder.finish();
        assert_eq!(index.len(), 11);

        let mut f2: &[u8];
        for item in index {
            f2 = &item.first_key;
            let x: i32 = PrimitiveFixedWidthEncode::decode(&mut f2);
            assert_eq!(x, 1);
        }

        // Test for null first key
        let mut builder =
            I32ColumnBuilder::new(true, ColumnBuilderOptions::record_first_key_test());
        for _ in 0..10 {
            builder.append(&I32Array::from_iter(
                [None].iter().cycle().cloned().take(item_each_block),
            ));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 11);

        for item in index {
            assert!(item.is_first_key_null);
        }
    }
}
