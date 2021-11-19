use std::iter::Peekable;

use risinglight_proto::rowset::block_checksum::ChecksumType;
use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use crate::array::Array;
use crate::storage::secondary::block::BlockBuilder;

use super::super::{
    PlainNullablePrimitiveBlockBuilder, PlainPrimitiveBlockBuilder, PrimitiveFixedWidthEncode,
};
use super::ColumnBuilder;
use crate::storage::secondary::{BlockHeader, ColumnBuilderOptions, BLOCK_HEADER_SIZE};

/// All supported block builders for primitive types.
pub(super) enum BlockBuilderImpl<T: PrimitiveFixedWidthEncode> {
    Plain(PlainPrimitiveBlockBuilder<T>),
    PlainNullable(PlainNullablePrimitiveBlockBuilder<T>),
}

pub type I32ColumnBuilder = PrimitiveColumnBuilder<i32>;
pub type F64ColumnBuilder = PrimitiveColumnBuilder<f64>;
pub type BoolColumnBuilder = PrimitiveColumnBuilder<bool>;

/// Column builder of primitive types.
pub struct PrimitiveColumnBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,
    index: Vec<BlockIndex>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<BlockBuilderImpl<T>>,

    /// Count of rows which has been sent to builder
    row_count: usize,

    /// Begin row count of the current block
    last_row_count: usize,

    /// Indicates whether the current column accepts null elements
    nullable: bool,
}

impl<T: PrimitiveFixedWidthEncode> PrimitiveColumnBuilder<T> {
    pub fn new(nullable: bool, options: ColumnBuilderOptions) -> Self {
        Self {
            data: vec![],
            index: vec![],
            options,
            current_builder: None,
            row_count: 0,
            last_row_count: 0,
            nullable,
        }
    }

    fn finish_builder(&mut self) {
        let (block_type, mut block_data) = match self.current_builder.take().unwrap() {
            BlockBuilderImpl::Plain(builder) => (BlockType::Plain, builder.finish()),
            BlockBuilderImpl::PlainNullable(builder) => {
                (BlockType::PlainNullable, builder.finish())
            }
        };

        self.index.push(BlockIndex {
            block_type: block_type.into(),
            offset: self.data.len() as u64,
            length: (block_data.len() + BLOCK_HEADER_SIZE) as u64,
            first_rowid: self.last_row_count as u32,
            row_count: (self.row_count - self.last_row_count) as u32,
            /// TODO(chi): support sort key
            first_key: "".into(),
            stats: vec![],
        });

        // the new block will begin at the current row count
        self.last_row_count = self.row_count;

        let mut block_header = vec![0; BLOCK_HEADER_SIZE];

        let mut block_header_ref = &mut block_header[..];

        BlockHeader {
            block_type,
            checksum_type: ChecksumType::None,
            // TODO(chi): add checksum support
            checksum: 0,
        }
        .encode(&mut block_header_ref);

        assert!(block_header_ref.is_empty());

        // add data to the column file
        self.data.append(&mut block_header);
        self.data.append(&mut block_data);
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
                if self.nullable {
                    self.current_builder = Some(BlockBuilderImpl::PlainNullable(
                        PlainNullablePrimitiveBlockBuilder::new(
                            self.options.target_block_size - 16,
                        ),
                    ));
                } else {
                    self.current_builder = Some(BlockBuilderImpl::Plain(
                        PlainPrimitiveBlockBuilder::new(self.options.target_block_size - 16),
                    ));
                }
            }

            let (row_count, should_finish) = match self.current_builder.as_mut().unwrap() {
                BlockBuilderImpl::Plain(builder) => append_one_by_one(&mut iter, builder),
                BlockBuilderImpl::PlainNullable(builder) => append_one_by_one(&mut iter, builder),
            };

            self.row_count += row_count;

            // finish the current block
            if should_finish {
                self.finish_builder();
            }
        }
    }

    fn finish(mut self) -> (Vec<BlockIndex>, Vec<u8>) {
        self.finish_builder();

        (self.index, self.data)
    }
}

#[cfg(test)]
mod tests {
    use crate::array::I32Array;
    use std::iter::FromIterator;

    use super::*;

    #[test]
    fn test_i32_column_builder_finish_boundary() {
        let item_each_block = (128 - 16) / 4;
        // In the first case, we append array that just fits size of each block
        let mut builder = I32ColumnBuilder::new(
            false,
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
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
        let mut builder = I32ColumnBuilder::new(
            false,
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
        for _ in 0..12 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(4),
            ));
        }
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append two array that sums up to exactly 2 blocks
        let mut builder = I32ColumnBuilder::new(
            false,
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(30),
        ));
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(26),
        ));
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append an array that is larger than 1 block.
        let mut builder = I32ColumnBuilder::new(
            false,
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
        builder.append(&I32Array::from_iter(
            [Some(1)]
                .iter()
                .cycle()
                .cloned()
                .take(item_each_block * 100),
        ));
        assert_eq!(builder.finish().0.len(), 100);

        // And finally, some chaos test
        let mut builder = I32ColumnBuilder::new(
            false,
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
        for _ in 0..100 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(23),
            ));
        }
        assert_eq!(builder.finish().0.len(), 83);
    }

    #[test]
    fn test_nullable_i32_column_builder() {
        let mut builder = I32ColumnBuilder::new(
            true,
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
        for _ in 0..100 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(23),
            ));
        }
        builder.finish();
    }
}
