use std::iter::Peekable;

use risinglight_proto::rowset::block_checksum::ChecksumType;
use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use bytes::BufMut;

use crate::array::{Array, I32Array};

use super::{BlockBuilder, ColumnBuilder, PlainI32BlockBuilder};

/// All supported block builders for `i32`.
pub(super) enum I32BlockBuilderImpl {
    PlainI32(PlainI32BlockBuilder),
}

/// Options for [`ColumnBuilder`]s.
#[derive(Clone)]
pub struct ColumnBuilderOptions {
    pub target_size: usize,
}

/// `i32` column builder.
pub struct I32ColumnBuilder {
    data: Vec<u8>,
    index: Vec<BlockIndex>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<I32BlockBuilderImpl>,

    /// Count of rows which has been sent to builder
    row_count: usize,

    /// Begin row count of the current block
    last_row_count: usize,
}

impl I32ColumnBuilder {
    pub fn new(options: ColumnBuilderOptions) -> Self {
        Self {
            data: vec![],
            index: vec![],
            options,
            current_builder: None,
            row_count: 0,
            last_row_count: 0,
        }
    }

    fn finish_builder(&mut self) {
        use I32BlockBuilderImpl::*;

        let (block_type, mut block_data) = match self.current_builder.take().unwrap() {
            PlainI32(builder) => (BlockType::Plain, builder.finish()),
        };

        self.index.push(BlockIndex {
            block_type: block_type.into(),
            offset: self.data.len() as u64,
            length: block_data.len() as u64,
            first_rowid: self.last_row_count as u64,
            /// TODO(chi): support sort key
            first_key: "".into(),
        });

        // the new block will begin at the current row count
        self.last_row_count = self.row_count;

        let mut block_header = vec![0; 16];

        let mut block_header_ref = &mut block_header[..];

        // add block type
        block_header_ref.put_i32(block_type.into());

        // add checksum
        block_header_ref.put_i32(ChecksumType::None.into());

        // TODO(chi): add checksum support
        block_header_ref.put_u64(0);

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
fn append_one_by_one<'a>(
    iter: &mut Peekable<impl Iterator<Item = Option<&'a i32>>>,
    builder: &mut impl BlockBuilder<I32Array>,
) -> (usize, bool) {
    let mut cnt = 0;
    while let Some(item) = iter.peek() {
        // peek and see if we could push more items into the builder
        let to_be_appended = item.unwrap_or(&0);

        if builder.should_finish(to_be_appended) {
            return (cnt, true);
        }

        // get the item from iterator and push it to the builder
        let to_be_appended = iter.next().unwrap().unwrap_or(&0);

        builder.append(to_be_appended);
        cnt += 1;
    }

    (cnt, false)
}

impl ColumnBuilder<I32Array> for I32ColumnBuilder {
    fn append(&mut self, array: &I32Array) {
        use I32BlockBuilderImpl::*;
        let mut iter = array.iter().peekable();

        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                self.current_builder = Some(PlainI32(PlainI32BlockBuilder::new(
                    self.options.target_size - 16,
                )));
            }

            let (row_count, should_finish) = match self.current_builder.as_mut().unwrap() {
                PlainI32(builder) => append_one_by_one(&mut iter, builder),
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
        let mut builder = I32ColumnBuilder::new(ColumnBuilderOptions { target_size: 128 });
        for _ in 0..10 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(item_each_block),
            ));
        }
        assert_eq!(builder.finish().0.len(), 10);

        // In this case, we append array that is smaller than each block, and fill fewer than 2 blocks of contents
        let mut builder = I32ColumnBuilder::new(ColumnBuilderOptions { target_size: 128 });
        for _ in 0..12 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(4),
            ));
        }
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append two array that sums up to exactly 2 blocks
        let mut builder = I32ColumnBuilder::new(ColumnBuilderOptions { target_size: 128 });
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(30),
        ));
        builder.append(&I32Array::from_iter(
            [Some(1)].iter().cycle().cloned().take(26),
        ));
        assert_eq!(builder.finish().0.len(), 2);

        // In this case, we append an array that is larger than 1 block.
        let mut builder = I32ColumnBuilder::new(ColumnBuilderOptions { target_size: 128 });
        builder.append(&I32Array::from_iter(
            [Some(1)]
                .iter()
                .cycle()
                .cloned()
                .take(item_each_block * 100),
        ));
        assert_eq!(builder.finish().0.len(), 100);

        // And finally, some chaos test
        let mut builder = I32ColumnBuilder::new(ColumnBuilderOptions { target_size: 128 });
        for _ in 0..100 {
            builder.append(&I32Array::from_iter(
                [Some(1)].iter().cycle().cloned().take(23),
            ));
        }
        assert_eq!(builder.finish().0.len(), 83);
    }
}
