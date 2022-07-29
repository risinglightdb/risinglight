// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{BlockBuilder, BlockIndexBuilder, PlainCharBlockBuilder};
use super::{append_one_by_one, ColumnBuilder};
use crate::array::{Array, Utf8Array};
use crate::storage::secondary::block::{DictBlockBuilder, PlainBlobBlockBuilder, RleBlockBuilder};
use crate::storage::secondary::{ColumnBuilderOptions, EncodeType};

/// All supported block builders for char types.
pub(super) enum CharBlockBuilderImpl {
    PlainFixedChar(PlainCharBlockBuilder),
    PlainVarchar(PlainBlobBlockBuilder<str>),
    RleFixedChar(RleBlockBuilder<Utf8Array, PlainCharBlockBuilder>),
    RleVarchar(RleBlockBuilder<Utf8Array, PlainBlobBlockBuilder<str>>),
    DictFixedChar(DictBlockBuilder<Utf8Array, PlainCharBlockBuilder>),
    DictVarchar(DictBlockBuilder<Utf8Array, PlainBlobBlockBuilder<str>>),
}

/// Column builder of char types.
pub struct CharColumnBuilder {
    data: Vec<u8>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<CharBlockBuilderImpl>,

    /// Block index builder
    block_index_builder: BlockIndexBuilder,

    /// Indicates whether the current column accepts null elements
    nullable: bool,

    /// Width of the char column
    char_width: Option<u64>,

    /// First key
    first_key: Option<Vec<u8>>,
}

impl CharColumnBuilder {
    pub fn new(nullable: bool, char_width: Option<u64>, options: ColumnBuilderOptions) -> Self {
        Self {
            data: vec![],
            block_index_builder: BlockIndexBuilder::new(options.clone()),
            options,
            current_builder: None,
            nullable,
            char_width,
            first_key: None,
        }
    }

    fn finish_builder(&mut self) {
        if self.current_builder.is_none() {
            return;
        }

        let (block_type, stats, mut block_data) = match self.current_builder.take().unwrap() {
            CharBlockBuilderImpl::PlainFixedChar(builder) => (
                BlockType::PlainFixedChar,
                builder.get_statistics(),
                builder.finish(),
            ),
            CharBlockBuilderImpl::PlainVarchar(builder) => (
                BlockType::PlainVarchar,
                builder.get_statistics(),
                builder.finish(),
            ),
            CharBlockBuilderImpl::RleFixedChar(builder) => (
                BlockType::RleFixedChar,
                builder.get_statistics(),
                builder.finish(),
            ),
            CharBlockBuilderImpl::RleVarchar(builder) => (
                BlockType::RleVarchar,
                builder.get_statistics(),
                builder.finish(),
            ),
            CharBlockBuilderImpl::DictFixedChar(builder) => (
                BlockType::DictFixedChar,
                builder.get_statistics(),
                builder.finish(),
            ),
            CharBlockBuilderImpl::DictVarchar(builder) => (
                BlockType::DictVarchar,
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

impl ColumnBuilder<Utf8Array> for CharColumnBuilder {
    fn append(&mut self, array: &Utf8Array) {
        let mut iter = array.iter().peekable();

        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                match (self.char_width, self.nullable, self.options.encode_type) {
                    (Some(char_width), false, EncodeType::RunLength) => {
                        let builder = PlainCharBlockBuilder::new(
                            self.options.target_block_size - 16,
                            char_width,
                        );
                        self.current_builder =
                            Some(CharBlockBuilderImpl::RleFixedChar(RleBlockBuilder::<
                                Utf8Array,
                                PlainCharBlockBuilder,
                            >::new(
                                builder
                            )));
                    }
                    (Some(char_width), false, EncodeType::Plain) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainFixedChar(
                            PlainCharBlockBuilder::new(
                                self.options.target_block_size - 16,
                                char_width,
                            ),
                        ));
                    }
                    (Some(char_width), false, EncodeType::Dictionary) => {
                        let builder = PlainCharBlockBuilder::new(
                            self.options.target_block_size - 16,
                            char_width,
                        );
                        self.current_builder =
                            Some(CharBlockBuilderImpl::DictFixedChar(DictBlockBuilder::<
                                Utf8Array,
                                PlainCharBlockBuilder,
                            >::new(
                                builder
                            )));
                    }
                    (None, _, EncodeType::RunLength) => {
                        let builder =
                            PlainBlobBlockBuilder::new(self.options.target_block_size - 16);
                        self.current_builder =
                            Some(CharBlockBuilderImpl::RleVarchar(RleBlockBuilder::<
                                Utf8Array,
                                PlainBlobBlockBuilder<str>,
                            >::new(
                                builder
                            )));
                    }
                    (None, _, EncodeType::Plain) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainVarchar(
                            PlainBlobBlockBuilder::new(self.options.target_block_size - 16),
                        ));
                    }
                    (None, _, EncodeType::Dictionary) => {
                        let builder =
                            PlainBlobBlockBuilder::new(self.options.target_block_size - 16);
                        self.current_builder =
                            Some(CharBlockBuilderImpl::DictVarchar(DictBlockBuilder::<
                                Utf8Array,
                                PlainBlobBlockBuilder<str>,
                            >::new(
                                builder
                            )));
                    }
                    (char_width, nullable, _) => unimplemented!(
                        "width {:?} with nullable {} not implemented",
                        char_width,
                        nullable
                    ),
                }

                if let Some(to_be_appended) = iter.peek() {
                    if self.options.record_first_key {
                        self.first_key = to_be_appended.map(|x| x.as_bytes().to_vec());
                    }
                }
            }

            let (row_count, should_finish) = match self.current_builder.as_mut().unwrap() {
                CharBlockBuilderImpl::PlainFixedChar(builder) => {
                    append_one_by_one(&mut iter, builder)
                }
                CharBlockBuilderImpl::PlainVarchar(builder) => {
                    append_one_by_one(&mut iter, builder)
                }
                CharBlockBuilderImpl::RleFixedChar(builder) => {
                    append_one_by_one(&mut iter, builder)
                }
                CharBlockBuilderImpl::RleVarchar(builder) => append_one_by_one(&mut iter, builder),
                CharBlockBuilderImpl::DictFixedChar(builder) => {
                    append_one_by_one(&mut iter, builder)
                }
                CharBlockBuilderImpl::DictVarchar(builder) => append_one_by_one(&mut iter, builder),
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

    #[test]
    fn test_char_column_builder() {
        let item_each_block = (128 - 16) / 8;
        let mut builder = CharColumnBuilder::new(
            false,
            Some(8),
            ColumnBuilderOptions::default_for_block_test(),
        );
        for _ in 0..10 {
            builder.append(&Utf8Array::from_iter(
                [Some("2333")].iter().cycle().cloned().take(item_each_block),
            ));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 10);
        assert_eq!(index[3].first_rowid as usize, item_each_block * 3);
        assert_eq!(index[3].row_count as usize, item_each_block);
    }

    #[test]
    fn test_char_column_rle_builder() {
        let distinct_item_each_block = (128 - 16) / 8;
        let mut builder = CharColumnBuilder::new(
            false,
            Some(8),
            ColumnBuilderOptions::default_for_rle_block_test(),
        );
        for num in 0..(distinct_item_each_block + 1) {
            builder.append(&Utf8Array::from_iter(
                [Some(num.to_string().as_str())]
                    .iter()
                    .cycle()
                    .cloned()
                    .take(23),
            ));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 2);
        assert_eq!(index[0].first_rowid as usize, 0);
        // not `distinct_item_each_block * 23` because the `should_finish` of `RleBlockBuilder`
        // isn't very accurate, so the first block append 1 more item then expected.
        assert_eq!(
            index[1].first_rowid as usize,
            (distinct_item_each_block - 1) * 23 + 1
        );
        assert_eq!(index[1].row_count as usize, 22 + 23);
    }

    #[test]
    fn test_char_column_builder_large_block() {
        // We set char width to 233, which is larger than target block size
        let mut builder = CharColumnBuilder::new(
            false,
            Some(233),
            ColumnBuilderOptions::default_for_block_test(),
        );
        for _ in 0..10 {
            builder.append(&Utf8Array::from_iter([Some("2333")]));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 10);
    }
}
