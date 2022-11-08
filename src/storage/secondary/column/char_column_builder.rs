// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{BlockBuilder, BlockIndexBuilder, PlainCharBlockBuilder};
use super::{append_one_by_one, ColumnBuilder};
use crate::array::{Array, Utf8Array};
use crate::storage::secondary::block::{
    DictBlockBuilder, NullableBlockBuilder, PlainBlobBlockBuilder, RleBlockBuilder,
};
use crate::storage::secondary::{ColumnBuilderOptions, EncodeType};

type PlainNullableCharBlockBuilder = NullableBlockBuilder<Utf8Array, PlainCharBlockBuilder>;
type PlainNullableVarcharBlockBuilder = NullableBlockBuilder<Utf8Array, PlainBlobBlockBuilder<str>>;

/// All supported block builders for char types.
pub(super) enum CharBlockBuilderImpl {
    PlainFixedChar(PlainCharBlockBuilder),
    PlainNullableFixedChar(PlainNullableCharBlockBuilder),
    PlainVarchar(PlainBlobBlockBuilder<str>),
    PlainNullableVarchar(PlainNullableVarcharBlockBuilder),
    RleFixedChar(RleBlockBuilder<Utf8Array, PlainCharBlockBuilder>),
    RleNullableFixedChar(RleBlockBuilder<Utf8Array, PlainNullableCharBlockBuilder>),
    RleVarchar(RleBlockBuilder<Utf8Array, PlainBlobBlockBuilder<str>>),
    RleNullableVarchar(RleBlockBuilder<Utf8Array, PlainNullableVarcharBlockBuilder>),
    DictFixedChar(DictBlockBuilder<Utf8Array, PlainCharBlockBuilder>),
    DictNullableFixedChar(DictBlockBuilder<Utf8Array, PlainNullableCharBlockBuilder>),
    DictVarchar(DictBlockBuilder<Utf8Array, PlainBlobBlockBuilder<str>>),
    DictNullableVarchar(DictBlockBuilder<Utf8Array, PlainNullableVarcharBlockBuilder>),
}

macro_rules! for_all_char_block_builder_enum {
    ($marco:tt) => {
        $marco! {
            PlainFixedChar,
            PlainNullableFixedChar,
            PlainVarchar,
            PlainNullableVarchar,
            RleFixedChar,
            RleNullableFixedChar,
            RleVarchar,
            RleNullableVarchar,
            DictFixedChar,
            DictNullableFixedChar,
            DictVarchar,
            DictNullableVarchar
        }
    };
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

        macro_rules! finish_current_builder {
            ($($enum_val:ident),*) => {
                match self.current_builder.take().unwrap() {
                    $(
                    CharBlockBuilderImpl::$enum_val(builder) => (
                        BlockType::$enum_val,
                        builder.get_statistics(),
                        builder.finish(),
                    ),)*
            }
            }
        }

        let (block_type, stats, mut block_data) =
            for_all_char_block_builder_enum! {finish_current_builder};

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
                let target_size = self.options.target_block_size - 16;
                match (self.char_width, self.nullable, self.options.encode_type) {
                    (Some(char_width), false, EncodeType::RunLength) => {
                        let builder = PlainCharBlockBuilder::new(target_size, char_width);
                        self.current_builder =
                            Some(CharBlockBuilderImpl::RleFixedChar(RleBlockBuilder::<
                                Utf8Array,
                                PlainCharBlockBuilder,
                            >::new(
                                builder
                            )));
                    }
                    (Some(char_width), true, EncodeType::RunLength) => {
                        let nullable_builder = NullableBlockBuilder::new(
                            PlainCharBlockBuilder::new(target_size, char_width),
                            target_size,
                        );
                        self.current_builder = Some(CharBlockBuilderImpl::RleNullableFixedChar(
                            RleBlockBuilder::new(nullable_builder),
                        ));
                    }
                    (Some(char_width), false, EncodeType::Plain) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainFixedChar(
                            PlainCharBlockBuilder::new(target_size, char_width),
                        ));
                    }
                    (Some(char_width), true, EncodeType::Plain) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainNullableFixedChar(
                            NullableBlockBuilder::new(
                                PlainCharBlockBuilder::new(target_size, char_width),
                                target_size,
                            ),
                        ));
                    }
                    (Some(char_width), false, EncodeType::Dictionary) => {
                        let builder = PlainCharBlockBuilder::new(target_size, char_width);
                        self.current_builder =
                            Some(CharBlockBuilderImpl::DictFixedChar(DictBlockBuilder::<
                                Utf8Array,
                                PlainCharBlockBuilder,
                            >::new(
                                builder
                            )));
                    }
                    (Some(char_width), true, EncodeType::Dictionary) => {
                        let nullable_builder = NullableBlockBuilder::new(
                            PlainCharBlockBuilder::new(target_size, char_width),
                            target_size,
                        );
                        self.current_builder = Some(CharBlockBuilderImpl::DictNullableFixedChar(
                            DictBlockBuilder::new(nullable_builder),
                        ));
                    }
                    (None, false, EncodeType::RunLength) => {
                        let builder = PlainBlobBlockBuilder::new(target_size);
                        self.current_builder =
                            Some(CharBlockBuilderImpl::RleVarchar(RleBlockBuilder::<
                                Utf8Array,
                                PlainBlobBlockBuilder<str>,
                            >::new(
                                builder
                            )));
                    }
                    (None, true, EncodeType::RunLength) => {
                        let nullable_builder =
                            NullableBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(
                                PlainBlobBlockBuilder::new(target_size),
                                target_size,
                            );
                        self.current_builder = Some(CharBlockBuilderImpl::RleNullableVarchar(
                            RleBlockBuilder::new(nullable_builder),
                        ));
                    }
                    (None, false, EncodeType::Plain) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainVarchar(
                            PlainBlobBlockBuilder::new(target_size),
                        ));
                    }
                    (None, true, EncodeType::Plain) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainNullableVarchar(
                            NullableBlockBuilder::new(
                                PlainBlobBlockBuilder::new(target_size),
                                target_size,
                            ),
                        ));
                    }
                    (None, false, EncodeType::Dictionary) => {
                        let builder = PlainBlobBlockBuilder::new(target_size);
                        self.current_builder =
                            Some(CharBlockBuilderImpl::DictVarchar(DictBlockBuilder::<
                                Utf8Array,
                                PlainBlobBlockBuilder<str>,
                            >::new(
                                builder
                            )));
                    }
                    (None, true, EncodeType::Dictionary) => {
                        let nullable_builder =
                            NullableBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(
                                PlainBlobBlockBuilder::new(target_size),
                                target_size,
                            );
                        self.current_builder = Some(CharBlockBuilderImpl::DictNullableVarchar(
                            DictBlockBuilder::new(nullable_builder),
                        ));
                    }
                }

                if let Some(to_be_appended) = iter.peek() {
                    if self.options.record_first_key {
                        self.first_key = to_be_appended.map(|x| x.as_bytes().to_vec());
                    }
                }
            }

            macro_rules! append_one_by_one {
                ($($enum_val:ident),*) => {
                    match self.current_builder.as_mut().unwrap() {
                        $(
                            CharBlockBuilderImpl::$enum_val(builder) => {append_one_by_one(&mut iter, builder)}
                        ),*
                    }
                }
            }

            let (row_count, should_finish) = for_all_char_block_builder_enum! { append_one_by_one };

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

    use itertools::Itertools;

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
    fn test_varchar_nullable_column_builder() {
        // leave 8 bytes for null bitmap & a offset entry
        let item_each_block = (128 - 16 - 8) / 8;
        let mut builder =
            CharColumnBuilder::new(true, None, ColumnBuilderOptions::default_for_block_test());
        for _ in 0..10 {
            // `item_each_block` is 13, so will be 7 Some and 6 None entries
            builder.append(&Utf8Array::from_iter(
                [Some("nijigaku"), None]
                    .iter()
                    .cycle()
                    .cloned()
                    .take(item_each_block),
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
    fn test_varchar_nullable_column_dict_builder_large_block() {
        let mut builder = CharColumnBuilder::new(
            true,
            None,
            ColumnBuilderOptions::default_for_dict_block_test(),
        );

        // We set char width to 150, which is larger than target block size
        let width_110_char = ["2"].iter().cycle().take(150).join("");
        for _ in 0..5 {
            builder.append(&Utf8Array::from_iter([Some(&width_110_char), None]))
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 10);
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
