use risinglight_proto::rowset::block_checksum::ChecksumType;
use risinglight_proto::rowset::block_index::BlockType;
use risinglight_proto::rowset::BlockIndex;

use super::super::{BlockBuilder, PlainCharBlockBuilder, PlainVarcharBlockBuilder};
use super::{append_one_by_one, ColumnBuilder};
use crate::array::{Array, UTF8Array};
use crate::storage::secondary::{BlockHeader, ColumnBuilderOptions, BLOCK_HEADER_SIZE};

/// All supported block builders for char types.
pub(super) enum CharBlockBuilderImpl {
    PlainFixedChar(PlainCharBlockBuilder),
    PlainVarchar(PlainVarcharBlockBuilder),
}

/// Column builder of char types.
pub struct CharColumnBuilder {
    data: Vec<u8>,
    index: Vec<BlockIndex>,
    options: ColumnBuilderOptions,

    /// Current block builder
    current_builder: Option<CharBlockBuilderImpl>,

    /// Count of rows which has been sent to builder
    row_count: usize,

    /// Begin row count of the current block
    last_row_count: usize,

    /// Indicates whether the current column accepts null elements
    nullable: bool,

    /// Width of the char column
    char_width: Option<u64>,
}

impl CharColumnBuilder {
    pub fn new(nullable: bool, char_width: Option<u64>, options: ColumnBuilderOptions) -> Self {
        Self {
            data: vec![],
            index: vec![],
            options,
            current_builder: None,
            row_count: 0,
            last_row_count: 0,
            nullable,
            char_width,
        }
    }

    fn finish_builder(&mut self) {
        let (block_type, mut block_data) = match self.current_builder.take().unwrap() {
            CharBlockBuilderImpl::PlainFixedChar(builder) => {
                (BlockType::PlainFixedChar, builder.finish())
            }
            CharBlockBuilderImpl::PlainVarchar(builder) => {
                (BlockType::PlainVarchar, builder.finish())
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

impl ColumnBuilder<UTF8Array> for CharColumnBuilder {
    fn append(&mut self, array: &UTF8Array) {
        let mut iter = array.iter().peekable();

        while iter.peek().is_some() {
            if self.current_builder.is_none() {
                match (self.char_width, self.nullable) {
                    (Some(char_width), false) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainFixedChar(
                            PlainCharBlockBuilder::new(
                                self.options.target_block_size - 16,
                                char_width,
                            ),
                        ));
                    }
                    (None, false) => {
                        self.current_builder = Some(CharBlockBuilderImpl::PlainVarchar(
                            PlainVarcharBlockBuilder::new(self.options.target_block_size - 16),
                        ));
                    }
                    _ => unimplemented!(),
                }
            }

            let (row_count, should_finish) = match self.current_builder.as_mut().unwrap() {
                CharBlockBuilderImpl::PlainFixedChar(builder) => {
                    append_one_by_one(&mut iter, builder)
                }
                CharBlockBuilderImpl::PlainVarchar(builder) => {
                    append_one_by_one(&mut iter, builder)
                }
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
    use std::iter::FromIterator;

    use super::*;

    #[test]
    fn test_char_column_builder() {
        let item_each_block = (128 - 16) / 8;
        let mut builder = CharColumnBuilder::new(
            false,
            Some(7),
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
        for _ in 0..10 {
            builder.append(&UTF8Array::from_iter(
                [Some("2333")].iter().cycle().cloned().take(item_each_block),
            ));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 10);
        assert_eq!(index[3].first_rowid as usize, item_each_block * 3);
        assert_eq!(index[3].row_count as usize, item_each_block);
    }

    #[test]
    fn test_char_column_builder_large_block() {
        // We set char width to 233, which is larger than target block size
        let mut builder = CharColumnBuilder::new(
            false,
            Some(233),
            ColumnBuilderOptions {
                target_block_size: 128,
            },
        );
        for _ in 0..10 {
            builder.append(&UTF8Array::from_iter([Some("2333")]));
        }
        let (index, _) = builder.finish();
        assert_eq!(index.len(), 10);
    }
}
