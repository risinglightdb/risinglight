// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;

use super::{Block, BlockIterator, NonNullableBlockIterator};
use crate::array::{ArrayBuilder, Utf8Array, Utf8ArrayBuilder};

/// Scans one or several arrays from the block content.
pub struct PlainCharBlockIterator {
    /// Block content
    block: Block,

    /// Total count of elements in block
    row_count: usize,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    /// Width of the char column
    char_width: usize,
}

impl PlainCharBlockIterator {
    pub fn new(block: Block, row_count: usize, char_width: usize) -> Self {
        Self {
            block,
            row_count,
            next_row: 0,
            char_width,
        }
    }
}

impl NonNullableBlockIterator<Utf8Array> for PlainCharBlockIterator {
    fn next_batch_non_null(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <Utf8Array as crate::array::Array>::Builder,
    ) -> usize {
        if self.next_row >= self.row_count {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        let mut buffer = &self.block[self.next_row * self.char_width..];

        loop {
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                if cnt >= expected_size {
                    break;
                }
            }

            if self.next_row >= self.row_count {
                break;
            }

            // take a slice out of the buffer
            let data_buffer = &buffer[..self.char_width];
            // find the first `\0` inside
            let pos = data_buffer
                .iter()
                .find_position(|x| **x == 0)
                .map(|x| x.0)
                .unwrap_or(self.char_width);

            builder.push(Some(std::str::from_utf8(&buffer[..pos]).unwrap()));
            buffer = &buffer[self.char_width..];

            cnt += 1;
            self.next_row += 1;
        }

        cnt
    }
}

impl BlockIterator<Utf8Array> for PlainCharBlockIterator {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut Utf8ArrayBuilder,
    ) -> usize {
        self.next_batch_non_null(expected_size, builder)
    }

    fn skip(&mut self, cnt: usize) {
        self.next_row += cnt;
    }

    fn remaining_items(&self) -> usize {
        self.row_count - self.next_row
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::array::{ArrayBuilder, ArrayToVecExt, Utf8ArrayBuilder};
    use crate::storage::secondary::block::{BlockBuilder, PlainCharBlockBuilder};
    use crate::storage::secondary::BlockIterator;

    #[test]
    fn test_scan_char() {
        let mut builder = PlainCharBlockBuilder::new(128, 20);
        let width_20_char = ["2"].iter().cycle().take(20).join("");

        builder.append(Some("233"));
        builder.append(Some("2333"));
        builder.append(Some("23333"));
        builder.append(Some(&width_20_char));
        let data = builder.finish();

        let mut scanner = PlainCharBlockIterator::new(Bytes::from(data), 4, 20);

        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 3);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some("2333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some("23333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some(width_20_char)]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
