use super::{Block, BlockIterator};
use crate::array::{ArrayBuilder, Utf8Array, Utf8ArrayBuilder};
use bytes::Buf;

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

impl BlockIterator<Utf8Array> for PlainCharBlockIterator {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut Utf8ArrayBuilder,
    ) -> usize {
        if self.next_row >= self.row_count {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        let mut buffer = &self.block[self.next_row * (self.char_width + 1)..];

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

            let length = buffer.get_u8() as usize;
            builder.push(Some(std::str::from_utf8(&buffer[..length]).unwrap()));
            buffer = &buffer[self.char_width..];

            cnt += 1;
            self.next_row += 1;
        }

        cnt
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

    use crate::{
        array::{ArrayBuilder, ArrayToVecExt, Utf8ArrayBuilder},
        storage::secondary::{
            block::{BlockBuilder, PlainCharBlockBuilder},
            BlockIterator,
        },
    };

    use super::*;

    #[test]
    fn test_scan_char() {
        let mut builder = PlainCharBlockBuilder::new(128, 20);
        builder.append(Some("233"));
        builder.append(Some("2333"));
        builder.append(Some("23333"));
        let data = builder.finish();

        let mut scanner = PlainCharBlockIterator::new(Bytes::from(data), 3, 20);

        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 2);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some("2333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some("23333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
