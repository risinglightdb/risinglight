use super::{Block, BlockIterator};
use crate::array::{ArrayBuilder, Utf8Array, Utf8ArrayBuilder};
use bytes::Buf;

/// Scans one or several arrays from the block content.
pub struct PlainVarcharBlockIterator {
    /// Block content
    block: Block,

    /// Total count of elements in block
    row_count: usize,

    /// Indicates the beginning row of the next batch
    next_row: usize,
}

impl PlainVarcharBlockIterator {
    pub fn new(block: Block, row_count: usize) -> Self {
        Self {
            block,
            row_count,
            next_row: 0,
        }
    }
}

impl BlockIterator<Utf8Array> for PlainVarcharBlockIterator {
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
        const OFFSET: usize = std::mem::size_of::<u32>();
        let offsets_length = OFFSET * self.row_count;
        let offset_buffer = &self.block[0..offsets_length];
        let data_buffer = &self.block[offsets_length..];

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

            let from;
            let to;

            if self.next_row == 0 {
                let mut cur_offsets = offset_buffer;
                from = 0;
                to = cur_offsets.get_u32_le() as usize;
            } else {
                let mut cur_offsets = &offset_buffer[(self.next_row - 1) * OFFSET..];
                from = cur_offsets.get_u32_le() as usize;
                to = cur_offsets.get_u32_le() as usize;
            }
            builder.push(Some(std::str::from_utf8(&data_buffer[from..to]).unwrap()));

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

    use crate::array::ArrayToVecExt;
    use crate::array::{ArrayBuilder, Utf8ArrayBuilder};
    use crate::storage::secondary::block::{BlockBuilder, PlainVarcharBlockBuilder};
    use crate::storage::secondary::BlockIterator;

    use super::*;

    #[test]
    fn test_scan_varchar() {
        let mut builder = PlainVarcharBlockBuilder::new(128);
        builder.append(Some("233"));
        builder.append(Some("2333"));
        builder.append(Some("23333"));
        let data = builder.finish();

        let mut scanner = PlainVarcharBlockIterator::new(Bytes::from(data), 3);

        let mut builder = Utf8ArrayBuilder::new(0);

        scanner.skip(1);
        assert_eq!(scanner.remaining_items(), 2);

        assert_eq!(scanner.next_batch(Some(1), &mut builder), 1);
        assert_eq!(builder.finish().to_vec(), vec![Some("2333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new(0);
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 1);

        assert_eq!(builder.finish().to_vec(), vec![Some("23333".to_string())]);

        let mut builder = Utf8ArrayBuilder::new(0);
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
