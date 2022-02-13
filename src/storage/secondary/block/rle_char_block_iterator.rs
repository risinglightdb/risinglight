// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::Buf;
use itertools::Itertools;

use super::{Block, BlockIterator};
use crate::array::{Array, ArrayBuilder, Utf8Array};

/// Scans one or several arrays from the RLE Char block content,
/// including plain char and varchar block.
pub struct RLECharBlockIterator {
    /// Block content
    block: Block,

    /// char width
    char_width: Option<usize>,

    /// Indicates the beginning row of the next batch
    next_row: usize,

    /// current pos count
    cur_count: usize,

    /// The number of rle_counts
    rle_counts_num: usize,
}

impl RLECharBlockIterator {
    pub fn new(block: Block, char_width: Option<usize>) -> Self {
        let mut rle_counts_num = &block[..];
        let rle_counts_num = rle_counts_num.get_u32_le() as usize;
        Self {
            block,
            char_width,
            next_row: 0,
            cur_count: 0,
            rle_counts_num,
        }
    }

    fn get_cur_data_inner(&self) -> (String, u16) {
        let rle_counts_length =
            std::mem::size_of::<u32>() + std::mem::size_of::<u16>() * self.rle_counts_num;
        let rle_counts_buffer = &self.block[std::mem::size_of::<u32>()..rle_counts_length];
        let mut cur_rle_counts_buf =
            &rle_counts_buffer[self.next_row * std::mem::size_of::<u16>()..];
        let rle_count = cur_rle_counts_buf.get_u16_le();

        let mut buffer = &self.block[rle_counts_length..];
        let data = if let Some(char_width) = self.char_width {
            let cur_left = self.next_row * char_width;
            let data_buffer = &buffer[cur_left..(cur_left + char_width)];
            // find the first `\0` inside
            let pos = data_buffer
                .iter()
                .find_position(|x| **x == 0)
                .map(|x| x.0)
                .unwrap_or(char_width);
            let data_cloned = data_buffer[..pos].iter().copied().collect_vec();
            String::from_utf8(data_cloned).unwrap()
        } else {
            let offsets_length = std::mem::size_of::<u32>() * self.rle_counts_num;
            let offset_buffer = &buffer[..offsets_length];
            buffer = &buffer[offsets_length..];
            let from;
            let to;
            if self.next_row == 0 {
                let mut cur_offsets = offset_buffer;
                from = 0;
                to = cur_offsets.get_u32_le() as usize;
            } else {
                let mut cur_offsets =
                    &offset_buffer[(self.next_row - 1) * std::mem::size_of::<u32>()..];
                from = cur_offsets.get_u32_le() as usize;
                to = cur_offsets.get_u32_le() as usize;
            }
            let data_cloned = buffer[from..to].iter().copied().collect_vec();
            String::from_utf8(data_cloned).unwrap()
        };

        (data, rle_count)
    }
}

impl BlockIterator<Utf8Array> for RLECharBlockIterator {
    fn next_batch(
        &mut self,
        expected_size: Option<usize>,
        builder: &mut <Utf8Array as Array>::Builder,
    ) -> usize {
        if self.next_row >= self.rle_counts_num {
            return 0;
        }

        // TODO(chi): error handling on corrupted block

        let mut cnt = 0;
        let (mut data, mut rle_count) = self.get_cur_data_inner();

        loop {
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                if cnt >= expected_size {
                    break;
                }
            }

            if self.cur_count < rle_count as usize {
                builder.push(Some(&data[..]));
                self.cur_count += 1;
                cnt += 1;
            } else {
                self.next_row += 1;
                self.cur_count = 0;
                if self.next_row >= self.rle_counts_num {
                    break;
                }
                (data, rle_count) = self.get_cur_data_inner();
            }
        }

        cnt
    }

    fn skip(&mut self, cnt: usize) {
        let mut cnt = cnt;
        let rle_counts_buffer = &self.block[std::mem::size_of::<u32>()..];
        while cnt > 0 {
            let mut cur_rle_counts_buf =
                &rle_counts_buffer[self.next_row * std::mem::size_of::<u16>()..];
            let rle_count = cur_rle_counts_buf.get_u16_le();
            let cur_left = rle_count as usize - self.cur_count;
            if cur_left > cnt {
                self.cur_count += cnt;
                cnt = 0;
            } else {
                cnt -= cur_left;
                self.cur_count = 0;
                self.next_row += 1;
                if self.next_row >= self.rle_counts_num {
                    break;
                }
            }
        }
    }

    fn remaining_items(&self) -> usize {
        let mut remaining_items: usize = 0;
        let rle_counts_buffer = &self.block[std::mem::size_of::<u32>()..];
        for next_row in self.next_row..self.rle_counts_num {
            let mut cur_rle_counts_buf =
                &rle_counts_buffer[next_row * std::mem::size_of::<u16>()..];
            let rle_count = cur_rle_counts_buf.get_u16_le();
            remaining_items += rle_count as usize;
        }
        remaining_items - self.cur_count
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use super::RLECharBlockIterator;
    use crate::array::{ArrayBuilder, ArrayToVecExt, Utf8ArrayBuilder};
    use crate::storage::secondary::block::{
        BlockBuilder, PlainCharBlockBuilder, PlainVarcharBlockBuilder, RLECharBlockBuilder,
    };
    use crate::storage::secondary::BlockIterator;

    #[test]
    fn test_scan_rle_char() {
        let builder = PlainCharBlockBuilder::new(150, 40);
        let mut rle_builder =
            RLECharBlockBuilder::<PlainCharBlockBuilder>::new(builder, 150, Some(40));

        let width_40_char = ["2"].iter().cycle().take(40).join("");

        for item in [Some("233")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some("2333")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        let data = rle_builder.finish();

        let mut scanner = RLECharBlockIterator::new(Bytes::from(data), Some(40));

        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 6);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("2333".to_string()), Some("2333".to_string())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("2333".to_string()), Some(width_40_char.clone())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(3), &mut builder), 2);

        assert_eq!(
            builder.finish().to_vec(),
            vec![Some(width_40_char.clone()), Some(width_40_char.clone())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }

    #[test]
    fn test_scan_rle_varchar() {
        // Test rle block iterator for varchar
        let builder = PlainVarcharBlockBuilder::new(40);
        let mut rle_builder =
            RLECharBlockBuilder::<PlainVarcharBlockBuilder>::new(builder, 40, None);
        for item in [Some("233")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some("23333")].iter().cycle().cloned().take(3) {
            rle_builder.append(item);
        }
        for item in [Some("2333333")].iter().cycle().cloned().take(2) {
            rle_builder.append(item);
        }
        let data = rle_builder.finish();

        let mut scanner = RLECharBlockIterator::new(Bytes::from(data), None);

        let mut builder = Utf8ArrayBuilder::new();

        scanner.skip(3);
        assert_eq!(scanner.remaining_items(), 5);

        assert_eq!(scanner.next_batch(Some(2), &mut builder), 2);
        assert_eq!(
            builder.finish().to_vec(),
            vec![Some("23333".to_string()), Some("23333".to_string())]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(Some(6), &mut builder), 3);

        assert_eq!(
            builder.finish().to_vec(),
            vec![
                Some("23333".to_string()),
                Some("2333333".to_string()),
                Some("2333333".to_string())
            ]
        );

        let mut builder = Utf8ArrayBuilder::new();
        assert_eq!(scanner.next_batch(None, &mut builder), 0);
    }
}
