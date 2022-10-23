// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::hash::Hash;

use bytes::BufMut;
use risinglight_proto::rowset::BlockStatistics;

use super::PlainPrimitiveBlockBuilder;
use crate::array::{Array, I32Array};
use crate::storage::secondary::block::{BlockBuilder, RleBlockBuilder};

pub(crate) const DICT_NULL_VALUE_KEY: i32 = i32::MIN;
/// Encodes fixed-width data into a block with dict encoding. The layout is
/// ```plain
/// | rle_block_length(u64) |  dict_count_sum (u32) | rle_block | dict_block |
/// ```
pub struct DictBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq,
    <A::Item as ToOwned>::Owned: Eq + Hash,
{
    dict_map: HashMap<<A::Item as ToOwned>::Owned, i32>,
    data_builder: B,
    rle_builder: RleBlockBuilder<I32Array, PlainPrimitiveBlockBuilder<i32>>,
    cur_index: i32,
}

impl<A, B> DictBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq,
    <A::Item as ToOwned>::Owned: Eq + Hash,
{
    pub fn new(block_builder: B) -> Self {
        let builder = PlainPrimitiveBlockBuilder::new(block_builder.estimated_size());
        // create rle_builder to help record dictionary values and compress them using run-length
        // encoding
        let rle_builder =
            RleBlockBuilder::<I32Array, PlainPrimitiveBlockBuilder<i32>>::new(builder);
        Self {
            dict_map: HashMap::new(),
            data_builder: block_builder,
            rle_builder,
            cur_index: DICT_NULL_VALUE_KEY,
        }
    }
}

impl<A, B> BlockBuilder<A> for DictBlockBuilder<A, B>
where
    A: Array,
    B: BlockBuilder<A>,
    A::Item: PartialEq + Eq + Hash,
    <A::Item as ToOwned>::Owned: Eq + Hash,
{
    fn append(&mut self, item: Option<&A::Item>) {
        let mut key = DICT_NULL_VALUE_KEY;
        if let Some(item) = item {
            if let Some(value) = self.dict_map.get(item) {
                key = value.to_owned();
            } else {
                self.cur_index += 1;
                key = self.cur_index;
                self.data_builder.append(Some(item));
                self.dict_map.insert(item.to_owned(), key);
            }
        }
        self.rle_builder.append(Some(&key));
    }

    fn estimated_size(&self) -> usize {
        2 * std::mem::size_of::<u32>()
            + self.rle_builder.estimated_size()
            + self.data_builder.estimated_size()
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        // Tracking issue: https://github.com/risinglightdb/risinglight/issues/674
        vec![]
    }

    fn should_finish(&self, next_item: &Option<&A::Item>) -> bool {
        self.data_builder.should_finish(next_item)
            || self
                .rle_builder
                .should_finish(&Some(&(DICT_NULL_VALUE_KEY)))
    }

    fn finish(self) -> Vec<u8> {
        let mut encoded_data: Vec<u8> = vec![];
        let rle_block = self.rle_builder.finish();
        encoded_data.put_u64(rle_block.len() as u64);
        encoded_data.put_u32(self.dict_map.len() as u32);
        encoded_data.extend(rle_block);
        encoded_data.extend(self.data_builder.finish());
        encoded_data
    }

    fn get_target_size(&self) -> usize {
        self.data_builder.get_target_size()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use ordered_float::OrderedFloat;

    use crate::array::{I64Array, Utf8Array};
    use crate::storage::secondary::block::dict_block_builder::DictBlockBuilder;
    use crate::storage::secondary::block::{
        BlockBuilder, NullableBlockBuilder, PlainBlobBlockBuilder, PlainCharBlockBuilder,
        PlainPrimitiveBlockBuilder,
    };
    use crate::types::F64;

    #[test]
    fn test_build_dict_primitive_f64() {
        let inner_builder = PlainPrimitiveBlockBuilder::<F64>::new(13);
        let builder = NullableBlockBuilder::new(inner_builder, 13);
        let mut dict_builder = DictBlockBuilder::new(builder);
        for num in 1..4 {
            for item in [Some(&(OrderedFloat::from(f64::from(num))))]
                .iter()
                .cycle()
                .cloned()
                .take(30)
            {
                dict_builder.append(item);
            }
        }
        // rle_counts_num (u32) | rle_count (u16) | rle_count | data
        assert_eq!(
            dict_builder.estimated_size(),
            4 * 2 + (4 + 2 * 3 + 4 * 3) + 8 * 3 + 1
        );
        assert!(dict_builder.should_finish(&Some(&OrderedFloat::from(3.0))));
        assert!(dict_builder.should_finish(&Some(&OrderedFloat::from(4.0))));
        dict_builder.finish();
    }

    #[test]
    fn test_build_dict_primitive_i64() {
        let builder = PlainPrimitiveBlockBuilder::<i64>::new(13);
        let mut dict_builder =
            DictBlockBuilder::<I64Array, PlainPrimitiveBlockBuilder<i64>>::new(builder);
        for item in [Some(&(1))].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&(2))].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&(3))].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        // rle_counts_num (u32) | rle_count (u16) | rle_count | data
        assert_eq!(
            dict_builder.estimated_size(),
            4 * 2 + (4 + 2 * 3 + 4 * 3) + 8 * 3
        );
        assert!(dict_builder.should_finish(&Some(&3)));
        assert!(dict_builder.should_finish(&Some(&4)));
        dict_builder.finish();
    }

    #[test]
    fn test_build_dict_primitive_nullable_i64() {
        let inner_builder = PlainPrimitiveBlockBuilder::<i64>::new(48);
        let builder = NullableBlockBuilder::new(inner_builder, 48);
        let mut dict_builder = DictBlockBuilder::new(builder);
        for item in [None].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&1)].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&2)].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&3)].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&4)].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [None].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&5)]
            .iter()
            .cycle()
            .cloned()
            .take(u16::MAX as usize * 2)
        {
            dict_builder.append(item);
        }
        assert_eq!(dict_builder.estimated_size(), 4 * 2 + (64) + 8 * 5 + 1);
        assert!(dict_builder.should_finish(&Some(&5)));
        dict_builder.finish();
    }

    #[test]
    fn test_build_dict_char() {
        let builder = PlainCharBlockBuilder::new(120, 160);
        let mut dict_builder = DictBlockBuilder::new(builder);

        let width_40_char = ["2333"].iter().cycle().take(40).join("");

        for item in [Some("233")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some("2333")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        assert_eq!(
            dict_builder.estimated_size(),
            4 * 2 + (4 + 4 * 2 + 4 * 4) + 4 * 40 * 3
        );
        assert!(dict_builder.should_finish(&Some(&width_40_char[..])));
        assert!(dict_builder.should_finish(&Some("2333333")));
        dict_builder.finish();
    }

    #[test]
    fn test_build_dict_varchar() {
        let builder = PlainBlobBlockBuilder::new(30);
        let mut dict_builder =
            DictBlockBuilder::<Utf8Array, PlainBlobBlockBuilder<str>>::new(builder);
        let width_40_char = ["2333"].iter().cycle().take(40).join("");
        for item in [Some("233")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some("23333")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some("2333333")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some("233")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some("23333")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some("2333333")].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        for item in [Some(&width_40_char[..])].iter().cycle().cloned().take(30) {
            dict_builder.append(item);
        }
        assert_eq!(
            dict_builder.estimated_size(),
            4 * 2 + (4 + 8 * 2 + 8 * 4) + 7 + 9 + 11 + 164
        ); // 37
        assert!(dict_builder.should_finish(&Some("2333333")));
        assert!(dict_builder.should_finish(&Some("23333333")));
        dict_builder.finish();
    }
}
