// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::prelude::{BitVec, Lsb0};
use bytes::BufMut;
use risinglight_proto::rowset::BlockStatistics;

use super::super::statistics::StatisticsBuilder;
use super::{BlockBuilder, NonNullableBlockBuilder};
use crate::array::VectorArray;
use crate::types::VectorRef;

/// Encodes fixed-chunk data into a block. The data layout is
/// ```plain
/// | data | data | data | element_size |
/// ```
/// The `element_size` is the size for each vector element, and the data is aligned to the
/// `element_size`. The length of each element is `element_size * std::mem::size_of::<f64>()`.
pub struct PlainVectorBlockBuilder {
    data: Vec<u8>,
    element_size: Option<usize>,
    target_size: usize,
}

impl PlainVectorBlockBuilder {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        Self {
            data,
            element_size: None,
            target_size,
        }
    }
}

impl PlainVectorBlockBuilder {
    fn update_element_size(&mut self, new_element_size: usize) {
        if let Some(element_size) = self.element_size {
            assert_eq!(element_size, new_element_size);
        }
        self.element_size = Some(new_element_size);
    }
}

impl NonNullableBlockBuilder<VectorArray> for PlainVectorBlockBuilder {
    fn append_value(&mut self, item: &VectorRef) {
        for i in item.iter() {
            self.data.extend_from_slice(&i.to_le_bytes());
        }
        self.update_element_size(item.len());
    }

    fn append_default(&mut self) {
        panic!("PlainVectorBlockBuilder does not support append_default");
    }

    fn get_statistics_with_bitmap(&self, selection: &BitVec<u8, Lsb0>) -> Vec<BlockStatistics> {
        let selection_empty = selection.is_empty();
        let mut stats_builder = StatisticsBuilder::new();
        let element_size = self.element_size.unwrap();
        let item_cnt = self.data.len() / element_size / std::mem::size_of::<f64>();
        for idx in 0..item_cnt {
            let begin_pos = idx * element_size * std::mem::size_of::<f64>();
            let end_pos = begin_pos + element_size * std::mem::size_of::<f64>();

            if selection_empty || selection[idx] {
                stats_builder.add_item(Some(&self.data[begin_pos..end_pos]));
            }
        }
        stats_builder.get_statistics()
    }

    fn estimated_size_with_next_item(&self, next_item: &Option<&VectorRef>) -> usize {
        self.estimated_size()
            + next_item
                .map(|x| x.len() * std::mem::size_of::<f64>())
                .unwrap_or(0)
            + std::mem::size_of::<u32>()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl BlockBuilder<VectorArray> for PlainVectorBlockBuilder {
    fn append(&mut self, item: Option<&VectorRef>) {
        match item {
            Some(item) => {
                self.append_value(item);
            }
            None => {
                self.append_default();
            }
        }
    }

    fn estimated_size(&self) -> usize {
        self.data.len() + std::mem::size_of::<u32>() // element_size
    }

    fn should_finish(&self, next_item: &Option<&VectorRef>) -> bool {
        !self.is_empty() && self.estimated_size_with_next_item(next_item) > self.target_size
    }

    fn get_statistics(&self) -> Vec<BlockStatistics> {
        self.get_statistics_with_bitmap(&BitVec::new())
    }

    fn finish(self) -> Vec<u8> {
        let mut encoded_data = vec![];
        encoded_data.extend(self.data);
        encoded_data.put_u32(self.element_size.unwrap() as u32); // so that we can likely get vectors aligned
        encoded_data
    }

    fn get_target_size(&self) -> usize {
        self.target_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_vector() {
        let mut builder = PlainVectorBlockBuilder::new(128);
        builder.append(Some(VectorRef::new(&[1.0.into(), 2.0.into(), 3.0.into()])));
        builder.append(Some(VectorRef::new(&[4.0.into(), 5.0.into(), 6.0.into()])));
        builder.append_value(VectorRef::new(&[7.0.into(), 8.0.into(), 9.0.into()]));
        assert_eq!(builder.estimated_size(), 3 * 3 * 8 + 4);
        assert!(!builder.should_finish(&Some(VectorRef::new(&[
            10.0.into(),
            11.0.into(),
            12.0.into()
        ]))));
        builder.finish();
    }
}
