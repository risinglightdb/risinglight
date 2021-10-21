use crate::array::I32Array;

use super::BlockBuilder;

pub struct PlainI32BlockBuilder {
    data: Vec<u8>,
    target_size: usize,
}

impl PlainI32BlockBuilder {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        Self { data, target_size }
    }
}

impl BlockBuilder<I32Array> for PlainI32BlockBuilder {
    fn append(&mut self, item: &i32) {
        self.data.extend(item.to_le_bytes());
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &i32) -> bool {
        !self.data.is_empty() && self.data.len() + std::mem::size_of::<i32>() > self.target_size
    }

    fn finish(self) -> Vec<u8> {
        self.data
    }
}
