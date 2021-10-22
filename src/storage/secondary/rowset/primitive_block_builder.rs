use std::marker::PhantomData;

use super::encode::PrimitiveFixedWidthEncode;
use super::BlockBuilder;

pub struct PlainPrimitiveBlockBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,
    target_size: usize,
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainPrimitiveBlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        Self {
            data,
            target_size,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockBuilder<T::ArrayType> for PlainPrimitiveBlockBuilder<T> {
    fn append(&mut self, item: &T) {
        item.encode(&mut self.data);
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &T) -> bool {
        !self.data.is_empty() && self.data.len() + T::WIDTH > self.target_size
    }

    fn finish(self) -> Vec<u8> {
        self.data
    }
}
