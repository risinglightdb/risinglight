use std::marker::PhantomData;

use super::super::PrimitiveFixedWidthEncode;
use super::BlockBuilder;

/// Encodes fixed-width data into a block, with null element support.
///
/// The layout is fixed-width data and a u8 bitmap, concatenated together.
pub struct PlainNullablePrimitiveBlockBuilder<T: PrimitiveFixedWidthEncode> {
    data: Vec<u8>,
    bitmap: Vec<bool>,
    target_size: usize,
    _phantom: PhantomData<T>,
}

impl<T: PrimitiveFixedWidthEncode> PlainNullablePrimitiveBlockBuilder<T> {
    pub fn new(target_size: usize) -> Self {
        let data = Vec::with_capacity(target_size);
        let bitmap = Vec::with_capacity(target_size);
        Self {
            data,
            target_size,
            bitmap,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveFixedWidthEncode> BlockBuilder<T::ArrayType>
    for PlainNullablePrimitiveBlockBuilder<T>
{
    fn append(&mut self, item: Option<&T>) {
        if let Some(item) = item {
            item.encode(&mut self.data);
            self.bitmap.push(true);
        } else {
            T::DEAFULT_VALUE.encode(&mut self.data);
            self.bitmap.push(false);
        }
    }

    fn estimated_size(&self) -> usize {
        self.data.len() + self.bitmap.len()
    }

    fn should_finish(&self, _next_item: &Option<&T>) -> bool {
        !self.data.is_empty() && self.estimated_size() + 1 + T::WIDTH > self.target_size
    }

    fn finish(self) -> Vec<u8> {
        let mut data = self.data;
        data.extend(self.bitmap.iter().map(|x| *x as u8));
        data
    }
}
