use std::marker::PhantomData;

use super::super::encode::PrimitiveFixedWidthEncode;
use super::BlockBuilder;

/// Encodes fixed-width data into a block. The layout is simply an array of
/// little endian fixed-width data.
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
    fn append(&mut self, item: Option<&T>) {
        item.expect("nullable item found in non-nullable block builder")
            .encode(&mut self.data);
    }

    fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn should_finish(&self, _next_item: &Option<&T>) -> bool {
        !self.data.is_empty() && self.estimated_size() + T::WIDTH > self.target_size
    }

    fn finish(self) -> Vec<u8> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_i32() {
        let mut builder = PlainPrimitiveBlockBuilder::<i32>::new(128);
        builder.append(Some(&1));
        builder.append(Some(&2));
        builder.append(Some(&3));
        assert_eq!(builder.estimated_size(), 12);
        assert!(!builder.should_finish(&Some(&4)));
        builder.finish();
    }
}
