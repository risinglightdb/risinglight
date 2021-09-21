// Author: Alex Chi (iskyzh@gmail.com)

use super::{Array, ArrayBuilder};
use crate::types::NativeType;
use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};
use std::iter::FromIterator;

/// `PrimitiveArray` is a collection of primitive types, such as `i32`, `f32`.
#[derive(PartialEq, Serialize, Deserialize)]
pub struct PrimitiveArray<T: NativeType> {
    valid: BitVec,
    data: Vec<T>,
}

impl<T: NativeType> FromIterator<Option<T>> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for e in iter {
            builder.push(e.as_ref());
        }
        builder.finish()
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    type Item = T;
    type Builder = PrimitiveArrayBuilder<T>;

    fn get(&self, idx: usize) -> Option<&T> {
        self.valid[idx].then(|| &self.data[idx])
    }

    fn len(&self) -> usize {
        self.valid.len()
    }
}

/// `PrimitiveArrayBuilder` constructs a `PrimitiveArray` from `Option<Primitive>`.
pub struct PrimitiveArrayBuilder<T: NativeType> {
    valid: BitVec,
    data: Vec<T>,
}

impl<T: NativeType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type Array = PrimitiveArray<T>;

    fn new(capacity: usize) -> Self {
        Self {
            valid: BitVec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<&T>) {
        self.valid.push(value.is_some());
        self.data.push(value.cloned().unwrap_or_default());
    }

    fn append(&mut self, other: &PrimitiveArray<T>) {
        self.valid.extend_from_bitslice(&other.valid);
        self.data.extend_from_slice(&other.data);
    }

    fn finish(self) -> PrimitiveArray<T> {
        PrimitiveArray {
            valid: self.valid,
            data: self.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::cast::FromPrimitive;

    fn test_builder<T: FromPrimitive + NativeType>() {
        let iter = (0..1000).map(|x| if x % 2 == 0 { None } else { T::from_usize(x) });
        let array = iter.clone().collect::<PrimitiveArray<T>>();
        assert_eq!(
            array.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            iter.collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_builder_i16() {
        test_builder::<i16>();
    }

    #[test]
    fn test_builder_i32() {
        test_builder::<i32>();
    }

    #[test]
    fn test_builder_i64() {
        test_builder::<i64>();
    }

    #[test]
    fn test_builder_f32() {
        test_builder::<f32>();
    }

    #[test]
    fn test_builder_f64() {
        test_builder::<f64>();
    }
}
