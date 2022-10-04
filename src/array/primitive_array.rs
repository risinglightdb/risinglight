// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::iter::{FromIterator, TrustedLen};
use std::mem;

use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};

use super::{Array, ArrayBuilder, ArrayEstimateExt, ArrayFromDataExt, ArrayValidExt};
use crate::types::{NativeType, F32, F64};

mod simd;
pub use self::simd::*;

/// A collection of primitive types, such as `i32`, `F32`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PrimitiveArray<T: NativeType> {
    valid: BitVec,
    data: Vec<T>,
}

// Enable `collect()` an array from iterator of `Option<T>`.
impl<T: NativeType> FromIterator<Option<T>> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::with_capacity(iter.size_hint().0);
        for e in iter {
            builder.push(e.as_ref());
        }
        builder.finish()
    }
}

// Enable `collect()` an array from iterator of `T`.
impl<T: NativeType> FromIterator<T> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let data: Vec<T> = iter.into_iter().collect();
        let size = data.len();
        Self {
            data,
            valid: BitVec::repeat(true, size),
        }
    }
}

impl FromIterator<f32> for PrimitiveArray<F32> {
    fn from_iter<I: IntoIterator<Item = f32>>(iter: I) -> Self {
        let data: Vec<F32> = iter.into_iter().map(F32::from).collect();
        let size = data.len();
        Self {
            data,
            valid: BitVec::repeat(true, size),
        }
    }
}

impl FromIterator<f64> for PrimitiveArray<F64> {
    fn from_iter<I: IntoIterator<Item = f64>>(iter: I) -> Self {
        let data: Vec<F64> = iter.into_iter().map(F64::from).collect();
        let size = data.len();
        Self {
            data,
            valid: BitVec::repeat(true, size),
        }
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    type Item = T;
    type Builder = PrimitiveArrayBuilder<T>;
    type RawIter<'a> = std::slice::Iter<'a, T>;

    fn get(&self, idx: usize) -> Option<&T> {
        self.valid[idx].then(|| &self.data[idx])
    }

    fn get_unchecked(&self, idx: usize) -> &T {
        &self.data[idx]
    }

    fn len(&self) -> usize {
        self.valid.len()
    }

    fn raw_iter(&self) -> Self::RawIter<'_> {
        self.data.iter()
    }
}

impl<T: NativeType> ArrayValidExt for PrimitiveArray<T> {
    fn get_valid_bitmap(&self) -> &BitVec {
        &self.valid
    }
}

impl<T: NativeType> ArrayEstimateExt for PrimitiveArray<T> {
    fn get_estimated_size(&self) -> usize {
        self.data.len() * std::mem::size_of::<T>() + self.valid.len() / 8
    }
}

impl<T: NativeType> ArrayFromDataExt for PrimitiveArray<T> {
    fn from_data(
        data_iter: impl Iterator<Item = <Self::Item as ToOwned>::Owned> + TrustedLen,
        valid: BitVec,
    ) -> Self {
        let data = data_iter.collect();
        Self { valid, data }
    }
}

/// A builder that constructs a [`PrimitiveArray`] from `Option<T>`.
pub struct PrimitiveArrayBuilder<T: NativeType> {
    valid: BitVec,
    data: Vec<T>,
}

impl<T: NativeType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type Array = PrimitiveArray<T>;

    fn extend_from_raw_data(&mut self, raw: &[<<Self::Array as Array>::Item as ToOwned>::Owned]) {
        self.data.extend_from_slice(raw);
    }

    fn extend_from_nulls(&mut self, count: usize) {
        self.data.extend((0..count).map(|_| T::default()));
    }

    fn replace_bitmap(&mut self, valid: BitVec) {
        let _ = mem::replace(&mut self.valid, valid);
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            valid: BitVec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, capacity: usize) {
        self.valid.reserve(capacity);
        self.data.reserve(capacity);
    }

    fn push(&mut self, value: Option<&T>) {
        self.valid.push(value.is_some());
        self.data.push(value.cloned().unwrap_or_default());
    }

    fn append(&mut self, other: &PrimitiveArray<T>) {
        self.valid.extend_from_bitslice(&other.valid);
        self.data.extend_from_slice(&other.data);
    }

    fn take(&mut self) -> PrimitiveArray<T> {
        PrimitiveArray {
            valid: mem::take(&mut self.valid),
            data: mem::take(&mut self.data),
        }
    }
}

#[cfg(test)]
mod tests {
    use num_traits::cast::FromPrimitive;
    use rust_decimal::Decimal;

    use super::*;
    use crate::types::{F32, F64};

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
        test_builder::<F32>();
    }

    #[test]
    fn test_builder_f64() {
        test_builder::<F64>();
    }

    #[test]
    fn test_builder_decimal() {
        test_builder::<Decimal>();
    }
}
