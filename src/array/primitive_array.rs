// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::iter::FromIterator;
use std::mem;

use bitvec::vec::BitVec;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::ops::BitVecExt;
use super::{Array, ArrayBuilder, ArrayEstimateExt, ArrayFromDataExt, ArrayValidExt, BoolArray};
use crate::types::{NativeType, F32, F64};

/// A collection of primitive types, such as `i32`, `F32`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PrimitiveArray<T: NativeType> {
    valid: BitVec,
    data: Box<[T]>,
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
        let data: Box<[T]> = iter.into_iter().collect();
        let size = data.len();
        Self {
            data,
            valid: BitVec::repeat(true, size),
        }
    }
}

impl FromIterator<f32> for PrimitiveArray<F32> {
    fn from_iter<I: IntoIterator<Item = f32>>(iter: I) -> Self {
        let data: Box<[F32]> = iter.into_iter().map(F32::from).collect();
        let size = data.len();
        Self {
            data,
            valid: BitVec::repeat(true, size),
        }
    }
}

impl FromIterator<f64> for PrimitiveArray<F64> {
    fn from_iter<I: IntoIterator<Item = f64>>(iter: I) -> Self {
        let data: Box<[F64]> = iter.into_iter().map(F64::from).collect();
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

    fn is_null(&self, idx: usize) -> bool {
        !self.valid[idx]
    }

    fn get_raw(&self, idx: usize) -> &T {
        &self.data[idx]
    }

    fn len(&self) -> usize {
        self.valid.len()
    }

    fn raw_iter(&self) -> impl DoubleEndedIterator<Item = &Self::Item> {
        self.data.iter()
    }

    fn filter(&self, p: &[bool]) -> Self {
        assert_eq!(p.len(), self.len());
        let mut builder = Self::Builder::with_capacity(self.len());
        for (i, &v) in p.iter().enumerate() {
            if v {
                builder.valid.push(unsafe { *self.valid.get_unchecked(i) });
                builder.data.push(self.data[i]);
            }
        }
        builder.finish()
    }
}

impl<T: NativeType> ArrayValidExt for PrimitiveArray<T> {
    fn get_valid_bitmap(&self) -> &BitVec {
        &self.valid
    }
    fn get_valid_bitmap_mut(&mut self) -> &mut BitVec {
        &mut self.valid
    }
}

impl<T: NativeType> ArrayEstimateExt for PrimitiveArray<T> {
    fn get_estimated_size(&self) -> usize {
        self.data.len() * std::mem::size_of::<T>() + self.valid.len() / 8
    }
}

impl<T: NativeType> ArrayFromDataExt for PrimitiveArray<T> {
    fn from_data(data_iter: impl Iterator<Item = impl Borrow<Self::Item>>, valid: BitVec) -> Self {
        let data = data_iter.map(|v| *v.borrow()).collect();
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

    fn push_n(&mut self, n: usize, value: Option<&T>) {
        self.valid.resize(self.valid.len() + n, value.is_some());
        self.data
            .extend(std::iter::repeat(value.cloned().unwrap_or_default()).take(n));
    }

    fn append(&mut self, other: &PrimitiveArray<T>) {
        self.valid.extend_from_bitslice(&other.valid);
        self.data.extend_from_slice(&other.data);
    }

    fn take(&mut self) -> PrimitiveArray<T> {
        PrimitiveArray {
            valid: mem::take(&mut self.valid),
            data: mem::take(&mut self.data).into(),
        }
    }
}

impl PrimitiveArray<bool> {
    /// Converts the raw bool array into a [`BitVec`].
    pub fn to_raw_bitvec(&self) -> BitVec {
        BitVec::from_bool_slice(&self.data)
    }

    /// Returns a bool array of `true` values.
    pub fn true_array(&self) -> &[bool] {
        &self.data
    }
}

impl PrimitiveArray<Decimal> {
    /// Rescale the decimals.
    pub fn rescale(&mut self, scale: u8) {
        for v in &mut self.data {
            v.rescale(scale as u32);
        }
    }
}

pub fn clear_null(mut array: BoolArray) -> BoolArray {
    let mut valid = Vec::with_capacity(array.valid.as_raw_slice().len() * 64);
    for &bitmask in array.valid.as_raw_slice() {
        let chunk = std::simd::Mask::<i8, 64>::from_bitmask(bitmask as u64).to_array();
        valid.extend_from_slice(&chunk);
    }
    for (d, v) in array.data.iter_mut().zip(valid) {
        *d &= v;
    }
    array
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
