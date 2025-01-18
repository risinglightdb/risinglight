// Copyright 2025 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::marker::PhantomData;
use std::mem;

use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};

use super::{
    Array, ArrayBuilder, ArrayEstimateExt, ArrayFromDataExt, ArrayValidExt, PrimitiveValueType,
    ValueRef,
};
use crate::types::{VectorRef, F64};

// A collection of fixed-length values.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkedArray<T: ValueRef<U> + ?Sized, U: PrimitiveValueType = u8> {
    /// The length of each element. If the array is empty, it could be 0 instead of the actual
    /// type's length.
    element_length: usize,
    valid: BitVec,
    data: Box<[U]>,
    _type: PhantomData<T>,
}

pub type VectorArray = ChunkedArray<VectorRef, F64>;
pub type VectorArrayBuilder = ChunkedArrayBuilder<VectorRef, F64>;

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> Clone for ChunkedArray<T, U> {
    fn clone(&self) -> Self {
        Self {
            element_length: self.element_length,
            valid: self.valid.clone(),
            data: self.data.clone(),
            _type: PhantomData,
        }
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> Array for ChunkedArray<T, U> {
    type Item = T;
    type Builder = ChunkedArrayBuilder<T, U>;

    fn is_null(&self, idx: usize) -> bool {
        !self.valid[idx]
    }

    fn get_raw(&self, idx: usize) -> &T {
        let data_slice = &self.data[self.element_length * idx..self.element_length * (idx + 1)];
        T::from_primitives(data_slice)
    }

    fn len(&self) -> usize {
        self.valid.len()
    }

    fn filter(&self, p: &[bool]) -> Self {
        assert_eq!(p.len(), self.len());
        let mut builder = Self::Builder::with_capacity(self.len());
        for (i, &v) in p.iter().enumerate() {
            if v {
                builder.push(self.get(i));
            }
        }
        builder.finish()
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayValidExt for ChunkedArray<T, U> {
    fn get_valid_bitmap(&self) -> &BitVec {
        &self.valid
    }
    fn get_valid_bitmap_mut(&mut self) -> &mut BitVec {
        &mut self.valid
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayEstimateExt for ChunkedArray<T, U> {
    fn get_estimated_size(&self) -> usize {
        self.data.len() + self.valid.len() / 8
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayFromDataExt for ChunkedArray<T, U> {
    fn from_data(data_iter: impl Iterator<Item = impl Borrow<Self::Item>>, valid: BitVec) -> Self {
        let mut data = Vec::with_capacity(valid.len());
        let mut element_length = None;
        for raw in data_iter {
            data.extend_from_slice(raw.borrow().as_ref());
            element_length = Some(raw.borrow().as_ref().len());
        }
        Self {
            valid,
            data: data.into(),
            element_length: element_length.unwrap_or_default(),
            _type: PhantomData,
        }
    }
}

/// A builder that uses `&T` to build an [`BytesArray`].
pub struct ChunkedArrayBuilder<T: ValueRef<U> + ?Sized, U: PrimitiveValueType = u8> {
    element_length: Option<usize>,
    valid: BitVec,
    data: Vec<U>,
    _type: PhantomData<T>,
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ChunkedArrayBuilder<T, U> {
    fn update_element_length(&mut self, length: usize) {
        if let Some(element_length) = self.element_length {
            assert_eq!(element_length, length);
        } else {
            self.element_length = Some(length);
        }
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayBuilder for ChunkedArrayBuilder<T, U> {
    type Array = ChunkedArray<T, U>;

    fn extend_from_raw_data(&mut self, raws: &[<<Self::Array as Array>::Item as ToOwned>::Owned]) {
        for raw in raws {
            self.data.extend_from_slice(raw.borrow().as_ref());
            self.update_element_length(raw.borrow().as_ref().len());
        }
    }

    fn extend_from_nulls(&mut self, _: usize) {
        panic!("null value in chunked array builder");
    }

    fn replace_bitmap(&mut self, valid: BitVec) {
        let _ = mem::replace(&mut self.valid, valid);
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            element_length: None,
            valid: BitVec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
            _type: PhantomData,
        }
    }

    fn reserve(&mut self, capacity: usize) {
        self.valid.reserve(capacity);
        // For variable-length values, we cannot know the exact size of the value.
        // Therefore, we reserve `capacity` here, but it may overflow during use.
        self.data.reserve(capacity);
    }

    fn push(&mut self, value: Option<&T>) {
        self.valid.push(value.is_some());
        if let Some(x) = value {
            self.data.extend_from_slice(x.as_ref());
            self.update_element_length(x.as_ref().len());
        } else {
            panic!("null value in chunked array builder");
        }
    }

    fn push_n(&mut self, n: usize, value: Option<&T>) {
        self.valid.resize(self.valid.len() + n, value.is_some());
        if let Some(value) = value {
            self.data.reserve(value.as_ref().len() * n);
            self.update_element_length(value.as_ref().len());
            // TODO: optimize: push the value only once
            for _ in 0..n {
                self.data.extend_from_slice(value.as_ref());
            }
        } else {
            panic!("null value in chunked array builder");
        }
    }

    fn append(&mut self, other: &ChunkedArray<T, U>) {
        self.valid.extend_from_bitslice(&other.valid);
        self.data.extend_from_slice(&other.data);
        self.update_element_length(other.element_length);
    }

    fn take(&mut self) -> ChunkedArray<T, U> {
        ChunkedArray {
            valid: mem::take(&mut self.valid),
            data: mem::take(&mut self.data).into(),
            element_length: self.element_length.unwrap_or_default(),
            _type: PhantomData,
        }
    }
}

#[allow(dead_code)]
struct ChunkedArrayWriter<'a, T: ValueRef<U> + ?Sized, U: PrimitiveValueType> {
    builder: &'a mut ChunkedArrayBuilder<T, U>,
    written_length: usize,
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ChunkedArrayWriter<'_, T, U> {
    #[allow(dead_code)]
    fn write_chunk(&mut self, s: &[U]) {
        self.builder.data.extend_from_slice(s);
        self.written_length += s.len();
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> Drop for ChunkedArrayWriter<'_, T, U> {
    fn drop(&mut self) {
        self.builder.update_element_length(self.written_length);
        self.builder.valid.push(true);
    }
}

// Enable `collect()` an array from iterator of `Option<&T>` or `Option<T::Owned>`.
impl<O: AsRef<T>, T: ValueRef<U> + ?Sized, U: PrimitiveValueType> FromIterator<Option<O>>
    for ChunkedArray<T, U>
{
    fn from_iter<I: IntoIterator<Item = Option<O>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::with_capacity(iter.size_hint().0);
        for e in iter {
            if let Some(s) = e {
                builder.push(Some(s.as_ref()));
            } else {
                builder.push(None);
            }
        }
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_vector_array_builder() {
        let mut builder = VectorArrayBuilder::with_capacity(100);
        for i in 0..100 {
            if i % 2 == 0 {
                builder.push(Some(VectorRef::new(&[
                    F64::from(i),
                    F64::from(i * 2),
                    F64::from(i * 3),
                ])));
            } else {
                builder.push(Some(VectorRef::new(&[
                    F64::from(i * 4),
                    F64::from(i * 5),
                    F64::from(i * 6),
                ])));
            }
        }
        builder.finish();
    }
}
