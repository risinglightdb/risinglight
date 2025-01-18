// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::fmt::{Display, Write};
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;

use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};

use super::{Array, ArrayBuilder, ArrayEstimateExt, ArrayFromDataExt, ArrayValidExt};
use crate::types::{BlobRef, VectorRef, F64};

// A collection of variable-length values.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VarArray<T: ValueRef<U> + ?Sized, U: PrimitiveValueType = u8> {
    offset: Box<[usize]>,
    valid: BitVec,
    data: Box<[U]>,
    _type: PhantomData<T>,
}

pub trait PrimitiveValueType: Send + Sync + 'static + Copy + Clone + PartialEq + Eq + Hash + Default {}

impl PrimitiveValueType for u8 {}
impl PrimitiveValueType for F64 {}

/// The borrowed type of a variable-length value.
pub trait ValueRef<U>: ToOwned + AsRef<[U]> + Send + Sync + 'static {
    fn from_primitives(s: &[U]) -> &Self;
}

impl ValueRef<u8> for str {
    fn from_primitives(s: &[u8]) -> &Self {
        unsafe { std::str::from_utf8_unchecked(s) }
    }
}

impl ValueRef<u8> for BlobRef {
    fn from_primitives(s: &[u8]) -> &Self {
        BlobRef::new(s)
    }
}

impl BlobRef {
    pub fn from_bytes(s: &[u8]) -> &Self {
        BlobRef::new(s)
    }
}

impl ValueRef<F64> for VectorRef {
    fn from_primitives(s: &[F64]) -> &Self {
        VectorRef::new(s)
    }
}

pub type StringArray = VarArray<str>;
pub type BlobArray = VarArray<BlobRef>;
pub type StringArrayBuilder = BytesArrayBuilder<str>;
pub type BlobArrayBuilder = BytesArrayBuilder<BlobRef>;

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> Clone for VarArray<T, U> {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset.clone(),
            valid: self.valid.clone(),
            data: self.data.clone(),
            _type: PhantomData,
        }
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> Array for VarArray<T, U> {
    type Item = T;
    type Builder = BytesArrayBuilder<T, U>;

    fn is_null(&self, idx: usize) -> bool {
        !self.valid[idx]
    }

    fn get_raw(&self, idx: usize) -> &T {
        let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
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

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayValidExt for VarArray<T, U> {
    fn get_valid_bitmap(&self) -> &BitVec {
        &self.valid
    }
    fn get_valid_bitmap_mut(&mut self) -> &mut BitVec {
        &mut self.valid
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayEstimateExt for VarArray<T, U> {
    fn get_estimated_size(&self) -> usize {
        self.data.len() + self.offset.len() + self.valid.len() / 8
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayFromDataExt for VarArray<T, U> {
    fn from_data(data_iter: impl Iterator<Item = impl Borrow<Self::Item>>, valid: BitVec) -> Self {
        let mut data = Vec::with_capacity(valid.len());
        let mut offset = Vec::with_capacity(valid.len() + 1);
        offset.push(0);
        for raw in data_iter {
            data.extend_from_slice(raw.borrow().as_ref());
            offset.push(data.len());
        }
        Self {
            valid,
            data: data.into(),
            offset: offset.into(),
            _type: PhantomData,
        }
    }
}

/// A builder that uses `&T` to build an [`BytesArray`].
pub struct BytesArrayBuilder<T: ValueRef<U> + ?Sized, U: PrimitiveValueType = u8> {
    offset: Vec<usize>,
    valid: BitVec,
    data: Vec<U>,
    _type: PhantomData<T>,
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> ArrayBuilder for BytesArrayBuilder<T, U> {
    type Array = VarArray<T, U>;

    fn extend_from_raw_data(&mut self, raws: &[<<Self::Array as Array>::Item as ToOwned>::Owned]) {
        for raw in raws {
            self.data.extend_from_slice(raw.borrow().as_ref());
            self.offset.push(self.data.len());
        }
    }

    fn extend_from_nulls(&mut self, count: usize) {
        let len = self.data.len();
        self.offset.extend((0..count).map(|_| len));
    }

    fn replace_bitmap(&mut self, valid: BitVec) {
        let _ = mem::replace(&mut self.valid, valid);
    }

    fn with_capacity(capacity: usize) -> Self {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Self {
            offset,
            data: Vec::with_capacity(capacity),
            valid: BitVec::with_capacity(capacity),
            _type: PhantomData,
        }
    }

    fn reserve(&mut self, capacity: usize) {
        self.offset.reserve(capacity + 1);
        self.valid.reserve(capacity);
        // For variable-length values, we cannot know the exact size of the value.
        // Therefore, we reserve `capacity` here, but it may overflow during use.
        self.data.reserve(capacity);
    }

    fn push(&mut self, value: Option<&T>) {
        self.valid.push(value.is_some());
        if let Some(x) = value {
            self.data.extend_from_slice(x.as_ref());
        }
        self.offset.push(self.data.len());
    }

    fn push_n(&mut self, n: usize, value: Option<&T>) {
        self.valid.resize(self.valid.len() + n, value.is_some());
        if let Some(value) = value {
            self.data.reserve(value.as_ref().len() * n);
            self.offset.reserve(n);
            // TODO: optimize: push the value only once
            for _ in 0..n {
                self.data.extend_from_slice(value.as_ref());
                self.offset.push(self.data.len());
            }
        } else {
            self.offset
                .extend(std::iter::repeat(self.data.len()).take(n));
        }
    }

    fn append(&mut self, other: &VarArray<T, U>) {
        self.valid.extend_from_bitslice(&other.valid);
        self.data.extend_from_slice(&other.data);
        let start = *self.offset.last().unwrap();
        for other_offset in &other.offset[1..] {
            self.offset.push(*other_offset + start);
        }
    }

    fn take(&mut self) -> VarArray<T, U> {
        VarArray {
            valid: mem::take(&mut self.valid),
            data: mem::take(&mut self.data).into(),
            offset: mem::replace(&mut self.offset, vec![0]).into(),
            _type: PhantomData,
        }
    }
}

struct BytesArrayWriter<'a, T: ValueRef<U> + ?Sized, U: PrimitiveValueType> {
    builder: &'a mut BytesArrayBuilder<T, U>,
}

impl<T: ValueRef<u8> + ?Sized> Write for BytesArrayWriter<'_, T, u8> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.builder.data.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

impl<T: ValueRef<U> + ?Sized, U: PrimitiveValueType> Drop for BytesArrayWriter<'_, T, U> {
    fn drop(&mut self) {
        self.builder.offset.push(self.builder.data.len());
        self.builder.valid.push(true);
    }
}

impl StringArray {
    pub fn from_iter_display(iter: impl IntoIterator<Item = Option<impl Display>>) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::with_capacity(iter.size_hint().0);
        for e in iter {
            if let Some(s) = e {
                let mut writer = BytesArrayWriter {
                    builder: &mut builder,
                };
                write!(writer, "{}", s).unwrap();
            } else {
                builder.push(None);
            }
        }
        builder.finish()
    }
}

// Enable `collect()` an array from iterator of `Option<&T>` or `Option<T::Owned>`.
impl<O: AsRef<T>, T: ValueRef<U> + ?Sized, U: PrimitiveValueType> FromIterator<Option<O>>
    for VarArray<T, U>
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
    fn test_string_array_builder() {
        let mut builder = StringArrayBuilder::with_capacity(100);
        for i in 0..100 {
            if i % 2 == 0 {
                builder.push(Some(&format!("{}", i)));
            } else {
                builder.push(None);
            }
        }
        builder.finish();
    }
}
