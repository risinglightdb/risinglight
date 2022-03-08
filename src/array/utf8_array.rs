// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::mem;

use bitvec::vec::BitVec;
use serde::{Deserialize, Serialize};

use super::iterator::NoNullArrayIter;
use super::{Array, ArrayBuilder, ArrayEstimateExt, ArrayValidExt};
use crate::types::BlobRef;

/// A collection of variable-length values.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct BytesArray<T: ValueRef + ?Sized> {
    offset: Vec<usize>,
    valid: BitVec,
    data: Vec<u8>,
    _type: PhantomData<T>,
}

/// The borrowed type of a variable-length value.
pub trait ValueRef: ToOwned + AsRef<[u8]> + Send + Sync + 'static {
    fn from_bytes(s: &[u8]) -> &Self;
}

impl ValueRef for str {
    fn from_bytes(s: &[u8]) -> &Self {
        unsafe { std::str::from_utf8_unchecked(s) }
    }
}

impl ValueRef for BlobRef {
    fn from_bytes(s: &[u8]) -> &Self {
        BlobRef::new(s)
    }
}

pub type Utf8Array = BytesArray<str>;
pub type BlobArray = BytesArray<BlobRef>;
pub type Utf8ArrayBuilder = BytesArrayBuilder<str>;
pub type BlobArrayBuilder = BytesArrayBuilder<BlobRef>;

impl<T: ValueRef + ?Sized> Clone for BytesArray<T> {
    fn clone(&self) -> Self {
        Self {
            offset: self.offset.clone(),
            valid: self.valid.clone(),
            data: self.data.clone(),
            _type: PhantomData,
        }
    }
}

impl<T: ValueRef + ?Sized> Array for BytesArray<T> {
    type Item = T;
    type Builder = BytesArrayBuilder<T>;
    type NonNullIterator<'a> = NoNullArrayIter<'a, Self>;

    fn get(&self, idx: usize) -> Option<&T> {
        if self.valid[idx] {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(T::from_bytes(data_slice))
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.valid.len()
    }

    fn non_null_iter(&self) -> Self::NonNullIterator<'_> {
        NoNullArrayIter::new(&self)
    }
}

impl<T: ValueRef + ?Sized> ArrayValidExt for BytesArray<T> {
    fn get_valid_bitmap(&self) -> &BitVec {
        &self.valid
    }
}

impl<T: ValueRef + ?Sized> ArrayEstimateExt for BytesArray<T> {
    fn get_estimated_size(&self) -> usize {
        self.data.len() + self.offset.len() + self.valid.len() / 8
    }
}

/// A builder that uses `&T` to build an [`BytesArray`].
pub struct BytesArrayBuilder<T: ValueRef + ?Sized> {
    offset: Vec<usize>,
    valid: BitVec,
    data: Vec<u8>,
    _type: PhantomData<T>,
}

impl<T: ValueRef + ?Sized> ArrayBuilder for BytesArrayBuilder<T> {
    type Array = BytesArray<T>;

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

    fn append(&mut self, other: &BytesArray<T>) {
        self.valid.extend_from_bitslice(&other.valid);
        self.data.extend_from_slice(&other.data);
        let start = *self.offset.last().unwrap();
        for other_offset in &other.offset[1..] {
            self.offset.push(*other_offset + start);
        }
    }

    fn take(&mut self) -> BytesArray<T> {
        BytesArray {
            valid: mem::take(&mut self.valid),
            data: mem::take(&mut self.data),
            offset: mem::replace(&mut self.offset, vec![0]),
            _type: PhantomData,
        }
    }
}

// Enable `collect()` an array from iterator of `Option<&T>` or `Option<T::Owned>`.
impl<O: AsRef<T>, T: ValueRef + ?Sized> FromIterator<Option<O>> for BytesArray<T> {
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
    fn test_utf8_builder() {
        let mut builder = Utf8ArrayBuilder::with_capacity(100);
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
