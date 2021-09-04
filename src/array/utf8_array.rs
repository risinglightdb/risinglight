// Author: Alex Chi (iskyzh@gmail.com)
use super::{Array, ArrayBuilder};
use std::iter::FromIterator;

/// `UTF8Array` is a collection of Rust UTF8 `String`s.
pub struct UTF8Array {
    offset: Vec<usize>,
    bitmap: Vec<bool>,
    data: Vec<u8>,
}

impl Array for UTF8Array {
    type Item = str;
    type Builder = UTF8ArrayBuilder;

    fn get(&self, idx: usize) -> Option<&str> {
        if self.bitmap[idx] {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(unsafe { std::str::from_utf8_unchecked(data_slice) })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }
}

/// `UTF8ArrayBuilder` use `&str` to build an `UTF8Array`.
pub struct UTF8ArrayBuilder {
    offset: Vec<usize>,
    bitmap: Vec<bool>,
    data: Vec<u8>,
}

impl ArrayBuilder for UTF8ArrayBuilder {
    type Array = UTF8Array;

    fn new(capacity: usize) -> Self {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Self {
            offset,
            data: Vec::with_capacity(capacity),
            bitmap: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<&str>) {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.data.extend_from_slice(x.as_bytes());
                self.offset.push(self.data.len())
            }
            None => {
                self.bitmap.push(false);
                self.offset.push(self.data.len())
            }
        }
    }

    fn append(&mut self, other: &UTF8Array) {
        self.bitmap.extend_from_slice(&other.bitmap);
        self.data.extend_from_slice(&other.data);
        let start = *self.offset.last().unwrap();
        for other_offset in &other.offset[1..] {
            self.offset.push(*other_offset + start);
        }
    }

    fn finish(self) -> UTF8Array {
        UTF8Array {
            bitmap: self.bitmap,
            data: self.data,
            offset: self.offset,
        }
    }
}

impl<'a> FromIterator<Option<&'a str>> for UTF8Array {
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for e in iter {
            builder.push(e);
        }
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_utf8_builder() {
        let mut builder = UTF8ArrayBuilder::new(0);
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
