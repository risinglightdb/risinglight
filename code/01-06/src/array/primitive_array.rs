use std::fmt::Debug;
use std::iter::FromIterator;

use bitvec::vec::BitVec;

use super::{Array, ArrayBuilder};

/// A collection of primitive types, such as `i32`, `f32`.
#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveArray<T: Primitive> {
    valid: BitVec,
    data: Vec<T>,
}

/// A trait over primitive types.
pub trait Primitive:
    PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
}

macro_rules! impl_primitive {
    ($($t:ty),*) => {
        $(impl Primitive for $t {})*
    }
}
impl_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64, bool);

/// Enable `collect()` an array from iterator of `Option<T>`.
impl<T: Primitive> FromIterator<Option<T>> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::with_capacity(iter.size_hint().0);
        for e in iter {
            builder.push(e.as_ref());
        }
        builder.finish()
    }
}

/// Enable `collect()` an array from iterator of `T`.
impl<T: Primitive> FromIterator<T> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        iter.into_iter().map(Some).collect()
    }
}

impl<T: Primitive> Array for PrimitiveArray<T> {
    type Item = T;
    type Builder = PrimitiveArrayBuilder<T>;

    fn get(&self, idx: usize) -> Option<&T> {
        self.valid[idx].then(|| &self.data[idx])
    }

    fn len(&self) -> usize {
        self.valid.len()
    }
}

/// A builder that constructs a [`PrimitiveArray`] from `Option<T>`.
pub struct PrimitiveArrayBuilder<T: Primitive> {
    valid: BitVec,
    data: Vec<T>,
}

impl<T: Primitive> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type Array = PrimitiveArray<T>;

    fn with_capacity(capacity: usize) -> Self {
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

    #[test]
    fn test_collect() {
        let iter = (0..1000).map(|x| if x % 2 == 0 { None } else { Some(x) });
        let array = iter.clone().collect::<PrimitiveArray<i32>>();
        assert_eq!(
            array.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            iter.collect::<Vec<_>>()
        );
    }
}
