#[cfg(feature = "simd")]
use crate::types::NativeType;
use crate::types::{DataType, DataTypeKind, DataValue};
#[cfg(feature = "simd")]
use core_simd::{LaneCount, SimdElement, SupportedLaneCount};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::ops::{Bound, RangeBounds};

mod data_chunk;
mod iterator;
mod primitive_array;
mod utf8_array;

pub use self::data_chunk::*;
pub use self::iterator::ArrayIter;
pub use self::primitive_array::*;
pub use self::utf8_array::*;

mod valid_ext;
pub use valid_ext::*;

mod shuffle_ext;
pub use shuffle_ext::*;

/// A trait over all array builders.
///
/// `ArrayBuilder` is a trait over all builders. You could build an array with
/// `push` with the help of `ArrayBuilder` trait. The `push` function always
/// accepts reference to an element. e.g. for `PrimitiveArray`,
/// you must do `builder.push(Some(&1))`. For `UTF8Array`, you must do
/// `builder.push(Some("xxx"))`. Note that you don't need to construct a `String`.
///
/// The associated type `Array` is the type of the corresponding array. It is the
/// return type of `finish`.
pub trait ArrayBuilder {
    /// Corresponding `Array` of this builder
    type Array: Array<Builder = Self>;

    /// Create a new builder with `capacity`.
    fn new(capacity: usize) -> Self;

    /// Append a value to builder.
    fn push(&mut self, value: Option<&<Self::Array as Array>::Item>);

    /// Append an array to builder.
    fn append(&mut self, other: &Self::Array);

    /// Finish build and return a new array.
    fn finish(self) -> Self::Array;
}

/// A trait over all array.
///
/// `Array` must be built with an `ArrayBuilder`. The array trait provides several
/// unified interface on an array, like `len`, `get` and `iter`.
///
/// The `Builder` associated type is the builder for this array.
/// The `Item` is the item you could retrieve from this array.
///
/// For example, `PrimitiveArray` could return an `Option<&u32>`, and `UTF8Array` will
/// return an `Option<&str>`.
pub trait Array: Sized {
    /// Corresponding builder of this array.
    type Builder: ArrayBuilder<Array = Self>;

    /// Type of element in the array.
    type Item: ToOwned + ?Sized;

    /// Retrieve a reference to value.
    fn get(&self, idx: usize) -> Option<&Self::Item>;

    /// Number of items of array.
    fn len(&self) -> usize;

    /// Get iterator of current array.
    fn iter(&self) -> ArrayIter<'_, Self> {
        ArrayIter::new(self)
    }

    /// Check if `Array` is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// An extension trait for [`Array`].
pub trait ArrayExt: Array {
    /// Filter the elements and return a new array.
    fn filter(&self, visibility: impl Iterator<Item = bool>) -> Self;

    /// Return a slice of self for the provided range.
    fn slice(&self, range: impl RangeBounds<usize>) -> Self;
}

impl<A: Array> ArrayExt for A {
    /// Filter the elements and return a new array.
    fn filter(&self, visibility: impl Iterator<Item = bool>) -> Self {
        let mut builder = Self::Builder::new(self.len());
        for (a, visible) in self.iter().zip(visibility) {
            if visible {
                builder.push(a);
            }
        }
        builder.finish()
    }

    /// Return a slice of self for the provided range.
    fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let len = self.len();
        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };
        assert!(begin <= end, "range start must not be greater than end");
        assert!(end <= len, "range end out of bounds");

        let mut builder = Self::Builder::new(end - begin);
        for i in begin..end {
            builder.push(self.get(i));
        }
        builder.finish()
    }
}

pub type BoolArray = PrimitiveArray<bool>;
pub type I32Array = PrimitiveArray<i32>;
pub type F64Array = PrimitiveArray<f64>;

/// Embeds all types of arrays in `array` module.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub enum ArrayImpl {
    Bool(BoolArray),
    // Int16(PrimitiveArray<i16>),
    Int32(I32Array),
    // Int64(PrimitiveArray<i64>),
    // Float32(PrimitiveArray<f32>),
    Float64(F64Array),
    UTF8(UTF8Array),
}

pub type BoolArrayBuilder = PrimitiveArrayBuilder<bool>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<f64>;

/// Embeds all types of array builders in `array` module.
pub enum ArrayBuilderImpl {
    Bool(BoolArrayBuilder),
    // Int16(PrimitiveArrayBuilder<i16>),
    Int32(I32ArrayBuilder),
    // Int64(PrimitiveArrayBuilder<i64>),
    // Float32(PrimitiveArrayBuilder<f32>),
    Float64(F64ArrayBuilder),
    UTF8(UTF8ArrayBuilder),
}

/// An error which can be returned when downcasting an [`ArrayImpl`] into a concrete type array.
#[derive(Debug, Clone)]
pub struct TypeMismatch;

macro_rules! impl_into {
    ($x:ty, $y:ident) => {
        impl From<$x> for ArrayImpl {
            fn from(array: $x) -> Self {
                Self::$y(array)
            }
        }

        impl TryFrom<ArrayImpl> for $x {
            type Error = TypeMismatch;

            fn try_from(array: ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    ArrayImpl::$y(array) => Ok(array),
                    _ => Err(TypeMismatch),
                }
            }
        }

        impl<'a> TryFrom<&'a ArrayImpl> for &'a $x {
            type Error = TypeMismatch;

            fn try_from(array: &'a ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    ArrayImpl::$y(array) => Ok(array),
                    _ => Err(TypeMismatch),
                }
            }
        }
    };
}

impl_into! { PrimitiveArray<bool>, Bool }
// impl_into! { PrimitiveArray<i16>, Int16 }
impl_into! { PrimitiveArray<i32>, Int32 }
// impl_into! { PrimitiveArray<i64>, Int64 }
// impl_into! { PrimitiveArray<f32>, Float32 }
impl_into! { PrimitiveArray<f64>, Float64 }
impl_into! { UTF8Array, UTF8 }

#[cfg(feature = "simd")]
pub trait ArraySIMDSum<T, const N: usize>
where
    T: SimdElement + NativeType,
    LaneCount<N>: SupportedLaneCount,
{
    fn simd_sum(&self) -> T;
}

macro_rules! impl_simd_sum_for_arr {
    ($t:ty, $e: expr) => {
        #[cfg(feature = "simd")]
        impl ArraySIMDSum<$t, $e> for PrimitiveArray<$t> {
            fn simd_sum(&self) -> $t {
                let mut iter = self.batch_iter::<$e>();
                let mut sum: $t = 0 as $t;
                while let Some(batch) = iter.next() {
                    sum += batch.sum();
                }
                sum
            }
        }
    };
}

impl_simd_sum_for_arr!(i32, 32);
impl_simd_sum_for_arr!(f64, 32);

impl ArrayBuilderImpl {
    /// Create a new array builder from data type.
    pub fn new(ty: &DataType) -> Self {
        match ty.kind() {
            DataTypeKind::Boolean => Self::Bool(PrimitiveArrayBuilder::<bool>::new(0)),
            DataTypeKind::Int => Self::Int32(PrimitiveArrayBuilder::<i32>::new(0)),
            DataTypeKind::Double => Self::Float64(PrimitiveArrayBuilder::<f64>::new(0)),
            DataTypeKind::Char(_) => Self::UTF8(UTF8ArrayBuilder::new(0)),
            DataTypeKind::Varchar(_) => Self::UTF8(UTF8ArrayBuilder::new(0)),
            _ => panic!("unsupported data type"),
        }
    }

    /// Create a new array builder from data value.
    pub fn from_type_of_value(val: &DataValue) -> Self {
        match val {
            DataValue::Bool(_) => Self::Bool(PrimitiveArrayBuilder::<bool>::new(0)),
            DataValue::Int32(_) => Self::Int32(PrimitiveArrayBuilder::<i32>::new(0)),
            DataValue::Float64(_) => Self::Float64(PrimitiveArrayBuilder::<f64>::new(0)),
            _ => panic!("unsupported data type"),
        }
    }

    /// Create a new array builder with the same type of given array.
    pub fn from_type_of_array(array: &ArrayImpl) -> Self {
        match array {
            ArrayImpl::Bool(_) => Self::Bool(PrimitiveArrayBuilder::<bool>::new(0)),
            ArrayImpl::Int32(_) => Self::Int32(PrimitiveArrayBuilder::<i32>::new(0)),
            ArrayImpl::Float64(_) => Self::Float64(PrimitiveArrayBuilder::<f64>::new(0)),
            ArrayImpl::UTF8(_) => Self::UTF8(UTF8ArrayBuilder::new(0)),
        }
    }

    /// Appends an element to the back of array.
    pub fn push(&mut self, v: &DataValue) {
        match (self, v) {
            (Self::Bool(a), DataValue::Bool(v)) => a.push(Some(v)),
            (Self::Int32(a), DataValue::Int32(v)) => a.push(Some(v)),
            (Self::Float64(a), DataValue::Float64(v)) => a.push(Some(v)),
            (Self::UTF8(a), DataValue::String(v)) => a.push(Some(v)),
            (Self::Bool(a), DataValue::Null) => a.push(None),
            (Self::Int32(a), DataValue::Null) => a.push(None),
            (Self::Float64(a), DataValue::Null) => a.push(None),
            (Self::UTF8(a), DataValue::Null) => a.push(None),
            _ => panic!("failed to push value: type mismatch"),
        }
    }

    /// Finish build and return a new array.
    pub fn finish(self) -> ArrayImpl {
        match self {
            Self::Bool(a) => ArrayImpl::Bool(a.finish()),
            Self::Int32(a) => ArrayImpl::Int32(a.finish()),
            Self::Float64(a) => ArrayImpl::Float64(a.finish()),
            Self::UTF8(a) => ArrayImpl::UTF8(a.finish()),
        }
    }

    /// Appends a DataArrayImpl
    pub fn append(&mut self, array_impl: &ArrayImpl) {
        match (self, array_impl) {
            (Self::Bool(builder), ArrayImpl::Bool(arr)) => builder.append(arr),
            (Self::Int32(builder), ArrayImpl::Int32(arr)) => builder.append(arr),
            (Self::Float64(builder), ArrayImpl::Float64(arr)) => builder.append(arr),
            (Self::UTF8(builder), ArrayImpl::UTF8(arr)) => builder.append(arr),
            _ => panic!("failed to push value: type mismatch"),
        }
    }
}

impl ArrayImpl {
    /// Get the value and convert it to string.
    pub fn get_to_string(&self, idx: usize) -> String {
        match self {
            Self::Bool(a) => a.get(idx).map(|v| v.to_string()),
            Self::Int32(a) => a.get(idx).map(|v| v.to_string()),
            Self::Float64(a) => a.get(idx).map(|v| v.to_string()),
            Self::UTF8(a) => a.get(idx).map(|v| v.to_string()),
        }
        .unwrap_or_else(|| "NULL".into())
    }

    /// Get the value at the given index.
    pub fn get(&self, idx: usize) -> DataValue {
        match self {
            Self::Bool(a) => match a.get(idx) {
                Some(val) => DataValue::Bool(*val),
                None => DataValue::Null,
            },
            Self::Int32(a) => match a.get(idx) {
                Some(val) => DataValue::Int32(*val),
                None => DataValue::Null,
            },
            Self::Float64(a) => match a.get(idx) {
                Some(val) => DataValue::Float64(*val),
                None => DataValue::Null,
            },
            Self::UTF8(a) => match a.get(idx) {
                Some(val) => DataValue::String(val.to_string()),
                None => DataValue::Null,
            },
        }
    }

    /// Number of items of array.
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(a) => a.len(),
            Self::Int32(a) => a.len(),
            Self::Float64(a) => a.len(),
            Self::UTF8(a) => a.len(),
        }
    }

    /// Check if array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Filter the elements and return a new array.
    pub fn filter(&self, visibility: impl Iterator<Item = bool>) -> Self {
        match self {
            Self::Bool(a) => Self::Bool(a.filter(visibility)),
            Self::Int32(a) => Self::Int32(a.filter(visibility)),
            Self::Float64(a) => Self::Float64(a.filter(visibility)),
            Self::UTF8(a) => Self::UTF8(a.filter(visibility)),
        }
    }

    /// Return a slice of self for the provided range.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        match self {
            Self::Bool(a) => Self::Bool(a.slice(range)),
            Self::Int32(a) => Self::Int32(a.slice(range)),
            Self::Float64(a) => Self::Float64(a.slice(range)),
            Self::UTF8(a) => Self::UTF8(a.slice(range)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter() {
        let array: PrimitiveArray<i32> = (0..=60).map(Some).collect();
        let array: PrimitiveArray<i32> = array
            .iter()
            .filter(|x| *x.unwrap_or(&0) >= 60)
            .map(|x| x.cloned())
            .collect();
        assert_eq!(
            array.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            vec![Some(60)]
        );
    }

    use crate::types::NativeType;
    use num_traits::cast::AsPrimitive;
    use num_traits::ops::checked::CheckedAdd;

    fn vec_add<T1, T2, T3>(a: &PrimitiveArray<T1>, b: &PrimitiveArray<T2>) -> PrimitiveArray<T3>
    where
        T1: NativeType + AsPrimitive<T3>,
        T2: NativeType + AsPrimitive<T3>,
        T3: NativeType + CheckedAdd,
    {
        assert_eq!(a.len(), b.len());
        a.iter()
            .zip(b.iter())
            .map(|(a, b)| match (a, b) {
                (Some(a), Some(b)) => Some(a.as_() + b.as_()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_vectorized_add() {
        let array1 = (0i32..=60).map(Some).collect();
        let array2 = (0i16..=60).map(Some).collect();

        let final_array = vec_add(&array1, &array2) as PrimitiveArray<i64>;
        assert_eq!(
            final_array.iter().map(|x| x.cloned()).collect::<Vec<_>>(),
            (0i64..=60).map(|i| Some(i * 2)).collect::<Vec<_>>()
        );
    }
}
